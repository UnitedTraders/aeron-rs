/*
 * Copyright 2020 UT OVERSEAS INC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::sync::atomic::{fence, Ordering};

use crate::{
    concurrent::{
        atomic_buffer::AtomicBuffer,
        logbuffer::{
            buffer_claim::BufferClaim,
            data_frame_header, frame_descriptor,
            header::HeaderWriter,
            log_buffer_descriptor,
            term_appender::{OnReservedValueSupplier, TERM_APPENDER_FAILED},
        },
    },
    utils::{
        bit_utils,
        types::{Index, I64_SIZE},
    },
};

pub struct ExclusiveTermAppender {
    term_buffer: AtomicBuffer,
    tail_addr: *const i64,
}

impl ExclusiveTermAppender {
    pub fn new(term_buffer: AtomicBuffer, meta_data_buffer: AtomicBuffer, partition_index: Index) -> Self {
        // This check implemented as assert. Looks like out of bounds here can be reached only
        // due to a bug elsewhere. Therefore don't need more sophisticated error handling.
        meta_data_buffer.bounds_check(
            *log_buffer_descriptor::TERM_TAIL_COUNTER_OFFSET + (partition_index * I64_SIZE),
            I64_SIZE,
        );

        Self {
            term_buffer,
            tail_addr: (meta_data_buffer.buffer() as usize
                + *log_buffer_descriptor::TERM_TAIL_COUNTER_OFFSET as usize
                + (partition_index * I64_SIZE) as usize) as *const i64,
        }
    }

    pub fn term_buffer(&self) -> AtomicBuffer {
        self.term_buffer
    }

    pub fn raw_tail(&self) -> i64 {
        unsafe { *self.tail_addr } // brrrrr....
    }

    pub fn claim(
        &mut self,
        term_id: i32,
        term_offset: Index,
        header: &HeaderWriter,
        length: Index,
        buffer_claim: &mut BufferClaim,
    ) -> Index {
        let frame_length = length + data_frame_header::LENGTH;
        let aligned_length = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);

        let term_length = self.term_buffer.capacity();
        let mut resulting_offset = term_offset + aligned_length;
        self.put_raw_tail_ordered(term_id as i64, resulting_offset);

        if resulting_offset > term_length {
            resulting_offset = Self::handle_end_of_log_condition(&self.term_buffer, term_id, term_offset, header, term_length);
        } else {
            header.write(&self.term_buffer, term_offset, frame_length, term_id);
            buffer_claim.wrap_with_offset(&self.term_buffer, term_offset, frame_length);
        }

        resulting_offset
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append_unfragmented_message(
        &mut self,
        term_id: i32,
        term_offset: Index,
        header: &HeaderWriter,
        src_buffer: AtomicBuffer,
        src_offset: Index,
        length: Index,
        reserved_value_supplier: OnReservedValueSupplier,
    ) -> Index {
        let frame_length = length + data_frame_header::LENGTH;
        let aligned_length = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);

        let term_length = self.term_buffer.capacity();

        let mut resulting_offset = term_offset + aligned_length;
        self.put_raw_tail_ordered(term_id as i64, resulting_offset);

        if resulting_offset > term_length {
            resulting_offset = Self::handle_end_of_log_condition(&self.term_buffer, term_id, term_offset, header, term_length);
        } else {
            header.write(&self.term_buffer, term_offset, frame_length, term_id);
            self.term_buffer
                .copy_from(term_offset + data_frame_header::LENGTH, &src_buffer, src_offset, length);

            let reserved_value = reserved_value_supplier(self.term_buffer, term_offset, frame_length);
            self.term_buffer
                .put::<i64>(term_offset + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET, reserved_value);

            frame_descriptor::set_frame_length_ordered(&self.term_buffer, term_offset, frame_length);
        }

        resulting_offset
    }

    pub fn append_unfragmented_message_bulk(
        &mut self,
        term_id: i32,
        term_offset: Index,
        header: &HeaderWriter,
        buffers: Vec<AtomicBuffer>,
        length: Index,
        reserved_value_supplier: OnReservedValueSupplier,
    ) -> Index {
        let frame_length: Index = length + data_frame_header::LENGTH;
        let aligned_length: Index = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);

        let term_length = self.term_buffer.capacity();
        let mut resulting_offset = term_offset + aligned_length;

        self.put_raw_tail_ordered(term_id as i64, resulting_offset);

        if resulting_offset > term_length {
            resulting_offset =
                ExclusiveTermAppender::handle_end_of_log_condition(&self.term_buffer, term_id, term_offset, header, term_length);
        } else {
            header.write(&self.term_buffer, term_offset, frame_length, term_id);

            let mut offset = term_offset + data_frame_header::LENGTH;

            for buf in buffers.iter() {
                let ending_offset = offset + length;
                if offset >= ending_offset {
                    break;
                }
                offset += buf.capacity();

                self.term_buffer.copy_from(offset, buf, 0, buf.capacity());
            }

            let reserved_value = reserved_value_supplier(self.term_buffer, term_offset, frame_length);
            self.term_buffer
                .put::<i64>(term_offset + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET, reserved_value);

            frame_descriptor::set_frame_length_ordered(&self.term_buffer, term_offset, frame_length);
        }

        resulting_offset
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append_fragmented_message(
        &mut self,
        term_id: i32,
        term_offset: Index,
        header: &HeaderWriter,
        src_buffer: AtomicBuffer,
        src_offset: Index,
        length: Index,
        max_payload_length: Index,
        reserved_value_supplier: OnReservedValueSupplier,
    ) -> Index {
        let num_max_payloads = length / max_payload_length;
        let remaining_payload = length % max_payload_length;
        let last_frame_length = if remaining_payload > 0 {
            bit_utils::align(
                remaining_payload + data_frame_header::LENGTH,
                frame_descriptor::FRAME_ALIGNMENT,
            )
        } else {
            0
        };

        let required_length = (num_max_payloads * (max_payload_length + data_frame_header::LENGTH)) + last_frame_length;

        let term_length = self.term_buffer.capacity();

        let mut resulting_offset = term_offset + required_length;
        self.put_raw_tail_ordered(term_id as i64, resulting_offset);

        if resulting_offset > term_length {
            resulting_offset = Self::handle_end_of_log_condition(&self.term_buffer, term_id, term_offset, header, term_length);
        } else {
            let mut flags = frame_descriptor::BEGIN_FRAG;
            let mut remaining = length;
            let mut offset = term_offset;

            loop {
                let bytes_to_write = std::cmp::min(remaining, max_payload_length);
                let frame_length = bytes_to_write + data_frame_header::LENGTH;
                let aligned_length = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);

                header.write(&self.term_buffer, offset, frame_length, term_id);
                self.term_buffer.copy_from(
                    offset + data_frame_header::LENGTH,
                    &src_buffer,
                    src_offset + (length - remaining),
                    bytes_to_write,
                );

                if remaining <= max_payload_length {
                    flags |= frame_descriptor::END_FRAG;
                }

                frame_descriptor::set_frame_flags(&self.term_buffer, offset, flags);

                let reserved_value = reserved_value_supplier(self.term_buffer, offset, frame_length);
                self.term_buffer
                    .put::<i64>(offset + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET, reserved_value);

                frame_descriptor::set_frame_length_ordered(&self.term_buffer, offset, frame_length);

                flags = 0;
                offset += aligned_length;
                remaining -= bytes_to_write;

                if remaining <= 0 {
                    break;
                }
            }
        }

        resulting_offset
    }

    fn handle_end_of_log_condition(
        term_buffer: &AtomicBuffer,
        term_id: i32,
        term_offset: Index,
        header: &HeaderWriter,
        term_length: Index,
    ) -> Index {
        if term_offset < term_length {
            let padding_length = term_length - term_offset;
            header.write(term_buffer, term_offset, padding_length, term_id);
            frame_descriptor::set_frame_type(term_buffer, term_offset, data_frame_header::HDR_TYPE_PAD);
            frame_descriptor::set_frame_length_ordered(term_buffer, term_offset, padding_length);
        }

        TERM_APPENDER_FAILED
    }

    fn put_raw_tail_ordered(&mut self, term_id: i64, term_offset: Index) {
        unsafe {
            fence(Ordering::Release);
            *(self.tail_addr as *mut i64) = (term_id * (1_i64 << 32)) | term_offset as i64;
        }
    }
}
