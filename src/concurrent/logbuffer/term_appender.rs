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

use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::concurrent::logbuffer::buffer_claim::BufferClaim;
use crate::concurrent::logbuffer::{data_frame_header, frame_descriptor, header::HeaderWriter, log_buffer_descriptor};
use crate::utils::bit_utils;
use crate::utils::errors::AeronError;
use crate::utils::types::{Index, SZ_I32, SZ_I64};

/**
 * Supplies the reserved value field for a data frame header. The returned value will be set in the header as
 * Little Endian format.
 *
 * This will be called as the last action of encoding a data frame right before the length is set. All other fields
 * in the header plus the body of the frame will have been written at the point of supply.
 *
 * @param term_buffer for the message
 * @param term_offset of the start of the message
 * @param length of the message in bytes
 */
pub type OnReservedValueSupplier = fn(&AtomicBuffer, Index, Index) -> i64;

pub const TERM_APPENDER_FAILED: Index = SZ_I32 - 2;

fn default_reserved_value_supplier(_term_buffer: AtomicBuffer, _term_offset: Index, _length: Index) -> i64 {
    0
}

struct TermAppender<'a> {
    term_buffer: &'a AtomicBuffer,
    tail_buffer: &'a AtomicBuffer,
    tail_offset: Index,
}

impl<'a> TermAppender<'a> {
    // Term buffer - for messages
    // Meta data buffer - for metadata used by Aeron for maintaining messages flow
    // Partition index - is number 0, 1 ... X which are indexes of this particular term appender
    // in the array of other appenders. All appenders share one metadata buffer therefore
    // each appender uses its own space inside metadata buffer known as tail_buffer.
    pub fn new(term_buffer: &'a AtomicBuffer, meta_data_buffer: &'a AtomicBuffer, partition_index: Index) -> Self {
        Self {
            term_buffer,
            tail_buffer: meta_data_buffer,
            tail_offset: *log_buffer_descriptor::TERM_TAIL_COUNTER_OFFSET + (partition_index * SZ_I64 as Index),
        }
    }

    pub fn term_buffer(&self) -> &AtomicBuffer {
        self.term_buffer
    }

    pub fn raw_tail_volatile(&self) -> i64 {
        self.tail_buffer.get_volatile::<i64>(self.tail_offset)
    }

    // This fn publish message (with the size less than MTU) which was previously
    // stored in internal term_buffer via append_..._message().
    // MTU is a maximum transmission unit and its set on a channel level.
    // header - header writer which already points to header part of metadata buffer. Specific header info will be written inside claim()
    // length - length of the message body (payload)
    // buffer_claim - buffer in to which msg will be "written" (actually wrapped) from term_buffer
    // active_term_id - the term to write message to
    pub fn claim(
        &self,
        header: &HeaderWriter,
        length: Index,
        buffer_claim: &mut BufferClaim,
        active_term_id: i32,
    ) -> Result<Index, AeronError> {
        let frame_length: Index = length + data_frame_header::LENGTH;
        let aligned_length: Index = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);
        let raw_tail: i64 = self.get_and_add_raw_tail(aligned_length);
        let term_offset: i64 = raw_tail & 0xFFFFFFFF;
        let term_id: i32 = log_buffer_descriptor::term_id(raw_tail);

        let term_length = self.term_buffer.capacity();

        TermAppender::check_term(active_term_id, term_id)?;

        let mut resulting_offset = term_offset + aligned_length as i64;
        if resulting_offset > term_length as i64 {
            resulting_offset =
                TermAppender::handle_end_of_log_condition(self.term_buffer, term_offset, header, term_length, term_id) as i64;
        } else {
            let frame_offset = term_offset as Index;
            header.write(self.term_buffer, frame_offset, frame_length, term_id);
            buffer_claim.wrap_with_offset(self.term_buffer, frame_offset, frame_length);
        }

        Ok(resulting_offset as Index)
    }

    // This fn copy supplied (in src_buffer) message in to internal term_buffer
    pub fn append_unfragmented_message(
        &self,
        header: &HeaderWriter,
        src_buffer: &AtomicBuffer,
        src_offset: Index,
        length: Index,
        reserved_value_supplier: OnReservedValueSupplier,
        active_term_id: i32,
    ) -> Result<Index, AeronError> {
        let frame_length: Index = length + data_frame_header::LENGTH;
        let aligned_length: Index = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);
        let raw_tail: i64 = self.get_and_add_raw_tail(aligned_length);
        let term_offset: i64 = raw_tail & 0xFFFFFFFF;
        let term_id: i32 = log_buffer_descriptor::term_id(raw_tail);

        let term_length = self.term_buffer.capacity();

        TermAppender::check_term(active_term_id, term_id)?;

        let mut resulting_offset = term_offset + aligned_length as i64;

        if resulting_offset > term_length as i64 {
            resulting_offset =
                TermAppender::handle_end_of_log_condition(self.term_buffer, term_offset, header, term_length, term_id) as i64;
        } else {
            let frame_offset = term_offset as Index;
            header.write(self.term_buffer, frame_offset, frame_length, term_id);
            self.term_buffer
                .copy_from(frame_offset + data_frame_header::LENGTH, src_buffer, src_offset, length);

            let reserved_value: i64 = reserved_value_supplier(self.term_buffer, frame_offset, frame_length);
            self.term_buffer
                .put::<i64>(frame_offset + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET, reserved_value);

            frame_descriptor::set_frame_length_ordered(self.term_buffer, frame_offset, frame_length);
        }

        Ok(resulting_offset as Index)
    }

    /* Looks like this generic fn is not used. Therefore leave it incomplete for now.
     * It will need proper constraint on T to be fully functional.
    pub fn append_unfragmented_message_buf_iter<T>(&self, header: &HeaderWriter,
                                                    buffer_iter: T,
                                                    length: Index,
                                                    reserved_value_supplier: OnReservedValueSupplier,
                                                    active_term_id: i32) -> i32 {
        let frame_length: Index = length + data_frame_header::LENGTH;
        let aligned_length: Index = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);
        let raw_tail: i64 = get_and_add_raw_tail(aligned_length);
        let term_offset: i64 = raw_tail & 0xFFFFFFFF;
        let term_id: i32 = log_buffer_descriptor::term_id(raw_tail);

        let term_length: i32 = self.term_buffer.capacity();

        check_term(active_term_id, term_id);

        let mut resulting_offset = term_offset + aligned_length as i64;

        if resulting_offset > term_length as i64 {
            resulting_offset = handle_end_of_log_condition(self.term_buffer, term_offset, header, term_length, term_id);
        } else {
            let frame_offset: i32 = term_offset as i32;
            header.write(self.term_buffer, frame_offset, frame_length, term_id);

            let mut offset = frame_offset + data_frame_header::LENGTH;

            loop {
                let ending_offset = offset + length;
                if offset >= ending_offset {
                    break;
                }
                offset += buffer_iter.capacity();
                buffer_iter.next();

                self.term_buffer.copy_from(offset, *buffer_iter, 0, buffer_iter.capacity());
            }

            let reserved_value = reserved_value_supplier(self.term_buffer, frame_offset, frame_length);
            self.term_buffer.put::<i64>(frame_offset + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET, reserved_value);

            frame_descriptor::set_frame_length_ordered(self.term_buffer, frame_offset, frame_length);
            }

            resulting_offset as i32
    }
    */

    // This fn copy supplied (in src_buffer) message in to internal term_buffer
    pub fn append_fragmented_message(
        &self,
        header: &HeaderWriter,
        src_buffer: &AtomicBuffer,
        src_offset: Index,
        length: Index,
        max_payload_length: Index,
        reserved_value_supplier: OnReservedValueSupplier,
        active_term_id: i32,
    ) -> Result<Index, AeronError> {
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

        let required_length: Index = (num_max_payloads * (max_payload_length + data_frame_header::LENGTH)) + last_frame_length;
        let raw_tail: i64 = self.get_and_add_raw_tail(required_length);
        let term_offset: i64 = raw_tail & 0xFFFFFFFF;
        let term_id: i32 = log_buffer_descriptor::term_id(raw_tail);

        let term_length = self.term_buffer.capacity();

        TermAppender::check_term(active_term_id, term_id)?;

        let mut resulting_offset = term_offset + required_length as i64;

        if resulting_offset > term_length as i64 {
            resulting_offset =
                TermAppender::handle_end_of_log_condition(self.term_buffer, term_offset, header, term_length, term_id) as i64;
        } else {
            let mut flags: u8 = frame_descriptor::BEGIN_FRAG;
            let mut remaining: Index = length;
            let mut frame_offset = term_offset as Index;

            loop {
                let bytes_to_write: Index = std::cmp::min(remaining, max_payload_length);
                let frame_length: Index = bytes_to_write + data_frame_header::LENGTH;
                let aligned_length: Index = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);

                header.write(self.term_buffer, frame_offset, frame_length, term_id);

                self.term_buffer.copy_from(
                    frame_offset + data_frame_header::LENGTH,
                    src_buffer,
                    src_offset + (length - remaining),
                    bytes_to_write,
                );

                if remaining <= max_payload_length {
                    flags |= frame_descriptor::END_FRAG;
                }

                frame_descriptor::set_frame_flags(self.term_buffer, frame_offset, flags);

                let reserved_value: i64 = reserved_value_supplier(self.term_buffer, frame_offset, frame_length);
                self.term_buffer
                    .put::<i64>(frame_offset + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET, reserved_value);

                frame_descriptor::set_frame_length_ordered(self.term_buffer, frame_offset, frame_length);

                flags = 0;
                frame_offset += aligned_length;
                remaining -= bytes_to_write;

                if remaining <= 0 {
                    break;
                }
            }
        }

        Ok(resulting_offset as Index)
    }

    fn check_term(expected_term_id: i32, term_id: i32) -> Result<(), AeronError> {
        if term_id != expected_term_id {
            return Err(AeronError::IllegalStateException(format!(
                "action possibly delayed: expected_term_id={} term_id={}",
                expected_term_id, term_id
            )));
        }
        Ok(())
    }

    fn handle_end_of_log_condition(
        term_buffer: &AtomicBuffer,
        term_offset: i64,
        header: &HeaderWriter,
        term_length: Index,
        term_id: i32,
    ) -> Index {
        if term_offset < term_length as i64 {
            let offset = term_offset as Index;
            let padding_length = term_length - offset;
            header.write(term_buffer, offset, padding_length, term_id);
            frame_descriptor::set_frame_type(term_buffer, offset, data_frame_header::HDR_TYPE_PAD);
            frame_descriptor::set_frame_length_ordered(term_buffer, offset, padding_length);
        }

        TERM_APPENDER_FAILED
    }

    fn get_and_add_raw_tail(&self, aligned_length: Index) -> i64 {
        self.tail_buffer.get_and_add_i64(self.tail_offset, aligned_length as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::concurrent::atomic_buffer::AlignedBuffer;
    use lazy_static::lazy_static;

    const TERM_BUFFER_CAPACITY: Index = log_buffer_descriptor::TERM_MIN_LENGTH;
    const META_DATA_BUFFER_CAPACITY: Index = log_buffer_descriptor::LOG_META_DATA_LENGTH;
    const MAX_FRAME_LENGTH: Index = 1024;

    const MAX_PAYLOAD_LENGTH: Index = MAX_FRAME_LENGTH - data_frame_header::LENGTH;
    const SRC_BUFFER_CAPACITY: Index = 2 * 1024;
    const TERM_ID: i32 = 101;
    const RESERVED_VALUE: i64 = 777;
    const PARTITION_INDEX: Index = 1;
    lazy_static! {
        pub static ref TERM_TAIL_OFFSET: Index = *log_buffer_descriptor::TERM_TAIL_COUNTER_OFFSET + PARTITION_INDEX * SZ_I64;
    }

    #[macro_export]
    macro_rules! gen_term_appender {
        ($ta:ident, $tb_size:expr, $mb_size:expr) => {
            let term_buffer_mem = AlignedBuffer::with_capacity($tb_size);
            let term_buffer = AtomicBuffer::from_aligned(&term_buffer_mem);

            // One meta buffer for all term buffers
            let meta_buffer_mem = AlignedBuffer::with_capacity($mb_size);
            let meta_buffer = AtomicBuffer::from_aligned(&meta_buffer_mem);

            let $ta = TermAppender::new(&term_buffer, &meta_buffer, PARTITION_INDEX);
        };
    }

    fn pack_raw_tail(term_id: i32, term_offset: i32) -> i64 {
        (term_id << 32 | term_offset) as i64
    }

    fn reserved_value_supplier(_buf: &AtomicBuffer, _unused: Index, _unused1: Index) -> i64 {
        RESERVED_VALUE
    }

    #[test]
    fn test_term_appender_buf_capacity() {
        gen_term_appender!(ta, TERM_BUFFER_CAPACITY, META_DATA_BUFFER_CAPACITY);

        assert_eq!(ta.term_buffer().capacity(), TERM_BUFFER_CAPACITY);
    }

    #[test]
    fn test_term_appender_claim() {
        let term_buffer_mem0 = AlignedBuffer::with_capacity(1000);
        let term_buffer0 = AtomicBuffer::from_aligned(&term_buffer_mem0);

        // One meat buffer for all term buffers
        let meta_buffer_mem = AlignedBuffer::with_capacity(1000);
        let meta_buffer = AtomicBuffer::from_aligned(&meta_buffer_mem);

        let ta0 = TermAppender::new(&term_buffer0, &meta_buffer, 0);

        // Uses part of meta data buffer to store frame header.
        // Returns atomic buffer which wraps this header.
        let header_buffer = log_buffer_descriptor::default_frame_header(&meta_buffer);

        // Construct header writer which charged with emtpy buffer to write header to.
        let hw = HeaderWriter::new(&header_buffer);

        let mut buffer_claim = BufferClaim::default();

        let msg: [u8; 10] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 0];

        ta0.claim(&hw, msg.len() as Index, &mut buffer_claim, 0);
    }
}
