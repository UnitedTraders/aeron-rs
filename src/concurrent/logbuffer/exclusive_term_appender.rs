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
use crate::concurrent::logbuffer::header::HeaderWriter;
use crate::concurrent::logbuffer::term_appender::{OnReservedValueSupplier, TERM_APPENDER_FAILED};
use crate::concurrent::logbuffer::{data_frame_header, frame_descriptor, log_buffer_descriptor};
use crate::utils::bit_utils;
use crate::utils::errors::AeronError;
use crate::utils::types::{Index, SZ_I64};
use std::sync::atomic::{fence, Ordering};

struct ExclusiveTermAppender<'a> {
    term_buffer: &'a AtomicBuffer,
    tail_addr: *const i64,
}

impl<'a> ExclusiveTermAppender<'a> {
    pub fn new(term_buffer: &'a AtomicBuffer, meta_data_buffer: &'a AtomicBuffer, partition_index: usize) -> Self {
        // This check implemented as assert. Looks like out of bounds here can be reached only
        // due to a bug elsewhere. Therefore don't need more sophisticated error handling.
        meta_data_buffer.bounds_check(
            *log_buffer_descriptor::TERM_TAIL_COUNTER_OFFSET + (partition_index * SZ_I64) as i32,
            SZ_I64 as isize,
        );

        Self {
            term_buffer,
            tail_addr: (meta_data_buffer.buffer() as (usize)
                + *log_buffer_descriptor::TERM_TAIL_COUNTER_OFFSET as usize
                + (partition_index * SZ_I64)) as (*const i64),
        }
    }

    pub fn term_buffer(&self) -> &AtomicBuffer {
        self.term_buffer
    }

    pub fn raw_tail(&self) -> i64 {
        unsafe { *self.tail_addr } // brrrrr....
    }

    pub fn claim(
        &mut self,
        term_id: i32,
        term_offset: i32,
        header: &HeaderWriter,
        length: Index,
        buffer_claim: &mut BufferClaim,
    ) -> i32 {
        let frame_length = length + data_frame_header::LENGTH;
        let aligned_length = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);

        let term_length = self.term_buffer.capacity();
        let mut resulting_offset = term_offset + aligned_length;
        self.put_raw_tail_ordered(term_id as i64, resulting_offset);

        if resulting_offset > term_length {
            resulting_offset = Self::handle_end_of_log_condition(self.term_buffer, term_id, term_offset, header, term_length);
        } else {
            header.write(self.term_buffer, term_offset, frame_length, term_id);
            buffer_claim.wrap_with_offset(self.term_buffer, term_offset, frame_length);
        }

        resulting_offset
    }

    pub fn append_unfragmented_message(
        &mut self,
        term_id: i32,
        term_offset: i32,
        header: &HeaderWriter,
        src_buffer: &AtomicBuffer,
        src_offset: Index,
        length: Index,
        reserved_value_supplier: OnReservedValueSupplier,
    ) -> i32 {
        let frame_length = length + data_frame_header::LENGTH;
        let aligned_length = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);

        let term_length = self.term_buffer.capacity();

        let mut resulting_offset = term_offset + aligned_length;
        self.put_raw_tail_ordered(term_id as i64, resulting_offset);

        if resulting_offset > term_length {
            resulting_offset = Self::handle_end_of_log_condition(self.term_buffer, term_id, term_offset, header, term_length);
        } else {
            header.write(self.term_buffer, term_offset, frame_length, term_id);
            self.term_buffer
                .copy_from(term_offset + data_frame_header::LENGTH, src_buffer, src_offset, length);

            let reservedValue = reserved_value_supplier(self.term_buffer, term_offset, frame_length);
            self.term_buffer
                .put::<i64>(term_offset + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET, reservedValue);

            frame_descriptor::set_frame_length_ordered(self.term_buffer, term_offset, frame_length);
        }

        resulting_offset
    }
    /* TODO: Not sure that its used! Port it if actually used or remove from here!
    template <class BufferIterator> inline std::int32_t append_unfragmented_message(
    std::int32_t term_id,
    std::int32_t term_offset,
    const &HeaderWriter header,
    BufferIterator bufferIt,
    util::index_t length,
    const on_reserved_value_supplier_t& reserved_value_supplier)
    {
    const util::index_t frame_length = length + data_frame_header::LENGTH;
    const util::index_t aligned_length = util::BitUtil::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);

    const std::int32_t term_length = self.term_buffer.capacity();

    std::int32_t resulting_offset = term_offset + aligned_length;
    put_raw_tail_ordered(term_id, resulting_offset);

    if (resulting_offset > term_length)
    {
    resulting_offset = handleEndOfLogCondition(self.term_buffer, term_id, term_offset, header, term_length);
    }
    else
    {
    header.write(self.term_buffer, term_offset, frame_length, term_id);

    std::int32_t offset = term_offset + data_frame_header::LENGTH;
    for (
    std::int32_t endingOffset = offset + length;
    offset < endingOffset;
    offset += bufferIt->capacity(), ++bufferIt)
    {
    self.term_buffer.putBytes(offset, *bufferIt, 0, bufferIt->capacity());
    }

    const std::int64_t reservedValue = reserved_value_supplier(self.term_buffer, term_offset, frame_length);
    self.term_buffer.putInt64(term_offset + data_frame_header::RESERVED_VALUE_FIELD_OFFSET, reservedValue);

    frame_descriptor::frame_lengthOrdered(self.term_buffer, term_offset, frame_length);
    }

    return resulting_offset;
    }
    */

    pub fn append_fragmented_message(
        &mut self,
        term_id: i32,
        term_offset: i32,
        header: &HeaderWriter,
        src_buffer: &AtomicBuffer,
        src_offset: Index,
        length: Index,
        max_payload_length: Index,
        reserved_value_supplier: OnReservedValueSupplier,
    ) -> i32 {
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
            resulting_offset = Self::handle_end_of_log_condition(self.term_buffer, term_id, term_offset, header, term_length);
        } else {
            let mut flags = frame_descriptor::BEGIN_FRAG;
            let mut remaining = length;
            let mut offset = term_offset;

            loop {
                let bytes_to_write = std::cmp::min(remaining, max_payload_length);
                let frame_length = bytes_to_write + data_frame_header::LENGTH;
                let aligned_length = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);

                header.write(self.term_buffer, offset, frame_length, term_id);
                self.term_buffer.copy_from(
                    offset + data_frame_header::LENGTH,
                    src_buffer,
                    src_offset + (length - remaining),
                    bytes_to_write,
                );

                if remaining <= max_payload_length {
                    flags |= frame_descriptor::END_FRAG;
                }

                frame_descriptor::set_frame_flags(self.term_buffer, offset, flags);

                let reserved_value = reserved_value_supplier(self.term_buffer, offset, frame_length);
                self.term_buffer
                    .put::<i64>(offset + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET, reserved_value);

                frame_descriptor::set_frame_length_ordered(self.term_buffer, offset, frame_length);

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

    /* TODO: Not sure that its used! Port it if actually used or remove from here!
    template <class BufferIterator> std::int32_t appendFragmentedMessage(
    std::int32_t term_id,
    std::int32_t term_offset,
    const &HeaderWriter header,
    BufferIterator bufferIt,
    util::index_t length,
    util::index_t max_payload_length,
    const on_reserved_value_supplier_t& reserved_value_supplier)
    {
    const int num_max_payloads = length / max_payload_length;
    const util::index_t remaining_payload = length % max_payload_length;
    const util::index_t last_frame_length = (remaining_payload > 0) ?
    util::BitUtil::align(remaining_payload + data_frame_header::LENGTH, frame_descriptor::FRAME_ALIGNMENT) : 0;
    const util::index_t required_length =
    (num_max_payloads * (max_payload_length + data_frame_header::LENGTH)) + last_frame_length;

    const std::int32_t term_length = self.term_buffer.capacity();

    std::int32_t resulting_offset = term_offset + required_length;
    put_raw_tail_ordered(term_id, resulting_offset);

    if (resulting_offset > term_length)
    {
    resulting_offset = handleEndOfLogCondition(self.term_buffer, term_id, term_offset, header, term_length);
    }
    else
    {
    std::uint8_t flags = frame_descriptor::BEGIN_FRAG;
    util::index_t remaining = length;
    std::int32_t offset = static_cast<std::int32_t>(term_offset);
    util::index_t currentBufferOffset = 0;

    do
    {
    const util::index_t bytes_to_write = std::min(remaining, max_payload_length);
    const util::index_t frame_length = bytes_to_write + data_frame_header::LENGTH;
    const util::index_t aligned_length = util::BitUtil::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);

    header.write(self.term_buffer, offset, frame_length, term_id);

    util::index_t bytesWritten = 0;
    util::index_t payloadOffset = offset + data_frame_header::LENGTH;
    do
    {
    const util::index_t currentBufferRemaining = bufferIt->capacity() - currentBufferOffset;
    const util::index_t numBytes = std::min(bytes_to_write - bytesWritten, currentBufferRemaining);

    self.term_buffer.putBytes(payloadOffset, *bufferIt, currentBufferOffset, numBytes);

    bytesWritten += numBytes;
    payloadOffset += numBytes;
    currentBufferOffset += numBytes;

    if (currentBufferRemaining <= numBytes)
    {
    ++bufferIt;
    currentBufferOffset = 0;
    }
    }
    while (bytesWritten < bytes_to_write);

    if (remaining <= max_payload_length)
    {
    flags |= frame_descriptor::END_FRAG;
    }

    frame_descriptor::frameFlags(self.term_buffer, offset, flags);

    const std::int64_t reservedValue = reserved_value_supplier(self.term_buffer, offset, frame_length);
    self.term_buffer.putInt64(offset + data_frame_header::RESERVED_VALUE_FIELD_OFFSET, reservedValue);

    frame_descriptor::frame_lengthOrdered(self.term_buffer, offset, frame_length);

    flags = 0;
    offset += aligned_length;
    remaining -= bytes_to_write;
    }
    while (remaining > 0);
    }

    return resulting_offset;
    }
    */

    fn handle_end_of_log_condition(
        term_buffer: &AtomicBuffer,
        term_id: i32,
        term_offset: i32,
        header: &HeaderWriter,
        term_length: Index,
    ) -> i32 {
        if term_offset < term_length {
            let padding_length = term_length - term_offset;
            header.write(term_buffer, term_offset, padding_length, term_id);
            frame_descriptor::set_frame_type(term_buffer, term_offset, data_frame_header::HDR_TYPE_PAD);
            frame_descriptor::set_frame_length_ordered(term_buffer, term_offset, padding_length);
        }

        TERM_APPENDER_FAILED
    }

    fn put_raw_tail_ordered(&mut self, term_id: i64, term_offset: i32) {
        unsafe {
            fence(Ordering::Release);
            *(self.tail_addr as *mut i64) = term_id * (1_i64 << 32) | term_offset as i64;
        }
    }
}
