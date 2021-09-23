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

use crate::ttrace;
use crate::utils::errors::IllegalStateError;
use crate::{
    concurrent::{
        atomic_buffer::AtomicBuffer,
        logbuffer::{
            buffer_claim::BufferClaim, data_frame_header, frame_descriptor, header::HeaderWriter, log_buffer_descriptor,
        },
    },
    utils::{
        bit_utils,
        errors::AeronError,
        types::{Index, I64_SIZE},
    },
};

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
pub type OnReservedValueSupplier = fn(AtomicBuffer, Index, Index) -> i64;

pub const TERM_APPENDER_FAILED: Index = -2;

pub fn default_reserved_value_supplier(_term_buffer: AtomicBuffer, _term_offset: Index, _length: Index) -> i64 {
    0
}

pub struct TermAppender {
    term_buffer: AtomicBuffer,
    tail_buffer: AtomicBuffer,
    tail_offset: Index,
}

impl TermAppender {
    /// Term buffer - for messages
    /// Meta data buffer - for metadata used by Aeron for maintaining messages flow
    /// Partition index - is number 0, 1 ... X which are indexes of this particular term appender
    /// in the array of other appenders. All appenders share one metadata buffer therefore
    /// each appender uses its own space inside metadata buffer known as tail_buffer.
    pub fn new(term_buffer: AtomicBuffer, meta_data_buffer: AtomicBuffer, partition_index: Index) -> Self {
        Self {
            term_buffer,
            tail_buffer: meta_data_buffer,
            tail_offset: *log_buffer_descriptor::TERM_TAIL_COUNTER_OFFSET + (partition_index * I64_SIZE as Index),
        }
    }

    pub fn term_buffer(&self) -> AtomicBuffer {
        self.term_buffer
    }

    pub fn raw_tail_volatile(&self) -> i64 {
        self.tail_buffer.get_volatile::<i64>(self.tail_offset)
    }

    /// This fn publish message (with the size less than MTU) which was previously
    /// stored in internal term_buffer via append_..._message().
    /// MTU is a maximum transmission unit and its set on a channel level.
    /// header - header writer which already points to header part of metadata buffer. Specific header info will be written inside claim()
    /// length - length of the message body (payload)
    /// buffer_claim - buffer in to which msg will be "written" (actually wrapped) from term_buffer
    /// active_term_id - the term to write message to
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
        let term_offset: i64 = raw_tail & 0xFFFF_FFFF;
        let term_id: i32 = log_buffer_descriptor::term_id(raw_tail);

        let term_length = self.term_buffer.capacity();

        TermAppender::check_term(active_term_id, term_id)?;

        let mut resulting_offset = term_offset + aligned_length as i64;
        if resulting_offset > term_length as i64 {
            resulting_offset =
                TermAppender::handle_end_of_log_condition(&self.term_buffer, term_offset, header, term_length, term_id) as i64;
        } else {
            let frame_offset = term_offset as Index;
            header.write(&self.term_buffer, frame_offset, frame_length, term_id);
            buffer_claim.wrap_with_offset(&self.term_buffer, frame_offset, frame_length);
        }

        Ok(resulting_offset as Index)
    }

    /// This fn copy supplied (in msg_body_buffer) message in to internal term_buffer
    pub fn append_unfragmented_message(
        &self,
        header: &HeaderWriter,
        msg_body_buffer: &AtomicBuffer,
        msg_body_offset: Index,
        length: Index,
        reserved_value_supplier: OnReservedValueSupplier,
        active_term_id: i32,
    ) -> Result<Index, AeronError> {
        let frame_length: Index = length + data_frame_header::LENGTH;
        let aligned_length: Index = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);
        let raw_tail: i64 = self.get_and_add_raw_tail(aligned_length);
        let term_offset: i64 = raw_tail & 0xFFFF_FFFF;
        let term_id: i32 = log_buffer_descriptor::term_id(raw_tail);

        let term_length = self.term_buffer.capacity();

        TermAppender::check_term(active_term_id, term_id)?;

        let mut resulting_offset = term_offset + aligned_length as i64;

        if resulting_offset > term_length as i64 {
            ttrace!("append_unfragmented_message: end of log condition detected");
            resulting_offset =
                TermAppender::handle_end_of_log_condition(&self.term_buffer, term_offset, header, term_length, term_id) as i64;
        } else {
            let frame_offset = term_offset as Index;
            header.write(&self.term_buffer, frame_offset, frame_length, term_id);
            self.term_buffer.copy_from(
                frame_offset + data_frame_header::LENGTH,
                msg_body_buffer,
                msg_body_offset,
                length,
            );

            let reserved_value: i64 = reserved_value_supplier(self.term_buffer, frame_offset, frame_length);
            self.term_buffer
                .put::<i64>(frame_offset + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET, reserved_value);

            frame_descriptor::set_frame_length_ordered(&self.term_buffer, frame_offset, frame_length);
        }

        Ok(resulting_offset as Index)
    }

    /// Appends unfrag message which is inside several AtomicBuffers passed as Vec
    pub fn append_unfragmented_message_bulk(
        &self,
        header: &HeaderWriter,
        buffers: Vec<AtomicBuffer>,
        length: Index,
        reserved_value_supplier: OnReservedValueSupplier,
        active_term_id: i32,
    ) -> Result<Index, AeronError> {
        let frame_length: Index = length + data_frame_header::LENGTH;
        let aligned_length: Index = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);
        let raw_tail: i64 = self.get_and_add_raw_tail(aligned_length);
        let term_offset: i64 = raw_tail & 0xFFFF_FFFF;
        let term_id: i32 = log_buffer_descriptor::term_id(raw_tail);

        let term_length = self.term_buffer.capacity();

        TermAppender::check_term(active_term_id, term_id)?;

        let mut resulting_offset = term_offset + aligned_length as i64;

        if resulting_offset > term_length as i64 {
            resulting_offset =
                TermAppender::handle_end_of_log_condition(&self.term_buffer, term_offset, header, term_length, term_id) as i64;
        } else {
            let frame_offset = term_offset as Index;
            header.write(&self.term_buffer, frame_offset, frame_length, term_id);

            let mut offset = frame_offset + data_frame_header::LENGTH;

            for buf in buffers.iter() {
                let ending_offset = offset + length;
                if offset >= ending_offset {
                    break;
                }
                offset += buf.capacity();

                self.term_buffer.copy_from(offset, buf, 0, buf.capacity());
            }

            let reserved_value = reserved_value_supplier(self.term_buffer, frame_offset, frame_length);
            self.term_buffer
                .put::<i64>(frame_offset + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET, reserved_value);

            frame_descriptor::set_frame_length_ordered(&self.term_buffer, frame_offset, frame_length);
        }

        Ok(resulting_offset as Index)
    }

    /// This fn copy supplied (in msg_body_buffer) message in to internal term_buffer
    #[allow(clippy::too_many_arguments)]
    pub fn append_fragmented_message(
        &self,
        header: &HeaderWriter,
        msg_body_buffer: &AtomicBuffer,
        msg_body_offset: Index,
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
        let term_offset: i64 = raw_tail & 0xFFFF_FFFF;
        let term_id: i32 = log_buffer_descriptor::term_id(raw_tail);

        let term_length = self.term_buffer.capacity();

        TermAppender::check_term(active_term_id, term_id)?;

        let mut resulting_offset = term_offset + required_length as i64;

        if resulting_offset > term_length as i64 {
            ttrace!("append_fragmented_message: end of log condition detected");
            resulting_offset =
                TermAppender::handle_end_of_log_condition(&self.term_buffer, term_offset, header, term_length, term_id) as i64;
        } else {
            let mut flags: u8 = frame_descriptor::BEGIN_FRAG;
            let mut remaining: Index = length;
            let mut frame_offset = term_offset as Index;

            loop {
                let bytes_to_write: Index = std::cmp::min(remaining, max_payload_length);
                let frame_length: Index = bytes_to_write + data_frame_header::LENGTH;
                let aligned_length: Index = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);

                header.write(&self.term_buffer, frame_offset, frame_length, term_id);

                self.term_buffer.copy_from(
                    frame_offset + data_frame_header::LENGTH,
                    msg_body_buffer,
                    msg_body_offset + (length - remaining),
                    bytes_to_write,
                );

                if remaining <= max_payload_length {
                    flags |= frame_descriptor::END_FRAG;
                }
                ttrace!("append_fragmented_message: writing fragment with flags {:x}", flags);

                frame_descriptor::set_frame_flags(&self.term_buffer, frame_offset, flags);

                let reserved_value: i64 = reserved_value_supplier(self.term_buffer, frame_offset, frame_length);
                self.term_buffer
                    .put::<i64>(frame_offset + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET, reserved_value);

                frame_descriptor::set_frame_length_ordered(&self.term_buffer, frame_offset, frame_length);

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

    pub fn append_fragmented_message_bulk(
        &mut self,
        header: &HeaderWriter,
        buffers: Vec<AtomicBuffer>,
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

        let required_length = (num_max_payloads * (max_payload_length + data_frame_header::LENGTH)) + last_frame_length;
        let raw_tail = self.get_and_add_raw_tail(required_length);
        let term_offset = raw_tail & 0xFFFF_FFFF;
        let term_id = log_buffer_descriptor::term_id(raw_tail);

        let term_length = self.term_buffer.capacity();

        TermAppender::check_term(active_term_id, term_id)?;

        let mut resulting_offset = term_offset + required_length as i64;
        if resulting_offset > term_length as i64 {
            resulting_offset =
                TermAppender::handle_end_of_log_condition(&self.term_buffer, term_offset, header, term_length, term_id) as i64;
        } else {
            let mut flags = frame_descriptor::BEGIN_FRAG;
            let mut remaining = length;
            let mut frame_offset = term_offset as i32;
            let mut current_buffer_offset = 0;

            loop {
                let bytes_to_write = std::cmp::min(remaining, max_payload_length);
                let frame_length = bytes_to_write + data_frame_header::LENGTH;
                let aligned_length: Index = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);

                header.write(&self.term_buffer, frame_offset, frame_length, term_id);

                let mut bytes_written = 0;
                let mut payload_offset = frame_offset + data_frame_header::LENGTH;

                let mut buffers_iter = buffers.iter();
                let mut curr_buffer = buffers_iter.next().expect("At least one buffer must be supplied");

                loop {
                    let current_buffer_remaining = curr_buffer.capacity() - current_buffer_offset;
                    let num_bytes = std::cmp::min(bytes_to_write - bytes_written, current_buffer_remaining);

                    self.term_buffer
                        .copy_from(payload_offset, curr_buffer, current_buffer_offset, num_bytes);

                    bytes_written += num_bytes;
                    payload_offset += num_bytes;
                    current_buffer_offset += num_bytes;

                    if current_buffer_remaining <= num_bytes {
                        if let Some(next_buf) = buffers_iter.next() {
                            curr_buffer = next_buf;
                            current_buffer_offset = 0;
                        } else {
                            break; // all buffers appended
                        }
                    }

                    if bytes_written >= bytes_to_write {
                        break;
                    }
                }

                if remaining <= max_payload_length {
                    flags |= frame_descriptor::END_FRAG;
                }

                frame_descriptor::set_frame_flags(&self.term_buffer, frame_offset, flags);

                let reserved_value = reserved_value_supplier(self.term_buffer, frame_offset, frame_length);
                self.term_buffer
                    .put::<i64>(frame_offset + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET, reserved_value);

                frame_descriptor::set_frame_length_ordered(&self.term_buffer, frame_offset, frame_length);

                flags = 0;
                frame_offset += aligned_length;
                remaining -= bytes_to_write;

                if remaining <= 0 {
                    break;
                }
            }
        }

        Ok(resulting_offset as i32)
    }

    fn check_term(expected_term_id: i32, term_id: i32) -> Result<(), AeronError> {
        if term_id != expected_term_id {
            return Err(IllegalStateError::ActionPossiblyDelayed {
                expected_term_id,
                term_id,
            }
            .into());
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
    use lazy_static::lazy_static;

    use super::*;
    use crate::concurrent::atomic_buffer::AlignedBuffer;

    const TERM_BUFFER_CAPACITY: Index = log_buffer_descriptor::TERM_MIN_LENGTH;
    const META_DATA_BUFFER_CAPACITY: Index = log_buffer_descriptor::LOG_META_DATA_LENGTH;
    const MAX_FRAME_LENGTH: Index = 1024;

    const MAX_PAYLOAD_LENGTH: Index = MAX_FRAME_LENGTH - data_frame_header::LENGTH;
    const SRC_BUFFER_CAPACITY: Index = 2 * 1024;
    const TERM_ID: i32 = 101;
    const RESERVED_VALUE: i64 = 777;
    const PARTITION_INDEX: Index = 1;
    lazy_static! {
        pub static ref TERM_TAIL_OFFSET: Index = *log_buffer_descriptor::TERM_TAIL_COUNTER_OFFSET + PARTITION_INDEX * I64_SIZE;
    }

    // The macro creates two sets of buffer to support a-la Mock logic.
    // Buffers listed in macro arguments should be filled by these tests while
    // not visible from inside buffers will be used for TermAppender and HeaderWriter
    macro_rules! gen_test_data {
        ($metadata_buffer:ident, $term_buffer:ident, $hdr:ident, $msg_body:ident, $term_appender:ident, $header_writer:ident, $hidden_metadata_buffer:ident) => {
            let m_buff = AlignedBuffer::with_capacity(META_DATA_BUFFER_CAPACITY);
            let $metadata_buffer = AtomicBuffer::from_aligned(&m_buff);
            let t_buff = AlignedBuffer::with_capacity(TERM_BUFFER_CAPACITY);
            let $term_buffer = AtomicBuffer::from_aligned(&t_buff);
            let h_buff = AlignedBuffer::with_capacity(data_frame_header::LENGTH);
            let $hdr = AtomicBuffer::from_aligned(&h_buff);
            let m_buff = AlignedBuffer::with_capacity(SRC_BUFFER_CAPACITY);
            let $msg_body = AtomicBuffer::from_aligned(&m_buff);

            let hm_buff = AlignedBuffer::with_capacity(META_DATA_BUFFER_CAPACITY);
            let $hidden_metadata_buffer = AtomicBuffer::from_aligned(&hm_buff);
            let ht_buff = AlignedBuffer::with_capacity(TERM_BUFFER_CAPACITY);
            let hidden_term_buffer = AtomicBuffer::from_aligned(&ht_buff);

            let $term_appender = TermAppender::new(hidden_term_buffer, $hidden_metadata_buffer, PARTITION_INDEX);
            let $header_writer = HeaderWriter::new($hdr);

            $metadata_buffer.set_memory(0, $metadata_buffer.capacity(), 0);
            $term_buffer.set_memory(0, $term_buffer.capacity(), 0);
            $hdr.set_memory(0, $hdr.capacity(), 0);
            $msg_body.set_memory(0, $msg_body.capacity(), 1);

            $hidden_metadata_buffer.set_memory(0, $hidden_metadata_buffer.capacity(), 0);
            hidden_term_buffer.set_memory(0, hidden_term_buffer.capacity(), 0);
        };
    }

    fn pack_raw_tail(term_id: i32, term_offset: Index) -> i64 {
        ((term_id as i64) << 32) | term_offset as i64
    }

    fn reserved_value_supplier(_buf: AtomicBuffer, _unused: Index, _unused1: Index) -> i64 {
        RESERVED_VALUE
    }

    #[test]
    #[allow(unused_variables)]
    fn test_term_appender_buf_capacity() {
        gen_test_data!(
            metadata_buffer,
            term_buffer,
            hdr,
            msg_body,
            term_appender,
            header_writer,
            hidden_metadata_buffer
        );

        assert_eq!(term_appender.term_buffer().capacity(), TERM_BUFFER_CAPACITY);
    }

    // All these test are ported copies of original C++ Mock tests. The philosophy of these
    // tests is to make two independent computations: one by tested functions and other with
    // simplified steps in the test code itself. Then compare results.
    #[test]
    #[allow(unused_variables)]
    fn test_term_appender_append_frame_to_empty_log() {
        let msg_length: Index = 20;
        let frame_length: Index = data_frame_header::LENGTH + msg_length;
        let aligned_frame_length = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);
        let tail: Index = 0;

        gen_test_data!(
            metadata_buffer,
            term_buffer,
            hdr,
            msg_body,
            term_appender,
            header_writer,
            hidden_metadata_buffer
        );

        // Write TERM_ID in to metadata buffer used by TermAppender
        let packed_tail = pack_raw_tail(TERM_ID, tail);
        let _prev_tail = hidden_metadata_buffer.get_and_add_i64(*TERM_TAIL_OFFSET, packed_tail);

        // Mark this message as pending
        term_buffer.put_ordered::<i32>(frame_descriptor::length_offset(tail), -frame_length);
        // Copy the message from msg_body in to term buffer
        term_buffer.copy_from(data_frame_header::LENGTH, &msg_body, 0, msg_length);
        // Write reserved value i.e. reserve needed amount of term buffer
        term_buffer.put::<i64>(tail + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET, RESERVED_VALUE);
        // Mark message as ready for transmission
        term_buffer.put_ordered::<i32>(frame_descriptor::length_offset(tail), frame_length);

        let resulting_offset =
            term_appender.append_unfragmented_message(&header_writer, &msg_body, 0, msg_length, reserved_value_supplier, TERM_ID);

        assert!(resulting_offset.is_ok());

        assert_eq!(resulting_offset.unwrap(), aligned_frame_length);
    }

    #[test]
    #[allow(unused_variables)]
    fn test_term_appender_append_frame_twice() {
        let msg_length: Index = 20;
        let frame_length: Index = data_frame_header::LENGTH + msg_length;
        let aligned_frame_length = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);
        let mut tail: Index = 0;

        gen_test_data!(
            metadata_buffer,
            term_buffer,
            hdr,
            msg_body,
            term_appender,
            header_writer,
            hidden_metadata_buffer
        );

        // Write TERM_ID in to metadata buffer used by TermAppender
        let packed_tail = pack_raw_tail(TERM_ID, tail);
        let _prev_tail = hidden_metadata_buffer.get_and_add_i64(*TERM_TAIL_OFFSET, packed_tail);

        term_buffer.put_ordered::<i32>(frame_descriptor::length_offset(tail), -frame_length);
        term_buffer.copy_from(tail + data_frame_header::LENGTH, &msg_body, 0, msg_length);
        term_buffer.put::<i64>(tail + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET, RESERVED_VALUE);
        term_buffer.put_ordered::<i32>(frame_descriptor::length_offset(tail), frame_length);

        let resulting_offset0 =
            term_appender.append_unfragmented_message(&header_writer, &msg_body, 0, msg_length, reserved_value_supplier, TERM_ID);
        assert!(resulting_offset0.is_ok());
        assert_eq!(resulting_offset0.unwrap(), aligned_frame_length);

        tail = aligned_frame_length;

        term_buffer.put_ordered::<i32>(frame_descriptor::length_offset(tail), -frame_length);
        term_buffer.copy_from(tail + data_frame_header::LENGTH, &msg_body, 0, msg_length);
        term_buffer.put::<i64>(tail + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET, RESERVED_VALUE);
        term_buffer.put_ordered::<i32>(frame_descriptor::length_offset(tail), frame_length);

        let resulting_offset1 =
            term_appender.append_unfragmented_message(&header_writer, &msg_body, 0, msg_length, reserved_value_supplier, TERM_ID);
        assert!(resulting_offset1.is_ok());
        assert_eq!(resulting_offset1.unwrap(), aligned_frame_length * 2);
    }

    // Should pad log when appending with insufficient remaining capacity
    #[test]
    #[allow(unused_variables)]
    fn test_term_appender_pad_log() {
        let msg_length: Index = 120;
        let required_frame_size = bit_utils::align(msg_length + data_frame_header::LENGTH, frame_descriptor::FRAME_ALIGNMENT);
        let tail_value = TERM_BUFFER_CAPACITY - bit_utils::align(msg_length, frame_descriptor::FRAME_ALIGNMENT);
        let frame_length = TERM_BUFFER_CAPACITY - tail_value;

        gen_test_data!(
            metadata_buffer,
            term_buffer,
            hdr,
            msg_body,
            term_appender,
            header_writer,
            hidden_metadata_buffer
        );

        // Write TERM_ID in to metadata buffer used by TermAppender
        let packed_tail = pack_raw_tail(TERM_ID, tail_value);
        let _prev_tail = hidden_metadata_buffer.get_and_add_i64(*TERM_TAIL_OFFSET, packed_tail);

        term_buffer.put_ordered::<i32>(frame_descriptor::length_offset(tail_value), -frame_length);
        term_buffer.put::<u16>(frame_descriptor::type_offset(tail_value), data_frame_header::HDR_TYPE_PAD);
        term_buffer.put_ordered::<i32>(frame_descriptor::length_offset(tail_value), frame_length);

        let resulting_offset =
            term_appender.append_unfragmented_message(&header_writer, &msg_body, 0, msg_length, reserved_value_supplier, TERM_ID);
        assert!(resulting_offset.is_ok());
        assert_eq!(resulting_offset.unwrap(), TERM_APPENDER_FAILED);
    }

    // Should fragment message over two frames
    #[test]
    #[allow(unused_variables)]
    fn test_term_appender_fragment_message() {
        let msg_length: Index = MAX_PAYLOAD_LENGTH + 1;
        let frame_length = data_frame_header::LENGTH + 1;
        let required_capacity = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT) + MAX_FRAME_LENGTH;
        let mut tail: Index = 0;

        gen_test_data!(
            metadata_buffer,
            term_buffer,
            hdr,
            msg_body,
            term_appender,
            header_writer,
            hidden_metadata_buffer
        );

        // Write TERM_ID in to metadata buffer used by TermAppender
        let packed_tail = pack_raw_tail(TERM_ID, tail);
        let _prev_tail = hidden_metadata_buffer.get_and_add_i64(*TERM_TAIL_OFFSET, packed_tail);

        term_buffer.put_ordered::<i32>(frame_descriptor::length_offset(tail), -MAX_FRAME_LENGTH);
        term_buffer.copy_from(tail + data_frame_header::LENGTH, &msg_body, 0, MAX_PAYLOAD_LENGTH);
        term_buffer.put::<u8>(frame_descriptor::flags_offset(tail), frame_descriptor::BEGIN_FRAG);
        term_buffer.put::<i64>(tail + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET, RESERVED_VALUE);
        term_buffer.put_ordered::<i32>(frame_descriptor::length_offset(tail), MAX_FRAME_LENGTH);

        tail = MAX_FRAME_LENGTH;

        term_buffer.put_ordered::<i32>(frame_descriptor::length_offset(tail), -frame_length);
        term_buffer.copy_from(tail + data_frame_header::LENGTH, &msg_body, MAX_PAYLOAD_LENGTH, 1);
        term_buffer.put::<u8>(frame_descriptor::flags_offset(tail), frame_descriptor::END_FRAG);
        term_buffer.put::<i64>(tail + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET, RESERVED_VALUE);
        term_buffer.put_ordered::<i32>(frame_descriptor::length_offset(tail), frame_length);

        let resulting_offset = term_appender.append_fragmented_message(
            &header_writer,
            &msg_body,
            0,
            msg_length,
            MAX_PAYLOAD_LENGTH,
            reserved_value_supplier,
            TERM_ID,
        );
        assert!(resulting_offset.is_ok());
        assert_eq!(resulting_offset.unwrap(), required_capacity);
    }

    #[test]
    #[allow(unused_variables)]
    fn test_term_appender_claim_region_for_zero_copy_encoding() {
        let msg_length: Index = 20;
        let frame_length = data_frame_header::LENGTH + msg_length;
        let aligned_frame_length = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);
        let tail: Index = 0;

        let mut buffer_claim = BufferClaim::default();

        gen_test_data!(
            metadata_buffer,
            term_buffer,
            hdr,
            msg_body,
            term_appender,
            header_writer,
            hidden_metadata_buffer
        );

        // Write TERM_ID in to metadata buffer used by TermAppender
        let packed_tail = pack_raw_tail(TERM_ID, tail);
        let _prev_tail = hidden_metadata_buffer.get_and_add_i64(*TERM_TAIL_OFFSET, packed_tail);

        term_buffer.put_ordered::<i32>(frame_descriptor::length_offset(tail), -frame_length);

        let resulting_offset = term_appender.claim(&header_writer, msg_length, &mut buffer_claim, TERM_ID);
        assert!(resulting_offset.is_ok());
        assert_eq!(resulting_offset.unwrap(), aligned_frame_length);

        assert_eq!(buffer_claim.offset(), (tail + data_frame_header::LENGTH));
        assert_eq!(buffer_claim.length(), msg_length);

        buffer_claim.commit();
    }
}
