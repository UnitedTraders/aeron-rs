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

use crate::{
    concurrent::{
        atomic_buffer::AtomicBuffer,
        logbuffer::{data_frame_header, frame_descriptor},
    },
    utils::{bit_utils, types::Index},
};

/**
 * Callback for handling a block of messages being read from a log.
 *
 * @param buffer    containing the block of message fragments.
 * @param offset    at which the block begins.
 * @param length    of the block in bytes.
 * @param sessionId of the stream containing this block of message fragments.
 * @param term_id    of the stream containing this block of message fragments.
 */
pub type BlockHandler = fn(&AtomicBuffer, Index, Index, i32, i32);

pub fn scan(term_buffer: &AtomicBuffer, term_offset: Index, limit_offset: Index) -> Index {
    let mut offset = term_offset;

    while offset < limit_offset {
        let frame_length = frame_descriptor::frame_length_volatile(term_buffer, offset);
        if frame_length <= 0 {
            break;
        }

        let aligned_frame_length = bit_utils::align(frame_length as Index, frame_descriptor::FRAME_ALIGNMENT);

        if frame_descriptor::is_padding_frame(term_buffer, offset) {
            if term_offset == offset {
                offset += aligned_frame_length;
            }

            break;
        }

        if offset + aligned_frame_length > limit_offset {
            break;
        }

        offset += aligned_frame_length;
    }

    offset
}

/// GapHandler is called for each found gap
/// Params:
/// 1. i32 - term ID
/// 2. &AtomicBuffer - term (log) buffer we are scanning
/// 3. Index - offset in the buffer where gap begins
/// 4. Index - gap length
pub type GapHandler = fn(i32, &AtomicBuffer, Index, Index);

pub fn scan_for_gap(
    term_buffer: &AtomicBuffer,
    term_id: i32,
    mut rebuild_offset: Index,
    hwm_offset: Index,
    mut handler: impl FnMut(i32, &AtomicBuffer, Index, Index),
) -> Index {
    loop {
        let frame_length = frame_descriptor::frame_length_volatile(term_buffer, rebuild_offset);
        if frame_length <= 0 {
            break;
        }

        rebuild_offset += bit_utils::align(frame_length as Index, frame_descriptor::FRAME_ALIGNMENT);

        if rebuild_offset >= hwm_offset {
            break;
        }
    }

    let gap_begin_offset = rebuild_offset;
    if rebuild_offset < hwm_offset {
        let limit = hwm_offset - frame_descriptor::ALIGNED_HEADER_LENGTH;

        while rebuild_offset < limit {
            rebuild_offset += frame_descriptor::FRAME_ALIGNMENT;

            if term_buffer.get_volatile::<i32>(rebuild_offset) != 0 {
                rebuild_offset -= frame_descriptor::ALIGNED_HEADER_LENGTH;
                break;
            }
        }

        let gap_length = (rebuild_offset - gap_begin_offset) + frame_descriptor::ALIGNED_HEADER_LENGTH;
        handler(term_id, term_buffer, gap_begin_offset, gap_length);
    }

    gap_begin_offset
}

pub fn scan_outcome(padding: Index, available: Index) -> i64 {
    (padding as i64) << 32 | available as i64
}

pub fn available(scan_outcome: i64) -> i32 {
    scan_outcome as i32
}

pub fn padding(scan_outcome: i64) -> i32 {
    (scan_outcome >> 32) as i32
}

pub fn scan_for_availability(term_buffer: &AtomicBuffer, offset: Index, max_length: Index) -> i64 {
    let max_length = std::cmp::min(max_length, term_buffer.capacity() - offset);
    let mut available: Index = 0;
    let mut padding: Index = 0;

    loop {
        let frame_offset = offset + available;
        let frame_length = frame_descriptor::frame_length_volatile(term_buffer, frame_offset);

        if frame_length <= 0 {
            break;
        }

        let mut aligned_frame_length = bit_utils::align(frame_length as Index, frame_descriptor::FRAME_ALIGNMENT);

        if frame_descriptor::is_padding_frame(term_buffer, frame_offset) {
            padding = aligned_frame_length - data_frame_header::LENGTH;
            aligned_frame_length = data_frame_header::LENGTH;
        }

        available += aligned_frame_length;

        if available > max_length {
            available -= aligned_frame_length;
            padding = 0;
            break;
        }

        if (available + padding) >= max_length {
            break;
        }
    }

    scan_outcome(padding, available)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::concurrent::{
        atomic_buffer::AlignedBuffer,
        logbuffer::{log_buffer_descriptor, term_scan},
    };

    const LOG_BUFFER_CAPACITY: Index = log_buffer_descriptor::TERM_MIN_LENGTH;
    const TERM_ID: i32 = 1;
    const MTU_LENGTH: Index = 1024;

    // Term scanner tests
    #[test]
    fn test_scan_empty_buffer() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);
        log.set_memory(0, log.capacity(), 0);

        let offset = 0;
        let limit_offset = log.capacity();

        let new_offset = term_scan::scan(&log, offset, limit_offset);

        assert_eq!(new_offset, offset);
    }

    #[test]
    fn test_scan_read_first_message() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);

        let offset = 0;
        let limit_offset = log.capacity();
        let message_length = 50;
        let aligned_message_length = bit_utils::align(message_length, frame_descriptor::FRAME_ALIGNMENT);

        // Write message length in to log
        log.put_ordered::<i32>(frame_descriptor::length_offset(offset), message_length);

        // Set type
        log.put::<u16>(frame_descriptor::type_offset(offset), data_frame_header::HDR_TYPE_DATA);

        // Write next message length as 0 in to log
        log.put_ordered::<i32>(frame_descriptor::length_offset(aligned_message_length), 0);

        let new_offset = term_scan::scan(&log, offset, limit_offset);

        assert_eq!(new_offset, aligned_message_length);
    }

    #[test]
    fn test_scan_read_block_of_two_messages() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);
        let offset = 0;
        let limit_offset = log.capacity();
        let message_length = 50;
        let aligned_message_length = bit_utils::align(message_length, frame_descriptor::FRAME_ALIGNMENT);

        // Write message length in to log
        log.put_ordered::<i32>(frame_descriptor::length_offset(offset), message_length);
        // Set type
        log.put::<u16>(frame_descriptor::type_offset(offset), data_frame_header::HDR_TYPE_DATA);
        // Write next message length
        log.put_ordered::<i32>(frame_descriptor::length_offset(aligned_message_length), message_length);
        // Set type
        log.put::<u16>(
            frame_descriptor::type_offset(aligned_message_length),
            data_frame_header::HDR_TYPE_DATA,
        );
        // Write next message length as 0 in to log
        log.put_ordered::<i32>(frame_descriptor::length_offset(aligned_message_length * 2), 0);

        let new_offset = term_scan::scan(&log, offset, limit_offset);

        assert_eq!(new_offset, aligned_message_length * 2);
    }

    #[test]
    fn test_scan_read_block_of_three_messages_that_fill_buffer() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);

        let offset = 0;
        let limit_offset = log.capacity();
        let message_length = 50;
        let aligned_message_length = bit_utils::align(message_length, frame_descriptor::FRAME_ALIGNMENT);
        let third_message_length = limit_offset - (2 * aligned_message_length);

        // Write message length in to log
        log.put_ordered::<i32>(frame_descriptor::length_offset(offset), message_length);
        // Set type
        log.put::<u16>(frame_descriptor::type_offset(offset), data_frame_header::HDR_TYPE_DATA);
        // Write next message length
        log.put_ordered::<i32>(frame_descriptor::length_offset(aligned_message_length), message_length);
        // Set type for second msg
        log.put::<u16>(
            frame_descriptor::type_offset(aligned_message_length),
            data_frame_header::HDR_TYPE_DATA,
        );
        // Write next message length
        log.put_ordered::<i32>(
            frame_descriptor::length_offset(aligned_message_length * 2),
            third_message_length,
        );
        // Set type for third msg
        log.put::<u16>(
            frame_descriptor::type_offset(aligned_message_length * 2),
            data_frame_header::HDR_TYPE_DATA,
        );

        let new_offset = term_scan::scan(&log, offset, limit_offset);

        assert_eq!(new_offset, limit_offset);
    }

    #[test]
    fn test_scan_read_block_of_two_messages_because_of_limit() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);

        let offset = 0;
        let message_length = 50;
        let aligned_message_length = bit_utils::align(message_length, frame_descriptor::FRAME_ALIGNMENT);
        let limit_offset = (2 * aligned_message_length) + 1;

        // Write message length in to log
        log.put_ordered::<i32>(frame_descriptor::length_offset(offset), message_length);
        // Set type
        log.put::<u16>(frame_descriptor::type_offset(offset), data_frame_header::HDR_TYPE_DATA);
        // Write next message length as 0 in to log
        log.put_ordered::<i32>(frame_descriptor::length_offset(aligned_message_length), message_length);
        // Set type for second msg
        log.put::<u16>(
            frame_descriptor::type_offset(aligned_message_length),
            data_frame_header::HDR_TYPE_DATA,
        );
        // Write next message length as 0 in to log
        log.put_ordered::<i32>(frame_descriptor::length_offset(aligned_message_length * 2), message_length);
        // Set type for second msg
        log.put::<u16>(
            frame_descriptor::type_offset(aligned_message_length * 2),
            data_frame_header::HDR_TYPE_DATA,
        );

        let new_offset = term_scan::scan(&log, offset, limit_offset);

        assert_eq!(new_offset, aligned_message_length * 2);
    }

    #[test]
    fn test_scan_fail_to_read_first_message_because_of_limit() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);

        let offset = 0;
        let message_length = 50;
        let aligned_message_length = bit_utils::align(message_length, frame_descriptor::FRAME_ALIGNMENT);
        let limit_offset = aligned_message_length - 1;

        // Write message length in to log
        log.put_ordered::<i32>(frame_descriptor::length_offset(offset), message_length);
        // Set type
        log.put::<u16>(frame_descriptor::type_offset(offset), data_frame_header::HDR_TYPE_DATA);

        let new_offset = term_scan::scan(&log, offset, limit_offset);

        assert_eq!(new_offset, offset);
    }

    #[test]
    fn test_scan_read_one_message_on_limit() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);

        let offset = 0;
        let message_length = 50;
        let aligned_message_length = bit_utils::align(message_length, frame_descriptor::FRAME_ALIGNMENT);
        let limit_offset = aligned_message_length;

        // Write message length in to log
        log.put_ordered::<i32>(frame_descriptor::length_offset(offset), message_length);
        // Set type
        log.put::<u16>(frame_descriptor::type_offset(offset), data_frame_header::HDR_TYPE_DATA);

        let new_offset = term_scan::scan(&log, offset, limit_offset);

        assert_eq!(new_offset, aligned_message_length);
    }

    #[test]
    fn test_scan_read_block_of_one_message_then_padding() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);

        let offset = 0;
        let message_length = 50;
        let aligned_message_length = bit_utils::align(message_length, frame_descriptor::FRAME_ALIGNMENT);
        let limit_offset = log.capacity();

        // Write message length in to log
        log.put_ordered::<i32>(frame_descriptor::length_offset(offset), message_length);
        // Set type
        log.put::<u16>(frame_descriptor::type_offset(offset), data_frame_header::HDR_TYPE_DATA);
        // Write message length in to log
        log.put_ordered::<i32>(frame_descriptor::length_offset(aligned_message_length), message_length);
        // Set type
        log.put::<u16>(
            frame_descriptor::type_offset(aligned_message_length),
            data_frame_header::HDR_TYPE_PAD,
        );

        let offset_one = term_scan::scan(&log, offset, limit_offset);
        assert_eq!(offset_one, aligned_message_length);

        let offset_two = term_scan::scan(&log, offset_one, limit_offset);
        assert_eq!(offset_two, aligned_message_length * 2);
    }

    // Gap scanner tests
    #[test]
    fn test_scan_report_gap_at_beginning_of_buffer() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);

        let mut called = false;
        let frame_offset = bit_utils::align(data_frame_header::LENGTH * 3, frame_descriptor::FRAME_ALIGNMENT);
        let high_water_mark = frame_offset + frame_descriptor::ALIGNED_HEADER_LENGTH;

        log.set_memory(0, log.capacity(), 0);

        log.put_ordered::<i32>(frame_offset, data_frame_header::LENGTH);

        let handler = |term_id: i32, _buffer: &AtomicBuffer, offset: Index, length: Index| {
            assert_eq!(TERM_ID, term_id);
            assert_eq!(0, offset);
            assert_eq!(frame_offset, length);
            called = true;
        };

        assert_eq!(0, term_scan::scan_for_gap(&log, TERM_ID, 0, high_water_mark, handler));

        assert!(called);
    }

    #[test]
    fn test_scan_report_single_gap_when_buffer_not_full() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);

        let mut called = false;
        let tail = bit_utils::align(data_frame_header::LENGTH, frame_descriptor::FRAME_ALIGNMENT);
        let high_water_mark = frame_descriptor::FRAME_ALIGNMENT * 3;

        log.set_memory(0, log.capacity(), 0);

        log.put_ordered::<i32>(tail - frame_descriptor::ALIGNED_HEADER_LENGTH, data_frame_header::LENGTH);
        log.put_ordered::<i32>(tail, 0);
        log.put_ordered::<i32>(
            high_water_mark - frame_descriptor::ALIGNED_HEADER_LENGTH,
            data_frame_header::LENGTH,
        );

        let handler = |term_id: i32, _buffer: &AtomicBuffer, offset: Index, length: Index| {
            assert_eq!(TERM_ID, term_id);
            assert_eq!(tail, offset);
            assert_eq!(frame_descriptor::ALIGNED_HEADER_LENGTH, length);
            called = true;
        };

        assert_eq!(tail, term_scan::scan_for_gap(&log, TERM_ID, 0, high_water_mark, handler));

        assert!(called);
    }

    #[test]
    fn test_scan_report_single_gap_when_buffer_is_full() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);

        let mut called = false;
        let tail = LOG_BUFFER_CAPACITY - (frame_descriptor::ALIGNED_HEADER_LENGTH * 2);
        let high_water_mark = LOG_BUFFER_CAPACITY;

        log.set_memory(0, log.capacity(), 0);

        log.put_ordered::<i32>(tail - frame_descriptor::ALIGNED_HEADER_LENGTH, data_frame_header::LENGTH);
        log.put_ordered::<i32>(tail, 0);
        log.put_ordered::<i32>(
            high_water_mark - frame_descriptor::ALIGNED_HEADER_LENGTH,
            data_frame_header::LENGTH,
        );

        let handler = |term_id: i32, _buffer: &AtomicBuffer, offset: Index, length: Index| {
            assert_eq!(TERM_ID, term_id);
            assert_eq!(tail, offset);
            assert_eq!(frame_descriptor::ALIGNED_HEADER_LENGTH, length);
            called = true;
        };

        assert_eq!(tail, term_scan::scan_for_gap(&log, TERM_ID, tail, high_water_mark, handler));

        assert!(called);
    }

    #[test]
    fn test_scan_report_no_gap_when_hwm_is_in_padding() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);

        let mut called = false;
        let padding_length = frame_descriptor::ALIGNED_HEADER_LENGTH * 2;
        let tail = LOG_BUFFER_CAPACITY - padding_length;
        let high_water_mark = LOG_BUFFER_CAPACITY - padding_length + data_frame_header::LENGTH;

        log.set_memory(0, log.capacity(), 0);

        log.put_ordered::<i32>(tail, padding_length);
        log.put_ordered::<i32>(tail + data_frame_header::LENGTH, 0);

        let handler = |_term_id: i32, _buffer: &AtomicBuffer, _offset: Index, _length: Index| {
            called = true;
        };

        assert_eq!(
            LOG_BUFFER_CAPACITY,
            term_scan::scan_for_gap(&log, TERM_ID, tail, high_water_mark, handler)
        );

        assert!(!called);
    }

    #[test]
    fn test_scan_pack_padding_and_offset_into_resulting_status() {
        let padding = 77;
        let available = 65000;

        let scan_outcome = term_scan::scan_outcome(padding, available);

        assert_eq!(padding, term_scan::padding(scan_outcome));
        assert_eq!(available, term_scan::available(scan_outcome));
    }

    #[test]
    fn test_scan_single_message() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);

        let msg_length = 1;
        let frame_length = data_frame_header::LENGTH + msg_length;
        let aligned_frame_length = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);
        let frame_offset = 0;

        log.put_ordered::<i32>(frame_offset, frame_length);
        log.put::<u16>(frame_descriptor::type_offset(frame_offset), data_frame_header::HDR_TYPE_DATA);
        log.put_ordered::<i32>(aligned_frame_length, 0);

        let scan_outcome = term_scan::scan_for_availability(&log, frame_offset, MTU_LENGTH);

        assert_eq!(aligned_frame_length, term_scan::available(scan_outcome));
        assert_eq!(0, term_scan::padding(scan_outcome));
    }

    #[test]
    fn test_scan_fail_to_scan_message_larger_than_max_length() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);

        let msg_length = 1;
        let frame_length = data_frame_header::LENGTH + msg_length;
        let aligned_frame_length = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);
        let max_length = aligned_frame_length - 1;
        let frame_offset = 0;

        log.put_ordered::<i32>(frame_offset, frame_length);
        log.put::<u16>(frame_descriptor::type_offset(frame_offset), data_frame_header::HDR_TYPE_DATA);

        let scan_outcome = term_scan::scan_for_availability(&log, frame_offset, max_length);

        assert_eq!(0, term_scan::available(scan_outcome));
        assert_eq!(0, term_scan::padding(scan_outcome));
    }

    fn expect_scan_two_messages(
        buffer: &AtomicBuffer,
        frame_length_one: Index,
        frame_length_two: Index,
        frame_offset: Index,
        frame_type_one: u16,
        frame_type_two: u16,
    ) -> Index {
        let aligned_length_one = bit_utils::align(frame_length_one, frame_descriptor::FRAME_ALIGNMENT);
        let aligned_length_two = bit_utils::align(frame_length_two, frame_descriptor::FRAME_ALIGNMENT);

        buffer.put_ordered::<i32>(frame_offset, aligned_length_one);
        buffer.put::<u16>(frame_descriptor::type_offset(frame_offset), frame_type_one);

        buffer.put_ordered::<i32>(frame_offset + aligned_length_one, frame_length_two);
        buffer.put::<u16>(
            frame_descriptor::type_offset(frame_offset + aligned_length_one),
            frame_type_two,
        );

        aligned_length_one + aligned_length_two
    }

    #[test]
    fn test_scan_two_messages_that_fit_in_single_mtu() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);

        let msg_length = 100;
        let frame_length = data_frame_header::LENGTH + msg_length;

        let total_length = expect_scan_two_messages(
            &log,
            frame_length,
            frame_length,
            0,
            data_frame_header::HDR_TYPE_DATA,
            data_frame_header::HDR_TYPE_DATA,
        );
        log.put_ordered::<i32>(total_length, 0);

        let scan_outcome = term_scan::scan_for_availability(&log, 0, MTU_LENGTH);

        assert_eq!(total_length, term_scan::available(scan_outcome));
        assert_eq!(0, term_scan::padding(scan_outcome));
    }

    #[test]
    fn test_scan_two_messages_and_stop_at_mtu_boundary() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);

        let frame_two_length = bit_utils::align(data_frame_header::LENGTH + 1, frame_descriptor::FRAME_ALIGNMENT);
        let frame_one_length = bit_utils::align(MTU_LENGTH - frame_two_length, frame_descriptor::FRAME_ALIGNMENT);
        let frame_offset = 0;

        let total_length = expect_scan_two_messages(
            &log,
            frame_one_length,
            frame_two_length,
            0,
            data_frame_header::HDR_TYPE_DATA,
            data_frame_header::HDR_TYPE_DATA,
        );

        let scan_outcome = term_scan::scan_for_availability(&log, frame_offset, MTU_LENGTH);

        assert_eq!(total_length, term_scan::available(scan_outcome));
        assert_eq!(0, term_scan::padding(scan_outcome));
    }

    #[test]
    fn test_scan_two_messages_and_stop_at_second_that_spans_mtu() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);

        let frame_two_length = bit_utils::align(data_frame_header::LENGTH * 2, frame_descriptor::FRAME_ALIGNMENT);
        let frame_one_length = MTU_LENGTH - (frame_two_length / 2);
        let frame_offset = 0;

        expect_scan_two_messages(
            &log,
            frame_one_length,
            frame_two_length,
            0,
            data_frame_header::HDR_TYPE_DATA,
            data_frame_header::HDR_TYPE_DATA,
        );

        let scan_outcome = term_scan::scan_for_availability(&log, frame_offset, MTU_LENGTH);

        assert_eq!(frame_one_length, term_scan::available(scan_outcome));
        assert_eq!(0, term_scan::padding(scan_outcome));
    }

    #[test]
    fn test_scan_last_frame_in_buffer() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);

        let aligned_frame_length = bit_utils::align(data_frame_header::LENGTH * 2, frame_descriptor::FRAME_ALIGNMENT);
        let frame_offset = LOG_BUFFER_CAPACITY - aligned_frame_length;

        log.put_ordered::<i32>(frame_offset, aligned_frame_length);
        log.put::<u16>(frame_descriptor::type_offset(frame_offset), data_frame_header::HDR_TYPE_DATA);

        let scan_outcome = term_scan::scan_for_availability(&log, frame_offset, MTU_LENGTH);

        assert_eq!(aligned_frame_length, term_scan::available(scan_outcome));
        assert_eq!(0, term_scan::padding(scan_outcome));
    }

    #[test]
    fn test_scan_last_message_in_buffer_plus_padding() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);

        let aligned_frame_length = bit_utils::align(data_frame_header::LENGTH * 2, frame_descriptor::FRAME_ALIGNMENT);
        let padding_frame_length = bit_utils::align(data_frame_header::LENGTH * 3, frame_descriptor::FRAME_ALIGNMENT);
        let frame_offset = LOG_BUFFER_CAPACITY - (aligned_frame_length + padding_frame_length);

        expect_scan_two_messages(
            &log,
            aligned_frame_length,
            padding_frame_length,
            frame_offset,
            data_frame_header::HDR_TYPE_DATA,
            data_frame_header::HDR_TYPE_PAD,
        );

        let scan_outcome = term_scan::scan_for_availability(&log, frame_offset, MTU_LENGTH);

        assert_eq!(
            aligned_frame_length + data_frame_header::LENGTH,
            term_scan::available(scan_outcome) as Index
        );
        assert_eq!(
            padding_frame_length - data_frame_header::LENGTH,
            term_scan::padding(scan_outcome) as Index
        );
    }

    #[test]
    fn test_scan_last_message_in_buffer_minus_padding_limited_by_mtu() {
        let t_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
        let log = AtomicBuffer::from_aligned(&t_buff);

        let aligned_frame_length = bit_utils::align(data_frame_header::LENGTH, frame_descriptor::FRAME_ALIGNMENT);
        let frame_offset =
            LOG_BUFFER_CAPACITY - bit_utils::align(data_frame_header::LENGTH * 3, frame_descriptor::FRAME_ALIGNMENT);
        let mtu = aligned_frame_length + 8;

        expect_scan_two_messages(
            &log,
            aligned_frame_length,
            aligned_frame_length * 2,
            frame_offset,
            data_frame_header::HDR_TYPE_DATA,
            data_frame_header::HDR_TYPE_PAD,
        );

        let scan_outcome = term_scan::scan_for_availability(&log, frame_offset, mtu);

        assert_eq!(aligned_frame_length, term_scan::available(scan_outcome));
        assert_eq!(0, term_scan::padding(scan_outcome));
    }
}
