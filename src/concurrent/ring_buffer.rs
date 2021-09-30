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

use thiserror::Error;

use crate::{
    command::control_protocol_events::AeronCommand,
    concurrent::atomic_buffer::AtomicBuffer,
    utils::{
        bit_utils::{align, is_power_of_two},
        misc::CACHE_LINE_LENGTH,
        types::Index,
    },
};

//todo: rewrite all these index-based accessors using blitted structs + custom volatile cells?
pub const TAIL_POSITION_OFFSET: Index = CACHE_LINE_LENGTH * 2;
pub const HEAD_CACHE_POSITION_OFFSET: Index = CACHE_LINE_LENGTH * 4;
pub const HEAD_POSITION_OFFSET: Index = CACHE_LINE_LENGTH * 6;
pub const CORRELATION_COUNTER_OFFSET: Index = CACHE_LINE_LENGTH * 8;
pub const CONSUMER_HEARTBEAT_OFFSET: Index = CACHE_LINE_LENGTH * 10;

/// Total length of the trailer in bytes
pub const TRAILER_LENGTH: Index = CACHE_LINE_LENGTH * 12;

fn check_capacity(capacity: Index) -> Result<(), RingBufferError> {
    if is_power_of_two(capacity) {
        Ok(())
    } else {
        Err(RingBufferError::CapacityIsNotTwoPower { capacity })
    }
}

pub mod record_descriptor {
    /**
     * Header length made up of fields for message length, message type, and then the encoded message.
     * <p>
     * Writing of the record length signals the message recording is complete.
     * <pre>
     *   0                   1                   2                   3
     *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |R|                       Record Length                         |
     *  +-+-------------------------------------------------------------+
     *  |                              Type                             |
     *  +---------------------------------------------------------------+
     *  |                       Encoded Message                        ...
     * ...                                                              |
     *  +---------------------------------------------------------------+
     * </pre>
     */
    use super::RingBufferError;
    use crate::command::control_protocol_events::AeronCommand;
    use crate::utils::types::{Index, I32_SIZE};

    pub const HEADER_LENGTH: Index = I32_SIZE * 2;
    pub const ALIGNMENT: Index = HEADER_LENGTH;

    #[inline]
    pub fn length_offset(record_offset: Index) -> Index {
        record_offset
    }

    #[inline]
    pub fn type_offset(record_offset: Index) -> Index {
        record_offset + I32_SIZE
    }

    #[inline]
    pub fn encoded_msg_offset(record_offset: Index) -> Index {
        record_offset + HEADER_LENGTH
    }

    #[inline]
    pub fn make_header(len: Index, command: AeronCommand) -> i64 {
        // high 32 bits are from `command`, low 32 bits are from `len`
        (((command as i64) & 0xFFFF_FFFF) << 32) | ((len as i64) & 0xFFFF_FFFF)
    }

    #[inline]
    pub fn record_length(header: i64) -> Index {
        // explicitly cut off the higher 32 bits
        (header & 0xFFFF_FFFF) as Index
    }

    #[inline]
    pub fn message_type_id(header: i64) -> i32 {
        (header >> 32) as i32
    }

    #[inline]
    pub fn message_type(header: i64) -> AeronCommand {
        let type_id = message_type_id(header);
        AeronCommand::from_command_id(type_id)
    }

    #[inline]
    pub fn check_msg_type_id(msg_type_id: i32) -> Result<(), RingBufferError> {
        if msg_type_id < 1 {
            return Err(RingBufferError::NonPositiveMessageTypeId(msg_type_id));
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Error)]
pub enum RingBufferError {
    #[error("Insufficient capacity")]
    InsufficientCapacity,
    #[error("Encoded message exceeds maxMsgLength of {max}: length={msg}")]
    MessageTooLong { msg: Index, max: Index },
    #[error("Message type id must be greater than zero, msgTypeId={0}")]
    NonPositiveMessageTypeId(i32),
    #[error("Capacity must be a positive power of 2 + TRAILER_LENGTH: capacity={capacity}")]
    CapacityIsNotTwoPower { capacity: Index },
}

#[derive(Debug)]
pub struct ManyToOneRingBuffer {
    buffer: AtomicBuffer,
    capacity: Index,
    max_msg_len: Index,
    tail_position: Index,
    head_cache_position: Index,
    head_position: Index,
    correlation_id_counter: Index,
    consumer_heartbeat: Index,
}

impl ManyToOneRingBuffer {
    pub fn new(buffer: AtomicBuffer) -> Result<Self, RingBufferError> {
        let capacity = buffer.capacity() - TRAILER_LENGTH;
        check_capacity(capacity)?;
        let max_msg_len = capacity / 8;
        let tail_position = capacity + TAIL_POSITION_OFFSET;
        let head_cache_position = capacity + HEAD_CACHE_POSITION_OFFSET;
        let head_position = capacity + HEAD_POSITION_OFFSET;
        let correlation_id_counter = capacity + CORRELATION_COUNTER_OFFSET;
        let consumer_heartbeat = capacity + CONSUMER_HEARTBEAT_OFFSET;

        Ok(Self {
            buffer,
            capacity,
            max_msg_len,
            tail_position,
            head_cache_position,
            head_position,
            correlation_id_counter,
            consumer_heartbeat,
        })
    }

    pub fn write(
        &self,
        cmd: AeronCommand,
        src_buffer: AtomicBuffer,
        src_index: Index,
        length: Index,
    ) -> Result<(), RingBufferError> {
        record_descriptor::check_msg_type_id(cmd as i32)?;
        self.check_msg_length(length)?;

        let record_len = length + record_descriptor::HEADER_LENGTH;
        let required_capacity = align(record_len, record_descriptor::ALIGNMENT);
        // once we claim the required capacity we can write without conflicts
        let record_index = self.claim(required_capacity)?;

        self.buffer
            .put_ordered::<i64>(record_index, record_descriptor::make_header(-record_len, cmd));

        self.buffer.copy_from(
            record_descriptor::encoded_msg_offset(record_index),
            &src_buffer,
            src_index,
            length,
        );

        self.buffer
            .put_ordered::<i32>(record_descriptor::length_offset(record_index), record_len);

        Ok(())
    }

    /// Read from the ring buffer until either wrap-around or `msg_count_max` messages have been
    /// processed.
    /// Returns the number of messages processed.
    pub fn read<F: FnMut(AeronCommand, AtomicBuffer)>(&self, mut handler: F, msg_count_limit: i32) -> i32 {
        let head = self.buffer.get_volatile::<i64>(self.head_position);
        let head_index = (head & (self.capacity - 1) as i64) as i32;
        let contiguous_block_len = self.capacity - head_index;

        let mut messages_read = 0;
        let mut bytes_read = 0;

        while bytes_read < contiguous_block_len && messages_read < msg_count_limit {
            let record_index = head_index + bytes_read;
            let header = self.buffer.get_volatile::<i64>(record_index);
            let record_len = record_descriptor::record_length(header);
            if record_len <= 0 {
                break;
            }

            bytes_read += align(record_len, record_descriptor::ALIGNMENT);

            let msg_type = record_descriptor::message_type(header);
            if let AeronCommand::Padding = msg_type {
                continue;
            }
            messages_read += 1;
            let view = self.buffer.view(
                record_descriptor::encoded_msg_offset(record_index),
                record_len - record_descriptor::HEADER_LENGTH,
            );
            handler(msg_type, view)
        }

        // todo: move to a guard, or prevent corruption on panic
        if bytes_read != 0 {
            // zero-out memory which was already read.
            self.buffer.set_memory(head_index, bytes_read, 0);
            // advance reader position
            self.buffer.put_ordered(self.head_position, head + bytes_read as i64);
        }
        messages_read
    }

    // Read all messages
    #[inline]
    pub fn read_all<F: FnMut(AeronCommand, AtomicBuffer)>(&self, handler: F) -> i32 {
        self.read(handler, std::i32::MAX)
    }

    #[inline]
    pub fn capacity(&self) -> Index {
        self.capacity
    }

    #[inline]
    pub fn max_msg_len(&self) -> Index {
        self.max_msg_len
    }

    #[inline]
    pub fn next_correlation_id(&self) -> i64 {
        self.buffer.get_and_add_i64(self.correlation_id_counter, 1)
    }

    #[inline]
    pub fn set_consumer_heartbeat_time(&self, time: i64) {
        self.buffer.put_ordered(self.consumer_heartbeat, time)
    }

    #[inline]
    pub fn consumer_heartbeat_time(&self) -> i64 {
        self.buffer.get_volatile::<i64>(self.consumer_heartbeat)
    }

    #[inline]
    pub fn size(&self) -> Index {
        let mut tail: i64;
        let mut head_after = self.buffer.get_volatile::<i64>(self.head_position);

        loop {
            let head_before = head_after;
            tail = self.buffer.get_volatile::<i64>(self.tail_position);
            head_after = self.buffer.get_volatile::<i64>(self.head_position);

            if head_after == head_before {
                return (tail - head_after) as Index;
            }
        }
    }

    #[allow(clippy::comparison_chain)]
    pub fn unblock(&self) -> bool {
        let head_position = self.buffer.get_volatile::<i64>(self.head_position);
        let tail_position = self.buffer.get_volatile::<i64>(self.tail_position);

        if tail_position == head_position {
            return false;
        }

        let mask = (self.capacity - 1) as i64;
        let consumer_index = (head_position & mask) as Index;
        let producer_index = (tail_position & mask) as Index;

        let mut unblocked = false;
        let length: Index = self.buffer.get_volatile(consumer_index);
        if length < 0 {
            self.buffer
                .put_ordered(consumer_index, record_descriptor::make_header(-length, AeronCommand::Padding));
            unblocked = true;
        } else if length == 0 {
            let limit = if producer_index > consumer_index {
                producer_index
            } else {
                self.buffer.capacity()
            };

            let mut i = consumer_index + record_descriptor::ALIGNMENT;
            loop {
                let length: i32 = self.buffer.get_volatile(i);
                if length != 0 {
                    if self.scan_back_to_confirm_still_zeroed(i, consumer_index) {
                        self.buffer.put_ordered(
                            consumer_index,
                            record_descriptor::make_header(i - consumer_index, AeronCommand::Padding),
                        );
                        unblocked = true;
                    }

                    break;
                }

                i += record_descriptor::ALIGNMENT;
                if i >= limit {
                    break;
                }
            }
        }
        unblocked
    }

    fn claim(&self, required_capacity: Index) -> Result<Index, RingBufferError> {
        let mask = (self.capacity - 1) as i64;
        let mut head = self.buffer.get_volatile::<i64>(self.head_cache_position);

        let (padding, tail_index) = loop {
            let tail = self.buffer.get_volatile::<i64>(self.tail_position);
            let available_capacity = self.capacity - (tail - head) as Index;

            if required_capacity > available_capacity {
                head = self.buffer.get_volatile::<i64>(self.head_position);
                if required_capacity > (self.capacity - (tail - head) as Index) {
                    return Err(RingBufferError::InsufficientCapacity);
                }
                self.buffer.put_ordered::<i64>(self.head_cache_position, head);
            }

            let mut padding = 0;
            let tail_index = (tail & mask) as Index;
            let len_to_buffer_end = self.capacity - tail_index;

            if required_capacity > len_to_buffer_end {
                let mut head_index = (head & mask) as Index;
                if required_capacity > head_index {
                    head = self.buffer.get_volatile::<i64>(self.head_position);
                    head_index = (head & mask) as Index;
                    if required_capacity > head_index {
                        return Err(RingBufferError::InsufficientCapacity);
                    }
                    self.buffer.put_ordered::<i64>(self.head_cache_position, head);
                }
                padding = len_to_buffer_end;
            }
            let t2 = tail + required_capacity as i64 + padding as i64;
            if self.buffer.compare_and_set_i64(self.tail_position, tail, t2) {
                break (padding, tail_index);
            }
        };

        if padding != 0 {
            self.buffer
                .put_ordered::<i64>(tail_index, record_descriptor::make_header(padding, AeronCommand::Padding));
            Ok(0)
        } else {
            Ok(tail_index)
        }
    }

    fn check_msg_length(&self, length: Index) -> Result<(), RingBufferError> {
        if length > self.max_msg_len {
            return Err(RingBufferError::MessageTooLong {
                msg: length,
                max: self.max_msg_len,
            });
        }
        Ok(())
    }

    fn scan_back_to_confirm_still_zeroed(&self, from: Index, limit: Index) -> bool {
        let mut i = from - record_descriptor::ALIGNMENT;

        while i >= limit {
            if self.buffer.get_volatile::<i32>(i) != 0 {
                return false;
            }

            i -= record_descriptor::ALIGNMENT;
        }
        true
    }
}

unsafe impl Send for ManyToOneRingBuffer {}
unsafe impl Sync for ManyToOneRingBuffer {}

#[cfg(test)]
mod tests {
    // use std::sync::atomic::{AtomicI64, Ordering};
    // use std::sync::Arc;

    use lazy_static::lazy_static;

    use super::*;
    use crate::command::control_protocol_events::AeronCommand;
    use crate::concurrent::ring_buffer::record_descriptor::{make_header, message_type, message_type_id, record_length};
    use crate::{
        concurrent::atomic_buffer::{AlignedBuffer, AtomicBuffer},
        utils::{bit_utils::align, types::Index},
    };
    use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    const CAPACITY: Index = 1024;
    const BUFFER_SZ: Index = CAPACITY + TRAILER_LENGTH;
    const ODD_BUFFER_SZ: Index = BUFFER_SZ - 1;

    const HEAD_COUNTER_INDEX: Index = 1024 + HEAD_POSITION_OFFSET;
    const TAIL_COUNTER_INDEX: Index = 1024 + TAIL_POSITION_OFFSET;

    const MSG_TYPE_ID: Index = 101;

    struct Test {
        ab: AtomicBuffer,
        src_ab: AtomicBuffer,
        ring_buffer: ManyToOneRingBuffer,
        _buffer: AlignedBuffer,
        _src_buffer: AlignedBuffer,
    }

    impl Test {
        pub fn new() -> Self {
            let buffer = AlignedBuffer::with_capacity(BUFFER_SZ);
            let ab = AtomicBuffer::from_aligned(&buffer);

            let src_buffer = AlignedBuffer::with_capacity(BUFFER_SZ);
            let src_ab = AtomicBuffer::from_aligned(&src_buffer);

            let ring_buffer = ManyToOneRingBuffer::new(ab).unwrap();

            Self {
                ab,
                src_ab,
                ring_buffer,
                _buffer: buffer,
                _src_buffer: src_buffer,
            }
        }
    }

    #[test]
    fn ring_buffer_calculate_capacity_for_buffer() {
        let test = Test::new();
        assert_eq!(test.ab.capacity(), BUFFER_SZ);
        assert_eq!(test.ring_buffer.capacity(), BUFFER_SZ - TRAILER_LENGTH)
    }

    #[test]
    fn ring_buffer_that_capacity_ok() {
        let b = AlignedBuffer::with_capacity(1024);
        let ab = AtomicBuffer::from_aligned(&b);
        let cap = ab.capacity();
        assert_eq!(cap, 1024);

        let p = ManyToOneRingBuffer::new(ab).unwrap();
        assert_eq!(p.capacity, cap - TRAILER_LENGTH)
    }

    #[test]
    fn ring_buffer_capacity_not_power_of_two() {
        let test_buffer = AlignedBuffer::with_capacity(ODD_BUFFER_SZ);
        let ab = AtomicBuffer::from_aligned(&test_buffer);
        let ring_res = ManyToOneRingBuffer::new(ab);
        assert_eq!(
            ring_res.unwrap_err(),
            RingBufferError::CapacityIsNotTwoPower {
                capacity: ODD_BUFFER_SZ - TRAILER_LENGTH
            }
        );
    }

    #[test]
    fn ring_buffer_max_message_size_exceeded() {
        let test = Test::new();
        let size = test.ring_buffer.max_msg_len() + 1;
        let write_res = test
            .ring_buffer
            .write(AeronCommand::UnitTestMessageTypeID, test.src_ab, 0, size);

        assert_eq!(write_res.unwrap_err(), RingBufferError::MessageTooLong { msg: 129, max: 128 });
    }

    #[test]
    fn ring_buffer_that_writes_to_empty() {
        let test = Test::new();

        let tail: Index = 0;
        let tail_index: Index = 0;
        let length: Index = 8;
        let expected_record_length: Index = length + record_descriptor::HEADER_LENGTH;
        let aligned_record_length: Index = align(expected_record_length, record_descriptor::ALIGNMENT);

        test.ring_buffer
            .write(AeronCommand::UnitTestMessageTypeID, test.src_ab, 0, length)
            .unwrap();

        let record_length = test.ab.get::<i32>(record_descriptor::length_offset(tail_index));
        assert_eq!(record_length, expected_record_length);

        let msg_type = test.ab.get::<i32>(record_descriptor::type_offset(tail_index));
        assert_eq!(msg_type, MSG_TYPE_ID);

        let tail_counter = test.ab.get::<i64>(TAIL_COUNTER_INDEX);
        assert_eq!(tail_counter, (tail + aligned_record_length) as i64);
    }

    #[test]
    fn ring_buffer_should_reject_write_when_insufficient_space() {
        let test = Test::new();

        let length: Index = 100;
        let head: Index = 0;
        let tail: Index = head + (CAPACITY as Index - align(length - record_descriptor::ALIGNMENT, record_descriptor::ALIGNMENT));
        let _src_index: Index = 0;

        test.ab.put::<i64>(HEAD_COUNTER_INDEX, head as i64);
        test.ab.put::<i64>(TAIL_COUNTER_INDEX, tail as i64);

        let err = test
            .ring_buffer
            .write(AeronCommand::UnitTestMessageTypeID, test.src_ab, 0, length)
            .unwrap_err();
        assert_eq!(err, RingBufferError::InsufficientCapacity);
        assert_eq!(test.ab.get::<i64>(TAIL_COUNTER_INDEX), tail as i64);
    }

    #[test]
    fn ring_buffer_should_reject_write_when_buffer_full() {
        let test = Test::new();

        let length: Index = 8;
        let head: Index = 0;
        let tail: Index = head + CAPACITY;
        let _src_index: Index = 0;

        test.ab.put::<i64>(HEAD_COUNTER_INDEX, head as i64);
        test.ab.put::<i64>(TAIL_COUNTER_INDEX, tail as i64);

        let err = test
            .ring_buffer
            .write(AeronCommand::UnitTestMessageTypeID, test.src_ab, 0, length)
            .unwrap_err();
        assert_eq!(err, RingBufferError::InsufficientCapacity);
        assert_eq!(test.ab.get::<i64>(TAIL_COUNTER_INDEX), tail as i64);
    }

    #[test]
    fn ring_buffer_should_insert_padding_record_plus_message_on_buffer_wrap() {
        let test = Test::new();

        let length: Index = 100;
        let record_length: Index = length + record_descriptor::HEADER_LENGTH;
        let aligned_record_length: Index = align(record_length, record_descriptor::ALIGNMENT);
        let tail: Index = CAPACITY - record_descriptor::ALIGNMENT;
        let head: Index = tail - (record_descriptor::ALIGNMENT * 4);
        let _src_index: Index = 0;

        test.ab.put::<i64>(HEAD_COUNTER_INDEX, head as i64);
        test.ab.put::<i64>(TAIL_COUNTER_INDEX, tail as i64);

        test.ring_buffer
            .write(AeronCommand::UnitTestMessageTypeID, test.src_ab, 0, length)
            .unwrap();

        assert_eq!(
            test.ab.get::<i32>(record_descriptor::type_offset(tail)),
            AeronCommand::Padding as i32
        );
        assert_eq!(
            test.ab.get::<i32>(record_descriptor::length_offset(tail)),
            record_descriptor::ALIGNMENT
        );

        assert_eq!(test.ab.get::<i32>(record_descriptor::length_offset(0)), record_length);
        assert_eq!(test.ab.get::<i32>(record_descriptor::type_offset(0)), MSG_TYPE_ID);
        assert_eq!(
            test.ab.get::<i64>(TAIL_COUNTER_INDEX),
            (tail + aligned_record_length + record_descriptor::ALIGNMENT) as i64
        );
    }

    #[test]
    fn ring_buffer_should_insert_padding_record_plus_message_on_buffer_wrap_with_head_equal_to_tail() {
        let test = Test::new();

        let length: Index = 100;
        let record_length: Index = length + record_descriptor::HEADER_LENGTH;
        let aligned_record_length: Index = align(record_length, record_descriptor::ALIGNMENT);
        let tail: Index = CAPACITY - record_descriptor::ALIGNMENT;
        let head: Index = tail;
        let _src_index: Index = 0;

        test.ab.put::<i64>(HEAD_COUNTER_INDEX, head as i64);
        test.ab.put::<i64>(TAIL_COUNTER_INDEX, tail as i64);

        test.ring_buffer
            .write(AeronCommand::UnitTestMessageTypeID, test.src_ab, 0, length)
            .unwrap();

        assert_eq!(
            test.ab.get::<i32>(record_descriptor::type_offset(tail)),
            AeronCommand::Padding as i32
        );
        assert_eq!(
            test.ab.get::<i32>(record_descriptor::length_offset(tail)),
            record_descriptor::ALIGNMENT
        );

        assert_eq!(test.ab.get::<i32>(record_descriptor::length_offset(0)), record_length);
        assert_eq!(test.ab.get::<i32>(record_descriptor::type_offset(0)), MSG_TYPE_ID);
        assert_eq!(
            test.ab.get::<i64>(TAIL_COUNTER_INDEX),
            (tail + aligned_record_length + record_descriptor::ALIGNMENT) as i64
        );
    }

    #[test]
    fn ring_buffer_should_read_nothing_from_empty_buffer() {
        let test = Test::new();

        let tail: Index = 0;
        let head: Index = 0;

        test.ab.put::<i64>(HEAD_COUNTER_INDEX, head as i64);
        test.ab.put::<i64>(TAIL_COUNTER_INDEX, tail as i64);

        let mut times_called = 0;
        let handler = |_command, _buffer| times_called += 1;

        let messages_read = test.ring_buffer.read(handler, 1);

        assert_eq!(messages_read, 0);
        assert_eq!(times_called, 0);
    }

    #[test]
    fn ring_buffer_should_read_single_message() {
        let test = Test::new();

        let length: Index = 8;
        let head: Index = 0;
        let record_length: Index = length + record_descriptor::HEADER_LENGTH;
        let aligned_record_length: Index = align(record_length, record_descriptor::ALIGNMENT);
        let tail: Index = aligned_record_length;

        test.ab.put::<i64>(HEAD_COUNTER_INDEX, head as i64);
        test.ab.put::<i64>(TAIL_COUNTER_INDEX, tail as i64);

        test.ab.put::<i32>(record_descriptor::type_offset(0), MSG_TYPE_ID);
        test.ab.put::<i32>(record_descriptor::length_offset(0), record_length);

        let mut times_called = 0;
        let handler = |_command, _buffer| times_called += 1;

        let messages_read = test.ring_buffer.read(handler, 1);

        assert_eq!(messages_read, 1);
        assert_eq!(times_called, 1);

        assert_eq!(test.ab.get::<i64>(HEAD_COUNTER_INDEX), (head + aligned_record_length) as i64);

        for i in (0..record_descriptor::ALIGNMENT).step_by(4) {
            assert_eq!(test.ab.get::<i32>(i), 0);
        }
    }

    #[test]
    fn ring_buffer_should_not_read_single_message_part_way_through_writing() {
        let test = Test::new();

        let length: Index = 8;
        let head: Index = 0;
        let record_length: Index = length + record_descriptor::HEADER_LENGTH;
        let aligned_record_length: Index = align(record_length, record_descriptor::ALIGNMENT);
        let end_tail: Index = aligned_record_length;

        test.ab.put::<i64>(TAIL_COUNTER_INDEX, end_tail as i64);
        test.ab.put::<i32>(record_descriptor::type_offset(0), MSG_TYPE_ID);
        test.ab.put::<i32>(record_descriptor::length_offset(0), -record_length);

        let mut times_called = 0;
        let handler = |_command, _buffer| times_called += 1;

        let messages_read = test.ring_buffer.read(handler, 1);

        assert_eq!(messages_read, 0);
        assert_eq!(times_called, 0);

        assert_eq!(test.ab.get::<i64>(HEAD_COUNTER_INDEX), head as i64);
    }

    #[test]
    fn ring_buffer_should_read_two_messages() {
        let test = Test::new();

        let length: Index = 8;
        let head: Index = 0;
        let record_length: Index = length + record_descriptor::HEADER_LENGTH;
        let aligned_record_length: Index = align(record_length, record_descriptor::ALIGNMENT);
        let tail: Index = aligned_record_length * 2;

        test.ab.put::<i64>(HEAD_COUNTER_INDEX, head as i64);
        test.ab.put::<i64>(TAIL_COUNTER_INDEX, tail as i64);

        test.ab.put::<i32>(record_descriptor::type_offset(0), MSG_TYPE_ID);
        test.ab.put::<i32>(record_descriptor::length_offset(0), record_length);

        test.ab
            .put::<i32>(record_descriptor::type_offset(aligned_record_length), MSG_TYPE_ID);
        test.ab
            .put::<i32>(record_descriptor::length_offset(aligned_record_length), record_length);

        let mut times_called = 0;
        let handler = |_command, _buffer| times_called += 1;

        let messages_read = test.ring_buffer.read(handler, 2);

        assert_eq!(messages_read, 2);
        assert_eq!(times_called, 2);

        assert_eq!(
            test.ab.get::<i64>(HEAD_COUNTER_INDEX),
            (head + aligned_record_length * 2) as i64
        );

        for i in (0..record_descriptor::ALIGNMENT).step_by(4) {
            assert_eq!(test.ab.get::<i32>(i), 0);
        }
    }

    #[test]
    fn ring_buffer_should_limit_read_of_messages() {
        let test = Test::new();

        let length: Index = 8;
        let head: Index = 0;
        let record_length: Index = length + record_descriptor::HEADER_LENGTH;
        let aligned_record_length: Index = align(record_length, record_descriptor::ALIGNMENT);
        let tail: Index = aligned_record_length * 2;

        test.ab.put::<i64>(HEAD_COUNTER_INDEX, head as i64);
        test.ab.put::<i64>(TAIL_COUNTER_INDEX, tail as i64);

        test.ab.put::<i32>(record_descriptor::type_offset(0), MSG_TYPE_ID);
        test.ab.put::<i32>(record_descriptor::length_offset(0), record_length);

        test.ab
            .put::<i32>(record_descriptor::type_offset(aligned_record_length), MSG_TYPE_ID);
        test.ab
            .put::<i32>(record_descriptor::length_offset(aligned_record_length), record_length);

        let mut times_called = 0;
        let handler = |_command, _buffer| times_called += 1;

        let messages_read = test.ring_buffer.read(handler, 1);

        assert_eq!(messages_read, 1);
        assert_eq!(times_called, 1);

        assert_eq!(test.ab.get::<i64>(HEAD_COUNTER_INDEX), (head + aligned_record_length) as i64);

        for i in (0..record_descriptor::ALIGNMENT).step_by(4) {
            assert_eq!(test.ab.get::<i32>(i), 0);
        }
        assert_eq!(
            test.ab.get::<i32>(record_descriptor::length_offset(aligned_record_length)),
            record_length
        );
    }

    #[test]
    fn ring_buffer_should_cope_with_exception_from_handler() {
        let test = Test::new();

        let length: Index = 8;
        let head: Index = 0;
        let record_length: Index = length + record_descriptor::HEADER_LENGTH;
        let aligned_record_length: Index = align(record_length, record_descriptor::ALIGNMENT);
        let tail: Index = aligned_record_length * 2;

        test.ab.put::<i64>(HEAD_COUNTER_INDEX, head as i64);
        test.ab.put::<i64>(TAIL_COUNTER_INDEX, tail as i64);

        test.ab.put::<i32>(record_descriptor::type_offset(0), MSG_TYPE_ID);
        test.ab.put::<i32>(record_descriptor::length_offset(0), record_length);

        test.ab
            .put::<i32>(record_descriptor::type_offset(aligned_record_length), MSG_TYPE_ID);
        test.ab
            .put::<i32>(record_descriptor::length_offset(aligned_record_length), record_length);

        let mut times_called = 0;

        let mut exception_threw: bool = false;

        let handler = |_command, _buffer| {
            times_called += 1;
            if times_called == 2 {
                exception_threw = true;
            }
        };

        test.ring_buffer.read(handler, 2);

        assert_eq!(times_called, 2);
        assert!(exception_threw);

        assert_eq!(
            test.ab.get::<i64>(HEAD_COUNTER_INDEX),
            (head + aligned_record_length * 2) as i64
        );

        for i in (0..record_descriptor::ALIGNMENT * 2).step_by(4) {
            assert_eq!(test.ab.get::<i32>(i), 0);
        }
    }

    #[test]
    fn ring_buffer_should_not_unblock_when_empty() {
        let test = Test::new();

        let tail: Index = record_descriptor::ALIGNMENT * 4;
        let head: Index = tail;

        test.ab.put::<i64>(TAIL_COUNTER_INDEX, tail as i64);
        test.ab.put::<i64>(HEAD_COUNTER_INDEX, head as i64);

        assert!(!test.ring_buffer.unblock());
    }

    #[test]
    fn ring_buffer_should_unblock_message_with_header() {
        let test = Test::new();

        let message_length: Index = record_descriptor::ALIGNMENT * 4;
        let head: Index = message_length;
        let tail: Index = message_length * 2;

        test.ab.put::<i64>(HEAD_COUNTER_INDEX, head as i64);
        test.ab.put::<i64>(TAIL_COUNTER_INDEX, tail as i64);

        test.ab.put::<i32>(record_descriptor::type_offset(head), MSG_TYPE_ID);
        test.ab.put::<i32>(record_descriptor::length_offset(head), -message_length);

        assert!(test.ring_buffer.unblock());

        assert_eq!(
            test.ab.get::<i32>(record_descriptor::type_offset(head)),
            AeronCommand::Padding as i32
        );
        assert_eq!(test.ab.get::<i32>(record_descriptor::length_offset(head)), message_length);

        assert_eq!(test.ab.get::<i64>(HEAD_COUNTER_INDEX), message_length as i64);
        assert_eq!(test.ab.get::<i64>(TAIL_COUNTER_INDEX), (message_length * 2) as i64);
    }

    #[test]
    fn ring_buffer_should_unblock_gap_with_zeros() {
        let test = Test::new();

        let message_length: Index = record_descriptor::ALIGNMENT * 4;
        let head: Index = message_length;
        let tail: Index = message_length * 3;

        test.ab.put::<i64>(HEAD_COUNTER_INDEX, head as i64);
        test.ab.put::<i64>(TAIL_COUNTER_INDEX, tail as i64);

        test.ab
            .put::<i32>(record_descriptor::length_offset(message_length * 2), message_length);

        assert!(test.ring_buffer.unblock());

        assert_eq!(
            test.ab.get::<i32>(record_descriptor::type_offset(head)),
            AeronCommand::Padding as i32
        );
        assert_eq!(test.ab.get::<i32>(record_descriptor::length_offset(head)), message_length);

        assert_eq!(test.ab.get::<i64>(HEAD_COUNTER_INDEX), message_length as i64);
        assert_eq!(test.ab.get::<i64>(TAIL_COUNTER_INDEX), (message_length * 3) as i64);
    }

    #[test]
    fn ring_buffer_should_not_unblock_gap_with_message_race_on_second_message_increasing_tail_then_interrupting() {
        let test = Test::new();

        let message_length: Index = record_descriptor::ALIGNMENT * 4;
        let head = message_length;
        let tail = message_length * 3;

        test.ab.put_ordered::<i64>(HEAD_COUNTER_INDEX, head as i64);
        test.ab.put_ordered::<i64>(TAIL_COUNTER_INDEX, tail as i64);

        test.ab.put_ordered::<i32>(message_length * 2, 0);

        assert!(!test.ring_buffer.unblock());
    }

    // Can't be implemented without Mock framework
    //#[test]
    //fn ring_buffer_should_not_unblock_gap_with_message_race_when_scan_forward_takes_an_interrupt() {
    //}

    const NUM_IDS_PER_THREAD: i64 = 10 * 1000 * 1000;
    const NUM_PUBLISHERS: i64 = 2;

    lazy_static! {
        pub static ref PUB_COUNT_DOWN1: AtomicI64 = AtomicI64::new(NUM_PUBLISHERS);
    }

    #[test]
    fn ring_buffer_should_provide_correlation_ids() {
        let mpsc_buffer = AlignedBuffer::with_capacity(BUFFER_SZ);
        let mpsc_ab = AtomicBuffer::from_aligned(&mpsc_buffer);
        mpsc_ab.set_memory(0, mpsc_ab.capacity(), 0);

        let ring_buffer = Arc::new(ManyToOneRingBuffer::new(mpsc_ab).unwrap());

        let mut threads: Vec<std::thread::JoinHandle<()>> = Vec::new();

        for _i in 0..NUM_PUBLISHERS {
            let ring_buffer = ring_buffer.clone();
            threads.push(std::thread::spawn(move || {
                PUB_COUNT_DOWN1.fetch_sub(1, Ordering::SeqCst);
                while PUB_COUNT_DOWN1.load(Ordering::SeqCst) > 0 {
                    std::thread::yield_now();
                }

                for _i in 0..NUM_IDS_PER_THREAD {
                    ring_buffer.next_correlation_id();
                }
            }));
        }

        threads.into_iter().for_each(|thread| thread.join().unwrap());
        assert_eq!(ring_buffer.next_correlation_id(), NUM_IDS_PER_THREAD * NUM_PUBLISHERS);
    }

    lazy_static! {
        pub static ref PUB_COUNT_DOWN2: AtomicI64 = AtomicI64::new(NUM_PUBLISHERS);
        pub static ref PUBLISHER_ID: AtomicI32 = AtomicI32::new(0);
        pub static ref COUNTS: Arc<Mutex<[i32; NUM_PUBLISHERS as usize]>> = Arc::new(Mutex::new([0; NUM_PUBLISHERS as usize]));
    }

    const NUM_MESSAGES_PER_PUBLISHER: i64 = 10000; // * 1000;

    #[test]
    fn ring_buffer_should_exchange_messages() {
        let mpsc_buffer = AlignedBuffer::with_capacity(BUFFER_SZ);
        let mpsc_ab = AtomicBuffer::from_aligned(&mpsc_buffer);
        mpsc_ab.set_memory(0, mpsc_ab.capacity(), 0);

        let ring_buffer = Arc::new(ManyToOneRingBuffer::new(mpsc_ab).unwrap());

        let mut threads: Vec<std::thread::JoinHandle<()>> = Vec::new();

        for _i in 0..NUM_PUBLISHERS {
            let ring_buffer = ring_buffer.clone();
            threads.push(std::thread::spawn(move || {
                let src_buf = AlignedBuffer::with_capacity(BUFFER_SZ);
                let src_ab = AtomicBuffer::from_aligned(&src_buf);
                src_ab.set_memory(0, src_ab.capacity(), 0);

                let id = PUBLISHER_ID.fetch_add(1, Ordering::SeqCst);

                PUB_COUNT_DOWN2.fetch_sub(1, Ordering::SeqCst);
                while PUB_COUNT_DOWN2.load(Ordering::SeqCst) > 0 {
                    std::thread::yield_now();
                }

                let message_length = 4 + 4;
                let message_num_offset = 4;

                src_ab.put::<i32>(0, id);

                // Put here more messages then buffer can hold. Reader should read it so all messages
                // will be published after some time.
                for i in 0..NUM_MESSAGES_PER_PUBLISHER {
                    src_ab.put::<i32>(message_num_offset, i as i32);
                    while ring_buffer
                        .write(AeronCommand::UnitTestMessageTypeID, src_ab, 0, message_length)
                        .is_err()
                    {
                        std::thread::yield_now();
                    }
                }
            }));
        }

        let mut msg_count = 0;

        let handler = |command: AeronCommand, buffer: AtomicBuffer| {
            let id: usize = buffer.get::<i32>(0) as usize;
            let message_number = buffer.get::<i32>(4);

            assert_eq!(buffer.capacity(), 4 + 4);
            assert_eq!(command, AeronCommand::UnitTestMessageTypeID);

            let mut counts = COUNTS.lock().unwrap();
            assert_eq!(counts[id], message_number);
            counts[id] += 1;
        };

        while msg_count < NUM_MESSAGES_PER_PUBLISHER * NUM_PUBLISHERS {
            let read_count = ring_buffer.read(handler, (NUM_MESSAGES_PER_PUBLISHER * NUM_PUBLISHERS) as i32);

            if 0 == read_count {
                std::thread::sleep(Duration::from_millis(1));
            }

            msg_count += read_count as i64;
        }

        threads.into_iter().for_each(|thread| thread.join().unwrap());
    }

    #[test]
    fn ring_buffer_header_construction() {
        let header = make_header(111, AeronCommand::RemoveCounter);

        assert_eq!(record_length(header), 111);
        assert_eq!(message_type_id(header), AeronCommand::RemoveCounter as i32);
        assert_eq!(message_type(header), AeronCommand::RemoveCounter);

        let header = make_header(222_111, AeronCommand::AddCounter);

        assert_eq!(record_length(header), 222_111);
        assert_eq!(message_type_id(header), AeronCommand::AddCounter as i32);
        assert_eq!(message_type(header), AeronCommand::AddCounter);
    }
    /*
       pub fn make_header(len: Index, command: AeronCommand) -> i64 {
        // high 32 bits are from `command`, low 32 bits are from `len`
        (((command as i64) & 0xFFFF_FFFF) << 32) | ((len as i64) & 0xFFFF_FFFF)
    }

    #[inline]
    pub fn record_length(header: i64) -> Index {
        // explicitly cut off the higher 32 bits
        (header & 0xFFFF_FFFF) as Index
    }

    #[inline]
    pub fn message_type_id(header: i64) -> i32 {
        (header >> 32) as i32
    }

    #[inline]
    pub fn message_type(header: i64) -> AeronCommand {
        let type_id = message_type_id(header);
        AeronCommand::from_command_id(type_id)
    }

    */
}
