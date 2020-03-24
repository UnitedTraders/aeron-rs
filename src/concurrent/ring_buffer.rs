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

use std::fmt;

use crate::command::control_protocol_events::AeronCommand;
use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::utils::{
    bit_utils::{align, is_power_of_two},
    misc::CACHE_LINE_LENGTH,
    types::Index,
};

// The read handler function signature
//trait HandlerFn: Fn(i32, &AtomicBuffer, Index, Index) {}

//todo: rewrite all these index-based accessors using blitted structs + custom volatile cells?
pub const TAIL_POSITION_OFFSET: Index = CACHE_LINE_LENGTH * 2;
pub const HEAD_CACHE_POSITION_OFFSET: Index = CACHE_LINE_LENGTH * 4;
pub const HEAD_POSITION_OFFSET: Index = CACHE_LINE_LENGTH * 6;
pub const CORRELATION_COUNTER_OFFSET: Index = CACHE_LINE_LENGTH * 8;
pub const CONSUMER_HEARTBEAT_OFFSET: Index = CACHE_LINE_LENGTH * 10;

// Total length of the trailer in bytes
pub const TRAILER_LENGTH: Index = CACHE_LINE_LENGTH * 12;

fn check_capacity(capacity: Index) -> Result<(), Error> {
    if is_power_of_two(capacity) {
        Ok(())
    } else {
        Err(Error::CapacityIsNotTwoPower { capacity })
    }
}

mod record_descriptor {
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
    use super::Error;
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
    pub fn check_msg_type_id(msg_type_id: i32) -> Result<(), Error> {
        if msg_type_id < 1 {
            return Err(Error::NonPositiveMessageTypeId(msg_type_id));
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Error {
    InsufficientCapacity,
    MessageTooLong { msg: Index, max: Index },
    NonPositiveMessageTypeId(i32),
    CapacityIsNotTwoPower { capacity: Index },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            Error::InsufficientCapacity => "Insufficient capacity".into(),
            Error::MessageTooLong { msg, max } => format!("Encoded message exceeds maxMsgLength of {}: length={}", max, msg),
            Error::NonPositiveMessageTypeId(type_id) => {
                format!("Message type id must be greater than zero, msgTypeId={}", type_id)
            }
            Error::CapacityIsNotTwoPower { capacity } => format!(
                "Capacity must be a positive power of 2 + TRAILER_LENGTH: capacity={}",
                capacity
            ),
        };

        write!(f, "{}", msg)
    }
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
    pub fn new(buffer: AtomicBuffer) -> Result<Self, Error> {
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

    // todo: replace src+start/len with a single struct blitted over a buffer?
    pub fn write(&self, cmd: AeronCommand, src: &[u8], length: Index) -> Result<(), Error> {
        // record_descriptor::check_msg_type_id()?;
        self.check_msg_length(length)?;

        let record_len = length + record_descriptor::HEADER_LENGTH;
        let required_capacity = align(record_len, record_descriptor::ALIGNMENT);
        // once we claim the required capacity we can write without conflicts
        let record_index = self.claim(required_capacity)?;

        self.buffer
            .put_ordered(record_index, record_descriptor::make_header(-record_len, cmd));

        self.buffer
            .put_bytes(record_descriptor::encoded_msg_offset(record_index), src);

        self.buffer
            .put_ordered(record_descriptor::length_offset(record_index), record_len);

        Ok(())
    }

    /// Read from the ring buffer until either wrap-around or `msg_count_max` messages have been
    /// processed.
    /// Returns the number of messages processed.
    pub fn read<F: FnMut(AeronCommand, AtomicBuffer)>(&self, mut handler: F, msg_count_limit: i32) -> i32 {
        let head: i64 = self.buffer.get(self.head_position); // non - volatile read?
        let head_index = head as Index & (self.capacity - 1);
        let contiguous_block_len = self.capacity - head_index;

        let mut messages_read = 0;
        let mut bytes_read = 0;

        while bytes_read < contiguous_block_len && messages_read < msg_count_limit {
            let record_index = head_index + bytes_read;
            let header: i64 = self.buffer.get_volatile(record_index);
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
            // zero-out memory and advance the reader
            self.buffer.set_memory(head_index, bytes_read, 0);
            self.buffer.put_ordered(self.head_position, head + bytes_read as i64)
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
        self.buffer.get_volatile(self.consumer_heartbeat)
    }

    #[inline]
    pub fn producer_position(&self) -> i64 {
        self.buffer.get_volatile(self.tail_position)
    }

    #[inline]
    pub fn consumer_position(&self) -> i64 {
        self.buffer.get_volatile(self.head_position)
    }

    #[inline]
    pub fn size(&self) -> Index {
        let mut tail: i64;
        let mut head_after = self.consumer_position();

        loop {
            let head_before = head_after;
            tail = self.producer_position();
            head_after = self.consumer_position();

            if head_after == head_before {
                return (tail - head_after) as Index;
            }
        }
    }

    #[allow(clippy::comparison_chain)]
    pub fn unblock(&self) -> bool {
        let head_position = self.consumer_position();
        let tail_position = self.producer_position();

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

    fn claim(&self, required_capacity: Index) -> Result<Index, Error> {
        let mask = (self.capacity - 1) as i64;
        let mut head: i64 = self.buffer.get_volatile(self.head_cache_position);

        let (padding, tail_index) = loop {
            let tail: i64 = self.producer_position();
            let available_capacity = self.capacity - (tail - head) as Index;

            if required_capacity > available_capacity {
                head = self.consumer_position();
                if required_capacity > (self.capacity - (tail - head) as Index) {
                    return Err(Error::InsufficientCapacity);
                }
                self.buffer.put_ordered(self.head_cache_position, head);
            }

            let mut padding = 0;
            let tail_index = (tail & mask) as Index;
            let len_to_buffer_end = self.capacity - tail_index;

            if required_capacity > len_to_buffer_end {
                let mut head_index = (head & mask) as Index;
                if required_capacity > head_index {
                    head = self.consumer_position();
                    head_index = (head & mask) as Index;
                    if required_capacity > head_index {
                        return Err(Error::InsufficientCapacity);
                    }
                    self.buffer.put_ordered(self.head_cache_position, head)
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
                .put_ordered(tail_index, record_descriptor::make_header(padding, AeronCommand::Padding));
            Ok(0)
        } else {
            Ok(tail_index)
        }
    }

    fn check_msg_length(&self, length: Index) -> Result<(), Error> {
        if length > self.max_msg_len {
            return Err(Error::MessageTooLong {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        concurrent::atomic_buffer::{AlignedBuffer, AtomicBuffer},
        utils::{bit_utils::align, types::Index},
    };

    const CAPACITY: Index = 1024;
    const BUFFER_SZ: Index = CAPACITY + TRAILER_LENGTH;
    const ODD_BUFFER_SZ: Index = BUFFER_SZ - 1;

    const HEAD_COUNTER_INDEX: Index = 1024 + HEAD_POSITION_OFFSET;
    const TAIL_COUNTER_INDEX: Index = 1024 + TAIL_POSITION_OFFSET;

    struct Test {
        ab: AtomicBuffer,
        src_ab: AtomicBuffer,
        ring_buffer: ManyToOneRingBuffer,
        buffer: AlignedBuffer,
        src_buffer: AlignedBuffer,
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
                buffer,
                src_buffer,
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
            Error::CapacityIsNotTwoPower {
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
            .write(AeronCommand::UnitTestMessageTypeID, test.src_ab.as_sub_slice(0, size), size);

        assert_eq!(write_res.unwrap_err(), Error::MessageTooLong { msg: 129, max: 128 });
    }

    #[test]
    fn ring_buffer_that_writes_to_empty() {
        let test = Test::new();

        let src = [1, 2, 3];
        test.ring_buffer.write(AeronCommand::UnitTestMessageTypeID, &src, 3).unwrap();

        let tail_index = 0;
        let record_length: Index = test.ab.get(record_descriptor::length_offset(tail_index));
        assert_eq!(record_length, record_descriptor::HEADER_LENGTH + src.len() as Index);

        // let msg_type: Index = test.ab.get(record_descriptor::type_offset(tail_index));
        // assert_eq!(msg_type, PADDING_MSG_TYPE_ID);

        let aligned_record_length = align(record_length, record_descriptor::ALIGNMENT);
        let msg_type: Index = test.ab.get(TAIL_COUNTER_INDEX);
        assert_eq!(msg_type, tail_index + aligned_record_length);
    }

    #[test]
    fn ring_buffer_should_reject_write_when_insufficient_space() {
        // TODO
        let mut test = Test::new();

        let length: Index = 100;
        let head: Index = 0;
        let tail: Index = head + (CAPACITY as Index - align(length - record_descriptor::ALIGNMENT, record_descriptor::ALIGNMENT));
        let _src_index: Index = 0;

        test.ab.put(HEAD_COUNTER_INDEX, head);
        test.ab.put(TAIL_COUNTER_INDEX, tail);

        let slice = test.src_ab.as_mutable_slice();

        let err = test
            .ring_buffer
            .write(AeronCommand::ClientKeepAlive, slice, slice.len() as Index)
            .unwrap_err();
        assert_eq!(err, Error::MessageTooLong { msg: 1792, max: 128 });
        assert_eq!(test.ab.get::<i64>(TAIL_COUNTER_INDEX), tail as i64);
    }
}
