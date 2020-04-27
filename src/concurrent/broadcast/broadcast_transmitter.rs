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
    command::control_protocol_events::AeronCommand,
    concurrent::{
        atomic_buffer::AtomicBuffer,
        atomics,
        broadcast::{broadcast_buffer_descriptor, record_descriptor, BroadcastTransmitError},
    },
    utils::{bit_utils::align, types::Index},
};

#[derive(Debug)]
pub struct BroadcastTransmitter {
    buffer: AtomicBuffer,
    capacity: Index,
    mask: Index,
    max_msg_length: Index,
    tail_intent_counter_index: Index,
    tail_counter_index: Index,
    latest_counter_index: Index,
}

impl BroadcastTransmitter {
    pub fn new(buffer: AtomicBuffer) -> Result<Self, BroadcastTransmitError> {
        let capacity = buffer.capacity() - broadcast_buffer_descriptor::TRAILER_LENGTH;
        {
            broadcast_buffer_descriptor::check_capacity(capacity)?;
        }

        Ok(Self {
            buffer,
            capacity,
            mask: capacity - 1,
            max_msg_length: record_descriptor::calculate_max_message_length(capacity),
            tail_intent_counter_index: capacity + broadcast_buffer_descriptor::TAIL_INTENT_COUNTER_OFFSET,
            tail_counter_index: capacity + broadcast_buffer_descriptor::TAIL_COUNTER_OFFSET,
            latest_counter_index: capacity + broadcast_buffer_descriptor::LATEST_COUNTER_OFFSET,
        })
    }

    pub fn capacity(&self) -> Index {
        self.capacity
    }

    pub fn max_msg_length(&self) -> Index {
        self.max_msg_length
    }

    pub fn transmit(
        &mut self,
        msg_type_id: i32,
        src_buffer: &AtomicBuffer,
        src_index: Index,
        length: Index,
    ) -> Result<(), BroadcastTransmitError> {
        record_descriptor::check_msg_type_id(msg_type_id)?;
        self.check_message_length(length)?;

        let mut current_tail = self.buffer.get::<i64>(self.tail_counter_index);
        let mut record_offset = (current_tail & self.mask as i64) as Index; //зачем тут маска, если он всегда равен выравниванию

        let record_length: Index = length + record_descriptor::HEADER_LENGTH;
        let aligned_record_length: Index = align(record_length, record_descriptor::RECORD_ALIGNMENT);
        let new_tail: i64 = current_tail + aligned_record_length as i64;
        let to_end_of_buffer: Index = self.capacity - record_offset;

        if to_end_of_buffer < aligned_record_length {
            self.signal_tail_intent(new_tail + to_end_of_buffer as i64);

            self.insert_padding_record(record_offset, to_end_of_buffer);

            current_tail += to_end_of_buffer as i64;
            record_offset = 0;
        } else {
            self.signal_tail_intent(new_tail);
        }

        self.buffer
            .put::<i32>(record_descriptor::length_offset(record_offset), record_length);
        self.buffer
            .put::<i32>(record_descriptor::type_offset(record_offset), msg_type_id);

        self.buffer
            .copy_from(record_descriptor::msg_offset(record_offset), src_buffer, src_index, length);

        self.buffer.put::<i64>(self.latest_counter_index, current_tail);
        self.buffer
            .put_ordered::<i64>(self.tail_counter_index, current_tail + aligned_record_length as i64);

        Ok(())
    }

    //private part

    fn check_message_length(&self, length: Index) -> Result<(), BroadcastTransmitError> {
        if length > self.max_msg_length {
            return Err(BroadcastTransmitError::EncodedMessageExceedsMaxMsgLength {
                max_msg_length: self.max_msg_length,
                length,
            });
        }
        Ok(())
    }

    fn signal_tail_intent(&mut self, new_tail: i64) {
        self.buffer.put_ordered::<i64>(self.tail_intent_counter_index, new_tail);
        atomics::release();
    }

    fn insert_padding_record(&mut self, record_offset: Index, length: Index) {
        self.buffer
            .put::<i32>(record_descriptor::length_offset(record_offset), length);
        self.buffer
            .put::<i32>(record_descriptor::type_offset(record_offset), AeronCommand::Padding as i32);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::concurrent::atomic_buffer::AlignedBuffer;

    const CAPACITY: Index = 1024;
    const MSG_TYPE_ID: i32 = 7;

    struct BroadcastTransmitterTest {
        buffer: AtomicBuffer,
        message_buffer_owner: Vec<AlignedBuffer>,
    }

    impl BroadcastTransmitterTest {
        fn new(capacity: Index) -> Self {
            let (buffer, owner) = Self::sized_buffer_with_trailed(capacity);

            Self {
                buffer,
                message_buffer_owner: vec![owner],
            }
        }

        fn sized_buffer(capacity: Index) -> (AtomicBuffer, AlignedBuffer) {
            let owner = AlignedBuffer::with_capacity(capacity);
            (AtomicBuffer::from_aligned(&owner), owner)
        }

        fn sized_buffer_with_trailed(capacity: Index) -> (AtomicBuffer, AlignedBuffer) {
            Self::sized_buffer(capacity + 128)
        }

        fn create_transmitter(&self) -> BroadcastTransmitter {
            self.try_create_transmitter().unwrap()
        }

        fn try_create_transmitter(&self) -> Result<BroadcastTransmitter, BroadcastTransmitError> {
            BroadcastTransmitter::new(self.buffer)
        }

        fn create_message_buffer(&mut self, capacity: Index) -> AtomicBuffer {
            let (buffer, owner) = Self::sized_buffer(capacity);
            self.message_buffer_owner.push(owner);
            buffer
        }
    }

    #[inline]
    #[allow(dead_code)]
    fn sized_buffer_filled_with_range(capacity: Index) -> (AtomicBuffer, Vec<u8>) {
        assert!(capacity < 255);

        let _aligned_buffer = AlignedBuffer::with_capacity(capacity);

        let mut data: Vec<u8> = (0u8..(capacity as u8)).collect();
        (AtomicBuffer::new(data.as_mut_ptr(), capacity as Index), data)
    }

    #[test]
    fn should_calculate_capacity_for_buffer() {
        let test = BroadcastTransmitterTest::new(1024);
        let transmitter = test.create_transmitter();

        assert_eq!(transmitter.capacity(), CAPACITY);
    }

    #[test]
    fn should_throw_exception_for_capacity_that_is_not_power_of_two() {
        let test = BroadcastTransmitterTest::new(777);
        let transmitter = test.try_create_transmitter();

        assert_eq!(transmitter.unwrap_err(), BroadcastTransmitError::NotPowerOfTwo(777));
    }

    #[test]
    fn should_throw_exception_when_max_message_length_exceeded() {
        let mut test = BroadcastTransmitterTest::new(16);
        let mut transmitter = test.create_transmitter();

        let src_buffer = test.create_message_buffer(16);
        assert_eq!(
            transmitter
                .transmit(MSG_TYPE_ID, &src_buffer, 0, transmitter.max_msg_length() + 1)
                .unwrap_err(),
            BroadcastTransmitError::EncodedMessageExceedsMaxMsgLength {
                max_msg_length: 2,
                length: 3,
            }
        );
    }

    #[test]
    fn should_throw_exception_when_message_type_id_invalid() {
        let mut test = BroadcastTransmitterTest::new(16);
        let mut transmitter = test.create_transmitter();
        let src_buffer = test.create_message_buffer(16);

        const INVALID_MSG_TYPE_ID: i32 = -1;

        let err = transmitter.transmit(INVALID_MSG_TYPE_ID, &src_buffer, 0, 32).unwrap_err();
        assert_eq!(
            err,
            BroadcastTransmitError::MessageIdShouldBeGreaterThenZero(INVALID_MSG_TYPE_ID)
        )
    }

    #[test]
    fn should_transmit_into_empty_buffer() {
        const LENGTH: Index = 8;
        const RECORD_LENGTH: Index = LENGTH + record_descriptor::HEADER_LENGTH;
        let _aligned_record_length: Index = align(RECORD_LENGTH, record_descriptor::RECORD_ALIGNMENT);
        const SRC_INDEX: Index = 0;

        let mut test = BroadcastTransmitterTest::new(64);
        let mut transmitter = test.create_transmitter();
        let src_buffer = test.create_message_buffer(LENGTH as Index);

        src_buffer.put_bytes(0, &[0, 1, 2, 3, 4, 5, 6, 7]);

        //act
        transmitter.transmit(MSG_TYPE_ID, &src_buffer, SRC_INDEX, LENGTH).unwrap();

        //assert
        dbg!(test.buffer);
        assert_eq!(
            test.buffer.as_sub_slice(0, 16),
            &[16, 0, 0, 0, 7, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7]
        )
        //todo assert trail
    }

    #[test]
    fn should_transmit_into_used_buffer() {
        const LENGTH: Index = 8;
        const RECORD_LENGTH: Index = LENGTH + record_descriptor::HEADER_LENGTH;
        let _aligned_record_length: Index = align(RECORD_LENGTH, record_descriptor::RECORD_ALIGNMENT);
        const SRC_INDEX: Index = 0;

        let mut test = BroadcastTransmitterTest::new(64);
        let mut transmitter = test.create_transmitter();
        let src_buffer = test.create_message_buffer(16);

        src_buffer.put_bytes(0, &[0, 1, 2, 3, 4, 5, 6, 7, 7, 6, 5, 4, 3, 2, 1, 0]);

        //act
        transmitter.transmit(7, &src_buffer, SRC_INDEX, LENGTH).unwrap();
        transmitter.transmit(1024, &src_buffer, 8, LENGTH).unwrap();

        //assert
        dbg!(test.buffer);
        assert_eq!(
            test.buffer.as_sub_slice(0, 32),
            &[
                16, 0, 0, 0, /*type    7*/ 7, 0, 0, 0, /*msg*/ 0, 1, 2, 3, 4, 5, 6, 7, 16, 0, 0, 0,
                /*type 1024*/ 0, 4, 0, 0, /*msg*/ 7, 6, 5, 4, 3, 2, 1, 0,
            ]
        )

        //todo assert trail
    }

    #[test]
    fn should_transmit_into_end_of_buffer() {
        let mut test = BroadcastTransmitterTest::new(CAPACITY);

        let src_buffer = test.create_message_buffer(CAPACITY);

        const LENGTH: Index = 1000;
        const RECORD_LENGTH: Index = LENGTH + record_descriptor::HEADER_LENGTH;

        let aligned_record_length: Index = align(RECORD_LENGTH, record_descriptor::RECORD_ALIGNMENT);

        let tail = (CAPACITY - aligned_record_length) as i64;

        let _record_offset = tail as Index;

        let mut transmitter = test.create_transmitter();

        src_buffer.put_bytes(0, &vec![42; LENGTH as usize]);

        for i in 0..8 {
            let index = i * 120;
            transmitter.transmit(MSG_TYPE_ID, &src_buffer, index, 120).unwrap();
        }

        src_buffer.put_bytes(LENGTH, &[1; 20]);

        transmitter.transmit(MSG_TYPE_ID, &src_buffer, LENGTH, 16).unwrap();
        dbg!(test.buffer);
    }
}
