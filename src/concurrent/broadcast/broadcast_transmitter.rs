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
use crate::concurrent::atomics;
use crate::concurrent::broadcast::record_descriptor;
use crate::concurrent::broadcast::{broadcast_buffer_descriptor, BroadcastTransmitError};
use crate::utils::bit_utils::align;
use crate::utils::types::Index;

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
        let mut record_offset = (current_tail & self.mask as i64) as isize; //зачем тут маска, если он всегда равен выравниванию

        let record_length: isize = length + record_descriptor::HEADER_LENGTH;
        let aligned_record_length: isize = align(record_length, record_descriptor::RECORD_ALIGNMENT);
        let new_tail: i64 = current_tail + aligned_record_length as i64;
        let to_end_of_buffer: isize = self.capacity - record_offset;

        if to_end_of_buffer < aligned_record_length {
            self.signal_tail_intent(new_tail + to_end_of_buffer as i64);

            self.insert_padding_record(record_offset, to_end_of_buffer);

            current_tail += to_end_of_buffer as i64;
            record_offset = 0;
        } else {
            self.signal_tail_intent(new_tail);
        }

        self.buffer
            .put::<i32>(record_descriptor::length_offset(record_offset), record_length as i32);
        self.buffer
            .put::<i32>(record_descriptor::type_offset(record_offset), msg_type_id);

        self.buffer.put_bytes(
            record_descriptor::msg_offset(record_offset),
            src_buffer.as_sub_slice(src_index, length),
        );

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

    fn insert_padding_record(&mut self, record_offset: isize, length: isize) {
        self.buffer
            .put::<i32>(record_descriptor::length_offset(record_offset), length as i32);
        self.buffer.put::<i32>(
            record_descriptor::type_offset(record_offset),
            record_descriptor::PADDING_MSG_TYPE_ID,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::concurrent::atomic_buffer::AlignedBuffer;

    const CAPACITY: isize = (1024);
    const TOTAL_BUFFER_LENGTH: isize = (CAPACITY + broadcast_buffer_descriptor::TRAILER_LENGTH);
    const SRC_BUFFER_SIZE: i32 = (1024);
    const MSG_TYPE_ID: i32 = (7);
    const TAIL_INTENT_COUNTER_INDEX: isize = (CAPACITY + broadcast_buffer_descriptor::TAIL_INTENT_COUNTER_OFFSET);
    const TAIL_COUNTER_INDEX: isize = (CAPACITY + broadcast_buffer_descriptor::TAIL_COUNTER_OFFSET);
    const LATEST_COUNTER_INDEX: isize = (CAPACITY + broadcast_buffer_descriptor::LATEST_COUNTER_OFFSET);

    struct BroadcastTransmitterTest {
        buffer: AtomicBuffer,
        message_buffer_owner: Vec<AlignedBuffer>,
    }

    impl BroadcastTransmitterTest {
        fn new(capacity: isize) -> Self {
            let (buffer, owner) = Self::sized_buffer_with_trailed(capacity);

            Self {
                buffer,
                message_buffer_owner: vec![owner],
            }
        }

        fn sized_buffer(capacity: isize) -> (AtomicBuffer, AlignedBuffer) {
            let mut owner = AlignedBuffer::with_capacity(capacity);
            (AtomicBuffer::from_aligned(&owner), owner)
        }

        fn sized_buffer_with_trailed(capacity: isize) -> (AtomicBuffer, AlignedBuffer) {
            Self::sized_buffer(capacity + 128)
        }

        fn create_transmitter(&self) -> BroadcastTransmitter {
            self.try_create_transmitter().unwrap()
        }

        fn try_create_transmitter(&self) -> Result<BroadcastTransmitter, BroadcastTransmitError> {
            BroadcastTransmitter::new(self.buffer)
        }

        fn create_message_buffer(&mut self, capacity: isize) -> AtomicBuffer {
            let (buffer, owner) = Self::sized_buffer(capacity);
            self.message_buffer_owner.push(owner);
            buffer
        }
    }

    #[inline]
    fn sized_buffer_filled_with_range(capacity: isize) -> (AtomicBuffer, Vec<u8>) {
        assert!(capacity < 255);

        let aligned_buffer = AlignedBuffer::with_capacity(capacity);

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
        const TAIL: i64 = 0;
        const RECORD_OFFSET: i32 = TAIL as i32;
        const LENGTH: isize = 8;
        const RECORD_LENGTH: isize = LENGTH + record_descriptor::HEADER_LENGTH;
        let aligned_record_length: isize = align(RECORD_LENGTH, record_descriptor::RECORD_ALIGNMENT);
        const SRC_INDEX: Index = 0;

        let mut test = BroadcastTransmitterTest::new(64);
        let mut transmitter = test.create_transmitter();
        let src_buffer = test.create_message_buffer(LENGTH as isize);
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
        const TAIL: i64 = (record_descriptor::RECORD_ALIGNMENT * 3) as i64;
        const RECORD_OFFSET: i32 = TAIL as i32;
        const LENGTH: isize = 8;
        const RECORD_LENGTH: isize = LENGTH + record_descriptor::HEADER_LENGTH;
        let aligned_record_length: isize = align(RECORD_LENGTH, record_descriptor::RECORD_ALIGNMENT);
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
        //    TEST_F(BroadcastTransmitterTest, shouldTransmitIntoEndOfBuffer)
        //    {
        //    AERON_DECL_ALIGNED(src_buffer_t buffer, 16);

        let mut test = BroadcastTransmitterTest::new(CAPACITY);

        //    AtomicBuffer srcBuffer(&buffer[0], buffer.size());
        let src_buffer = test.create_message_buffer(CAPACITY);

        //    const std::int32_t length = 8;
        const LENGTH: isize = 1000;

        //    const std::int32_t recordLength = length + RecordDescriptor::HEADER_LENGTH;
        const RECORD_LENGTH: isize = LENGTH + record_descriptor::HEADER_LENGTH;

        //    const std::int32_t alignedRecordLength = util::BitUtil::align(recordLength, RecordDescriptor::RECORD_ALIGNMENT);
        let aligned_record_length: isize = align(RECORD_LENGTH, record_descriptor::RECORD_ALIGNMENT);

        //    const std::int64_t tail = CAPACITY - alignedRecordLength;
        let tail = (CAPACITY - aligned_record_length) as i64;

        //    const std::int32_t recordOffset = (std::int32_t)tail;
        let record_offset = tail as i32;

        //    const util::index_t srcIndex = 0;
        const SRC_INDEX: Index = 0;

        //    m_broadcastTransmitter.transmit(MSG_TYPE_ID, srcBuffer, srcIndex, length);
        let mut transmitter = test.create_transmitter();
        src_buffer.put_bytes(0, &vec![42; LENGTH as usize]);
        for i in 0..8 {
            let index = i * 120;
            transmitter.transmit(MSG_TYPE_ID, &src_buffer, index, 120).unwrap();
        }

        src_buffer.put_bytes(LENGTH, &vec![1; 20]);
        transmitter.transmit(MSG_TYPE_ID, &src_buffer, LENGTH, 16).unwrap();
        dbg!(test.buffer);
    }

    //    TEST_F(BroadcastTransmitterTest, shouldApplyPaddingWhenInsufficientSpaceAtEndOfBuffer)
    //    {
    //    AERON_DECL_ALIGNED(src_buffer_t buffer, 16);
    //    AtomicBuffer srcBuffer(&buffer[0], buffer.size());
    //    std::int64_t tail = CAPACITY - RecordDescriptor::RECORD_ALIGNMENT;
    //    std::int32_t recordOffset = (std::int32_t)tail;
    //    const std::int32_t length = RecordDescriptor::RECORD_ALIGNMENT + 8;
    //    const std::int32_t recordLength = length + RecordDescriptor::HEADER_LENGTH;
    //    const std::int32_t alignedRecordLength = util::BitUtil::align(recordLength, RecordDescriptor::RECORD_ALIGNMENT);
    //    const std::int32_t toEndOfBuffer = CAPACITY - recordOffset;
    //    const util::index_t srcIndex = 0;
    //    testing::Sequence sequence;
    //
    //    EXPECT_CALL(m_mockBuffer, getInt64(TAIL_COUNTER_INDEX))
    //    .Times(1)
    //    .InSequence(sequence)
    //    .WillOnce(testing::Return(tail));
    //
    //    EXPECT_CALL(m_mockBuffer, putInt64Ordered(TAIL_INTENT_COUNTER_INDEX, tail + alignedRecordLength + toEndOfBuffer))
    //    .Times(1)
    //    .InSequence(sequence);
    //    EXPECT_CALL(m_mockBuffer, putInt32(RecordDescriptor::lengthOffset(recordOffset), toEndOfBuffer))
    //    .Times(1)
    //    .InSequence(sequence);
    //    EXPECT_CALL(m_mockBuffer, putInt32(RecordDescriptor::typeOffset(recordOffset), RecordDescriptor::PADDING_MSG_TYPE_ID))
    //    .Times(1)
    //    .InSequence(sequence);
    //
    //    tail += toEndOfBuffer;
    //    recordOffset = 0;
    //
    //    EXPECT_CALL(m_mockBuffer, putInt32(RecordDescriptor::lengthOffset(recordOffset), recordLength))
    //    .Times(1)
    //    .InSequence(sequence);
    //    EXPECT_CALL(m_mockBuffer, putInt32(RecordDescriptor::typeOffset(recordOffset), MSG_TYPE_ID))
    //    .Times(1)
    //    .InSequence(sequence);
    //    EXPECT_CALL(m_mockBuffer, putBytes(RecordDescriptor::msgOffset(recordOffset), testing::Ref(srcBuffer), srcIndex, length))
    //    .Times(1)
    //    .InSequence(sequence);
    //
    //    EXPECT_CALL(m_mockBuffer, putInt64(LATEST_COUNTER_INDEX, tail))
    //    .Times(1)
    //    .InSequence(sequence);
    //    EXPECT_CALL(m_mockBuffer, putInt64Ordered(TAIL_COUNTER_INDEX, tail + alignedRecordLength))
    //    .Times(1)
    //    .InSequence(sequence);
    //
    //    m_broadcastTransmitter.transmit(MSG_TYPE_ID, srcBuffer, srcIndex, length);
    //    }
}
