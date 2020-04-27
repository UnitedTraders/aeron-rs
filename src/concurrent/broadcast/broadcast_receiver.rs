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

use std::sync::atomic::{AtomicU64, Ordering};

use crate::{
    command::control_protocol_events::AeronCommand,
    concurrent::{
        atomic_buffer::AtomicBuffer,
        atomics,
        broadcast::{broadcast_buffer_descriptor, record_descriptor, BroadcastTransmitError},
    },
    utils::{bit_utils::align, types::Index},
};

pub struct BroadcastReceiver {
    buffer: AtomicBuffer,
    capacity: Index,
    mask: Index,
    tail_intent_counter_index: Index,
    tail_counter_index: Index,
    latest_counter_index: Index,
    record_offset: Index,
    cursor: i64,
    next_record: i64,
    lapped_count: AtomicU64,
}

impl BroadcastReceiver {
    pub fn new(buffer: AtomicBuffer) -> Result<BroadcastReceiver, BroadcastTransmitError> {
        let capacity = buffer.capacity() - broadcast_buffer_descriptor::TRAILER_LENGTH;
        broadcast_buffer_descriptor::check_capacity(capacity)?;

        let mut rx = Self {
            buffer,
            capacity,
            mask: capacity - 1,
            tail_intent_counter_index: capacity + broadcast_buffer_descriptor::TAIL_INTENT_COUNTER_OFFSET,
            tail_counter_index: capacity + broadcast_buffer_descriptor::TAIL_COUNTER_OFFSET,
            latest_counter_index: capacity + broadcast_buffer_descriptor::LATEST_COUNTER_OFFSET,
            record_offset: (0),
            cursor: (0),
            next_record: (0),
            lapped_count: AtomicU64::new(0),
        };

        rx.cursor = rx.buffer.get::<i64>(rx.latest_counter_index);
        rx.next_record = rx.cursor;

        rx.record_offset = rx.cursor as Index & rx.mask;

        Ok(rx)
    }

    pub fn capacity(&self) -> Index {
        self.capacity
    }

    pub fn lapped_count(&self) -> u64 {
        self.lapped_count.load(Ordering::SeqCst)
    }

    pub fn type_id(&self) -> i32 {
        self.buffer.get::<i32>(record_descriptor::type_offset(self.record_offset))
    }

    pub fn offset(&self) -> Index {
        record_descriptor::msg_offset(self.record_offset)
    }

    pub fn length(&self) -> i32 {
        self.buffer.get::<i32>(record_descriptor::length_offset(self.record_offset)) - record_descriptor::HEADER_LENGTH
    }

    pub fn buffer(&self) -> &AtomicBuffer {
        &self.buffer
    }

    pub fn receive_next(&mut self) -> bool {
        let mut is_available = false;
        let tail = self.buffer.get_volatile::<i64>(self.tail_counter_index);
        let mut cursor = self.next_record;

        if tail > cursor {
            if !self.do_validate(cursor as Index) {
                let _ignored = self.lapped_count.fetch_add(1, Ordering::SeqCst);
                cursor = self.buffer.get::<i64>(self.latest_counter_index);
            }

            let mut record_offset: Index = cursor as Index & self.mask;

            self.cursor = cursor;
            self.next_record = cursor
                + align(
                    self.buffer.get::<i32>(record_descriptor::length_offset(record_offset)) as Index,
                    record_descriptor::RECORD_ALIGNMENT,
                ) as i64;

            if AeronCommand::Padding as i32 == self.buffer.get::<i32>(record_descriptor::type_offset(record_offset)) {
                record_offset = 0;
                self.cursor = self.next_record;
                self.next_record += align(
                    self.buffer.get::<i32>(record_descriptor::length_offset(record_offset)) as Index,
                    record_descriptor::RECORD_ALIGNMENT,
                ) as i64;
            }

            self.record_offset = record_offset;
            is_available = true;
        }

        is_available
    }

    pub fn validate(&self) -> bool {
        atomics::acquire();
        self.do_validate(self.cursor as Index)
    }

    fn do_validate(&self, cursor: Index) -> bool {
        cursor + self.capacity > self.buffer.get_volatile::<i64>(self.tail_intent_counter_index) as Index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::concurrent::{atomic_buffer::AlignedBuffer, broadcast::broadcast_transmitter::BroadcastTransmitter};

    fn channel(buffer: AtomicBuffer) -> (BroadcastTransmitter, BroadcastReceiver) {
        (
            BroadcastTransmitter::new(buffer).unwrap(),
            BroadcastReceiver::new(buffer).unwrap(),
        )
    }

    #[test]
    fn test_1() {
        let data = AlignedBuffer::with_capacity(128 + broadcast_buffer_descriptor::TRAILER_LENGTH);
        let rx_tx_buf = AtomicBuffer::from_aligned(&data);

        let (mut tx, _rx) = channel(rx_tx_buf);

        let tx_data = AlignedBuffer::with_capacity(16);
        let tx_buf = AtomicBuffer::from_aligned(&tx_data);

        tx.transmit(2, &tx_buf, 0, 4).expect("cant' trasmit");
    }
}
