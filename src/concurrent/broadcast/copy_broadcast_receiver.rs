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

use std::sync::{Arc, Mutex};

use super::broadcast_receiver::BroadcastReceiver;
use super::BroadcastTransmitError;
use crate::{
    command::control_protocol_events::AeronCommand,
    concurrent::atomic_buffer::{AlignedBuffer, AtomicBuffer},
    utils::types::Index,
};

trait Handler {
    fn handle(message_type_id: i32, buffer: AtomicBuffer, i1: Index, i2: Index);
}

#[allow(dead_code)]
pub struct CopyBroadcastReceiver {
    receiver: Arc<Mutex<BroadcastReceiver>>,
    scratch_buffer: AtomicBuffer,
    aligned_buffer: AlignedBuffer,
}

impl CopyBroadcastReceiver {
    pub fn new(receiver: Arc<Mutex<BroadcastReceiver>>) -> Self {
        let scratch = AlignedBuffer::with_capacity(4096);
        Self {
            receiver,
            scratch_buffer: AtomicBuffer::from_aligned(&scratch),
            aligned_buffer: scratch,
        }
    }

    pub fn receive<F>(&mut self, mut handler: F) -> Result<usize, BroadcastTransmitError>
    where
        F: FnMut(AeronCommand, AtomicBuffer, Index, Index),
    {
        let mut messages_received: usize = 0;
        let mut receiver = self.receiver.lock().expect("Mutex poisoned");
        let last_seen_lapped_count = receiver.lapped_count();

        if receiver.receive_next() {
            if last_seen_lapped_count != receiver.lapped_count() {
                return Err(BroadcastTransmitError::UnableToKeepUpWithBroadcastBuffer);
            }

            let length = receiver.length() as Index;
            if length > self.scratch_buffer.capacity() {
                return Err(BroadcastTransmitError::BufferTooSmall {
                    need: length,
                    capacity: self.scratch_buffer.capacity(),
                });
            }

            let msg = AeronCommand::from_command_id(receiver.type_id());

            self.scratch_buffer.copy_from(0, receiver.buffer(), receiver.offset(), length);

            if !receiver.validate() {
                return Err(BroadcastTransmitError::UnableToKeepUpWithBroadcastBuffer);
            }

            handler(msg, self.scratch_buffer, 0, length);

            messages_received = 1;
        }

        Ok(messages_received)
    }
}
