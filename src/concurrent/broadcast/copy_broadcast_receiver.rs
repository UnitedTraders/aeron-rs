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

//typedef std::array<std::uint8_t, 4096> scratch_buffer_t;

//** The data handler function signature */
//typedef std::function<void(std::int32_t, concurrent::AtomicBuffer&, util::index_t, util::index_t)> handler_t;

trait Handler {
    fn handle(message_type_id: i32, buffer: AtomicBuffer, i1: Index, i2: Index);
}

use super::broadcast_receiver::BroadcastReceiver;
use super::BroadcastTransmitError;
use crate::command::control_protocol_events::AeronCommand;
use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::utils::types::Index;

pub struct CopyBroadcastReceiver {
    receiver: BroadcastReceiver,
    scratch_buffer: AtomicBuffer,
}

impl CopyBroadcastReceiver {
    pub fn new(receiver: BroadcastReceiver, scratch_buffer: AtomicBuffer) -> Self {
        Self {
            receiver,
            scratch_buffer,
        }
    }

    pub fn receive<F>(&mut self, handler: F) -> Result<usize, BroadcastTransmitError>
    where
        F: Fn(AeronCommand, AtomicBuffer, Index, Index),
    {
        let mut messages_received: usize = 0;
        let last_seen_lapped_count = self.receiver.lapped_count();

        if self.receiver.receive_next() {
            if last_seen_lapped_count != self.receiver.lapped_count() {
                return Err(BroadcastTransmitError::UnableToKeepUpWithBroadcastBuffer);
            }

            let length = self.receiver.length() as Index;
            if length > self.scratch_buffer.capacity() {
                return Err(BroadcastTransmitError::BufferTooSmall {
                    need: length as isize,
                    capacity: self.scratch_buffer.capacity(),
                });
            }

            let msg = AeronCommand::from_command_id(self.receiver.type_id());

            self.scratch_buffer.put_bytes(0, self.receiver.buffer().as_slice());

            if !self.receiver.validate() {
                return Err(BroadcastTransmitError::UnableToKeepUpWithBroadcastBuffer);
            }

            handler(msg, self.scratch_buffer, 0, length);

            messages_received = 1;
        }

        Ok(messages_received)
    }
}
