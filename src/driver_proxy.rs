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

use std::ffi::CString;
use std::sync::Arc;

use crate::utils::errors::IllegalStateError;
use crate::{
    command::{
        control_protocol_events::AeronCommand,
        correlated_message_flyweight::{CorrelatedMessageFlyweight, CORRELATED_MESSAGE_LENGTH},
        counter_message_flyweight::CounterMessageFlyweight,
        destination_message_flyweight::DestinationMessageFlyweight,
        publication_message_flyweight::PublicationMessageFlyweight,
        remove_message_flyweight::RemoveMessageFlyweight,
        subscription_message_flyweight::SubscriptionMessageFlyweight,
        terminate_driver_flyweight::TerminateDriverFlyweight,
    },
    concurrent::{atomic_buffer::AtomicBuffer, ring_buffer::ManyToOneRingBuffer},
    ttrace,
    utils::{errors::AeronError, types::Index},
};

pub struct DriverProxy {
    to_driver_command_buffer: Arc<ManyToOneRingBuffer>,
    client_id: i64,
}

impl DriverProxy {
    pub fn new(to_driver_command_buffer: Arc<ManyToOneRingBuffer>) -> Self {
        Self {
            to_driver_command_buffer: to_driver_command_buffer.clone(),
            client_id: to_driver_command_buffer.next_correlation_id(),
        }
    }

    pub fn time_of_last_driver_keepalive(&self) -> i64 {
        self.to_driver_command_buffer.consumer_heartbeat_time()
    }

    pub fn client_id(&self) -> i64 {
        self.client_id
    }

    pub fn add_publication(&self, channel: CString, stream_id: i32) -> Result<i64, AeronError> {
        let correlation_id = self.to_driver_command_buffer.next_correlation_id();

        self.write_command_to_driver(|buffer, length| {
            let mut publication_message = PublicationMessageFlyweight::new(buffer, 0);

            publication_message.set_client_id(self.client_id);
            publication_message.set_correlation_id(correlation_id);
            publication_message.set_stream_id(stream_id);
            publication_message.set_channel(channel.as_bytes());

            *length = publication_message.length();

            Ok(AeronCommand::AddPublication)
        })?;

        Ok(correlation_id)
    }

    pub fn add_exclusive_publication(&self, channel: CString, stream_id: i32) -> Result<i64, AeronError> {
        let correlation_id = self.to_driver_command_buffer.next_correlation_id();
        self.write_command_to_driver(|buffer, length| {
            let mut publication_message = PublicationMessageFlyweight::new(buffer, 0);

            publication_message.set_client_id(self.client_id);
            publication_message.set_correlation_id(correlation_id);
            publication_message.set_stream_id(stream_id);
            publication_message.set_channel(channel.as_bytes());

            *length = publication_message.length();

            Ok(AeronCommand::AddExclusivePublication)
        })?;

        Ok(correlation_id)
    }

    pub fn remove_publication(&self, registration_id: i64) -> Result<i64, AeronError> {
        let correlation_id = self.to_driver_command_buffer.next_correlation_id();

        self.write_command_to_driver(|buffer, length| {
            let mut remove_message = RemoveMessageFlyweight::new(buffer, 0);

            remove_message.set_client_id(self.client_id);
            remove_message.set_correlation_id(correlation_id);
            remove_message.set_registration_id(registration_id);

            *length = remove_message.length();

            Ok(AeronCommand::RemovePublication)
        })?;

        Ok(correlation_id)
    }

    pub fn add_subscription(&self, channel: CString, stream_id: i32) -> Result<i64, AeronError> {
        let correlation_id = self.to_driver_command_buffer.next_correlation_id();

        self.write_command_to_driver(|buffer, length| {
            let mut subscription_message = SubscriptionMessageFlyweight::new(buffer, 0);

            subscription_message.set_client_id(self.client_id);
            subscription_message.set_registration_correlation_id(-1);
            subscription_message.set_correlation_id(correlation_id);
            subscription_message.set_stream_id(stream_id);
            subscription_message.set_channel(channel.as_bytes());

            *length = subscription_message.length();

            Ok(AeronCommand::AddSubscription)
        })?;

        Ok(correlation_id)
    }

    pub fn remove_subscription(&self, registration_id: i64) -> Result<i64, AeronError> {
        let correlation_id = self.to_driver_command_buffer.next_correlation_id();

        self.write_command_to_driver(|buffer, length| {
            let mut remove_message = RemoveMessageFlyweight::new(buffer, 0);

            remove_message.set_client_id(self.client_id);
            remove_message.set_correlation_id(correlation_id);
            remove_message.set_registration_id(registration_id);

            *length = remove_message.length();

            Ok(AeronCommand::RemoveSubscription)
        })?;
        Ok(correlation_id)
    }

    pub fn send_client_keepalive(&self) -> Result<(), AeronError> {
        self.write_command_to_driver(|buffer, length| {
            let mut correlated_message = CorrelatedMessageFlyweight::new(buffer, 0);

            correlated_message.set_client_id(self.client_id);
            correlated_message.set_correlation_id(0);

            *length = CORRELATED_MESSAGE_LENGTH;

            Ok(AeronCommand::ClientKeepAlive)
        })
    }

    pub fn add_destination(&self, publication_registration_id: i64, channel: CString) -> Result<i64, AeronError> {
        let correlation_id = self.to_driver_command_buffer.next_correlation_id();

        self.write_command_to_driver(|buffer, length| {
            let mut add_message = DestinationMessageFlyweight::new(buffer, 0);

            add_message.set_client_id(self.client_id);
            add_message.set_registration_id(publication_registration_id);
            add_message.set_correlation_id(correlation_id);
            add_message.set_channel(channel.as_bytes());

            *length = add_message.length();

            Ok(AeronCommand::AddDestination)
        })?;

        Ok(correlation_id)
    }

    pub fn remove_destination(&self, publication_registration_id: i64, channel: CString) -> Result<i64, AeronError> {
        let correlation_id = self.to_driver_command_buffer.next_correlation_id();

        self.write_command_to_driver(|buffer, length| {
            let mut remove_message = DestinationMessageFlyweight::new(buffer, 0);

            remove_message.set_client_id(self.client_id);
            remove_message.set_registration_id(publication_registration_id);
            remove_message.set_correlation_id(correlation_id);
            remove_message.set_channel(channel.as_bytes());

            *length = remove_message.length();

            Ok(AeronCommand::RemoveDestination)
        })?;

        Ok(correlation_id)
    }

    pub fn add_rcv_destination(&self, subscription_registration_id: i64, channel: CString) -> Result<i64, AeronError> {
        let correlation_id = self.to_driver_command_buffer.next_correlation_id();

        self.write_command_to_driver(|buffer, length| {
            let mut add_message = DestinationMessageFlyweight::new(buffer, 0);

            add_message.set_client_id(self.client_id);
            add_message.set_registration_id(subscription_registration_id);
            add_message.set_correlation_id(correlation_id);
            add_message.set_channel(channel.as_bytes());

            *length = add_message.length();

            Ok(AeronCommand::AddRcvDestination)
        })?;

        Ok(correlation_id)
    }

    pub fn remove_rcv_destination(&self, subscription_registration_id: i64, channel: CString) -> Result<i64, AeronError> {
        let correlation_id = self.to_driver_command_buffer.next_correlation_id();

        self.write_command_to_driver(|buffer, length| {
            let mut remove_message = DestinationMessageFlyweight::new(buffer, 0);

            remove_message.set_client_id(self.client_id);
            remove_message.set_registration_id(subscription_registration_id);
            remove_message.set_correlation_id(correlation_id);
            remove_message.set_channel(channel.as_bytes());

            *length = remove_message.length();

            Ok(AeronCommand::RemoveRcvDestination)
        })?;

        Ok(correlation_id)
    }

    pub fn add_counter(&self, type_id: i32, key: &[u8], label: CString) -> Result<i64, AeronError> {
        let correlation_id = self.to_driver_command_buffer.next_correlation_id();

        self.write_command_to_driver(|buffer, length| {
            let mut command = CounterMessageFlyweight::new(buffer, 0);

            command.set_client_id(self.client_id);
            command.set_correlation_id(correlation_id);
            command.set_type_id(type_id);
            unsafe {
                command.set_key_buffer(key.as_ptr(), key.len() as i32);
            }
            command.set_label(label.as_bytes());

            *length = command.length();

            Ok(AeronCommand::AddCounter)
        })?;

        Ok(correlation_id)
    }

    pub fn remove_counter(&self, registration_id: i64) -> Result<i64, AeronError> {
        let correlation_id = self.to_driver_command_buffer.next_correlation_id();

        self.write_command_to_driver(|buffer, length| {
            let mut command = RemoveMessageFlyweight::new(buffer, 0);

            command.set_client_id(self.client_id);
            command.set_correlation_id(correlation_id);
            command.set_registration_id(registration_id);

            *length = command.length();

            Ok(AeronCommand::RemoveCounter)
        })?;

        Ok(correlation_id)
    }

    pub fn client_close(&self) -> Result<i64, AeronError> {
        let correlation_id = self.to_driver_command_buffer.next_correlation_id();

        self.write_command_to_driver(|buffer, length| {
            let mut correlated_message = CorrelatedMessageFlyweight::new(buffer, 0);

            correlated_message.set_client_id(self.client_id);
            correlated_message.set_correlation_id(correlation_id);

            *length = CORRELATED_MESSAGE_LENGTH;

            Ok(AeronCommand::ClientClose)
        })?;

        Ok(correlation_id)
    }

    pub fn terminate_driver(&self, token_buffer: *const u8, token_length: Index) -> Result<(), AeronError> {
        self.write_command_to_driver(|buffer, length| {
            let mut request = TerminateDriverFlyweight::new(buffer, 0);

            request.set_client_id(self.client_id);
            request.set_correlation_id(-1);
            unsafe {
                request.set_token_buffer(token_buffer, token_length);
            }

            *length = request.length();

            Ok(AeronCommand::TerminateDriver)
        })
    }

    fn write_command_to_driver(
        &self,
        filler: impl Fn(AtomicBuffer, &mut Index) -> Result<AeronCommand, AeronError>,
    ) -> Result<(), AeronError> {
        let mut message_buffer = DriverProxyCommandBuffer::default();

        let buffer = AtomicBuffer::new(&mut message_buffer.data[0] as *mut u8, message_buffer.data.len() as Index);
        let mut length = buffer.capacity();

        // Filler returns not only msg type but also actual msg length via mut ref length param.
        let msg_type = filler(buffer, &mut length)?;

        if self.to_driver_command_buffer.write(msg_type, buffer, 0, length).is_err() {
            ttrace!("Driver command {:#x} failed", msg_type);
            return Err(IllegalStateError::CouldNotWriteCommandToDriver.into());
        }

        ttrace!("Successfully written driver command: {:#x}", msg_type);

        Ok(())
    }
}

/// 16 byte alignment was used in C++ code. To make pointers work it is enough to
/// align this buffer at 8 bytes (on 64 bit architecture). And to make access to
/// the buffer (may be) faster it could be aligned to CACHE_LINE_LENGTH (64 bytes for modern x86 CPUs)
#[repr(C, align(16))]
struct DriverProxyCommandBuffer {
    data: [u8; 512],
}

impl Default for DriverProxyCommandBuffer {
    fn default() -> Self {
        Self {
            data: [0; 512], // zero the memory
        }
    }
}
