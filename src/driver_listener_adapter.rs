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

use crate::command::client_timeout_flyweight::ClientTimeoutFlyweight;
use crate::command::control_protocol_events::AeronCommand;
use crate::command::counter_update_flyweight::CounterUpdateFlyweight;
use crate::command::error_response_flyweight::{ErrorResponseFlyweight, ERROR_CODE_CHANNEL_ENDPOINT_ERROR};
use crate::command::image_buffers_ready_flyweight::ImageBuffersReadyFlyweight;
use crate::command::image_message_flyweight::ImageMessageFlyweight;
use crate::command::operation_succeeded_flyweight::OperationSucceededFlyweight;
use crate::command::publication_buffers_ready_flyweight::*;
use crate::command::subscription_ready_flyweight::SubscriptionReadyFlyweight;
use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::concurrent::broadcast::copy_broadcast_receiver::CopyBroadcastReceiver;
use crate::concurrent::broadcast::BroadcastTransmitError;
use crate::utils::types::Index;

pub trait DriverListener {
    fn on_new_publication(
        &self,
        registration_id: i64,
        original_registration_id: i64,
        stream_id: i32,
        session_id: i32,
        publication_limit_counter_id: i32,
        channel_status_indicator_id: i32,
        log_filename: CString,
    );

    fn on_new_exclusive_publication(
        &self,
        registration_id: i64,
        original_registration_id: i64,
        stream_id: i32,
        session_id: i32,
        publication_limit_counter_id: i32,
        channel_status_indicator_id: i32,
        log_filename: CString,
    );

    fn on_subscription_ready(&self, registration_id: i64, channel_status_id: i32);

    fn on_operation_success(&self, correlation_id: i64);

    fn on_channel_endpoint_error_response(&self, offending_command_correlation_id: i64, error_message: CString);

    fn on_error_response(&self, offending_command_correlation_id: i64, error_code: i32, error_message: CString);

    fn on_available_image(
        &self,
        correlation_id: i64,
        session_id: i32,
        subscriber_position_id: i32,
        subscription_registration_id: i64,
        log_filename: CString,
        source_identity: CString,
    );

    fn on_unavailable_image(&self, correlation_id: i64, subscription_registration_id: i64);

    fn on_available_counter(&self, registration_id: i64, counter_id: i32);

    fn on_unavailable_counter(&self, registration_id: i64, counter_id: i32);

    fn on_client_timeout(&self, client_id: i64);
}

pub struct DriverListenerAdapter<'a, T: DriverListener> {
    broadcast_receiver: CopyBroadcastReceiver<'a>,
    driver_listener: &'a T,
}

impl<'a, T: DriverListener> DriverListenerAdapter<'a, T> {
    pub fn new(broadcast_receiver: CopyBroadcastReceiver<'a>, driver_listener: &'a T) -> Self {
        Self {
            broadcast_receiver,
            driver_listener,
        }
    }

    pub fn receive_messages(&mut self) -> Result<usize, BroadcastTransmitError> {
        let this_driver_listener = self.driver_listener;

        let receive_handler = |msg: AeronCommand, buffer: AtomicBuffer, offset: Index, _length: Index| match msg {
            AeronCommand::ResponseOnPublicationReady => {
                let publication_ready = PublicationBuffersReadyFlyweight::new(buffer, offset);

                this_driver_listener.on_new_publication(
                    publication_ready.correlation_id(),
                    publication_ready.registration_id(),
                    publication_ready.stream_id(),
                    publication_ready.session_id(),
                    publication_ready.position_limit_counter_id(),
                    publication_ready.channel_status_indicator_id(),
                    publication_ready.log_file_name(),
                );
            }
            AeronCommand::ResponseOnExclusivePublicationReady => {
                let publication_ready = PublicationBuffersReadyFlyweight::new(buffer, offset);

                this_driver_listener.on_new_exclusive_publication(
                    publication_ready.correlation_id(),
                    publication_ready.registration_id(),
                    publication_ready.stream_id(),
                    publication_ready.session_id(),
                    publication_ready.position_limit_counter_id(),
                    publication_ready.channel_status_indicator_id(),
                    publication_ready.log_file_name(),
                );
            }
            AeronCommand::ResponseOnSubscriptionReady => {
                let subscription_ready = SubscriptionReadyFlyweight::new(buffer, offset);

                this_driver_listener.on_subscription_ready(
                    subscription_ready.correlation_id(),
                    subscription_ready.channel_status_indicator_id(),
                );
            }
            AeronCommand::ResponseOnAvailableImage => {
                let image_ready = ImageBuffersReadyFlyweight::new(buffer, offset);

                this_driver_listener.on_available_image(
                    image_ready.correlation_id(),
                    image_ready.session_id(),
                    image_ready.subscriber_position_id(),
                    image_ready.subscription_registration_id(),
                    image_ready.log_file_name(),
                    image_ready.source_identity(),
                );
            }
            AeronCommand::ResponseOnOperationSuccess => {
                let operation_succeeded = OperationSucceededFlyweight::new(buffer, offset);

                this_driver_listener.on_operation_success(operation_succeeded.correlation_id());
            }
            AeronCommand::ResponseOnUnavailableImage => {
                let image_message = ImageMessageFlyweight::new(buffer, offset);

                this_driver_listener
                    .on_unavailable_image(image_message.correlation_id(), image_message.subscription_registration_id());
            }
            AeronCommand::ResponseOnError => {
                let error_response = ErrorResponseFlyweight::new(buffer, offset);

                let error_code = error_response.error_code();

                if ERROR_CODE_CHANNEL_ENDPOINT_ERROR == error_code {
                    this_driver_listener.on_channel_endpoint_error_response(
                        error_response.offending_command_correlation_id(),
                        error_response.error_message(),
                    );
                } else {
                    this_driver_listener.on_error_response(
                        error_response.offending_command_correlation_id(),
                        error_code,
                        error_response.error_message(),
                    );
                }
            }
            AeronCommand::ResponseOnCounterReady => {
                let response = CounterUpdateFlyweight::new(buffer, offset);
                this_driver_listener.on_available_counter(response.correlation_id(), response.counter_id());
            }
            AeronCommand::ResponseOnUnavailableCounter => {
                let response = CounterUpdateFlyweight::new(buffer, offset);
                this_driver_listener.on_unavailable_counter(response.correlation_id(), response.counter_id());
            }
            AeronCommand::ResponseOnClientTimeout => {
                let response = ClientTimeoutFlyweight::new(buffer, offset);
                this_driver_listener.on_client_timeout(response.client_id());
            }
            _ => {
                unreachable!("Unexpected control protocol event: {}", msg as i32);
            }
        };

        self.broadcast_receiver.receive(receive_handler)
    }
}
