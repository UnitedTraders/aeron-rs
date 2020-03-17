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

use std::ffi::CStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::distributions::{Distribution, Uniform};
use rand::Rng;

use crate::client_conductor::ClientConductor;
use crate::cnc_file_descriptor;
use crate::command::control_protocol_events::AeronCommand;
use crate::concurrent::agent_invoker::AgentInvoker;
use crate::concurrent::agent_runner::AgentRunner;
use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::concurrent::broadcast::broadcast_receiver::BroadcastReceiver;
use crate::concurrent::broadcast::copy_broadcast_receiver::CopyBroadcastReceiver;
use crate::concurrent::ring_buffer::ManyToOneRingBuffer;
use crate::concurrent::strategies::SleepingIdleStrategy;
use crate::context::{Context, OnAvailableCounter, OnAvailableImage, OnCloseClient, OnUnavailableCounter, OnUnavailableImage};
use crate::driver_proxy::DriverProxy;
use crate::utils::errors::AeronError;
use crate::utils::memory_mapped_file::MemoryMappedFile;
use crate::utils::misc::{semantic_version_major, unix_time_ms};
use crate::utils::types::{Index, Moment};

/**
 * Aeron entry point for communicating to the Media Driver for creating {@link Publication}s and {@link Subscription}s.
 * Use a {@link Context} to configure the Aeron object.
 * <p>
 * A client application requires only one Aeron object per Media Driver.
 */
struct Aeron<'a> {
    random_engine: rand::Rng,
    session_id_distribution: Uniform,

    context: &'a Context,

    cnc_buffer: MemoryMappedFile, // May be it should be Arc ???

    to_driver_atomic_buffer: AtomicBuffer,
    to_clients_atomic_buffer: AtomicBuffer,
    counters_metadata_buffer: AtomicBuffer,
    counters_value_buffer: AtomicBuffer,

    to_driver_ring_buffer: ManyToOneRingBuffer,
    driver_proxy: Arc<DriverProxy<'a>>, // need to use Arc here to avoid self-referencing inside Aeron

    to_clients_broadcast_receiver: BroadcastReceiver,
    to_clients_copy_receiver: CopyBroadcastReceiver<'a>,

    conductor: Arc<ClientConductor<'a>>,
    idle_strategy: Arc<SleepingIdleStrategy>,
    conductor_runner: AgentRunner<ClientConductor<'a>, SleepingIdleStrategy>,
    conductor_invoker: AgentInvoker<ClientConductor<'a>>,
}

const IDLE_SLEEP_MS: Moment = 4;
const IDLE_SLEEP_MS_1: Moment = 1;
const IDLE_SLEEP_MS_16: Moment = 16;
const IDLE_SLEEP_MS_100: Moment = 100;

const AGENT_NAME: &str = "client-conductor";

impl<'a> Aeron {

    /**
     * Create an Aeron instance and connect to the media driver.
     * <p>
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     *
     * @param context for configuration of the client.
     */

    pub fn new(context: &Context) -> Self {

        let cnc_buf = Self::map_cnc_file(context).expect("Error mapping CnC file");
        let local_to_driver_atomic_buffer = cnc_file_descriptor::create_to_driver_buffer(&cnc_buf);
        let local_to_clients_atomic_buffer = cnc_file_descriptor::create_to_clients_buffer(&cnc_buf);
        let local_counters_metadata_buffer = cnc_file_descriptor::create_counter_metadata_buffer(&cnc_buf);
        let local_counters_value_buffer = cnc_file_descriptor::create_counter_values_buffer(&cnc_buf);
        let local_to_driver_ring_buffer = ManyToOneRingBuffer::new(local_to_driver_atomic_buffer);
        let local_to_clients_broadcast_receiver = BroadcastReceiver::new(local_to_clients_atomic_buffer).expect("Failed to create BroadcastReceiver");
        let local_driver_proxy = Arc::new(DriverProxy::new(&local_to_driver_ring_buffer));
        let local_idle_strategy = Arc::new(SleepingIdleStrategy::new(IDLE_SLEEP_MS));

        let local_conductor = Arc::new(ClientConductor::new(
            current_time_millis,
            local_driver_proxy.clone(),
            to_clients_copy_receiver,
            counters_metadata_buffer,
            counters_value_buffer,
            context.new_publication_handler(),
            context.new_exclusive_publication_handler(),
            context.new_subscription_handler(),
            context.error_handler(),
            context.available_counter_handler(),
            context.unavailable_counter_handler(),
            context.close_client_handler(),
            context.media_driver_timeout(),
            context.resource_linger_timeout(),
            cnc_file_descriptor::client_liveness_timeout(cnc_buffer),
            context.pre_touch_mapped_memory()));

        let mut aeronchik = Self {
            random_engine: rand::thread_rng(),
            session_id_distribution: Uniform::from(std::i32::MIN, std::i32::MAX),
            context,
            cnc_buffer: cnc_buf,
            to_driver_atomic_buffer: local_to_driver_atomic_buffer,
            to_clients_atomic_buffer: local_to_clients_atomic_buffer,
            counters_metadata_buffer: local_counters_metadata_buffer,
            counters_value_buffer: local_counters_value_buffer,
            to_driver_ring_buffer: local_to_driver_ring_buffer,
            driver_proxy: local_driver_proxy.clone(),
            to_clients_broadcast_receiver: local_to_clients_broadcast_receiver,
            to_clients_copy_receiver: CopyBroadcastReceiver::new(&local_to_clients_broadcast_receiver),
            conductor: local_conductor.clone(),
            idle_strategy: local_idle_strategy.clone(),
            conductor_runner: AgentRunner::new(local_conductor.clone(), local_idle_strategy.clone(), context.error_handler(), AGENT_NAME),
            conductor_invoker: AgentInvoker::new(local_conductor.clone(), context.error_handler()),
        };

        if context.use_conductor_agent_invoker() {
            aeronchik.conductor_invoker.start();
        } else {
            aeronchik.conductor_runner.start();
        }

        aeronchik
    }

    /**
     * Indicate if the instance is closed and can not longer be used.
     *
     * @return true is the instance is closed and can no longer be used, otherwise false.
     */
    pub fn is_closed(&self) -> bool {
        self.conductor.is_closed()
    }

    /**
     * Create an Aeron instance and connect to the media driver.
     * <p>
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     *
     * @param context for configuration of the client.
     * @return the new Aeron instance connected to the Media Driver.
     */
    pub fn connect_ctx(context: &Context) -> Arc<Aeron> {
        Arc::new(Aeron::new(context))
    }

    /**
     * Create an Aeron instance and connect to the media driver.
     * <p>
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     *
     * @return the new Aeron instance connected to the Media Driver.
     */
    pub fn connect() -> Arc<Aeron> {
        Arc::new(Aeron::new(&Context::new()))
    }

    /**
     * Add a {@link Publication} for publishing messages to subscribers
     *
     * This function returns immediately and does not wait for the response from the media driver. The returned
     * registration id is to be used to determine the status of the command with the media driver.
     *
     * @param channel for sending the messages known to the media layer.
     * @param stream_id within the channel scope.
     * @return registration id for the publication
     */
    pub fn add_publication(&mut self, channel: &str, stream_id: i32) -> Result<i64, AeronError> {
        self.conductor.add_publication(channel, stream_id)
    }

    /**
     * Retrieve the Publication associated with the given registration_id.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registration_id is unknown, then AeronError is returned.
     * - If the media driver has not answered the add command, then AeronError is returned.
     * - If the media driver has successfully added the Publication then what is returned is the Publication.
     * - If the media driver has returned an error, this method will bring back the error returned.
     *
     * @see Aeron::add_publication
     *
     * @param registration_id of the Publication returned by Aeron::add_publication
     * @return Publication associated with the registration_id
     */
    pub fn find_publication(&mut self, registration_id: i64) -> Result<Arc<Mutex<Publication>>, AeronError> {
        self.conductor.find_publication(registration_id)
    }

    /**
     * Add an {@link ExclusivePublication} for publishing messages to subscribers from a single thread.
     *
     * @param channel  for sending the messages known to the media layer.
     * @param stream_id within the channel scope.
     * @return registration id for the publication
     */
    pub fn add_exclusive_publication(&mut self, channel: &str, stream_id: i32) -> Result<i64, AeronError> {
        self.conductor.add_exclusive_publication(channel, stream_id)
    }

    /**
     * Retrieve the ExclusivePublication associated with the given registration_id.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registration_id is unknown, then a nullptr is returned.
     * - If the media driver has not answered the add command, then a nullptr is returned.
     * - If the media driver has successfully added the ExclusivePublication then what is returned is the ExclusivePublication.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::add_exclusive_publication
     *
     * @param registration_id of the ExclusivePublication returned by Aeron::add_exclusive_publication
     * @return ExclusivePublication associated with the registration_id
     */
    pub fn find_exclusive_publication(&mut self, registration_id: i64) -> Result<Arc<Mutex<ExclusivePublication>>, AeronError>  {
        self.conductor.find_exclusive_publication(registration_id)
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     *
     * This function returns immediately and does not wait for the response from the media driver. The returned
     * registration id is to be used to determine the status of the command with the media driver.
     *
     * @param channel  for receiving the messages known to the media layer.
     * @param stream_id within the channel scope.
     * @return registration id for the subscription
     */
    pub fn add_subscription(&mut self, channel: &str, stream_id: i32) -> Result<i64, AeronError> {
        self.conductor.add_subscription(
            channel, stream_id, self.context.available_image_handler(), self.context.unavailable_image_handler())
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     *
     * This method will override the default handlers from the {@link Context}.
     *
     * @param channel                 for receiving the messages known to the media layer.
     * @param stream_id                within the channel scope.
     * @param availableImageHandler   called when {@link Image}s become available for consumption.
     * @param unavailableImageHandler called when {@link Image}s go unavailable for consumption.
     * @return registration id for the subscription
     */
    pub fn add_subscription_opt(&mut self,
                                channel: &str,
                                stream_id: i32,
                                on_available_image_handler: OnAvailableImage,
                                on_unavailable_image_handler: OnUnavailableImage) -> Result<i64, AeronError>
    {
        self.conductor.add_subscription(channel, stream_id, on_available_image_handler, on_unavailable_image_handler)
    }

    /**
     * Retrieve the Subscription associated with the given registration_id.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registration_id is unknown, then a nullptr is returned.
     * - If the media driver has not answered the add command, then a nullptr is returned.
     * - If the media driver has successfully added the Subscription then what is returned is the Subscription.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::add_subscription
     *
     * @param registration_id of the Subscription returned by Aeron::add_subscription
     * @return Subscription associated with the registration_id
     */
    pub fn find_subscription(&mut self, registration_id: i64) -> Result<Arc<Mutex<Subscription>>, AeronError> {
        self.conductor.find_subscription(registration_id)
    }

    /**
     * Generate the next correlation id that is unique for the connected Media Driver.
     *
     * This is useful generating correlation identifiers for pairing requests with responses in a clients own
     * application protocol.
     *
     * This method is thread safe and will work across processes that all use the same media driver.
     *
     * @return next correlation id that is unique for the Media Driver.
     */
    pub fn next_correlation_id(&self) -> Result<i64, AeronError> {
        self.conductor.ensure_open()?;
        Ok(self.to_driver_ring_buffer.next_correlation_id())
    }

    /**
     * Allocate a counter on the media driver and return a {@link Counter} for it.
     *
     * @param type_id      for the counter.
     * @param key_buffer   containing the optional key for the counter.
     * @param key_length   of the key in the key_buffer.
     * @param label       for the counter.
     * @return registration id for the Counter
     */
    pub fn add_сounter(&mut self,
        type_id: i32,
        key_buffer: &[u8],
        label: &CStr) -> Result<i64, AeronError> {
        self.conductor.add_counter(type_id, key_buffer, label)
    }

    /**
     * Retrieve the Counter associated with the given registration_id.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registration_id is unknown, then a AeronError is returned.
     * - If the media driver has not answered the add command, then a AeronError is returned.
     * - If the media driver has successfully added the Counter then what is returned is the Counter.
     * - If the media driver has returned an error, this method will return the error.
     *
     * @see Aeron::addCounter
     *
     * @param registration_id of the Counter returned by Aeron::addCounter
     * @return Counter associated with the registration_id
     */
    pub fn find_counter(&mut self, registration_id: i64) -> Result<Arc<Mutex<Counter>>, AeronError> {
        self.conductor.find_counter(registration_id)
    }

    /**
     * Add a handler to the list to be called when a counter becomes available.
     *
     * @param handler to be added to the available counters list.
     */
    pub fn add_available_counter_handler(&mut self, handler: OnAvailableCounter){
        self.conductor.add_available_counter_handler(handler);
    }

    /**
     * Remove a handler from the list to be called when a counter becomes available.
     *
     * @param handler to be removed from the available counters list.
     */
    pub fn remove_available_counter_handler(&mut self, handler: OnAvailableCounter) {
        self.conductor.remove_available_counter_handler(handler);
    }

    /**
     * Add a handler to the list to be called when a counter becomes unavailable.
     *
     * @param handler to be added to the unavailable counters list.
     */
    pub fn add_unavailable_counter_handler(&mut self, handler: OnUnavailableCounter) {
        self.conductor.add_unavailable_counter_handler(handler);
    }

    /**
     * Remove a handler from the list to be called when a counter becomes unavailable.
     *
     * @param handler to be removed from the unavailable counters list.
     */
    pub fn remove_unavailable_counter_handler(&mut self, handler: OnUnavailableCounter) {
        self.conductor.remove_unavailable_counter_handler(handler);
    }

    /**
     * Add a handler to the list to be called when the client is closed.
     *
     * @param handler to be added to the close client handlers list.
     */
    pub fn add_close_client_handler(&mut self, handler: OnCloseClient) {
        self.conductor.add_close_client_handler(handler);
    }

    /**
     * Remove a handler from the list to be called when the client is closed.
     *
     * @param handler to be removed from the close client handlers list.
     */
    pub fn remove_close_client_handler(&mut self, handler: OnCloseClient) {
        self.conductor.remove_close_client_handler(handler);
    }

    /**
     * Return the AgentInvoker for the client conductor.
     *
     * @return AgentInvoker for the conductor.
     */
    pub fn conductorAgentInvoker(&self) -> &AgentInvoker<ClientConductor<'a>> {
        &self.conductor_invoker
    }

    /**
     * Return whether the AgentInvoker is used or not.
     *
     * @return true if AgentInvoker used or false if not.
     */
    pub fn uses_agent_invoker(&self) -> bool {
        self.context.use_conductor_agent_invoker()
    }

    /**
     * Get the CountersReader for the Aeron media driver counters.
     *
     * @return CountersReader for the Aeron media driver in use.
     */
    pub fn counters_reader(&self) -> &CountersReade {
        self.conductor.counters_reader()
    }

    /**
     * Get the client identity that has been allocated for communicating with the media driver.
     *
     * @return the client identity that has been allocated for communicating with the media driver.
     */
    pub fn client_id(&self) -> i64 {
        self.driver_proxy.client_id()
    }

    /**
     * Get the Aeron Context object used in construction of the Aeron instance.
     *
     * @return Context instance in use.
     */
    pub fn context(&self) -> &Context {
        self.context
    }

    /**
     * Return the static version and build string for the binary library.
     *
     * @return static version and build string for the binary library.
     */
    pub fn version(&self) -> String {
        String::from("aeron version 0.1")
    }


    pub fn map_cnc_file(context: &Context) -> Result<MemoryMappedFile, AeronError> {
        let start_ms = unix_time_ms();
        
        loop {
            while MemoryMappedFile::file_size(context.cnc_file_name()) <= 0 {

                if unix_time_ms() > start_ms + context.media_driver_timeout() {
                    return Err(AeronError::DriverTimeoutException(format!(
                        "CnC file not created: {}",
                        context.cnc_file_name()
                    )));
                }

                std::thread::sleep(Duration::from_millis(IDLE_SLEEP_MS_16));
            }

            let cnc_buffer = MemoryMappedFile::map_existing(context.cnc_file_name(), false)?;

            let mut cnc_version = cnc_file_descriptor::cnc_version_volatile(&cnc_buffer);

            while 0 == cnc_version {
                if unix_time_ms() > start_ms + context.media_driver_timeout() {
                    return Err(AeronError::DriverTimeoutException(format!(
                        "CnC file is created but not initialised: {}",
                        context.cnc_file_name()
                    )));
                }

                std::thread::sleep(Duration::from_millis(IDLE_SLEEP_MS_1));
                cnc_version = cnc_file_descriptor::cnc_version_volatile(&cnc_buffer);
            }

            if semantic_version_major(cnc_version) != semantic_version_major(cnc_file_descriptor::CNC_VERSION) {
                return Err(AeronError::GenericError(format!(
                    "Aeron CnC version does not match:  app={} file={}",
                    semantic_version_major(cnc_file_descriptor::CNC_VERSION),
                    semantic_version_major(cnc_version)
                )));
            }

            let to_driver_buffer = cnc_file_descriptor::create_to_driver_buffer(&cnc_buffer);
            let ring_buffer = ManyToOneRingBuffer::new(to_driver_buffer);

            while 0 == ring_buffer.consumer_heartbeat_time() {
                if unix_time_ms() > start_ms + context.media_driver_timeout() {
                    return Err(AeronError::DriverTimeoutException(format!(
                        "no driver heartbeat detected"
                    )));
                }

                std::thread::sleep(Duration::from_millis(IDLE_SLEEP_MS_1));
            }

            let time_ms = unix_time_ms();
            if (ring_buffer.consumer_heartbeat_time() as Moment) < time_ms - context.media_driver_timeout() {
                if time_ms > start_ms + context.media_driver_timeout() {
                    return Err(AeronError::DriverTimeoutException(format!(
                        "no driver heartbeat detected"
                    )));
                }

                std::thread::sleep(Duration::from_millis(IDLE_SLEEP_MS_100));
                continue; // make another startup try if not timed out
            }

            // If we are here and not returned with error earlier then we successfully connected to MD
            return Ok(cnc_buffer);
        }
    }
}

impl Drop for Aeron {
    fn drop(&mut self) {
        if self.context.use_conductor_agent_invoker() {
            self.conductor_invoker.close();
        } else {
            self.conductor_runner.close();
        }
    }
}

