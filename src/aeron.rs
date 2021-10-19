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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::distributions::Uniform;

use crate::utils::errors::{DriverInteractionError, GenericError};
use crate::utils::misc::semantic_version_to_string;
use crate::{
    client_conductor::ClientConductor,
    cnc_file_descriptor,
    concurrent::{
        agent_invoker::AgentInvoker,
        agent_runner::{AgentRunner, AgentStopper},
        atomic_buffer::AtomicBuffer,
        broadcast::{broadcast_receiver::BroadcastReceiver, copy_broadcast_receiver::CopyBroadcastReceiver},
        counters::CountersReader,
        ring_buffer::ManyToOneRingBuffer,
        strategies::SleepingIdleStrategy,
    },
    context::{Context, OnAvailableCounter, OnAvailableImage, OnCloseClient, OnUnavailableCounter, OnUnavailableImage},
    counter::Counter,
    driver_proxy::DriverProxy,
    exclusive_publication::ExclusivePublication,
    publication::Publication,
    subscription::Subscription,
    utils::{
        errors::AeronError,
        memory_mapped_file::MemoryMappedFile,
        misc::{semantic_version_major, unix_time_ms},
        types::Moment,
    },
};

/**
 * Aeron entry point for communicating to the Media Driver for creating {@link Publication}s and {@link Subscription}s.
 * Use a {@link Context} to configure the Aeron object.
 * <p>
 * A client application requires only one Aeron object per Media Driver.
 */

#[allow(dead_code)]
pub struct Aeron {
    session_id_distribution: rand::distributions::Uniform<i32>,

    context: Context,

    cnc_buffer: MemoryMappedFile, // May be it should be Arc ???

    to_driver_atomic_buffer: AtomicBuffer,
    to_clients_atomic_buffer: AtomicBuffer,
    counters_metadata_buffer: AtomicBuffer,
    counters_value_buffer: AtomicBuffer,

    to_driver_ring_buffer: Arc<ManyToOneRingBuffer>,
    driver_proxy: Arc<DriverProxy>, // need to use Arc here to avoid self-referencing inside Aeron

    to_clients_broadcast_receiver: Arc<Mutex<BroadcastReceiver>>, // This is passed to CopyBroadcastReceiver
    to_clients_copy_receiver: CopyBroadcastReceiver,

    conductor: Arc<Mutex<ClientConductor>>, // need mutable access to conductor
    idle_strategy: Arc<SleepingIdleStrategy>,
    conductor_stopper: Option<AgentStopper>,
    conductor_invoker: AgentInvoker<ClientConductor>,
}

const IDLE_SLEEP_MS: Moment = 4;
const IDLE_SLEEP_MS_1: Moment = 1;
const IDLE_SLEEP_MS_16: Moment = 16;
const IDLE_SLEEP_MS_100: Moment = 100;

impl Aeron {
    /**
     * Create an Aeron instance and connect to the media driver.
     * <p>
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     *
     * @param context for configuration of the client.
     */

    pub fn new(context: Context) -> Result<Self, AeronError> {
        // Most of Aeron internal field will be represented as Arc's to avoid self referencing.
        let cnc_buf = Self::map_cnc_file(&context)?;
        let local_to_driver_atomic_buffer = cnc_file_descriptor::create_to_driver_buffer(&cnc_buf);
        let local_to_clients_atomic_buffer = cnc_file_descriptor::create_to_clients_buffer(&cnc_buf);
        let local_counters_metadata_buffer = cnc_file_descriptor::create_counter_metadata_buffer(&cnc_buf);
        let local_counters_value_buffer = cnc_file_descriptor::create_counter_values_buffer(&cnc_buf);
        let local_to_driver_ring_buffer = Arc::new(ManyToOneRingBuffer::new(local_to_driver_atomic_buffer)?);
        let local_to_clients_broadcast_receiver = Arc::new(Mutex::new(BroadcastReceiver::new(local_to_clients_atomic_buffer)?));
        let local_driver_proxy = Arc::new(DriverProxy::new(local_to_driver_ring_buffer.clone()));
        let local_idle_strategy = Arc::new(SleepingIdleStrategy::new(IDLE_SLEEP_MS));
        let local_copy_broadcast_receiver = Arc::new(Mutex::new(CopyBroadcastReceiver::new(
            local_to_clients_broadcast_receiver.clone(),
        )));

        let local_conductor = ClientConductor::new(
            unix_time_ms,
            local_driver_proxy.clone(),
            local_copy_broadcast_receiver,
            local_counters_metadata_buffer,
            local_counters_value_buffer,
            context.new_publication_handler(),
            context.new_exclusive_publication_handler(),
            context.new_subscription_handler(),
            context.error_handler(),
            context.available_counter_handler(),
            context.unavailable_counter_handler(),
            context.close_client_handler(),
            context.media_driver_timeout(),
            context.resource_linger_timeout(),
            cnc_file_descriptor::client_liveness_timeout(&cnc_buf) as u64,
            context.pre_touch_mapped_memory(),
        );

        let use_agent_invoker = context.use_conductor_agent_invoker();

        let mut aeronchik = Self {
            session_id_distribution: Uniform::from(std::i32::MIN..std::i32::MAX),
            context: context.clone(),
            cnc_buffer: cnc_buf,
            to_driver_atomic_buffer: local_to_driver_atomic_buffer,
            to_clients_atomic_buffer: local_to_clients_atomic_buffer,
            counters_metadata_buffer: local_counters_metadata_buffer,
            counters_value_buffer: local_counters_value_buffer,
            to_driver_ring_buffer: local_to_driver_ring_buffer,
            driver_proxy: local_driver_proxy,
            to_clients_broadcast_receiver: local_to_clients_broadcast_receiver.clone(),
            to_clients_copy_receiver: CopyBroadcastReceiver::new(local_to_clients_broadcast_receiver),
            conductor: local_conductor.clone(),
            idle_strategy: local_idle_strategy.clone(),
            conductor_stopper: None,
            conductor_invoker: AgentInvoker::new(local_conductor.clone(), context.error_handler()),
        };

        let conductor_runner = AgentRunner::new(
            local_conductor,
            local_idle_strategy,
            context.error_handler(),
            &context.agent_name(),
        );

        if use_agent_invoker {
            aeronchik.conductor_invoker.start();
        } else {
            aeronchik.conductor_stopper = Some(AgentRunner::start(conductor_runner)?);
        }

        Ok(aeronchik)
    }

    /**
     * Indicate if the instance is closed and can not longer be used.
     *
     * @return true is the instance is closed and can no longer be used, otherwise false.
     */
    pub fn is_closed(&self) -> bool {
        self.conductor.lock().expect("Mutex poisoned").is_closed()
    }

    /**
     * Create an Aeron instance and connect to the media driver.
     * <p>
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     *
     * @param context for configuration of the client.
     * @return the new Aeron instance connected to the Media Driver.
     */
    pub fn connect_ctx(context: Context) -> Arc<Result<Aeron, AeronError>> {
        Arc::new(Aeron::new(context))
    }

    /**
     * Create an Aeron instance and connect to the media driver.
     * <p>
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     *
     * @return the new Aeron instance connected to the Media Driver.
     */
    pub fn connect() -> Arc<Result<Aeron, AeronError>> {
        Arc::new(Aeron::new(Context::new()))
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
    pub fn add_publication(&mut self, channel: CString, stream_id: i32) -> Result<i64, AeronError> {
        self.conductor
            .lock()
            .expect("Mutex poisoned")
            .add_publication(channel, stream_id)
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
        self.conductor
            .lock()
            .expect("Mutex poisoned")
            .find_publication(registration_id)
    }

    /**
     * Add an {@link ExclusivePublication} for publishing messages to subscribers from a single thread.
     *
     * @param channel  for sending the messages known to the media layer.
     * @param stream_id within the channel scope.
     * @return registration id for the publication
     */
    pub fn add_exclusive_publication(&mut self, channel: CString, stream_id: i32) -> Result<i64, AeronError> {
        self.conductor
            .lock()
            .expect("Mutex poisoned")
            .add_exclusive_publication(channel, stream_id)
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
    pub fn find_exclusive_publication(&mut self, registration_id: i64) -> Result<Arc<Mutex<ExclusivePublication>>, AeronError> {
        self.conductor
            .lock()
            .expect("Mutex poisoned")
            .find_exclusive_publication(registration_id)
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
    pub fn add_subscription(&mut self, channel: CString, stream_id: i32) -> Result<i64, AeronError> {
        self.conductor.lock().expect("Mutex poisoned").add_subscription(
            channel,
            stream_id,
            self.context.available_image_handler(),
            self.context.unavailable_image_handler(),
        )
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
    pub fn add_subscription_opt(
        &mut self,
        channel: CString,
        stream_id: i32,
        on_available_image_handler: Box<dyn OnAvailableImage>,
        on_unavailable_image_handler: Box<dyn OnUnavailableImage>,
    ) -> Result<i64, AeronError> {
        self.conductor.lock().expect("Mutex poisoned").add_subscription(
            channel,
            stream_id,
            on_available_image_handler,
            on_unavailable_image_handler,
        )
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
        self.conductor
            .lock()
            .expect("Mutex poisoned")
            .find_subscription(registration_id)
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
        self.conductor.lock().expect("Mutex poisoned").ensure_open()?;
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
    pub fn add_counter(&mut self, type_id: i32, key_buffer: &[u8], label: &str) -> Result<i64, AeronError> {
        self.conductor
            .lock()
            .expect("Mutex poisoned")
            .add_counter(type_id, key_buffer, label)
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
    pub fn find_counter(&mut self, registration_id: i64) -> Result<Arc<Counter>, AeronError> {
        self.conductor.lock().expect("Mutex poisoned").find_counter(registration_id)
    }

    /**
     * Add a handler to the list to be called when a counter becomes available.
     *
     * @param handler to be added to the available counters list.
     */
    pub fn add_available_counter_handler(&mut self, handler: Box<dyn OnAvailableCounter>) {
        let _ignored = self
            .conductor
            .lock()
            .expect("Mutex poisoned")
            .add_available_counter_handler(handler);
    }

    /**
     * Remove a handler from the list to be called when a counter becomes available.
     *
     * @param handler to be removed from the available counters list.
     */
    pub fn remove_available_counter_handler(&mut self, handler: Box<dyn OnAvailableCounter>) {
        let _ignored = self
            .conductor
            .lock()
            .expect("Mutex poisoned")
            .remove_available_counter_handler(handler);
    }

    /**
     * Add a handler to the list to be called when a counter becomes unavailable.
     *
     * @param handler to be added to the unavailable counters list.
     */
    pub fn add_unavailable_counter_handler(&mut self, handler: Box<dyn OnUnavailableCounter>) {
        let _ignored = self
            .conductor
            .lock()
            .expect("Mutex poisoned")
            .add_unavailable_counter_handler(handler);
    }

    /**
     * Remove a handler from the list to be called when a counter becomes unavailable.
     *
     * @param handler to be removed from the unavailable counters list.
     */
    pub fn remove_unavailable_counter_handler(&mut self, handler: Box<dyn OnUnavailableCounter>) {
        let _ignored = self
            .conductor
            .lock()
            .expect("Mutex poisoned")
            .remove_unavailable_counter_handler(handler);
    }

    /**
     * Add a handler to the list to be called when the client is closed.
     *
     * @param handler to be added to the close client handlers list.
     */
    pub fn add_close_client_handler(&mut self, handler: Box<dyn OnCloseClient>) {
        let _ignored = self
            .conductor
            .lock()
            .expect("Mutex poisoned")
            .add_close_client_handler(handler);
    }

    /**
     * Remove a handler from the list to be called when the client is closed.
     *
     * @param handler to be removed from the close client handlers list.
     */
    pub fn remove_close_client_handler(&mut self, handler: Box<dyn OnCloseClient>) {
        let _ignored = self
            .conductor
            .lock()
            .expect("Mutex poisoned")
            .remove_close_client_handler(handler);
    }

    /**
     * Return the AgentInvoker for the client conductor.
     *
     * @return AgentInvoker for the conductor.
     */
    pub fn conductor_agent_invoker(&self) -> &AgentInvoker<ClientConductor> {
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
    pub fn counters_reader(&self) -> Result<Arc<CountersReader>, AeronError> {
        self.conductor.lock().expect("Mutex poisoned").counters_reader()
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
        &self.context
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
            while MemoryMappedFile::get_file_size(context.cnc_file_name())? == 0 {
                if unix_time_ms() > start_ms + context.media_driver_timeout() {
                    return Err(DriverInteractionError::CncNotCreated {
                        file_name: context.cnc_file_name(),
                    }
                    .into());
                }

                std::thread::sleep(Duration::from_millis(IDLE_SLEEP_MS_16));
            }

            let cnc_buffer = MemoryMappedFile::map_existing(context.cnc_file_name(), false)?;

            let mut cnc_version = cnc_file_descriptor::cnc_version_volatile(&cnc_buffer);

            while 0 == cnc_version {
                if unix_time_ms() > start_ms + context.media_driver_timeout() {
                    return Err(DriverInteractionError::CncCreatedButNotInitialised {
                        file_name: context.cnc_file_name(),
                    }
                    .into());
                }

                std::thread::sleep(Duration::from_millis(IDLE_SLEEP_MS_1));
                cnc_version = cnc_file_descriptor::cnc_version_volatile(&cnc_buffer);
            }

            if semantic_version_major(cnc_version) != semantic_version_major(cnc_file_descriptor::CNC_VERSION) {
                return Err(GenericError::CncVersionDoesntMatch {
                    app_version: semantic_version_to_string(cnc_file_descriptor::CNC_VERSION),
                    file_version: semantic_version_to_string(cnc_version),
                }
                .into());
            }

            let to_driver_buffer = cnc_file_descriptor::create_to_driver_buffer(&cnc_buffer);
            let ring_buffer = ManyToOneRingBuffer::new(to_driver_buffer).expect("Error creating ring_buffer");

            while 0 == ring_buffer.consumer_heartbeat_time() {
                if unix_time_ms() > start_ms + context.media_driver_timeout() {
                    return Err(DriverInteractionError::NoHeartbeatDetected.into());
                }

                std::thread::sleep(Duration::from_millis(IDLE_SLEEP_MS_1));
            }

            let time_ms = unix_time_ms();
            if (ring_buffer.consumer_heartbeat_time() as Moment) < time_ms - context.media_driver_timeout() {
                if time_ms > start_ms + context.media_driver_timeout() {
                    return Err(DriverInteractionError::NoHeartbeatDetected.into());
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
            self.conductor_stopper.as_mut().unwrap().stop();
        }
    }
}
