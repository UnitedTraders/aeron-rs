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
use crate::utils::types::{Moment, MAX_MOMENT};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use std::ffi::{CStr, CString};

use crate::concurrent::agent_runner::Agent;
use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::concurrent::atomic_counter::AtomicCounter;
use crate::concurrent::broadcast::copy_broadcast_receiver::CopyBroadcastReceiver;
use crate::concurrent::counters;
use crate::concurrent::counters::CountersReader;
use crate::concurrent::logbuffer::term_reader::ErrorHandler;
use crate::concurrent::position::UnsafeBufferPosition;
use crate::concurrent::status::status_indicator_reader;
use crate::context::{
    OnAvailableCounter, OnAvailableImage, OnCloseClient, OnNewPublication, OnNewSubscription, OnUnavailableCounter,
    OnUnavailableImage,
};
use crate::counter::Counter;
use crate::driver_listener_adapter::{DriverListener, DriverListenerAdapter};
use crate::driver_proxy::DriverProxy;
use crate::exclusive_publication::ExclusivePublication;
use crate::heartbeat_timestamp;
use crate::image::Image;
use crate::publication::Publication;
use crate::subscription::Subscription;
use crate::utils::errors::AeronError;
use crate::utils::errors::AeronError::{ChannelEndpointException, ClientTimeoutException};
use crate::utils::log_buffers::LogBuffers;
use crate::utils::misc::CallbackGuard;

type EpochClock = fn() -> Moment;
type NanoClock = fn() -> Moment;

const KEEPALIVE_TIMEOUT_MS: Moment = 500;
const RESOURCE_TIMEOUT_MS: Moment = 1000;

// MediaDriver
#[derive(PartialEq, Debug)]
enum RegistrationStatus {
    Awaiting,
    Registered,
    Errored,
}

struct PublicationStateDefn {
    error_message: CString,
    buffers: Option<Arc<LogBuffers>>,       // PublicationStateDefn could be created without it
    publication: Option<Weak<Publication>>, // and then these fields will be set later.
    channel: CString,
    registration_id: i64,
    original_registration_id: i64,
    time_of_registration_ms: Moment,
    stream_id: i32,
    session_id: i32,
    publication_limit_counter_id: i32,
    channel_status_id: i32,
    error_code: i32,
    status: RegistrationStatus,
}

impl PublicationStateDefn {
    pub fn new(channel: CString, registration_id: i64, stream_id: i32, now_ms: Moment) -> Self {
        Self {
            error_message: CString::new("").unwrap(),
            buffers: None,
            publication: None,
            channel,
            registration_id,
            time_of_registration_ms: now_ms,
            stream_id,
            original_registration_id: -1,
            session_id: -1,
            publication_limit_counter_id: -1,
            channel_status_id: -1,
            error_code: -1,
            status: RegistrationStatus::Awaiting,
        }
    }
}

struct ExclusivePublicationStateDefn {
    error_message: CString,
    buffers: Option<Arc<LogBuffers>>,
    publication: Option<Weak<ExclusivePublication>>,
    channel: CString,
    registration_id: i64,
    // original_registration_id: i64,
    time_of_registration_ms: Moment,
    stream_id: i32,
    session_id: i32,
    publication_limit_counter_id: i32,
    channel_status_id: i32,
    error_code: i32,
    status: RegistrationStatus,
}

impl ExclusivePublicationStateDefn {
    pub fn new(channel: CString, registration_id: i64, stream_id: i32, now_ms: Moment) -> Self {
        Self {
            error_message: CString::new("").unwrap(),
            buffers: None,
            publication: None,
            channel,
            registration_id,
            time_of_registration_ms: now_ms,
            stream_id,
            // original_registration_id: -1,
            session_id: -1,
            publication_limit_counter_id: -1,
            channel_status_id: -1,
            error_code: -1,
            status: RegistrationStatus::Awaiting,
        }
    }
}

struct SubscriptionStateDefn {
    error_message: CString,
    subscription_cache: Option<Arc<Mutex<Subscription>>>,
    subscription: Option<Weak<Mutex<Subscription>>>,
    on_available_image_handler: OnAvailableImage,
    on_unavailable_image_handler: OnUnavailableImage,
    channel: CString,
    registration_id: i64,
    time_of_registration_ms: Moment,
    stream_id: i32,
    error_code: i32,
    status: RegistrationStatus,
}

impl SubscriptionStateDefn {
    pub fn new(
        channel: CString,
        registration_id: i64,
        stream_id: i32,
        now_ms: Moment,
        on_available_image_handler: OnAvailableImage,
        on_unavailable_image_handler: OnUnavailableImage,
    ) -> Self {
        Self {
            error_message: CString::new("").unwrap(),
            subscription_cache: None,
            subscription: None,
            on_available_image_handler,
            on_unavailable_image_handler,
            channel,
            registration_id,
            time_of_registration_ms: now_ms,
            stream_id,
            error_code: -1,
            status: RegistrationStatus::Awaiting,
        }
    }
}

struct CounterStateDefn {
    error_message: CString,
    counter_cache: Option<Arc<Counter>>,
    counter: Option<Weak<Counter>>,
    registration_id: i64,
    time_of_registration_ms: Moment,
    counter_id: i32,
    status: RegistrationStatus,
    error_code: i32,
}

impl CounterStateDefn {
    pub fn new(registration_id: i64, now_ms: Moment) -> Self {
        Self {
            error_message: CString::new("").unwrap(),
            counter_cache: None,
            counter: None,
            registration_id,
            time_of_registration_ms: now_ms,
            error_code: -1,
            status: RegistrationStatus::Awaiting,
            counter_id: 0,
        }
    }
}

struct ImageListLingerDefn {
    image_array: Vec<Image>,
    time_of_last_state_change_ms: Moment,
}

impl ImageListLingerDefn {
    pub fn new(now_ms: Moment, image_array: Vec<Image>) -> Self {
        Self {
            image_array,
            time_of_last_state_change_ms: now_ms,
        }
    }
}

struct LogBuffersDefn {
    log_buffers: Arc<LogBuffers>,
    time_of_last_state_change_ms: Moment,
}

impl LogBuffersDefn {
    pub fn new(buffers: Arc<LogBuffers>) -> Self {
        Self {
            log_buffers: buffers,
            time_of_last_state_change_ms: MAX_MOMENT,
        }
    }
}

struct DestinationStateDefn {
    error_message: CString,
    correlation_id: i64,
    registration_id: i64,
    time_of_registration_ms: Moment,
    error_code: i32,
    status: RegistrationStatus,
}

impl DestinationStateDefn {
    pub fn new(correlation_id: i64, registration_id: i64, now_ms: Moment) -> Self {
        Self {
            error_message: CString::new("").unwrap(),
            registration_id,
            correlation_id,
            time_of_registration_ms: now_ms,
            error_code: -1,
            status: RegistrationStatus::Awaiting,
        }
    }
}

pub struct ClientConductor {
    publication_by_registration_id: HashMap<i64, PublicationStateDefn>,
    exclusive_publication_by_registration_id: HashMap<i64, ExclusivePublicationStateDefn>,
    subscription_by_registration_id: HashMap<i64, SubscriptionStateDefn>,
    counter_by_registration_id: HashMap<i64, CounterStateDefn>,
    destination_state_by_correlation_id: HashMap<i64, DestinationStateDefn>,

    log_buffers_by_registration_id: HashMap<i64, LogBuffersDefn>,
    lingering_image_lists: Vec<ImageListLingerDefn>,

    driver_proxy: Arc<DriverProxy>,
    driver_listener_adapter: Option<DriverListenerAdapter<ClientConductor>>,

    counters_reader: CountersReader,
    counter_values_buffer: AtomicBuffer,

    on_new_publication_handler: OnNewPublication,
    on_new_exclusive_publication_handler: OnNewPublication,
    on_new_subscription_handler: OnNewSubscription,
    error_handler: ErrorHandler,

    on_available_counter_handlers: Vec<OnAvailableCounter>,
    on_unavailable_counter_handlers: Vec<OnUnavailableCounter>,
    on_close_client_handlers: Vec<OnCloseClient>,

    epoch_clock: EpochClock,
    driver_timeout_ms: Moment,
    resource_linger_timeout_ms: Moment,
    inter_service_timeout_ms: Moment,
    pre_touch_mapped_memory: bool,
    is_in_callback: bool,
    driver_active: AtomicBool,
    is_closed: AtomicBool,
    admin_lock: Mutex<()>,
    heartbeat_timestamp: Option<Box<AtomicCounter>>,

    time_of_last_do_work_ms: Moment,
    time_of_last_keepalive_ms: Moment,
    time_of_last_check_managed_resources_ms: Moment,

    arced_self: Option<Arc<Mutex<ClientConductor>>>,

    padding: [u8; crate::utils::misc::CACHE_LINE_LENGTH as usize],
}

impl ClientConductor {
    pub fn new(
        epoch_clock: EpochClock,
        driver_proxy: Arc<DriverProxy>,
        broadcast_receiver: Arc<Mutex<CopyBroadcastReceiver>>,
        counter_metadata_buffer: AtomicBuffer,
        counter_values_buffer: AtomicBuffer,
        on_new_publication_handler: OnNewPublication,
        on_new_exclusive_publication_handler: OnNewPublication,
        on_new_subscription_handler: OnNewSubscription,
        error_handler: ErrorHandler,
        on_available_counter_handler: OnAvailableCounter,
        on_unavailable_counter_handler: OnUnavailableCounter,
        on_close_client_handler: OnCloseClient,
        driver_timeout_ms: Moment,
        resource_linger_timeout_ms: Moment,
        inter_service_timeout_ns: Moment,
        pre_touch_mapped_memory: bool,
    ) -> Arc<Mutex<Self>> {
        let mut selfy = Self {
            publication_by_registration_id: Default::default(),
            exclusive_publication_by_registration_id: Default::default(),
            subscription_by_registration_id: Default::default(),
            counter_by_registration_id: Default::default(),
            destination_state_by_correlation_id: Default::default(),
            log_buffers_by_registration_id: Default::default(),
            lingering_image_lists: vec![],
            driver_proxy,
            driver_listener_adapter: None,
            counters_reader: CountersReader::new(counter_metadata_buffer, counter_values_buffer),
            counter_values_buffer,
            on_new_publication_handler,
            on_new_exclusive_publication_handler,
            on_new_subscription_handler,
            error_handler,
            on_available_counter_handlers: vec![],
            on_unavailable_counter_handlers: vec![],
            on_close_client_handlers: vec![],
            epoch_clock,
            driver_timeout_ms,
            resource_linger_timeout_ms,
            inter_service_timeout_ms: inter_service_timeout_ns / 1_000_000,
            pre_touch_mapped_memory,
            is_in_callback: false,
            driver_active: AtomicBool::from(true),
            is_closed: AtomicBool::from(false),
            admin_lock: Mutex::new(()),
            heartbeat_timestamp: None,
            time_of_last_do_work_ms: epoch_clock(),
            time_of_last_keepalive_ms: epoch_clock(),
            time_of_last_check_managed_resources_ms: epoch_clock(),
            arced_self: None,
            padding: [0; crate::utils::misc::CACHE_LINE_LENGTH as usize],
        };

        selfy.on_available_counter_handlers.push(on_available_counter_handler);
        selfy.on_unavailable_counter_handlers.push(on_unavailable_counter_handler);
        selfy.on_close_client_handlers.push(on_close_client_handler);

        let arc_selfy = Arc::new(Mutex::new(selfy));
        let another_selfy = arc_selfy.clone();
        let mut another_selfy_mut = another_selfy.lock().expect("Mutex poisoned");
        another_selfy_mut.driver_listener_adapter = Some(DriverListenerAdapter::new(broadcast_receiver, arc_selfy.clone()));
        another_selfy_mut.arced_self = Some(arc_selfy.clone());

        arc_selfy
    }

    pub fn counters_reader(&self) -> Result<&CountersReader, AeronError> {
        self.ensure_open()?;
        Ok(&self.counters_reader)
    }

    pub fn channel_status(&self, counter_id: i32) -> i64 {
        match counter_id {
            0 => status_indicator_reader::CHANNEL_ENDPOINT_INITIALIZING,
            status_indicator_reader::NO_ID_ALLOCATED => status_indicator_reader::CHANNEL_ENDPOINT_ACTIVE,
            _ => self
                .counters_reader
                .counter_value(counter_id)
                .expect("Error getting counter value") as i64,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Acquire)
    }

    pub fn ensure_open(&self) -> Result<(), AeronError> {
        if self.is_closed() {
            Err(AeronError::GenericError(String::from("Aeron client conductor is closed")))
        } else {
            Ok(())
        }
    }

    fn on_heartbeat_check_timeouts(&mut self) -> Result<i64, AeronError> {
        let now_ms = (self.epoch_clock)();
        let mut result: i64 = 0;

        if now_ms > self.time_of_last_do_work_ms + self.inter_service_timeout_ms {
            self.close_all_resources(now_ms);

            let err = AeronError::ConductorServiceTimeout(format!(
                "timeout between service calls over {} ms",
                self.inter_service_timeout_ms
            ));

            (self.error_handler)(err);
        }

        self.time_of_last_do_work_ms = now_ms;

        if now_ms > self.time_of_last_keepalive_ms + KEEPALIVE_TIMEOUT_MS {
            if now_ms > self.driver_proxy.time_of_last_driver_keepalive() as Moment + self.driver_timeout_ms {
                self.driver_active.store(false, Ordering::SeqCst);

                let err = AeronError::ConductorServiceTimeout(format!(
                    "driver has been inactive for over {} ms",
                    self.driver_timeout_ms
                ));

                (self.error_handler)(err);
            }

            let client_id = self.driver_proxy.client_id();
            if let Some(heartbeat_timestamp) = &self.heartbeat_timestamp {
                if heartbeat_timestamp::is_active(
                    &self.counters_reader,
                    heartbeat_timestamp.id(),
                    heartbeat_timestamp::CLIENT_HEARTBEAT_TYPE_ID,
                    client_id,
                ) {
                    heartbeat_timestamp.set_ordered(now_ms as i64);
                } else {
                    self.close_all_resources(now_ms);

                    let err = AeronError::GenericError(String::from("client heartbeat timestamp not active"));

                    (self.error_handler)(err);
                }
            } else {
                let counter_id = heartbeat_timestamp::find_counter_id_by_registration_id(
                    &self.counters_reader,
                    heartbeat_timestamp::CLIENT_HEARTBEAT_TYPE_ID,
                    client_id,
                );

                if let Some(id) = counter_id {
                    let new_counter = Box::new(AtomicCounter::new(self.counter_values_buffer, id));
                    new_counter.set_ordered(now_ms as i64);
                    self.heartbeat_timestamp = Some(new_counter);
                }
            }

            self.time_of_last_keepalive_ms = now_ms;
            result = 1;
        }

        if now_ms > self.time_of_last_check_managed_resources_ms + RESOURCE_TIMEOUT_MS {
            self.on_check_managed_resources(now_ms);
            self.time_of_last_check_managed_resources_ms = now_ms;
            result = 1;
        }

        Ok(result)
    }

    pub fn verify_driver_is_active(&self) -> Result<(), AeronError> {
        if !self.driver_active.load(Ordering::SeqCst) {
            Err(AeronError::DriverTimeout(String::from("driver is inactive")))
        } else {
            Ok(())
        }
    }

    pub fn verify_driver_is_active_via_error_handler(&self) {
        if !self.driver_active.load(Ordering::SeqCst) {
            let err = AeronError::DriverTimeout(String::from("driver is inactive"));
            (self.error_handler)(err);
        }
    }

    pub fn ensure_not_reentrant(&self) {
        if self.is_in_callback {
            let err = AeronError::ReentrantException(String::from("client cannot be invoked within callback"));
            (self.error_handler)(err);
        }
    }

    // Returns thread safe shared mutable instance of LogBuffers
    pub fn get_log_buffers(
        &mut self,
        registration_id: i64,
        log_filename: CString,
        channel: CString,
    ) -> Result<Arc<LogBuffers>, AeronError> {
        if let Some(lb) = self.log_buffers_by_registration_id.get_mut(&registration_id) {
            lb.time_of_last_state_change_ms = MAX_MOMENT;
            Ok(lb.log_buffers.clone())
        } else {
            let touch = self.pre_touch_mapped_memory && !channel.to_string_lossy().contains("sparse=true");
            let log_buffer = LogBuffers::from_existing(log_filename.to_str().expect("CString conv error"), touch)?;

            let log_buffers = Arc::new(log_buffer);
            self.log_buffers_by_registration_id
                .insert(registration_id, LogBuffersDefn::new(log_buffers.clone()));

            Ok(log_buffers)
        }
    }

    pub fn current_time_millis() -> Moment {
        crate::utils::misc::unix_time_ms()
    }

    pub fn system_nano_clock() -> Moment {
        crate::utils::misc::unix_time_ns()
    }

    // This function returns address
    /*
    template<typename T, typename... U>
    static size_t getAddress(const std::function<T(U...)>& f)
    {
    typedef T(fnType)(U...);
    auto fnPointer = f.template target<fnType*>();

    return (size_t)*fnPointer;
    }
    */
    pub fn add_publication(&mut self, channel: CString, stream_id: i32) -> Result<i64, AeronError> {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in add_publication");
        self.verify_driver_is_active()?;
        self.ensure_not_reentrant();
        self.ensure_open()?;

        let registration_id = self.driver_proxy.add_publication(channel.clone(), stream_id)?;

        self.publication_by_registration_id.insert(
            registration_id,
            PublicationStateDefn::new(channel, registration_id, stream_id, (self.epoch_clock)()),
        );

        Ok(registration_id)
    }

    pub fn find_publication(&mut self, registration_id: i64) -> Result<Arc<Publication>, AeronError> {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in find_publication");
        self.ensure_not_reentrant();
        self.ensure_open()?;

        // These two tricky fields are needed to avoid double mut borrows of publication_by_registration_id
        let mut publication_to_remove: Option<i64> = None;
        let mut error_to_return: AeronError = AeronError::GenericError(String::from("Doesn't matter"));

        let result = if let Some(state) = self.publication_by_registration_id.get_mut(&registration_id) {
            // try to upgrade weak ptr to strong one and use it
            if let Some(maybe_publication) = &state.publication {
                if let Some(publication) = maybe_publication.upgrade() {
                    match state.status {
                        RegistrationStatus::Awaiting => {
                            if (self.epoch_clock)() > state.time_of_registration_ms + self.driver_timeout_ms {
                                return Err(AeronError::ConductorServiceTimeout(format!(
                                    "no response from driver in {} ms",
                                    self.driver_timeout_ms
                                )));
                            }
                        }
                        RegistrationStatus::Registered => {
                            let publication_limit =
                                UnsafeBufferPosition::new(self.counter_values_buffer, state.publication_limit_counter_id);

                            if let Some(buffers) = &state.buffers {
                                let self_for_pub = self.arced_self.as_ref().unwrap();
                                let publication = Publication::new(
                                    self_for_pub.clone(),
                                    state.channel.clone(),
                                    state.registration_id,
                                    state.original_registration_id,
                                    state.stream_id,
                                    state.session_id,
                                    publication_limit,
                                    state.channel_status_id,
                                    buffers.clone(),
                                );

                                let new_pub = Arc::new(publication);
                                state.publication = Some(Arc::downgrade(&new_pub));
                            } else {
                                return Err(AeronError::GenericError(format!(
                                    "buffers was not set for Publication with registration_id {}",
                                    state.registration_id
                                )));
                            }
                        }

                        RegistrationStatus::Errored => {
                            publication_to_remove = Some(registration_id);
                            error_to_return = ClientConductor::return_registration_error(state.error_code, &state.error_message);
                        }
                    }
                    Ok(publication)
                } else {
                    Err(AeronError::GenericError(String::from("publication already dropped")))
                }
            } else {
                Err(AeronError::GenericError(String::from("publication is None")))
            }
        } else {
            // error, publication not found
            Err(AeronError::GenericError(String::from("publication not found")))
        };

        if let Some(id) = publication_to_remove {
            self.publication_by_registration_id.remove(&id);
            return Err(error_to_return);
        }

        result
    }

    pub fn return_registration_error(err_code: i32, err_message: &CStr) -> AeronError {
        AeronError::RegistrationException(format!(
            "error code {}, error message: {}",
            err_code,
            err_message.to_str().expect("CStr conversion error")
        ))
    }

    pub fn release_publication(&mut self, registration_id: i64) {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in release_publication");
        self.verify_driver_is_active_via_error_handler();

        if let Some(_publication) = self.publication_by_registration_id.get(&registration_id) {
            let _result = self.driver_proxy.remove_publication(registration_id);
            self.publication_by_registration_id.remove(&registration_id);
        }
    }

    pub fn add_exclusive_publication(&mut self, channel: CString, stream_id: i32) -> Result<i64, AeronError> {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in add_exclusive_publication");
        self.verify_driver_is_active()?;
        self.ensure_not_reentrant();
        self.ensure_open()?;

        let registration_id = self.driver_proxy.add_exclusive_publication(channel.clone(), stream_id)?;

        self.exclusive_publication_by_registration_id.insert(
            registration_id,
            ExclusivePublicationStateDefn::new(channel, registration_id, stream_id, (self.epoch_clock)()),
        );

        Ok(registration_id)
    }

    // TODO: looks like it could be made generic together with find_publication()
    pub(crate) fn find_exclusive_publication(&mut self, registration_id: i64) -> Result<Arc<ExclusivePublication>, AeronError> {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in find_exclusive_publication");
        self.ensure_not_reentrant();
        self.ensure_open()?;

        // These two tricky fields are needed to avoid double mut borrows of publication_by_registration_id
        let mut publication_to_remove: Option<i64> = None;
        let mut error_to_return: AeronError = AeronError::GenericError(String::from("Doesn't matter"));

        let result = if let Some(state) = self.exclusive_publication_by_registration_id.get_mut(&registration_id) {
            // try to upgrade weak ptr to strong one and use it

            if let Some(maybe_publication) = &state.publication {
                if let Some(publication) = maybe_publication.upgrade() {
                    match state.status {
                        RegistrationStatus::Awaiting => {
                            if (self.epoch_clock)() > state.time_of_registration_ms + self.driver_timeout_ms {
                                return Err(AeronError::ConductorServiceTimeout(format!(
                                    "no response from driver in {} ms",
                                    self.driver_timeout_ms
                                )));
                            }
                        }
                        RegistrationStatus::Registered => {
                            let publication_limit =
                                UnsafeBufferPosition::new(self.counter_values_buffer, state.publication_limit_counter_id);

                            if let Some(buffers) = &state.buffers {
                                let publication = ExclusivePublication::new(
                                    self.arced_self.as_ref().unwrap().clone(),
                                    state.channel.clone(),
                                    state.registration_id,
                                    state.stream_id,
                                    state.session_id,
                                    publication_limit,
                                    state.channel_status_id,
                                    buffers.clone(),
                                );

                                let new_pub = Arc::new(publication);
                                state.publication = Some(Arc::downgrade(&new_pub));
                            } else {
                                return Err(AeronError::GenericError(format!(
                                    "buffers was not set for ExclusivePublication with registration_id {}",
                                    state.registration_id
                                )));
                            }
                        }

                        RegistrationStatus::Errored => {
                            publication_to_remove = Some(registration_id);
                            error_to_return = ClientConductor::return_registration_error(state.error_code, &state.error_message);
                        }
                    }
                    Ok(publication)
                } else {
                    Err(AeronError::GenericError(String::from("publication already dropped")))
                }
            } else {
                Err(AeronError::GenericError(String::from("exclusive publication is None")))
            }
        } else {
            Err(AeronError::GenericError(String::from("publication not found")))
        };

        if let Some(id) = publication_to_remove {
            self.exclusive_publication_by_registration_id.remove(&id);
            return Err(error_to_return);
        }

        result
    }

    pub fn release_exclusive_publication(&mut self, registration_id: i64) {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in release_exclusive_publication");
        self.verify_driver_is_active_via_error_handler();

        if let Some(_publication) = self.publication_by_registration_id.get(&registration_id) {
            let _result = self.driver_proxy.remove_publication(registration_id);
            self.exclusive_publication_by_registration_id.remove(&registration_id);
        }
    }

    pub fn add_subscription(
        &mut self,
        channel: CString,
        stream_id: i32,
        on_available_image_handler: OnAvailableImage,
        on_unavailable_image_handler: OnUnavailableImage,
    ) -> Result<i64, AeronError> {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in add_subscription");
        self.verify_driver_is_active()?;
        self.ensure_not_reentrant();
        self.ensure_open()?;

        let registration_id = self.driver_proxy.add_subscription(channel.clone(), stream_id)?;

        self.subscription_by_registration_id.insert(
            registration_id,
            SubscriptionStateDefn::new(
                channel,
                registration_id,
                stream_id,
                (self.epoch_clock)(),
                on_available_image_handler,
                on_unavailable_image_handler,
            ),
        );

        Ok(registration_id)
    }

    pub fn find_subscription(&mut self, registration_id: i64) -> Result<Arc<Mutex<Subscription>>, AeronError> {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in find_subscription");
        self.ensure_not_reentrant();
        self.ensure_open()?;

        // These two tricky fields are needed to avoid double mut borrows of publication_by_registration_id
        let mut subscription_to_remove: Option<i64> = None;
        let mut error_to_return: AeronError = AeronError::GenericError(String::from("Doesn't matter"));

        let result = if let Some(state) = self.subscription_by_registration_id.get_mut(&registration_id) {
            state.subscription_cache = None;

            // try to upgrade weak ptr to strong one and use it
            if let Some(maybe_subscription) = &state.subscription {
                if let Some(subscription) = maybe_subscription.upgrade() {
                    Ok(subscription)
                } else {
                    // subscription has been dropped already
                    if RegistrationStatus::Awaiting == state.status {
                        if (self.epoch_clock)() > state.time_of_registration_ms + self.driver_timeout_ms {
                            return Err(AeronError::DriverTimeout(format!(
                                "no response from driver in {} ms",
                                self.driver_timeout_ms
                            )));
                        }
                    } else if RegistrationStatus::Errored == state.status {
                        subscription_to_remove = Some(registration_id);
                        error_to_return = ClientConductor::return_registration_error(state.error_code, &state.error_message);
                    }

                    Err(AeronError::GenericError(String::from(
                        "subscription has been dropped already",
                    )))
                }
            } else {
                Err(AeronError::GenericError(String::from("subscription is None")))
            }
        } else {
            Err(AeronError::GenericError(String::from("subscription not found")))
        };

        if let Some(id) = subscription_to_remove {
            self.subscription_by_registration_id.remove(&id);
            return Err(error_to_return);
        }

        result
    }

    pub fn release_subscription(&mut self, registration_id: i64, mut images: Vec<Image>) {
        //let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in release_subscription"); FIXME is it needed?
        self.verify_driver_is_active_via_error_handler();

        let mut is_remove_subscription = false;
        if let Some(subscription) = self.subscription_by_registration_id.get(&registration_id) {
            is_remove_subscription = true;
            let _result = self.driver_proxy.remove_subscription(registration_id);

            for image in images.iter_mut() {
                // close the image
                image.close();

                let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
                (subscription.on_unavailable_image_handler)(&image);
            }
        }

        if is_remove_subscription {
            self.linger_all_resources((self.epoch_clock)(), images);
            self.subscription_by_registration_id.remove(&registration_id);
        }
    }

    pub fn add_counter(&mut self, type_id: i32, key_buffer: &[u8], label: &str) -> Result<i64, AeronError> {
        let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in add_counter");
        self.verify_driver_is_active()?;
        self.ensure_not_reentrant();
        self.ensure_open()?;

        if key_buffer.len() > counters::MAX_KEY_LENGTH as usize {
            return Err(AeronError::IllegalArgumentException(format!(
                "key length out of bounds: {}",
                key_buffer.len()
            )));
        }

        if label.len() > counters::MAX_LABEL_LENGTH as usize {
            return Err(AeronError::IllegalArgumentException(format!(
                "label length out of bounds: {}",
                label.len()
            )));
        }

        let registration_id = self
            .driver_proxy
            .add_counter(type_id, key_buffer, CString::new(label).unwrap())?;

        self.counter_by_registration_id
            .insert(registration_id, CounterStateDefn::new(registration_id, (self.epoch_clock)()));

        Ok(registration_id)
    }

    pub fn find_counter(&mut self, registration_id: i64) -> Result<Arc<Counter>, AeronError> {
        let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in find_counter");
        self.ensure_not_reentrant();
        self.ensure_open()?;

        // These two tricky fields are needed to avoid double mut borrows of publication_by_registration_id
        let mut counter_to_remove: Option<i64> = None;
        let mut error_to_return: AeronError = AeronError::GenericError(String::from("Doesn't matter"));

        let result = if let Some(state) = self.counter_by_registration_id.get_mut(&registration_id) {
            state.counter_cache = None;

            // try to upgrade weak ptr to strong one and use it
            if let Some(maybe_counter) = &state.counter {
                if let Some(counter) = maybe_counter.upgrade() {
                    Ok(counter)
                } else {
                    // counter has been dropped already
                    if RegistrationStatus::Awaiting == state.status {
                        if (self.epoch_clock)() > state.time_of_registration_ms + self.driver_timeout_ms {
                            return Err(AeronError::DriverTimeout(format!(
                                "no response from driver in {} ms",
                                self.driver_timeout_ms
                            )));
                        }
                    } else if RegistrationStatus::Errored == state.status {
                        counter_to_remove = Some(registration_id);
                        error_to_return = ClientConductor::return_registration_error(state.error_code, &state.error_message);
                    }

                    Err(AeronError::GenericError(String::from("counter has been dropped already")))
                }
            } else {
                Err(AeronError::GenericError(String::from("counter is None")))
            }
        } else {
            Err(AeronError::GenericError(String::from("counter not found")))
        };

        if let Some(id) = counter_to_remove {
            self.counter_by_registration_id.remove(&id);
            return Err(error_to_return);
        }

        result
    }

    pub fn release_counter(&mut self, registration_id: i64) -> Result<(), AeronError> {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in release_counter");
        self.verify_driver_is_active_via_error_handler();

        if let Some(_counter) = self.counter_by_registration_id.get(&registration_id) {
            self.driver_proxy.remove_counter(registration_id)?;
            self.counter_by_registration_id.remove(&registration_id);
        }

        Ok(())
    }

    pub fn add_destination(&mut self, publication_registration_id: i64, endpoint_channel: CString) -> Result<i64, AeronError> {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in add_destination");
        self.verify_driver_is_active()?;
        self.ensure_not_reentrant();
        self.ensure_open()?;

        let correlation_id = self
            .driver_proxy
            .add_destination(publication_registration_id, endpoint_channel)?;

        self.destination_state_by_correlation_id.insert(
            correlation_id,
            DestinationStateDefn::new(correlation_id, publication_registration_id, (self.epoch_clock)()),
        );

        Ok(correlation_id)
    }

    pub fn remove_destination(&mut self, publication_registration_id: i64, endpoint_channel: CString) -> Result<i64, AeronError> {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in remove_destination");
        self.verify_driver_is_active()?;
        self.ensure_not_reentrant();
        self.ensure_open()?;

        let correlation_id = self
            .driver_proxy
            .remove_destination(publication_registration_id, endpoint_channel)?;

        // FIXME: the code is ported from C++ as is. But it seems there is a bug. We need to remove destination from
        // destination_state_by_correlation_id instead of inserting.
        self.destination_state_by_correlation_id.insert(
            correlation_id,
            DestinationStateDefn::new(correlation_id, publication_registration_id, (self.epoch_clock)()),
        );

        Ok(correlation_id)
    }

    pub fn add_rcv_destination(
        &mut self,
        subscription_registration_id: i64,
        endpoint_channel: CString,
    ) -> Result<i64, AeronError> {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in add_rcv_destination");
        self.verify_driver_is_active()?;
        self.ensure_not_reentrant();
        self.ensure_open()?;

        let correlation_id = self
            .driver_proxy
            .add_rcv_destination(subscription_registration_id, endpoint_channel)?;

        self.destination_state_by_correlation_id.insert(
            correlation_id,
            DestinationStateDefn::new(correlation_id, subscription_registration_id, (self.epoch_clock)()),
        );

        Ok(correlation_id)
    }

    pub fn remove_rcv_destination(
        &mut self,
        subscription_registration_id: i64,
        endpoint_channel: CString,
    ) -> Result<i64, AeronError> {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in remove_rcv_destination");
        self.verify_driver_is_active()?;
        self.ensure_not_reentrant();
        self.ensure_open()?;

        let correlation_id = self
            .driver_proxy
            .remove_rcv_destination(subscription_registration_id, endpoint_channel)?;

        self.destination_state_by_correlation_id.insert(
            correlation_id,
            DestinationStateDefn::new(correlation_id, subscription_registration_id, (self.epoch_clock)()),
        );

        Ok(correlation_id)
    }

    pub fn find_destination_response(&mut self, correlation_id: i64) -> Result<bool, AeronError> {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in find_destination_response");
        self.ensure_not_reentrant();
        self.ensure_open()?;

        let destination_to_remove: Option<i64> = None;

        let result = if let Some(state) = self.destination_state_by_correlation_id.get_mut(&correlation_id) {
            match state.status {
                RegistrationStatus::Awaiting => {
                    if (self.epoch_clock)() > state.time_of_registration_ms + self.driver_timeout_ms {
                        Err(AeronError::ConductorServiceTimeout(format!(
                            "no response from driver in {} ms",
                            self.driver_timeout_ms
                        )))
                    } else {
                        Ok(false)
                    }
                }
                RegistrationStatus::Registered => Ok(true),
                RegistrationStatus::Errored => Err(ClientConductor::return_registration_error(
                    state.error_code,
                    &state.error_message,
                )),
            }
        } else {
            Err(AeronError::GenericError(String::from("correlation_id unknown")))
        };

        if let Some(id) = destination_to_remove {
            // Regardless of status remove this destination from the map
            self.destination_state_by_correlation_id.remove(&id);
        }

        result
    }

    pub fn add_available_counter_handler(&mut self, handler: OnAvailableCounter) -> Result<(), AeronError> {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in add_available_counter_handler");
        self.ensure_not_reentrant();
        self.ensure_open()?;

        self.on_available_counter_handlers.push(handler);
        Ok(())
    }

    pub fn remove_available_counter_handler(&mut self, _handler: OnAvailableCounter) -> Result<(), AeronError> {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in remove_available_counter_handler");
        self.ensure_not_reentrant();
        self.ensure_open()?;

        // self.on_available_counter_handlers.retain(|item| item as usize != handler as usize); FIXME: add registration ID for handlers
        Ok(())
    }

    pub fn add_unavailable_counter_handler(&mut self, handler: OnUnavailableCounter) -> Result<(), AeronError> {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in add_unavailable_counter_handler");
        self.ensure_not_reentrant();
        self.ensure_open()?;

        self.on_unavailable_counter_handlers.push(handler);
        Ok(())
    }

    pub fn remove_unavailable_counter_handler(&mut self, _handler: OnUnavailableCounter) -> Result<(), AeronError> {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in remove_unavailable_counter_handler");
        self.ensure_not_reentrant();
        self.ensure_open()?;
        //self.on_unavailable_counter_handlers.retain(|item| item != handler); FIXME
        Ok(())
    }

    pub fn add_close_client_handler(&mut self, handler: OnCloseClient) -> Result<(), AeronError> {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in find_publication");
        self.ensure_not_reentrant();
        self.ensure_open()?;

        self.on_close_client_handlers.push(handler);
        Ok(())
    }

    pub fn remove_close_client_handler(&mut self, _handler: OnCloseClient) -> Result<(), AeronError> {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in find_publication");
        self.ensure_not_reentrant();
        self.ensure_open()?;

        //self.on_close_client_handlers.retain(|item| item != handler); FIXME
        Ok(())
    }

    pub fn close_all_resources(&mut self, now_ms: Moment) {
        self.is_closed.store(true, Ordering::Release);

        for pub_defn in self.publication_by_registration_id.values() {
            if let Some(maybe_publication) = &pub_defn.publication {
                if let Some(publication) = maybe_publication.upgrade() {
                    publication.close();
                }
            }
        }
        self.publication_by_registration_id.clear();

        for pub_defn in self.exclusive_publication_by_registration_id.values() {
            if let Some(maybe_publication) = &pub_defn.publication {
                if let Some(publication) = maybe_publication.upgrade() {
                    publication.close();
                }
            }
        }
        self.exclusive_publication_by_registration_id.clear();

        let mut subscriptions_to_hold_until_cleared: Vec<Arc<Mutex<Subscription>>> = Vec::default();

        let mut images_to_linger: Vec<Vec<Image>> = Vec::new();

        for sub_defn in self.subscription_by_registration_id.values_mut() {
            if let Some(maybe_subscription) = &sub_defn.subscription {
                if let Some(subscription) = maybe_subscription.upgrade() {
                    if let Some(mut images) = subscription.lock().expect("Mutex poisoned").close_and_remove_images() {
                        for image in images.iter_mut() {
                            image.close();

                            let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
                            (sub_defn.on_unavailable_image_handler)(&image);
                        }
                        images_to_linger.push(images);
                    }

                    if let Some(cache) = &sub_defn.subscription_cache {
                        subscriptions_to_hold_until_cleared.push(cache.clone());
                        sub_defn.subscription_cache = None;
                    }
                }
            }
        }

        for images in images_to_linger {
            self.linger_all_resources(now_ms, images);
        }

        self.subscription_by_registration_id.clear();

        let mut counters_to_hold_until_cleared: Vec<Arc<Counter>> = Vec::default();

        for cnt_defn in self.counter_by_registration_id.values_mut() {
            if let Some(maybe_counter) = &cnt_defn.counter {
                if let Some(counter) = maybe_counter.upgrade() {
                    counter.close();
                    let registration_id = counter.registration_id();
                    let counter_id = counter.id();

                    for handler in &self.on_unavailable_counter_handlers {
                        let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
                        handler(&self.counters_reader, registration_id, counter_id);
                    }

                    if let Some(cache) = &cnt_defn.counter_cache {
                        counters_to_hold_until_cleared.push(cache.clone());
                        cnt_defn.counter_cache = None;
                    }
                }
            }
        }
        self.counter_by_registration_id.clear();

        for handler in &self.on_close_client_handlers {
            let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
            handler();
        }
    }

    pub fn on_check_managed_resources(&mut self, now_ms: Moment) {
        //let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in self.on_check_managed_resources"); FIXME: is it needed?

        let mut log_buffers_to_remove: Vec<i64> = Vec::new();
        for (id, mut entry) in &mut self.log_buffers_by_registration_id {
            if Arc::strong_count(&entry.log_buffers) == 1 {
                if MAX_MOMENT == entry.time_of_last_state_change_ms {
                    entry.time_of_last_state_change_ms = now_ms;
                } else if now_ms - self.resource_linger_timeout_ms > entry.time_of_last_state_change_ms {
                    log_buffers_to_remove.push(*id);
                }
            }
        }

        // Remove marked log buffers
        let _removed: Vec<Option<LogBuffersDefn>> = log_buffers_to_remove
            .into_iter()
            .map(|id| self.log_buffers_by_registration_id.remove(&id))
            .collect();

        //remove outdated lingering Images
        let resource_linger_timeout_ms = self.resource_linger_timeout_ms;
        self.lingering_image_lists
            .retain(|img| now_ms - resource_linger_timeout_ms <= img.time_of_last_state_change_ms);
    }

    pub fn linger_resource(&mut self, now_ms: Moment, images: Vec<Image>) {
        self.lingering_image_lists.push(ImageListLingerDefn::new(now_ms, images));
    }

    pub fn linger_all_resources(&mut self, now_ms: Moment, images: Vec<Image>) {
        self.linger_resource(now_ms, images);
    }
}

impl Agent for ClientConductor {
    fn on_start(&mut self) -> Result<(), AeronError> {
        // Empty
        Ok(())
    }

    fn do_work(&mut self) -> Result<i32, AeronError> {
        let mut work_count = 0;

        work_count += self.driver_listener_adapter.as_mut().unwrap().receive_messages()?; // driver_listener_adapter must be Some here!
        work_count += self.on_heartbeat_check_timeouts()? as usize;
        Ok(work_count as i32)
    }

    fn on_close(&mut self) -> Result<(), AeronError> {
        if !self.is_closed.load(Ordering::SeqCst) {
            //let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_close"); // don't allow other threads do close_all_resources() in parallel
            self.close_all_resources((self.epoch_clock)());
        }

        Ok(())
    }
}

impl DriverListener for ClientConductor {
    fn on_new_publication(
        &mut self,
        registration_id: i64,
        original_registration_id: i64,
        stream_id: i32,
        session_id: i32,
        publication_limit_counter_id: i32,
        channel_status_indicator_id: i32,
        log_file_name: CString,
    ) {
        //let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_new_publication");

        let mut channel: CString = CString::new("").unwrap();

        if let Some(state) = self.publication_by_registration_id.get(&registration_id) {
            channel = state.channel.clone();
        }

        let log_buffers: Option<Arc<LogBuffers>> = Some(
            self.get_log_buffers(original_registration_id, log_file_name, channel)
                .expect("get_log_buffers failed"),
        );

        if let Some(state) = self.publication_by_registration_id.get_mut(&registration_id) {
            state.status = RegistrationStatus::Registered;
            state.session_id = session_id;
            state.publication_limit_counter_id = publication_limit_counter_id;
            state.channel_status_id = channel_status_indicator_id;
            state.buffers = log_buffers;
            state.original_registration_id = original_registration_id;

            let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
            (self.on_new_publication_handler)(state.channel.clone(), stream_id, session_id, registration_id);
        }
    }

    fn on_new_exclusive_publication(
        &mut self,
        registration_id: i64,
        original_registration_id: i64,
        stream_id: i32,
        session_id: i32,
        publication_limit_counter_id: i32,
        channel_status_indicator_id: i32,
        log_file_name: CString,
    ) {
        assert_eq!(registration_id, original_registration_id);

        //let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_new_exclusive_publication");

        let mut channel: CString = CString::new("").unwrap();

        if let Some(state) = self.exclusive_publication_by_registration_id.get(&registration_id) {
            channel = state.channel.clone();
        }

        let log_buffers: Option<Arc<LogBuffers>> = Some(
            self.get_log_buffers(original_registration_id, log_file_name, channel)
                .expect("get_log_buffers failed"),
        );

        if let Some(state) = self.exclusive_publication_by_registration_id.get_mut(&registration_id) {
            state.status = RegistrationStatus::Registered;
            state.session_id = session_id;
            state.publication_limit_counter_id = publication_limit_counter_id;
            state.channel_status_id = channel_status_indicator_id;
            state.buffers = log_buffers;

            let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
            (self.on_new_exclusive_publication_handler)(state.channel.clone(), stream_id, session_id, registration_id);
        }
    }

    fn on_subscription_ready(&mut self, registration_id: i64, channel_status_id: i32) {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in on_subscription_ready");

        if let Some(state) = self.subscription_by_registration_id.get_mut(&registration_id) {
            state.status = RegistrationStatus::Registered;

            let subscr = Arc::new(Mutex::new(Subscription::new(
                self.arced_self.as_ref().unwrap().clone(),
                state.registration_id,
                state.channel.clone(),
                state.stream_id,
                channel_status_id,
            )));
            state.subscription_cache = Some(subscr.clone());
            state.subscription = Some(Arc::downgrade(&subscr));

            let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
            (self.on_new_subscription_handler)(state.channel.clone(), state.stream_id, registration_id);
        }
    }

    fn on_operation_success(&mut self, correlation_id: i64) {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in on_operation_success");

        if let Some(state) = self.destination_state_by_correlation_id.get_mut(&correlation_id) {
            if state.status == RegistrationStatus::Awaiting {
                state.status = RegistrationStatus::Registered;
            }
        }
    }

    fn on_channel_endpoint_error_response(&mut self, offending_command_correlation_id: i64, error_message: CString) {
        //let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_channel_endpoint_error_response");

        let mut subscription_to_remove: Vec<i64> = Vec::new();
        let mut linger_images: Vec<Vec<Image>> = Vec::new();

        for (reg_id, subscr_defn) in &mut self.subscription_by_registration_id {
            if let Some(maybe_subscription) = &subscr_defn.subscription {
                if let Some(protected_subscription) = maybe_subscription.upgrade() {
                    let mut subscription = protected_subscription.lock().expect("Mutex poisoned");
                    if subscription.channel_status_id() == offending_command_correlation_id as i32 {
                        (self.error_handler)(ChannelEndpointException((
                            offending_command_correlation_id,
                            String::from(error_message.to_str().expect("CString conversion error")),
                        )));

                        if let Some(mut images) = subscription.close_and_remove_images() {
                            for image in images.iter_mut() {
                                image.close();

                                let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
                                (subscr_defn.on_unavailable_image_handler)(&image);
                            }
                            linger_images.push(images);
                            subscription_to_remove.push(*reg_id);
                        }
                    }
                }
            }
        }

        for images in linger_images {
            self.linger_all_resources((self.epoch_clock)(), images);
        }

        let _removed_subs: Vec<Option<SubscriptionStateDefn>> = subscription_to_remove
            .into_iter()
            .map(|id| self.subscription_by_registration_id.remove(&id))
            .collect();

        let mut publication_to_remove: Vec<i64> = Vec::new();
        for (reg_id, publication_defn) in &self.publication_by_registration_id {
            if let Some(maybe_publication) = &publication_defn.publication {
                if let Some(publication) = maybe_publication.upgrade() {
                    if publication.channel_status_id() == offending_command_correlation_id as i32 {
                        (self.error_handler)(ChannelEndpointException((
                            offending_command_correlation_id,
                            String::from(error_message.to_str().expect("CString conversion error")),
                        )));
                        publication.close();
                        publication_to_remove.push(*reg_id);
                    }
                }
            }
        }
        let _removed_pubs: Vec<Option<PublicationStateDefn>> = publication_to_remove
            .into_iter()
            .map(|id| self.publication_by_registration_id.remove(&id))
            .collect();

        let mut epublication_to_remove: Vec<i64> = Vec::new();
        for (reg_id, publication_defn) in &self.exclusive_publication_by_registration_id {
            if let Some(maybe_publication) = &publication_defn.publication {
                if let Some(publication) = maybe_publication.upgrade() {
                    if publication.channel_status_id() == offending_command_correlation_id as i32 {
                        (self.error_handler)(ChannelEndpointException((
                            offending_command_correlation_id,
                            String::from(error_message.to_str().expect("CString conversion error")),
                        )));
                        publication.close();
                        epublication_to_remove.push(*reg_id);
                    }
                }
            }
        }
        let _removed_epubs: Vec<Option<ExclusivePublicationStateDefn>> = epublication_to_remove
            .into_iter()
            .map(|id| self.exclusive_publication_by_registration_id.remove(&id))
            .collect();
    }

    fn on_error_response(&mut self, offending_command_correlation_id: i64, error_code: i32, error_message: CString) {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in on_error_response");

        if let Some(subscription) = self
            .subscription_by_registration_id
            .get_mut(&offending_command_correlation_id)
        {
            subscription.status = RegistrationStatus::Errored;
            subscription.error_code = error_code;
            subscription.error_message = error_message;
            return;
        }

        if let Some(publication) = self.publication_by_registration_id.get_mut(&offending_command_correlation_id) {
            publication.status = RegistrationStatus::Errored;
            publication.error_code = error_code;
            publication.error_message = error_message;
            return;
        }

        if let Some(publication) = self
            .exclusive_publication_by_registration_id
            .get_mut(&offending_command_correlation_id)
        {
            publication.status = RegistrationStatus::Errored;
            publication.error_code = error_code;
            publication.error_message = error_message;
            return;
        }

        if let Some(counter) = self.counter_by_registration_id.get_mut(&offending_command_correlation_id) {
            counter.status = RegistrationStatus::Errored;
            counter.error_code = error_code;
            counter.error_message = error_message;
            return;
        }

        if let Some(destination) = self
            .destination_state_by_correlation_id
            .get_mut(&offending_command_correlation_id)
        {
            destination.status = RegistrationStatus::Errored;
            destination.error_code = error_code;
            destination.error_message = error_message;
            return;
        }
    }

    fn on_available_image(
        &mut self,
        correlation_id: i64,
        session_id: i32,
        subscriber_position_id: i32,
        subscription_registration_id: i64,
        log_filename: CString,
        source_identity: CString,
    ) {
        //let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_available_image");

        let mut channel = CString::new("").unwrap();
        if let Some(subscr_defn) = self.subscription_by_registration_id.get(&subscription_registration_id) {
            channel = subscr_defn.channel.clone();
        }

        let log_buffers = self
            .get_log_buffers(correlation_id, log_filename, channel)
            .expect("Get log_buffers failed");

        let mut linger_images: Option<Vec<Image>> = None;

        if let Some(subscr_defn) = self.subscription_by_registration_id.get_mut(&subscription_registration_id) {
            if let Some(maybe_subscription) = &subscr_defn.subscription {
                if let Some(subscription) = maybe_subscription.upgrade() {
                    let subscriber_position = UnsafeBufferPosition::new(self.counter_values_buffer, subscriber_position_id);
                    let image = Image::create(
                        session_id,
                        correlation_id,
                        subscription_registration_id,
                        source_identity,
                        &subscriber_position,
                        log_buffers,
                        self.error_handler,
                    );

                    let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
                    (subscr_defn.on_available_image_handler)(&image);

                    linger_images = Some(subscription.lock().expect("Mutex poisoned").add_image(image));
                }
            }
        }

        if let Some(images) = linger_images {
            self.linger_resource((self.epoch_clock)(), images);
        }
    }

    fn on_unavailable_image(&mut self, correlation_id: i64, subscription_registration_id: i64) {
        //let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_unavailable_image");
        let now_ms = (self.epoch_clock)();

        let mut linger_images: Option<Vec<Image>> = None;

        if let Some(subscr_defn) = self.subscription_by_registration_id.get(&subscription_registration_id) {
            if let Some(maybe_subscription) = &subscr_defn.subscription {
                if let Some(subscription) = maybe_subscription.upgrade() {
                    // If Image was actually removed
                    if let Some((old_image_array, index)) =
                        subscription.lock().expect("Mutex poisoned").remove_image(correlation_id)
                    {
                        let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
                        (subscr_defn.on_unavailable_image_handler)(
                            old_image_array.get(index as usize).expect("Bug in image handling"),
                        );
                        linger_images = Some(old_image_array);
                    }
                }
            }
        }

        if let Some(images) = linger_images {
            self.linger_resource(now_ms, images);
        }
    }

    fn on_available_counter(&mut self, registration_id: i64, counter_id: i32) {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in on_available_counter");

        if let Some(state) = self.counter_by_registration_id.get_mut(&registration_id) {
            if state.status == RegistrationStatus::Awaiting {
                state.status = RegistrationStatus::Registered;
                state.counter_id = counter_id;

                let cnt = Arc::new(Counter::new(
                    self.arced_self.as_ref().unwrap().clone(),
                    self.counter_values_buffer,
                    state.registration_id,
                    counter_id,
                ));
                state.counter = Some(Arc::downgrade(&cnt));
                state.counter_cache = Some(cnt);
            }

            for handler in &self.on_available_counter_handlers {
                let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
                handler(&self.counters_reader, registration_id, counter_id);
            }
        }
    }

    fn on_unavailable_counter(&mut self, registration_id: i64, counter_id: i32) {
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in on_unavailable_counter");

        for handler in &self.on_unavailable_counter_handlers {
            let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
            handler(&self.counters_reader, registration_id, counter_id);
        }
    }

    fn on_client_timeout(&mut self, client_id: i64) {
        if self.driver_proxy.client_id() == client_id && !self.is_closed() {
            //let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_client_timeout");
            self.close_all_resources((self.epoch_clock)());
            (self.error_handler)(ClientTimeoutException(String::from("client timeout from driver")));
        }
    }
}

impl Drop for ClientConductor {
    fn drop(&mut self) {
        for _img in &self.lingering_image_lists {
            // img.image_array.drop(); FIXME: check whether drop for Images is needed
        }
        let _res = self.driver_proxy.client_close();
    }
}

unsafe impl Send for ClientConductor {}
unsafe impl Sync for ClientConductor {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::control_protocol_events::AeronCommand;
    use crate::command::publication_message_flyweight::PublicationMessageFlyweight;
    use crate::concurrent::atomic_buffer::AlignedBuffer;
    use crate::concurrent::broadcast::broadcast_buffer_descriptor;
    use crate::concurrent::broadcast::broadcast_receiver::BroadcastReceiver;
    use crate::concurrent::logbuffer::log_buffer_descriptor;
    use crate::concurrent::ring_buffer;
    use crate::concurrent::ring_buffer::ManyToOneRingBuffer;
    use crate::utils::memory_mapped_file::MemoryMappedFile;
    use crate::utils::misc::unix_time_ms;
    use nix::unistd;

    const CHANNEL: &str = "aeron:udp?endpoint=localhost:40123";
    const STREAM_ID: i32 = 10;
    const SESSION_ID: i32 = 200;
    const PUBLICATION_LIMIT_COUNTER_ID: i32 = 0;
    const PUBLICATION_LIMIT_COUNTER_ID_2: i32 = 1;
    const CHANNEL_STATUS_INDICATOR_ID: i32 = 2;
    const COUNTER_ID: i32 = 3;
    const TERM_LENGTH: i32 = log_buffer_descriptor::TERM_MIN_LENGTH;
    const PAGE_SIZE: i32 = log_buffer_descriptor::AERON_PAGE_MIN_SIZE;
    const COUNTER_TYPE_ID: i32 = 102;
    const LOG_FILE_LENGTH: i32 = ((TERM_LENGTH * 3) + log_buffer_descriptor::LOG_META_DATA_LENGTH);
    const SOURCE_IDENTITY: &str = "127.0.0.1:43567";
    const COUNTER_LABEL: &str = "counter label";

    const CAPACITY: i32 = 1024;
    const MANY_TO_ONE_RING_BUFFER_LENGTH: i32 = CAPACITY + ring_buffer::TRAILER_LENGTH;
    const BROADCAST_BUFFER_LENGTH: i32 = CAPACITY + broadcast_buffer_descriptor::TRAILER_LENGTH;
    const COUNTER_VALUES_BUFFER_LENGTH: i32 = 1024 * 1024;
    const COUNTER_METADATA_BUFFER_LENGTH: i32 = 4 * 1024 * 1024;

    const DRIVER_TIMEOUT_MS: Moment = 10 * 1000;
    const RESOURCE_LINGER_TIMEOUT_MS: Moment = 5 * 1000;
    const INTER_SERVICE_TIMEOUT_NS: Moment = 5 * 1000 * 1000 * 1000;
    const INTER_SERVICE_TIMEOUT_MS: Moment = INTER_SERVICE_TIMEOUT_NS / 1_000_000;
    const PRE_TOUCH_MAPPED_MEMORY: bool = false;

    type TestManyToOneRingBuffer = [u8; MANY_TO_ONE_RING_BUFFER_LENGTH as usize];
    type TestBroadcastBuffer = [u8; BROADCAST_BUFFER_LENGTH as usize];
    type TestCounterValuesBuffer = [u8; COUNTER_VALUES_BUFFER_LENGTH as usize];
    type TestCounterMetadataBuffer = [u8; COUNTER_METADATA_BUFFER_LENGTH as usize];

    fn make_temp_file_name() -> String {
        match unistd::mkstemp("/tmp/aeron-c.XXXXXXX") {
            Ok((_fd, path)) => {
                unistd::unlink(path.as_path()).unwrap(); // flag file to be deleted at app termination
                path.to_string_lossy().to_string()
            }
            Err(e) => panic!("mkstemp failed: {}", e),
        }
    }

    fn on_new_publication_handler(_channel: CString, _stream_id: i32, _session_id: i32, _correlation_id: i64) {}

    fn on_new_exclusive_publication_handler(_channel: CString, _stream_id: i32, _session_id: i32, _correlation_id: i64) {}

    fn on_new_subscription_handler(_channel: CString, _stream_id: i32, _correlation_id: i64) {}

    fn error_handler(err: AeronError) {
        println!("Got error: {:?}", err);
    }

    fn on_available_counter_handler(_counters_reader: &CountersReader, _registration_id: i64, _counter_id: i32) {}

    fn on_unavailable_counter_handler(_counters_reader: &CountersReader, _registration_id: i64, _counter_id: i32) {}

    struct ClientConductorTest {
        log_file_name: String,
        log_file_name2: String,

        //to_driver: TestManyToOneRingBuffer,
        //to_clients: TestBroadcastBuffer,
        //counter_values: TestCounterValuesBuffer,
        //counter_metadata: TestCounterMetadataBuffer,
        to_driver: AlignedBuffer,
        to_clients: AlignedBuffer,
        counter_metadata: AlignedBuffer,
        counter_values: AlignedBuffer,

        to_driver_buffer: AtomicBuffer,
        to_clients_buffer: AtomicBuffer,
        //counter_metadata_buffer: AtomicBuffer,
        //counter_values_buffer: AtomicBuffer,
        many_to_one_ring_buffer: Arc<ManyToOneRingBuffer>,
        //broadcast_receiver: BroadcastReceiver,

        //driver_proxy: DriverProxy,
        //copy_broadcast_receiver: CopyBroadcastReceiver,
        current_time: Moment,
        conductor: Arc<Mutex<ClientConductor>>,
        //error_handler: ErrorHandler,

        //on_available_counter_handlers: OnAvailableCounter,
        //on_unavailable_counter_handlers: OnUnavailableCounter,
        //on_available_image_handlers: OnAvailableImage,
        //on_unavailable_image_handlers: OnUnavailableImage,
    }

    impl ClientConductorTest {
        pub fn new() -> Self {
            let to_driver = AlignedBuffer::with_capacity(MANY_TO_ONE_RING_BUFFER_LENGTH);
            let to_clients = AlignedBuffer::with_capacity(BROADCAST_BUFFER_LENGTH);
            let counter_metadata = AlignedBuffer::with_capacity(BROADCAST_BUFFER_LENGTH);
            let counter_values = AlignedBuffer::with_capacity(COUNTER_METADATA_BUFFER_LENGTH);

            let to_driver_buffer = AtomicBuffer::from_aligned(&to_driver);
            let to_clients_buffer = AtomicBuffer::from_aligned(&to_clients);
            let counters_metadata_buffer = AtomicBuffer::from_aligned(&counter_metadata);
            let counters_values_buffer = AtomicBuffer::from_aligned(&counter_values);

            let fname1 = make_temp_file_name();
            let fname2 = make_temp_file_name();

            let mut logbuffer1 = MemoryMappedFile::create_new(&fname1, 0, LOG_FILE_LENGTH).unwrap();
            let mut logbuffer2 = MemoryMappedFile::create_new(&fname2, 0, LOG_FILE_LENGTH).unwrap();

            let local_to_driver_ring_buffer =
                Arc::new(ManyToOneRingBuffer::new(to_driver_buffer).expect("Failed to create RingBuffer"));
            let local_to_clients_broadcast_receiver = Arc::new(Mutex::new(
                BroadcastReceiver::new(to_clients_buffer).expect("Failed to create BroadcastReceiver"),
            ));
            let local_driver_proxy = Arc::new(DriverProxy::new(local_to_driver_ring_buffer.clone()));
            let local_copy_broadcast_receiver =
                Arc::new(Mutex::new(CopyBroadcastReceiver::new(local_to_clients_broadcast_receiver)));

            let local_conductor = ClientConductor::new(
                unix_time_ms,
                local_driver_proxy,
                local_copy_broadcast_receiver,
                counters_metadata_buffer,
                counters_values_buffer,
                on_new_publication_handler,
                on_new_exclusive_publication_handler,
                on_new_subscription_handler,
                error_handler,
                on_available_counter_handler,
                on_unavailable_counter_handler,
                on_close_client_handler,
                DRIVER_TIMEOUT_MS,
                RESOURCE_LINGER_TIMEOUT_MS,
                INTER_SERVICE_TIMEOUT_MS,
                PRE_TOUCH_MAPPED_MEMORY,
            );

            fn on_close_client_handler() {}
            fn on_media_driver_timeout() {}

            let instance = Self {
                to_driver,
                to_clients,

                counter_metadata,
                log_file_name: fname1,
                log_file_name2: fname2,

                to_driver_buffer,
                to_clients_buffer,

                many_to_one_ring_buffer: local_to_driver_ring_buffer,
                current_time: unix_time_ms(),
                conductor: local_conductor,
                counter_values,
            };

            // Now setup initial state
            instance
                .to_driver_buffer
                .set_memory(0, instance.to_driver_buffer.capacity(), 0);
            instance
                .to_clients_buffer
                .set_memory(0, instance.to_clients_buffer.capacity(), 0);

            instance
                .many_to_one_ring_buffer
                .set_consumer_heartbeat_time(instance.current_time as i64);

            // Init metadata inside the test files.
            unsafe {
                let log_meta_data_buffer = AtomicBuffer::new(
                    logbuffer1
                        .memory_mut_ptr()
                        .as_mut_ptr()
                        .offset((LOG_FILE_LENGTH - log_buffer_descriptor::LOG_META_DATA_LENGTH) as isize),
                    log_buffer_descriptor::LOG_META_DATA_LENGTH,
                );
                log_meta_data_buffer.put::<i32>(*log_buffer_descriptor::LOG_TERM_LENGTH_OFFSET, TERM_LENGTH);
                log_meta_data_buffer.put::<i32>(*log_buffer_descriptor::LOG_PAGE_SIZE_OFFSET, PAGE_SIZE);

                let log_meta_data_buffer2 = AtomicBuffer::new(
                    logbuffer2
                        .memory_mut_ptr()
                        .as_mut_ptr()
                        .offset((LOG_FILE_LENGTH - log_buffer_descriptor::LOG_META_DATA_LENGTH) as isize),
                    log_buffer_descriptor::LOG_META_DATA_LENGTH,
                );
                log_meta_data_buffer2.put::<i32>(*log_buffer_descriptor::LOG_TERM_LENGTH_OFFSET, TERM_LENGTH);
                log_meta_data_buffer2.put::<i32>(*log_buffer_descriptor::LOG_PAGE_SIZE_OFFSET, PAGE_SIZE);
            }

            instance
        }
    }

    fn str_to_c(val: &str) -> CString {
        CString::new(val).expect("Error converting str to CString")
    }

    #[test]
    fn should_return_null_for_unknown_publication() {
        let test = ClientConductorTest::new();

        let publication = test.conductor.lock().unwrap().find_publication(100);

        assert!(publication.is_err());
    }

    #[test]
    fn should_return_null_for_publication_without_log_buffers() {
        let test = ClientConductorTest::new();
        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        let publication = test.conductor.lock().unwrap().find_publication(id);

        assert!(publication.is_err());
    }

    #[test]
    fn should_send_add_publication_to_driver() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        let count = test.many_to_one_ring_buffer.read(
            |msg_type_id, buffer| {
                let message = PublicationMessageFlyweight::new(buffer, 0);

                assert_eq!(msg_type_id, AeronCommand::AddPublication);
                assert_eq!(message.correlation_id(), id);
                assert_eq!(message.stream_id(), STREAM_ID);
                assert_eq!(message.channel(), str_to_c(CHANNEL));
            },
            1000,
        );

        assert_eq!(count, 1);
    }

    #[test]
    fn test() {
        let test = ClientConductorTest::new();

        let _publication = test.conductor.lock().unwrap().find_publication(100);
    }
}

/*

TEST_F(ClientConductorTest, shouldReturnPublicationAfterLogBuffersCreated)
{
    let id = test.conductor.lock().unwrap().add_publication(str_to_c(str_to_c(CHANNEL)), STREAM_ID).expect("failed to add publication");

    test.conductor.lock().unwrap().onNewPublication(
        id, id, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, self.log_file_name);

    std::shared_ptr<Publication> pub = test.conductor.lock().unwrap().findPublication(id);

    ASSERT_TRUE(pub != nullptr);
    assert_eq!(pub->registrationId(), id);
    assert_eq!(pub->str_to_c(CHANNEL)(), str_to_c(CHANNEL));
    assert_eq!(pub->stream_id(), STREAM_ID);
    assert_eq!(pub->sessionId(), SESSION_ID);
}

TEST_F(ClientConductorTest, shouldReleasePublicationAfterGoingOutOfScope)
{
    let id = test.conductor.lock().unwrap().add_publication(str_to_c(CHANNEL), STREAM_ID).expect("failed to add publication");
    static std::int32_t REMOVE_PUBLICATION = ControlProtocolEvents::REMOVE_PUBLICATION;

    test.many_to_one_ring_buffer.read(
        [&](std::int32_t, concurrent::AtomicBuffer&, util::index_t, util::index_t)
        {
        });

    test.conductor.lock().unwrap().onNewPublication(
        id, id, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, self.log_file_name);

    {
        std::shared_ptr<Publication> pub = test.conductor.lock().unwrap().findPublication(id);

        ASSERT_TRUE(pub != nullptr);
    }

    int count = test.many_to_one_ring_buffer.read(
        [&](std::int32_t msg_type_id, concurrent::AtomicBuffer& buffer, util::index_t offset, util::index_t length)
        {
            const RemoveMessageFlyweight message(buffer, offset);

            assert_eq!(msg_type_id, REMOVE_PUBLICATION);
            assert_eq!(message.registrationId(), id);
        });

    assert_eq!(count, 1);

    std::shared_ptr<Publication> pubPost = test.conductor.lock().unwrap().findPublication(id);
    ASSERT_TRUE(pubPost == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnSamePublicationAfterLogBuffersCreated)
{
    let id = test.conductor.lock().unwrap().add_publication(str_to_c(CHANNEL), STREAM_ID).expect("failed to add publication");

    test.conductor.lock().unwrap().onNewPublication(
        id, id, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, self.log_file_name);

    std::shared_ptr<Publication> pub1 = test.conductor.lock().unwrap().findPublication(id);
    std::shared_ptr<Publication> pub2 = test.conductor.lock().unwrap().findPublication(id);

    ASSERT_TRUE(pub1 != nullptr);
    ASSERT_TRUE(pub2 != nullptr);
    ASSERT_TRUE(pub1 == pub2);
}

TEST_F(ClientConductorTest, shouldIgnorePublicationReadyForUnknowncorrelation_id)
{
    let id = test.conductor.lock().unwrap().add_publication(str_to_c(CHANNEL), STREAM_ID).expect("failed to add publication");

    test.conductor.lock().unwrap().onNewPublication(
        id + 1,
        id + 1,
        STREAM_ID,
        SESSION_ID,
        PUBLICATION_LIMIT_COUNTER_ID,
        CHANNEL_STATUS_INDICATOR_ID,
        self.log_file_name);

    std::shared_ptr<Publication> pub = test.conductor.lock().unwrap().findPublication(id);

    ASSERT_TRUE(pub == nullptr);
}

TEST_F(ClientConductorTest, shouldTimeoutAddPublicationWithoutPublicationReady)
{
    let id = test.conductor.lock().unwrap().add_publication(str_to_c(CHANNEL), STREAM_ID).expect("failed to add publication");

    self.current_time += DRIVER_TIMEOUT_MS + 1;

    ASSERT_THROW(
        {
            std::shared_ptr<Publication> pub = test.conductor.lock().unwrap().findPublication(id);
        },
        util::DriverTimeoutException);
}

TEST_F(ClientConductorTest, shouldExceptionOnFindWhenReceivingErrorResponseOnAddPublication)
{
    let id = test.conductor.lock().unwrap().add_publication(str_to_c(CHANNEL), STREAM_ID).expect("failed to add publication");

    test.conductor.lock().unwrap().onErrorResponse(id, ERROR_CODE_INVALID_CHANNEL, "invalid channel");

    ASSERT_THROW(
        {
            std::shared_ptr<Publication> pub = test.conductor.lock().unwrap().findPublication(id);
        },
        util::RegistrationException);
}

TEST_F(ClientConductorTest, shouldReturnNullForUnknownExclusivePublication)
{
    std::shared_ptr<ExclusivePublication> pub = test.conductor.lock().unwrap().findExclusivePublication(100);

    EXPECT_TRUE(pub == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnNullForExclusivePublicationWithoutLogBuffers)
{
    let id = test.conductor.lock().unwrap().addExclusivePublication(CHANNEL, STREAM_ID);

    std::shared_ptr<ExclusivePublication> pub = test.conductor.lock().unwrap().findExclusivePublication(id);

    EXPECT_TRUE(pub == nullptr);
}

TEST_F(ClientConductorTest, shouldSendAddExclusivePublicationToDriver)
{
    let id = test.conductor.lock().unwrap().addExclusivePublication(CHANNEL, STREAM_ID);
    static std::int32_t ADD_EXCLUSIVE_PUBLICATION = ControlProtocolEvents::ADD_EXCLUSIVE_PUBLICATION;

    int count = test.many_to_one_ring_buffer.read(
        [&](std::int32_t msg_type_id, concurrent::AtomicBuffer& buffer, util::index_t offset, util::index_t length)
        {
            const PublicationMessageFlyweight message(buffer, offset);

            assert_eq!(msg_type_id, ADD_EXCLUSIVE_PUBLICATION);
            assert_eq!(message.correlation_id(), id);
            assert_eq!(message.stream_id(), STREAM_ID);
            assert_eq!(message.channel(), CHANNEL);
        });

    assert_eq!(count, 1);
}

TEST_F(ClientConductorTest, shouldReturnExclusivePublicationAfterLogBuffersCreated)
{
    let id = test.conductor.lock().unwrap().addExclusivePublication(CHANNEL, STREAM_ID);

    test.conductor.lock().unwrap().onNewExclusivePublication(
        id, id, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, self.log_file_name);

    std::shared_ptr<ExclusivePublication> pub = test.conductor.lock().unwrap().findExclusivePublication(id);

    ASSERT_TRUE(pub != nullptr);
    assert_eq!(pub->registrationId(), id);
    assert_eq!(pub->channel(), CHANNEL);
    assert_eq!(pub->stream_id(), STREAM_ID);
    assert_eq!(pub->sessionId(), SESSION_ID);
}

TEST_F(ClientConductorTest, shouldReleaseExclusivePublicationAfterGoingOutOfScope)
{
    let id = test.conductor.lock().unwrap().addExclusivePublication(CHANNEL, STREAM_ID);
    static std::int32_t REMOVE_PUBLICATION = ControlProtocolEvents::REMOVE_PUBLICATION;

    test.many_to_one_ring_buffer.read(
        [&](std::int32_t, concurrent::AtomicBuffer&, util::index_t, util::index_t)
        {
        });

    test.conductor.lock().unwrap().onNewExclusivePublication(
        id, id, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, self.log_file_name);

    {
        std::shared_ptr<ExclusivePublication> pub = test.conductor.lock().unwrap().findExclusivePublication(id);

        ASSERT_TRUE(pub != nullptr);
    }

    int count = test.many_to_one_ring_buffer.read(
        [&](std::int32_t msg_type_id, concurrent::AtomicBuffer& buffer, util::index_t offset, util::index_t length)
        {
            const RemoveMessageFlyweight message(buffer, offset);

            assert_eq!(msg_type_id, REMOVE_PUBLICATION);
            assert_eq!(message.registrationId(), id);
        });

    assert_eq!(count, 1);

    std::shared_ptr<ExclusivePublication> pubPost = test.conductor.lock().unwrap().findExclusivePublication(id);
    ASSERT_TRUE(pubPost == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnDifferentIdForDuplicateAddExclusivePublication)
{
    let id1 = test.conductor.lock().unwrap().addExclusivePublication(CHANNEL, STREAM_ID);
    let id2 = test.conductor.lock().unwrap().addExclusivePublication(CHANNEL, STREAM_ID);

    EXPECT_NE(id1, id2);
}

TEST_F(ClientConductorTest, shouldReturnSameExclusivePublicationAfterLogBuffersCreated)
{
    let id = test.conductor.lock().unwrap().addExclusivePublication(CHANNEL, STREAM_ID);

    test.conductor.lock().unwrap().onNewExclusivePublication(
        id, id, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, self.log_file_name);

    std::shared_ptr<ExclusivePublication> pub1 = test.conductor.lock().unwrap().findExclusivePublication(id);
    std::shared_ptr<ExclusivePublication> pub2 = test.conductor.lock().unwrap().findExclusivePublication(id);

    ASSERT_TRUE(pub1 != nullptr);
    ASSERT_TRUE(pub2 != nullptr);
    ASSERT_TRUE(pub1 == pub2);
}

TEST_F(ClientConductorTest, shouldIgnoreExclusivePublicationReadyForUnknowncorrelation_id)
{
    let id = test.conductor.lock().unwrap().addExclusivePublication(CHANNEL, STREAM_ID);

    test.conductor.lock().unwrap().onNewExclusivePublication(
        id + 1,
        id + 1,
        STREAM_ID,
        SESSION_ID,
        PUBLICATION_LIMIT_COUNTER_ID,
        CHANNEL_STATUS_INDICATOR_ID,
        self.log_file_name);

    std::shared_ptr<ExclusivePublication> pub = test.conductor.lock().unwrap().findExclusivePublication(id);

    ASSERT_TRUE(pub == nullptr);
}

TEST_F(ClientConductorTest, shouldTimeoutAddExclusivePublicationWithoutPublicationReady)
{
    let id = test.conductor.lock().unwrap().addExclusivePublication(CHANNEL, STREAM_ID);

    self.current_time += DRIVER_TIMEOUT_MS + 1;

    ASSERT_THROW(
        {
            std::shared_ptr<ExclusivePublication> pub = test.conductor.lock().unwrap().findExclusivePublication(id);
        },
        util::DriverTimeoutException);
}

TEST_F(ClientConductorTest, shouldExceptionOnFindWhenReceivingErrorResponseOnAddExclusivePublication)
{
    let id = test.conductor.lock().unwrap().addExclusivePublication(CHANNEL, STREAM_ID);

    test.conductor.lock().unwrap().onErrorResponse(id, ERROR_CODE_INVALID_CHANNEL, "invalid channel");

    ASSERT_THROW(
        {
            std::shared_ptr<ExclusivePublication> pub = test.conductor.lock().unwrap().findExclusivePublication(id);
        },
        util::RegistrationException);
}

TEST_F(ClientConductorTest, shouldReturnNullForUnknownSubscription)
{
    std::shared_ptr<Subscription> sub = test.conductor.lock().unwrap().findSubscription(100);

    EXPECT_TRUE(sub == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnNullForSubscriptionWithoutOperationSuccess)
{
    let id = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    std::shared_ptr<Subscription> sub = test.conductor.lock().unwrap().findSubscription(id);

    EXPECT_TRUE(sub == nullptr);
}

TEST_F(ClientConductorTest, shouldSendAddSubscriptionToDriver)
{
    let id = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    static std::int32_t ADD_SUBSCRIPTION = ControlProtocolEvents::ADD_SUBSCRIPTION;

    int count = test.many_to_one_ring_buffer.read(
        [&](std::int32_t msg_type_id, concurrent::AtomicBuffer& buffer, util::index_t offset, util::index_t length)
        {
            const SubscriptionMessageFlyweight message(buffer, offset);

            assert_eq!(msg_type_id, ADD_SUBSCRIPTION);
            assert_eq!(message.correlation_id(), id);
            assert_eq!(message.stream_id(), STREAM_ID);
            assert_eq!(message.channel(), CHANNEL);
        });

    assert_eq!(count, 1);
}

TEST_F(ClientConductorTest, shouldReturnSubscriptionAfterOperationSuccess)
{
    let id = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    test.conductor.lock().unwrap().onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);

    std::shared_ptr<Subscription> sub = test.conductor.lock().unwrap().findSubscription(id);

    ASSERT_TRUE(sub != nullptr);
    assert_eq!(sub->registrationId(), id);
    assert_eq!(sub->channel(), CHANNEL);
    assert_eq!(sub->stream_id(), STREAM_ID);
}

TEST_F(ClientConductorTest, shouldReleaseSubscriptionAfterGoingOutOfScope)
{
    let id = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    static std::int32_t REMOVE_SUBSCRIPTION = ControlProtocolEvents::REMOVE_SUBSCRIPTION;

    test.many_to_one_ring_buffer.read(
        [&](std::int32_t, concurrent::AtomicBuffer&, util::index_t, util::index_t)
        {
        });

    test.conductor.lock().unwrap().onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);

    {
        std::shared_ptr<Subscription> sub = test.conductor.lock().unwrap().findSubscription(id);

        ASSERT_TRUE(sub != nullptr);
    }

    int count = test.many_to_one_ring_buffer.read(
        [&](std::int32_t msg_type_id, concurrent::AtomicBuffer& buffer, util::index_t offset, util::index_t length)
        {
            const RemoveMessageFlyweight message(buffer, offset);

            assert_eq!(msg_type_id, REMOVE_SUBSCRIPTION);
            assert_eq!(message.registrationId(), id);
        });

    assert_eq!(count, 1);

    std::shared_ptr<Subscription> subPost = test.conductor.lock().unwrap().findSubscription(id);
    ASSERT_TRUE(subPost == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnDifferentIdsForDuplicateAddSubscription)
{
    let id1 = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    let id2 = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    EXPECT_NE(id1, id2);
}

TEST_F(ClientConductorTest, shouldReturnSameFindSubscriptionAfterOperationSuccess)
{
    let id = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    test.conductor.lock().unwrap().onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);

    std::shared_ptr<Subscription> sub1 = test.conductor.lock().unwrap().findSubscription(id);
    std::shared_ptr<Subscription> sub2 = test.conductor.lock().unwrap().findSubscription(id);

    ASSERT_TRUE(sub1 != nullptr);
    ASSERT_TRUE(sub2 != nullptr);
    ASSERT_TRUE(sub1 == sub2);
}

TEST_F(ClientConductorTest, shouldReturnDifferentSubscriptionAfterOperationSuccess)
{
    let id1 = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    let id2 = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    test.conductor.lock().unwrap().onSubscriptionReady(id1, CHANNEL_STATUS_INDICATOR_ID);
    test.conductor.lock().unwrap().onSubscriptionReady(id2, CHANNEL_STATUS_INDICATOR_ID);

    std::shared_ptr<Subscription> sub1 = test.conductor.lock().unwrap().findSubscription(id1);
    std::shared_ptr<Subscription> sub2 = test.conductor.lock().unwrap().findSubscription(id2);

    ASSERT_TRUE(sub1 != nullptr);
    ASSERT_TRUE(sub2 != nullptr);
    ASSERT_TRUE(sub1 != sub2);
}

TEST_F(ClientConductorTest, shouldIgnoreOperationSuccessForUnknowncorrelation_id)
{
    let id = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    test.conductor.lock().unwrap().onSubscriptionReady(id + 1, CHANNEL_STATUS_INDICATOR_ID);

    std::shared_ptr<Subscription> sub = test.conductor.lock().unwrap().findSubscription(id);

    ASSERT_TRUE(sub == nullptr);
}

TEST_F(ClientConductorTest, shouldTimeoutAddSubscriptionWithoutOperationSuccess)
{
    let id = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    self.current_time += DRIVER_TIMEOUT_MS + 1;

    ASSERT_THROW(
        {
            std::shared_ptr<Subscription> sub = test.conductor.lock().unwrap().findSubscription(id);
        },
        util::DriverTimeoutException);
}

TEST_F(ClientConductorTest, shouldExceptionOnFindWhenReceivingErrorResponseOnAddSubscription)
{
    let id = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    test.conductor.lock().unwrap().onErrorResponse(id, ERROR_CODE_INVALID_CHANNEL, "invalid channel");

    ASSERT_THROW(
        {
            std::shared_ptr<Subscription> sub = test.conductor.lock().unwrap().findSubscription(id);
        },
        util::RegistrationException);
}

TEST_F(ClientConductorTest, shouldCallErrorHandlerWhenInterServiceTimeoutExceeded)
{
    bool called = false;

    m_errorHandler =
        [&](const std::exception& exception)
        {
            assert_eq!(typeid(ConductorServiceTimeoutException), typeid(exception));
            called = true;
        };

    self.current_time += INTER_SERVICE_TIMEOUT_MS + 1;
    test.conductor.lock().unwrap().doWork();
    EXPECT_TRUE(called);
}

TEST_F(ClientConductorTest, shouldCallErrorHandlerWhenDriverInactiveOnIdle)
{
    bool called = false;

    m_errorHandler =
        [&](const std::exception& exception)
        {
            assert_eq!(typeid(DriverTimeoutException), typeid(exception));
            called = true;
        };

    doWorkUntilDriverTimeout();
    EXPECT_TRUE(called);
}

TEST_F(ClientConductorTest, shouldExceptionWhenAddPublicationAfterDriverInactive)
{
    bool called = false;
    m_errorHandler = [&](const std::exception& exception) { called = true; };

    doWorkUntilDriverTimeout();
    EXPECT_TRUE(called);

    ASSERT_THROW(
        {
            test.conductor.lock().unwrap().add_publication(str_to_c(CHANNEL), STREAM_ID).expect("failed to add publication");
        },
        util::DriverTimeoutException);
}

TEST_F(ClientConductorTest, shouldExceptionWhenReleasePublicationAfterDriverInactive)
{
    bool called = false;
    m_errorHandler = [&](const std::exception& exception) { called = true; };

    doWorkUntilDriverTimeout();
    EXPECT_TRUE(called);

    ASSERT_NO_THROW(
        {
            test.conductor.lock().unwrap().releasePublication(100);
        });
}

TEST_F(ClientConductorTest, shouldExceptionWhenAddSubscriptionAfterDriverInactive)
{
    bool called = false;
    m_errorHandler = [&](const std::exception& exception) { called = true; };

    doWorkUntilDriverTimeout();
    EXPECT_TRUE(called);

    ASSERT_THROW(
        {
            test.conductor.lock().unwrap().addSubscription(CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
        },
        util::DriverTimeoutException);
}

TEST_F(ClientConductorTest, shouldExceptionWhenReleaseSubscriptionAfterDriverInactive)
{
    bool called = false;
    m_errorHandler = [&](const std::exception& exception) { called = true; };

    doWorkUntilDriverTimeout();
    EXPECT_TRUE(called);

    ASSERT_NO_THROW(
        {
            test.conductor.lock().unwrap().releaseSubscription(100, nullptr, 0);
        });
}

TEST_F(ClientConductorTest, shouldCallOnNewPubAfterLogBuffersCreated)
{
    let id = test.conductor.lock().unwrap().add_publication(str_to_c(CHANNEL), STREAM_ID).expect("failed to add publication");

    EXPECT_CALL(m_handlers, onNewPub(testing::StrEq(CHANNEL), STREAM_ID, SESSION_ID, id))
        .Times(1);

    test.conductor.lock().unwrap().onNewPublication(id, id,
        STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, self.log_file_name);
}

TEST_F(ClientConductorTest, shouldCallOnNewSubAfterOperationSuccess)
{
    let id = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    EXPECT_CALL(m_handlers, onNewSub(testing::StrEq(CHANNEL), STREAM_ID, id))
        .Times(1);

    test.conductor.lock().unwrap().onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);
    std::shared_ptr<Subscription> sub = test.conductor.lock().unwrap().findSubscription(id);
    ASSERT_TRUE(sub != nullptr);
}

TEST_F(ClientConductorTest, shouldCallNewConnectionAfterOnNewConnection)
{
    let id = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    let correlation_id = id + 1;
    testing::Sequence sequence;

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(1)
        .InSequence(sequence);

    test.conductor.lock().unwrap().onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);
    // must be able to handle newImage even if findSubscription not called
    test.conductor.lock().unwrap().onAvailableImage(correlation_id, SESSION_ID, 1, id, self.log_file_name, SOURCE_IDENTITY);

    std::shared_ptr<Subscription> sub = test.conductor.lock().unwrap().findSubscription(id);
    ASSERT_TRUE(sub != nullptr);
    ASSERT_TRUE(sub->hasImage(correlation_id));
}

TEST_F(ClientConductorTest, shouldNotCallNewConnectionIfNoOperationSuccess)
{
    let id = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    let correlation_id = id + 1;

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(0);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(0);

    // must be able to handle newImage even if findSubscription not called
    test.conductor.lock().unwrap().onAvailableImage(correlation_id, SESSION_ID, 1, id, self.log_file_name, SOURCE_IDENTITY);

    std::shared_ptr<Subscription> sub = test.conductor.lock().unwrap().findSubscription(id);
    ASSERT_TRUE(sub == nullptr);
}

TEST_F(ClientConductorTest, shouldNotCallNewConnectionIfUninterestingRegistrationId)
{
    let id = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    let correlation_id = id + 1;

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(1);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(0);

    test.conductor.lock().unwrap().onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);
    // must be able to handle newImage even if findSubscription not called
    test.conductor.lock().unwrap().onAvailableImage(correlation_id, SESSION_ID, 1, id + 1, self.log_file_name, SOURCE_IDENTITY);

    std::shared_ptr<Subscription> sub = test.conductor.lock().unwrap().findSubscription(id);
    ASSERT_TRUE(sub != nullptr);
    ASSERT_FALSE(sub->hasImage(correlation_id));
}

TEST_F(ClientConductorTest, shouldCallInactiveConnecitonAfterInactiveConnection)
{
    let id = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    let correlation_id = id + 1;
    testing::Sequence sequence;

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_handlers, onInactive(testing::_))
        .Times(1)
        .InSequence(sequence);

    test.conductor.lock().unwrap().onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);
    std::shared_ptr<Subscription> sub = test.conductor.lock().unwrap().findSubscription(id);
    test.conductor.lock().unwrap().onAvailableImage(correlation_id, SESSION_ID, 1, id, self.log_file_name, SOURCE_IDENTITY);
    test.conductor.lock().unwrap().onUnavailableImage(correlation_id, id);
    EXPECT_FALSE(sub->hasImage(correlation_id));
}

TEST_F(ClientConductorTest, shouldNotCallInactiveConnectionIfNoOperationSuccess)
{
    let id = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    let correlation_id = id + 1;

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(0);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(0);
    EXPECT_CALL(m_handlers, onInactive(testing::_))
        .Times(0);

    // must be able to handle newImage even if findSubscription not called
    test.conductor.lock().unwrap().onAvailableImage(correlation_id, SESSION_ID, 1, id, self.log_file_name, SOURCE_IDENTITY);
    test.conductor.lock().unwrap().onUnavailableImage(correlation_id, id);
}

TEST_F(ClientConductorTest, shouldNotCallInactiveConnectionIfUninterestingConnectioncorrelation_id)
{
    let id = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    let correlation_id = id + 1;
    testing::Sequence sequence;

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_handlers, onInactive(testing::_))
        .Times(0);

    test.conductor.lock().unwrap().onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);
    std::shared_ptr<Subscription> sub = test.conductor.lock().unwrap().findSubscription(id);
    test.conductor.lock().unwrap().onAvailableImage(correlation_id, SESSION_ID, 1, id, self.log_file_name, SOURCE_IDENTITY);
    test.conductor.lock().unwrap().onUnavailableImage(correlation_id + 1, id);
    EXPECT_TRUE(sub->hasImage(correlation_id));

    testing::Mock::VerifyAndClearExpectations(&m_handlers);  // avoid catching unavailable call on sub release
}

TEST_F(ClientConductorTest, shouldCallUnavailableImageIfSubscriptionReleased)
{
    let id = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    let correlation_id = id + 1;
    testing::Sequence sequence;

    EXPECT_CALL(m_handlers, onNewSub(CHANNEL, STREAM_ID, id))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_handlers, onNewImage(testing::_))
        .Times(1)
        .InSequence(sequence);
    EXPECT_CALL(m_handlers, onInactive(testing::_))
        .Times(1);

    test.conductor.lock().unwrap().onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);
    std::shared_ptr<Subscription> sub = test.conductor.lock().unwrap().findSubscription(id);
    test.conductor.lock().unwrap().onAvailableImage(correlation_id, SESSION_ID, 1, id, self.log_file_name, SOURCE_IDENTITY);
    EXPECT_TRUE(sub->hasImage(correlation_id));
}

TEST_F(ClientConductorTest, shouldClosePublicationOnInterServiceTimeout)
{
    let id = test.conductor.lock().unwrap().add_publication(str_to_c(CHANNEL), STREAM_ID).expect("failed to add publication");

    test.conductor.lock().unwrap().onNewPublication(
        id, id, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, self.log_file_name);

    std::shared_ptr<Publication> pub = test.conductor.lock().unwrap().findPublication(id);

    ASSERT_TRUE(pub != nullptr);

    test.conductor.lock().unwrap().closeAllResources(self.current_time);
    EXPECT_TRUE(pub->isClosed());
}

TEST_F(ClientConductorTest, shouldCloseExclusivePublicationOnInterServiceTimeout)
{
    let id = test.conductor.lock().unwrap().addExclusivePublication(CHANNEL, STREAM_ID);

    test.conductor.lock().unwrap().onNewExclusivePublication(
        id, id, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, self.log_file_name);

    std::shared_ptr<ExclusivePublication> pub = test.conductor.lock().unwrap().findExclusivePublication(id);

    ASSERT_TRUE(pub != nullptr);

    test.conductor.lock().unwrap().closeAllResources(self.current_time);
    EXPECT_TRUE(pub->isClosed());
}

TEST_F(ClientConductorTest, shouldCloseSubscriptionOnInterServiceTimeout)
{
    let id = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    test.conductor.lock().unwrap().onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);

    std::shared_ptr<Subscription> sub = test.conductor.lock().unwrap().findSubscription(id);

    ASSERT_TRUE(sub != nullptr);

    test.conductor.lock().unwrap().closeAllResources(self.current_time);

    EXPECT_TRUE(sub->isClosed());
}

TEST_F(ClientConductorTest, shouldCloseAllPublicationsAndSubscriptionsOnInterServiceTimeout)
{
    let pubId = test.conductor.lock().unwrap().add_publication(str_to_c(CHANNEL), STREAM_ID).expect("failed to add publication");
    let exPubId = test.conductor.lock().unwrap().addExclusivePublication(CHANNEL, STREAM_ID);
    let subId = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);

    test.conductor.lock().unwrap().onNewPublication(
        pubId, pubId, STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID, CHANNEL_STATUS_INDICATOR_ID, self.log_file_name);
    test.conductor.lock().unwrap().onNewExclusivePublication(exPubId, exPubId,
        STREAM_ID, SESSION_ID, PUBLICATION_LIMIT_COUNTER_ID_2, CHANNEL_STATUS_INDICATOR_ID, self.log_file_name2);
    test.conductor.lock().unwrap().onSubscriptionReady(subId, CHANNEL_STATUS_INDICATOR_ID);

    std::shared_ptr<Publication> pub = test.conductor.lock().unwrap().findPublication(pubId);

    ASSERT_TRUE(pub != nullptr);

    std::shared_ptr<Subscription> sub = test.conductor.lock().unwrap().findSubscription(subId);

    ASSERT_TRUE(sub != nullptr);

    std::shared_ptr<ExclusivePublication> exPub = test.conductor.lock().unwrap().findExclusivePublication(exPubId);

    ASSERT_TRUE(exPub != nullptr);

    test.conductor.lock().unwrap().closeAllResources(self.current_time);
    EXPECT_TRUE(pub->isClosed());
    EXPECT_TRUE(sub->isClosed());
    EXPECT_TRUE(exPub->isClosed());
}

TEST_F(ClientConductorTest, shouldRemoveImageOnInterServiceTimeout)
{
    let id = test.conductor.lock().unwrap().addSubscription(
        CHANNEL, STREAM_ID, m_onAvailableImageHandler, m_onUnavailableImageHandler);
    let correlation_id = id + 1;

    test.conductor.lock().unwrap().onSubscriptionReady(id, CHANNEL_STATUS_INDICATOR_ID);

    std::shared_ptr<Subscription> sub = test.conductor.lock().unwrap().findSubscription(id);

    ASSERT_TRUE(sub != nullptr);

    test.conductor.lock().unwrap().onAvailableImage(correlation_id, SESSION_ID, 1, id, self.log_file_name, SOURCE_IDENTITY);
    ASSERT_TRUE(sub->hasImage(correlation_id));

    test.conductor.lock().unwrap().closeAllResources(self.current_time);

    std::shared_ptr<Image> image = sub->imageBySessionId(SESSION_ID);

    EXPECT_TRUE(sub->isClosed());
    EXPECT_TRUE(image == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnNullForUnknownCounter)
{
    std::shared_ptr<Counter> counter = test.conductor.lock().unwrap().findCounter(100);

    EXPECT_TRUE(counter == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnNullForCounterWithoutOnAvailableCounter)
{
    let id = test.conductor.lock().unwrap().addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);

    std::shared_ptr<Counter> counter = test.conductor.lock().unwrap().findCounter(id);

    EXPECT_TRUE(counter == nullptr);
}

TEST_F(ClientConductorTest, shouldSendAddCounterToDriver)
{
    let id = test.conductor.lock().unwrap().addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);
    static std::int32_t ADD_COUNTER = ControlProtocolEvents::ADD_COUNTER;

    int count = test.many_to_one_ring_buffer.read(
        [&](std::int32_t msg_type_id, concurrent::AtomicBuffer& buffer, util::index_t offset, util::index_t length)
        {
            const CounterMessageFlyweight message(buffer, offset);

            assert_eq!(msg_type_id, ADD_COUNTER);
            assert_eq!(message.correlation_id(), id);
            assert_eq!(message.typeId(), COUNTER_TYPE_ID);
            assert_eq!(message.keyLength(), 0);
            assert_eq!(message.label(), COUNTER_LABEL);
        });

    assert_eq!(count, 1);
}

TEST_F(ClientConductorTest, shouldReturnCounterAfterOnAvailableCounter)
{
    let id = test.conductor.lock().unwrap().addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);

    EXPECT_CALL(m_handlers, onAvailableCounter(testing::_, id, COUNTER_ID))
        .Times(1);

    test.conductor.lock().unwrap().onAvailableCounter(id, COUNTER_ID);

    std::shared_ptr<Counter> counter = test.conductor.lock().unwrap().findCounter(id);

    ASSERT_TRUE(counter != nullptr);
    assert_eq!(counter->registrationId(), id);
    assert_eq!(counter->id(), COUNTER_ID);
}

TEST_F(ClientConductorTest, shouldReleaseCounterAfterGoingOutOfScope)
{
    let id = test.conductor.lock().unwrap().addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);
    static std::int32_t REMOVE_COUNTER = ControlProtocolEvents::REMOVE_COUNTER;

    test.many_to_one_ring_buffer.read(
        [&](std::int32_t, concurrent::AtomicBuffer&, util::index_t, util::index_t)
        {
        });

    test.conductor.lock().unwrap().onAvailableCounter(id, COUNTER_ID);

    {
        std::shared_ptr<Counter> counter = test.conductor.lock().unwrap().findCounter(id);

        ASSERT_TRUE(counter != nullptr);
    }

    int count = test.many_to_one_ring_buffer.read(
        [&](std::int32_t msg_type_id, concurrent::AtomicBuffer& buffer, util::index_t offset, util::index_t length)
        {
            const RemoveMessageFlyweight message(buffer, offset);

            assert_eq!(msg_type_id, REMOVE_COUNTER);
            assert_eq!(message.registrationId(), id);
        });

    assert_eq!(count, 1);

    std::shared_ptr<Counter> counterPost = test.conductor.lock().unwrap().findCounter(id);
    ASSERT_TRUE(counterPost == nullptr);
}

TEST_F(ClientConductorTest, shouldReturnDifferentIdsForDuplicateAddCounter)
{
    let id1 = test.conductor.lock().unwrap().addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);
    let id2 = test.conductor.lock().unwrap().addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);

    EXPECT_NE(id1, id2);
}

TEST_F(ClientConductorTest, shouldReturnSameFindCounterAfterOnAvailableCounter)
{
    let id = test.conductor.lock().unwrap().addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);

    test.conductor.lock().unwrap().onAvailableCounter(id, COUNTER_ID);

    std::shared_ptr<Counter> counter1 = test.conductor.lock().unwrap().findCounter(id);
    std::shared_ptr<Counter> counter2 = test.conductor.lock().unwrap().findCounter(id);

    ASSERT_TRUE(counter1 != nullptr);
    ASSERT_TRUE(counter2 != nullptr);
    ASSERT_TRUE(counter1 == counter2);
}

TEST_F(ClientConductorTest, shouldReturnDifferentCounterAfterOnAvailableCounter)
{
    let id1 = test.conductor.lock().unwrap().addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);
    let id2 = test.conductor.lock().unwrap().addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);

    EXPECT_CALL(m_handlers, onAvailableCounter(testing::_, id1, COUNTER_ID))
        .Times(1);
    EXPECT_CALL(m_handlers, onAvailableCounter(testing::_, id2, COUNTER_ID))
        .Times(1);

    test.conductor.lock().unwrap().onAvailableCounter(id1, COUNTER_ID);
    test.conductor.lock().unwrap().onAvailableCounter(id2, COUNTER_ID);

    std::shared_ptr<Counter> counter1 = test.conductor.lock().unwrap().findCounter(id1);
    std::shared_ptr<Counter> counter2 = test.conductor.lock().unwrap().findCounter(id2);

    ASSERT_TRUE(counter1 != nullptr);
    ASSERT_TRUE(counter2 != nullptr);
    ASSERT_TRUE(counter1 != counter2);
}

TEST_F(ClientConductorTest, shouldNotFindCounterOnAvailableCounterForUnknowncorrelation_id)
{
    let id = test.conductor.lock().unwrap().addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);

    EXPECT_CALL(m_handlers, onAvailableCounter(testing::_, id + 1, COUNTER_ID))
        .Times(1);

    test.conductor.lock().unwrap().onAvailableCounter(id + 1, COUNTER_ID);

    std::shared_ptr<Counter> counter = test.conductor.lock().unwrap().findCounter(id);

    ASSERT_TRUE(counter == nullptr);
}

TEST_F(ClientConductorTest, shouldTimeoutAddCounterWithoutOnAvailableCounter)
{
    let id = test.conductor.lock().unwrap().addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);

    self.current_time += DRIVER_TIMEOUT_MS + 1;

    ASSERT_THROW(
        {
            std::shared_ptr<Counter> counter = test.conductor.lock().unwrap().findCounter(id);
        },
        util::DriverTimeoutException);
}

TEST_F(ClientConductorTest, shouldExceptionOnFindWhenReceivingErrorResponseOnAddCounter)
{
    let id = test.conductor.lock().unwrap().addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);

    test.conductor.lock().unwrap().onErrorResponse(id, ERROR_CODE_GENERIC_ERROR, "can't add counter");

    ASSERT_THROW(
        {
            std::shared_ptr<Counter> counter = test.conductor.lock().unwrap().findCounter(id);
        },
        util::RegistrationException);
}

TEST_F(ClientConductorTest, shouldCallOnUnavailableCounter)
{
    let id = 101;

    EXPECT_CALL(m_handlers, onUnavailableCounter(testing::_, id, COUNTER_ID))
        .Times(1);

    test.conductor.lock().unwrap().onUnavailableCounter(id, COUNTER_ID);
}

TEST_F(ClientConductorTest, shouldThrowExceptionOnReentrantCallback)
{
    test.conductor.lock().unwrap().addAvailableCounterHandler(
        [&](CountersReader& countersReader, let registrationId, std::int32_t counterId)
        {
            test.conductor.lock().unwrap().addCounter(COUNTER_TYPE_ID, nullptr, 0, COUNTER_LABEL);
        });

    let id = 101;
    std::int32_t counterId = 7;
    bool called = false;
    m_errorHandler = [&](const std::exception& exception) { called = true; };

    test.conductor.lock().unwrap().onAvailableCounter(id, counterId);

    EXPECT_TRUE(called);
}
*/
