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

use std::{
    collections::HashMap,
    ffi::{CStr, CString},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Weak,
    },
};

use crate::utils::errors::{DriverInteractionError, GenericError, IllegalArgumentError};
use crate::{
    concurrent::{
        agent_runner::Agent,
        atomic_buffer::AtomicBuffer,
        atomic_counter::AtomicCounter,
        broadcast::copy_broadcast_receiver::CopyBroadcastReceiver,
        counters::{self, CountersReader},
        logbuffer::term_reader::ErrorHandler,
        position::UnsafeBufferPosition,
        status::status_indicator_reader,
    },
    context::{
        OnAvailableCounter, OnAvailableImage, OnCloseClient, OnNewPublication, OnNewSubscription, OnUnavailableCounter,
        OnUnavailableImage,
    },
    counter::Counter,
    driver_listener_adapter::{DriverListener, DriverListenerAdapter},
    driver_proxy::DriverProxy,
    exclusive_publication::ExclusivePublication,
    heartbeat_timestamp,
    image::Image,
    publication::Publication,
    subscription::Subscription,
    ttrace,
    utils::{
        errors::AeronError::{self, ChannelEndpointException},
        log_buffers::LogBuffers,
        misc::CallbackGuard,
        types::{Moment, MAX_MOMENT},
    },
};

type EpochClock = fn() -> Moment;

const KEEPALIVE_TIMEOUT_MS: Moment = 500;
const RESOURCE_TIMEOUT_MS: Moment = 1000;

/// MediaDriver
#[derive(PartialEq, Debug, Copy, Clone)]
pub enum RegistrationStatus {
    Awaiting,
    Registered,
    Errored,
}

struct PublicationStateDefn {
    error_message: CString,
    buffers: Option<Arc<LogBuffers>>, // PublicationStateDefn could be created without it
    publication: Option<Weak<Mutex<Publication>>>, // and then these fields will be set later.
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
    publication: Option<Weak<Mutex<ExclusivePublication>>>,
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
    on_available_image_handler: Box<dyn OnAvailableImage>,
    on_unavailable_image_handler: Box<dyn OnUnavailableImage>,
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
        on_available_image_handler: Box<dyn OnAvailableImage>,
        on_unavailable_image_handler: Box<dyn OnUnavailableImage>,
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
    #[allow(dead_code)]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
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

    counters_reader: Arc<CountersReader>,
    counter_values_buffer: AtomicBuffer,

    on_new_publication_handler: Box<dyn OnNewPublication>,
    on_new_exclusive_publication_handler: Box<dyn OnNewPublication>,
    on_new_subscription_handler: Box<dyn OnNewSubscription>,
    error_handler: Box<dyn ErrorHandler>,

    on_available_counter_handlers: Vec<Box<dyn OnAvailableCounter>>,
    on_unavailable_counter_handlers: Vec<Box<dyn OnUnavailableCounter>>,
    on_close_client_handlers: Vec<Box<dyn OnCloseClient>>,

    epoch_clock: Box<dyn Fn() -> Moment>,
    driver_timeout_ms: Moment,
    resource_linger_timeout_ms: Moment,
    inter_service_timeout_ms: Moment,
    pre_touch_mapped_memory: bool,
    is_in_callback: bool,
    driver_active: AtomicBool,
    is_closed: AtomicBool,
    //admin_lock: Mutex<()>,
    heartbeat_timestamp: Option<Box<AtomicCounter>>,

    time_of_last_do_work_ms: Moment,
    time_of_last_keepalive_ms: Moment,
    time_of_last_check_managed_resources_ms: Moment,

    arced_self: Option<Arc<Mutex<ClientConductor>>>,

    padding: [u8; crate::utils::misc::CACHE_LINE_LENGTH as usize],
}

impl ClientConductor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        epoch_clock: EpochClock,
        driver_proxy: Arc<DriverProxy>,
        broadcast_receiver: Arc<Mutex<CopyBroadcastReceiver>>,
        counter_metadata_buffer: AtomicBuffer,
        counter_values_buffer: AtomicBuffer,
        on_new_publication_handler: Box<dyn OnNewPublication>,
        on_new_exclusive_publication_handler: Box<dyn OnNewPublication>,
        on_new_subscription_handler: Box<dyn OnNewSubscription>,
        error_handler: Box<dyn ErrorHandler>,
        on_available_counter_handler: Box<dyn OnAvailableCounter>,
        on_unavailable_counter_handler: Box<dyn OnUnavailableCounter>,
        on_close_client_handler: Box<dyn OnCloseClient>,
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
            counters_reader: Arc::new(CountersReader::new(counter_metadata_buffer, counter_values_buffer)),
            counter_values_buffer,
            on_new_publication_handler,
            on_new_exclusive_publication_handler,
            on_new_subscription_handler,
            error_handler,
            on_available_counter_handlers: vec![],
            on_unavailable_counter_handlers: vec![],
            on_close_client_handlers: vec![],
            epoch_clock: Box::new(epoch_clock),
            driver_timeout_ms,
            resource_linger_timeout_ms,
            inter_service_timeout_ms: inter_service_timeout_ns / 1_000_000,
            pre_touch_mapped_memory,
            is_in_callback: false,
            driver_active: AtomicBool::from(true),
            is_closed: AtomicBool::from(false),
            //admin_lock: Mutex::new(()),
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

    pub fn set_epoch_clock_provider(&mut self, new_provider: Box<dyn Fn() -> Moment>) {
        self.epoch_clock = new_provider;
    }

    pub fn set_error_handler(&mut self, new_handler: Box<dyn ErrorHandler>) {
        self.error_handler = new_handler;
    }

    pub fn set_on_new_publication_handler(&mut self, new_handler: Box<dyn OnNewPublication>) {
        self.on_new_publication_handler = new_handler;
    }

    pub fn set_on_new_subscription_handler(&mut self, new_handler: Box<dyn OnNewSubscription>) {
        self.on_new_subscription_handler = new_handler;
    }

    pub fn add_on_available_counter_handler(&mut self, handler: Box<dyn OnAvailableCounter>) {
        self.on_available_counter_handlers.push(handler);
    }

    pub fn add_on_unavailable_counter_handler(&mut self, handler: Box<dyn OnUnavailableCounter>) {
        self.on_unavailable_counter_handlers.push(handler);
    }

    pub fn counters_reader(&self) -> Result<Arc<CountersReader>, AeronError> {
        self.ensure_open()?;
        Ok(self.counters_reader.clone())
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
            Err(GenericError::ClientConductorClosed.into())
        } else {
            Ok(())
        }
    }

    pub fn counter_values_buffer(&self) -> AtomicBuffer {
        self.counter_values_buffer
    }

    fn on_heartbeat_check_timeouts(&mut self) -> i64 {
        let now_ms = (self.epoch_clock)();
        let mut result: i64 = 0;

        if now_ms > self.time_of_last_do_work_ms + self.inter_service_timeout_ms {
            self.close_all_resources(now_ms);

            let err = GenericError::TimeoutBetweenServiceCallsOverTimeout(self.inter_service_timeout_ms).into();

            ttrace!("on_heartbeat_check_timeouts: {:?}", &err);

            self.error_handler.call(err);
        }

        self.time_of_last_do_work_ms = now_ms;

        if now_ms > self.time_of_last_keepalive_ms + KEEPALIVE_TIMEOUT_MS {
            let last_keepalive: Moment = if self.driver_proxy.time_of_last_driver_keepalive() >= 0 {
                self.driver_proxy.time_of_last_driver_keepalive() as Moment + self.driver_timeout_ms
            } else {
                MAX_MOMENT
            };

            if now_ms > last_keepalive {
                self.driver_active.store(false, Ordering::SeqCst);

                let err = DriverInteractionError::WasInactive(self.driver_timeout_ms).into();

                ttrace!("on_heartbeat_check_timeouts: {:?}", &err);

                self.error_handler.call(err);
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

                    let err = GenericError::ClientHeartbeatNotActive.into();

                    ttrace!("on_heartbeat_check_timeouts: {:?}", &err);

                    self.error_handler.call(err);
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

        result
    }

    pub fn verify_driver_is_active(&self) -> Result<(), AeronError> {
        if !self.driver_active.load(Ordering::SeqCst) {
            Err(DriverInteractionError::Inactive.into())
        } else {
            Ok(())
        }
    }

    pub fn verify_driver_is_active_via_error_handler(&self) {
        if !self.driver_active.load(Ordering::SeqCst) {
            let err = DriverInteractionError::Inactive.into();
            self.error_handler.call(err);
        }
    }

    pub fn ensure_not_reentrant(&self) {
        if self.is_in_callback {
            let err = AeronError::ReentrantException;
            self.error_handler.call(err);
        }
    }

    /// Returns thread safe shared mutable instance of LogBuffers
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
            let log_buffer = LogBuffers::from_existing(log_filename.into_string().expect("CString conv error"), touch)?;

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
        /*
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in add_publication");
            */
        ttrace!(
            "add_publication: on channel:{} stream:{}",
            channel.to_str().unwrap(),
            stream_id
        );

        self.verify_driver_is_active()?;
        self.ensure_not_reentrant();
        self.ensure_open()?;

        let registration_id = self.driver_proxy.add_publication(channel.clone(), stream_id)?;

        self.publication_by_registration_id.insert(
            registration_id,
            PublicationStateDefn::new(channel, registration_id, stream_id, (self.epoch_clock)()),
        );

        ttrace!("add_publication: publication ADDED with registration_id {}", registration_id);

        Ok(registration_id)
    }

    pub fn find_publication(&mut self, registration_id: i64) -> Result<Arc<Mutex<Publication>>, AeronError> {
        /*
        let _guard = self
            .admin_lock
            .lock()
            .expect("Failed to obtain admin_lock in find_publication");
            */
        ttrace!("find_publication: with registration_id {}", registration_id);

        self.ensure_not_reentrant();
        self.ensure_open()?;

        let mut publication_to_remove: Option<i64> = None;

        let result = if let Some(state) = self.publication_by_registration_id.get_mut(&registration_id) {
            // try to upgrade weak ptr to strong one.
            if let Some(maybe_publication) = &state.publication {
                // If there is publication - just return it
                if let Some(publication) = maybe_publication.upgrade() {
                    ttrace!(
                        "find_publication: existing publication with registration_id {} FOUND",
                        registration_id
                    );
                    Ok(publication)
                } else {
                    Err(GenericError::PublicationAlreadyDropped.into())
                }
            } else {
                // Otherwise fill in the state.publication
                match state.status {
                    RegistrationStatus::Awaiting => {
                        if (self.epoch_clock)() > state.time_of_registration_ms + self.driver_timeout_ms {
                            Err(DriverInteractionError::NoResponse(self.driver_timeout_ms).into())
                        } else {
                            Err(AeronError::PublicationNotReady(registration_id))
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

                            let new_pub = Arc::new(Mutex::new(publication));
                            state.publication = Some(Arc::downgrade(&new_pub));
                            ttrace!(
                                "find_publication: publication with registration_id {} CREATED",
                                registration_id
                            );

                            Ok(new_pub)
                        } else {
                            Err(GenericError::BufferNotSetForPublication {
                                registration_id: state.registration_id,
                            }
                            .into())
                        }
                    }

                    RegistrationStatus::Errored => {
                        publication_to_remove = Some(registration_id);
                        Err(ClientConductor::return_registration_error(
                            state.error_code,
                            &state.error_message,
                        ))
                    }
                }
            }
        } else {
            // error, publication not found
            return Err(GenericError::PublicationNotFound.into());
        };

        if let Some(id) = publication_to_remove {
            self.publication_by_registration_id.remove(&id);
        }

        result
    }

    pub fn return_registration_error(err_code: i32, err_message: &CStr) -> AeronError {
        AeronError::RegistrationException(err_code, String::from(err_message.to_str().expect("CStr conversion error")))
    }

    pub fn release_publication(&mut self, registration_id: i64) -> Result<(), AeronError> {
        ttrace!("release_publication: with registration_id {}", registration_id);

        self.verify_driver_is_active_via_error_handler();

        if let Some(_publication) = self.publication_by_registration_id.get(&registration_id) {
            let _result = self.driver_proxy.remove_publication(registration_id);
            self.publication_by_registration_id.remove(&registration_id);
            ttrace!(
                "release_publication: publication with registration_id {} RELEASED",
                registration_id
            );
        } else {
            ttrace!(
                "release_publication: publication with registration_id {} not found",
                registration_id
            );
            return Err(GenericError::UnknownRegistrationId(registration_id).into());
        }

        Ok(())
    }

    pub fn add_exclusive_publication(&mut self, channel: CString, stream_id: i32) -> Result<i64, AeronError> {
        ttrace!(
            "add_exclusive_publication: on channel:{} stream:{}",
            channel.to_str().unwrap(),
            stream_id
        );

        self.verify_driver_is_active()?;
        self.ensure_not_reentrant();
        self.ensure_open()?;

        let registration_id = self.driver_proxy.add_exclusive_publication(channel.clone(), stream_id)?;

        self.exclusive_publication_by_registration_id.insert(
            registration_id,
            ExclusivePublicationStateDefn::new(channel, registration_id, stream_id, (self.epoch_clock)()),
        );

        ttrace!(
            "add_exclusive_publication: exclusive publication ADDED with registration_id {}",
            registration_id
        );

        Ok(registration_id)
    }

    pub(crate) fn find_exclusive_publication(
        &mut self,
        registration_id: i64,
    ) -> Result<Arc<Mutex<ExclusivePublication>>, AeronError> {
        ttrace!("find_exclusive_publication: with registration_id {}", registration_id);

        self.ensure_not_reentrant();
        self.ensure_open()?;
        let mut publication_to_remove: Option<i64> = None;

        let result = if let Some(state) = self.exclusive_publication_by_registration_id.get_mut(&registration_id) {
            // try to upgrade weak ptr to strong one.
            if let Some(maybe_publication) = &state.publication {
                // If there is publication - just return it
                if let Some(publication) = maybe_publication.upgrade() {
                    ttrace!(
                        "find_exclusive_publication: existing exclusive publication with registration_id {} FOUND",
                        registration_id
                    );
                    Ok(publication)
                } else {
                    Err(GenericError::ExclusivePublicationAlreadyDropped.into())
                }
            } else {
                // Otherwise fill in the state.publication
                match state.status {
                    RegistrationStatus::Awaiting => {
                        if (self.epoch_clock)() > state.time_of_registration_ms + self.driver_timeout_ms {
                            Err(DriverInteractionError::NoResponse(self.driver_timeout_ms).into())
                        } else {
                            Err(GenericError::ExclusivePublicationNotReadyYet { status: state.status }.into())
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

                            let new_pub = Arc::new(Mutex::new(publication));
                            state.publication = Some(Arc::downgrade(&new_pub));

                            ttrace!(
                                "find_exclusive_publication: exclusive publication with registration_id {} CREATED",
                                registration_id
                            );

                            Ok(new_pub)
                        } else {
                            Err(GenericError::BufferNotSetForExclusivePublication {
                                registration_id: state.registration_id,
                            }
                            .into())
                        }
                    }

                    RegistrationStatus::Errored => {
                        publication_to_remove = Some(registration_id);
                        Err(ClientConductor::return_registration_error(
                            state.error_code,
                            &state.error_message,
                        ))
                    }
                }
            }
        } else {
            // error, publication not found
            return Err(GenericError::ExclusivePublicationNotFound.into());
        };

        if let Some(id) = publication_to_remove {
            self.exclusive_publication_by_registration_id.remove(&id);
        }

        result
    }

    pub fn release_exclusive_publication(&mut self, registration_id: i64) -> Result<(), AeronError> {
        ttrace!("release_exclusive_publication: with registration_id {}", registration_id);

        self.verify_driver_is_active_via_error_handler();

        if let Some(_publication) = self.exclusive_publication_by_registration_id.get(&registration_id) {
            let _result = self.driver_proxy.remove_publication(registration_id);
            self.exclusive_publication_by_registration_id.remove(&registration_id);
            ttrace!(
                "release_exclusive_publication: exclusive publication with registration_id {} RELEASED",
                registration_id
            );
        } else {
            ttrace!(
                "release_exclusive_publication: exclusive publication with registration_id {} not found",
                registration_id
            );
            return Err(GenericError::UnknownRegistrationId(registration_id).into());
        }
        Ok(())
    }

    pub fn add_subscription(
        &mut self,
        channel: CString,
        stream_id: i32,
        on_available_image_handler: Box<dyn OnAvailableImage>,
        on_unavailable_image_handler: Box<dyn OnUnavailableImage>,
    ) -> Result<i64, AeronError> {
        ttrace!(
            "add_subscription: on channel:{} stream:{}",
            channel.to_str().unwrap(),
            stream_id
        );

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

        ttrace!(
            "add_subscription: subscription ADDED with registration_id {}",
            registration_id
        );

        Ok(registration_id)
    }

    pub fn find_subscription(&mut self, registration_id: i64) -> Result<Arc<Mutex<Subscription>>, AeronError> {
        ttrace!("find_subscription: with registration_id {}", registration_id);

        self.ensure_not_reentrant();
        self.ensure_open()?;

        // These two tricky fields are needed to avoid double mut borrows of publication_by_registration_id
        let mut subscription_to_remove: Option<i64> = None;

        let result = if let Some(state) = self.subscription_by_registration_id.get_mut(&registration_id) {
            // try to upgrade weak ptr to strong one.
            if let Some(maybe_subscription) = &state.subscription {
                // If there is subscription - just return it
                let this_arm_result = if let Some(subscription) = maybe_subscription.upgrade() {
                    ttrace!(
                        "find_subscription: existing subscription with registration_id {} FOUND",
                        registration_id
                    );
                    Ok(subscription)
                } else {
                    Err(GenericError::SubscriptionAlreadyDropped.into())
                };

                state.subscription_cache = None; // Clear the cache. It will decrease Arc cnt for previous subscription so it may drop.

                this_arm_result
            } else {
                // state.subscription in None - was not set in the past
                if RegistrationStatus::Awaiting == state.status {
                    if (self.epoch_clock)() > state.time_of_registration_ms + self.driver_timeout_ms {
                        Err(DriverInteractionError::NoResponse(self.driver_timeout_ms).into())
                    } else {
                        Err(AeronError::SubscriptionNotReady(registration_id))
                    }
                } else if RegistrationStatus::Errored == state.status {
                    subscription_to_remove = Some(registration_id);
                    Err(ClientConductor::return_registration_error(
                        state.error_code,
                        &state.error_message,
                    ))
                } else {
                    Err(GenericError::SubscriptionWasNotCreatedBefore { status: state.status }.into())
                }
            }
        } else {
            Err(GenericError::SubscriptionNotFound.into())
        };

        if let Some(id) = subscription_to_remove {
            self.subscription_by_registration_id.remove(&id);
        }

        result
    }

    pub fn release_subscription(&mut self, registration_id: i64, mut images: Vec<Image>) -> Result<(), AeronError> {
        ttrace!("release_subscription: with registration_id {}", registration_id);

        self.verify_driver_is_active_via_error_handler();

        if let Some(subscription) = self.subscription_by_registration_id.get(&registration_id) {
            let _result = self.driver_proxy.remove_subscription(registration_id);

            for image in images.iter_mut() {
                // close the image
                image.close();

                let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
                subscription.on_unavailable_image_handler.call(image);
            }
        } else {
            ttrace!(
                "release_subscription: subscription with registration_id {} not found",
                registration_id
            );
            return Err(GenericError::UnknownRegistrationId(registration_id).into());
        }

        self.linger_all_resources((self.epoch_clock)(), images);
        self.subscription_by_registration_id.remove(&registration_id);
        ttrace!(
            "release_subscription: subscription with registration_id {} RELEASED together with its images",
            registration_id
        );

        Ok(())
    }

    pub fn add_counter(&mut self, type_id: i32, key_buffer: &[u8], label: &str) -> Result<i64, AeronError> {
        ttrace!("add_counter: with type_id:{} label:{}", type_id, label);

        self.verify_driver_is_active()?;
        self.ensure_not_reentrant();
        self.ensure_open()?;

        if key_buffer.len() > counters::MAX_KEY_LENGTH as usize {
            return Err(IllegalArgumentError::KeyLengthIsOutOfBounds {
                key_length: key_buffer.len(),
                limit: counters::MAX_KEY_LENGTH,
            }
            .into());
        }

        if label.len() > counters::MAX_LABEL_LENGTH as usize {
            return Err(IllegalArgumentError::LabelLengthIsOutOfBounds {
                label_length: label.len(),
                limit: counters::MAX_LABEL_LENGTH,
            }
            .into());
        }

        let registration_id = self
            .driver_proxy
            .add_counter(type_id, key_buffer, CString::new(label).unwrap())?;

        self.counter_by_registration_id
            .insert(registration_id, CounterStateDefn::new(registration_id, (self.epoch_clock)()));

        ttrace!(
            "add_counter: counter type_id:{} label:{} ADDED with registration_id {}",
            type_id,
            label,
            registration_id
        );

        Ok(registration_id)
    }

    pub fn find_counter(&mut self, registration_id: i64) -> Result<Arc<Counter>, AeronError> {
        ttrace!("find_counter: with registration_id {}", registration_id);

        self.ensure_not_reentrant();
        self.ensure_open()?;

        let mut counter_to_remove: Option<i64> = None;

        let result = if let Some(state) = self.counter_by_registration_id.get_mut(&registration_id) {
            // try to upgrade weak ptr to strong one.
            if let Some(maybe_counter) = &state.counter {
                // If there is counter - just return it
                let this_arm_result = if let Some(counter) = maybe_counter.upgrade() {
                    ttrace!(
                        "find_counter: existing counter with registration_id {} FOUND",
                        registration_id
                    );
                    Ok(counter)
                } else {
                    Err(GenericError::CounterAlreadyDropped.into())
                };

                state.counter_cache = None; // Clear the cache. It will decrease Arc cnt for previous subscription so it may drop.

                this_arm_result
            } else {
                // state.counter in None - was not set in the past
                if RegistrationStatus::Awaiting == state.status {
                    if (self.epoch_clock)() > state.time_of_registration_ms + self.driver_timeout_ms {
                        Err(DriverInteractionError::NoResponse(self.driver_timeout_ms).into())
                    } else {
                        Err(GenericError::CounterNotReadyYet { status: state.status }.into())
                    }
                } else if RegistrationStatus::Errored == state.status {
                    counter_to_remove = Some(registration_id);
                    Err(ClientConductor::return_registration_error(
                        state.error_code,
                        &state.error_message,
                    ))
                } else {
                    Err(GenericError::CounterWasNotCreatedBefore { status: state.status }.into())
                }
            }
        } else {
            Err(GenericError::CounterNotFound.into())
        };

        if let Some(id) = counter_to_remove {
            self.counter_by_registration_id.remove(&id);
        }

        result
    }

    pub fn release_counter(&mut self, registration_id: i64) -> Result<(), AeronError> {
        self.verify_driver_is_active_via_error_handler();

        if let Some(_counter) = self.counter_by_registration_id.get(&registration_id) {
            self.driver_proxy.remove_counter(registration_id)?;
            self.counter_by_registration_id.remove(&registration_id);
            ttrace!("release_counter: counter with registration_id {} RELEASED", registration_id);
        } else {
            ttrace!("release_counter: counter with registration_id {} not found", registration_id);
            return Err(GenericError::UnknownRegistrationId(registration_id).into());
        }

        Ok(())
    }

    pub fn add_destination(&mut self, publication_registration_id: i64, endpoint_channel: CString) -> Result<i64, AeronError> {
        ttrace!(
            "add_destination: with publication_registration_id:{} channel: {}",
            publication_registration_id,
            endpoint_channel.to_str().unwrap()
        );

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

        ttrace!("add_destination: destination ADDED with correlation_id {}", correlation_id);

        Ok(correlation_id)
    }

    pub fn remove_destination(&mut self, publication_registration_id: i64, endpoint_channel: CString) -> Result<i64, AeronError> {
        ttrace!(
            "remove_destination: with publication_registration_id:{} channel: {}",
            publication_registration_id,
            endpoint_channel.to_str().unwrap()
        );

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

        ttrace!("remove_destination: destination REMOVED correlation_id:{}", correlation_id);

        Ok(correlation_id)
    }

    pub fn add_rcv_destination(
        &mut self,
        subscription_registration_id: i64,
        endpoint_channel: CString,
    ) -> Result<i64, AeronError> {
        ttrace!(
            "add_rcv_destination: with subscription_registration_id:{} channel: {}",
            subscription_registration_id,
            endpoint_channel.to_str().unwrap()
        );

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

        ttrace!(
            "add_rcv_destination: rcv destination ADDED with correlation_id {}",
            correlation_id
        );

        Ok(correlation_id)
    }

    pub fn remove_rcv_destination(
        &mut self,
        subscription_registration_id: i64,
        endpoint_channel: CString,
    ) -> Result<i64, AeronError> {
        ttrace!(
            "remove_rcv_destination: with subscription_registration_id:{} channel: {}",
            subscription_registration_id,
            endpoint_channel.to_str().unwrap()
        );

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

        ttrace!(
            "remove_rcv_destination: rcv destination REMOVED correlation_id:{}",
            correlation_id
        );

        Ok(correlation_id)
    }

    pub fn find_destination_response(&mut self, correlation_id: i64) -> Result<bool, AeronError> {
        self.ensure_not_reentrant();
        self.ensure_open()?;

        let destination_to_remove: Option<i64> = None;

        let result = if let Some(state) = self.destination_state_by_correlation_id.get_mut(&correlation_id) {
            match state.status {
                RegistrationStatus::Awaiting => {
                    if (self.epoch_clock)() > state.time_of_registration_ms + self.driver_timeout_ms {
                        Err(DriverInteractionError::NoResponse(self.driver_timeout_ms).into())
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
            Err(GenericError::UnknownCorrelationId(correlation_id).into())
        };

        if let Some(id) = destination_to_remove {
            // Regardless of status remove this destination from the map
            self.destination_state_by_correlation_id.remove(&id);
        }

        result
    }

    pub fn add_available_counter_handler(&mut self, handler: Box<dyn OnAvailableCounter>) -> Result<(), AeronError> {
        self.ensure_not_reentrant();
        self.ensure_open()?;

        self.on_available_counter_handlers.push(handler);
        Ok(())
    }

    pub fn remove_available_counter_handler(&mut self, _handler: Box<dyn OnAvailableCounter>) -> Result<(), AeronError> {
        self.ensure_not_reentrant();
        self.ensure_open()?;

        // self.on_available_counter_handlers.retain(|item| item as usize != handler as usize); FIXME: add registration ID for handlers
        Ok(())
    }

    pub fn add_unavailable_counter_handler(&mut self, handler: Box<dyn OnUnavailableCounter>) -> Result<(), AeronError> {
        self.ensure_not_reentrant();
        self.ensure_open()?;

        self.on_unavailable_counter_handlers.push(handler);
        Ok(())
    }

    pub fn remove_unavailable_counter_handler(&mut self, _handler: Box<dyn OnUnavailableCounter>) -> Result<(), AeronError> {
        self.ensure_not_reentrant();
        self.ensure_open()?;
        //self.on_unavailable_counter_handlers.retain(|item| item != handler); FIXME
        Ok(())
    }

    pub fn add_close_client_handler(&mut self, handler: Box<dyn OnCloseClient>) -> Result<(), AeronError> {
        self.ensure_not_reentrant();
        self.ensure_open()?;

        self.on_close_client_handlers.push(handler);
        Ok(())
    }

    pub fn remove_close_client_handler(&mut self, _handler: Box<dyn OnCloseClient>) -> Result<(), AeronError> {
        self.ensure_not_reentrant();
        self.ensure_open()?;

        //self.on_close_client_handlers.retain(|item| item != handler); FIXME
        Ok(())
    }

    pub fn close_all_resources(&mut self, now_ms: Moment) {
        ttrace!("close_all_resources: closing all resources");

        self.is_closed.store(true, Ordering::Release);

        for pub_defn in self.publication_by_registration_id.values() {
            if let Some(maybe_publication) = &pub_defn.publication {
                if let Some(publication) = maybe_publication.upgrade() {
                    publication.lock().expect("Mutex on pub poisoned").close();
                }
            }
        }
        self.publication_by_registration_id.clear();

        for pub_defn in self.exclusive_publication_by_registration_id.values() {
            if let Some(maybe_publication) = &pub_defn.publication {
                if let Some(publication) = maybe_publication.upgrade() {
                    publication.lock().expect("Mutex on ExPub poisoned").close();
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
                            sub_defn.on_unavailable_image_handler.call(image);
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
                        handler.call(&self.counters_reader, registration_id, counter_id);
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
            handler.call();
        }
    }

    pub fn on_check_managed_resources(&mut self, now_ms: Moment) {
        //let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in self.on_check_managed_resources");

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
        ttrace!("on_start: started as agent");
        Ok(())
    }

    fn do_work(&mut self) -> Result<i32, AeronError> {
        let mut work_count = 0;

        let dla = self.driver_listener_adapter.take().unwrap();
        work_count += dla.receive_messages(self)?;
        self.driver_listener_adapter.replace(dla);
        work_count += self.on_heartbeat_check_timeouts() as usize;
        Ok(work_count as i32)
    }

    fn on_close(&mut self) -> Result<(), AeronError> {
        ttrace!("on_close: stopping as agent");
        if !self.is_closed.load(Ordering::SeqCst) {
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
        let mut log_buffers: Option<Arc<LogBuffers>> = None;
        let mut maybe_channel: Option<CString> = None;

        if let Some(state) = self.publication_by_registration_id.get(&registration_id) {
            ttrace!("on_new_publication: registration_id {}, original_registration_id {}, stream_id {}, session_id {}, publication_limit_counter_id {}, channel_status_indicator_id {}, log_file_name \"{}\"",
                registration_id,
                original_registration_id,
                stream_id,
                session_id,
                publication_limit_counter_id,
                channel_status_indicator_id,
                log_file_name.to_str().unwrap()
            );

            maybe_channel = Some(state.channel.clone());
        }

        if let Some(channel) = maybe_channel {
            log_buffers = Some(
                self.get_log_buffers(original_registration_id, log_file_name.clone(), channel)
                    .unwrap_or_else(|err| {
                        panic!(
                            "Get log_buffers failed, log_filename \"{}\", error {:?}",
                            log_file_name.to_str().unwrap(),
                            err
                        )
                    }),
            );
        }

        if let Some(state) = self.publication_by_registration_id.get_mut(&registration_id) {
            state.status = RegistrationStatus::Registered;
            state.session_id = session_id;
            state.publication_limit_counter_id = publication_limit_counter_id;
            state.channel_status_id = channel_status_indicator_id;
            state.buffers = log_buffers;
            state.original_registration_id = original_registration_id;

            let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
            self.on_new_publication_handler
                .call(state.channel.clone(), stream_id, session_id, registration_id);
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

        let mut log_buffers: Option<Arc<LogBuffers>> = None;
        let mut maybe_channel: Option<CString> = None;

        if let Some(state) = self.exclusive_publication_by_registration_id.get(&registration_id) {
            ttrace!("on_new_exclusive_publication: registration_id {}, original_registration_id {}, stream_id {}, session_id {}, publication_limit_counter_id {}, channel_status_indicator_id {}, log_file_name \"{}\"",
                registration_id,
                original_registration_id,
                stream_id,
                session_id,
                publication_limit_counter_id,
                channel_status_indicator_id,
                log_file_name.to_str().unwrap()
            );

            maybe_channel = Some(state.channel.clone());
        }

        if let Some(channel) = maybe_channel {
            log_buffers = Some(
                self.get_log_buffers(original_registration_id, log_file_name.clone(), channel)
                    .unwrap_or_else(|err| {
                        panic!(
                            "Get log_buffers failed, log_filename \"{}\", error {:?}",
                            log_file_name.to_str().unwrap(),
                            err
                        )
                    }),
            );
        }

        if let Some(state) = self.exclusive_publication_by_registration_id.get_mut(&registration_id) {
            state.status = RegistrationStatus::Registered;
            state.session_id = session_id;
            state.publication_limit_counter_id = publication_limit_counter_id;
            state.channel_status_id = channel_status_indicator_id;
            state.buffers = log_buffers;

            let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
            self.on_new_exclusive_publication_handler
                .call(state.channel.clone(), stream_id, session_id, registration_id);
        }
    }

    fn on_subscription_ready(&mut self, registration_id: i64, channel_status_id: i32) {
        if let Some(state) = self.subscription_by_registration_id.get_mut(&registration_id) {
            ttrace!(
                "on_subscription_ready: registration_id {}, channel_status_id {}",
                registration_id,
                channel_status_id
            );

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
            self.on_new_subscription_handler
                .call(state.channel.clone(), state.stream_id, registration_id);
        }
    }

    fn on_operation_success(&mut self, correlation_id: i64) {
        if let Some(state) = self.destination_state_by_correlation_id.get_mut(&correlation_id) {
            ttrace!("on_operation_success: correlation_id {}", correlation_id);

            if state.status == RegistrationStatus::Awaiting {
                state.status = RegistrationStatus::Registered;
            }
        }
    }

    fn on_channel_endpoint_error_response(&mut self, offending_command_correlation_id: i64, error_message: CString) {
        let mut subscription_to_remove: Vec<i64> = Vec::new();
        let mut linger_images: Vec<Vec<Image>> = Vec::new();

        for (reg_id, subscr_defn) in &mut self.subscription_by_registration_id {
            if let Some(maybe_subscription) = &subscr_defn.subscription {
                if let Some(protected_subscription) = maybe_subscription.upgrade() {
                    let mut subscription = protected_subscription.lock().expect("Mutex poisoned");
                    if subscription.channel_status_id() == offending_command_correlation_id as i32 {
                        ttrace!("on_channel_endpoint_error_response: for subscription, offending_command_correlation_id {}, error_message {}", offending_command_correlation_id, error_message.to_str().unwrap());

                        self.error_handler.call(ChannelEndpointException(
                            offending_command_correlation_id,
                            String::from(error_message.to_str().expect("CString conversion error")),
                        ));

                        if let Some(mut images) = subscription.close_and_remove_images() {
                            for image in images.iter_mut() {
                                image.close();

                                let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
                                subscr_defn.on_unavailable_image_handler.call(image);
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
                    if publication.lock().expect("Mutex on pub poisoned").channel_status_id()
                        == offending_command_correlation_id as i32
                    {
                        ttrace!("on_channel_endpoint_error_response: for publication, offending_command_correlation_id {}, error_message {}", offending_command_correlation_id, error_message.to_str().unwrap());

                        self.error_handler.call(ChannelEndpointException(
                            offending_command_correlation_id,
                            String::from(error_message.to_str().expect("CString conversion error")),
                        ));
                        publication.lock().expect("Mutex on pub poisoned").close();
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
                    if publication.lock().expect("Mutex on pub poisoned").channel_status_id()
                        == offending_command_correlation_id as i32
                    {
                        self.error_handler.call(ChannelEndpointException(
                            offending_command_correlation_id,
                            String::from(error_message.to_str().expect("CString conversion error")),
                        ));
                        publication.lock().expect("Mutex on pub poisoned").close();
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
        if let Some(subscription) = self
            .subscription_by_registration_id
            .get_mut(&offending_command_correlation_id)
        {
            ttrace!(
                "on_error_response: for subscription, offending_command_correlation_id {}, error_code {}, error_message {}",
                offending_command_correlation_id,
                error_code,
                error_message.to_str().unwrap()
            );
            subscription.status = RegistrationStatus::Errored;
            subscription.error_code = error_code;
            subscription.error_message = error_message;
            return;
        }

        if let Some(publication) = self.publication_by_registration_id.get_mut(&offending_command_correlation_id) {
            ttrace!(
                "on_error_response: for publication, offending_command_correlation_id {}, error_code {}, error_message {}",
                offending_command_correlation_id,
                error_code,
                error_message.to_str().unwrap()
            );
            publication.status = RegistrationStatus::Errored;
            publication.error_code = error_code;
            publication.error_message = error_message;
            return;
        }

        if let Some(publication) = self
            .exclusive_publication_by_registration_id
            .get_mut(&offending_command_correlation_id)
        {
            ttrace!("on_error_response: for exclusive publication, offending_command_correlation_id {}, error_code {}, error_message {}", offending_command_correlation_id, error_code, error_message.to_str().unwrap());
            publication.status = RegistrationStatus::Errored;
            publication.error_code = error_code;
            publication.error_message = error_message;
            return;
        }

        if let Some(counter) = self.counter_by_registration_id.get_mut(&offending_command_correlation_id) {
            ttrace!(
                "on_error_response: for counter, offending_command_correlation_id {}, error_code {}, error_message {}",
                offending_command_correlation_id,
                error_code,
                error_message.to_str().unwrap()
            );
            counter.status = RegistrationStatus::Errored;
            counter.error_code = error_code;
            counter.error_message = error_message;
            return;
        }

        if let Some(destination) = self
            .destination_state_by_correlation_id
            .get_mut(&offending_command_correlation_id)
        {
            ttrace!(
                "on_error_response: for destination, offending_command_correlation_id {}, error_code {}, error_message {}",
                offending_command_correlation_id,
                error_code,
                error_message.to_str().unwrap()
            );
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
        let mut log_buffers: Option<Arc<LogBuffers>> = None;
        let mut maybe_channel: Option<CString> = None;

        // This piece of code is separated from the below (with same condition) to make borrow checker happy.
        if let Some(subscr_defn) = self.subscription_by_registration_id.get(&subscription_registration_id) {
            if let Some(maybe_subscription) = &subscr_defn.subscription {
                if let Some(_subscription) = maybe_subscription.upgrade() {
                    maybe_channel = Some(subscr_defn.channel.clone());

                    ttrace!(
                        "on_available_image correlation_id {}, session_id {}, subscriber_position_id {}, subscription_registration_id {}, log_filename {}, source_identity {}",
                        correlation_id,
                        session_id,
                        subscriber_position_id,
                        subscription_registration_id,
                        log_filename.to_str().unwrap(),
                        source_identity.to_str().unwrap()
                    );
                }
            }
        }

        if let Some(channel) = maybe_channel {
            log_buffers = Some(
                self.get_log_buffers(correlation_id, log_filename.clone(), channel)
                    .unwrap_or_else(|err| {
                        panic!(
                            "Get log_buffers failed, log_filename \"{}\", error {:?}",
                            log_filename.to_str().unwrap(),
                            err
                        )
                    }),
            );
        }

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
                        log_buffers.unwrap(),
                        self.error_handler.clone_box(),
                    );

                    let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
                    subscr_defn.on_available_image_handler.call(&image);

                    linger_images = Some(subscription.lock().expect("Mutex poisoned").add_image(image));
                }
            }
        }

        if let Some(images) = linger_images {
            self.linger_resource((self.epoch_clock)(), images);
        }
    }

    fn on_unavailable_image(&mut self, correlation_id: i64, subscription_registration_id: i64) {
        let now_ms = (self.epoch_clock)();

        let mut linger_images: Option<Vec<Image>> = None;

        if let Some(subscr_defn) = self.subscription_by_registration_id.get(&subscription_registration_id) {
            ttrace!(
                "on_unavailable_image correlation_id {}, subscription_registration_id {}",
                correlation_id,
                subscription_registration_id,
            );

            if let Some(maybe_subscription) = &subscr_defn.subscription {
                if let Some(subscription) = maybe_subscription.upgrade() {
                    // If Image was actually removed
                    if let Some((old_image_array, index)) =
                        subscription.lock().expect("Mutex poisoned").remove_image(correlation_id)
                    {
                        let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
                        subscr_defn
                            .on_unavailable_image_handler
                            .call(old_image_array.get(index as usize).expect("Bug in image handling"));
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
        if let Some(state) = self.counter_by_registration_id.get_mut(&registration_id) {
            ttrace!(
                "on_available_counter registration_id {}, counter_id {}",
                registration_id,
                counter_id,
            );

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
        }
        // Handler are called for all counters (not only those created by this Aeron client)
        for handler in &self.on_available_counter_handlers {
            let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
            handler.call(&self.counters_reader, registration_id, counter_id);
        }
    }

    fn on_unavailable_counter(&mut self, registration_id: i64, counter_id: i32) {
        ttrace!(
            "on_unavailable_counter registration_id {}, counter_id {}",
            registration_id,
            counter_id,
        );

        for handler in &self.on_unavailable_counter_handlers {
            let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
            handler.call(&self.counters_reader, registration_id, counter_id);
        }
    }

    fn on_client_timeout(&mut self, client_id: i64) {
        if self.driver_proxy.client_id() == client_id && !self.is_closed() {
            ttrace!("on_client_timeout client_id {}. Closing all resources.", client_id,);

            self.close_all_resources((self.epoch_clock)());
            self.error_handler.call(AeronError::ClientTimeoutException);
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
    use std::sync::atomic::{AtomicBool, Ordering};

    use galvanic_assert::matchers::any_value;
    use galvanic_assert::{assert_that, has_structure, structure};
    use lazy_static::lazy_static;
    use nix::unistd;

    use crate::command::control_protocol_events::AeronCommand;
    use crate::command::counter_message_flyweight::CounterMessageFlyweight;
    use crate::command::error_response_flyweight::{ERROR_CODE_GENERIC_ERROR, ERROR_CODE_INVALID_CHANNEL};
    use crate::command::publication_message_flyweight::PublicationMessageFlyweight;
    use crate::command::remove_message_flyweight::RemoveMessageFlyweight;
    use crate::command::subscription_message_flyweight::SubscriptionMessageFlyweight;
    use crate::concurrent::atomic_buffer::AlignedBuffer;
    use crate::concurrent::broadcast::broadcast_buffer_descriptor;
    use crate::concurrent::broadcast::broadcast_receiver::BroadcastReceiver;
    use crate::concurrent::logbuffer::log_buffer_descriptor;
    use crate::concurrent::ring_buffer;
    use crate::concurrent::ring_buffer::ManyToOneRingBuffer;
    use crate::utils::memory_mapped_file::MemoryMappedFile;
    use crate::utils::misc::unix_time_ms;

    use super::*;

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
    const LOG_FILE_LENGTH: i32 = (TERM_LENGTH * 3) + log_buffer_descriptor::LOG_META_DATA_LENGTH;
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
    const PRE_TOUCH_MAPPED_MEMORY: bool = false;

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

    #[allow(dead_code)]
    struct ClientConductorTest {
        log_file_name: String,
        log_file_name2: String,

        to_driver: AlignedBuffer,
        to_clients: AlignedBuffer,
        counter_metadata: AlignedBuffer,
        counter_values: AlignedBuffer,

        to_driver_buffer: AtomicBuffer,
        to_clients_buffer: AtomicBuffer,
        many_to_one_ring_buffer: Arc<ManyToOneRingBuffer>,

        current_time: Arc<Mutex<Moment>>,
        conductor: Arc<Mutex<ClientConductor>>,
    }

    impl ClientConductorTest {
        pub fn new() -> Self {
            let to_driver = AlignedBuffer::with_capacity(MANY_TO_ONE_RING_BUFFER_LENGTH);
            let to_clients = AlignedBuffer::with_capacity(BROADCAST_BUFFER_LENGTH);
            let counter_metadata = AlignedBuffer::with_capacity(COUNTER_METADATA_BUFFER_LENGTH);
            let counter_values = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);

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
                Box::new(on_new_publication_handler),
                Box::new(on_new_exclusive_publication_handler),
                Box::new(on_new_subscription_handler),
                Box::new(error_handler),
                Box::new(on_available_counter_handler),
                Box::new(on_unavailable_counter_handler),
                Box::new(on_close_client_handler),
                DRIVER_TIMEOUT_MS,
                RESOURCE_LINGER_TIMEOUT_MS,
                INTER_SERVICE_TIMEOUT_NS,
                PRE_TOUCH_MAPPED_MEMORY,
            );

            fn on_close_client_handler() {}

            #[allow(dead_code)]
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
                current_time: Arc::new(Mutex::new(unix_time_ms())),
                conductor: local_conductor,
                counter_values,
            };

            let cloned_curr_time = instance.current_time.clone();
            instance
                .conductor
                .lock()
                .unwrap()
                .set_epoch_clock_provider(Box::new(move || cloned_curr_time.lock().unwrap().to_owned()));

            // Now setup initial state
            instance
                .to_driver_buffer
                .set_memory(0, instance.to_driver_buffer.capacity(), 0);
            instance
                .to_clients_buffer
                .set_memory(0, instance.to_clients_buffer.capacity(), 0);

            instance
                .many_to_one_ring_buffer
                .set_consumer_heartbeat_time(unix_time_ms() as i64);

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

        fn do_work_until_driver_timeout(&mut self) {
            let time_guard = self.current_time.lock().unwrap();
            let start_time = *time_guard;
            let mut current_test_time = *time_guard;

            drop(time_guard); // Unlock it

            while current_test_time <= start_time + DRIVER_TIMEOUT_MS {
                current_test_time += 1000;

                {
                    // Update self.current_time and release the lock
                    let mut time_guard = self.current_time.lock().unwrap();
                    *time_guard = current_test_time;
                }

                let _res = self.conductor.lock().unwrap().do_work();
            }
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
    fn should_send_publication_to_driver() {
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
    fn should_return_publication_after_log_buffers_created() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        test.conductor.lock().unwrap().on_new_publication(
            id,
            id,
            STREAM_ID,
            SESSION_ID,
            PUBLICATION_LIMIT_COUNTER_ID,
            CHANNEL_STATUS_INDICATOR_ID,
            str_to_c(&test.log_file_name),
        );

        let publication = test.conductor.lock().unwrap().find_publication(id).unwrap();
        let publication = publication.lock().unwrap();

        assert_eq!(publication.registration_id(), id);
        assert_eq!(publication.channel(), str_to_c(CHANNEL));
        assert_eq!(publication.stream_id(), STREAM_ID);
        assert_eq!(publication.session_id(), SESSION_ID);
    }

    #[test]
    fn should_release_publication_after_going_out_of_scope() {
        let test = ClientConductorTest::new();

        // Writes new publication in to to_driver buffer
        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        // Read and process all messages seen in to_driver buffer
        let _count = test.many_to_one_ring_buffer.read(|_msg_type_id, _buffer| {}, 1000);

        // In production this will be called when new publication comes from driver. Adds this publication in to internal map.
        test.conductor.lock().unwrap().on_new_publication(
            id,
            id,
            STREAM_ID,
            SESSION_ID,
            PUBLICATION_LIMIT_COUNTER_ID,
            CHANNEL_STATUS_INDICATOR_ID,
            str_to_c(&test.log_file_name),
        );

        {
            let publication = test.conductor.lock().unwrap().find_publication(id);

            assert!(publication.is_ok());
        }

        let count = test.many_to_one_ring_buffer.read(
            |msg_type_id, buffer| {
                let message = RemoveMessageFlyweight::new(buffer, 0);

                assert_eq!(msg_type_id, AeronCommand::RemovePublication);
                assert_eq!(message.registration_id(), id);
            },
            1000,
        );

        assert_eq!(count, 1);

        let publication_post = test.conductor.lock().unwrap().find_publication(id);
        assert!(publication_post.is_err());
    }

    #[test]
    fn should_return_same_publication_after_log_buffers_created() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        test.conductor.lock().unwrap().on_new_publication(
            id,
            id,
            STREAM_ID,
            SESSION_ID,
            PUBLICATION_LIMIT_COUNTER_ID,
            CHANNEL_STATUS_INDICATOR_ID,
            str_to_c(&test.log_file_name),
        );

        let publication1 = test.conductor.lock().unwrap().find_publication(id);
        let publication2 = test.conductor.lock().unwrap().find_publication(id);

        assert!(publication1.is_ok());
        assert!(publication2.is_ok());
        assert!(Arc::ptr_eq(&publication1.unwrap(), &publication2.unwrap()));
    }

    #[test]
    fn should_ignore_publication_ready_for_unknown_correlation_id() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        test.conductor.lock().unwrap().on_new_publication(
            id + 1,
            id + 1,
            STREAM_ID,
            SESSION_ID,
            PUBLICATION_LIMIT_COUNTER_ID,
            CHANNEL_STATUS_INDICATOR_ID,
            str_to_c(&test.log_file_name),
        );

        let publication = test.conductor.lock().unwrap().find_publication(id);

        assert!(publication.is_err());
    }

    fn driver_timeout_provider() -> Moment {
        unix_time_ms() + DRIVER_TIMEOUT_MS + 1
    }

    #[test]
    fn should_timeout_add_publication_without_publication_ready() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        test.conductor
            .lock()
            .unwrap()
            .set_epoch_clock_provider(Box::new(driver_timeout_provider));

        let publication = test.conductor.lock().unwrap().find_publication(id);

        assert_that!(
            &publication.err().unwrap(),
            has_structure!(AeronError::DriverTimeout[any_value()])
        );
    }

    #[test]
    fn should_exception_on_find_when_receiving_error_response_on_add_publication() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        test.conductor.lock().unwrap().on_error_response(
            id,
            ERROR_CODE_INVALID_CHANNEL,
            CString::new("invalid channel").unwrap(),
        );

        let publication = test.conductor.lock().unwrap().find_publication(id);
        assert_that!(
            &publication.err().unwrap(),
            has_structure!(AeronError::RegistrationException[any_value(), any_value()])
        );
    }

    #[test]
    fn should_return_null_for_unknown_exclusive_publication() {
        let test = ClientConductorTest::new();

        let publication = test.conductor.lock().unwrap().find_exclusive_publication(100);

        assert!(publication.is_err());
    }

    #[test]
    fn should_return_null_for_exclusive_publication_without_log_buffers() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_exclusive_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        let publication = test.conductor.lock().unwrap().find_exclusive_publication(id);

        assert!(publication.is_err());
    }

    #[test]
    fn should_send_add_exclusive_publication_to_driver() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_exclusive_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        let count = test.many_to_one_ring_buffer.read(
            |msg_type_id, buffer| {
                let message = PublicationMessageFlyweight::new(buffer, 0);

                assert_eq!(msg_type_id, AeronCommand::AddExclusivePublication);
                assert_eq!(message.correlation_id(), id);
                assert_eq!(message.stream_id(), STREAM_ID);
                assert_eq!(message.channel(), str_to_c(CHANNEL));
            },
            1000,
        );

        assert_eq!(count, 1);
    }

    #[test]
    fn should_return_exclusive_publication_after_log_buffers_created() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_exclusive_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        test.conductor.lock().unwrap().on_new_exclusive_publication(
            id,
            id,
            STREAM_ID,
            SESSION_ID,
            PUBLICATION_LIMIT_COUNTER_ID,
            CHANNEL_STATUS_INDICATOR_ID,
            str_to_c(&test.log_file_name),
        );

        let publication = test.conductor.lock().unwrap().find_exclusive_publication(id).unwrap();
        let publication = publication.lock().unwrap();

        assert_eq!(publication.registration_id(), id);
        assert_eq!(publication.channel(), str_to_c(CHANNEL));
        assert_eq!(publication.stream_id(), STREAM_ID);
        assert_eq!(publication.session_id(), SESSION_ID);
    }

    #[test]
    fn should_release_exclusive_publication_after_going_out_of_scope() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_exclusive_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        let _count = test.many_to_one_ring_buffer.read(|_msg_type_id, _buffer| {}, 1000);

        test.conductor.lock().unwrap().on_new_exclusive_publication(
            id,
            id,
            STREAM_ID,
            SESSION_ID,
            PUBLICATION_LIMIT_COUNTER_ID,
            CHANNEL_STATUS_INDICATOR_ID,
            str_to_c(&test.log_file_name),
        );

        {
            let publication = test.conductor.lock().unwrap().find_exclusive_publication(id);

            assert!(publication.is_ok());
        }

        let count = test.many_to_one_ring_buffer.read(
            |msg_type_id, buffer| {
                let message = RemoveMessageFlyweight::new(buffer, 0);

                assert_eq!(msg_type_id, AeronCommand::RemovePublication);
                assert_eq!(message.registration_id(), id);
            },
            1000,
        );

        assert_eq!(count, 1);

        let publication_post = test.conductor.lock().unwrap().find_exclusive_publication(id);
        assert!(publication_post.is_err());
    }

    #[test]
    fn should_return_different_id_for_duplicate_add_exclusive_publication() {
        let test = ClientConductorTest::new();

        let id1 = test
            .conductor
            .lock()
            .unwrap()
            .add_exclusive_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");
        let id2 = test
            .conductor
            .lock()
            .unwrap()
            .add_exclusive_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");
        assert_ne!(id1, id2);
    }

    #[test]
    fn should_return_same_exclusive_publication_after_log_buffers_created() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_exclusive_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        test.conductor.lock().unwrap().on_new_exclusive_publication(
            id,
            id,
            STREAM_ID,
            SESSION_ID,
            PUBLICATION_LIMIT_COUNTER_ID,
            CHANNEL_STATUS_INDICATOR_ID,
            str_to_c(&test.log_file_name),
        );

        let publication1 = test.conductor.lock().unwrap().find_exclusive_publication(id);
        let publication2 = test.conductor.lock().unwrap().find_exclusive_publication(id);

        assert!(publication1.is_ok());
        assert!(publication2.is_ok());
        assert!(Arc::ptr_eq(&publication1.unwrap(), &publication2.unwrap()));
    }

    #[test]
    fn should_ignore_exclusive_publication_ready_for_unknown_correlation_id() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_exclusive_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        test.conductor.lock().unwrap().on_new_exclusive_publication(
            id + 1,
            id + 1,
            STREAM_ID,
            SESSION_ID,
            PUBLICATION_LIMIT_COUNTER_ID,
            CHANNEL_STATUS_INDICATOR_ID,
            str_to_c(&test.log_file_name),
        );

        let publication = test.conductor.lock().unwrap().find_exclusive_publication(id);

        assert!(publication.is_err());
    }

    #[test]
    fn should_timeout_add_exclusive_publication_without_publication_ready() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_exclusive_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        test.conductor
            .lock()
            .unwrap()
            .set_epoch_clock_provider(Box::new(driver_timeout_provider));

        let publication = test.conductor.lock().unwrap().find_exclusive_publication(id);

        assert_that!(
            &publication.err().unwrap(),
            has_structure!(AeronError::DriverTimeout[any_value()])
        );
    }

    #[test]
    fn should_exception_on_find_when_receiving_error_response_on_add_exclusive_publication() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_exclusive_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        test.conductor.lock().unwrap().on_error_response(
            id,
            ERROR_CODE_INVALID_CHANNEL,
            CString::new("invalid channel").unwrap(),
        );

        let publication = test.conductor.lock().unwrap().find_exclusive_publication(id);
        assert_that!(
            &publication.err().unwrap(),
            has_structure!(AeronError::RegistrationException[any_value(), any_value()])
        );
    }

    #[test]
    fn should_return_null_for_unknown_subscription() {
        let test = ClientConductorTest::new();

        let subscription = test.conductor.lock().unwrap().find_subscription(100);

        assert!(subscription.is_err());
    }

    fn on_available_image_handler(_image: &Image) {}

    fn on_unavailable_image_handler(_image: &Image) {}

    #[test]
    fn should_return_null_for_subscription_without_operation_success() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();

        // Return Err as subscription was not processed by Driver and in Awaiting state.
        let subscription = test.conductor.lock().unwrap().find_subscription(id);

        assert!(subscription.is_err());
    }

    #[test]
    fn should_send_subscription_to_driver() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();

        // See added subscription in to_driver buffer
        let count = test.many_to_one_ring_buffer.read(
            |msg_type_id, buffer| {
                let message = SubscriptionMessageFlyweight::new(buffer, 0);

                assert_eq!(msg_type_id, AeronCommand::AddSubscription);
                assert_eq!(message.correlation_id(), id);
                assert_eq!(message.stream_id(), STREAM_ID);
                assert_eq!(message.channel(), str_to_c(CHANNEL));
            },
            1000,
        );

        assert_eq!(count, 1);
    }

    #[test]
    fn should_return_subscription_after_operation_success() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();

        test.conductor
            .lock()
            .unwrap()
            .on_subscription_ready(id, CHANNEL_STATUS_INDICATOR_ID);

        let result = test.conductor.lock().unwrap().find_subscription(id).expect("Not found");

        let subscription = result.lock().expect("Mutex poisoned");

        assert_eq!(subscription.registration_id(), id);
        assert_eq!(subscription.channel(), str_to_c(CHANNEL));
        assert_eq!(subscription.stream_id(), STREAM_ID);
    }

    #[test]
    fn should_release_subscription_after_going_out_of_scope() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();

        let _count = test.many_to_one_ring_buffer.read(|_msg_type_id, _buffer| {}, 1000);

        test.conductor
            .lock()
            .unwrap()
            .on_subscription_ready(id, CHANNEL_STATUS_INDICATOR_ID);

        {
            let subscription = test.conductor.lock().unwrap().find_subscription(id);
            assert!(subscription.is_ok());
        }

        let count = test.many_to_one_ring_buffer.read(
            |msg_type_id, buffer| {
                let message = RemoveMessageFlyweight::new(buffer, 0);

                assert_eq!(msg_type_id, AeronCommand::RemoveSubscription);
                assert_eq!(message.registration_id(), id);
            },
            1000,
        );

        assert_eq!(count, 1);

        let subscription_post = test.conductor.lock().unwrap().find_subscription(id);
        assert!(subscription_post.is_err());
    }

    #[test]
    fn should_return_different_ids_for_duplicate_add_subscription() {
        let test = ClientConductorTest::new();

        let id1 = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();
        let id2 = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();

        assert_ne!(id1, id2);
    }

    #[test]
    fn should_return_same_find_subscription_after_operation_success() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();

        test.conductor
            .lock()
            .unwrap()
            .on_subscription_ready(id, CHANNEL_STATUS_INDICATOR_ID);

        let subscription1 = test.conductor.lock().unwrap().find_subscription(id);
        let subscription2 = test.conductor.lock().unwrap().find_subscription(id);

        assert!(subscription1.is_ok());
        assert!(subscription2.is_ok());
        assert!(Arc::ptr_eq(&subscription1.unwrap(), &subscription2.unwrap()));
    }

    #[test]
    fn should_return_different_subscription_after_operation_success() {
        let test = ClientConductorTest::new();

        let id1 = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();
        let id2 = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();

        test.conductor
            .lock()
            .unwrap()
            .on_subscription_ready(id1, CHANNEL_STATUS_INDICATOR_ID);
        test.conductor
            .lock()
            .unwrap()
            .on_subscription_ready(id2, CHANNEL_STATUS_INDICATOR_ID);

        let subscription1 = test.conductor.lock().unwrap().find_subscription(id1);
        let subscription2 = test.conductor.lock().unwrap().find_subscription(id2);

        assert!(subscription1.is_ok());
        assert!(subscription2.is_ok());
        assert!(!Arc::ptr_eq(&subscription1.unwrap(), &subscription2.unwrap()));
    }

    #[test]
    fn should_ignore_operation_success_for_unknown_correlation_id() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();

        test.conductor
            .lock()
            .unwrap()
            .on_subscription_ready(id + 1, CHANNEL_STATUS_INDICATOR_ID);

        let subscription = test.conductor.lock().unwrap().find_subscription(id);

        assert!(subscription.is_err());
    }

    #[test]
    fn should_timeout_add_subscription_without_operation_success() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();

        test.conductor
            .lock()
            .unwrap()
            .set_epoch_clock_provider(Box::new(driver_timeout_provider));

        let subscription = test.conductor.lock().unwrap().find_subscription(id);

        assert_that!(
            &subscription.err().unwrap(),
            has_structure!(AeronError::DriverTimeout[any_value()])
        );
    }

    #[test]
    fn should_exception_on_find_when_receiving_error_response_on_add_subscription() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();

        test.conductor
            .lock()
            .unwrap()
            .on_error_response(id, ERROR_CODE_INVALID_CHANNEL, str_to_c("invalid channel"));

        let subscription = test.conductor.lock().unwrap().find_subscription(id);

        assert_that!(
            &subscription.err().unwrap(),
            has_structure!(AeronError::RegistrationException[any_value(), any_value()])
        );
    }

    fn error_handler1(_error: AeronError) {
        ERR_HANDLER_CALLED.store(true, Ordering::SeqCst);
        //assert_eq!(error, AeronError::ConductorServiceTimeout(String::from("Doesn't matter")));

        // Here we actually have two calls of error handler: one with ConductorServiceTimeout and
        // another with DriverTimeout. This is how original code should work I believe.
    }

    lazy_static! {
        pub static ref ERR_HANDLER_CALLED: AtomicBool = AtomicBool::from(false);
        pub static ref ERR_HANDLER_CALLED2: AtomicBool = AtomicBool::from(false);
    }

    #[test]
    fn should_call_error_handler_when_inter_service_timeout_exceeded() {
        let test = ClientConductorTest::new();

        test.conductor.lock().unwrap().set_error_handler(Box::new(error_handler1));
        test.conductor
            .lock()
            .unwrap()
            .set_epoch_clock_provider(Box::new(driver_timeout_provider));

        let _res = test.conductor.lock().unwrap().do_work();
        assert!(ERR_HANDLER_CALLED.load(Ordering::SeqCst));
    }

    #[allow(dead_code)]
    fn error_handler2(error: AeronError) {
        ERR_HANDLER_CALLED2.store(true, Ordering::SeqCst);
        assert_that!(&error, has_structure!(AeronError::DriverTimeout[any_value()]));
        println!("It was called");
    }

    fn error_handler3(error: AeronError) {
        ERR_HANDLER_CALLED3.store(true, Ordering::SeqCst);
        assert_that!(&error, has_structure!(AeronError::DriverTimeout[any_value()]));
    }

    lazy_static! {
        pub static ref ERR_HANDLER_CALLED3: AtomicBool = AtomicBool::from(false);
    }

    #[test]
    fn should_call_error_handler_when_driver_inactive_on_idle() {
        let mut test = ClientConductorTest::new();

        test.conductor.lock().unwrap().set_error_handler(Box::new(error_handler3));

        test.do_work_until_driver_timeout();

        let called: bool = ERR_HANDLER_CALLED3.load(Ordering::SeqCst);
        assert!(called);
    }

    fn error_handler4(error: AeronError) {
        ERR_HANDLER_CALLED4.store(true, Ordering::SeqCst);
        assert_that!(&error, has_structure!(AeronError::DriverTimeout[any_value()]));
    }

    lazy_static! {
        pub static ref ERR_HANDLER_CALLED4: AtomicBool = AtomicBool::from(false);
    }

    #[test]
    fn should_exception_when_add_publication_after_driver_inactive() {
        let mut test = ClientConductorTest::new();

        test.conductor.lock().unwrap().set_error_handler(Box::new(error_handler4));

        test.do_work_until_driver_timeout();

        let called: bool = ERR_HANDLER_CALLED4.load(Ordering::SeqCst);
        assert!(called);

        let result = test.conductor.lock().unwrap().add_publication(str_to_c(CHANNEL), STREAM_ID);

        assert_that!(&result.err().unwrap(), has_structure!(AeronError::DriverTimeout[any_value()]));
    }

    fn error_handler5(error: AeronError) {
        ERR_HANDLER_CALLED5.store(true, Ordering::SeqCst);
        assert_that!(&error, has_structure!(AeronError::DriverTimeout[any_value()]));
    }

    lazy_static! {
        pub static ref ERR_HANDLER_CALLED5: AtomicBool = AtomicBool::from(false);
    }

    #[test]
    fn should_exception_when_release_publication_after_driver_inactive() {
        let mut test = ClientConductorTest::new();

        test.conductor.lock().unwrap().set_error_handler(Box::new(error_handler5));

        test.do_work_until_driver_timeout();
        let called: bool = ERR_HANDLER_CALLED5.load(Ordering::SeqCst);
        assert!(called);

        let result = test.conductor.lock().unwrap().release_publication(100);

        assert!(result.is_err());
    }

    fn error_handler6(error: AeronError) {
        ERR_HANDLER_CALLED6.store(true, Ordering::SeqCst);
        assert_that!(&error, has_structure!(AeronError::DriverTimeout[any_value()]));
    }

    lazy_static! {
        pub static ref ERR_HANDLER_CALLED6: AtomicBool = AtomicBool::from(false);
    }

    #[test]
    fn should_exception_when_add_subscription_after_driver_inactive() {
        let mut test = ClientConductorTest::new();

        test.conductor.lock().unwrap().set_error_handler(Box::new(error_handler6));

        test.do_work_until_driver_timeout();
        let called: bool = ERR_HANDLER_CALLED6.load(Ordering::SeqCst);
        assert!(called);

        let result = test.conductor.lock().unwrap().add_subscription(
            str_to_c(CHANNEL),
            STREAM_ID,
            Box::new(on_available_image_handler),
            Box::new(on_unavailable_image_handler),
        );
        assert_that!(&result.err().unwrap(), has_structure!(AeronError::DriverTimeout[any_value()]));
    }

    lazy_static! {
        pub static ref ERR_HANDLER_CALLED7: AtomicBool = AtomicBool::from(false);
    }

    #[test]
    fn should_exception_when_release_subscription_after_driver_inactive() {
        let mut test = ClientConductorTest::new();

        test.conductor.lock().unwrap().set_error_handler(Box::new(|error| {
            ERR_HANDLER_CALLED7.store(true, Ordering::SeqCst);
            assert_that!(&error, has_structure!(AeronError::DriverTimeout[any_value()]));
        }));

        test.do_work_until_driver_timeout();
        let called: bool = ERR_HANDLER_CALLED7.load(Ordering::SeqCst);
        assert!(called);

        let result = test.conductor.lock().unwrap().release_subscription(100, Vec::<Image>::new());
        assert_that!(&result.err().unwrap(), has_structure!(AeronError::Generic[any_value()]));
    }

    fn on_new_publication_handler1(channel: CString, stream_id: i32, session_id: i32, correlation_id: i64) {
        ON_NEW_PUB_CALLED.store(true, Ordering::SeqCst);
        assert_eq!(channel, str_to_c(CHANNEL));
        assert_eq!(stream_id, STREAM_ID);
        assert_eq!(session_id, SESSION_ID);
        assert_eq!(correlation_id, 0);
    }

    lazy_static! {
        pub static ref ON_NEW_PUB_CALLED: AtomicBool = AtomicBool::from(false);
    }

    #[test]
    fn should_call_on_new_pub_after_log_buffers_created() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        test.conductor
            .lock()
            .unwrap()
            .set_on_new_publication_handler(Box::new(on_new_publication_handler1));

        test.conductor.lock().unwrap().on_new_publication(
            id,
            id,
            STREAM_ID,
            SESSION_ID,
            PUBLICATION_LIMIT_COUNTER_ID,
            CHANNEL_STATUS_INDICATOR_ID,
            str_to_c(&test.log_file_name),
        );

        let called: bool = ON_NEW_PUB_CALLED.load(Ordering::SeqCst);
        assert!(called);
    }

    fn on_new_subscription_handler1(channel: CString, stream_id: i32, correlation_id: i64) {
        ON_NEW_SUB_CALLED.store(true, Ordering::SeqCst);
        assert_eq!(channel, str_to_c(CHANNEL));
        assert_eq!(stream_id, STREAM_ID);
        assert_eq!(correlation_id, 0);
    }

    lazy_static! {
        pub static ref ON_NEW_SUB_CALLED: AtomicBool = AtomicBool::from(false);
    }

    #[test]
    fn should_call_on_new_sub_after_operation_success() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();

        test.conductor
            .lock()
            .unwrap()
            .set_on_new_subscription_handler(Box::new(on_new_subscription_handler1));

        test.conductor
            .lock()
            .unwrap()
            .on_subscription_ready(id, CHANNEL_STATUS_INDICATOR_ID);
        let subscription = test.conductor.lock().unwrap().find_subscription(id);
        assert!(subscription.is_ok());
        let called: bool = ON_NEW_SUB_CALLED.load(Ordering::SeqCst);
        assert!(called);
    }

    fn on_new_subscription_handler2(channel: CString, stream_id: i32, correlation_id: i64) {
        ON_NEW_SUB_CALLED2.store(true, Ordering::SeqCst);
        assert_eq!(channel, str_to_c(CHANNEL));
        assert_eq!(stream_id, STREAM_ID);
        assert_eq!(correlation_id, 0);
    }

    lazy_static! {
        pub static ref ON_NEW_SUB_CALLED2: AtomicBool = AtomicBool::from(false);
        pub static ref ON_NEW_IMG_CALLED2: AtomicBool = AtomicBool::from(false);
    }

    fn on_available_image_handler2(_img: &Image) {
        ON_NEW_IMG_CALLED2.store(true, Ordering::SeqCst);
    }

    #[test]
    fn should_call_new_connection_after_on_new_connection() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler2),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();
        let correlation_id = id + 1;

        test.conductor
            .lock()
            .unwrap()
            .set_on_new_subscription_handler(Box::new(on_new_subscription_handler2));

        test.conductor
            .lock()
            .unwrap()
            .on_subscription_ready(id, CHANNEL_STATUS_INDICATOR_ID);
        // must be able to handle newImage even if find_subscription not called
        test.conductor.lock().unwrap().on_available_image(
            correlation_id,
            SESSION_ID,
            1,
            id,
            str_to_c(&test.log_file_name),
            str_to_c(SOURCE_IDENTITY),
        );

        let subscription = test.conductor.lock().unwrap().find_subscription(id);
        assert!(subscription.is_ok());
        assert!(subscription.unwrap().lock().unwrap().has_image(correlation_id));

        let sub_called: bool = ON_NEW_SUB_CALLED2.load(Ordering::SeqCst);
        assert!(sub_called);
        let img_called: bool = ON_NEW_IMG_CALLED2.load(Ordering::SeqCst);
        assert!(img_called);
    }

    fn on_new_subscription_handler3(channel: CString, stream_id: i32, correlation_id: i64) {
        ON_NEW_SUB_CALLED3.store(true, Ordering::SeqCst);
        assert_eq!(channel, str_to_c(CHANNEL));
        assert_eq!(stream_id, STREAM_ID);
        assert_eq!(correlation_id, 0);
    }

    lazy_static! {
        pub static ref ON_NEW_SUB_CALLED3: AtomicBool = AtomicBool::from(false);
        pub static ref ON_NEW_IMG_CALLED3: AtomicBool = AtomicBool::from(false);
    }

    fn on_available_image_handler3(_img: &Image) {
        ON_NEW_IMG_CALLED3.store(true, Ordering::SeqCst);
    }

    #[test]
    fn should_not_call_new_connection_if_no_operation_success() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler3),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();
        let correlation_id = id + 1;
        test.conductor
            .lock()
            .unwrap()
            .set_on_new_subscription_handler(Box::new(on_new_subscription_handler3));

        // must be able to handle newImage even if find_subscription not called
        test.conductor.lock().unwrap().on_available_image(
            correlation_id,
            SESSION_ID,
            1,
            id,
            str_to_c(&test.log_file_name),
            str_to_c(SOURCE_IDENTITY),
        );

        let subscription = test.conductor.lock().unwrap().find_subscription(id);
        assert!(subscription.is_err());

        let sub_called: bool = ON_NEW_SUB_CALLED3.load(Ordering::SeqCst);
        assert!(!sub_called);
        let img_called: bool = ON_NEW_IMG_CALLED3.load(Ordering::SeqCst);
        assert!(!img_called);
    }

    fn on_new_subscription_handler4(channel: CString, stream_id: i32, correlation_id: i64) {
        ON_NEW_SUB_CALLED4.store(true, Ordering::SeqCst);
        assert_eq!(channel, str_to_c(CHANNEL));
        assert_eq!(stream_id, STREAM_ID);
        assert_eq!(correlation_id, 0);
    }

    lazy_static! {
        pub static ref ON_NEW_SUB_CALLED4: AtomicBool = AtomicBool::from(false);
        pub static ref ON_NEW_IMG_CALLED4: AtomicBool = AtomicBool::from(false);
    }

    fn on_available_image_handler4(_img: &Image) {
        ON_NEW_IMG_CALLED4.store(true, Ordering::SeqCst);
    }

    #[test]
    fn should_not_call_new_connection_if_uninteresting_registration_id() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler4),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();
        let correlation_id = id + 1;
        test.conductor
            .lock()
            .unwrap()
            .set_on_new_subscription_handler(Box::new(on_new_subscription_handler4));

        test.conductor
            .lock()
            .unwrap()
            .on_subscription_ready(id, CHANNEL_STATUS_INDICATOR_ID);
        // must be able to handle newImage even if find_subscription not called
        test.conductor.lock().unwrap().on_available_image(
            correlation_id,
            SESSION_ID,
            1,
            id + 1,
            str_to_c(&test.log_file_name),
            str_to_c(SOURCE_IDENTITY),
        );

        let subscription = test.conductor.lock().unwrap().find_subscription(id);
        assert!(subscription.is_ok());
        assert!(!subscription.unwrap().lock().unwrap().has_image(correlation_id));

        let sub_called: bool = ON_NEW_SUB_CALLED4.load(Ordering::SeqCst);
        assert!(sub_called);
        let img_called: bool = ON_NEW_IMG_CALLED4.load(Ordering::SeqCst);
        assert!(!img_called);
    }

    fn on_new_subscription_handler5(channel: CString, stream_id: i32, correlation_id: i64) {
        ON_NEW_SUB_CALLED5.store(true, Ordering::SeqCst);
        assert_eq!(channel, str_to_c(CHANNEL));
        assert_eq!(stream_id, STREAM_ID);
        assert_eq!(correlation_id, 0);
    }

    lazy_static! {
        pub static ref ON_NEW_SUB_CALLED5: AtomicBool = AtomicBool::from(false);
        pub static ref ON_NEW_IMG_CALLED5: AtomicBool = AtomicBool::from(false);
        pub static ref ON_INACTIVE_CALLED5: AtomicBool = AtomicBool::from(false);
    }

    fn on_available_image_handler5(_img: &Image) {
        ON_NEW_IMG_CALLED5.store(true, Ordering::SeqCst);
    }

    fn on_unavailable_image_handler5(_img: &Image) {
        ON_INACTIVE_CALLED5.store(true, Ordering::SeqCst);
    }

    #[test]
    fn should_call_inactive_connection_after_inactive_connection() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler5),
                Box::new(on_unavailable_image_handler5),
            )
            .unwrap();
        let correlation_id = id + 1;
        test.conductor
            .lock()
            .unwrap()
            .set_on_new_subscription_handler(Box::new(on_new_subscription_handler5));

        test.conductor
            .lock()
            .unwrap()
            .on_subscription_ready(id, CHANNEL_STATUS_INDICATOR_ID);
        let subscription = test.conductor.lock().unwrap().find_subscription(id);
        test.conductor.lock().unwrap().on_available_image(
            correlation_id,
            SESSION_ID,
            1,
            id,
            str_to_c(&test.log_file_name),
            str_to_c(SOURCE_IDENTITY),
        );
        test.conductor.lock().unwrap().on_unavailable_image(correlation_id, id);
        assert!(!subscription.unwrap().lock().unwrap().has_image(correlation_id));

        let sub_called: bool = ON_NEW_SUB_CALLED5.load(Ordering::SeqCst);
        assert!(sub_called);
        let img_called: bool = ON_NEW_IMG_CALLED5.load(Ordering::SeqCst);
        assert!(img_called);
        let un_img_called: bool = ON_INACTIVE_CALLED5.load(Ordering::SeqCst);
        assert!(un_img_called);
    }

    fn on_new_subscription_handler6(channel: CString, stream_id: i32, correlation_id: i64) {
        ON_NEW_SUB_CALLED6.store(true, Ordering::SeqCst);
        assert_eq!(channel, str_to_c(CHANNEL));
        assert_eq!(stream_id, STREAM_ID);
        assert_eq!(correlation_id, 0);
    }

    lazy_static! {
        pub static ref ON_NEW_SUB_CALLED6: AtomicBool = AtomicBool::from(false);
        pub static ref ON_NEW_IMG_CALLED6: AtomicBool = AtomicBool::from(false);
        pub static ref ON_INACTIVE_CALLED6: AtomicBool = AtomicBool::from(false);
    }

    fn on_available_image_handler6(_img: &Image) {
        ON_NEW_IMG_CALLED6.store(true, Ordering::SeqCst);
    }

    fn on_unavailable_image_handler6(_img: &Image) {
        ON_INACTIVE_CALLED6.store(true, Ordering::SeqCst);
    }

    #[test]
    fn should_not_call_inactive_connection_if_no_operation_success() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler6),
                Box::new(on_unavailable_image_handler6),
            )
            .unwrap();
        let correlation_id = id + 1;
        test.conductor
            .lock()
            .unwrap()
            .set_on_new_subscription_handler(Box::new(on_new_subscription_handler6));

        // must be able to handle newImage even if find_subscription not called
        test.conductor.lock().unwrap().on_available_image(
            correlation_id,
            SESSION_ID,
            1,
            id,
            str_to_c(&test.log_file_name),
            str_to_c(SOURCE_IDENTITY),
        );
        test.conductor.lock().unwrap().on_unavailable_image(correlation_id, id);

        let sub_called: bool = ON_NEW_SUB_CALLED6.load(Ordering::SeqCst);
        assert!(!sub_called);
        let img_called: bool = ON_NEW_IMG_CALLED6.load(Ordering::SeqCst);
        assert!(!img_called);
        let un_img_called: bool = ON_INACTIVE_CALLED6.load(Ordering::SeqCst);
        assert!(!un_img_called);
    }

    fn on_new_subscription_handler7(channel: CString, stream_id: i32, correlation_id: i64) {
        ON_NEW_SUB_CALLED7.store(true, Ordering::SeqCst);
        assert_eq!(channel, str_to_c(CHANNEL));
        assert_eq!(stream_id, STREAM_ID);
        assert_eq!(correlation_id, 0);
    }

    lazy_static! {
        pub static ref ON_NEW_SUB_CALLED7: AtomicBool = AtomicBool::from(false);
        pub static ref ON_NEW_IMG_CALLED7: AtomicBool = AtomicBool::from(false);
        pub static ref ON_INACTIVE_CALLED7: AtomicBool = AtomicBool::from(false);
    }

    fn on_available_image_handler7(_img: &Image) {
        ON_NEW_IMG_CALLED7.store(true, Ordering::SeqCst);
    }

    fn on_unavailable_image_handler7(_img: &Image) {
        ON_INACTIVE_CALLED7.store(true, Ordering::SeqCst);
    }

    #[test]
    fn should_not_call_inactive_connection_if_uninteresting_connection_correlation_id() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler7),
                Box::new(on_unavailable_image_handler7),
            )
            .unwrap();
        let correlation_id = id + 1;
        test.conductor
            .lock()
            .unwrap()
            .set_on_new_subscription_handler(Box::new(on_new_subscription_handler7));

        test.conductor
            .lock()
            .unwrap()
            .on_subscription_ready(id, CHANNEL_STATUS_INDICATOR_ID);
        let subscription = test.conductor.lock().unwrap().find_subscription(id);
        test.conductor.lock().unwrap().on_available_image(
            correlation_id,
            SESSION_ID,
            1,
            id,
            str_to_c(&test.log_file_name),
            str_to_c(SOURCE_IDENTITY),
        );
        test.conductor.lock().unwrap().on_unavailable_image(correlation_id + 1, id);

        let sub_called: bool = ON_NEW_SUB_CALLED7.load(Ordering::SeqCst);
        assert!(sub_called);
        let img_called: bool = ON_NEW_IMG_CALLED7.load(Ordering::SeqCst);
        assert!(img_called);
        let un_img_called: bool = ON_INACTIVE_CALLED7.load(Ordering::SeqCst);
        assert!(!un_img_called);

        assert!(subscription.unwrap().lock().unwrap().has_image(correlation_id));
    }

    fn on_new_subscription_handler8(channel: CString, stream_id: i32, correlation_id: i64) {
        ON_NEW_SUB_CALLED8.store(true, Ordering::SeqCst);
        assert_eq!(channel, str_to_c(CHANNEL));
        assert_eq!(stream_id, STREAM_ID);
        assert_eq!(correlation_id, 0);
    }

    lazy_static! {
        pub static ref ON_NEW_SUB_CALLED8: AtomicBool = AtomicBool::from(false);
        pub static ref ON_NEW_IMG_CALLED8: AtomicBool = AtomicBool::from(false);
        pub static ref ON_INACTIVE_CALLED8: AtomicBool = AtomicBool::from(false);
    }

    fn on_available_image_handler8(_img: &Image) {
        ON_NEW_IMG_CALLED8.store(true, Ordering::SeqCst);
    }

    fn on_unavailable_image_handler8(_img: &Image) {
        ON_INACTIVE_CALLED8.store(true, Ordering::SeqCst);
    }

    #[test]
    fn should_call_unavailable_image_if_subscription_released() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler8),
                Box::new(on_unavailable_image_handler8),
            )
            .unwrap();
        let correlation_id = id + 1;
        test.conductor
            .lock()
            .unwrap()
            .set_on_new_subscription_handler(Box::new(on_new_subscription_handler8));

        test.conductor
            .lock()
            .unwrap()
            .on_subscription_ready(id, CHANNEL_STATUS_INDICATOR_ID);

        {
            let subscription = test.conductor.lock().unwrap().find_subscription(id);
            test.conductor.lock().unwrap().on_available_image(
                correlation_id,
                SESSION_ID,
                1,
                id,
                str_to_c(&test.log_file_name),
                str_to_c(SOURCE_IDENTITY),
            );
            assert!(subscription.unwrap().lock().unwrap().has_image(correlation_id));
        }

        let sub_called: bool = ON_NEW_SUB_CALLED8.load(Ordering::SeqCst);
        assert!(sub_called);
        let img_called: bool = ON_NEW_IMG_CALLED8.load(Ordering::SeqCst);
        assert!(img_called);
        let un_img_called: bool = ON_INACTIVE_CALLED8.load(Ordering::SeqCst);
        assert!(un_img_called);
    }

    #[test]
    fn should_close_publication_on_inter_service_timeout() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        test.conductor.lock().unwrap().on_new_publication(
            id,
            id,
            STREAM_ID,
            SESSION_ID,
            PUBLICATION_LIMIT_COUNTER_ID,
            CHANNEL_STATUS_INDICATOR_ID,
            str_to_c(&test.log_file_name),
        );

        let publication = test.conductor.lock().unwrap().find_publication(id);

        assert!(publication.is_ok());

        test.conductor
            .lock()
            .unwrap()
            .close_all_resources(*test.current_time.lock().unwrap());
        assert!(publication.unwrap().lock().unwrap().is_closed());
    }

    #[test]
    fn should_close_exclusive_publication_on_inter_service_timeout() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_exclusive_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");

        test.conductor.lock().unwrap().on_new_exclusive_publication(
            id,
            id,
            STREAM_ID,
            SESSION_ID,
            PUBLICATION_LIMIT_COUNTER_ID,
            CHANNEL_STATUS_INDICATOR_ID,
            str_to_c(&test.log_file_name),
        );

        let publication = test.conductor.lock().unwrap().find_exclusive_publication(id);

        assert!(publication.is_ok());

        test.conductor
            .lock()
            .unwrap()
            .close_all_resources(*test.current_time.lock().unwrap());
        assert!(publication.unwrap().lock().unwrap().is_closed());
    }

    #[test]
    fn should_close_subscription_on_inter_service_timeout() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();

        test.conductor
            .lock()
            .unwrap()
            .on_subscription_ready(id, CHANNEL_STATUS_INDICATOR_ID);

        let subscription = test.conductor.lock().unwrap().find_subscription(id);

        assert!(subscription.is_ok());

        test.conductor
            .lock()
            .unwrap()
            .close_all_resources(*test.current_time.lock().unwrap());

        assert!(subscription.unwrap().lock().unwrap().is_closed());
    }

    #[test]
    fn should_close_all_publications_and_subscriptions_on_inter_service_timeout() {
        let test = ClientConductorTest::new();

        let pub_id = test
            .conductor
            .lock()
            .unwrap()
            .add_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");
        let ex_pub_id = test
            .conductor
            .lock()
            .unwrap()
            .add_exclusive_publication(str_to_c(CHANNEL), STREAM_ID)
            .expect("failed to add publication");
        let sub_id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();

        test.conductor.lock().unwrap().on_new_publication(
            pub_id,
            pub_id,
            STREAM_ID,
            SESSION_ID,
            PUBLICATION_LIMIT_COUNTER_ID,
            CHANNEL_STATUS_INDICATOR_ID,
            str_to_c(&test.log_file_name),
        );
        test.conductor.lock().unwrap().on_new_exclusive_publication(
            ex_pub_id,
            ex_pub_id,
            STREAM_ID,
            SESSION_ID,
            PUBLICATION_LIMIT_COUNTER_ID_2,
            CHANNEL_STATUS_INDICATOR_ID,
            str_to_c(&test.log_file_name2),
        );
        test.conductor
            .lock()
            .unwrap()
            .on_subscription_ready(sub_id, CHANNEL_STATUS_INDICATOR_ID);

        let publication = test.conductor.lock().unwrap().find_publication(pub_id);

        assert!(publication.is_ok());

        let subscription = test.conductor.lock().unwrap().find_subscription(sub_id);

        assert!(subscription.is_ok());

        let ex_pub = test.conductor.lock().unwrap().find_exclusive_publication(ex_pub_id);

        assert!(ex_pub.is_ok());

        test.conductor
            .lock()
            .unwrap()
            .close_all_resources(*test.current_time.lock().unwrap());
        assert!(publication.unwrap().lock().unwrap().is_closed());
        assert!(subscription.unwrap().lock().unwrap().is_closed());
        assert!(ex_pub.unwrap().lock().unwrap().is_closed());
    }

    #[test]
    fn should_remove_image_on_inter_service_timeout() {
        let test = ClientConductorTest::new();

        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_subscription(
                str_to_c(CHANNEL),
                STREAM_ID,
                Box::new(on_available_image_handler),
                Box::new(on_unavailable_image_handler),
            )
            .unwrap();
        let correlation_id = id + 1;

        test.conductor
            .lock()
            .unwrap()
            .on_subscription_ready(id, CHANNEL_STATUS_INDICATOR_ID);

        let sub = test.conductor.lock().unwrap().find_subscription(id).unwrap();

        test.conductor.lock().unwrap().on_available_image(
            correlation_id,
            SESSION_ID,
            1,
            id,
            str_to_c(&test.log_file_name),
            str_to_c(SOURCE_IDENTITY),
        );
        {
            let subscription = sub.lock().unwrap();
            assert!(subscription.has_image(correlation_id));
            // Free mutex
        }
        test.conductor
            .lock()
            .unwrap()
            .close_all_resources(*test.current_time.lock().unwrap());

        let subscription = sub.lock().unwrap();
        let image = subscription.image_by_session_id(SESSION_ID);

        assert!(subscription.is_closed());
        assert!(image.is_none());
    }

    #[test]
    fn should_return_null_for_unknown_counter() {
        let test = ClientConductorTest::new();
        let mut conductor = test.conductor.lock().expect("Mutex on conductor is poisoned!");
        let counter = conductor.find_counter(100);

        assert!(counter.is_err());
    }

    #[test]
    fn should_return_null_for_counter_without_on_available_counter() {
        let test = ClientConductorTest::new();

        let no_key_buffer = Vec::with_capacity(1);
        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_counter(COUNTER_TYPE_ID, &no_key_buffer, COUNTER_LABEL)
            .unwrap();

        let counter = test.conductor.lock().unwrap().find_counter(id);

        assert!(counter.is_err());
    }

    #[test]
    fn should_send_add_counter_to_driver() {
        let test = ClientConductorTest::new();

        let no_key_buffer = Vec::with_capacity(1);
        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_counter(COUNTER_TYPE_ID, &no_key_buffer, COUNTER_LABEL)
            .unwrap();

        let count = test.many_to_one_ring_buffer.read(
            |msg_type_id, buffer| {
                let message = CounterMessageFlyweight::new(buffer, 0);

                assert_eq!(msg_type_id, AeronCommand::AddCounter);
                assert_eq!(message.correlation_id(), id);
                assert_eq!(message.type_id(), COUNTER_TYPE_ID);
                assert_eq!(message.key_length(), 0);
                assert_eq!(message.label(), str_to_c(COUNTER_LABEL));
            },
            1000,
        );

        assert_eq!(count, 1);
    }

    fn on_available_counter1(_counters_reader: &CountersReader, _registration_id: i64, _counter_id: i32) {
        ON_AV_COUNTER_CALLED1.store(true, Ordering::SeqCst);
    }

    lazy_static! {
        pub static ref ON_AV_COUNTER_CALLED1: AtomicBool = AtomicBool::from(false);
    }

    #[test]
    fn should_return_counter_after_on_available_counter() {
        let test = ClientConductorTest::new();

        test.conductor
            .lock()
            .unwrap()
            .add_on_available_counter_handler(Box::new(on_available_counter1));

        let no_key_buffer = Vec::with_capacity(1);
        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_counter(COUNTER_TYPE_ID, &no_key_buffer, COUNTER_LABEL)
            .unwrap();

        test.conductor.lock().unwrap().on_available_counter(id, COUNTER_ID);

        let counter = test.conductor.lock().unwrap().find_counter(id).unwrap();

        assert_eq!(counter.registration_id(), id);
        assert_eq!(counter.id(), COUNTER_ID);

        let called = ON_AV_COUNTER_CALLED1.load(Ordering::Relaxed);
        assert!(called);
    }

    #[test]
    fn should_release_counter_after_going_out_of_scope() {
        let test = ClientConductorTest::new();

        let no_key_buffer = Vec::with_capacity(1);
        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_counter(COUNTER_TYPE_ID, &no_key_buffer, COUNTER_LABEL)
            .unwrap();

        let _count = test.many_to_one_ring_buffer.read(|_msg_type_id, _buffer| {}, 1000);

        test.conductor.lock().unwrap().on_available_counter(id, COUNTER_ID);

        {
            let counter = test.conductor.lock().unwrap().find_counter(id);

            assert!(counter.is_ok());
        }

        let count = test.many_to_one_ring_buffer.read(
            |msg_type_id, buffer| {
                let message = RemoveMessageFlyweight::new(buffer, 0);

                assert_eq!(msg_type_id, AeronCommand::RemoveCounter);
                assert_eq!(message.registration_id(), id);
            },
            1000,
        );

        assert_eq!(count, 1);

        let counter_post = test.conductor.lock().unwrap().find_counter(id);
        assert!(counter_post.is_err());
    }

    #[test]
    fn should_return_different_ids_for_duplicate_add_counter_calls() {
        let test = ClientConductorTest::new();

        let no_key_buffer = Vec::with_capacity(1);
        let id1 = test
            .conductor
            .lock()
            .unwrap()
            .add_counter(COUNTER_TYPE_ID, &no_key_buffer, COUNTER_LABEL)
            .unwrap();
        let id2 = test
            .conductor
            .lock()
            .unwrap()
            .add_counter(COUNTER_TYPE_ID, &no_key_buffer, COUNTER_LABEL)
            .unwrap();

        assert_ne!(id1, id2);
    }

    #[test]
    fn should_return_same_find_counter_after_on_available_counter() {
        let test = ClientConductorTest::new();

        let no_key_buffer = Vec::with_capacity(1);
        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_counter(COUNTER_TYPE_ID, &no_key_buffer, COUNTER_LABEL)
            .unwrap();

        test.conductor.lock().unwrap().on_available_counter(id, COUNTER_ID);

        let counter1 = test.conductor.lock().unwrap().find_counter(id).unwrap();
        let counter2 = test.conductor.lock().unwrap().find_counter(id).unwrap();

        assert!(Arc::ptr_eq(&counter1, &counter2));
    }

    fn on_available_counter2(_counters_reader: &CountersReader, _registration_id: i64, _counter_id: i32) {
        let mut val = ON_AV_COUNTER_CALLED2.load(Ordering::SeqCst);
        val += 1;
        ON_AV_COUNTER_CALLED2.store(val, Ordering::SeqCst);
    }

    lazy_static! {
        pub static ref ON_AV_COUNTER_CALLED2: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::from(0);
    }

    #[test]
    fn should_return_different_counter_after_on_available_counters() {
        let test = ClientConductorTest::new();

        let no_key_buffer = Vec::with_capacity(1);
        let id1 = test
            .conductor
            .lock()
            .unwrap()
            .add_counter(COUNTER_TYPE_ID, &no_key_buffer, COUNTER_LABEL)
            .unwrap();
        let id2 = test
            .conductor
            .lock()
            .unwrap()
            .add_counter(COUNTER_TYPE_ID, &no_key_buffer, COUNTER_LABEL)
            .unwrap();

        test.conductor
            .lock()
            .unwrap()
            .add_on_available_counter_handler(Box::new(on_available_counter2));
        test.conductor.lock().unwrap().on_available_counter(id1, COUNTER_ID);
        test.conductor.lock().unwrap().on_available_counter(id2, COUNTER_ID);

        let counter1 = test.conductor.lock().unwrap().find_counter(id1).unwrap();
        let counter2 = test.conductor.lock().unwrap().find_counter(id2).unwrap();

        assert!(!Arc::ptr_eq(&counter1, &counter2));

        let val = ON_AV_COUNTER_CALLED2.load(Ordering::SeqCst);
        assert_eq!(val, 2);
    }

    #[test]
    fn should_not_find_counter_on_available_counter_for_unknown_correlation_id() {
        let test = ClientConductorTest::new();

        let no_key_buffer = Vec::with_capacity(1);
        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_counter(COUNTER_TYPE_ID, &no_key_buffer, COUNTER_LABEL)
            .unwrap();

        test.conductor.lock().unwrap().on_available_counter(id + 1, COUNTER_ID);

        let counter = test.conductor.lock().unwrap().find_counter(id);

        assert!(counter.is_err());
    }

    #[test]
    fn should_timeout_add_counter_without_on_available_counter() {
        let test = ClientConductorTest::new();

        let no_key_buffer = Vec::with_capacity(1);
        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_counter(COUNTER_TYPE_ID, &no_key_buffer, COUNTER_LABEL)
            .unwrap();

        test.conductor
            .lock()
            .unwrap()
            .set_epoch_clock_provider(Box::new(driver_timeout_provider));

        let counter = test.conductor.lock().unwrap().find_counter(id);

        assert_that!(
            &counter.err().unwrap(),
            has_structure!(AeronError::DriverTimeout[any_value()])
        );
    }

    #[test]
    fn should_exception_on_find_when_receiving_error_response_on_add_counter() {
        let test = ClientConductorTest::new();

        let no_key_buffer = Vec::with_capacity(1);
        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_counter(COUNTER_TYPE_ID, &no_key_buffer, COUNTER_LABEL)
            .unwrap();

        test.conductor
            .lock()
            .unwrap()
            .on_error_response(id, ERROR_CODE_GENERIC_ERROR, str_to_c("can't add counter"));

        let counter = test.conductor.lock().unwrap().find_counter(id);

        assert_that!(
            &counter.err().unwrap(),
            has_structure!(AeronError::RegistrationException[any_value(), any_value()])
        );
    }

    fn on_unavailable_counter1(_counters_reader: &CountersReader, _registration_id: i64, _counter_id: i32) {
        let mut val = ON_UNAV_COUNTER_CALLED1.load(Ordering::SeqCst);
        val += 1;
        ON_UNAV_COUNTER_CALLED1.store(val, Ordering::SeqCst);
    }

    lazy_static! {
        pub static ref ON_UNAV_COUNTER_CALLED1: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::from(0);
    }

    #[test]
    fn should_call_on_unavailable_counter() {
        let test = ClientConductorTest::new();

        let id = 101;
        test.conductor
            .lock()
            .unwrap()
            .add_on_unavailable_counter_handler(Box::new(on_unavailable_counter1));

        test.conductor.lock().unwrap().on_unavailable_counter(id, COUNTER_ID);

        let val = ON_UNAV_COUNTER_CALLED1.load(Ordering::SeqCst);
        assert_eq!(val, 1);
    }
    /*
    fn on_available_counter3(_counters_reader: &CountersReader, _registration_id: i64, _counter_id: i32) {
        let mut val = ON_AV_COUNTER_CALLED3.load(Ordering::SeqCst);
        val += 1;
        ON_AV_COUNTER_CALLED3.store(val, Ordering::SeqCst);

        if let Some(arc_conductor) = &*LAST_TEST_CONDUCTOR.lock().unwrap() {
            let no_key_buffer = Vec::with_capacity(1);
            let _unused = arc_conductor
                .lock()
                .unwrap()
                .add_counter(COUNTER_TYPE_ID, &no_key_buffer, COUNTER_LABEL);
        } else {
            panic!("Something wrong with the test");
        }
    }

    lazy_static! {
        pub static ref ON_AV_COUNTER_CALLED3: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::from(0);
        pub static ref LAST_TEST_CONDUCTOR: Mutex<Option<Arc<Mutex<ClientConductor>>>> = Mutex::new(None);
        pub static ref ERR_HANDLER_CALLED8: AtomicBool = AtomicBool::from(false);
    }

    fn error_handler8(error: AeronError) {
        ERR_HANDLER_CALLED8.store(true, Ordering::SeqCst);
        assert_eq!(error, AeronError::DriverTimeout(String::from("Doesn't matter")));
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn should_throw_exception_on_reentrant_callback() {
        // Can't be done in current architecture as Mutex double lock occures in on_available_counter
        // callback
        let test = ClientConductorTest::new();

        *LAST_TEST_CONDUCTOR.lock().unwrap() = Some(test.conductor.clone());

        let no_key_buffer = Vec::with_capacity(1);
        let id = test
            .conductor
            .lock()
            .unwrap()
            .add_counter(COUNTER_TYPE_ID, &no_key_buffer, COUNTER_LABEL)
            .unwrap();

        test.conductor
            .lock()
            .unwrap()
            .add_on_available_counter_handler(on_available_counter3);
        test.conductor.lock().unwrap().set_error_handler(error_handler8);

        test.conductor.lock().unwrap().on_available_counter(id, id as i32);

        let val = ON_AV_COUNTER_CALLED3.load(Ordering::SeqCst);
        assert_eq!(val, 1);

        let err_handler_called = ERR_HANDLER_CALLED8.load(Ordering::SeqCst);
        assert!(err_handler_called);
    }
    */
}
