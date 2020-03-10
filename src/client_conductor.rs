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
use std::sync::atomic::{AtomicBool, Ordering};
use crate::utils::types::{Moment, MAX_MOMENT, Index};
use std::sync::{Mutex, Arc, Weak};
use std::collections::HashMap;
use crate::driver_proxy::DriverProxy;
use crate::driver_listener_adapter::DriverListenerAdapter;
use crate::concurrent::counters::CountersReader;
use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::concurrent::broadcast::copy_broadcast_receiver::CopyBroadcastReceiver;
use crate::utils::log_buffers::LogBuffers;
use crate::context::{OnAvailableImage, OnNewPublication, OnNewSubscription, ErrorHandler, OnAvailableCounter, OnUnavailableCounter, OnCloseClient, OnUnavailableImage};
use crate::utils::errors::AeronError;
use crate::heartbeat_timestamp;
use crate::concurrent::counters;
use crate::concurrent::position::UnsafeBufferPosition;
use crate::image::Image;
use crate::utils::misc::CallbackGuard;
use std::ffi::CStr;
use crate::utils::errors::AeronError::{ChannelEndpointException, ClientTimeoutException};

type EpochClock = fn()->Moment;
type NanoClock = fn()->Moment;

const KEEPALIVE_TIMEOUT_MS: Moment = 500;
const RESOURCE_TIMEOUT_MS: Moment = 1000;

enum RegistrationStatus {
    AwaitingMediaDriver,
    RegisteredMediaDriver,
    ErroredMediaDriver,
}

struct PublicationStateDefn {
    error_message: String,
    buffers: Arc<LogBuffers>,
    publication: Weak<Publication>,
    channel: String,
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
    pub fn new(channel: String, registration_id: i64, stream_id: i32, now_ms: Moment) -> Self {
        Self {
            error_message: String::from(""),
            buffers: (),
            publication: (),
            channel,
            registration_id,
            time_of_registration_ms: now_ms,
            stream_id,
            original_registration_id: -1,
            session_id: -1,
            publication_limit_counter_id: -1,
            channel_status_id: -1,
            error_code: -1,
            status: RegistrationStatus::AwaitingMediaDriver,
        }
    }
}

struct ExclusivePublicationStateDefn {
    error_message: String,
    buffers: Arc<LogBuffers>,
    publication: Weak<ExclusivePublication>,
    channel: String,
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

impl ExclusivePublicationStateDefn {
    pub fn new(channel: String, registration_id: i64, stream_id: i32, now_ms: Moment) -> Self {
        Self {
            error_message: String::from(""),
            buffers: (),
            publication: (),
            channel,
            registration_id,
            time_of_registration_ms: now_ms,
            stream_id,
            original_registration_id: -1,
            session_id: -1,
            publication_limit_counter_id: -1,
            channel_status_id: -1,
            error_code: -1,
            status: RegistrationStatus::AwaitingMediaDriver,
        }
    }
}

struct SubscriptionStateDefn {
    error_message: String,
    subscription_cache: Option<Arc<Subscription>>,
    subscription: Weak<Subscription>,
    on_available_image_handler: OnAvailableImageHandler,
    on_unavailable_image_handler: OnUnavailableImageHandler,
    channel: String,
    registration_id: i64,
    time_of_registration_ms: Moment,
    stream_id: i32,
    error_code: i32,
    status: RegistrationStatus,
}

impl SubscriptionStateDefn {
    pub fn new(
    channel: String, registration_id: i64, stream_id: i32, now_ms: Moment,
        on_available_image_handler: OnAvailableImageHandler,
        on_unavailable_image_handler: OnUnavailableImageHandler,
    ) -> Self {
        Self {
            error_message: String::from(""),
            subscription_cache: None,
            subscription: (),
            on_available_image_handler,
            on_unavailable_image_handler,
            channel,
            registration_id,
            time_of_registration_ms: now_ms,
            stream_id,
            error_code: -1,
            status: RegistrationStatus::AwaitingMediaDriver,
        }
    }
}


struct CounterStateDefn {
    error_message: String,
    counter_cache: Option<Arc<Counter>>,
    counter: Weak<Counter>,
    registration_id: i64,
    time_of_registration_ms: Moment,
    counter_id: i32,
    status: RegistrationStatus,
    error_code: i32,
}

impl CounterStateDefn {
    pub fn new(registration_id: i64, now_ms: Moment) -> Self {
        Self {
            error_message: String::from(""),
            counter_cache: None,
            counter: (),
            registration_id,
            time_of_registration_ms: now_ms,
            error_code: -1,
            status: RegistrationStatus::AwaitingMediaDriver,
            counter_id: 0
        }
    }
}


struct ImageListLingerDefn {
    image_array: ImageArray,
    time_of_last_state_change_ms: Moment,// = LLONG_MAX;
}

impl ImageListLingerDefn {
    pub fn new(now_ms: Moment, image_array: ImageArray) -> Self {
        Self {
            image_array,
            time_of_last_state_change_ms: now_ms,
        }
    }
}


struct LogBuffersDefn {
    log_buffers: Arc<Mutex<LogBuffers>>,
    time_of_last_state_change_ms: Moment,
}

impl LogBuffersDefn {
    pub fn new(buffers: Arc<Mutex<LogBuffers>>) -> Self {
        Self {
            log_buffers: buffers,
            time_of_last_state_change_ms: MAX_MOMENT,
        }
    }
}

struct DestinationStateDefn {
    error_message: String,
    correlation_id: i64,
    registration_id: i64,
    time_of_registration_ms: Moment,
    error_code: i32,
    status: RegistrationStatus,
}

impl DestinationStateDefn {
    pub fn new(correlation_id: i64, registration_id: i64, now_ms: Moment) -> Self {
        Self {
            error_message:  String::from(""),
            registration_id,
            correlation_id,
            time_of_registration_ms: now_ms,
            error_code: -1,
            status: RegistrationStatus::AwaitingMediaDriver
        }
    }
}


struct ClientConductor<'a> {
    publication_by_registration_id: HashMap<i64, PublicationStateDefn>,
    exclusive_publication_by_registration_id: HashMap<i64, ExclusivePublicationStateDefn>,
    subscription_by_registration_id: HashMap<i64, SubscriptionStateDefn>,
    counter_by_registration_id: HashMap<i64, CounterStateDefn>,
    destination_state_by_correlation_id: HashMap<i64, DestinationStateDefn>,

    log_buffers_by_registration_id: HashMap<i64, LogBuffersDefn>,
    lingering_image_lists: Vec<ImageListLingerDefn>,

    driver_proxy: DriverProxy<'a>,
    driver_listener_adapter: DriverListenerAdapter<'a, ClientConductor<'a>>,

    counters_reader: CountersReader,
    counter_values_buffer: AtomicBuffer,

    on_new_publication_handler: OnNewPublication,
    on_new_exclusive_publication_handler: OnNewExclusivePublication,
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
    is_in_callback: bool, // = false;
    driver_active: AtomicBool,
    is_closed: AtomicBool,
    admin_lock: Mutex<bool>,
    heartbeat_timestamp: AtomicCounter,

    time_of_last_do_work_ms: Moment,
    time_of_last_keepalive_ms: Moment,
    time_of_last_check_managed_resources_ms: Moment,

    padding: [u8; crate::utils::misc::CACHE_LINE_LENGTH as usize],
}

impl ClientConductor {
    pub fn new(
        epoch_clock: EpochClock,
        driver_proxy: DriverProxy,
        broadcast_receiver: CopyBroadcastReceiver,
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
        inter_service_timeout_ns: Monent,
        pre_touch_mapped_memory: bool) -> Self {

        let mut selfy = Self {
            publication_by_registration_id: Default::default(),
            exclusive_publication_by_registration_id: Default::default(),
            subscription_by_registration_id: Default::default(),
            counter_by_registration_id: Default::default(),
            destination_state_by_correlation_id: Default::default(),
            log_buffers_by_registration_id: Default::default(),
            lingering_image_lists: vec![],
            driver_proxy,
            driver_listener_adapter: (),
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
            inter_service_timeout_ms: inter_service_timeout_ns / 1000000,
            pre_touch_mapped_memory,
            is_in_callback: false,
            driver_active: AtomicBool::from(true),
            is_closed: AtomicBool::from(false),
            admin_lock: Mutex::new(false),
            heartbeat_timestamp: AtomicCounter::from(0),
            time_of_last_do_work_ms: epoch_clock(),
            time_of_last_keepalive_ms: epoch_clock(),
            time_of_last_check_managed_resources_ms: epoch_clock(),
            padding: [0; crate::utils::misc::CACHE_LINE_LENGTH as usize]
        };

        selfy.driver_listener_adapter = DriverListenerAdapter::new(broadcast_receiver, &selfy);
        selfy.on_available_counter_handlers.push(on_available_counter_handler);
        selfy.on_unavailable_counter_handlers.push(on_unavailable_counter_handler);
        selfy.on_close_client_handlers.push(on_close_client_handler);
        
        selfy
    }
    
pub fn counters_reader(&self) -> &CountersReader {
    self.ensure_open()?;
    &self.counters_reader
}

pub fn channel_status(&self, counter_id: i32) -> i64 {
    match counter_id {
        0 => ChannelEndpointStatus::CHANNEL_ENDPOINT_INITIALIZING,
        ChannelEndpointStatus::NO_ID_ALLOCATED => ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE,
        _=> self.counters_reader.get_counter_value(counter_id)
    }
}

pub fn is_closed(&self) -> bool {
    self.is_closed.load(Ordering::Acquire)
}

pub fn ensure_open(&self) -> Result<(), AeronError>{
    if self.is_closed() {
        Err(AeronError::GenericError(String::from("Aeron client conductor is closed")))
    } else {
        Ok(())
    }
}

fn on_heartbeat_check_timeouts(&mut self) -> Result<i64, AeronError> {
    let now_ms = self.epoch_clock();
    let mut result: i64 = 0;

    if now_ms > self.time_of_last_do_work_ms + self.inter_service_timeout_ms {
        close_all_resources(now_ms);

        let err = Err(AeronError::ConductorServiceTimeout(format!(
            "timeout between service calls over {} ms",
            self.inter_service_timeout_ms
        )));
        
        self.error_handler(err);
    }

    self.time_of_last_do_work_ms = now_ms;

    if now_ms > self.time_of_last_keepalive_ms + KEEPALIVE_TIMEOUT_MS {
        if now_ms > self.driver_proxy.time_of_last_driver_keepalive() as Moment + self.driver_timeout_ms {
            self.driver_active.store(false, Ordering::SeqCst);

            let err = Err(AeronError::ConductorServiceTimeout(format!(
                "driver has been inactive for over {} ms",
                self.driver_timeout_ms
            )));

            self.error_handler(err);
        }

        let client_id = self.driver_proxy.client_id();
        if self.heartbeat_timestamp != 0 {
            if heartbeat_timestamp::is_active(
                    &self.counters_reader,
            self.heartbeat_timestamp.id(),
                    heartbeat_timestamp::CLIENT_HEARTBEAT_TYPE_ID,
                client_id) {
                self.heartbeat_timestamp.set_ordered(now_ms);
            } else{
                close_all_resources(now_ms);

                let err = Err(AeronError::GenericError(String::from(
                "client heartbeat timestamp not active")));
    
                self.error_handler(err);
            }
        } else {
            let counter_id = heartbeat_timestamp::find_counter_id_by_registration_id(
                &self.counters_reader, heartbeat_timestamp::CLIENT_HEARTBEAT_TYPE_ID, client_id);
            
            if let Some(id) = counter_id {
                self.heartbeat_timestamp.reset(AtomicCounter::new(self.counter_values_buffer, id));
                self.heartbeat_timestamp.set_ordered(now_ms);
            }
        }

        self.time_of_last_keepalive_ms = now_ms;
        result = 1;
    }

    if now_ms > self.time_of_last_check_managed_resources_ms + RESOURCE_TIMEOUT_MS {
        on_check_managed_resources(now_ms);
        self.time_of_last_check_managed_resources_ms = now_ms;
        result = 1;
    }
    
    Ok(result)
}

pub fn verify_driver_is_active(&self) -> Result<(), AeronError> {
    if !self.driver_active.load(Ordering::SeqCst) {
        Err(AeronError::DriverTimeout(String::from(
            "driver is inactive")))
    } else {
        Ok(())
    }
}

pub fn verify_driver_is_active_via_error_handler(&self) {
    if !self.driver_active.load(Ordering::SeqCst) {
        let err = Err(AeronError::DriverTimeout(String::from(
            "driver is inactive")));
        self.error_handler(err);
    }
}

pub fn ensure_not_reentrant(&self) {
    if self.is_in_callback {
        let err = Err(AeronError::ReentrantException(String::from(
            "client cannot be invoked within callback")));
        self.error_handler(err);
    }
}

    // Returns thread safe shared mutable instance of LogBuffers
pub fn get_log_buffers(
    &mut self,
    registration_id: i64, log_filename: &str, channel: &str) -> Result<Arc<Mutex<LogBuffers>>, AeronError>
{
    if let Some(lb) = self.log_buffers_by_registration_id.get_mut(&registration_id) {
        lb.time_of_last_state_change_ms = MAX_MOMENT;
        Ok(lb.log_buffers.clone())
    } else {
        let touch = self.pre_touch_mapped_memory && !channel.contains("sparse=true");
        let log_buffer = LogBuffers::from_existing(log_filename, touch)?;

        let log_buffers = Arc::new(Mutex::new(log_buffer));
        self.log_buffers_by_registration_id.insert(registration_id, LogBuffersDefn(log_buffers));

        Ok(log_buffers.clone())
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

pub fn on_start(&self) {
    // intentionally empty
}

pub fn do_work(&mut self) -> i64 {
    let mut work_count = 0;
    
    work_count += self.driver_listener_adapter.receive_messages();
    work_count += self.on_heartbeat_check_timeouts();
    work_count
}

pub fn on_close(&self) {
    if !self.is_closed.load(Ordering::SeqCst) {
        let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_close"); // don't allow other threads do close_all_resources() in parallel
        self.close_all_resources(self.epoch_clock());
    }
}

pub fn add_publication(&mut self, channel: &str, stream_id: i32) -> Result<i64, AeronError> {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in add_publication");
    self.verify_driver_is_active()?;
    self.ensure_not_reentrant()?;
    self.ensure_open()?;

    let registration_id = self.driver_proxy.add_publication(CString::from(channel), stream_id)?;

    self.publication_by_registration_id.insert(
    registration_id,
    PublicationStateDefn::new(String::from(channel), registration_id, stream_id, self.epoch_clock()));

    Ok(registration_id)
}

pub fn find_publication(&mut self, registration_id: i64) -> Result<Arc<Mutex<Publication>>, AeronError> {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in find_publication");
    self.ensure_not_reentrant()?;
    self.ensure_open()?;

    if let Some(state) = self.publication_by_registration_id.get_mut(&registration_id) {

        // try to upgrade weak ptr to strong one and use it
        if let Some(publication) = state.publication.upgrade() {
            match state.status {
                RegistrationStatus::AwaitingMediaDriver => {
                    if self.epoch_clock() > state.time_of_registration_ms + self.driver_timeout_ms {
                        return Err(AeronError::ConductorServiceTimeout(format!(
                            "no response from driver in {} ms",
                            self.driver_timeout_ms
                        )));
                    }
                }
                RegistrationStatus::RegisteredMediaDriver => {
                    let publication_limit = UnsafeBufferPosition::new(self.counter_values_buffer, state.publication_limit_counter_id);

                    let publication = Publication::new(
                        self,
                        &state.channel,
                        state.registration_id,
                        state.original_registration_id,
                        state.stream_id,
                        state.session_id,
                        publication_limit,
                        state.channel_status_id,
                        state.buffers.clone());

                    state.publication = Arc::new(publication).downgrade();
                }

                RegistrationStatus::ErroredMediaDriver => {
                    self.publication_by_registration_id.remove(&registration_id);

                    ClientConductor::return_registration_error(state.error_code, &state.error_message)?;
                }
            }
        } else {
            Err(AeronError::GenericError(String::from(
                "publication already dropped"
            )))
        }
        Ok(publication)
    } else {
        // error, publication not found
        Err(AeronError::GenericError(String::from(
            "publication not found"
        )))
    }

    Ok(publication)
}

    pub fn return_registration_error(err_code: i32, err_message: &str) -> Result<(),AeronError> {
        Err(AeronError::RegistrationException(format!(
            "error code {}, error message: ",
            err_code, err_message
        )))
    }

pub fn release_publication(&mut self, registration_id: i64) {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in release_publication");
    self.verify_driver_is_active_via_error_handler();
    
    if let Some(publication) = self.publication_by_registration_id.get(&registration_id) {
        self.driver_proxy.remove_publication(registration_id);
        self.publication_by_registration_id.remove(&registration_id);
    }
}

pub fn add_exclusive_publication(&mut self, channel: &str, stream_id: i32) -> Result<i64,AeronError> {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in add_exclusive_publication");
    self.verify_driver_is_active()?;
    self.ensure_not_reentrant()?;
    self.ensure_open()?;
    
    let registration_id = self.driver_proxy.add_exclusive_publication(CString::from(channel), stream_id)?;
    
    self.exclusive_publication_by_registration_id.insert(
    registration_id,
    ExclusivePublicationStateDefn::new(String::from(channel), registration_id, stream_id, self.epoch_clock())));

    Ok(registration_id)
}

    // TODO: looks like it could be made generic together with find_publication()
pub fn find_exclusive_publication(&mut self, registration_id: i64) -> Result<Arc<Mutex<ExclusivePublication>>, AeronError> {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in find_exclusive_publication");
    self.ensure_not_reentrant()?;
    self.ensure_open()?;

    if let Some(state) = self.exclusive_publication_by_registration_id.get_mut(&registration_id) {

        // try to upgrade weak ptr to strong one and use it
        if let Some(publication) = state.publication.upgrade() {
            match state.status {
                RegistrationStatus::AwaitingMediaDriver => {
                    if self.epoch_clock() > state.time_of_registration_ms + self.driver_timeout_ms {
                        return Err(AeronError::ConductorServiceTimeout(format!(
                            "no response from driver in {} ms",
                            self.driver_timeout_ms
                        )));
                    }
                }
                RegistrationStatus::RegisteredMediaDriver => {
                    let publication_limit = UnsafeBufferPosition::new(self.counter_values_buffer, state.publication_limit_counter_id);

                    let publication = ExclusivePublication::new(
                        self,
                        &state.channel,
                        state.registration_id,
                        state.original_registration_id,
                        state.stream_id,
                        state.session_id,
                        publication_limit,
                        state.channel_status_id,
                        state.buffers.clone());

                    state.publication = Arc::new(publication).downgrade();
                }

                RegistrationStatus::ErroredMediaDriver => {
                    self.exclusive_publication_by_registration_id.remove(&registration_id);

                    ClientConductor::return_registration_error(state.error_code, &state.error_message)?;
                }
            }
        } else {
            Err(AeronError::GenericError(String::from(
                "publication already dropped"
            )))
        }
        Ok(publication)
    } else {
        Err(AeronError::GenericError(String::from(
            "publication not found"
        )))
    }

    Ok(publication)
}

pub fn release_exclusive_publication(&mut self, registration_id: i64) {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in release_exclusive_publication");
    self.verify_driver_is_active_via_error_handler();

    if let Some(publication) = self.publication_by_registration_id.get(&registration_id) {
        self.driver_proxy.remove_publication(registration_id);
        self.exclusive_publication_by_registration_id.remove(&registration_id);
    }
}

pub fn add_subscription(
    &mut self,
    channel: &str,
    stream_id: i32,
    on_available_image_handler: OnAvailableImage,
    on_unavailable_image_handler: OnUnavailableImage) -> Result<i64, AeronError>
{
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in find_publication");
    self.verify_driver_is_active()?;
    self.ensure_not_reentrant()?;
    self.ensure_open()?;

    let registration_id = self.driver_proxy.add_subscription(CString::from(channel), stream_id)?;

    self.subscription_by_registration_id.insert(
    registration_id,
    SubscriptionStateDefn::new(
    String::from(channel), registration_id, stream_id, self.epoch_clock(), on_available_image_handler, on_unavailable_image_handler));

    Ok(registration_id)
}

pub fn find_subscription(&mut self, registration_id: i64) -> Result<Arc<Mutex<Subscription>>, AeronError> {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in find_publication");
    self.ensure_not_reentrant()?;
    self.ensure_open()?;

    if let Some(state) = self.subscription_by_registration_id.get_mut(&registration_id) {

        if let Some (cache) = state.subscription_cache.clone() {
            // Release the ownership of the managed object. Managed object should be dropped.
            state.subscription_cache.reset(); // FIXME: may be Option needed inside Arc()
        }

        // try to upgrade weak ptr to strong one and use it
        if let Some(subscription) = state.subscription.upgrade() {
            Ok(subscription)
        } else {
            // subscription has been dropped already
            if RegistrationStatus::AwaitingMediaDriver == state.status {
                if self.epoch_clock() > state.time_of_registration_ms + self.driver_timeout_ms {
                    return Err(AeronError::DriverTimeoutException(format!(
                        "no response from driver in {} ms",
                        self.driver_timeout_ms
                    )));
                }
            } else if RegistrationStatus::ErroredMediaDriver == state.status {
                self.subscription_by_registration_id.remove(&registration_id);

                ClientConductor::return_registration_error(state.error_code, &state.error_message)?;
            }

            Err(AeronError::GenericError(String::from(
                "subscription has been dropped already")))
        }
    } else {
        Err(AeronError::GenericError(String::from(
            "subscription not found"
        )))
    }
}

pub fn release_subscription(&mut self, registration_id: i64, images: Vec<Arc<Mutex<Image>>>) {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in find_publication");
    self.verify_driver_is_active_via_error_handler();
    
    if let Some(subscription) = self.subscription_by_registration_id.get(&registration_id) {
        self.driver_proxy.remove_subscription(registration_id);
        self.linger_all_resources(self.epoch_clock(), images);
        
        for mut image in images {
            // close the image
            image.lock().excpect("can't lock mutex on image").close();

            let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
            subscription.on_unavailable_image_handler(*image);
        }
    
        self.subscription_by_registration_id.remove(&registration_id);
    }
}

pub fn add_counter(
    &mut self,
    type_id: i32, key_buffer: &[u8], label: &CStr) -> Result<i64, AeronError>
{
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in add_counter");
    self.verify_driver_is_active()?;
    self.ensure_not_reentrant()?;
    self.ensure_open()?;

    if key.len() > counters::MAX_KEY_LENGTH {
        return Err(AeronError::IllegalArgumentException(format!(
            "key length out of bounds: {}",
            key.len()
        )));
    }

    if label.len() > counters::MAX_LABEL_LENGTH {
        return Err(AeronError::IllegalArgumentException(format!(
            "label length out of bounds: {}",
            label.len()
        )));
    }

    let registration_id = self.driver_proxy.add_counter(type_id, key_buffer, label)?;

    self.counter_by_registration_id.insert(
    registration_id, CounterStateDefn(registration_id, self.epoch_clock()));

    OK(registration_id)
}

pub fn find_counter(&mut self, registration_id: i64) -> Result<Arc<Mutex<Counter>>, AeronError>
{
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in find_counter");
    self.ensure_not_reentrant()?;
    self.ensure_open()?;

    if let Some(state) = self.counter_by_registration_id.get_mut(&registration_id) {
        if let Some(cnt) = state.counter_cache.clone() {
            // Release the ownership of the managed object. Managed object should be dropped.
            cnt.reset(); // FIXME: may be Option needed inside Arc()
        }

        // try to upgrade weak ptr to strong one and use it
        if let Some(counter) = state.counter.upgrade() {
            Ok(counter)
        } else {
            // counter has been dropped already
            if RegistrationStatus::AwaitingMediaDriver == state.status {
                if self.epoch_clock() > state.time_of_registration_ms + self.driver_timeout_ms {
                    return Err(AeronError::DriverTimeoutException(format!(
                        "no response from driver in {} ms",
                        self.driver_timeout_ms
                    )));
                }
            } else if RegistrationStatus::ErroredMediaDriver == state.status {
                self.counter_by_registration_id.remove(&registration_id);

                ClientConductor::return_registration_error(state.error_code, &state.error_message)?;
            }

            Err(AeronError::GenericError(String::from(
                "counter has been dropped already")))
        }
    } else {
        Err(AeronError::GenericError(String::from(
            "counter not found"
        )))
    }
}

pub fn release_counter(&mut self, registration_id: i64) {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in release_counter");
    self.verify_driver_is_active_via_error_handler();

    if let Some(counter)  = self.counter_by_registration_id.get(&registration_id) {
        self.driver_proxy.remove_counter(registration_id);
        self.counter_by_registration_id.remove(&registration_id);
    }
}

pub fn add_destination(&mut self,
    publication_registration_id: i64, endpoint_channel: &str) -> Result<i64, AeronError>
{
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in add_destination");
    self.verify_driver_is_active()?;
    self.ensure_not_reentrant()?;
    self.ensure_open()?;
    
    let correlation_id = self.driver_proxy.add_destination(publication_registration_id, CString::from(endpoint_channel))?;
    
    self.destination_state_by_correlation_id.insert(
    correlation_id,
    DestinationStateDefn::new(correlation_id, publication_registration_id, self.epoch_clock())));
    
    OK(correlation_id)
}

pub fn remove_destination(&mut self,
    publication_registration_id: i64, endpoint_channel: &str) -> Result<i64, AeronError>
{
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in remove_destination");
    self.verify_driver_is_active()?;
    self.ensure_not_reentrant()?;
    self.ensure_open()?;

    let correlation_id = self.driver_proxy.remove_destination(publication_registration_id, CString::from(endpoint_channel))?;

    // FIXME: the code is ported from C++ as is. But it seems there is a bug. We need to remove destination from
    // destination_state_by_correlation_id instead of inserting.
    self.destination_state_by_correlation_id.insert(
    correlation_id,
    DestinationStateDefn::new(correlation_id, publication_registration_id, self.epoch_clock())));

    Ok(correlation_id)
}

pub fn add_rcv_destination(&mut self,
subscription_registration_id: i64, endpoint_channel: &str) -> Result<i64, AeronError> {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in add_rcv_destination");
    self.verify_driver_is_active()?;
    self.ensure_not_reentrant()?;
    self.ensure_open()?;

    let correlation_id = self.driver_proxy.add_rcv_destination(subscription_registration_id, CString::from(endpoint_channel))?;

    self.destination_state_by_correlation_id.insert(
    correlation_id,
    DestinationStateDefn::new(correlation_id, subscription_registration_id, self.epoch_clock())));

    Ok(correlation_id)
}

pub fn remove_rcv_destination(&mut self,
        subscription_registration_id: i64, endpoint_channel: &str) -> Result<i64, AeronError>
{
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in remove_rcv_destination");
    self.verify_driver_is_active()?;
    self.ensure_not_reentrant()?;
    self.ensure_open()?;

    let correlation_id = self.driver_proxy.remove_rcv_destination(subscription_registration_id, CString::from(endpoint_channel))?;

    self.destination_state_by_correlation_id.insert(
    correlation_id,
    DestinationStateDefn::new(correlation_id, subscription_registration_id, self.epoch_clock())));

    Ok(correlation_id)
}

pub fn find_destination_response(&mut self, correlation_id: i64) -> Result<bool, AeronError>
{
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in find_destination_response");
    self.ensure_not_reentrant()?;
    self.ensure_open()?;

    if let Some(state) = self.destination_state_by_correlation_id.get_mut(&correlation_id) {

        // Regardless of status remove this destination from the map
        self.destination_state_by_correlation_id.remove(&correlation_id);

        match state.status {
            RegistrationStatus::AwaitingMediaDriver => {
                if self.epoch_clock() > state.time_of_registration_ms + self.driver_timeout_ms {
                    return Err(AeronError::ConductorServiceTimeout(format!(
                        "no response from driver in {} ms",
                        self.driver_timeout_ms
                    )));
                }
                Ok(false)
            }
            RegistrationStatus::RegisteredMediaDriver => {
                Ok(true)
            }
            RegistrationStatus::ErroredMediaDriver => {
                ClientConductor::return_registration_error(state.error_code, &state.error_message)
            }
        }
    } else {
        Err(AeronError::GenericError(String::from(
            "correlation_id unknown"
        )))
    }
}

pub fn add_available_counter_handler(&mut self, handler: OnAvailableCounter) {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in add_available_counter_handler");
    self.ensure_not_reentrant()?;
    self.ensure_open()?;

    self.on_available_counter_handlers.push(handler);
}

pub fn remove_available_counter_handler(&mut self, handler: OnAvailableCounter) {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in remove_available_counter_handler");
    self.ensure_not_reentrant()?;
    self.ensure_open()?;

    self.on_available_counter_handlers.retain(|item| item != handler);
}

pub fn add_unavailable_counter_handler(&mut self,  handler: OnUnavailableCounter) {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in add_unavailable_counter_handler");
    self.ensure_not_reentrant()?;
    self.ensure_open()?;
    
    self.on_unavailable_counter_handlers.push(handler);
}
    
    pub fn remove_unavailable_counter_handler(&mut self, handler: OnUnavailableCounter) {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in remove_unavailable_counter_handler");
    self.ensure_not_reentrant()?;
    self.ensure_open()?;
        self.on_unavailable_counter_handlers.retain(|item| item != handler);
}

pub fn add_close_client_handler(&mut self, handler: OnCloseClient) {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in find_publication");
    self.ensure_not_reentrant()?;
    self.ensure_open()?;
    
    self.on_close_client_handlers.push(handler);
}

pub fn removeCloseClientHandler(&mut self,  handler: OnCloseClient) {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in find_publication");
    self.ensure_not_reentrant()?;
    self.ensure_open()?;

    self.on_close_client_handlers.retain(|item| item != handler);
}

pub fn on_new_publication(&mut self,
                          registration_id: i64,
                          original_registration_id: i64,
                          stream_id: i32,
                          session_id: i32,
                          publication_limit_counter_id: i32,
                          channel_status_indicator_id: i32,
                          log_file_name: &str)
{
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_new_publication");

    if let Some(state) = self.publication_by_registration_id.get_mut(&registration_id) {
        state.status = RegistrationStatus::RegisteredMediaDriver;
        state.session_id = session_id;
        state.publication_limit_counter_id = publication_limit_counter_id;
        state.channel_status_id = channel_status_indicator_id;
        state.buffers = get_log_buffers(original_registration_id, log_file_name, state.channel.clone());
        state.original_registration_id = original_registration_id;

        let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
        self.on_new_publication_handler(state.channel.clone(), stream_id, session_id, registration_id);
    }
}

pub fn on_new_exclusive_publication(&mut self,
    registration_id: i64,
    original_registration_id: i64,
    stream_id: i32,
    session_id: i32,
    publication_limit_counter_id: i32,
    channel_status_indicator_id: i32,
    log_file_name: &str)
{
    assert_eq!(registration_id, original_registration_id);
    
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_new_exclusive_publication");

    if let Some(state) = self.exclusive_publication_by_registration_id.get_mut(&registration_id) {
        state.status = RegistrationStatus::RegisteredMediaDriver;
        state.session_id = session_id;
        state.publication_limit_counter_id = publication_limit_counter_id;
        state.channel_status_id = channel_status_indicator_id;
        state.buffers = get_log_buffers(original_registration_id, log_file_name, state.channel.clone());
        
        let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
        self.on_new_exclusive_publication_handler(state.channel.clone(), stream_id, session_id, registration_id);
    }
}

pub fn on_subscription_ready(&mut self, registration_id: i64, channel_status_id: i32)
{
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_subscription_ready");

    if let Some(state) = self.subscription_by_registration_id.get_mut(&registration_id) {

        state.status = RegistrationStatus::RegisteredMediaDriver;

        let subscr = Arc::new(Mutex::new(Subscription::new(
            self, state.registration_id, state.channel.clone(), state.stream_id, channel_status_id)));
        state.subscription_cache = Some(subscr);
        state.subscription = subscr.downgrade();
        
        let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
        self.on_new_subscription_handler(state.channel.clone(), state.stream_id, registration_id);
    }
}

pub fn on_available_counter(&mut self, registration_id: i64, counter_id: i32) {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_available_counter");

    if let Some(state) = self.counter_by_registration_id.get_mut(&registration_id) {
        if state.status == RegistrationStatus::AwaitingMediaDriver {
            state.status = RegistrationStatus::RegisteredMediaDriver;
            state.counter_id = counter_id;

            let cnt = Arc::new(Mutex::new(Counter::new(this, self.counter_values_buffer, state.registration_id, counter_id)));
            state.counter_cache = Some(cnt);
            state.counter = cnt.downgrade();
        }

        for handler in self.on_available_counter_handlers {
            let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
            handler(&self.counters_reader, registration_id, counter_id);
        }
    }
}

pub fn on_unavailable_counter(&mut self, registration_id: i64, counter_id: i32) {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_unavailable_counter");
    
    for handler in self.on_unavailable_counter_handlers {
        let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
        handler(&self.counters_reader, registration_id, counter_id);
    }
}

pub fn on_operation_success(&mut self, correlation_id: i64) {
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_operation_success");

    if let Some(state) = self.destination_state_by_correlation_id.get_mut(&correlation_id) {
        if state.status == RegistrationStatus::AwaitingMediaDriver {
            state.status = RegistrationStatus::RegisteredMediaDriver;
        }
    }
}

pub fn on_channel_endpoint_error_response(&mut self,
    offending_command_correlation_id: i64, error_message: &str)
{
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_channel_endpoint_error_response");

    for (reg_id, subscr_defn) in self.subscription_by_registration_id {

        if let Some(subscription) = subscr_defn.subscription.upgrade() {
            if subscription.channel_status_id() == offending_command_correlation_id {
                self.error_handler(ChannelEndpointException((offending_command_correlation_id, String::from(error_message))));

                let images_pair = subscription.close_and_remove_images();

                let images = images_pair.0; // images array TODO: return Vec<Arc<Image>> from close_and_remove_images()
                self.linger_all_resources(self.epoch_clock(), images);

                let length = images_pair.1; // number of images TODO: then don't need cnt param
                for image in images {
                    image.close();

                    let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
                    subscr_defn.on_unavailable_image_handler(image);
                }

                self.subscription_by_registration_id.remove(&reg_id);
            }
        }
    }

    for (reg_id, publication_defn) in self.publication_by_registration_id {
        if let Some(publication) = publication_defn.publication.upgrade() {
            if publication.channel_status_id() == offending_command_correlation_id {
                self.error_handler(ChannelEndpointException((offending_command_correlation_id, String::from(error_message))));
                publication.close();
                self.publication_by_registration_id.remove(&reg_id);
            }
        }
    }

    for (reg_id, publication_defn) in self.exclusive_publication_by_registration_id {
        if let Some(publication) = publication_defn.publication.upgrade() {
            if publication.channel_status_id() == offending_command_correlation_id {
                self.error_handler(ChannelEndpointException((offending_command_correlation_id, String::from(error_message))));
                publication.close();
                self.exclusive_publication_by_registration_id.remove(&reg_id);
            }
        }
    }
}

pub fn on_error_response(&mut self,
    offending_command_correlation_id: i64, error_code: i32, error_message: &str)
{
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_error_response");

    if let Some(subscription) = self.subscription_by_registration_id.get_mut(&offending_command_correlation_id) {
        subscription.status = RegistrationStatus::ErroredMediaDriver;
        subscription.error_code = error_code;
        subscription.error_message = String::from(error_message);
        return;
    }

    if let Some(publication) = self.publication_by_registration_id.get_mut(&offending_command_correlation_id) {
        publication.status = RegistrationStatus::ErroredMediaDriver;
        publication.error_code = error_code;
        publication.error_message = String::from(error_message);
        return;
    }

    if let Some(publication) = self.exclusive_publication_by_registration_id.get_mut(&offending_command_correlation_id) {
        publication.status = RegistrationStatus::ErroredMediaDriver;
        publication.error_code = error_code;
        publication.error_message = String::from(error_message);
        return;
    }

    if let Some(counter) = self.counter_by_registration_id.get_mut(&offending_command_correlation_id) {
        counter.status = RegistrationStatus::ErroredMediaDriver;
        ccounter.error_code = error_code;
        counter.error_message = String::from(error_message);
        return;
    }

    if let Some(destination) = self.destination_state_by_correlation_id.get_mut(&offending_command_correlation_id) {
        destination.status = RegistrationStatus::ErroredMediaDriver;
        destination.error_code = error_code;
        destination.error_message = String::from(error_message);
        return;
    }
}

pub fn on_available_image(&mut self,
                          correlation_id: i64,
                          session_id: i32,
                          subscriber_position_id: i32,
                          subscription_registration_id: i64,
                          log_filename: &str,
                          ource_identity: &str)
{
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_available_image");

    if let Some(subscr_defn) = self.subscription_by_registration_id.get_mut(&subscription_registration_id) {

        if let Some(subscription) = subscr_defn.subscription.upgrade() {
            let subscriber_position = UnsafeBufferPosition::new(self.counter_values_buffer, subscriber_position_id);

            let image = Arc::new(Image::create(
            session_id,
            correlation_id,
            subscription_registration_id,
            source_identity,
            subscriber_position,
            self.get_log_buffers(correlation_id, log_filename, entry.self.channel),
            self.error_handler);

            let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
            entry.self.on_available_image_handler(*image);

            if let Ok(old_image_array) = subscription.add_image(image) {
                self.linger_resource(self.epoch_clock(), old_image_array);
            }
        }
    }
}

pub fn on_unavailable_image(&mut self, correlation_id: i64, subscription_registration_id: i64)
{
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_unavailable_image");
    let now_ms = self.epoch_clock();

    if let Some(subscr_defn) = self.subscription_by_registration_id.get(&subscription_registration_id) {

        if let Some(subscription) = subscr_defn.subscription.upgrade() {
            let result = subscription.remove_image(correlation_id);
            // TODO: rewrite when Subscription will be ready
            Image::array_t oldImageArray = result.first;
            const std::size_t index = result.second;

            if (nullptr != oldImageArray)
            {
                lingerResource(now_ms, oldImageArray);

                let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
                entry.self.on_unavailable_image_handler(*oldImageArray[index]);
            }
        }
    }
}

pub fn on_client_timeout(&mut self, client_id: i64) {
    if self.driver_proxy.client_id() == client_id && !self.is_closed() {
        let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_client_timeout");
        self.close_all_resources(self.epoch_clock());
        self.error_handler(ClientTimeoutException(String::from("client timeout from driver")));
    }
}

pub fn close_all_resources(&mut self, now_ms: Moment) {
    self.is_closed.store(true, Ordering::Release);

    for (id, pub_defn) in self.publication_by_registration_id {
        if let Some(publication) = pub_defn.publication.upgrade() {
            publication.close();
        }
    }
    self.publication_by_registration_id.clear();

    for (id, pub_defn) in self.exclusive_publication_by_registration_id {
        if let Some(publication) = pub_defn.publication.upgrade() {
            publication.close();
        }
    }
    self.exclusive_publication_by_registration_id.clear();

    let mut subscriptions_to_hold_until_cleared: Vec<Arc<Subscription>> = Vec::default();

    for (id, sub_defn) in self.subscription_by_registration_id {
        if let Some(subscription) = sub_defn.subscription.upgrade() {

            let images_pair = subscription.close_and_remove_images();

            auto images = images_pair.first;
            self.linger_all_resources(now_ms, images);

            for image in images {
                image.close();

                let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
                sub_defn.on_unavailable_image_handler(image);
            }

            if let Some(cache) = sub_defn.subscription_cache {
                subscriptions_to_hold_until_cleared.from(sub_defn.subscription_cache);
                sub_defn.subscription_cache.reset(); // FIXME: need to release ownership here
            }
        }
    }

    self.subscription_by_registration_id.clear();

    let counters_to_hold_until_cleared: Vec<Arc<Counter>> = Vec::default();

    for (id, cnt_defn) in self.counter_by_registration_id {

        if let Some(counter) = cnt_defn.counter.upgrade() {

            counter.close();
            let registration_id = counter.registration_id();
            let counter_id = counter.id();

            for handler in cnt_defn.on_unavailable_counter_handlers {
                let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
                handler(self.counters_reader, registration_id, counter_id);
            }

            if let Some(cache) = cnt_defn.counter_cache {
                counters_to_hold_until_cleared.from(cnt_defn.counter_cache);
                kv.second.counter_cache.reset(); // FIXME
            }
        }
    }
    self.counter_by_registration_id.clear();

    for handler in self.on_close_client_handlers {
        let _callback_guard = CallbackGuard::new(&mut self.is_in_callback);
        handler();
    }
}

pub fn on_check_managed_resources(&mut self, now_ms: Moment)
{
    let _guard = self.admin_lock.lock().expect("Failed to obtain admin_lock in on_check_managed_resources");

    for (key, mut entry) in self.log_buffers_by_registration_id {

        if entry.log_buffers.strong_count() == 1 {
            if MAX_MOMENT == entry.time_of_last_state_change_ms {
                entry.time_of_last_state_change_ms = now_ms;
            } else if now_ms - self.resource_linger_timeout_ms > entry.time_of_last_state_change_ms {
                it = self.log_buffers_by_registration_id.erase(it); // FIXME
                continue;
            }
        }

        ++it;
    }

    auto arrayIt = std::remove_if(self.lingering_image_lists.begin(), self.lingering_image_lists.end(),
    [now_ms, this](ImageListLingerDefn &entry)
    {
    if ((now_ms - self.resource_linger_timeout_ms) > entry.time_of_last_state_change_ms)
    {
    delete [] entry.image_array;
    entry.image_array = nullptr;

    return true;
    }

    return false;
    });

    self.lingering_image_lists.erase(arrayIt, self.lingering_image_lists.end());
}

pub fn linger_resource(&mut self, now_ms: Moment, images: Image::array_t) {
    self.lingering_image_lists.push(now_ms, images);
}

pub fn linger_all_resources(&mut self, now_ms: Moment, images: Image::array_t) {
    if (nullptr != images) {
        self.linger_resource(now_ms, images);
    }
}
}

    impl Drop for ClientConductor {
        fn drop(&mut self) {
            for img in self.lingering_image_lists {
                img.image_array.drop();
            }
            self.driver_proxy.client_close();
        }
    }


