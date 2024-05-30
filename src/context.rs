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

use std::env;
use std::ffi::CString;
use std::sync::Arc;

use crate::cnc_file_descriptor;
use crate::concurrent::counters::CountersReader;
use crate::concurrent::logbuffer::term_reader::ErrorHandler;
use crate::concurrent::ring_buffer::ManyToOneRingBuffer;
use crate::driver_proxy::DriverProxy;
use crate::image::Image;
use crate::utils::errors::{AeronError, GenericError};
use crate::utils::memory_mapped_file::MemoryMappedFile;
use crate::utils::misc::{semantic_version_major, semantic_version_to_string};
use crate::utils::types::{Index, Moment};

/// This name is used for conductor thread and useful when debugging or examining logs from
/// application with several Aeron instances which run simultaneously.
const AGENT_NAME: &str = "client-conductor";

/**
 * Used to represent a null value for when some value is not yet set.
 */
pub const NULL_VALUE: i32 = -1; // TODO replace on Option

/**
 * Function called by Aeron to deliver notification of an available image.
 *
 * The Image passed may not be the image used internally, but may be copied or moved freely.
 *
 * Implementations should do the minimum work for passing off state to another thread for later processing
 * and should not make a reentrant call back into the Aeron instance.
 *
 * @param image that has become available.
 */
pub trait OnAvailableImage {
    fn call(&self, image: &Image);
    fn clone_box(&self) -> Box<dyn OnAvailableImage>;
}

impl<T> OnAvailableImage for T
where
    T: Fn(&Image) + Clone + 'static,
{
    fn clone_box(&self) -> Box<dyn OnAvailableImage> {
        Box::new(self.clone())
    }

    fn call(&self, image: &Image) {
        self(image)
    }
}

impl Clone for Box<dyn OnAvailableImage> {
    fn clone(&self) -> Box<dyn OnAvailableImage> {
        self.clone_box()
    }
}

/**
 * Function called by Aeron to deliver notification that an Image has become unavailable for polling.
 *
 * The Image passed is not guaranteed to be valid after the callback.
 *
 * Implementations should do the minimum work for passing off state to another thread for later processing
 * and should not make a reentrant call back into the Aeron instance.
 *
 * @param image that has become unavailable
 */
pub trait OnUnavailableImage {
    fn call(&self, image: &Image);
    fn clone_box(&self) -> Box<dyn OnUnavailableImage>;
}

impl Clone for Box<dyn OnUnavailableImage> {
    fn clone(&self) -> Box<dyn OnUnavailableImage> {
        self.clone_box()
    }
}

impl<F> OnUnavailableImage for F
where
    F: Fn(&Image) + Clone + 'static,
{
    fn call(&self, image: &Image) {
        self(image)
    }

    fn clone_box(&self) -> Box<dyn OnUnavailableImage> {
        Box::new(self.clone())
    }
}

/**
 * Function called by Aeron to deliver notification that the media driver has added a Publication successfully.
 *
 * Implementations should do the minimum work for passing off state to another thread for later processing
 * and should not make a reentrant call back into the Aeron instance.
 *
 * @param channel of the Publication
 * @param stream_id within the channel of the Publication
 * @param session_id of the Publication
 * @param correlation_id used by the Publication for adding. Aka the registration_id returned by
 * Aeron::add_publication
 */
pub trait OnNewPublication {
    fn call(&self, channel: CString, stream_id: i32, session_id: i32, correlation_id: i64);
    fn clone_box(&self) -> Box<dyn OnNewPublication>;
}

impl Clone for Box<dyn OnNewPublication> {
    fn clone(&self) -> Box<dyn OnNewPublication> {
        self.clone_box()
    }
}

impl<F> OnNewPublication for F
where
    F: Fn(CString, i32, i32, i64) + Clone + 'static,
{
    fn call(&self, channel: CString, stream_id: i32, session_id: i32, correlation_id: i64) {
        self(channel, stream_id, session_id, correlation_id)
    }

    fn clone_box(&self) -> Box<dyn OnNewPublication> {
        Box::new(self.clone())
    }
}

/**
 * Function called by Aeron to deliver notification that the media driver has added a Subscription successfully.
 *
 * Implementations should do the minimum work for passing off state to another thread for later processing
 * and should not make a reentrant call back into the Aeron instance.
 *
 * @param channel of the Subscription
 * @param stream_id within the channel of the Subscription
 * @param correlation_id used by the Subscription for adding. Aka the registration_id returned by
 * Aeron::add_subscription
 */
pub trait OnNewSubscription {
    fn call(&self, channel: CString, stream_id: i32, correlation_id: i64);
    fn clone_box(&self) -> Box<dyn OnNewSubscription>;
}

impl Clone for Box<dyn OnNewSubscription> {
    fn clone(&self) -> Box<dyn OnNewSubscription> {
        self.clone_box()
    }
}

impl<F> OnNewSubscription for F
where
    F: Fn(CString, i32, i64) + Clone + 'static,
{
    fn call(&self, channel: CString, stream_id: i32, correlation_id: i64) {
        self(channel, stream_id, correlation_id)
    }

    fn clone_box(&self) -> Box<dyn OnNewSubscription> {
        Box::new(self.clone())
    }
}

/**
 * Function called by Aeron to deliver notification of a Counter being available.
 *
 * Implementations should do the minimum work for passing off state to another thread for later processing
 * and should not make a reentrant call back into the Aeron instance.
 *
 * @param counters_reader for more detail on the counter.
 * @param registration_id for the counter.
 * @param counter_id      that is available.
 */
pub trait OnAvailableCounter {
    fn call(&self, counters_reader: &CountersReader, registration_id: i64, counter_id: i32);
    fn clone_box(&self) -> Box<dyn OnAvailableCounter>;
}

impl Clone for Box<dyn OnAvailableCounter> {
    fn clone(&self) -> Box<dyn OnAvailableCounter> {
        self.clone_box()
    }
}

impl<F> OnAvailableCounter for F
where
    F: Fn(&CountersReader, i64, i32) + Clone + 'static,
{
    fn call(&self, counters_reader: &CountersReader, registration_id: i64, counter_id: i32) {
        self(counters_reader, registration_id, counter_id)
    }

    fn clone_box(&self) -> Box<dyn OnAvailableCounter> {
        Box::new(self.clone())
    }
}

/**
 * Function called by Aeron to deliver notification of counter being removed.
 *
 * Implementations should do the minimum work for passing off state to another thread for later processing
 * and should not make a reentrant call back into the Aeron instance.
 *
 * @param counters_reader for more counter details.
 * @param registration_id for the counter.
 * @param counter_id      that is unavailable.
 */
pub trait OnUnavailableCounter {
    fn call(&self, counters_reader: &CountersReader, registration_id: i64, counter_id: i32);
    fn clone_box(&self) -> Box<dyn OnUnavailableCounter>;
}

impl Clone for Box<dyn OnUnavailableCounter> {
    fn clone(&self) -> Box<dyn OnUnavailableCounter> {
        self.clone_box()
    }
}

impl<F> OnUnavailableCounter for F
where
    F: Fn(&CountersReader, i64, i32) + Clone + 'static,
{
    fn call(&self, counters_reader: &CountersReader, registration_id: i64, counter_id: i32) {
        self(counters_reader, registration_id, counter_id)
    }

    fn clone_box(&self) -> Box<dyn OnUnavailableCounter> {
        Box::new(self.clone())
    }
}

/**
 * Function called when the Aeron client is closed to notify that the client or any of it associated resources
 * should not be used after this event.
 */
pub trait OnCloseClient {
    fn call(&self);
    fn clone_box(&self) -> Box<dyn OnCloseClient>;
}

impl Clone for Box<dyn OnCloseClient> {
    fn clone(&self) -> Box<dyn OnCloseClient> {
        self.clone_box()
    }
}

impl<F> OnCloseClient for F
where
    F: Fn() + Clone + 'static,
{
    fn call(&self) {
        self()
    }
    fn clone_box(&self) -> Box<dyn OnCloseClient> {
        Box::new(self.clone())
    }
}

const DEFAULT_MEDIA_DRIVER_TIMEOUT_MS: Moment = 10000;
const DEFAULT_RESOURCE_LINGER_MS: Moment = 5000;

/**
 * The Default handler for Aeron runtime exceptions.
 *
 * When a DriverTimeoutException is encountered, this handler will exit the program.
 *
 * The error handler can be overridden by supplying an {@link Context} with a custom handler.
 *
 * @see Context#errorHandler
 */
fn default_error_handler(exception: AeronError) {
    panic!("AeronError: {:?}", exception);
}

fn default_on_new_publication_handler(_channel: CString, _stream_id: i32, _session_id: i32, _correlation_id: i64) {}

fn default_on_available_image_handler(_img: &Image) {}

fn default_on_new_subscription_handler(_channel: CString, _stream_id: i32, _correlation_id: i64) {}

fn default_on_unavailable_image_handler(_img: &Image) {}

fn default_on_available_counter_handler(_counters_reader: &CountersReader, _registration_id: i64, _counter_id: i32) {}

fn default_on_unavailable_counter_handler(_counters_reader: &CountersReader, _registration_id: i64, _counter_id: i32) {}

fn default_on_close_client_handler() {}

/**
 * Context provides configuration for the {@link Aeron} class via the {@link Aeron::Aeron} or {@link Aeron::connect}
 * methods and its overloads. It gives applications some control over the interactions with the Aeron Media Driver.
 * It can also set up error handling as well as application callbacks for connection information from the
 * Media Driver.
 */
#[derive(Clone)]
pub struct Context {
    dir_name: String,
    error_handler: Box<dyn ErrorHandler + Send>,
    on_new_publication_handler: Box<dyn OnNewPublication>,
    on_new_exclusive_publication_handler: Box<dyn OnNewPublication>,
    on_new_subscription_handler: Box<dyn OnNewSubscription>,
    on_available_image_handler: Box<dyn OnAvailableImage>,
    on_unavailable_image_handler: Box<dyn OnUnavailableImage>,
    on_available_counter_handler: Box<dyn OnAvailableCounter>,
    on_unavailable_counter_handler: Box<dyn OnUnavailableCounter>,
    on_close_client_handler: Box<dyn OnCloseClient>,
    media_driver_timeout: Moment,
    resource_linger_timeout: Moment,
    use_conductor_agent_invoker: bool,
    is_on_new_exclusive_publication_handler_set: bool,
    pre_touch_mapped_memory: bool,
    agent_name: String,
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

impl Context {
    pub fn new() -> Self {
        Self {
            dir_name: Context::default_aeron_path(),
            error_handler: Box::new(default_error_handler),
            on_new_publication_handler: Box::new(default_on_new_publication_handler),
            on_new_exclusive_publication_handler: Box::new(default_on_new_publication_handler),
            on_new_subscription_handler: Box::new(default_on_new_subscription_handler),
            on_available_image_handler: Box::new(default_on_available_image_handler),
            on_unavailable_image_handler: Box::new(default_on_unavailable_image_handler),
            on_available_counter_handler: Box::new(default_on_available_counter_handler),
            on_unavailable_counter_handler: Box::new(default_on_unavailable_counter_handler),
            on_close_client_handler: Box::new(default_on_close_client_handler),
            media_driver_timeout: DEFAULT_MEDIA_DRIVER_TIMEOUT_MS,
            resource_linger_timeout: DEFAULT_RESOURCE_LINGER_MS,
            use_conductor_agent_invoker: false,
            is_on_new_exclusive_publication_handler_set: false,
            pre_touch_mapped_memory: false,
            agent_name: String::from(AGENT_NAME),
        }
    }

    pub fn conclude(&mut self) -> &Self {
        if !self.is_on_new_exclusive_publication_handler_set {
            self.on_new_exclusive_publication_handler = self.on_new_publication_handler.clone_box();
        }

        self
    }

    pub fn agent_name(&self) -> String {
        self.agent_name.clone()
    }

    pub fn set_agent_name(&mut self, name: &str) {
        self.agent_name = String::from(name);
    }

    /**
     * Set the directory that the Aeron client will use to communicate with the media driver.
     *
     * @param directory to use
     * @return reference to this Context instance
     */
    pub fn set_aeron_dir(&mut self, directory: String) -> &Self {
        self.dir_name = directory;
        self
    }

    pub fn aeron_dir(&self) -> String {
        self.dir_name.clone()
    }

    /**
     * Return the path to the CnC file used by the Aeron client for communication with the media driver.
     *
     * @return path of the CnC file
     */
    pub fn cnc_file_name(&self) -> String {
        self.dir_name.clone() + "/" + cnc_file_descriptor::CNC_FILE
    }

    /**
     * Set the handler for exceptions from the Aeron client.
     *
     * @param handler called when exceptions arise
     * @return reference to this Context instance
     *
     * @see default_error_handler for how the default behavior is handled
     */
    pub fn set_error_handler(&mut self, handler: impl ErrorHandler + Send + 'static) -> &Self {
        self.error_handler = Box::new(handler);
        self
    }

    pub fn error_handler(&self) -> Box<dyn ErrorHandler + Send + 'static> {
        self.error_handler.clone()
    }

    /**
     * Set the handler for successful Aeron::add_publication notifications.
     *
     * @param handler called when add is completed successfully
     * @return reference to this Context instance
     */
    pub fn set_new_publication_handler(&mut self, handler: impl OnNewPublication + 'static) -> &Self {
        self.on_new_publication_handler = Box::new(handler);
        self
    }

    pub fn new_publication_handler(&self) -> Box<dyn OnNewPublication> {
        self.on_new_publication_handler.clone_box()
    }

    /**
     * Set the handler for successful Aeron::add_exclusive_publication notifications.
     *
     * If not set, then will use new_publication_handler instead.
     *
     * @param handler called when add is completed successfully
     * @return reference to this Context instance
     */
    pub fn set_new_exclusive_publication_handler(&mut self, handler: Box<dyn OnNewPublication>) -> &Self {
        self.on_new_exclusive_publication_handler = handler;
        self.is_on_new_exclusive_publication_handler_set = true;
        self
    }

    pub fn new_exclusive_publication_handler(&self) -> Box<dyn OnNewPublication> {
        self.on_new_exclusive_publication_handler.clone_box()
    }

    /**
     * Set the handler for successful Aeron::add_subscription notifications.
     *
     * @param handler called when add is completed successfully
     * @return reference to this Context instance
     */
    pub fn set_new_subscription_handler(&mut self, handler: impl OnNewSubscription + 'static) -> &Self {
        self.on_new_subscription_handler = Box::new(handler);
        self
    }

    pub fn new_subscription_handler(&self) -> Box<dyn OnNewSubscription> {
        self.on_new_subscription_handler.clone_box()
    }

    /**
     * Set the handler for available image notifications.
     *
     * @param handler called when event occurs
     * @return reference to this Context instance
     */
    pub fn set_available_image_handler(&mut self, handler: impl OnAvailableImage + 'static) -> &Self {
        self.on_available_image_handler = Box::new(handler);
        self
    }

    pub fn available_image_handler(&self) -> Box<dyn OnAvailableImage> {
        self.on_available_image_handler.clone_box()
    }

    /**
     * Set the handler for inactive image notifications.
     *
     * @param handler called when event occurs
     * @return reference to this Context instance
     */
    pub fn set_unavailable_image_handler(&mut self, handler: impl OnUnavailableImage + 'static) -> &Self {
        self.on_unavailable_image_handler = Box::new(handler);
        self
    }

    pub fn unavailable_image_handler(&self) -> Box<dyn OnUnavailableImage> {
        self.on_unavailable_image_handler.clone_box()
    }

    /**
     * Set the handler for available counter notifications.
     *
     * @param handler called when event occurs
     * @return reference to this Context instance
     */
    pub fn set_available_counter_handler(&mut self, handler: impl OnAvailableCounter + 'static) -> &Self {
        self.on_available_counter_handler = Box::new(handler);
        self
    }

    pub fn available_counter_handler(&self) -> Box<dyn OnAvailableCounter> {
        self.on_available_counter_handler.clone_box()
    }

    /**
     * Set the handler for inactive counter notifications.
     *
     * @param handler called when event occurs
     * @return reference to this Context instance
     */
    pub fn set_unavailable_counter_handler(&mut self, handler: impl OnUnavailableCounter + 'static) -> &Self {
        self.on_unavailable_counter_handler = Box::new(handler);
        self
    }

    pub fn unavailable_counter_handler(&self) -> Box<dyn OnUnavailableCounter> {
        self.on_unavailable_counter_handler.clone_box()
    }

    /**
     * Set the handler to be called when the Aeron client is closed and not longer active.
     *
     * @param handler to be called when the Aeron client is closed.
     * @return reference to this Context instance.
     */
    pub fn set_close_client_handler(&mut self, handler: impl OnCloseClient + 'static) -> &Self {
        self.on_close_client_handler = Box::new(handler);
        self
    }

    pub fn close_client_handler(&self) -> Box<dyn OnCloseClient> {
        self.on_close_client_handler.clone_box()
    }

    /**
     * Set the amount of time, in milliseconds, that this client will wait until it determines the
     * Media Driver is unavailable. When this happens a DriverTimeoutException will be generated for the error
     * handler.
     *
     * @param value Number of milliseconds.
     * @return reference to this Context instance
     * @see errorHandler
     */
    pub fn set_media_driver_timeout(&mut self, value: Moment) -> &Self {
        self.media_driver_timeout = value;
        self
    }

    /**
     * Get the amount of time, in milliseconds, that this client will wait until it determines the
     * Media Driver is unavailable. When this happens a DriverTimeoutException will be generated for the error
     * handler.
     *
     * @return value in number of milliseconds.
     * @see errorHandler
     */
    pub fn media_driver_timeout(&self) -> Moment {
        self.media_driver_timeout
    }

    /**
     * Set the amount of time, in milliseconds, that this client will to linger inactive connections and internal
     * arrays before they are free'd.
     *
     * @param value Number of milliseconds.
     * @return reference to this Context instance
     */
    pub fn set_resource_linger_timeout(&mut self, value: Moment) -> &Self {
        self.resource_linger_timeout = value;
        self
    }

    pub fn resource_linger_timeout(&self) -> Moment {
        self.resource_linger_timeout
    }

    /**
     * Set whether to use an invoker to control the conductor agent or spawn a thread.
     *
     * @param use_conductor_agent_invoker to use an invoker or not.
     * @return reference to this Context instance
     */
    pub fn set_use_conductor_agent_invoker(&mut self, use_conductor_agent_invoker: bool) -> &Self {
        self.use_conductor_agent_invoker = use_conductor_agent_invoker;
        self
    }

    pub fn use_conductor_agent_invoker(&self) -> bool {
        self.use_conductor_agent_invoker
    }

    /**
     * Set whether memory mapped files should be pre-touched so they are pre-loaded to avoid later page faults.
     *
     * @param pre_touch_mapped_memory true to pre-touch memory otherwise false.
     * @return reference to this Context instance
     */
    pub fn set_pre_touch_mapped_memory(&mut self, pre_touch_mapped_memory: bool) -> &Self {
        self.pre_touch_mapped_memory = pre_touch_mapped_memory;
        self
    }

    pub fn pre_touch_mapped_memory(&self) -> bool {
        self.pre_touch_mapped_memory
    }

    pub fn request_driver_termination(directory: &str, token_buffer: *mut u8, token_length: Index) -> Result<(), AeronError> {
        let cnc_filename = String::from(directory) + "/" + cnc_file_descriptor::CNC_FILE;

        if MemoryMappedFile::get_file_size(cnc_filename.clone()).expect("Error getting CnC file size") > 0 {
            let cnc_file = MemoryMappedFile::map_existing(cnc_filename, false).expect("Unable to map file");

            let cnc_version = cnc_file_descriptor::cnc_version_volatile(&cnc_file);

            if semantic_version_major(cnc_version) != semantic_version_major(cnc_file_descriptor::CNC_VERSION) {
                return Err(GenericError::CncVersionDoesntMatch {
                    app_version: semantic_version_to_string(cnc_file_descriptor::CNC_VERSION),
                    file_version: semantic_version_to_string(cnc_version),
                }
                .into());
            }

            let to_driver_buffer = cnc_file_descriptor::create_to_driver_buffer(&cnc_file);
            let ring_buffer = ManyToOneRingBuffer::new(to_driver_buffer).expect("ManyToOneRingBuffer creation failed");
            let driver_proxy = DriverProxy::new(Arc::new(ring_buffer));

            driver_proxy.terminate_driver(token_buffer, token_length)?;
        }
        Ok(())
    }

    pub fn tmp_dir() -> String {
        let mut dir = String::from("/tmp");

        if let Ok(env_dir) = env::var("TMPDIR") {
            dir = env_dir;
        }

        dir
    }

    pub fn get_user_name() -> String {
        if let Ok(user) = env::var("USER") {
            user
        } else {
            String::from("default")
        }
    }

    pub fn default_aeron_path_parent() -> String {
        match env::consts::OS {
            // as per https://github.com/real-logic/aeron/wiki/Configuration-Options#common-options
            "linux" => { String::from("/dev/shm/") }
            _ => {Self::tmp_dir()}
        }
    }

    pub fn default_aeron_path() -> String {
        format!("{}aeron-{}", Self::default_aeron_path_parent(), Self::get_user_name())
    }
}
