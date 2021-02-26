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
    ffi::CString,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use crate::utils::errors::{GenericError, IllegalStateError};
use crate::{
    client_conductor::ClientConductor,
    concurrent::{
        atomic_buffer::AtomicBuffer,
        atomic_vec::AtomicVec,
        logbuffer::{header::Header, term_scan::BlockHandler},
        status::status_indicator_reader,
    },
    image::{ControlledPollAction, Image},
    utils::{errors::AeronError, types::Index},
};

pub struct Subscription {
    conductor: Arc<Mutex<ClientConductor>>,
    channel: CString,
    channel_status_id: i32,
    round_robin_index: Index,
    //todo std::size_t
    registration_id: i64,
    stream_id: i32,

    image_list: AtomicVec<Image>,
    is_closed: AtomicBool,
}

impl Subscription {
    pub fn new(
        conductor: Arc<Mutex<ClientConductor>>,
        registration_id: i64,
        channel: CString,
        stream_id: i32,
        channel_status_id: i32,
    ) -> Self {
        Self {
            conductor,
            channel,
            channel_status_id,
            round_robin_index: 0,
            registration_id,
            stream_id,
            image_list: AtomicVec::new(),
            is_closed: AtomicBool::from(false),
        }
    }

    /**
     * Media address for delivery to the channel.
     *
     * @return Media address for delivery to the channel.
     */
    pub fn channel(&self) -> CString {
        self.channel.clone()
    }

    /**
     * Stream identity for scoping within the channel media address.
     *
     * @return Stream identity for scoping within the channel media address.
     */
    pub fn stream_id(&self) -> i32 {
        self.stream_id
    }

    /**
     * Registration Id returned by Aeron::addSubscription when this Subscription was added.
     *
     * @return the registrationId of the subscription.
     */
    pub fn registration_id(&self) -> i64 {
        self.registration_id
    }

    /**
     * Get the counter id used to represent the channel status.
     *
     * @return the counter id used to represent the channel status.
     */
    pub fn channel_status_id(&self) -> i32 {
        self.channel_status_id
    }

    pub fn add_destination(&self, endpoint_channel: String) -> Result<i64, AeronError> {
        if self.is_closed() {
            return Err(IllegalStateError::SubscriptionClosed.into());
        }

        if let Ok(endpoint_channel_cstr) = CString::new(endpoint_channel) {
            self.conductor
                .lock()
                .expect("Mutex poisoned")
                .add_rcv_destination(self.registration_id, endpoint_channel_cstr)
        } else {
            Err(GenericError::StringToCStringConversionFailed.into())
        }
    }

    pub fn remove_destination(&self, endpoint_channel: String) -> Result<i64, AeronError> {
        if self.is_closed() {
            return Err(IllegalStateError::SubscriptionClosed.into());
        }

        if let Ok(endpoint_channel_cstr) = CString::new(endpoint_channel) {
            self.conductor
                .lock()
                .expect("Mutex poisoned")
                .remove_rcv_destination(self.registration_id, endpoint_channel_cstr)
        } else {
            Err(GenericError::StringToCStringConversionFailed.into())
        }
    }

    pub fn find_destination_response(&self, correlation_id: i64) -> Result<bool, AeronError> {
        self.conductor
            .lock()
            .expect("Mutex poisoned")
            .find_destination_response(correlation_id)
    }

    pub fn channel_status(&self) -> i64 {
        if self.is_closed() {
            return status_indicator_reader::NO_ID_ALLOCATED as i64;
        }

        self.conductor
            .lock()
            .expect("Mutex poisoned")
            .channel_status(self.channel_status_id)
    }

    /**
     * Poll the Image s under the subscription for having reached End of Stream.
     *
     * @param end_of_stream_handler callback for handling end of stream indication.
     * @return number of Image s that have reached End of Stream.
     * @deprecated
     */

    pub fn poll_end_of_streams(&self, end_of_stream_handler: EndOfStreamHandler) -> i32 {
        let mut num_end_of_streams = 0;

        let image_list = self.image_list.load();

        for image in image_list.iter() {
            if image.is_end_of_stream() {
                num_end_of_streams += 1;
                end_of_stream_handler(image);
            }
        }

        num_end_of_streams
    }

    /**
     * Poll the {@link Image}s under the subscription for available message fragments.
     * <p>
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
     * as a series of fragments ordered withing a session.
     *
     * @param fragment_handler callback for handling each message fragment as it is read.
     * @param fragment_limit   number of message fragments to limit for the poll across multiple Image s.
     * @return the number of fragments received
     *
     * @see fragment_handler_t
     */

    pub fn poll(&mut self, fragment_handler: &mut impl FnMut(&AtomicBuffer, Index, Index, &Header), fragment_limit: i32) -> i32 {
        let image_list = self.image_list.load_mut();

        let mut fragments_read = 0;

        let mut starting_index = self.round_robin_index as usize;
        self.round_robin_index += 1;

        if starting_index >= image_list.len() {
            self.round_robin_index = 0;
            starting_index = 0;
        }

        for i in starting_index..image_list.len() {
            if fragments_read < fragment_limit {
                fragments_read += image_list
                    .get_mut(i)
                    .expect("Error getting element from Image vec")
                    .poll(fragment_handler, fragment_limit - fragments_read);
            }
        }

        for i in 0..starting_index {
            if fragments_read < fragment_limit {
                fragments_read += image_list
                    .get_mut(i)
                    .expect("Error getting element from Image vec")
                    .poll(fragment_handler, fragment_limit - fragments_read);
            }
        }

        fragments_read
    }

    /**
     * Poll in a controlled manner the Image s under the subscription for available message fragments.
     * Control is applied to fragments in the stream. If more fragments can be read on another stream
     * they will even if BREAK or ABORT is returned from the fragment handler.
     * <p>
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
     * as a series of fragments ordered within a session.
     * <p>
     * To assemble messages that span multiple fragments then use controlled_poll_fragment_handler_t.
     *
     * @param fragment_handler callback for handling each message fragment as it is read.
     * @param fragment_limit   number of message fragments to limit for the poll operation across multiple Image s.
     * @return the number of fragments received
     * @see controlled_poll_fragment_handler_t
     */
    pub fn controlled_poll(
        &mut self,
        fragment_handler: impl FnMut(&AtomicBuffer, Index, Index, &Header) -> Result<ControlledPollAction, AeronError> + Copy,
        fragment_limit: i32,
    ) -> i32 {
        let image_list = self.image_list.load_mut();

        let mut fragments_read = 0;

        let mut starting_index = self.round_robin_index as usize;
        self.round_robin_index += 1;

        if starting_index >= image_list.len() {
            self.round_robin_index = 0;
            starting_index = 0;
        }

        for i in starting_index..image_list.len() {
            if fragments_read < fragment_limit {
                fragments_read += image_list
                    .get_mut(i)
                    .expect("Error getting element from Image vec")
                    .controlled_poll(fragment_handler, fragment_limit - fragments_read);
            }
        }

        for i in 0..starting_index {
            if fragments_read < fragment_limit {
                fragments_read += image_list
                    .get_mut(i)
                    .expect("Error getting element from Image vec")
                    .controlled_poll(fragment_handler, fragment_limit - fragments_read);
            }
        }

        fragments_read
    }

    /**
     * Poll the Image s under the subscription for available message fragments in blocks.
     *
     * @param block_handler     to receive a block of fragments from each Image.
     * @param block_length_limit for each individual block.
     * @return the number of bytes consumed.
     */
    pub fn block_poll(&mut self, block_handler: BlockHandler, block_length_limit: i32) -> i64 {
        let image_list = self.image_list.load();

        let mut bytes_consumed: i64 = 0;

        for image in image_list {
            bytes_consumed += image.block_poll(block_handler, block_length_limit) as i64;
        }

        bytes_consumed
    }

    /**
     * Is the subscription connected by having at least one open image available.
     *
     * @return true if the subscription has more than one open image available.
     */
    pub fn is_connected(&self) -> bool {
        let image_list = self.image_list.load();

        let length = image_list.len();

        (0..length).any(|idx| !image_list.get(idx).expect("Error getting element from Image vec").is_closed())
    }

    /**
     * Count of images associated with this subscription.
     *
     * @return count of images associated with this subscription.
     */
    pub fn image_count(&self) -> usize {
        let image_list = self.image_list.load();
        image_list.len()
    }

    /**
     * Return the {@link Image} associated with the given session_id.
     *
     * This method generates a new copy of the Image overlaying the logbuffer.
     * It is up to the application to not use the Image if it becomes unavailable.
     *
     * @param session_id associated with the Image.
     * @return Image associated with the given session_id or nullptr if no Image exist.
     */
    pub fn image_by_session_id(&self, session_id: i32) -> Option<&Image> {
        let list = self.image_list.load();
        list.iter().find(|img| img.session_id() == session_id)
    }

    /**
     * Get the image at the given index from the images array.
     *
     * This is only valid until the image list changes.
     *
     * @param index in the array
     * @return image at given index or exception if out of range.
     */

    pub fn image_by_index(&mut self, index: usize) -> Option<&mut Image> {
        let list = self.image_list.load_mut();
        list.get_mut(index)
    }

    /**
     * Get a std::vector of active {@link Image}s that match this subscription.
     *
     * @return a std::vector of active {@link Image}s that match this subscription.
     */
    pub fn images(&self) -> &Vec<Image> {
        self.image_list.load()
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Acquire)
    }

    pub fn has_image(&self, correlation_id: i64) -> bool {
        let list = self.image_list.load();
        list.iter().any(|img| img.correlation_id() == correlation_id)
    }

    /// Adds image to the subscription and returns Images
    /// as they were just before adding this Image
    pub fn add_image(&mut self, image: Image) -> Vec<Image> {
        self.image_list.add(image)
    }

    /// Removes image with given correlation_id and returns old Images (as of before removal)
    /// and index of removed element.
    /// Returns None if Image was not removed (e.g. was not found).
    pub fn remove_image(&mut self, correlation_id: i64) -> Option<(Vec<Image>, Index)> {
        self.image_list.remove(|image| {
            if image.correlation_id() == correlation_id {
                image.close();
                true
            } else {
                false
            }
        })
    }

    /// Removes all images and returns old Images if subscription is not closed.
    /// Returns None if subscription is closed.
    pub fn close_and_remove_images(&mut self) -> Option<Vec<Image>> {
        if !self.is_closed.swap(true, Ordering::SeqCst) {
            let image_list = self.image_list.load_val();
            self.image_list.store(Vec::new());
            Some(image_list)
        } else {
            None
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let list = self.image_list.load();

        let _unused = self
            .conductor
            .lock()
            .expect("Mutex poisoned")
            .release_subscription(self.registration_id, list.clone());
    }
}

type EndOfStreamHandler = fn(&Image);
