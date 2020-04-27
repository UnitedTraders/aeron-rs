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

use crate::command::flyweight::Flyweight;
use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::utils::{bit_utils, types::Index, types::I32_SIZE};

/**
* Message to denote that new buffers have been added for a subscription.
*
* NOTE: Layout should be SBE compliant
*
* @see ControlProtocolEvents
*
* 0                   1                   2                   3
* 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
* +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
* |                       Correlation ID                          |
* |                                                               |
* +---------------------------------------------------------------+
* |                         Session ID                            |
* +---------------------------------------------------------------+
* |                         Stream ID                             |
* +---------------------------------------------------------------+
* |                 Subscription Registration Id                  |
* |                                                               |
* +---------------------------------------------------------------+
* |                    Subscriber Position Id                     |
* +---------------------------------------------------------------+
* |                      Log File Length                          |
* +---------------------------------------------------------------+
* |                       Log File Name                          ...
*...                                                              |
* +---------------------------------------------------------------+
* |                    Source identity Length                     |
* +---------------------------------------------------------------+
* |                    Source identity Name                      ...
*...                                                              |
* +---------------------------------------------------------------+
*/

#[repr(C, packed(4))]
#[derive(Copy, Clone)]
pub struct ImageBuffersReadyDefn {
    correlation_id: i64,
    session_id: i32,
    stream_id: i32,
    subscription_registration_id: i64,
    subscriber_position_id: i32,
}

pub const IMAGE_BUFFERS_READY_LENGTH: Index = std::mem::size_of::<ImageBuffersReadyDefn>() as Index;

pub struct ImageBuffersReadyFlyweight {
    flyweight: Flyweight<ImageBuffersReadyDefn>,
}

impl ImageBuffersReadyFlyweight {
    pub fn new(buffer: AtomicBuffer, offset: Index) -> Self {
        Self {
            flyweight: Flyweight::new(buffer, offset),
        }
    }

    // Getters

    #[inline]
    pub fn correlation_id(&self) -> i64 {
        unsafe { (*self.flyweight.m_struct).correlation_id }
    }

    #[inline]
    pub fn session_id(&self) -> i32 {
        unsafe { (*self.flyweight.m_struct).session_id }
    }

    #[inline]
    pub fn stream_id(&self) -> i32 {
        unsafe { (*self.flyweight.m_struct).stream_id }
    }

    #[inline]
    pub fn subscription_registration_id(&self) -> i64 {
        unsafe { (*self.flyweight.m_struct).subscription_registration_id }
    }

    #[inline]
    pub fn subscriber_position_id(&self) -> i32 {
        unsafe { (*self.flyweight.m_struct).subscriber_position_id }
    }

    // Setters

    #[inline]
    pub fn set_correlation_id(&mut self, value: i64) {
        unsafe {
            (*self.flyweight.m_struct).correlation_id = value;
        }
    }

    #[inline]
    pub fn set_session_id(&mut self, value: i32) {
        unsafe {
            (*self.flyweight.m_struct).session_id = value;
        }
    }

    #[inline]
    pub fn set_stream_id(&mut self, value: i32) {
        unsafe {
            (*self.flyweight.m_struct).stream_id = value;
        }
    }

    #[inline]
    pub fn set_subscription_registration_id(&mut self, value: i64) {
        unsafe {
            (*self.flyweight.m_struct).subscription_registration_id = value;
        }
    }

    #[inline]
    pub fn set_subscriber_position_id(&mut self, value: i32) {
        unsafe {
            (*self.flyweight.m_struct).subscriber_position_id = value;
        }
    }

    // Interaction with Flyweight methods

    #[inline]
    pub fn log_file_name(&self) -> CString {
        self.flyweight.string_get(self.log_file_name_offset())
    }

    #[inline]
    pub fn set_log_file_name(&mut self, value: &[u8]) {
        self.flyweight.string_put(self.log_file_name_offset(), value);
    }

    #[inline]
    pub fn source_identity(&self) -> CString {
        self.flyweight.string_get(self.source_identity_offset())
    }

    #[inline]
    pub fn set_source_identity(&mut self, value: &[u8]) {
        self.flyweight.string_put(self.source_identity_offset(), value);
    }

    #[inline]
    pub fn length(&self) -> Index {
        let start_of_source_identity = self.source_identity_offset();
        start_of_source_identity + self.flyweight.string_get_length(start_of_source_identity) + I32_SIZE
    }
}

impl ImageBuffersReadyFlyweight {
    // Private methods

    #[inline]
    const fn log_file_name_offset(&self) -> Index {
        IMAGE_BUFFERS_READY_LENGTH
    }

    #[inline]
    fn source_identity_offset(&self) -> Index {
        let offset = self.log_file_name_offset();
        let alignment = I32_SIZE;
        let log_file_name_length = bit_utils::align(self.flyweight.string_get_length(offset), alignment);

        offset + alignment + log_file_name_length
    }
}
