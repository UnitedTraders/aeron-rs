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
#[cfg(test)]
use std::ffi::CString;

use crate::{
    command::correlated_message_flyweight::{CorrelatedMessageDefn, CorrelatedMessageFlyweight},
    concurrent::atomic_buffer::AtomicBuffer,
    utils::types::Index,
};

/**
* Control message for adding a subscription.
*
* <p>
* 0                   1                   2                   3
* 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
* +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
* |                         Client ID                             |
* |                                                               |
* +---------------------------------------------------------------+
* |                       Correlation ID                          |
* |                                                               |
* +---------------------------------------------------------------+
* |                 Registration Correlation ID                   |
* |                                                               |
* +---------------------------------------------------------------+
* |                         Stream Id                             |
* +---------------------------------------------------------------+
* |                      Channel Length                           |
* +---------------------------------------------------------------+
* |                          Channel                             ...
* ...                                                             |
* +---------------------------------------------------------------+
*/

#[repr(C, packed(4))]
#[derive(Copy, Clone)]
pub(crate) struct SubscriptionMessageDefn {
    correlated_message: CorrelatedMessageDefn,
    registration_correlation_id: i64,
    stream_id: i32,
    channel_length: i32,
    channel_data: [i8; 1],
}

pub(crate) struct SubscriptionMessageFlyweight {
    correlated_message_flyweight: CorrelatedMessageFlyweight,
    m_struct: *mut SubscriptionMessageDefn,
}

impl SubscriptionMessageFlyweight {
    pub fn new(buffer: AtomicBuffer, offset: Index) -> Self {
        let correlated_message_flyweight = CorrelatedMessageFlyweight::new(buffer, offset);
        let m_struct = correlated_message_flyweight
            .flyweight
            .overlay_struct::<SubscriptionMessageDefn>(0);
        Self {
            correlated_message_flyweight,
            m_struct,
        }
    }

    #[cfg(test)]
    pub fn stream_id(&self) -> i32 {
        unsafe { (*self.m_struct).stream_id }
    }

    #[inline]
    pub fn set_registration_correlation_id(&mut self, value: i64) {
        unsafe {
            (*self.m_struct).registration_correlation_id = value;
        }
    }

    #[inline]
    pub fn set_stream_id(&mut self, value: i32) {
        unsafe {
            (*self.m_struct).stream_id = value;
        }
    }

    #[cfg(test)]
    pub fn channel(&self) -> CString {
        self.correlated_message_flyweight
            .flyweight
            .string_get(offset_of!(SubscriptionMessageDefn, channel_length) as Index)
    }

    #[inline]
    pub fn set_channel(&mut self, value: &[u8]) {
        self.correlated_message_flyweight
            .flyweight
            .string_put(offset_of!(SubscriptionMessageDefn, channel_length) as Index, value);
    }

    #[inline]
    pub fn length(&self) -> Index {
        unsafe { offset_of!(SubscriptionMessageDefn, channel_data) as Index + (*self.m_struct).channel_length as Index }
    }

    // Parent Getters
    #[cfg(test)]
    pub fn correlation_id(&self) -> i64 {
        self.correlated_message_flyweight.correlation_id()
    }

    // Parent Setters

    #[inline]
    pub fn set_client_id(&mut self, value: i64) {
        self.correlated_message_flyweight.set_client_id(value);
    }

    #[inline]
    pub fn set_correlation_id(&mut self, value: i64) {
        self.correlated_message_flyweight.set_correlation_id(value);
    }
}
