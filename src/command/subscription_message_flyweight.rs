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

use crate::command::correlated_message_flyweight::{CorrelatedMessageDefn, CorrelatedMessageFlyweight};
use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::offset_of;
use crate::utils::types::Index;

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
struct SubscriptionMessageDefn {
    correlated_message: CorrelatedMessageDefn,
    registration_correlation_id: i64,
    stream_id: i32,
    channel_length: i32,
    channel_data: *mut i8,
}

struct PublicationMessageFlyweight {
    correlated_message_flyweight: CorrelatedMessageFlyweight,
    m_struct: SubscriptionMessageDefn,
}

impl PublicationMessageFlyweight {
    pub fn new(buffer: AtomicBuffer, offset: Index) -> Self {
        let correlated_message_flyweight = CorrelatedMessageFlyweight::new(buffer, offset);
        let m_struct = correlated_message_flyweight.flyweight.get::<SubscriptionMessageDefn>(0);
        Self {
            correlated_message_flyweight,
            m_struct,
        }
    }

    #[inline]
    pub fn registration_correlation_id(&self) -> i64 {
        self.m_struct.registration_correlation_id
    }

    #[inline]
    pub fn stream_id(&self) -> i32 {
        self.m_struct.stream_id
    }

    #[inline]
    pub fn set_registration_correlation_id(&mut self, value: i64) {
        self.m_struct.registration_correlation_id = value;
    }

    #[inline]
    pub fn set_stream_id(&mut self, value: i32) {
        self.m_struct.stream_id = value;
    }

    #[inline]
    pub fn channel(&self) -> CString {
        self.correlated_message_flyweight
            .flyweight
            .string_get(offset_of!(SubscriptionMessageDefn, channel_length))
    }

    #[inline]
    pub fn set_channel(&mut self, value: &[u8]) {
        self.correlated_message_flyweight
            .flyweight
            .string_put(offset_of!(SubscriptionMessageDefn, channel_length), value);
    }

    #[inline]
    pub fn length(&self) -> Index {
        offset_of!(SubscriptionMessageDefn, channel_data) + self.m_struct.channel_length as Index
    }
}
