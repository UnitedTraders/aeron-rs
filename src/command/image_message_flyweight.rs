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
use crate::offset_of;
use crate::utils::types::Index;

/**
* Control message flyweight for any message that needs to represent an image
*
* <p>
* 0                   1                   2                   3
* 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
* +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
* |                        Correlation ID                         |
* |                                                               |
* +---------------------------------------------------------------+
* |                 Subscription Registration ID                  |
* |                                                               |
* +---------------------------------------------------------------+
* |                          Stream ID                            |
* +---------------------------------------------------------------+
* |                        Channel Length                         |
* +---------------------------------------------------------------+
* |                           Channel                           ...
* ...                                                             |
* +---------------------------------------------------------------+
*/

#[repr(C, packed(4))]
#[derive(Copy, Clone)]
struct ImageMessageDefn {
    correlation_id: i64,
    subscription_registration_id: i64,
    stream_id: i32,
    channel_length: i32,
    channel_data: [i8; 1],
}

pub struct ImageMessageFlyweight {
    flyweight: Flyweight<ImageMessageDefn>,
}

impl ImageMessageFlyweight {
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
    pub fn subscription_registration_id(&self) -> i64 {
        unsafe { (*self.flyweight.m_struct).subscription_registration_id }
    }

    #[inline]
    pub fn stream_id(&self) -> i32 {
        unsafe { (*self.flyweight.m_struct).stream_id }
    }

    #[inline]
    pub fn channel(&self) -> CString {
        self.flyweight.string_get(offset_of!(ImageMessageDefn, channel_length))
    }

    #[inline]
    pub fn length(&self) -> Index {
        unsafe { offset_of!(ImageMessageDefn, channel_data) + (*self.flyweight.m_struct).channel_length as Index }
    }

    // Setters

    #[inline]
    pub fn set_correlation_id(&mut self, value: i64) {
        unsafe {
            (*self.flyweight.m_struct).correlation_id = value;
        }
    }

    #[inline]
    pub fn set_subscription_registration_id(&mut self, value: i64) {
        unsafe {
            (*self.flyweight.m_struct).subscription_registration_id = value;
        }
    }

    #[inline]
    pub fn set_stream_id(&mut self, value: i32) {
        unsafe {
            (*self.flyweight.m_struct).stream_id = value;
        }
    }

    #[inline]
    pub fn set_channel(&mut self, value: &[u8]) {
        self.flyweight.string_put(offset_of!(ImageMessageDefn, channel_length), value);
    }
}
