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
use crate::utils::types::Index;

/**
* Message to denote that new buffers have been added for a publication.
*
* @see ControlProtocolEvents
*
* 0                   1                   2                   3
* 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
* +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
* |                      Correlation ID                           |
* |                                                               |
* +---------------------------------------------------------------+
* |                      Registration ID                          |
* |                                                               |
* +---------------------------------------------------------------+
* |                        Session ID                             |
* +---------------------------------------------------------------+
* |                         Stream ID                             |
* +---------------------------------------------------------------+
* |                  Position Limit Counter Id                    |
* +---------------------------------------------------------------+
* |                  Channel Status Indicator ID                  |
* +---------------------------------------------------------------+
* |                       Log File Length                         |
* +---------------------------------------------------------------+
* |                        Log File Name                         ...
* ...                                                             |
* +---------------------------------------------------------------+
*/

#[repr(C, packed(4))]
#[derive(Copy, Clone, Debug)]
pub(crate) struct PublicationBuffersReadyDefn {
    correlation_id: i64,
    registration_id: i64,
    session_id: i32,
    stream_id: i32,
    position_limit_counter_id: i32,
    channel_status_indicator_id: i32,
    log_file_length: i32,
    log_file_data: [i8; 1],
}

pub(crate) struct PublicationBuffersReadyFlyweight {
    flyweight: Flyweight<PublicationBuffersReadyDefn>,
}

impl PublicationBuffersReadyFlyweight {
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
    pub fn registration_id(&self) -> i64 {
        unsafe { (*self.flyweight.m_struct).registration_id }
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
    pub fn position_limit_counter_id(&self) -> i32 {
        unsafe { (*self.flyweight.m_struct).position_limit_counter_id }
    }

    #[inline]
    pub fn channel_status_indicator_id(&self) -> i32 {
        unsafe { (*self.flyweight.m_struct).channel_status_indicator_id }
    }

    // Interaction with Flyweight methods
    #[inline]
    pub fn log_file_name(&self) -> CString {
        self.flyweight
            .string_get(offset_of!(PublicationBuffersReadyDefn, log_file_length) as Index)
    }
}
