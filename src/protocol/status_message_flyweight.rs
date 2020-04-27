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

use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::protocol::header_flyweight::{HeaderDefn, HeaderFlyweight};
use crate::utils::types::Index;

pub const STATUS_MESSAGE_DEFN_SIZE: Index = std::mem::size_of::<StatusMessageDefn>() as Index;

/**
 * Flow/Congestion control message to send feedback from subscriptions to publications.
 *
 * <p>
 *    0                   1                   2                   3
 *    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *   |R|                 Frame Length (=header + data)               |
 *   +---------------+-+-------------+-------------------------------+
 *   |   Version     |S|    Flags    |          Type (=0x03)         |
 *   +---------------+-+-------------+-------------------------------+
 *   |                          Session ID                           |
 *   +---------------------------------------------------------------+
 *   |                           Stream ID                           |
 *   +---------------------------------------------------------------+
 *   |                      Consumption Term ID                      |
 *   +---------------------------------------------------------------+
 *   |R|                  Consumption Term Offset                    |
 *   +---------------------------------------------------------------+
 *   |                        Receiver Window                        |
 *   +---------------------------------------------------------------+
 *   |                  Application Specific Feedback               ...
 *  ...                                                              |
 *   +---------------------------------------------------------------+
 */

#[repr(C, packed(4))]
#[derive(Copy, Clone)]
pub struct StatusMessageDefn {
    header: HeaderDefn,
    session_id: i32,
    stream_id: i32,
    consumption_term_id: i32,
    consumption_term_offset: i32,
    receiver_window: i32,
}

#[allow(dead_code)]
pub struct StatusMessageFlyweight {
    header_flyweight: HeaderFlyweight,
    m_struct: *mut StatusMessageDefn, // This is actually part of above field memory space
}

impl StatusMessageFlyweight {
    pub fn new(buffer: AtomicBuffer, offset: Index) -> Self {
        let header_flyweight = HeaderFlyweight::new(buffer, offset);
        let m_struct = header_flyweight.flyweight.overlay_struct::<StatusMessageDefn>(0);
        Self {
            header_flyweight,
            m_struct,
        }
    }

    // Getters
    #[inline]
    pub fn session_id(&self) -> i32 {
        unsafe { (*self.m_struct).session_id }
    }

    #[inline]
    pub fn stream_id(&self) -> i32 {
        unsafe { (*self.m_struct).stream_id }
    }

    #[inline]
    pub fn consumption_term_id(&self) -> i32 {
        unsafe { (*self.m_struct).consumption_term_id }
    }

    #[inline]
    pub fn consumption_term_offset(&self) -> i32 {
        unsafe { (*self.m_struct).consumption_term_offset }
    }

    #[inline]
    pub fn receiver_window(&self) -> i32 {
        unsafe { (*self.m_struct).receiver_window }
    }

    // Setters
    #[inline]
    pub fn set_session_id(&mut self, value: i32) {
        unsafe {
            (*self.m_struct).session_id = value;
        }
    }

    #[inline]
    pub fn set_stream_id(&mut self, value: i32) {
        unsafe {
            (*self.m_struct).stream_id = value;
        }
    }

    #[inline]
    pub fn set_consumption_term_id(&mut self, value: i32) {
        unsafe {
            (*self.m_struct).consumption_term_id = value;
        }
    }

    #[inline]
    pub fn set_consumption_term_offset(&mut self, value: i32) {
        unsafe {
            (*self.m_struct).consumption_term_offset = value;
        }
    }

    #[inline]
    pub fn set_receiver_window(&mut self, value: i32) {
        unsafe {
            (*self.m_struct).receiver_window = value;
        }
    }

    #[inline]
    pub const fn header_length() -> Index {
        STATUS_MESSAGE_DEFN_SIZE
    }
}
