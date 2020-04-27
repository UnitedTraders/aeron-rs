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

pub const SETUP_DEFN_SIZE: Index = std::mem::size_of::<SetupDefn>() as Index;

/**
 * HeaderFlyweight for Setup Frames
 * <p>
 * <a href="https://github.com/real-logic/Aeron/wiki/Protocol-Specification#stream-setup">Stream Setup</a>
 */

#[repr(C, packed(4))]
#[derive(Copy, Clone)]
pub struct SetupDefn {
    header: HeaderDefn,
    term_offset: i32,
    session_id: i32,
    stream_id: i32,
    initial_term_id: i32,
    action_term_id: i32,
    term_length: i32,
    mtu: i32,
}

#[allow(dead_code)]
pub struct SetupFlyweight {
    header_flyweight: HeaderFlyweight,
    m_struct: *mut SetupDefn, // This is actually part of above field memory space
}

impl SetupFlyweight {
    pub fn new(buffer: AtomicBuffer, offset: Index) -> Self {
        let header_flyweight = HeaderFlyweight::new(buffer, offset);
        let m_struct = header_flyweight.flyweight.overlay_struct::<SetupDefn>(0);
        Self {
            header_flyweight,
            m_struct,
        }
    }

    // Getters
    #[inline]
    pub fn term_offset(&self) -> i32 {
        unsafe { (*self.m_struct).term_offset }
    }

    #[inline]
    pub fn session_id(&self) -> i32 {
        unsafe { (*self.m_struct).session_id }
    }

    #[inline]
    pub fn stream_id(&self) -> i32 {
        unsafe { (*self.m_struct).stream_id }
    }

    #[inline]
    pub fn initial_term_id(&self) -> i32 {
        unsafe { (*self.m_struct).initial_term_id }
    }

    #[inline]
    pub fn action_term_id(&self) -> i32 {
        unsafe { (*self.m_struct).action_term_id }
    }

    #[inline]
    pub fn term_length(&self) -> i32 {
        unsafe { (*self.m_struct).term_length }
    }

    #[inline]
    pub fn mtu(&self) -> i32 {
        unsafe { (*self.m_struct).mtu }
    }

    // Setters
    #[inline]
    pub fn set_term_offset(&mut self, value: i32) {
        unsafe {
            (*self.m_struct).term_offset = value;
        }
    }

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
    pub fn set_initial_term_id(&mut self, value: i32) {
        unsafe {
            (*self.m_struct).initial_term_id = value;
        }
    }

    #[inline]
    pub fn set_action_term_id(&mut self, value: i32) {
        unsafe {
            (*self.m_struct).action_term_id = value;
        }
    }

    #[inline]
    pub fn set_term_length(&mut self, value: i32) {
        unsafe {
            (*self.m_struct).term_length = value;
        }
    }

    #[inline]
    pub fn set_mtu(&mut self, value: i32) {
        unsafe {
            (*self.m_struct).mtu = value;
        }
    }

    #[inline]
    pub const fn header_length() -> Index {
        SETUP_DEFN_SIZE
    }
}
