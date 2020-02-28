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

use crate::command::flyweight::Flyweight;
use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::utils::types::Index;

const HEADER_DEFN_SIZE: Index = std::mem::size_of::<HeaderDefn>() as Index;

/**
 * Flyweight for command header fields.
 */
#[repr(C, packed(4))]
#[derive(Copy, Clone)]
pub(crate) struct HeaderDefn {
    frame_length: i32,
    version: i8,
    flags: i8,
    h_type: i16,
}

pub(crate) struct HeaderFlyweight {
    pub(crate) flyweight: Flyweight<HeaderDefn>,
}

impl HeaderFlyweight {
    pub fn new(buffer: AtomicBuffer, offset: Index) -> Self {
        Self {
            flyweight: Flyweight::new(buffer, offset),
        }
    }

    // Getters
    #[inline]
    pub fn frame_length(&self) -> i32 {
        unsafe { (*self.flyweight.m_struct).frame_length }
    }

    #[inline]
    pub fn version(&self) -> i8 {
        unsafe { (*self.flyweight.m_struct).version }
    }

    #[inline]
    pub fn flags(&self) -> i8 {
        unsafe { (*self.flyweight.m_struct).flags }
    }

    #[inline]
    pub fn h_type(&self) -> i16 {
        unsafe { (*self.flyweight.m_struct).h_type }
    }

    // Setters
    #[inline]
    pub fn set_frame_length(&mut self, value: i32) {
        unsafe {
            (*self.flyweight.m_struct).frame_length = value;
        }
    }

    #[inline]
    pub fn set_version(&mut self, value: i8) {
        unsafe {
            (*self.flyweight.m_struct).version = value;
        }
    }

    #[inline]
    pub fn set_flags(&mut self, value: i8) {
        unsafe {
            (*self.flyweight.m_struct).flags = value;
        }
    }

    #[inline]
    pub fn set_h_type(&mut self, value: i16) {
        unsafe {
            (*self.flyweight.m_struct).h_type = value;
        }
    }

    #[inline]
    pub const fn header_length() -> Index {
        HEADER_DEFN_SIZE
    }

    /** header type PAD */
    pub const HDR_TYPE_PAD: i32 = 0x00;
    /** header type DATA */
    pub const HDR_TYPE_DATA: i32 = 0x01;
    /** header type NAK */
    pub const HDR_TYPE_NAK: i32 = 0x02;
    /** header type SM */
    pub const HDR_TYPE_SM: i32 = 0x03;
    /** header type ERR */
    pub const HDR_TYPE_ERR: i32 = 0x04;
    /** header type SETUP */
    pub const HDR_TYPE_SETUP: i32 = 0x05;
    /** header type EXT */
    pub const HDR_TYPE_EXT: i32 = 0xFFFF;

    pub const CURRENT_VERSION: i8 = 0x0;
}
