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
    utils::{
        bit_utils,
        types::{Index, I32_SIZE},
    },
};

/**
 * Message to denote a new counter.
 *
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                        Correlation ID                         |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                       Counter Type ID                         |
 *  +---------------------------------------------------------------+
 *  |                         Key Length                            |
 *  +---------------------------------------------------------------+
 *  |                         Key Buffer                           ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                        Label Length                           |
 *  +---------------------------------------------------------------+
 *  |                        Label (ASCII)                         ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 */

#[repr(C, packed(4))]
#[derive(Copy, Clone)]
pub(crate) struct CounterMessageDefn {
    correlated_message: CorrelatedMessageDefn,
    type_id: i32,
}

pub const COUNTER_MESSAGE_LENGTH: Index = std::mem::size_of::<CounterMessageDefn>() as Index;

pub(crate) struct CounterMessageFlyweight {
    correlated_message_flyweight: CorrelatedMessageFlyweight,
    m_struct: *mut CounterMessageDefn, // This is actually part of above field memory space
}

impl CounterMessageFlyweight {
    pub fn new(buffer: AtomicBuffer, offset: Index) -> Self {
        let correlated_message_flyweight = CorrelatedMessageFlyweight::new(buffer, offset);
        let m_struct = correlated_message_flyweight.flyweight.overlay_struct::<CounterMessageDefn>(0);
        Self {
            correlated_message_flyweight,
            m_struct,
        }
    }

    #[cfg(test)]
    pub fn type_id(&self) -> i32 {
        unsafe { (*self.m_struct).type_id }
    }

    #[inline]
    pub fn set_type_id(&mut self, value: i32) {
        unsafe {
            (*self.m_struct).type_id = value;
        }
    }

    #[inline]
    pub unsafe fn set_key_buffer(&mut self, key: *const u8, key_length: Index) {
        self.correlated_message_flyweight
            .flyweight
            .put::<i32>(self.key_length_offset(), key_length);

        if key_length > 0 {
            self.correlated_message_flyweight.flyweight.put_bytes(
                self.key_length_offset() + I32_SIZE,
                ::std::slice::from_raw_parts(key, key_length as usize),
            );
        }
    }

    #[inline]
    pub fn key_length(&self) -> i32 {
        let mut length: i32 = 0;

        self.correlated_message_flyweight.flyweight.get_bytes(
            self.key_length_offset(),
            &mut length as *mut i32 as *mut u8,
            I32_SIZE,
        );

        length
    }

    #[inline]
    pub fn label_length(&self) -> Index {
        self.correlated_message_flyweight
            .flyweight
            .string_get_length(self.label_length_offset())
    }

    #[cfg(test)]
    pub fn label(&self) -> CString {
        self.correlated_message_flyweight
            .flyweight
            .string_get(self.label_length_offset())
    }

    #[inline]
    pub fn set_label(&mut self, label: &[u8]) {
        self.correlated_message_flyweight
            .flyweight
            .string_put(self.label_length_offset(), label);
    }

    #[inline]
    pub fn length(&self) -> Index {
        self.label_length_offset() + I32_SIZE + self.label_length() as Index
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

impl CounterMessageFlyweight {
    // Private methods

    #[inline]
    const fn key_length_offset(&self) -> Index {
        COUNTER_MESSAGE_LENGTH
    }

    #[inline]
    fn label_length_offset(&self) -> Index {
        let offset = self.key_length_offset();
        let unaligned_key_length = self.key_length() as Index;
        let alignment = I32_SIZE;
        let aligned_key_length = bit_utils::align(unaligned_key_length, alignment);

        offset + alignment + aligned_key_length
    }
}
