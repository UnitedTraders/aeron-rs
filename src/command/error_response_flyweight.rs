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
 * Control message flyweight for any errors sent from driver to clients
 *
 * <p>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |              Offending Command Correlation ID                 |
 * |                                                               |
 * +---------------------------------------------------------------+
 * |                         Error Code                            |
 * +---------------------------------------------------------------+
 * |                   Error Message Length                        |
 * +---------------------------------------------------------------+
 * |                       Error Message                          ...
 * ...                                                             |
 * +---------------------------------------------------------------+
 */

#[repr(C, packed(4))]
#[derive(Copy, Clone)]
pub(crate) struct ErrorResponseDefn {
    offending_command_correlation_id: i64,
    error_code: i32,
    error_message_length: i32,
    error_message_data: [i8; 1],
}

pub const ERROR_CODE_UNKNOWN_CODE_VALUE: i32 = -1;

pub const ERROR_CODE_GENERIC_ERROR: i32 = 0;
pub const ERROR_CODE_INVALID_CHANNEL: i32 = 1;
pub const ERROR_CODE_UNKNOWN_SUBSCRIPTION: i32 = 2;
pub const ERROR_CODE_UNKNOWN_PUBLICATION: i32 = 3;
pub const ERROR_CODE_CHANNEL_ENDPOINT_ERROR: i32 = 4;
pub const ERROR_CODE_UNKNOWN_COUNTER: i32 = 5;
pub const ERROR_CODE_UNKNOWN_COMMAND_TYPE_ID: i32 = 6;
pub const ERROR_CODE_MALFORMED_COMMAND: i32 = 7;
pub const ERROR_CODE_NOT_SUPPORTED: i32 = 8;

pub struct ErrorResponseFlyweight {
    flyweight: Flyweight<ErrorResponseDefn>,
}

impl ErrorResponseFlyweight {
    pub fn new(buffer: AtomicBuffer, offset: Index) -> Self {
        Self {
            flyweight: Flyweight::new(buffer, offset),
        }
    }

    #[inline]
    pub fn offending_command_correlation_id(&self) -> i64 {
        unsafe { (*self.flyweight.m_struct).offending_command_correlation_id }
    }

    #[inline]
    pub fn error_code(&self) -> i32 {
        unsafe { (*self.flyweight.m_struct).error_code }
    }

    #[inline]
    pub fn error_message(&self) -> CString {
        self.flyweight
            .string_get(offset_of!(ErrorResponseDefn, error_message_length) as Index)
    }

    #[inline]
    pub fn length(&self) -> Index {
        unsafe {
            offset_of!(ErrorResponseDefn, error_message_data) as Index + (*self.flyweight.m_struct).error_message_length as Index
        }
    }
}
