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

/**
* 0                   1                   2                   3
* 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
* +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
* |                         Client ID                             |
* |                                                               |
* +---------------------------------------------------------------+
* |                       Correlation ID                          |
* |                                                               |
* +---------------------------------------------------------------+
*/

#[repr(C, packed(4))]
#[derive(Copy, Clone)]
pub(crate) struct CorrelatedMessageDefn {
    client_id: i64,
    correlation_id: i64,
}

pub const CORRELATED_MESSAGE_LENGTH: Index = std::mem::size_of::<CorrelatedMessageDefn>() as Index;

pub(crate) struct CorrelatedMessageFlyweight {
    pub flyweight: Flyweight<CorrelatedMessageDefn>,
}

impl CorrelatedMessageFlyweight {
    pub fn new(buffer: AtomicBuffer, offset: Index) -> Self {
        Self {
            flyweight: Flyweight::new(buffer, offset),
        }
    }

    // Getters

    #[cfg(test)]
    pub fn correlation_id(&self) -> i64 {
        unsafe { (*self.flyweight.m_struct).correlation_id }
    }

    // Setters

    #[inline]
    pub fn set_client_id(&mut self, value: i64) {
        unsafe {
            (*self.flyweight.m_struct).client_id = value;
        }
    }

    pub fn set_correlation_id(&mut self, value: i64) {
        unsafe {
            (*self.flyweight.m_struct).correlation_id = value;
        }
    }
}
