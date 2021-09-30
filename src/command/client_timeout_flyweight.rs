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
 * Message to denote that a client has timed out wrt the driver.
 *
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                         Client ID                             |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 */

#[repr(C, packed(4))]
#[derive(Copy, Clone)]
pub(crate) struct ClientTimeoutDefn {
    client_id: i64,
}

pub const CLIENT_TIMEOUT_LENGTH: Index = std::mem::size_of::<ClientTimeoutDefn>() as Index;

pub(crate) struct ClientTimeoutFlyweight {
    flyweight: Flyweight<ClientTimeoutDefn>,
}

impl ClientTimeoutFlyweight {
    pub fn new(buffer: AtomicBuffer, offset: Index) -> Self {
        Self {
            flyweight: Flyweight::new(buffer, offset),
        }
    }

    #[inline]
    pub fn client_id(&self) -> i64 {
        unsafe { (*self.flyweight.m_struct).client_id }
    }
}
