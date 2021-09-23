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
 * Message to denote that a Subscription has been successfully set up.
 *
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                        Correlation ID                         |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                  Channel Status Indicator ID                  |
 *  +---------------------------------------------------------------+
 */

#[repr(C, packed(4))]
#[derive(Copy, Clone)]
pub(crate) struct SubscriptionReadyDefn {
    correlation_id: i64,
    channel_status_indicator_id: i32,
}

pub const SUBSCRIPTION_READY_LENGTH: Index = std::mem::size_of::<SubscriptionReadyDefn>() as Index;

pub(crate) struct SubscriptionReadyFlyweight {
    flyweight: Flyweight<SubscriptionReadyDefn>,
}

impl SubscriptionReadyFlyweight {
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
    pub fn channel_status_indicator_id(&self) -> i32 {
        unsafe { (*self.flyweight.m_struct).channel_status_indicator_id }
    }
}
