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

use crate::utils::bit_utils::is_power_of_two;
use crate::utils::bit_utils::CACHE_LINE_LENGTH;
use crate::utils::types::Index;
use crate::utils::types::SZ_I64;

pub const TAIL_INTENT_COUNTER_OFFSET: Index = 0;
pub const TAIL_COUNTER_OFFSET: Index = TAIL_INTENT_COUNTER_OFFSET + SZ_I64 as Index;
pub const LATEST_COUNTER_OFFSET: Index = TAIL_COUNTER_OFFSET + SZ_I64 as Index;
pub const TRAILER_LENGTH: Index = CACHE_LINE_LENGTH * 2;

pub fn check_capacity(capacity: Index) {
    if !is_power_of_two(capacity) {
        panic!(
            "capacity must be a positive power of 2 + TRAILER_LENGTH: capacity={}",
            capacity
        );
    }
}
