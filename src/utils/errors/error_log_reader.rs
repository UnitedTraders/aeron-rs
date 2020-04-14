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

use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::utils::{bit_utils::align, errors::error_log_descriptor};

/**
Handler have four arguments:
    observation_count: i32,
    first_observation_timestamp: i64,
    last_observation_timestamp: i64,
    encoded_exception: CString

**/
#[inline]
pub fn read<T>(buffer: AtomicBuffer, mut consumer: T, since_timestamp: i64) -> i32
where
    T: FnMut(i32, i64, i64, CString),
{
    let mut entries = 0;
    let mut offset = 0;
    let capacity = buffer.capacity();

    while offset < capacity {
        let length = buffer.get_volatile::<i32>(offset + *error_log_descriptor::LENGTH_OFFSET);

        if length == 0 {
            break;
        }

        let last_observation_timestamp =
            buffer.get_volatile::<i64>(offset + *error_log_descriptor::LAST_OBSERVATION_TIMESTAMP_OFFSET);

        if last_observation_timestamp >= since_timestamp {
            let entry = buffer.overlay_struct::<error_log_descriptor::ErrorLogEntryDefn>(offset);

            entries += 1;

            unsafe {
                let entry = *entry;
                consumer(
                    entry.observation_count,
                    entry.first_observation_timestamp,
                    last_observation_timestamp,
                    buffer.get_string_without_length(
                        offset + error_log_descriptor::ENCODED_ERROR_OFFSET,
                        length - error_log_descriptor::HEADER_LENGTH,
                    ),
                );
            }
        }

        offset += align(length, error_log_descriptor::RECORD_ALIGNMENT);
    }
    entries
}
