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

use crate::concurrent::counters::{self, CountersReader};

/**
 * The heartbeat as a timestamp for an entity to indicate liveness.
 * <p>
 * Key has the following layout:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                       Registration ID                         |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 * </pre>
 */

/// Counter type id of a client heartbeat timestamp
pub const CLIENT_HEARTBEAT_TYPE_ID: i32 = 11;

#[derive(Copy, Clone)]
#[repr(C, packed(4))]
struct HeartbeatTimestampKeyDefn {
    registration_id: i64,
}

/**
 * Find the active counter id for an entity with a type id and registration id.
 *
 * @param counters_reader to search within.
 * @param counter_type_id  to match against.
 * @param registration_id for the active entity.
 * @return the counter id if found otherwise None.
 */
pub fn find_counter_id_by_registration_id(
    counters_reader: &CountersReader,
    counter_type_id: i32,
    registration_id: i64,
) -> Option<i32> {
    let buffer = counters_reader.meta_data_buffer();

    for i in 0..counters_reader.max_counter_id() {
        if counters_reader.counter_state(i).expect("Error getting counter state") == counters::RECORD_ALLOCATED {
            let record_offset = CountersReader::metadata_offset(i);
            let key = buffer.get::<HeartbeatTimestampKeyDefn>(record_offset + *counters::KEY_OFFSET);

            if registration_id == key.registration_id
                && buffer.get::<i32>(record_offset + *counters::TYPE_ID_OFFSET) == counter_type_id
            {
                return Some(i);
            }
        }
    }

    None
}

/**
 * Is the counter still active.
 *
 * @param counters_reader to search within.
 * @param counter_id      to search for.
 * @param counter_type_id  to match for counter type.
 * @param registration_id to match the entity key.
 * @return true if the counter is still active otherwise false.
 */
pub fn is_active(counters_reader: &CountersReader, counter_id: i32, counter_type_id: i32, registration_id: i64) -> bool {
    let buffer = counters_reader.meta_data_buffer();
    let record_offset = CountersReader::metadata_offset(counter_id);
    let key = buffer.get::<HeartbeatTimestampKeyDefn>(record_offset + *counters::KEY_OFFSET);

    registration_id == key.registration_id
        && buffer.get::<i32>(record_offset + *counters::TYPE_ID_OFFSET) == counter_type_id
        && counters_reader
            .counter_state(counter_id)
            .expect("Error getting counter state")
            == counters::RECORD_ALLOCATED
}
