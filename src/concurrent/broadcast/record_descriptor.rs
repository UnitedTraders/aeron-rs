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

use crate::concurrent::broadcast::BroadcastTransmitError;
use crate::utils::types::Index;

/**
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |R|                          Length                             |
 *  +-+-------------------------------------------------------------+
 *  |                             Type                              |
 *  +---------------------------------------------------------------+
 *  |                       Encoded Message                        ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 **/

const LENGTH_OFFSET: Index = 0;
const TYPE_OFFSET: Index = 4;

pub const HEADER_LENGTH: Index = 8;
pub const RECORD_ALIGNMENT: Index = HEADER_LENGTH;

pub fn calculate_max_message_length(capacity: Index) -> Index {
    capacity / 8
}

pub fn length_offset(record_offset: Index) -> Index {
    record_offset + LENGTH_OFFSET
}

pub fn type_offset(record_offset: Index) -> Index {
    record_offset + TYPE_OFFSET
}

pub fn msg_offset(record_offset: Index) -> Index {
    record_offset + HEADER_LENGTH
}

pub fn check_msg_type_id(msg_type_id: i32) -> Result<(), BroadcastTransmitError> {
    if msg_type_id < 1 {
        return Err(BroadcastTransmitError::MessageIdShouldBeGreaterThenZero(msg_type_id));
    }

    Ok(())
}
