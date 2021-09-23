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

use lazy_static::lazy_static;

use crate::utils::types::{Index, I64_SIZE};

/**
 * Distinct record of error observations. Rather than grow a record indefinitely when many errors of the same type
 * are logged, this log takes the approach of only recording distinct errors of the same type type and stack trace
 * and keeping a count and time of observation so that the record only grows with new distinct observations.
 *
 * The provided {@link AtomicBuffer} can wrap a memory-mapped file so logging can be out of process. This provides
 * the benefit that if a crash or lockup occurs then the log can be read externally without loss of data.
 *
 * This class is threadsafe to be used from multiple logging threads.
 *
 * The error records are recorded to the memory mapped buffer in the following format.
 *
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |R|                         Length                              |
 *  +-+-------------------------------------------------------------+
 *  |R|                     Observation Count                       |
 *  +-+-------------------------------------------------------------+
 *  |R|                Last Observation Timestamp                   |
 *  |                                                               |
 *  +-+-------------------------------------------------------------+
 *  |R|               First Observation Timestamp                   |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                     ASCII Encoded Error                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */

#[repr(C, packed(4))]
#[derive(Copy, Clone)]
pub struct ErrorLogEntryDefn {
    pub length: i32,
    pub observation_count: i32,
    pub last_observation_timestamp: i64,
    pub first_observation_timestamp: i64,
}

lazy_static! {
    pub static ref LENGTH_OFFSET: Index = offset_of!(ErrorLogEntryDefn, length) as Index;
    pub static ref OBSERVATION_COUNT_OFFSET: Index = offset_of!(ErrorLogEntryDefn, observation_count) as Index;
    pub static ref LAST_OBSERVATION_TIMESTAMP_OFFSET: Index = offset_of!(ErrorLogEntryDefn, last_observation_timestamp) as Index;
    pub static ref FIRST_OBSERVATION_TIMESTAMP_OFFSET: Index =
        offset_of!(ErrorLogEntryDefn, first_observation_timestamp) as Index;
}

pub const ENCODED_ERROR_OFFSET: Index = std::mem::size_of::<ErrorLogEntryDefn>() as Index;
pub const HEADER_LENGTH: Index = std::mem::size_of::<ErrorLogEntryDefn>() as Index;

pub const RECORD_ALIGNMENT: Index = I64_SIZE;
