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

use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::concurrent::reports::loss_report_descriptor::LossReportEntryDefn;
use crate::offset_of;
use crate::utils::bit_utils;
use crate::utils::bit_utils::CACHE_LINE_LENGTH;
use crate::utils::types::{Index, I32_SIZE};
use lazy_static::lazy_static;
use std::ffi::CString;

/**
 * A report of loss events on a message stream.
 * <p>
 * The provided AtomicBuffer can wrap a memory-mapped file so logging can be out of process. This provides
 * the benefit that if a crash or lockup occurs then the log can be read externally without loss of data.
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |R|                    Observation Count                        |
 *  |                                                               |
 *  +-+-------------------------------------------------------------+
 *  |R|                     Total Bytes Lost                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                 First Observation Timestamp                   |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                  Last Observation Timestamp                   |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                          Session ID                           |
 *  +---------------------------------------------------------------+
 *  |                           Stream ID                           |
 *  +---------------------------------------------------------------+
 *  |                 Channel encoded in US-ASCII                  ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                  Source encoded in US-ASCII                  ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */

mod loss_report_descriptor {
    use crate::utils::types::Index;

    #[repr(C, packed(4))]
    #[derive(Copy, Clone)]
    pub struct LossReportEntryDefn {
        pub observation_count: i64,
        total_bytes_lost: i64,
        first_observation_timestamp: i64,
        last_observation_timestamp: i64,
        session_id: i32,
        stream_id: i32,
    }

    pub(crate) const CHANNEL_OFFSET: Index = std::mem::size_of::<LossReportEntryDefn>() as Index;
    //
    const LOSS_REPORT_FILE_NAME: &str = "loss-report.dat";
    //
    // inline static std::string file(std::string& aeronDirectoryName)
    // {
    // #if defined(_MSC_VER)
    // return aeronDirectoryName + "\\" + LOSS_REPORT_FILE_NAME;
    // #else
    // return aeronDirectoryName + "/" + LOSS_REPORT_FILE_NAME;
    // #endif
    // }
}

lazy_static! {
    pub static ref OBSERVATION_COUNT_OFFSET: Index = offset_of!(LossReportEntryDefn, observation_count);
    pub static ref ENTRY_ALIGNMENT: Index = std::mem::size_of_val(&CACHE_LINE_LENGTH) as Index;
}

pub type LossConsumerHandler = fn(i64, LossReportEntryDefn, CString /*channel*/, CString /*source*/);

/**
 * Read a LossReport contained in the buffer. This can be done concurrently.
 *
 * @param buffer        containing the loss report.
 * @param consumer to be called to accept each entry in the report.
 * @return the number of entries read.
 */
#[inline]
fn read(buffer: &AtomicBuffer, consumer: LossConsumerHandler) -> i32 {
    let mut records_read = 0;
    let mut offset = 0;
    let capacity = buffer.capacity();

    while offset < capacity {
        let observation_count: i64 = buffer.get_volatile::<i64>(offset + *OBSERVATION_COUNT_OFFSET);

        if 0 == observation_count {
            break;
        }

        records_read += 1;

        let channel = buffer.get_string(offset + loss_report_descriptor::CHANNEL_OFFSET);
        let channel_length = channel.as_bytes().len() as Index;
        let source = buffer
            .get_string(offset + loss_report_descriptor::CHANNEL_OFFSET + std::mem::size_of::<i32>() as Index + channel_length);
        let source_length = source.as_bytes().len() as Index;

        let record = buffer.get::<loss_report_descriptor::LossReportEntryDefn>(offset);

        consumer(observation_count, record, channel, source);

        let record_length = loss_report_descriptor::CHANNEL_OFFSET + I32_SIZE * 2 + channel_length + source_length;

        offset += bit_utils::align(record_length, *ENTRY_ALIGNMENT);
    }

    return records_read;
}
