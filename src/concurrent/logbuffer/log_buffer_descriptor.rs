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

use crate::utils::errors::IllegalStateError;
use crate::{
    concurrent::atomic_buffer::AtomicBuffer,
    utils::{
        bit_utils::is_power_of_two,
        errors::AeronError,
        misc::CACHE_LINE_LENGTH,
        types::{Index, I32_SIZE, I64_SIZE},
    },
};

pub const TERM_MIN_LENGTH: Index = 64 * 1024;
pub const TERM_MAX_LENGTH: Index = 1024 * 1024 * 1024;
pub const AERON_PAGE_MIN_SIZE: Index = 4 * 1024;
pub const AERON_PAGE_MAX_SIZE: Index = 1024 * 1024 * 1024;

pub const PARTITION_COUNT: Index = 3;

/**
 * Layout description for log buffers which contains partitions of terms with associated term meta data,
 * plus ending with overall log meta data.
 *
 * <pre>
 *  +----------------------------+
 *  |           Term 0           |
 *  +----------------------------+
 *  |           Term 1           |
 *  +----------------------------+
 *  |           Term 2           |
 *  +----------------------------+
 *  |        Log Meta Data       |
 *  +----------------------------+
 * </pre>
 */

pub const LOG_META_DATA_SECTION_INDEX: Index = PARTITION_COUNT;

/**
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                       Tail Counter 0                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                       Tail Counter 1                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                       Tail Counter 2                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                      Active Term Count                        |
 *  +---------------------------------------------------------------+
 *  |                      Cache Line Padding                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                    End of Stream Position                     |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        Is Connected                           |
 *  +---------------------------------------------------------------+
 *  |                    Active Transport Count                     |
 *  +---------------------------------------------------------------+
 *  |                      Cache Line Padding                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                 Registration / Correlation ID                 |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        Initial Term Id                        |
 *  +---------------------------------------------------------------+
 *  |                  Default Frame Header Length                  |
 *  +---------------------------------------------------------------+
 *  |                          MTU Length                           |
 *  +---------------------------------------------------------------+
 *  |                         Term Length                           |
 *  +---------------------------------------------------------------+
 *  |                          Page Size                            |
 *  +---------------------------------------------------------------+
 *  |                      Cache Line Padding                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                    Default Frame Header                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */

pub const LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH: Index = CACHE_LINE_LENGTH * 2;

#[repr(C, packed(4))]
pub struct LogMetaDataDefn {
    term_tail_counters: [i64; PARTITION_COUNT as usize],
    active_term_count: i32,
    pad1: [u8; ((2 * CACHE_LINE_LENGTH) - ((PARTITION_COUNT * I64_SIZE) + I32_SIZE)) as usize],
    end_of_stream_position: i64,
    is_connected: i32,
    active_transport_count: i32,
    pad2: [u8; ((2 * CACHE_LINE_LENGTH) - (I64_SIZE + (2 * I32_SIZE))) as usize],
    correlation_id: i64,
    initial_term_id: i32,
    default_frame_header_length: i32,
    mtu_length: i32,
    term_length: i32,
    page_size: i32,
    pad3: [u8; (CACHE_LINE_LENGTH - (7 * I32_SIZE)) as usize],
}

lazy_static! {
    pub static ref TERM_TAIL_COUNTER_OFFSET: Index = offset_of!(LogMetaDataDefn, term_tail_counters) as Index;
    pub static ref LOG_ACTIVE_TERM_COUNT_OFFSET: Index = offset_of!(LogMetaDataDefn, active_term_count) as Index;
    pub static ref LOG_END_OF_STREAM_POSITION_OFFSET: Index = offset_of!(LogMetaDataDefn, end_of_stream_position) as Index;
    pub static ref LOG_IS_CONNECTED_OFFSET: Index = offset_of!(LogMetaDataDefn, is_connected) as Index;
    pub static ref LOG_ACTIVE_TRANSPORT_COUNT: Index = offset_of!(LogMetaDataDefn, active_transport_count) as Index;
    pub static ref LOG_INITIAL_TERM_ID_OFFSET: Index = offset_of!(LogMetaDataDefn, initial_term_id) as Index;
    pub static ref LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET: Index =
        offset_of!(LogMetaDataDefn, default_frame_header_length) as Index;
    pub static ref LOG_MTU_LENGTH_OFFSET: Index = offset_of!(LogMetaDataDefn, mtu_length) as Index;
    pub static ref LOG_TERM_LENGTH_OFFSET: Index = offset_of!(LogMetaDataDefn, term_length) as Index;
    pub static ref LOG_PAGE_SIZE_OFFSET: Index = offset_of!(LogMetaDataDefn, page_size) as Index;
}

pub const LOG_DEFAULT_FRAME_HEADER_OFFSET: Index = std::mem::size_of::<LogMetaDataDefn>() as Index;
pub const LOG_META_DATA_LENGTH: Index = 4 * 1024;

pub fn check_term_length(term_length: Index) -> Result<(), AeronError> {
    if term_length < TERM_MIN_LENGTH {
        return Err(IllegalStateError::TermLengthIsLessThanMinPossibleSize {
            term_length,
            term_min_length: TERM_MIN_LENGTH,
        }
        .into());
    }

    if term_length > TERM_MAX_LENGTH {
        return Err(IllegalStateError::TermLengthIsGreaterThanMaxPossibleSize {
            term_length,
            term_max_length: TERM_MAX_LENGTH,
        }
        .into());
    }

    if !is_power_of_two(term_length) {
        return Err(IllegalStateError::TermLengthIsNotPowerOfTwo(term_length).into());
    }

    Ok(())
}

pub fn check_page_size(page_size: Index) -> Result<(), AeronError> {
    if page_size < AERON_PAGE_MIN_SIZE {
        return Err(IllegalStateError::PageSizeLessThanMinPossibleSize {
            page_size,
            page_min_size: AERON_PAGE_MIN_SIZE,
        }
        .into());
    }

    if page_size > AERON_PAGE_MAX_SIZE {
        return Err(IllegalStateError::PageSizeGreaterThanMaxPossibleSize {
            page_size,
            page_max_size: AERON_PAGE_MAX_SIZE,
        }
        .into());
    }

    if !is_power_of_two(page_size) {
        return Err(IllegalStateError::PageSizeIsNotPowerOfTwo(page_size).into());
    }

    Ok(())
}

pub fn initial_term_id(log_meta_data_buffer: &AtomicBuffer) -> i32 {
    log_meta_data_buffer.get::<i32>(*LOG_INITIAL_TERM_ID_OFFSET)
}

pub fn mtu_length(log_meta_data_buffer: &AtomicBuffer) -> i32 {
    log_meta_data_buffer.get::<i32>(*LOG_MTU_LENGTH_OFFSET)
}

pub fn term_length(log_meta_data_buffer: &AtomicBuffer) -> i32 {
    log_meta_data_buffer.get::<i32>(*LOG_TERM_LENGTH_OFFSET)
}

pub fn page_size(log_meta_data_buffer: &AtomicBuffer) -> i32 {
    log_meta_data_buffer.get::<i32>(*LOG_PAGE_SIZE_OFFSET)
}

pub fn active_term_count(log_meta_data_buffer: &AtomicBuffer) -> i32 {
    log_meta_data_buffer.get_volatile::<i32>(*LOG_ACTIVE_TERM_COUNT_OFFSET)
}

pub fn set_active_term_count_ordered(log_meta_data_buffer: &AtomicBuffer, active_term_id: i32) {
    log_meta_data_buffer.put_ordered::<i32>(*LOG_ACTIVE_TERM_COUNT_OFFSET, active_term_id);
}

pub fn cas_active_term_count(log_meta_data_buffer: &AtomicBuffer, expected_term_count: i32, update_term_count: i32) -> bool {
    log_meta_data_buffer.compare_and_set_i32(*LOG_ACTIVE_TERM_COUNT_OFFSET, expected_term_count, update_term_count)
}

pub fn next_partition_index(current_index: Index) -> Index {
    (current_index + 1) % PARTITION_COUNT
}

pub fn previous_partition_index(current_index: Index) -> Index {
    (current_index + (PARTITION_COUNT - 1)) % PARTITION_COUNT
}

pub fn is_connected(log_meta_data_buffer: &AtomicBuffer) -> bool {
    log_meta_data_buffer.get_volatile::<i32>(*LOG_IS_CONNECTED_OFFSET) == 1
}

pub fn set_is_connected(log_meta_data_buffer: &AtomicBuffer, is_connected: bool) {
    log_meta_data_buffer.put_ordered::<i32>(*LOG_IS_CONNECTED_OFFSET, is_connected as i32)
}

pub fn active_transport_count(log_meta_data_buffer: &AtomicBuffer) -> i32 {
    log_meta_data_buffer.get_volatile::<i32>(*LOG_ACTIVE_TRANSPORT_COUNT)
}

pub fn set_active_transport_count(log_meta_data_buffer: &AtomicBuffer, number_of_active_transports: i32) {
    log_meta_data_buffer.put_ordered::<i32>(*LOG_ACTIVE_TRANSPORT_COUNT, number_of_active_transports);
}

pub fn end_of_stream_position(log_meta_data_buffer: &AtomicBuffer) -> i64 {
    log_meta_data_buffer.get_volatile::<i64>(*LOG_END_OF_STREAM_POSITION_OFFSET)
}

pub fn set_end_of_stream_position(log_meta_data_buffer: &AtomicBuffer, position: i64) {
    log_meta_data_buffer.put_ordered::<i64>(*LOG_END_OF_STREAM_POSITION_OFFSET, position);
}

pub fn index_by_term(initial_term_id: i32, active_term_id: i32) -> Index {
    (active_term_id - initial_term_id) as Index % PARTITION_COUNT
}

pub fn index_by_term_count(term_count: i64) -> Index {
    (term_count % PARTITION_COUNT as i64) as Index
}

pub fn index_by_position(position: i64, position_bits_to_shift: i32) -> Index {
    ((position >> position_bits_to_shift as i64) % PARTITION_COUNT as i64) as Index
}

pub fn compute_position(active_term_id: i32, term_offset: Index, position_bits_to_shift: i32, initial_term_id: i32) -> i64 {
    let term_count: i64 = active_term_id as i64 - initial_term_id as i64;

    (term_count << position_bits_to_shift as i64) + term_offset as i64
}

pub fn compute_term_begin_position(active_term_id: i32, position_bits_to_shift: i32, initial_term_id: i32) -> i64 {
    let term_count: i64 = active_term_id as i64 - initial_term_id as i64;

    term_count << position_bits_to_shift as i64
}

pub fn raw_tail_volatile(log_meta_data_buffer: &AtomicBuffer) -> i64 {
    let partition_index = index_by_term_count(active_term_count(log_meta_data_buffer) as i64);
    log_meta_data_buffer.get_volatile::<i64>(*TERM_TAIL_COUNTER_OFFSET + (partition_index * I64_SIZE))
}

pub fn raw_tail(log_meta_data_buffer: &AtomicBuffer) -> i64 {
    let partition_index = index_by_term_count(active_term_count(log_meta_data_buffer) as i64);
    log_meta_data_buffer.get::<i64>(*TERM_TAIL_COUNTER_OFFSET + (partition_index * I64_SIZE))
}

pub fn raw_tail_by_partition_index(log_meta_data_buffer: &AtomicBuffer, partition_index: Index) -> i64 {
    log_meta_data_buffer.get::<i64>(*TERM_TAIL_COUNTER_OFFSET + (partition_index * I64_SIZE))
}

pub fn term_id(raw_tail: i64) -> i32 {
    (raw_tail >> 32) as i32
}

pub fn term_offset(raw_tail: i64, term_length: i64) -> i32 {
    let tail = raw_tail & 0xFFFF_FFFF;

    std::cmp::min(tail, term_length) as i32
}

pub fn cas_raw_tail(
    log_meta_data_buffer: &AtomicBuffer,
    partition_index: Index,
    expected_raw_tail: i64,
    update_raw_tail: i64,
) -> bool {
    log_meta_data_buffer.compare_and_set_i64(
        *TERM_TAIL_COUNTER_OFFSET + (partition_index * I64_SIZE),
        expected_raw_tail,
        update_raw_tail,
    )
}

pub fn default_frame_header(log_meta_data_buffer: &AtomicBuffer) -> AtomicBuffer {
    let ptr: *mut u8 = unsafe { log_meta_data_buffer.buffer().offset(LOG_DEFAULT_FRAME_HEADER_OFFSET as isize) };
    AtomicBuffer::new(ptr, crate::concurrent::logbuffer::data_frame_header::LENGTH)
}

pub fn rotate_log(log_meta_data_buffer: &AtomicBuffer, current_term_count: i32, current_term_id: i32) {
    let next_term_id = current_term_id + 1;
    let next_term_count = current_term_count + 1;
    let next_index = index_by_term_count(next_term_count as i64);
    let expected_term_id = next_term_id - PARTITION_COUNT;
    let new_raw_tail: i64 = next_term_id as i64 * (1_i64 << 32);

    let mut raw_tail: i64;
    loop {
        raw_tail = raw_tail_by_partition_index(log_meta_data_buffer, next_index);
        if expected_term_id != term_id(raw_tail) {
            break;
        }

        if cas_raw_tail(log_meta_data_buffer, next_index, raw_tail, new_raw_tail) {
            break;
        }
    }

    cas_active_term_count(log_meta_data_buffer, current_term_count, next_term_count);
}

pub fn initialize_tail_with_term_id(log_meta_data_buffer: &AtomicBuffer, partition_index: Index, term_id: i32) {
    let raw_tail: i64 = term_id as i64 * (1_i64 << 32);
    log_meta_data_buffer.put::<i64>(*TERM_TAIL_COUNTER_OFFSET + (partition_index * I64_SIZE), raw_tail);
}
