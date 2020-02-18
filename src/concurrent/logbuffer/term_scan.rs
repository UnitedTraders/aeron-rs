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
use crate::concurrent::logbuffer::{data_frame_header, frame_descriptor};
use crate::utils::bit_utils;
use crate::utils::types::Index;

/**
 * Callback for handling a block of messages being read from a log.
 *
 * @param buffer    containing the block of message fragments.
 * @param offset    at which the block begins.
 * @param length    of the block in bytes.
 * @param sessionId of the stream containing this block of message fragments.
 * @param term_id    of the stream containing this block of message fragments.
 */
pub type BlockHandler = fn(&AtomicBuffer, Index, Index, i32, i32);

pub fn scan(term_buffer: &AtomicBuffer, term_offset: Index, limit_offset: Index) -> Index {
    let mut offset = term_offset;

    while offset < limit_offset {
        let frame_length = frame_descriptor::frame_length_volatile(term_buffer, offset);
        if frame_length <= 0 {
            break;
        }

        let aligned_frame_length = bit_utils::align(frame_length as Index, frame_descriptor::FRAME_ALIGNMENT);

        if frame_descriptor::is_padding_frame(term_buffer, offset) {
            if term_offset == offset {
                offset += aligned_frame_length;
            }

            break;
        }

        if offset + aligned_frame_length > limit_offset {
            break;
        }

        offset += aligned_frame_length;
    }

    offset
}

pub type GapHandler = fn(i32, &AtomicBuffer, Index, Index);

pub fn scan_for_gap(
    term_buffer: &AtomicBuffer,
    term_id: i32,
    mut rebuild_offset: Index,
    hwm_offset: Index,
    handler: GapHandler,
) -> Index {
    loop {
        let frame_length = frame_descriptor::frame_length_volatile(term_buffer, rebuild_offset);
        if frame_length <= 0 {
            break;
        }

        rebuild_offset += bit_utils::align(frame_length as Index, frame_descriptor::FRAME_ALIGNMENT);

        if rebuild_offset >= hwm_offset {
            break;
        }
    }

    let gap_begin_offset = rebuild_offset;
    if rebuild_offset < hwm_offset {
        let limit = hwm_offset - frame_descriptor::ALIGNED_HEADER_LENGTH;

        while rebuild_offset < limit {
            rebuild_offset += frame_descriptor::FRAME_ALIGNMENT;

            if term_buffer.get_volatile::<i32>(rebuild_offset) != 0 {
                rebuild_offset -= frame_descriptor::ALIGNED_HEADER_LENGTH;
                break;
            }
        }

        let gap_length = (rebuild_offset - gap_begin_offset) + frame_descriptor::ALIGNED_HEADER_LENGTH;
        handler(term_id, term_buffer, gap_begin_offset, gap_length);
    }

    gap_begin_offset
}

pub fn scan_outcome(padding: Index, available: Index) -> i64 {
    (padding as i64) << 32 | available as i64
}

pub fn available(scan_outcome: i64) -> i32 {
    scan_outcome as i32
}

pub fn padding(scan_outcome: i64) -> i32 {
    (scan_outcome >> 32) as i32
}

pub fn scan_for_availability(term_buffer: &AtomicBuffer, offset: Index, max_length: Index) -> i64 {
    let max_length = std::cmp::min(max_length, term_buffer.capacity() - offset);
    let mut available: Index = 0;
    let mut padding: Index = 0;

    loop {
        let frame_offset = offset + available;
        let frame_length = frame_descriptor::frame_length_volatile(term_buffer, frame_offset);

        if frame_length <= 0 {
            break;
        }

        let mut aligned_frame_length = bit_utils::align(frame_length as Index, frame_descriptor::FRAME_ALIGNMENT);

        if frame_descriptor::is_padding_frame(term_buffer, frame_offset) {
            padding = aligned_frame_length - data_frame_header::LENGTH;
            aligned_frame_length = data_frame_header::LENGTH;
        }

        available += aligned_frame_length;

        if available > max_length {
            available -= aligned_frame_length;
            padding = 0;
            break;
        }

        if (available + padding) >= max_length {
            break;
        }
    }

    scan_outcome(padding, available)
}
