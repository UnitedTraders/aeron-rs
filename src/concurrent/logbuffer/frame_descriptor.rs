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

use crate::concurrent::{atomic_buffer::AtomicBuffer, logbuffer::data_frame_header};
use crate::utils::errors::IllegalStateError;
use crate::utils::{errors::AeronError, types::Index};

/**
* Description of the structure for message framing in a log buffer.
*
* All messages are logged in frames that have a minimum header layout as follows plus a reserve then
* the encoded message follows:
*
* <pre>
*   0                   1                   2                   3
*   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
*  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*  |R|                       Frame Length                          |
*  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
*  |  Version      |B|E| Flags     |             Type              |
*  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
*  |R|                       Term Offset                           |
*  +-+-------------------------------------------------------------+
*  |                      Additional Fields                       ...
* ...                                                              |
*  +---------------------------------------------------------------+
*  |                        Encoded Message                       ...
* ...                                                              |
*  +---------------------------------------------------------------+
* </pre>
*
* The (B)egin and (E)nd flags are used for message fragmentation. (R) is for reserved bit.
* Both are set for a message that does not span frames.
*/

pub const FRAME_ALIGNMENT: Index = 32;

pub const BEGIN_FRAG: u8 = 0x80;
pub const END_FRAG: u8 = 0x40;
pub const UNFRAGMENTED: u8 = BEGIN_FRAG | END_FRAG;

pub const ALIGNED_HEADER_LENGTH: Index = 32;

pub const VERSION_OFFSET: Index = 4;
pub const FLAGS_OFFSET: Index = 5;
pub const TYPE_OFFSET: Index = 6;
pub const LENGTH_OFFSET: Index = 0;
pub const TERM_OFFSET: Index = 8;

pub const MAX_MESSAGE_LENGTH: Index = 16 * 1024 * 1024;

pub fn check_header_length(length: Index) -> Result<(), AeronError> {
    if length != data_frame_header::LENGTH {
        return Err(IllegalStateError::FrameHeaderLengthMustBeEqualToDataOffset {
            length,
            data_offset: data_frame_header::LENGTH,
        }
        .into());
    }
    Ok(())
}

pub fn check_max_frame_length(length: Index) -> Result<(), AeronError> {
    if (length & (FRAME_ALIGNMENT - 1)) != 0 {
        return Err(IllegalStateError::MaxFrameLengthMustBeMultipleOfFrameAlignment {
            length,
            frame_alignment: FRAME_ALIGNMENT,
        }
        .into());
    }
    Ok(())
}

pub fn compute_max_message_length(capacity: Index) -> Index {
    std::cmp::min(capacity / 8, MAX_MESSAGE_LENGTH)
}

pub fn type_offset(frame_offset: Index) -> Index {
    frame_offset + *data_frame_header::TYPE_FIELD_OFFSET
}

pub fn flags_offset(frame_offset: Index) -> Index {
    frame_offset + *data_frame_header::FLAGS_FIELD_OFFSET
}

pub fn length_offset(frame_offset: Index) -> Index {
    frame_offset + *data_frame_header::FRAME_LENGTH_FIELD_OFFSET
}

pub fn term_offset_offset(frame_offset: Index) -> Index {
    frame_offset + *data_frame_header::TERM_OFFSET_FIELD_OFFSET
}

pub fn set_frame_type(log_buffer: &AtomicBuffer, frame_offset: Index, frame_type: u16) {
    log_buffer.put::<u16>(type_offset(frame_offset), frame_type);
}

pub fn get_frame_type(log_buffer: &AtomicBuffer, frame_offset: Index) -> u16 {
    log_buffer.get::<u16>(frame_offset)
}

pub fn set_frame_flags(log_buffer: &AtomicBuffer, frame_offset: Index, flags: u8) {
    log_buffer.put::<u8>(flags_offset(frame_offset), flags);
}

pub fn set_frame_term_offset(log_buffer: &AtomicBuffer, frame_offset: Index, term_offset: i32) {
    log_buffer.put::<i32>(term_offset_offset(frame_offset), term_offset);
}

pub fn is_padding_frame(log_buffer: &AtomicBuffer, frame_offset: Index) -> bool {
    log_buffer.get::<u16>(type_offset(frame_offset)) == data_frame_header::HDR_TYPE_PAD
}

pub fn frame_length_volatile(log_buffer: &AtomicBuffer, frame_offset: Index) -> i32 {
    log_buffer.get_volatile::<i32>(length_offset(frame_offset))
}

pub fn set_frame_length_ordered(log_buffer: &AtomicBuffer, frame_offset: Index, frame_length: Index) {
    log_buffer.put_ordered::<i32>(length_offset(frame_offset), frame_length);
}

pub fn frame_version(log_buffer: &AtomicBuffer, frame_offset: Index) -> u8 {
    log_buffer.get::<u8>(frame_offset)
}
