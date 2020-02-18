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
use crate::concurrent::logbuffer::header::Header;
use crate::concurrent::logbuffer::{data_frame_header, frame_descriptor};
use crate::utils::bit_utils;
use crate::utils::errors::AeronError;
use crate::utils::types::Index;

/**
* Callback for handling fragments of data being read from a log.
*
* Handler for reading data that is coming from a log buffer. The frame will either contain a whole message
* or a fragment of a message to be reassembled. Messages are fragmented if greater than the frame for MTU in length.

* @param buffer containing the data.
* @param offset at which the data begins.
* @param length of the data in bytes.
* @param header representing the meta data for the data.
*/
pub type FragmentHandler = fn(&AtomicBuffer, Index, Index, &Header) -> Result<(), AeronError>;

/**
 * Callback to indicate an exception has occurred.
 *
 * @param exception that has occurred.
 */
pub type ExceptionHandler = fn(AeronError);

pub struct ReadOutcome {
    offset: Index,
    fragments_read: i32,
}

pub fn read(
    outcome: &mut ReadOutcome,
    term_buffer: AtomicBuffer,
    mut term_offset: i32,
    handler: FragmentHandler,
    fragments_limit: i32,
    header: &mut Header,
    exception_handler: ExceptionHandler,
) {
    outcome.fragments_read = 0;
    outcome.offset = term_offset;
    let capacity = term_buffer.capacity();

    while outcome.fragments_read < fragments_limit && term_offset < capacity {
        let frame_length = frame_descriptor::frame_length_volatile(&term_buffer, term_offset);
        if frame_length <= 0 {
            break;
        }

        let fragment_offset = term_offset;
        term_offset += bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);

        if !frame_descriptor::is_padding_frame(&term_buffer, fragment_offset) {
            header.set_buffer(term_buffer);
            header.set_offset(fragment_offset);

            if let Err(error) = handler(
                &term_buffer,
                fragment_offset + data_frame_header::LENGTH,
                frame_length - data_frame_header::LENGTH,
                header,
            ) {
                exception_handler(error);
                break;
            }

            outcome.fragments_read += 1;
        }
    }

    outcome.offset = term_offset;
}
