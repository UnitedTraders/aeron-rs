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

use crate::{
    concurrent::{
        atomic_buffer::AtomicBuffer,
        logbuffer::{
            header::Header,
            {data_frame_header, frame_descriptor},
        },
    },
    utils::{bit_utils, errors::AeronError, types::Index},
};

pub trait ErrorHandler {
    fn call(&self, error: AeronError);
    fn clone_box(&self) -> Box<dyn ErrorHandler + Send>;
}

impl Clone for Box<dyn ErrorHandler + Send> {
    fn clone(&self) -> Box<dyn ErrorHandler + Send> {
        self.clone_box()
    }
}

impl<F> ErrorHandler for F
where
    F: Fn(AeronError) + Clone + Send + 'static,
{
    fn call(&self, error: AeronError) {
        self(error)
    }
    fn clone_box(&self) -> Box<dyn ErrorHandler + Send> {
        Box::new(self.clone())
    }
}

/**
 * Fn(&AtomicBuffer, Index, Index, &Header) -> Result<(), AeronError>;
 * Callback for handling fragments of data being read from a log.
 *
 * Handler for reading data that is coming from a log buffer. The frame will either contain a whole message
 * or a fragment of a message to be reassembled. Messages are fragmented if greater than the frame for MTU in length.
 * @param buffer containing the data.
 * @param offset at which the data begins.
 * @param length of the data in bytes.
 * @param header representing the meta data for the data.
 *
 * Fn(AeronError)
 * Callback to indicate an exception has occurred.
 * @param error that has occurred.
 */

#[derive(Default, Debug)]
pub struct ReadOutcome {
    pub offset: Index,
    pub fragments_read: i32,
}

pub fn read(
    term_buffer: AtomicBuffer,
    mut term_offset: Index,
    data_handler: &mut impl FnMut(&AtomicBuffer, Index, Index, &Header),
    fragments_limit: i32,
    header: &mut Header,
) -> ReadOutcome {
    let mut outcome = ReadOutcome {
        offset: term_offset,
        ..Default::default()
    };

    let capacity = term_buffer.capacity();

    while outcome.fragments_read < fragments_limit && term_offset < capacity {
        let frame_length = frame_descriptor::frame_length_volatile(&term_buffer, term_offset);

        if frame_length <= 0 {
            break;
        }

        let fragment_offset = term_offset;
        term_offset += bit_utils::align(frame_length as Index, frame_descriptor::FRAME_ALIGNMENT);

        if !frame_descriptor::is_padding_frame(&term_buffer, fragment_offset) {
            header.set_buffer(term_buffer);
            header.set_offset(fragment_offset);

            data_handler(
                &term_buffer,
                fragment_offset + data_frame_header::LENGTH,
                frame_length as Index - data_frame_header::LENGTH,
                header,
            );

            outcome.fragments_read += 1;
        }
    }

    outcome.offset = term_offset;

    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::concurrent::{
        atomic_buffer::AlignedBuffer,
        logbuffer::{log_buffer_descriptor, term_reader},
    };

    const LOG_BUFFER_CAPACITY: Index = log_buffer_descriptor::TERM_MIN_LENGTH;
    // const META_DATA_BUFFER_CAPACITY: Index = log_buffer_descriptor::LOG_META_DATA_LENGTH;
    // const LOG_BUFFER_UNALIGNED_CAPACITY: Index = log_buffer_descriptor::TERM_MIN_LENGTH + frame_descriptor::FRAME_ALIGNMENT - 1;
    // const HDR_LENGTH: Index = data_frame_header::LENGTH;
    const INITIAL_TERM_ID: i32 = 7;
    const INT_MAX: i32 = std::i32::MAX;

    macro_rules! gen_test_data {
        ($log_buffer:ident, $fragment_header:ident) => {
            let l_buff = AlignedBuffer::with_capacity(LOG_BUFFER_CAPACITY);
            let $log_buffer = AtomicBuffer::from_aligned(&l_buff);
            $log_buffer.set_memory(0, $log_buffer.capacity(), 0);
            let mut $fragment_header = Header::new(INITIAL_TERM_ID, LOG_BUFFER_CAPACITY);
        };
    }

    fn data_handler(_buf: &AtomicBuffer, offset: Index, length: Index, _header: &Header) {
        println!("Data of length={} received at offset={}", length, offset);
    }

    #[test]
    fn test_term_reader_read_first_message() {
        gen_test_data!(log, fragment_header);

        let msg_length = 1;
        let frame_length = data_frame_header::LENGTH + msg_length;
        let aligned_frame_length = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);
        let term_offset = 0;

        log.put_ordered::<i32>(frame_descriptor::length_offset(0), frame_length);
        log.put::<u16>(frame_descriptor::type_offset(0), data_frame_header::HDR_TYPE_DATA);

        log.put_ordered::<i32>(frame_descriptor::length_offset(aligned_frame_length), 0);

        let read_outcome = term_reader::read(log, term_offset, &mut data_handler, INT_MAX, &mut fragment_header);

        assert_eq!(read_outcome.offset, aligned_frame_length);
        assert_eq!(read_outcome.fragments_read, 1);
    }

    #[test]
    fn test_term_reader_not_read_past_tail() {
        gen_test_data!(log, fragment_header);

        let term_offset = 0;

        log.put_ordered::<i32>(frame_descriptor::length_offset(0), 0);

        let read_outcome = term_reader::read(log, term_offset, &mut data_handler, INT_MAX, &mut fragment_header);

        assert_eq!(read_outcome.offset, term_offset);
        assert_eq!(read_outcome.fragments_read, 0);
    }

    #[test]
    fn test_term_reader_read_one_limited_message() {
        gen_test_data!(log, fragment_header);

        let msg_length = 1;
        let frame_length = data_frame_header::LENGTH + msg_length;
        let aligned_frame_length = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);
        let term_offset = 0;

        log.put_ordered::<i32>(frame_descriptor::length_offset(0), frame_length);

        log.put::<u16>(frame_descriptor::type_offset(0), data_frame_header::HDR_TYPE_DATA);

        let read_outcome = term_reader::read(log, term_offset, &mut data_handler, 1, &mut fragment_header);

        assert_eq!(read_outcome.offset, aligned_frame_length);
        assert_eq!(read_outcome.fragments_read, 1);
    }

    #[test]
    fn test_term_reader_read_multiple_messages() {
        gen_test_data!(log, fragment_header);

        let msg_length = 1;
        let frame_length = data_frame_header::LENGTH + msg_length;
        let aligned_frame_length = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);
        let term_offset = 0;

        log.put_ordered::<i32>(frame_descriptor::length_offset(0), frame_length);
        log.put::<u16>(frame_descriptor::type_offset(0), data_frame_header::HDR_TYPE_DATA);

        log.put_ordered::<i32>(frame_descriptor::length_offset(aligned_frame_length), frame_length);
        log.put::<u16>(
            frame_descriptor::type_offset(aligned_frame_length),
            data_frame_header::HDR_TYPE_DATA,
        );

        log.put_ordered::<i32>(frame_descriptor::length_offset(aligned_frame_length * 2), 0);

        let read_outcome = term_reader::read(log, term_offset, &mut data_handler, INT_MAX, &mut fragment_header);

        assert_eq!(read_outcome.offset, aligned_frame_length * 2);
        assert_eq!(read_outcome.fragments_read, 2);
    }

    #[test]
    fn test_term_reader_read_last_message() {
        gen_test_data!(log, fragment_header);

        let msg_length = 1;
        let frame_length = data_frame_header::LENGTH + msg_length;
        let aligned_frame_length = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);
        let start_of_message = LOG_BUFFER_CAPACITY - aligned_frame_length;

        log.put_ordered::<i32>(frame_descriptor::length_offset(start_of_message), frame_length);
        log.put::<u16>(
            frame_descriptor::type_offset(start_of_message),
            data_frame_header::HDR_TYPE_DATA,
        );

        let read_outcome = term_reader::read(log, start_of_message, &mut data_handler, INT_MAX, &mut fragment_header);

        assert_eq!(read_outcome.offset, LOG_BUFFER_CAPACITY);
        assert_eq!(read_outcome.fragments_read, 1);
    }

    #[test]
    fn test_term_reader_not_read_last_message_when_padding() {
        gen_test_data!(log, fragment_header);

        let msg_length = 1;
        let frame_length = data_frame_header::LENGTH + msg_length;
        let aligned_frame_length = bit_utils::align(frame_length, frame_descriptor::FRAME_ALIGNMENT);
        let start_of_message = LOG_BUFFER_CAPACITY - aligned_frame_length;

        log.put_ordered::<i32>(frame_descriptor::length_offset(start_of_message), frame_length);
        log.put::<u16>(
            frame_descriptor::type_offset(start_of_message),
            data_frame_header::HDR_TYPE_PAD,
        );

        let read_outcome = term_reader::read(log, start_of_message, &mut data_handler, INT_MAX, &mut fragment_header);

        assert_eq!(read_outcome.offset, LOG_BUFFER_CAPACITY);
        assert_eq!(read_outcome.fragments_read, 0);
    }
}
