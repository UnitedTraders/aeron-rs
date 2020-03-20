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

use std::cmp::min;
use std::ffi::CString;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::concurrent::logbuffer::header::Header;
use crate::concurrent::logbuffer::term_reader::{ErrorHandler, FragmentHandler, ReadOutcome};
use crate::concurrent::logbuffer::term_scan::{scan, BlockHandler};
use crate::concurrent::logbuffer::{data_frame_header, frame_descriptor, log_buffer_descriptor, term_reader};
use crate::concurrent::position::{ReadablePosition, UnsafeBufferPosition};
use crate::utils::bit_utils::{align, number_of_trailing_zeroes};
use crate::utils::errors::AeronError;
use crate::utils::log_buffers::LogBuffers;
use crate::utils::types::Index;

#[derive(Eq, PartialEq)]
pub enum ControlledPollAction {
    /**
     * Abort the current polling operation and do not advance the position for this fragment.
     */
    ABORT = 1,

    /**
     * Break from the current polling operation and commit the position as of the end of the current fragment
     * being handled.
     */
    BREAK,

    /**
     * Continue processing but commit the position as of the end of the current fragment so that
     * flow control is applied to this point.
     */
    COMMIT,

    /**
     * Continue processing taking the same approach as the in fragment_handler_t.
     */
    CONTINUE,
}

/**
 * Callback for handling fragments of data being read from a log.
 *
 * @param buffer containing the data.
 * @param offset at which the data begins.
 * @param length of the data in bytes.
 * @param header representing the meta data for the data.
 * @return The action to be taken with regard to the stream position after the callback.
 */
type ControlledPollFragmentHandler = fn(&AtomicBuffer, Index, Index, Header);

pub struct Image {
    source_identity: CString,
    log_buffers: Arc<LogBuffers>,
    exception_handler: ErrorHandler,
    term_buffers: Vec<AtomicBuffer>,
    subscriber_position: UnsafeBufferPosition,
    header: Header,
    is_closed: AtomicBool,
    is_eos: bool,
    term_length_mask: Index,
    position_bits_to_shift: i32,
    session_id: i32,
    join_position: i64,
    final_position: i64,
    subscription_registration_id: i64,
    correlation_id: i64,
}

enum ImageError {}

impl Image {
    pub(crate) fn create(
        session_id: i32,
        correlation_id: i64,
        subscription_registration_id: i64,
        source_identity: CString,
        subscriber_position: &UnsafeBufferPosition,
        log_buffers: Arc<LogBuffers>,
        exception_handler: ErrorHandler,
    ) -> Image {
        let header = Header::new(
            log_buffer_descriptor::initial_term_id(
                &log_buffers.atomic_buffer(log_buffer_descriptor::LOG_META_DATA_SECTION_INDEX),
            ),
            log_buffers.atomic_buffer(0).capacity(),
        );

        let mut term_buffers: Vec<AtomicBuffer> = Vec::new();

        for i in 0..log_buffer_descriptor::PARTITION_COUNT {
            term_buffers.push(log_buffers.atomic_buffer(i))
        }

        let capacity = term_buffers[0].capacity();

        let join_position = subscriber_position.get();
        let final_position = join_position;

        Self {
            term_buffers,
            header,
            subscriber_position: (*subscriber_position).clone(),
            log_buffers,
            source_identity,
            is_closed: AtomicBool::new(false),
            exception_handler,
            correlation_id,
            subscription_registration_id,
            session_id,
            final_position,
            join_position,
            term_length_mask: capacity - 1,
            position_bits_to_shift: number_of_trailing_zeroes(capacity),
            is_eos: false,
        }
    }

    fn validate_position(&self, new_position: i64) -> Result<(), AeronError> {
        let current_position = self.subscriber_position.get();
        let limit_position =
            (current_position - (current_position & self.term_length_mask as i64)) + self.term_length_mask as i64 + 1;

        if new_position < current_position || new_position > limit_position {
            return Err(AeronError::IllegalArgumentException(format!(
                "{} new_position out of range {} - {}",
                new_position, current_position, limit_position
            )));
        }

        if 0 != (new_position & (frame_descriptor::FRAME_ALIGNMENT - 1) as i64) {
            return Err(AeronError::IllegalArgumentException(format!(
                "{} new_position not aligned to FRAME_ALIGNMENT",
                new_position
            )));
        }

        Ok(())
    }

    /**
     * Get the length in bytes for each term partition in the log buffer.
     *
     * @return the length in bytes for each term partition in the log buffer.
     */
    pub fn term_buffer_length(&self) -> i32 {
        self.term_buffers[0].capacity() as i32
    }

    /**
     * Number of bits to right shift a position to get a term count for how far the stream has progressed.
     *
     * @return of bits to right shift a position to get a term count for how far the stream has progressed.
     */
    pub fn position_bits_to_shift(&self) -> i32 {
        self.position_bits_to_shift
    }

    /**
     * The sessionId for the steam of messages.
     *
     * @return the sessionId for the steam of messages.
     */
    pub fn session_id(&self) -> i32 {
        self.session_id
    }

    /**
     * The correlationId for identification of the image with the media driver.
     *
     * @return the correlationId for identification of the image with the media driver.
     */
    pub fn correlation_id(&self) -> i64 {
        self.correlation_id
    }

    /**
     * The registrationId for the Subscription of the Image.
     *
     * @return the registrationId for the Subscription of the Image.
     */
    pub fn subscription_registration_id(&self) -> i64 {
        self.subscription_registration_id
    }

    /**
     * The position at which this stream was joined.
     *
     * @return the position at which this stream was joined.
     */
    pub fn join_position(&self) -> i64 {
        self.join_position
    }

    /**
     * The initial term at which the stream started for this session.
     *
     * @return the initial term id.
     */
    pub fn initial_term_id(&self) -> i32 {
        self.header.initial_term_id()
    }

    /**
     * The source identity of the sending publisher as an abstract concept appropriate for the media.
     *
     * @return source identity of the sending publisher as an abstract concept appropriate for the media.
     */
    pub fn source_identity(&self) -> CString {
        self.source_identity.clone()
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Acquire)
    }

    /**
     * The position this Image has been consumed to by the subscriber.
     *
     * @return the position this Image has been consumed to by the subscriber or CLOSED if closed
     */
    pub fn position(&self) -> i64 {
        if self.is_closed() {
            return self.final_position;
        }

        self.subscriber_position.get()
    }

    /**
     * Get the counter id used to represent the subscriber position.
     *
     * @return the counter id used to represent the subscriber position.
     */
    pub fn subscriber_position_id(&self) -> i32 {
        self.subscriber_position.id()
    }

    /**
     * Set the subscriber position for this Image to indicate where it has been consumed to.
     *
     * @param newPosition for the consumption point.
     */
    pub fn set_position(&self, new_position: i64) -> Result<(), AeronError> {
        if !self.is_closed() {
            self.validate_position(new_position)?;
            self.subscriber_position.set_ordered(new_position);
        }

        Ok(())
    }

    /**
     * Is the current consumed position at the end of the stream?
     *
     * @return true if at the end of the stream or false if not.
     */
    pub fn is_end_of_stream(&self) -> bool {
        if self.is_closed() {
            return self.is_eos;
        }

        self.subscriber_position.get()
            >= log_buffer_descriptor::end_of_stream_position(
                &self
                    .log_buffers
                    .atomic_buffer(log_buffer_descriptor::LOG_META_DATA_SECTION_INDEX),
            )
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered via the fragment_handler_t up to a limited number of fragments as specified.
     *
     * @param fragmentHandler to which messages are delivered.
     * @param fragmentLimit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     *
     * @see fragment_handler_t
     */
    pub fn poll<T>(&mut self, fragment_handler: FragmentHandler<T>, fragment_limit: i32) -> i32 {
        if !self.is_closed() {
            let position = self.subscriber_position.get();
            let term_offset: Index = (position as Index) & self.term_length_mask;
            let index = log_buffer_descriptor::index_by_position(position, self.position_bits_to_shift);
            assert!(index >= 0 && index < log_buffer_descriptor::PARTITION_COUNT);
            let term_buffer = self.term_buffers[index as usize];
            let read_outcome: ReadOutcome = term_reader::read(
                term_buffer,
                term_offset,
                fragment_handler,
                fragment_limit,
                &mut self.header,
                self.exception_handler,
            );

            let new_position = position + (read_outcome.offset - term_offset) as i64;
            if new_position > position {
                self.subscriber_position.set_ordered(new_position);
            }

            read_outcome.fragments_read
        } else {
            0
        }
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered to the controlled_poll_fragment_handler_t up to a limited number of fragments as specified.
     *
     * To assemble messages that span multiple fragments then use ControlledFragmentAssembler.
     *
     * @param fragmentHandler to which message fragments are delivered.
     * @param fragmentLimit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     *
     * @see controlled_poll_fragment_handler_t
     */

    pub fn controlled_poll(&mut self, fragment_handler: FragmentHandler<ControlledPollAction>, fragment_limit: i32) -> i32 {
        if !self.is_closed() {
            let mut fragments_read = 0;
            let mut initial_position = self.subscriber_position.get();
            let mut initial_offset: Index = (initial_position as i32) & self.term_length_mask;
            let index = log_buffer_descriptor::index_by_position(initial_position, self.position_bits_to_shift);

            assert!(index >= 0 && index < log_buffer_descriptor::PARTITION_COUNT);

            let term_buffer = self.term_buffers[index as usize];
            let mut resulting_offset: Index = initial_offset;
            let capacity = term_buffer.capacity();

            self.header.set_buffer(term_buffer);

            while fragments_read < fragment_limit && resulting_offset < capacity {
                let length = frame_descriptor::frame_length_volatile(&term_buffer, resulting_offset as Index);
                if length <= 0 {
                    break;
                }

                let frame_offset = resulting_offset;
                let aligned_length = align(length, frame_descriptor::FRAME_ALIGNMENT);
                resulting_offset += aligned_length;

                if frame_descriptor::is_padding_frame(&term_buffer, frame_offset) {
                    continue;
                }

                self.header.set_offset(frame_offset);

                let action = fragment_handler(
                    &term_buffer,
                    frame_offset + data_frame_header::LENGTH,
                    length - data_frame_header::LENGTH,
                    &self.header,
                )
                .unwrap(); //todo unwrap

                // if let Err(err) = action {
                //     self.exception_handler(err)
                // }

                if ControlledPollAction::ABORT == action {
                    resulting_offset -= aligned_length;
                    break;
                }

                fragments_read += 1;

                if ControlledPollAction::BREAK == action {
                    break;
                } else if ControlledPollAction::COMMIT == action {
                    initial_position += (resulting_offset - initial_offset) as i64;
                    initial_offset = resulting_offset;
                    self.subscriber_position.set_ordered(initial_position);
                }
            }

            let resulting_position = initial_position + (resulting_offset - initial_offset) as i64;
            if resulting_position > initial_position {
                self.subscriber_position.set_ordered(resulting_position);
            }

            fragments_read
        } else {
            0
        }
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered to the controlled_poll_fragment_handler_t up to a limited number of fragments as specified or
     * the maximum position specified.
     *
     * To assemble messages that span multiple fragments then use ControlledFragmentAssembler.
     *
     * @param fragmentHandler to which message fragments are delivered.
     * @param maxPosition     to consume messages up to.
     * @param fragmentLimit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     * @see controlled_poll_fragment_handler_t
     */

    pub fn bounded_controlled_poll<F>(
        &mut self,
        fragment_handler: FragmentHandler<ControlledPollAction>,
        max_position: i64,
        fragment_limit: i32,
    ) -> i32 {
        if !self.is_closed() {
            let mut fragments_read = 0;
            let mut initial_position = self.subscriber_position.get();
            let mut initial_offset: Index = initial_position as Index & self.term_length_mask;
            let index = log_buffer_descriptor::index_by_position(initial_position, self.position_bits_to_shift);
            assert!(index >= 0 && index < log_buffer_descriptor::PARTITION_COUNT);
            let term_buffer = self.term_buffers[index as usize];
            let mut resulting_offset: Index = initial_offset;
            let capacity = term_buffer.capacity();
            let end_offset: Index = min(capacity, ((max_position - initial_position) + initial_offset as i64) as Index);

            self.header.set_buffer(term_buffer);

            while fragments_read < fragment_limit && resulting_offset < end_offset {
                let length = frame_descriptor::frame_length_volatile(&term_buffer, resulting_offset) as Index;
                if length <= 0 {
                    break;
                }

                let frame_offset = resulting_offset;
                let aligned_length = align(length, frame_descriptor::FRAME_ALIGNMENT);
                resulting_offset += aligned_length;

                if frame_descriptor::is_padding_frame(&term_buffer, frame_offset) {
                    continue;
                }

                self.header.set_offset(frame_offset);

                let action = fragment_handler(
                    &term_buffer,
                    frame_offset + data_frame_header::LENGTH,
                    length - data_frame_header::LENGTH,
                    &self.header,
                )
                .unwrap(); //todo

                if ControlledPollAction::ABORT == action {
                    resulting_offset -= aligned_length;
                    break;
                }

                fragments_read += 1;

                if ControlledPollAction::BREAK == action {
                    break;
                } else if ControlledPollAction::COMMIT == action {
                    initial_position += (resulting_offset - initial_offset) as i64;
                    initial_offset = resulting_offset;
                    self.subscriber_position.set_ordered(initial_position);
                }
            }

            let resulting_position = initial_position + (resulting_offset - initial_offset) as i64;
            if resulting_position > initial_position {
                self.subscriber_position.set_ordered(resulting_position);
            }

            fragments_read
        } else {
            0
        }
    }

    /**
     * Peek for new messages in a stream by scanning forward from an initial position. If new messages are found then
     * they will be delivered to the controlled_poll_fragment_handler_t up to a limited position.
     * <p>
     * To assemble messages that span multiple fragments then use ControlledFragmentAssembler. Scans must also
     * start at the beginning of a message so that the assembler is reset.
     *
     * @param initialPosition from which to peek forward.
     * @param fragmentHandler to which message fragments are delivered.
     * @param limitPosition   up to which can be scanned.
     * @return the resulting position after the scan terminates which is a complete message.
     * @see controlled_poll_fragment_handler_t
     */

    pub fn controlled_peek(
        &mut self,
        initial_position: i64,
        fragment_handler: FragmentHandler<ControlledPollAction>,
        limit_position: i64,
    ) -> Result<i64, AeronError> {
        let mut resulting_position = initial_position;

        if !self.is_closed() {
            self.validate_position(initial_position)?;

            let mut initial_offset: Index = initial_position as i32 & self.term_length_mask;
            let mut offset: Index = initial_offset;
            let mut position: i64 = initial_position;
            let index: Index = log_buffer_descriptor::index_by_position(initial_position, self.position_bits_to_shift);
            assert!(index >= 0 && index < log_buffer_descriptor::PARTITION_COUNT);
            let termb_buffer = self.term_buffers[index as usize];
            let capacity: Index = termb_buffer.capacity();

            self.header.set_buffer(termb_buffer); //todo a u sure?

            // try
            //     {
            while position < limit_position && offset < capacity {
                let length = frame_descriptor::frame_length_volatile(&termb_buffer, offset);
                if length <= 0 {
                    break;
                }

                let frame_offset = offset;
                let aligned_length = align(length as Index, frame_descriptor::FRAME_ALIGNMENT);
                offset += aligned_length;

                if frame_descriptor::is_padding_frame(&termb_buffer, frame_offset) {
                    position += (offset - initial_offset) as i64;
                    initial_offset = offset;
                    resulting_position = position;
                    continue;
                }

                self.header.set_offset(frame_offset);

                let action = fragment_handler(
                    &termb_buffer,
                    frame_offset + data_frame_header::LENGTH,
                    length - data_frame_header::LENGTH,
                    &self.header,
                )
                .unwrap(); //todo

                if ControlledPollAction::ABORT == action {
                    break;
                }

                position += (offset - initial_offset) as i64;
                initial_offset = offset;

                if self.header.flags() & frame_descriptor::END_FRAG != 0 {
                    resulting_position = position;
                }

                if ControlledPollAction::BREAK == action {
                    break;
                }
            }

            //     }
            // catch( const std::exception & ex)
            // {
            //     m_exceptionHandler(ex);
            // }
        }

        Ok(resulting_position)
    }

    /**
     * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
     * will be delivered via the block_handler_t up to a limited number of bytes.
     *
     * A scan will terminate if a padding frame is encountered. If first frame in a scan is padding then a block
     * for the padding is notified. If the padding comes after the first frame in a scan then the scan terminates
     * at the offset the padding frame begins. Padding frames are delivered singularly in a block.
     *
     * Padding frames may be for a greater range than the limit offset but only the header needs to be valid so
     * relevant length of the frame is sizeof DataHeaderDefn.
     *
     * @param blockHandler     to which block is delivered.
     * @param blockLengthLimit up to which a block may be in length.
     * @return the number of bytes that have been consumed.
     *
     * @see block_handler_t
     */

    pub fn block_poll(&self, block_handler: BlockHandler, block_length_limit: Index) -> i32 {
        if !self.is_closed() {
            let position = self.subscriber_position.get();
            let term_offset = position as Index & self.term_length_mask;
            let index = log_buffer_descriptor::index_by_position(position, self.position_bits_to_shift);
            assert!(index >= 0 && index < log_buffer_descriptor::PARTITION_COUNT);
            let term_buffer = self.term_buffers[index as usize];
            let limit_offset: Index = min(term_offset + block_length_limit, term_buffer.capacity());
            let resulting_offset: Index = scan(&term_buffer, term_offset, limit_offset);
            let length: Index = resulting_offset - term_offset;

            if resulting_offset > term_offset {
                let term_id = term_buffer.get::<i32>(term_offset + *data_frame_header::TERM_ID_FIELD_OFFSET);
                block_handler(&term_buffer, term_offset, length, self.session_id, term_id);

                self.subscriber_position.set_ordered(position + length as i64);
            }
            length
        } else {
            0
        }
    }

    pub fn log_buffers(&self) -> Arc<LogBuffers> {
        self.log_buffers.clone()
    }

    /// @cond HIDDEN_SYMBOLS
    pub fn close(&mut self) {
        if !self.is_closed() {
            self.final_position = self.subscriber_position.get_volatile();
            self.is_eos = self.final_position
                >= log_buffer_descriptor::end_of_stream_position(
                    &self
                        .log_buffers
                        .atomic_buffer(log_buffer_descriptor::LOG_META_DATA_SECTION_INDEX),
                );
            self.is_closed.store(true, Ordering::Release)
        }
    }
}

#[derive(Copy, Clone)]
pub struct ImageList {
    // images: Vec<Image>,
    pub(crate) ptr: *mut Image,
    pub length: isize,
}

impl ImageList {
    pub fn image(&mut self, pos: isize) -> &mut Image {
        assert!(pos < self.length);

        unsafe {
            let img = self.ptr.offset(pos);
            &mut *img
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::memory_mapped_file::MemoryMappedFile;

    #[test]
    fn test_create_new() {
        let unsafe_buffer_position = UnsafeBufferPosition::new(AtomicBuffer::wrap_slice(&mut []), 0);
        MemoryMappedFile::create_new("file", 0, 65536).unwrap();

        let log_buffers = LogBuffers::from_existing("file", false).unwrap();
        let buffers = Arc::new(log_buffers);

        let _image = Image::create(
            0,
            0,
            0,
            CString::new("hi").unwrap(),
            &unsafe_buffer_position,
            buffers,
            |_err| {},
        );
    }
}
