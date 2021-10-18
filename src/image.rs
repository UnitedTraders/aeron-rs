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

use std::{
    cmp::min,
    ffi::CString,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use crate::concurrent::{
    atomic_buffer::AtomicBuffer,
    logbuffer::{
        data_frame_header, frame_descriptor,
        header::Header,
        log_buffer_descriptor,
        term_reader::{self, ErrorHandler, ReadOutcome},
        term_scan::{scan, BlockHandler},
    },
    position::{ReadablePosition, UnsafeBufferPosition},
};
use crate::ttrace;
use crate::utils::errors::IllegalArgumentError;
use crate::utils::{
    bit_utils::{align, number_of_trailing_zeroes},
    errors::AeronError,
    log_buffers::LogBuffers,
    types::Index,
};

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
#[derive(Clone)]
pub struct Image {
    source_identity: CString,
    log_buffers: Arc<LogBuffers>,
    exception_handler: Box<dyn ErrorHandler + Send>,
    term_buffers: Vec<AtomicBuffer>,
    subscriber_position: UnsafeBufferPosition,
    header: Header,
    is_closed: Arc<AtomicBool>, // to make Image clonable
    is_eos: bool,
    term_length_mask: Index,
    position_bits_to_shift: i32,
    session_id: i32,
    join_position: i64,
    final_position: i64,
    subscription_registration_id: i64,
    correlation_id: i64,
}

unsafe impl Send for Image {}
unsafe impl Sync for Image {}

#[allow(dead_code)]
enum ImageError {}

impl Image {
    pub(crate) fn create(
        session_id: i32,
        correlation_id: i64,
        subscription_registration_id: i64,
        source_identity: CString,
        subscriber_position: &UnsafeBufferPosition,
        log_buffers: Arc<LogBuffers>,
        exception_handler: Box<dyn ErrorHandler + Send>,
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
            is_closed: Arc::new(AtomicBool::new(false)),
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
            return Err(IllegalArgumentError::NewPositionOutOfRange {
                new_position,
                left_bound: current_position,
                right_bound: limit_position,
            }
            .into());
        }

        if 0 != (new_position & (frame_descriptor::FRAME_ALIGNMENT - 1) as i64) {
            return Err(IllegalArgumentError::NewPositionNotAlignedToFrameAlignment {
                new_position,
                frame_alignment: frame_descriptor::FRAME_ALIGNMENT,
            }
            .into());
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
     * The session_id for the steam of messages.
     *
     * @return the session_id for the steam of messages.
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
     * @param fragment_limit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     *
     * @see fragment_handler_t
     */
    pub fn poll(&mut self, fragment_handler: &mut impl FnMut(&AtomicBuffer, Index, Index, &Header), fragment_limit: i32) -> i32 {
        if !self.is_closed() {
            let position = self.subscriber_position.get();
            let term_offset: Index = (position as Index) & self.term_length_mask;
            let index = log_buffer_descriptor::index_by_position(position, self.position_bits_to_shift);
            assert!((0..log_buffer_descriptor::PARTITION_COUNT).contains(&index));
            let term_buffer = self.term_buffers[index as usize];

            let read_outcome: ReadOutcome =
                term_reader::read(term_buffer, term_offset, fragment_handler, fragment_limit, &mut self.header);

            if read_outcome.fragments_read > 0 {
                ttrace!("Image {} poll returned: {:?}", self.correlation_id, read_outcome);
            }

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
     * will be delivered via the fragment_handler_t up to a limited number of fragments as specified or the
     * maximum position specified.
     *
     * @param fragmentHandler to which messages are delivered.
     * @param limitPosition   to consume messages up to.
     * @param fragment_limit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     *
     * @see fragment_handler_t
     */
    pub fn bounded_poll(
        &mut self,
        mut fragment_handler: impl FnMut(&AtomicBuffer, Index, Index, &Header),
        limit_position: i64,
        fragment_limit: i32,
    ) -> i32 {
        if !self.is_closed() {
            let mut fragments_read = 0;
            let initial_position = self.subscriber_position.get();
            let initial_offset = (initial_position & self.term_length_mask as i64) as i32;
            let index = log_buffer_descriptor::index_by_position(initial_position, self.position_bits_to_shift);
            assert!((0..log_buffer_descriptor::PARTITION_COUNT).contains(&index));

            let term_buffer = self.term_buffers[index as usize];
            let mut offset = initial_offset as i32;
            let capacity = term_buffer.capacity() as i64;
            let limit_offset = std::cmp::min(capacity, limit_position - initial_position + offset as i64) as i32;

            self.header.set_buffer(term_buffer);

            while fragments_read < fragment_limit && offset < limit_offset {
                let length = frame_descriptor::frame_length_volatile(&term_buffer, offset);
                if length <= 0 {
                    break;
                }

                let frame_offset = offset;
                let aligned_length = align(length, frame_descriptor::FRAME_ALIGNMENT);
                offset += aligned_length;

                if frame_descriptor::is_padding_frame(&term_buffer, frame_offset) {
                    continue;
                }

                self.header.set_offset(frame_offset);

                fragment_handler(
                    &term_buffer,
                    frame_offset + data_frame_header::LENGTH,
                    length - data_frame_header::LENGTH,
                    &self.header,
                );

                fragments_read += 1;
            }

            let resulting_position = initial_position + (offset - initial_offset as i32) as i64;
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
     * will be delivered to the controlled_poll_fragment_handler_t up to a limited number of fragments as specified.
     *
     * To assemble messages that span multiple fragments then use ControlledFragmentAssembler.
     *
     * @param fragmentHandler to which message fragments are delivered.
     * @param fragment_limit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     *
     * @see controlled_poll_fragment_handler_t
     */

    pub fn controlled_poll(
        &mut self,
        mut fragment_handler: impl FnMut(&AtomicBuffer, Index, Index, &Header) -> Result<ControlledPollAction, AeronError>,
        fragment_limit: i32,
    ) -> i32 {
        if !self.is_closed() {
            let mut fragments_read = 0;
            let mut initial_position = self.subscriber_position.get();
            let mut initial_offset: Index = (initial_position as i32) & self.term_length_mask;
            let index = log_buffer_descriptor::index_by_position(initial_position, self.position_bits_to_shift);

            assert!((0..log_buffer_descriptor::PARTITION_COUNT).contains(&index));

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
     * @param max_position     to consume messages up to.
     * @param fragment_limit   for the number of fragments to be consumed during one polling operation.
     * @return the number of fragments that have been consumed.
     * @see controlled_poll_fragment_handler_t
     */

    pub fn bounded_controlled_poll(
        &mut self,
        mut fragment_handler: impl FnMut(&AtomicBuffer, Index, Index, &Header) -> Result<ControlledPollAction, AeronError>,
        max_position: i64,
        fragment_limit: i32,
    ) -> i32 {
        if !self.is_closed() {
            let mut fragments_read = 0;
            let mut initial_position = self.subscriber_position.get();
            let mut initial_offset: Index = initial_position as Index & self.term_length_mask;
            let index = log_buffer_descriptor::index_by_position(initial_position, self.position_bits_to_shift);
            assert!((0..log_buffer_descriptor::PARTITION_COUNT).contains(&index));
            let term_buffer = self.term_buffers[index as usize];
            let mut resulting_offset: Index = initial_offset;
            let capacity = term_buffer.capacity() as i64;
            let end_offset: Index = min(capacity as i64, (max_position - initial_position) + initial_offset as i64) as Index;

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
     * @param initial_position from which to peek forward.
     * @param fragmentHandler to which message fragments are delivered.
     * @param limitPosition   up to which can be scanned.
     * @return the resulting position after the scan terminates which is a complete message.
     * @see controlled_poll_fragment_handler_t
     */

    pub fn controlled_peek(
        &mut self,
        initial_position: i64,
        mut fragment_handler: impl FnMut(&AtomicBuffer, Index, Index, &Header) -> Result<ControlledPollAction, AeronError>,
        limit_position: i64,
    ) -> Result<i64, AeronError> {
        let mut resulting_position = initial_position;

        if !self.is_closed() {
            self.validate_position(initial_position)?;

            let mut initial_offset: Index = initial_position as i32 & self.term_length_mask;
            let mut offset: Index = initial_offset;
            let mut position: i64 = initial_position;
            let index: Index = log_buffer_descriptor::index_by_position(initial_position, self.position_bits_to_shift);
            assert!((0..log_buffer_descriptor::PARTITION_COUNT).contains(&index));
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
            assert!((0..log_buffer_descriptor::PARTITION_COUNT).contains(&index));
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

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;

    use super::*;
    use crate::utils::errors::GenericError;
    use crate::{
        concurrent::{atomic_buffer::AlignedBuffer, logbuffer::data_frame_header::DataFrameHeaderDefn},
        utils::bit_utils::{align, number_of_trailing_zeroes},
    };

    const TERM_LENGTH: Index = log_buffer_descriptor::TERM_MIN_LENGTH;
    const PAGE_SIZE: Index = log_buffer_descriptor::AERON_PAGE_MIN_SIZE;
    const LOG_META_DATA_LENGTH: Index = log_buffer_descriptor::LOG_META_DATA_LENGTH;
    const SRC_BUFFER_LENGTH: Index = 1024;
    const COUNTER_VALUES_BUFFER_LENGTH: Index = 1024 * 1024;
    const LOG_BUFFER_LENGTH: Index = TERM_LENGTH * 3 + LOG_META_DATA_LENGTH;

    // type TermBuffer = [u8; LOG_BUFFER_LENGTH as usize];
    // type SrcBuffer = [u8; SRC_BUFFER_LENGTH as usize];

    const STREAM_ID: i32 = 10;
    const SESSION_ID: i32 = 200;
    const SUBSCRIBER_POSITION_ID: i32 = 0;

    const CORRELATION_ID: i64 = 100;
    const SUBSCRIPTION_REGISTRATION_ID: i64 = 99;
    const SOURCE_IDENTITY: &str = "test";

    const DATA: [u8; 17] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

    const INITIAL_TERM_ID: i32 = 0xFEDA;
    lazy_static! {
        static ref POSITION_BITS_TO_SHIFT: i32 = number_of_trailing_zeroes(TERM_LENGTH);
        static ref ALIGNED_FRAME_LENGTH: Index = align(
            data_frame_header::LENGTH + DATA.len() as i32,
            frame_descriptor::FRAME_ALIGNMENT
        );
    }

    fn error_handler(err: AeronError) {
        println!("error_handler: {:?}", err)
    }

    #[allow(dead_code)]
    fn fragment_handler(_buf: &AtomicBuffer, _offset: Index, _length: Index, _header: &Header) {
        println!("fragment_handler called");
    }

    fn controlled_poll_handler(
        _buf: &AtomicBuffer,
        _offset: Index,
        _length: Index,
        _header: &Header,
    ) -> Result<ControlledPollAction, AeronError> {
        Err(GenericError::Custom("Test".to_string()).into())
    }

    #[allow(clippy::unnecessary_wraps)]
    fn controlled_poll_handler_continue(
        _buf: &AtomicBuffer,
        _offset: Index,
        _length: Index,
        _header: &Header,
    ) -> Result<ControlledPollAction, AeronError> {
        Ok(ControlledPollAction::CONTINUE)
    }

    #[allow(clippy::unnecessary_wraps)]
    fn controlled_poll_handler_abort(
        _buf: &AtomicBuffer,
        _offset: Index,
        _length: Index,
        _header: &Header,
    ) -> Result<ControlledPollAction, AeronError> {
        Ok(ControlledPollAction::ABORT)
    }

    #[allow(clippy::unnecessary_wraps)]
    fn controlled_poll_handler_break(
        _buf: &AtomicBuffer,
        _offset: Index,
        _length: Index,
        _header: &Header,
    ) -> Result<ControlledPollAction, AeronError> {
        Ok(ControlledPollAction::BREAK)
    }

    #[allow(clippy::unnecessary_wraps)]
    fn controlled_poll_handler_commit(
        _buf: &AtomicBuffer,
        offset: Index,
        _length: Index,
        _header: &Header,
    ) -> Result<ControlledPollAction, AeronError> {
        if offset == data_frame_header::LENGTH {
            return Ok(ControlledPollAction::COMMIT);
        } else if offset == *ALIGNED_FRAME_LENGTH + data_frame_header::LENGTH {
            return Ok(ControlledPollAction::ABORT);
        }

        unreachable!("Should not get there!");
    }

    #[allow(clippy::unnecessary_wraps)]
    fn controlled_handler_cont_cont(
        _buf: &AtomicBuffer,
        offset: Index,
        _length: Index,
        _header: &Header,
    ) -> Result<ControlledPollAction, AeronError> {
        if offset == data_frame_header::LENGTH || offset == *ALIGNED_FRAME_LENGTH + data_frame_header::LENGTH {
            return Ok(ControlledPollAction::CONTINUE);
        }

        unreachable!("Should not get there!");
    }

    struct ImageTest {
        pub term_buffers: [AtomicBuffer; 3],
        pub log_meta_data_buffer: AtomicBuffer,
        pub src_buffer: AtomicBuffer,
        pub counter_values_buffer: AtomicBuffer,

        pub log_buffers: Arc<LogBuffers>,
        pub subscriber_position: UnsafeBufferPosition,
    }

    impl ImageTest {
        pub fn new(log_buf: &AlignedBuffer, src_buf: &AlignedBuffer, cnt_buf: &AlignedBuffer) -> Self {
            let counters_buffer = AtomicBuffer::from_aligned(cnt_buf);

            let l_log_buffers = unsafe { Arc::new(LogBuffers::new(log_buf.ptr, log_buf.len as isize, TERM_LENGTH)) };

            let ret = Self {
                term_buffers: [
                    l_log_buffers.atomic_buffer(0),
                    l_log_buffers.atomic_buffer(1),
                    l_log_buffers.atomic_buffer(2),
                ],
                log_meta_data_buffer: l_log_buffers.atomic_buffer(log_buffer_descriptor::LOG_META_DATA_SECTION_INDEX),
                src_buffer: AtomicBuffer::from_aligned(src_buf),
                counter_values_buffer: counters_buffer,
                log_buffers: l_log_buffers,
                subscriber_position: UnsafeBufferPosition::new(counters_buffer, SUBSCRIBER_POSITION_ID),
            };

            ret.term_buffers[0].set_memory(0, ret.term_buffers[0].capacity(), 0);
            ret.term_buffers[1].set_memory(0, ret.term_buffers[1].capacity(), 0);
            ret.term_buffers[2].set_memory(0, ret.term_buffers[2].capacity(), 0);
            ret.log_meta_data_buffer.set_memory(0, ret.log_meta_data_buffer.capacity(), 0);
            ret.counter_values_buffer
                .set_memory(0, ret.counter_values_buffer.capacity(), 0);

            ret.log_meta_data_buffer
                .put::<i32>(*log_buffer_descriptor::LOG_MTU_LENGTH_OFFSET, 3 * ret.src_buffer.capacity());
            ret.log_meta_data_buffer
                .put::<i32>(*log_buffer_descriptor::LOG_TERM_LENGTH_OFFSET, TERM_LENGTH);
            ret.log_meta_data_buffer
                .put::<i32>(*log_buffer_descriptor::LOG_PAGE_SIZE_OFFSET, PAGE_SIZE);
            ret.log_meta_data_buffer
                .put::<i32>(*log_buffer_descriptor::LOG_INITIAL_TERM_ID_OFFSET, INITIAL_TERM_ID);

            ret
        }

        pub fn insert_data_frame(&self, active_term_id: i32, offset: i32) {
            let term_buffer_index = log_buffer_descriptor::index_by_term(INITIAL_TERM_ID, active_term_id);
            let buffer: AtomicBuffer = self.term_buffers[term_buffer_index as usize];
            let frame = buffer.overlay_struct::<DataFrameHeaderDefn>(offset);
            let msg_length = DATA.len() as Index;

            unsafe {
                (*frame).frame_length = data_frame_header::LENGTH + msg_length;
                (*frame).version = data_frame_header::CURRENT_VERSION;
                (*frame).flags = frame_descriptor::UNFRAGMENTED;
                (*frame).frame_type = data_frame_header::HDR_TYPE_DATA;
                (*frame).term_offset = offset;
                (*frame).session_id = SESSION_ID;
                (*frame).stream_id = STREAM_ID;
                (*frame).term_id = active_term_id;
            }
            buffer.put_bytes(offset + data_frame_header::LENGTH, DATA.as_ref());
        }

        pub fn insert_padding_frame(&self, active_term_id: i32, offset: i32) {
            let term_buffer_index = log_buffer_descriptor::index_by_term(INITIAL_TERM_ID, active_term_id);
            let buffer: AtomicBuffer = self.term_buffers[term_buffer_index as usize];
            let frame = buffer.overlay_struct::<DataFrameHeaderDefn>(offset);

            unsafe {
                (*frame).frame_length = TERM_LENGTH - offset;
                (*frame).version = data_frame_header::CURRENT_VERSION;
                (*frame).flags = frame_descriptor::UNFRAGMENTED;
                (*frame).frame_type = data_frame_header::HDR_TYPE_PAD;
                (*frame).term_offset = offset;
                (*frame).session_id = SESSION_ID;
                (*frame).stream_id = STREAM_ID;
                (*frame).term_id = active_term_id;
            }
        }

        pub fn offset_of_frame(index: Index) -> Index {
            index * *ALIGNED_FRAME_LENGTH
        }
    }

    #[test]
    fn check_partition_count() {
        assert_eq!(log_buffer_descriptor::PARTITION_COUNT, 3); // partition count assumed to be 3 for these tests
    }

    #[test]
    fn should_report_correct_initial_term_id() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let message_index = 0;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            INITIAL_TERM_ID,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );

        image_test.subscriber_position.set(initial_position);
        let image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image.initial_term_id(), INITIAL_TERM_ID);
    }

    #[test]
    fn should_report_correct_term_buffer_length() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let message_index = 0;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            INITIAL_TERM_ID,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );

        image_test.subscriber_position.set(initial_position);
        let image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image.term_buffer_length(), TERM_LENGTH);
    }

    fn fragment_handler_check_len(_buf: &AtomicBuffer, _offset: Index, length: Index, _header: &Header) {
        assert_eq!(length as usize, DATA.len());
    }

    #[test]
    fn should_report_correct_position_on_reception() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let message_index = 0;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            INITIAL_TERM_ID,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index));

        let fragments = image.poll(&mut fragment_handler_check_len, std::i32::MAX);

        assert_eq!(fragments, 1);
        assert_eq!(
            image_test.subscriber_position.get(),
            initial_position + *ALIGNED_FRAME_LENGTH as i64 as i64
        );
        assert_eq!(image.position(), initial_position + *ALIGNED_FRAME_LENGTH as i64 as i64);
    }

    #[test]
    fn should_report_correct_position_on_reception_with_non_zero_position_initial_term_id() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let message_index = 5;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            INITIAL_TERM_ID,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index));

        let fragments = image.poll(&mut fragment_handler_check_len, std::i32::MAX);
        assert_eq!(fragments, 1);
        assert_eq!(
            image_test.subscriber_position.get(),
            initial_position + *ALIGNED_FRAME_LENGTH as i64
        );
        assert_eq!(image.position(), initial_position + *ALIGNED_FRAME_LENGTH as i64);
    }

    #[test]
    fn should_report_correct_position_on_reception_with_non_zero_position_in_noninitial_term_id() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let active_term_id = INITIAL_TERM_ID + 1;
        let message_index = 5;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            active_term_id,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        image_test.insert_data_frame(active_term_id, ImageTest::offset_of_frame(message_index));

        let fragments = image.poll(&mut fragment_handler_check_len, std::i32::MAX);
        assert_eq!(fragments, 1);
        assert_eq!(
            image_test.subscriber_position.get(),
            initial_position + *ALIGNED_FRAME_LENGTH as i64
        );
        assert_eq!(image.position(), initial_position + *ALIGNED_FRAME_LENGTH as i64);
    }

    #[test]
    fn should_ensure_image_is_open_before_reading_position() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let message_index = 0;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            INITIAL_TERM_ID,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        image.close();
        assert_eq!(image.position(), initial_position);
    }

    #[test]
    fn should_ensure_image_is_open_before_poll() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let message_index = 0;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            INITIAL_TERM_ID,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        image.close();
        assert_eq!(image.poll(&mut fragment_handler_check_len, std::i32::MAX), 0);
    }

    #[test]
    fn should_poll_no_fragments_to_bounded_fragment_handler_with_max_position_before_initial_position() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let message_index = 0;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            INITIAL_TERM_ID,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );
        let max_position = initial_position - data_frame_header::LENGTH as i64;

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index));
        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index + 1));

        let fragments = image.bounded_poll(fragment_handler_check_len, max_position, std::i32::MAX);
        assert_eq!(fragments, 0);
        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);
    }

    #[test]
    fn should_poll_fragments_to_bounded_fragment_handler_within_itial_offset_not_zero() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let message_index = 1;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            INITIAL_TERM_ID,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );
        let max_position = initial_position + *ALIGNED_FRAME_LENGTH as i64;

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index));
        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index + 1));

        let fragments = image.bounded_poll(fragment_handler_check_len, max_position, std::i32::MAX);
        assert_eq!(fragments, 1);
        assert_eq!(image_test.subscriber_position.get(), max_position);
        assert_eq!(image.position(), max_position);
    }

    #[test]
    fn should_poll_fragments_to_bounded_fragment_handler_with_max_position_before_next_message() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let message_index = 0;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            INITIAL_TERM_ID,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );
        let max_position = initial_position + *ALIGNED_FRAME_LENGTH as i64;

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index));
        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index + 1));

        let fragments = image.bounded_poll(fragment_handler_check_len, max_position, std::i32::MAX);
        assert_eq!(fragments, 1);
        assert_eq!(image_test.subscriber_position.get(), max_position);
        assert_eq!(image.position(), max_position);
    }

    #[test]
    fn should_poll_fragments_to_bounded_fragment_handler_with_max_position_after_end_of_term() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);
        let initial_offset = TERM_LENGTH - (*ALIGNED_FRAME_LENGTH * 2);
        let initial_position =
            log_buffer_descriptor::compute_position(INITIAL_TERM_ID, initial_offset, *POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        let max_position = initial_position + TERM_LENGTH as i64;

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        image_test.insert_data_frame(INITIAL_TERM_ID, initial_offset);
        image_test.insert_padding_frame(INITIAL_TERM_ID, initial_offset + *ALIGNED_FRAME_LENGTH);

        let fragments = image.bounded_poll(fragment_handler_check_len, max_position, std::i32::MAX);
        assert_eq!(fragments, 1);
        assert_eq!(image_test.subscriber_position.get(), TERM_LENGTH as i64);
        assert_eq!(image.position(), TERM_LENGTH as i64);
    }

    #[test]
    fn should_poll_fragments_to_bounded_fragment_handler_with_max_position_above_int_max_value() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);
        let initial_offset = TERM_LENGTH - (*ALIGNED_FRAME_LENGTH * 2);
        let initial_position =
            log_buffer_descriptor::compute_position(INITIAL_TERM_ID, initial_offset, *POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        let max_position = std::i32::MAX as i64 + 1000;

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        image_test.insert_data_frame(INITIAL_TERM_ID, initial_offset);
        image_test.insert_padding_frame(INITIAL_TERM_ID, initial_offset + *ALIGNED_FRAME_LENGTH);

        let fragments = image.bounded_poll(fragment_handler_check_len, max_position, std::i32::MAX);
        assert_eq!(fragments, 1);
        assert_eq!(image_test.subscriber_position.get(), TERM_LENGTH as i64);
        assert_eq!(image.position(), TERM_LENGTH as i64);
    }

    #[test]
    fn should_poll_no_fragments_to_controlled_fragment_handler() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let message_index = 0;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            INITIAL_TERM_ID,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        let fragments = image.controlled_poll(controlled_poll_handler, std::i32::MAX);
        assert_eq!(fragments, 0);
        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);
    }

    #[test]
    fn should_poll_one_fragment_to_controlled_fragment_handler_on_continue() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);
        let message_index = 0;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            INITIAL_TERM_ID,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index));

        let fragments = image.controlled_poll(controlled_poll_handler_continue, std::i32::MAX);
        assert_eq!(fragments, 1);
        assert_eq!(
            image_test.subscriber_position.get(),
            initial_position + *ALIGNED_FRAME_LENGTH as i64
        );
        assert_eq!(image.position(), initial_position + *ALIGNED_FRAME_LENGTH as i64);
    }

    #[test]
    fn should_not_poll_one_fragment_to_controlled_fragment_handler_on_abort() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let message_index = 0;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            INITIAL_TERM_ID,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index));

        let fragments = image.controlled_poll(controlled_poll_handler_abort, std::i32::MAX);
        assert_eq!(fragments, 0);
        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);
    }

    #[test]
    fn should_poll_one_fragment_to_controlled_fragment_handler_on_break() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let message_index = 0;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            INITIAL_TERM_ID,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index));
        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index + 1));

        let fragments = image.controlled_poll(controlled_poll_handler_break, std::i32::MAX);
        assert_eq!(fragments, 1);
        assert_eq!(
            image_test.subscriber_position.get(),
            initial_position + *ALIGNED_FRAME_LENGTH as i64
        );
        assert_eq!(image.position(), initial_position + *ALIGNED_FRAME_LENGTH as i64);
    }

    #[test]
    fn should_poll_fragments_to_controlled_fragment_handler_on_commit() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let message_index = 0;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            INITIAL_TERM_ID,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index));
        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index + 1));

        let fragments = image.controlled_poll(controlled_poll_handler_commit, std::i32::MAX);
        assert_eq!(fragments, 1);
        assert_eq!(
            image_test.subscriber_position.get(),
            initial_position + *ALIGNED_FRAME_LENGTH as i64
        );
        assert_eq!(image.position(), initial_position + *ALIGNED_FRAME_LENGTH as i64);
    }

    #[test]
    fn should_poll_fragments_to_controlled_fragment_handler_on_continue() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let message_index = 0;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            INITIAL_TERM_ID,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index));
        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index + 1));

        let fragments = image.controlled_poll(controlled_handler_cont_cont, std::i32::MAX);
        assert_eq!(fragments, 2);
        assert_eq!(
            image_test.subscriber_position.get(),
            initial_position + *ALIGNED_FRAME_LENGTH as i64 * 2
        );
        assert_eq!(image.position(), initial_position + *ALIGNED_FRAME_LENGTH as i64 * 2);
    }

    #[test]
    fn should_poll_no_fragments_to_bounded_controlled_fragment_handler_with_max_position_before_initial_position() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let message_index = 0;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            INITIAL_TERM_ID,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );
        let max_position = initial_position - data_frame_header::LENGTH as i64;

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index));
        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index + 1));

        let fragments = image.bounded_controlled_poll(controlled_poll_handler, max_position, std::i32::MAX);
        assert_eq!(fragments, 0);
        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);
    }

    #[test]
    fn should_poll_fragments_to_bounded_controlled_fragment_handler_with_initial_offset_not_zero() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let message_index = 1;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            INITIAL_TERM_ID,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );
        let max_position = initial_position + *ALIGNED_FRAME_LENGTH as i64;

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index));
        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index + 1));

        let fragments = image.bounded_controlled_poll(controlled_poll_handler_continue, max_position, std::i32::MAX);
        assert_eq!(fragments, 1);
        assert_eq!(image_test.subscriber_position.get(), max_position);
        assert_eq!(image.position(), max_position);
    }

    #[test]
    fn should_poll_fragments_to_bounded_controlled_fragment_handler_with_max_position_before_next_message() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let message_index = 0;
        let initial_term_offset = ImageTest::offset_of_frame(message_index);
        let initial_position = log_buffer_descriptor::compute_position(
            INITIAL_TERM_ID,
            initial_term_offset,
            *POSITION_BITS_TO_SHIFT,
            INITIAL_TERM_ID,
        );
        let max_position = initial_position + *ALIGNED_FRAME_LENGTH as i64;

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index));
        image_test.insert_data_frame(INITIAL_TERM_ID, ImageTest::offset_of_frame(message_index + 1));

        let fragments = image.bounded_controlled_poll(controlled_poll_handler_continue, max_position, std::i32::MAX);
        assert_eq!(fragments, 1);
        assert_eq!(image_test.subscriber_position.get(), max_position);
        assert_eq!(image.position(), max_position);
    }

    #[test]
    fn should_poll_fragments_to_bounded_controlled_fragment_handler_with_max_position_after_end_of_term() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let initial_offset = TERM_LENGTH - (*ALIGNED_FRAME_LENGTH * 2);
        let initial_position =
            log_buffer_descriptor::compute_position(INITIAL_TERM_ID, initial_offset, *POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        let max_position = initial_position + TERM_LENGTH as i64;

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        image_test.insert_data_frame(INITIAL_TERM_ID, initial_offset);
        image_test.insert_padding_frame(INITIAL_TERM_ID, initial_offset + *ALIGNED_FRAME_LENGTH);

        let fragments = image.bounded_controlled_poll(controlled_poll_handler_continue, max_position, std::i32::MAX);
        assert_eq!(fragments, 1);
        assert_eq!(image_test.subscriber_position.get(), TERM_LENGTH as i64);
        assert_eq!(image.position(), TERM_LENGTH as i64);
    }

    #[test]
    fn should_poll_fragments_to_bounded_controlled_fragment_handler_with_max_position_above_int_max_value() {
        let log_buf = AlignedBuffer::with_capacity(LOG_BUFFER_LENGTH);
        let src_buf = AlignedBuffer::with_capacity(SRC_BUFFER_LENGTH);
        let cnt_buf = AlignedBuffer::with_capacity(COUNTER_VALUES_BUFFER_LENGTH);
        let image_test = ImageTest::new(&log_buf, &src_buf, &cnt_buf);

        let initial_offset = TERM_LENGTH - (*ALIGNED_FRAME_LENGTH * 2);
        let initial_position =
            log_buffer_descriptor::compute_position(INITIAL_TERM_ID, initial_offset, *POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
        let max_position = std::i32::MAX as i64 + 1000;

        image_test.subscriber_position.set(initial_position);
        let mut image = Image::create(
            SESSION_ID,
            CORRELATION_ID,
            SUBSCRIPTION_REGISTRATION_ID,
            CString::new(SOURCE_IDENTITY).unwrap(),
            &image_test.subscriber_position,
            image_test.log_buffers.clone(),
            Box::new(error_handler),
        );

        assert_eq!(image_test.subscriber_position.get(), initial_position);
        assert_eq!(image.position(), initial_position);

        image_test.insert_data_frame(INITIAL_TERM_ID, initial_offset);
        image_test.insert_padding_frame(INITIAL_TERM_ID, initial_offset + *ALIGNED_FRAME_LENGTH);

        let fragments = image.bounded_controlled_poll(controlled_poll_handler_continue, max_position, std::i32::MAX);
        assert_eq!(fragments, 1);
        assert_eq!(image_test.subscriber_position.get(), TERM_LENGTH as i64);
        assert_eq!(image.position(), TERM_LENGTH as i64);
    }
}
