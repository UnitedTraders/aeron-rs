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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::client_conductor::ClientConductor;
use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::concurrent::logbuffer::buffer_claim::BufferClaim;
use crate::concurrent::logbuffer::header::HeaderWriter;
use crate::concurrent::logbuffer::term_appender::{default_reserved_value_supplier, OnReservedValueSupplier, TermAppender};
use crate::concurrent::logbuffer::{data_frame_header, frame_descriptor, log_buffer_descriptor};
use crate::concurrent::position::{ReadablePosition, UnsafeBufferPosition};
use crate::concurrent::status::status_indicator_reader;
use crate::utils::bit_utils::number_of_trailing_zeroes;
use crate::utils::errors::AeronError;
use crate::utils::log_buffers::LogBuffers;
use crate::utils::types::Index;
use std::ffi::CString;

const NOT_CONNECTED: i64 = -1;
const BACK_PRESSURED: i64 = -2;
const ADMIN_ACTION: i64 = -3;
const PUBLICATION_CLOSED: i64 = -4;
const MAX_POSITION_EXCEEDED: i64 = -5;

pub trait BulkPubSize {
    const SIZE: usize;
}

// FIXME: this is temporary solution to introduce ExclusivePublication
// It needs to be carefully ported thereafter
pub type ExclusivePublication = Publication;

/**
 * @example basic_publisher.rs
 */
/**
 * Aeron Publisher API for sending messages to subscribers of a given channel and stream_id pair. Publishers
 * are created via an {@link Aeron} object, and messages are sent via an offer method or a claim and commit
 * method combination.
 * <p>
 * The APIs used to send are all non-blocking.
 * <p>
 * Note: Publication instances are threadsafe and can be shared between publisher threads.
 * @see Aeron#add_publication
 * @see Aeron#findPublication
 */

pub struct Publication {
    conductor: Arc<Mutex<ClientConductor>>,
    log_meta_data_buffer: AtomicBuffer,
    channel: CString,
    registration_id: i64,
    original_registration_id: i64,
    max_possible_position: i64,
    stream_id: i32,
    session_id: i32,
    initial_term_id: i32,
    max_payload_length: Index,
    max_message_length: Index,
    position_bits_to_shift: i32,
    publication_limit: UnsafeBufferPosition,
    channel_status_id: i32,
    is_closed: AtomicBool, // default to false

    // The LogBuffers object must be dropped when last ref to it goes out of scope.
    log_buffers: Arc<LogBuffers>,

    // it was unique_ptr on TermAppender's
    appenders: [TermAppender; log_buffer_descriptor::PARTITION_COUNT as usize],
    header_writer: HeaderWriter,
}

impl Publication {
    pub fn new(
        conductor: Arc<Mutex<ClientConductor>>,
        channel: CString,
        registration_id: i64,
        original_registration_id: i64,
        stream_id: i32,
        session_id: i32,
        publication_limit: UnsafeBufferPosition,
        channel_status_id: i32,
        log_buffers: Arc<LogBuffers>,
    ) -> Self {
        let log_md_buffer = log_buffers.atomic_buffer(log_buffer_descriptor::LOG_META_DATA_SECTION_INDEX);

        Self {
            conductor,
            log_meta_data_buffer: log_md_buffer.clone(),
            channel,
            registration_id,
            original_registration_id,
            log_buffers: log_buffers.clone(),
            max_possible_position: (log_buffers.atomic_buffer(0).capacity() << 31) as i64,
            stream_id,
            session_id,
            initial_term_id: log_buffer_descriptor::initial_term_id(&log_md_buffer),
            max_payload_length: log_buffer_descriptor::mtu_length(&log_md_buffer) as Index - data_frame_header::LENGTH,
            max_message_length: frame_descriptor::compute_max_message_length(log_buffers.atomic_buffer(0).capacity()),
            position_bits_to_shift: number_of_trailing_zeroes(log_buffers.atomic_buffer(0).capacity()),
            publication_limit,
            channel_status_id,
            is_closed: AtomicBool::from(false),
            header_writer: HeaderWriter::new(log_buffer_descriptor::default_frame_header(&log_md_buffer)),
            appenders: [
                TermAppender::new(
                    log_buffers.atomic_buffer(0),
                    log_buffers.atomic_buffer(log_buffer_descriptor::LOG_META_DATA_SECTION_INDEX),
                    0,
                ),
                TermAppender::new(
                    log_buffers.atomic_buffer(1),
                    log_buffers.atomic_buffer(log_buffer_descriptor::LOG_META_DATA_SECTION_INDEX),
                    1,
                ),
                TermAppender::new(
                    log_buffers.atomic_buffer(2),
                    log_buffers.atomic_buffer(log_buffer_descriptor::LOG_META_DATA_SECTION_INDEX),
                    2,
                ),
            ],
        }
    }

    /**
     * Media address for delivery to the channel.
     *
     * @    Media address for delivery to the channel.
     */
    pub fn channel(&self) -> CString {
        self.channel.clone()
    }

    /**
     * Stream identity for scoping within the channel media address.
     *
     * @    Stream identity for scoping within the channel media address.
     */
    pub fn stream_id(&self) -> i32 {
        self.stream_id
    }

    /**
     * Session under which messages are published. Identifies this Publication instance.
     *
     * @    the session id for this publication.
     */
    pub fn session_id(&self) -> i32 {
        self.session_id
    }

    /**
     * The initial term id assigned when this Publication was created. This can be used to determine how many
     * terms have passed since creation.
     *
     * @    the initial term id.
     */
    pub fn initial_term_id(&self) -> i32 {
        self.initial_term_id
    }

    /**
     * Get the original registration used to register this Publication with the media driver by the first publisher.
     *
     * @    the original registration_id of the publication.
     */
    pub fn original_registration_id(&self) -> i64 {
        self.original_registration_id
    }

    /**
     * Registration Id returned by Aeron::add_publication when this Publication was added.
     *
     * @    the registration_id of the publication.
     */
    pub fn registration_id(&self) -> i64 {
        self.registration_id
    }

    /**
     * Is this Publication the original instance added to the driver? If not then it was added after another client
     * has already added the publication.
     *
     * @    true if this instance is the first added otherwise false.
     */
    pub fn is_original(&self) -> bool {
        self.original_registration_id == self.registration_id
    }

    /**
     * Maximum message length supported in bytes.
     *
     * @    maximum message length supported in bytes.
     */
    pub fn max_message_length(&self) -> Index {
        self.max_message_length
    }

    /**
     * Maximum length of a message payload that fits within a message fragment.
     *
     * This is he MTU length minus the message fragment header length.
     *
     * @    maximum message fragment payload length.
     */
    pub fn max_payload_length(&self) -> Index {
        self.max_payload_length
    }

    /**
     * Get the length in bytes for each term partition in the log buffer.
     *
     * @    the length in bytes for each term partition in the log buffer.
     */
    pub fn term_buffer_length(&self) -> i32 {
        self.appenders[0].term_buffer().capacity()
    }

    /**
     * Number of bits to right shift a position to get a term count for how far the stream has progressed.
     *
     * @    of bits to right shift a position to get a term count for how far the stream has progressed.
     */
    pub fn position_bits_to_shift(&self) -> i32 {
        self.position_bits_to_shift
    }

    /**
     * Has this Publication seen an active subscriber recently?
     *
     * @    true if this Publication has seen an active subscriber recently.
     */
    pub fn is_connected(&self) -> bool {
        !self.is_closed() && log_buffer_descriptor::is_connected(&self.log_meta_data_buffer)
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @    true if it has been closed otherwise false.
     */
    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Acquire)
    }

    /**
     * Get the current position to which the publication has advanced for this stream.
     *
     * @    the current position to which the publication has advanced for this stream or {@link CLOSED}.
     */
    pub fn position(&self) -> i64 {
        let mut result = PUBLICATION_CLOSED;

        if !self.is_closed() {
            let raw_tail = log_buffer_descriptor::raw_tail_volatile(&self.log_meta_data_buffer);
            let term_offset = log_buffer_descriptor::term_offset(raw_tail, self.term_buffer_length() as i64);

            result = log_buffer_descriptor::compute_position(
                log_buffer_descriptor::term_id(raw_tail),
                term_offset as Index,
                self.position_bits_to_shift,
                self.initial_term_id,
            );
        }

        result
    }

    /**
     * Get the position limit beyond which this {@link Publication} will be back pressured.
     *
     * This should only be used as a guide to determine when back pressure is likely to be applied.
     *
     * @    the position limit beyond which this {@link Publication} will be back pressured.
     */
    pub fn publication_limit(&self) -> i64 {
        if self.is_closed() {
            PUBLICATION_CLOSED
        } else {
            self.publication_limit.get_volatile()
        }
    }

    /**
     * Get the counter id used to represent the publication limit.
     *
     * @    the counter id used to represent the publication limit.
     */
    pub fn publication_limit_id(&self) -> i32 {
        self.publication_limit.id()
    }

    /**
     * Available window for offering into a publication before the {@link #positionLimit(&self)} is reached.
     *
     * @     window for offering into a publication before the {@link #positionLimit(&self)} is reached. If
     * the publication is closed then {@link #CLOSED} will be returned.
     */
    pub fn available_window(&self) -> i64 {
        if !self.is_closed() {
            self.publication_limit.get_volatile() - self.position()
        } else {
            PUBLICATION_CLOSED
        }
    }

    /**
     * Get the counter id used to represent the channel status.
     *
     * @    the counter id used to represent the channel status.
     */
    pub fn channel_status_id(&self) -> i32 {
        self.channel_status_id
    }

    /**
     * Non-blocking publish of a buffer containing a message.
     *
     * @param buffer containing message.
     * @param offset offset in the buffer at which the encoded message begins.
     * @param length in bytes of the encoded message.
     * @param reserved_value_supplier for the frame.
     * @    The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     */
    pub fn offer_opt(
        &self,
        buffer: AtomicBuffer,
        offset: Index,
        length: Index,
        reserved_value_supplier: OnReservedValueSupplier,
    ) -> Result<i64, AeronError> {
        let mut new_position = PUBLICATION_CLOSED;

        if !self.is_closed() {
            let limit = self.publication_limit.get_volatile();
            let term_count = log_buffer_descriptor::active_term_count(&self.log_meta_data_buffer);
            let term_appender = &self.appenders[log_buffer_descriptor::index_by_term_count(term_count as i64) as usize];
            let raw_tail = term_appender.raw_tail_volatile();
            let term_offset = raw_tail & 0xFFFFFFFF;
            let term_id = log_buffer_descriptor::term_id(raw_tail);
            let position =
                log_buffer_descriptor::compute_term_begin_position(term_id, self.position_bits_to_shift, self.initial_term_id)
                    + term_offset;

            if term_count != (term_id - self.initial_term_id) {
                return Ok(ADMIN_ACTION);
            }

            if position < limit {
                let resulting_offset = if length <= self.max_payload_length {
                    term_appender.append_unfragmented_message(
                        &self.header_writer,
                        &buffer,
                        offset,
                        length,
                        reserved_value_supplier,
                        term_id,
                    )
                } else {
                    self.check_max_message_length(length)?;
                    term_appender.append_fragmented_message(
                        &self.header_writer,
                        &buffer,
                        offset,
                        length,
                        self.max_payload_length,
                        reserved_value_supplier,
                        term_id,
                    )
                };

                new_position = self.new_position(
                    term_count,
                    term_offset as i32,
                    term_id,
                    position as Index,
                    resulting_offset.expect("Something wrong with resulting offset"),
                ); // FIXME: overflow of Index possible here
            } else {
                new_position = self.back_pressure_status(position, length);
            }
        }

        Ok(new_position)
    }

    /**
     * Non-blocking publish of a buffer containing a message.
     *
     * @param buffer containing message.
     * @param offset offset in the buffer at which the encoded message begins.
     * @param length in bytes of the encoded message.
     * @    The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     */
    pub fn offer_part(&self, buffer: AtomicBuffer, offset: Index, length: Index) -> Result<i64, AeronError> {
        self.offer_opt(buffer, offset, length, default_reserved_value_supplier)
    }

    /**
     * Non-blocking publish of a buffer containing a message.
     *
     * @param buffer containing message.
     * @    The new stream position on success, otherwise {@link BACK_PRESSURED} or {@link NOT_CONNECTED}.
     */
    pub fn offer(&self, buffer: AtomicBuffer) -> Result<i64, AeronError> {
        self.offer_part(buffer, 0, buffer.capacity())
    }

    /**
     * Non-blocking publish of buffers containing a message.
     *
     * @param startBuffer containing part of the message.
     * @param lastBuffer after the message.
     * @param reserved_value_supplier for the frame.
     * @    The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     */
    // NOT implemented. Translate it from C++ if you need one.
    //pub fn offer_buf_iter<T>(&self, startBuffer: T, lastBuffer: T, reserved_value_supplier: OnReservedValueSupplier) -> Result<i64, AeronError> { }

    /**
     * Non-blocking publish of array of buffers containing a message.
     *
     * @param buffers containing parts of the message.
     * @param length of the array of buffers.
     * @param reserved_value_supplier for the frame.
     * @    The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     */
    // NOT implemented. Translate it from C++ if you need one.
    //pub fn offer_arr(&self, buffers[]: AtomicBuffer, length: Index, reserved_value_supplier: OnReservedValueSupplier) -> Result<i64, AeronError> {
    //    offer(buffers, buffers + length, reserved_value_supplier)
    //}
    /**
     * Non-blocking publish of array of buffers containing a message.
     * The buffers are in vector and will be appended to the log file in the sequence they appear in the vec.
     *
     * @param buffers containing parts of the message.
     * @param reserved_value_supplier for the frame.
     * @    The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     */
    pub fn offer_bulk(
        &mut self,
        buffers: Vec<AtomicBuffer>,
        reserved_value_supplier: OnReservedValueSupplier,
    ) -> Result<i64, AeronError> {
        let length: Index = buffers.iter().map(|&ab| ab.capacity()).sum();

        if length > std::i32::MAX {
            return Err(AeronError::IllegalStateException(format!("length overflow: {}", length)));
        }

        let mut new_position = PUBLICATION_CLOSED;

        if !self.is_closed() {
            let limit = self.publication_limit.get_volatile();
            let term_count = log_buffer_descriptor::active_term_count(&self.log_meta_data_buffer);
            let term_appender = &mut self.appenders[(log_buffer_descriptor::index_by_term_count(term_count as i64)) as usize];
            let raw_tail = term_appender.raw_tail_volatile();
            let term_offset = raw_tail & 0xFFFFFFFF;
            let term_id = log_buffer_descriptor::term_id(raw_tail);
            let position =
                log_buffer_descriptor::compute_term_begin_position(term_id, self.position_bits_to_shift, self.initial_term_id)
                    + term_offset;

            if term_count != (term_id - self.initial_term_id) {
                return Ok(ADMIN_ACTION);
            }

            if position < limit {
                let resulting_offset = if length <= self.max_payload_length {
                    term_appender.append_unfragmented_message_bulk(
                        &self.header_writer,
                        buffers,
                        length,
                        reserved_value_supplier,
                        term_id,
                    )
                } else {
                    if length > self.max_message_length {
                        return Err(AeronError::IllegalArgumentException(format!(
                            "encoded message exceeds max_message_length of {}, length={}",
                            self.max_message_length, length
                        )));
                    }

                    term_appender.append_fragmented_message_bulk(
                        &self.header_writer,
                        buffers,
                        length,
                        self.max_payload_length,
                        reserved_value_supplier,
                        term_id,
                    )
                };

                new_position = self.new_position(
                    term_count,
                    term_offset as i32,
                    term_id,
                    position as Index,
                    resulting_offset.expect("Error getting resulting_offset"),
                );
            } else {
                new_position = self.back_pressure_status(position, length as Index);
                // FIXME: possible Index overflow
            }
        }

        Ok(new_position)
    }

    /**
     * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
     * Once the message has been written then {@link BufferClaim#commit(&self)} should be called thus making it available.
     * <p>
     * <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
     *
     * @code
     *     BufferClaim buffer_claim; // Can be stored and reused to avoid allocation
     *
     *     if (publication->try_claim(message_length, buffer_claim) > 0)
     *     {
     *         try
     *         {
     *              AtomicBuffer& buffer = buffer_claim.buffer(&self);
     *              const index_t offset = buffer_claim.offset(&self);
     *
     *              // Work with buffer directly or wrap with a flyweight
     *         }
     *         finally
     *         {
     *             buffer_claim.commit(&self);
     *         }
     *     }
     * @endcode
     *
     * @param length      of the range to claim, in bytes..
     * @param buffer_claim to be populate if the claim succeeds.
     * @    The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     * @throws IllegalArgumentException if the length is greater than max payload length within an MTU.
     * @see BufferClaim::commit
     */
    pub fn try_claim(&mut self, length: Index, mut buffer_claim: BufferClaim) -> Result<i64, AeronError> {
        self.check_payload_length(length)?;
        let mut new_position = PUBLICATION_CLOSED;

        if !self.is_closed() {
            let limit = self.publication_limit.get_volatile();
            let term_count = log_buffer_descriptor::active_term_count(&self.log_meta_data_buffer);
            let term_appender = &mut self.appenders[log_buffer_descriptor::index_by_term_count(term_count as i64) as usize];
            let raw_tail = term_appender.raw_tail_volatile();
            let term_offset = raw_tail & 0xFFFFFFFF;
            let term_id = log_buffer_descriptor::term_id(raw_tail);
            let position =
                log_buffer_descriptor::compute_term_begin_position(term_id, self.position_bits_to_shift, self.initial_term_id)
                    + term_offset;

            if term_count != (term_id - self.initial_term_id) {
                return Ok(ADMIN_ACTION);
            }

            if position < limit {
                let resulting_offset = term_appender.claim(&self.header_writer, length, &mut buffer_claim, term_id);
                new_position = self.new_position(
                    term_count,
                    term_offset as i32,
                    term_id,
                    position as Index,
                    resulting_offset.expect("Error getting resulting_offset"),
                );
            } else {
                new_position = self.back_pressure_status(position, length);
            }
        }

        Ok(new_position)
    }

    /**
     * Add a destination manually to a multi-destination-cast Publication.
     *
     * @param endpoint_channel for the destination to add
     * @    correlation id for the add command
     */
    pub fn add_destination(&mut self, endpoint_channel: CString) -> Result<i64, AeronError> {
        if self.is_closed() {
            return Err(AeronError::IllegalStateException(String::from("Publication is closed")));
        }

        self.conductor
            .lock()
            .expect("Mutex poisoned")
            .add_destination(self.original_registration_id, endpoint_channel)
    }

    /**
     * Remove a previously added destination manually from a multi-destination-cast Publication.
     *
     * @param endpoint_channel for the destination to remove
     * @    correlation id for the remove command
     */
    pub fn remove_destination(&mut self, endpoint_channel: CString) -> Result<i64, AeronError> {
        if self.is_closed() {
            return Err(AeronError::IllegalStateException(String::from("Publication is closed")));
        }

        self.conductor
            .lock()
            .expect("Mutex poisoned")
            .remove_destination(self.original_registration_id, endpoint_channel)
    }

    /**
     * Retrieve the status of the associated add or remove destination operation with the given correlation_id.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the correlation_id is unknown, then an exception is thrown.
     * - If the media driver has not answered the add/remove command, then a false is returned.
     * - If the media driver has successfully added or removed the destination then true is returned.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Publication::add_destination
     * @see Publication::remove_destination
     *
     * @param correlation_id of the add/remove command returned by Publication::add_destination
     * or Publication::remove_destination
     * @    true for added or false if not.
     */
    pub fn find_destination_response(&mut self, correlation_id: i64) -> Result<bool, AeronError> {
        self.conductor
            .lock()
            .expect("Mutex poisoned")
            .find_destination_response(correlation_id)
    }

    /**
     * Get the status for the channel of this {@link Publication}
     *
     * @    status code for this channel
     */
    pub fn channel_status(&self) -> i64 {
        if self.is_closed() {
            return status_indicator_reader::NO_ID_ALLOCATED as i64;
        }

        self.conductor
            .lock()
            .expect("Mutex poisoned")
            .channel_status(self.channel_status_id)
    }

    pub fn close(&self) {
        self.is_closed.store(true, Ordering::Release);
    }

    fn new_position(&self, term_count: Index, term_offset: Index, term_id: i32, position: Index, resulting_offset: Index) -> i64 {
        if resulting_offset > 0 {
            return ((position - term_offset) + resulting_offset) as i64;
        }

        if position + term_offset > self.max_possible_position as Index {
            return MAX_POSITION_EXCEEDED;
        }

        log_buffer_descriptor::rotate_log(&self.log_meta_data_buffer, term_count, term_id);

        ADMIN_ACTION
    }

    fn back_pressure_status(&self, current_position: i64, message_length: i32) -> i64 {
        if current_position + message_length as i64 >= self.max_possible_position {
            return MAX_POSITION_EXCEEDED;
        }

        if log_buffer_descriptor::is_connected(&self.log_meta_data_buffer) {
            return BACK_PRESSURED;
        }

        NOT_CONNECTED
    }

    fn check_max_message_length(&self, length: Index) -> Result<(), AeronError> {
        if length > self.max_message_length {
            Err(AeronError::IllegalArgumentException(format!(
                "encoded message exceeds max_message_length of {}, length={}",
                self.max_message_length, length
            )))
        } else {
            Ok(())
        }
    }

    fn check_payload_length(&self, length: Index) -> Result<(), AeronError> {
        if length > self.max_payload_length {
            Err(AeronError::IllegalArgumentException(format!(
                "encoded message exceeds max_payload_length of {}, length={}",
                self.max_payload_length, length
            )))
        } else {
            Ok(())
        }
    }
}

impl Drop for Publication {
    fn drop(&mut self) {
        self.is_closed.store(true, Ordering::Release);
        self.conductor
            .lock()
            .expect("Mutex poisoned")
            .release_publication(self.registration_id);
    }
}
