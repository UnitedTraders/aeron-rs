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
    ffi::CString,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use crate::utils::errors::{IllegalArgumentError, IllegalStateError};
use crate::{
    client_conductor::ClientConductor,
    concurrent::{
        atomic_buffer::AtomicBuffer,
        logbuffer::{
            buffer_claim::BufferClaim,
            data_frame_header, frame_descriptor,
            header::HeaderWriter,
            log_buffer_descriptor,
            term_appender::{default_reserved_value_supplier, OnReservedValueSupplier, TermAppender},
        },
        position::{ReadablePosition, UnsafeBufferPosition},
        status::status_indicator_reader,
    },
    ttrace,
    utils::{bit_utils::number_of_trailing_zeroes, errors::AeronError, log_buffers::LogBuffers, types::Index},
};

pub trait BulkPubSize {
    const SIZE: usize;
}

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

#[allow(dead_code)]
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
    #[allow(clippy::too_many_arguments)]
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
            log_meta_data_buffer: log_md_buffer,
            channel,
            registration_id,
            original_registration_id,
            log_buffers: log_buffers.clone(),
            max_possible_position: (log_buffers.atomic_buffer(0).capacity() as i64) << 31,
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
    pub fn position(&self) -> Result<i64, AeronError> {
        if !self.is_closed() {
            let raw_tail = log_buffer_descriptor::raw_tail_volatile(&self.log_meta_data_buffer);
            let term_offset = log_buffer_descriptor::term_offset(raw_tail, self.term_buffer_length() as i64);

            Ok(log_buffer_descriptor::compute_position(
                log_buffer_descriptor::term_id(raw_tail),
                term_offset as Index,
                self.position_bits_to_shift,
                self.initial_term_id,
            ))
        } else {
            Err(AeronError::PublicationClosed)
        }
    }

    /**
     * Get the position limit beyond which this {@link Publication} will be back pressured.
     *
     * This should only be used as a guide to determine when back pressure is likely to be applied.
     *
     * @    the position limit beyond which this {@link Publication} will be back pressured.
     */
    pub fn publication_limit(&self) -> Result<i64, AeronError> {
        if self.is_closed() {
            Err(AeronError::PublicationClosed)
        } else {
            Ok(self.publication_limit.get_volatile())
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
    pub fn available_window(&self) -> Result<i64, AeronError> {
        if !self.is_closed() {
            Ok(self.publication_limit.get_volatile() - self.position()?)
        } else {
            Err(AeronError::PublicationClosed)
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
    ) -> Result<u64, AeronError> {
        if !self.is_closed() {
            let limit = self.publication_limit.get_volatile();
            let term_count = log_buffer_descriptor::active_term_count(&self.log_meta_data_buffer);
            let term_appender = &self.appenders[log_buffer_descriptor::index_by_term_count(term_count as i64) as usize];
            let raw_tail = term_appender.raw_tail_volatile();
            let term_offset = raw_tail & 0xFFFF_FFFF;
            let term_id = log_buffer_descriptor::term_id(raw_tail);
            let position =
                log_buffer_descriptor::compute_term_begin_position(term_id, self.position_bits_to_shift, self.initial_term_id)
                    + term_offset;

            if term_count != (term_id - self.initial_term_id) {
                return Err(AeronError::AdminAction);
            }

            if position < limit {
                let resulting_offset = if length <= self.max_payload_length {
                    ttrace!("Appending unfragmented message on publication {}", self.registration_id);
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
                    ttrace!("Appending fragmented message on publication {}", self.registration_id);
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

                Ok(self.new_position(
                    term_count,
                    term_offset as i32,
                    term_id,
                    position,
                    resulting_offset.expect("Something wrong with resulting offset"),
                )?)
            } else {
                ttrace!(
                    "Current stream position is out of limit on publication {}",
                    self.registration_id
                );
                Err(self.back_pressure_status(position, length))
            }
        } else {
            ttrace!(
                "Unsuccessful attempt to publish a message via closed publication {}",
                self.registration_id
            );
            Err(AeronError::PublicationClosed)
        }
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
    pub fn offer_part(&self, buffer: AtomicBuffer, offset: Index, length: Index) -> Result<u64, AeronError> {
        self.offer_opt(buffer, offset, length, default_reserved_value_supplier)
    }

    /**
     * Non-blocking publish of a buffer containing a message.
     *
     * @param buffer containing message.
     * @    The new stream position on success, otherwise {@link BACK_PRESSURED} or {@link NOT_CONNECTED}.
     */
    pub fn offer(&self, buffer: AtomicBuffer) -> Result<u64, AeronError> {
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
    ) -> Result<u64, AeronError> {
        let length: Index = buffers.iter().map(|&ab| ab.capacity()).sum();

        if length == std::i32::MAX {
            return Err(IllegalStateError::LengthOverflow(length).into());
        }

        if !self.is_closed() {
            let limit = self.publication_limit.get_volatile();
            let term_count = log_buffer_descriptor::active_term_count(&self.log_meta_data_buffer);
            let term_appender = &mut self.appenders[(log_buffer_descriptor::index_by_term_count(term_count as i64)) as usize];
            let raw_tail = term_appender.raw_tail_volatile();
            let term_offset = raw_tail & 0xFFFF_FFFF;
            let term_id = log_buffer_descriptor::term_id(raw_tail);
            let position =
                log_buffer_descriptor::compute_term_begin_position(term_id, self.position_bits_to_shift, self.initial_term_id)
                    + term_offset;

            if term_count != (term_id - self.initial_term_id) {
                return Err(AeronError::AdminAction);
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
                        return Err(IllegalArgumentError::EncodedMessageExceedsMaxMessageLength {
                            length,
                            max_message_length: self.max_message_length,
                        }
                        .into());
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

                Ok(self.new_position(
                    term_count,
                    term_offset as i32,
                    term_id,
                    position,
                    resulting_offset.expect("Error getting resulting_offset"),
                )?)
            } else {
                Err(self.back_pressure_status(position, length as Index))
            }
        } else {
            Err(AeronError::PublicationClosed)
        }
    }

    /**
     * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
     * Once the message has been written then {@link BufferClaim#commit(&self)} should be called thus making it available.
     * <p>
     * <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
     *
     * @param length      of the range to claim, in bytes..
     * @param buffer_claim to be populate if the claim succeeds.
     * @    The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     * @throws IllegalArgumentException if the length is greater than max payload length within an MTU.
     * @see BufferClaim::commit
     */
    pub fn try_claim(&mut self, length: Index, buffer_claim: &mut BufferClaim) -> Result<u64, AeronError> {
        self.check_payload_length(length)?;

        if !self.is_closed() {
            let limit = self.publication_limit.get_volatile();
            let term_count = log_buffer_descriptor::active_term_count(&self.log_meta_data_buffer);
            let term_appender = &mut self.appenders[log_buffer_descriptor::index_by_term_count(term_count as i64) as usize];
            let raw_tail = term_appender.raw_tail_volatile();
            let term_offset = raw_tail & 0xFFFF_FFFF;
            let term_id = log_buffer_descriptor::term_id(raw_tail);
            let position =
                log_buffer_descriptor::compute_term_begin_position(term_id, self.position_bits_to_shift, self.initial_term_id)
                    + term_offset;

            if term_count != (term_id - self.initial_term_id) {
                return Err(AeronError::AdminAction);
            }

            if position < limit {
                let resulting_offset = term_appender.claim(&self.header_writer, length, buffer_claim, term_id);
                Ok(self.new_position(
                    term_count,
                    term_offset as i32,
                    term_id,
                    position,
                    resulting_offset.expect("Error getting resulting_offset"),
                )?)
            } else {
                Err(self.back_pressure_status(position, length))
            }
        } else {
            Err(AeronError::PublicationClosed)
        }
    }

    /**
     * Add a destination manually to a multi-destination-cast Publication.
     *
     * @param endpoint_channel for the destination to add
     * @    correlation id for the add command
     */
    pub fn add_destination(&mut self, endpoint_channel: CString) -> Result<i64, AeronError> {
        if self.is_closed() {
            return Err(IllegalStateError::PublicationClosed.into());
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
            return Err(IllegalStateError::PublicationClosed.into());
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

    fn new_position(
        &self,
        term_count: Index,
        term_offset: Index,
        term_id: i32,
        position: i64,
        resulting_offset: Index,
    ) -> Result<u64, AeronError> {
        if resulting_offset > 0 {
            let new_position = (position - term_offset as i64) + resulting_offset as i64;
            return if new_position >= 0 {
                Ok(new_position as u64)
            } else {
                Err(AeronError::UnknownCode(new_position))
            };
        }

        if position as i64 + term_offset as i64 > self.max_possible_position {
            return Err(AeronError::MaxPositionExceeded);
        }

        log_buffer_descriptor::rotate_log(&self.log_meta_data_buffer, term_count, term_id);

        Err(AeronError::AdminAction)
    }

    fn back_pressure_status(&self, current_position: i64, message_length: i32) -> AeronError {
        if current_position + message_length as i64 >= self.max_possible_position {
            return AeronError::MaxPositionExceeded;
        }

        if log_buffer_descriptor::is_connected(&self.log_meta_data_buffer) {
            return AeronError::BackPressured;
        }

        AeronError::NotConnected
    }

    fn check_max_message_length(&self, length: Index) -> Result<(), AeronError> {
        if length > self.max_message_length {
            Err(IllegalArgumentError::EncodedMessageExceedsMaxMessageLength {
                length,
                max_message_length: self.max_message_length,
            }
            .into())
        } else {
            Ok(())
        }
    }

    fn check_payload_length(&self, length: Index) -> Result<(), AeronError> {
        if length > self.max_payload_length {
            Err(IllegalArgumentError::EncodedMessageExceedsMaxPayloadLength {
                length,
                max_payload_length: self.max_payload_length,
            }
            .into())
        } else {
            Ok(())
        }
    }
}

impl Drop for Publication {
    fn drop(&mut self) {
        self.is_closed.store(true, Ordering::Release);
        let _unused = self
            .conductor
            .lock()
            .expect("Mutex poisoned")
            .release_publication(self.registration_id);
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::sync::{Arc, Mutex};

    use lazy_static::lazy_static;

    use crate::{
        client_conductor::ClientConductor,
        concurrent::{
            atomic_buffer::{AlignedBuffer, AtomicBuffer},
            broadcast::{
                broadcast_buffer_descriptor, broadcast_receiver::BroadcastReceiver,
                copy_broadcast_receiver::CopyBroadcastReceiver,
            },
            counters::CountersReader,
            logbuffer::{
                buffer_claim::BufferClaim,
                data_frame_header::LENGTH,
                frame_descriptor,
                log_buffer_descriptor::{self, AERON_PAGE_MIN_SIZE, TERM_MIN_LENGTH},
            },
            position::{ReadablePosition, UnsafeBufferPosition},
            ring_buffer::{self, ManyToOneRingBuffer},
            status::status_indicator_reader::{StatusIndicatorReader, NO_ID_ALLOCATED},
        },
        driver_proxy::DriverProxy,
        publication::Publication,
        utils::{
            errors::AeronError,
            log_buffers::LogBuffers,
            misc::unix_time_ms,
            types::{Index, Moment, I64_SIZE},
        },
    };

    lazy_static! {
        pub static ref CHANNEL: CString = CString::new("aeron:udp?endpoint=localhost:40123").unwrap();
    }
    const STREAM_ID: i32 = 10;
    const SESSION_ID: i32 = 200;
    const PUBLICATION_LIMIT_COUNTER_ID: i32 = 0;

    const CORRELATION_ID: i64 = 100;
    const ORIGINAL_REGISTRATION_ID: i64 = 100;
    const TERM_ID_1: i32 = 1;

    const DRIVER_TIMEOUT_MS: Moment = 10 * 1000;
    const RESOURCE_LINGER_TIMEOUT_MS: Moment = 5 * 1000;
    const INTER_SERVICE_TIMEOUT_NS: Moment = 5 * 1000 * 1000 * 1000;
    const INTER_SERVICE_TIMEOUT_MS: Moment = INTER_SERVICE_TIMEOUT_NS / 1_000_000;
    const PRE_TOUCH_MAPPED_MEMORY: bool = false;

    // const LOG_FILE_LENGTH: i32 = ((TERM_MIN_LENGTH * 3) + log_buffer_descriptor::LOG_META_DATA_LENGTH);

    const CAPACITY: i32 = 1024;
    const MANY_TO_ONE_RING_BUFFER_LENGTH: i32 = CAPACITY + ring_buffer::TRAILER_LENGTH;
    const BROADCAST_BUFFER_LENGTH: i32 = CAPACITY + broadcast_buffer_descriptor::TRAILER_LENGTH;
    // const COUNTER_VALUES_BUFFER_LENGTH: i32 = 1024 * 1024;
    const COUNTER_METADATA_BUFFER_LENGTH: i32 = 4 * 1024 * 1024;

    #[inline]
    fn raw_tail_value(term_id: i32, position: i64) -> i64 {
        (term_id as i64 * (1_i64 << 32)) as i64 | position
    }

    #[inline]
    fn term_tail_counter_offset(index: i32) -> Index {
        *log_buffer_descriptor::TERM_TAIL_COUNTER_OFFSET + (index * I64_SIZE)
    }

    fn on_new_publication_handler(_channel: CString, _stream_id: i32, _session_id: i32, _correlation_id: i64) {}

    fn on_new_exclusive_publication_handler(_channel: CString, _stream_id: i32, _session_id: i32, _correlation_id: i64) {}

    fn on_new_subscription_handler(_channel: CString, _stream_id: i32, _correlation_id: i64) {}

    fn error_handler(err: AeronError) {
        println!("Got error: {:?}", err);
    }

    fn on_available_counter_handler(_counters_reader: &CountersReader, _registration_id: i64, _counter_id: i32) {}

    fn on_unavailable_counter_handler(_counters_reader: &CountersReader, _registration_id: i64, _counter_id: i32) {}

    fn on_close_client_handler() {}

    #[allow(dead_code)]
    struct PublicationTest {
        src: AlignedBuffer,
        log: AlignedBuffer,

        conductor: Arc<Mutex<ClientConductor>>,
        to_driver: AlignedBuffer,
        to_clients: AlignedBuffer,
        counter_metadata: AlignedBuffer,
        counter_values: AlignedBuffer,

        to_driver_buffer: AtomicBuffer,
        to_clients_buffer: AtomicBuffer,

        many_to_one_ring_buffer: Arc<ManyToOneRingBuffer>,

        term_buffers: [AtomicBuffer; 3],
        log_meta_data_buffer: AtomicBuffer,
        src_buffer: AtomicBuffer,

        log_buffers: Arc<LogBuffers>,
        publication_limit: UnsafeBufferPosition,
        channel_status_indicator: StatusIndicatorReader,
        publication: Publication,
    }

    impl PublicationTest {
        pub fn new() -> Self {
            let log = AlignedBuffer::with_capacity(TERM_MIN_LENGTH * 3 + log_buffer_descriptor::LOG_META_DATA_LENGTH);
            let src = AlignedBuffer::with_capacity(1024);
            let src_buffer = AtomicBuffer::from_aligned(&src);
            let log_buffers =
                Arc::new(unsafe { LogBuffers::new(log.ptr, log.len as isize, log_buffer_descriptor::TERM_MIN_LENGTH) });

            let to_driver = AlignedBuffer::with_capacity(MANY_TO_ONE_RING_BUFFER_LENGTH);
            let to_clients = AlignedBuffer::with_capacity(BROADCAST_BUFFER_LENGTH);
            let counter_metadata = AlignedBuffer::with_capacity(BROADCAST_BUFFER_LENGTH);
            let counter_values = AlignedBuffer::with_capacity(COUNTER_METADATA_BUFFER_LENGTH);

            let to_driver_buffer = AtomicBuffer::from_aligned(&to_driver);
            let to_clients_buffer = AtomicBuffer::from_aligned(&to_clients);
            let counters_metadata_buffer = AtomicBuffer::from_aligned(&counter_metadata);
            let counters_values_buffer = AtomicBuffer::from_aligned(&counter_values);

            let local_to_driver_ring_buffer =
                Arc::new(ManyToOneRingBuffer::new(to_driver_buffer).expect("Failed to create RingBuffer"));
            let local_to_clients_broadcast_receiver = Arc::new(Mutex::new(
                BroadcastReceiver::new(to_clients_buffer).expect("Failed to create BroadcastReceiver"),
            ));
            let local_driver_proxy = Arc::new(DriverProxy::new(local_to_driver_ring_buffer.clone()));
            let local_copy_broadcast_receiver =
                Arc::new(Mutex::new(CopyBroadcastReceiver::new(local_to_clients_broadcast_receiver)));

            let conductor = ClientConductor::new(
                unix_time_ms,
                local_driver_proxy,
                local_copy_broadcast_receiver,
                counters_metadata_buffer,
                counters_values_buffer,
                Box::new(on_new_publication_handler),
                Box::new(on_new_exclusive_publication_handler),
                Box::new(on_new_subscription_handler),
                Box::new(error_handler),
                Box::new(on_available_counter_handler),
                Box::new(on_unavailable_counter_handler),
                Box::new(on_close_client_handler),
                DRIVER_TIMEOUT_MS,
                RESOURCE_LINGER_TIMEOUT_MS,
                INTER_SERVICE_TIMEOUT_MS,
                PRE_TOUCH_MAPPED_MEMORY,
            );

            let conductor_guard = conductor.lock().expect("Conductor mutex is poisoned");
            let publication_limit =
                UnsafeBufferPosition::new(conductor_guard.counter_values_buffer(), PUBLICATION_LIMIT_COUNTER_ID);
            let channel_status_indicator = StatusIndicatorReader::new(conductor_guard.counter_values_buffer(), NO_ID_ALLOCATED);
            drop(conductor_guard);

            let log_meta_data_buffer = log_buffers.atomic_buffer(log_buffer_descriptor::LOG_META_DATA_SECTION_INDEX);
            log_meta_data_buffer.put(*log_buffer_descriptor::LOG_MTU_LENGTH_OFFSET, 3 * src_buffer.capacity());
            log_meta_data_buffer.put(*log_buffer_descriptor::LOG_TERM_LENGTH_OFFSET, TERM_MIN_LENGTH);
            log_meta_data_buffer.put(*log_buffer_descriptor::LOG_PAGE_SIZE_OFFSET, AERON_PAGE_MIN_SIZE);
            log_meta_data_buffer.put(*log_buffer_descriptor::LOG_INITIAL_TERM_ID_OFFSET, TERM_ID_1);

            log_meta_data_buffer.put(*log_buffer_descriptor::LOG_ACTIVE_TERM_COUNT_OFFSET, 0);
            log_meta_data_buffer.put(term_tail_counter_offset(0), (TERM_ID_1 as i64) << 32);

            for i in 1..log_buffer_descriptor::PARTITION_COUNT {
                let expected_term_id = (TERM_ID_1 + i) - log_buffer_descriptor::PARTITION_COUNT;
                log_meta_data_buffer.put(term_tail_counter_offset(i), (expected_term_id as i64) << 32);
            }

            Self {
                src,
                log,
                conductor: conductor.clone(),
                to_driver,
                to_clients,
                counter_metadata,
                counter_values,
                to_driver_buffer,
                to_clients_buffer,
                many_to_one_ring_buffer: local_to_driver_ring_buffer,
                term_buffers: [
                    log_buffers.atomic_buffer(0),
                    log_buffers.atomic_buffer(1),
                    log_buffers.atomic_buffer(2),
                ],

                log_meta_data_buffer,
                src_buffer,
                log_buffers: log_buffers.clone(),
                publication_limit: publication_limit.clone(),
                channel_status_indicator,
                publication: Publication::new(
                    conductor,
                    (*CHANNEL).clone(),
                    CORRELATION_ID,
                    ORIGINAL_REGISTRATION_ID,
                    STREAM_ID,
                    SESSION_ID,
                    publication_limit,
                    NO_ID_ALLOCATED,
                    log_buffers,
                ),
            }
        }
    }

    #[test]
    fn should_report_initial_position() {
        let test = PublicationTest::new();
        let position = test.publication.position();
        assert!(position.is_ok());
        assert_eq!(position.unwrap(), 0);
    }

    #[test]
    fn should_report_max_message_length() {
        let test = PublicationTest::new();
        assert_eq!(
            test.publication.max_message_length(),
            frame_descriptor::compute_max_message_length(TERM_MIN_LENGTH)
        );
    }

    #[test]
    fn should_report_correct_term_buffer_length() {
        let test = PublicationTest::new();
        assert_eq!(test.publication.term_buffer_length(), TERM_MIN_LENGTH);
    }

    #[test]
    fn should_report_that_publication_has_not_been_connected_yet() {
        let test = PublicationTest::new();
        log_buffer_descriptor::set_is_connected(&test.log_meta_data_buffer, false);
        assert!(!test.publication.is_connected());
    }

    #[test]
    fn should_report_that_publication_has_been_connected_yet() {
        let test = PublicationTest::new();
        log_buffer_descriptor::set_is_connected(&test.log_meta_data_buffer, true);
        assert!(test.publication.is_connected());
    }

    #[test]
    fn should_ensure_the_publication_is_open_before_reading_position() {
        let test = PublicationTest::new();
        test.publication.close();

        let position = test.publication.position();
        assert!(position.is_err());
        assert_eq!(position.unwrap_err(), AeronError::PublicationClosed);
    }

    #[test]
    fn should_ensure_the_publication_is_open_before_offer() {
        let test = PublicationTest::new();
        test.publication.close();
        assert!(test.publication.is_closed());

        let offer_result = test.publication.offer(test.src_buffer);
        assert!(offer_result.is_err());
        assert_eq!(offer_result.unwrap_err(), AeronError::PublicationClosed);
    }

    #[test]
    fn should_ensure_the_publication_is_open_before_claim() {
        let mut test = PublicationTest::new();
        let mut buffer_claim = BufferClaim::default();

        test.publication.close();
        assert!(test.publication.is_closed());

        let claim_result = test.publication.try_claim(1024, &mut buffer_claim);
        assert!(claim_result.is_err());
        assert_eq!(claim_result.unwrap_err(), AeronError::PublicationClosed);
    }

    #[test]
    fn should_offer_a_message_upon_construction() {
        let test = PublicationTest::new();
        let expected_position = test.src_buffer.capacity() + LENGTH;
        test.publication_limit.set(2 * test.src_buffer.capacity() as i64);

        assert_eq!(
            test.publication.offer(test.src_buffer).unwrap() as i64,
            expected_position as i64
        );

        let position = test.publication.position();
        assert!(position.is_ok());
        assert_eq!(position.unwrap(), expected_position as i64);
    }

    #[test]
    fn should_fail_to_offer_a_message_when_limited() {
        let test = PublicationTest::new();

        test.publication_limit.set(0);

        let offer_result = test.publication.offer(test.src_buffer);
        assert!(offer_result.is_err());
        assert_eq!(offer_result.unwrap_err(), AeronError::NotConnected);
    }

    #[test]
    fn should_fail_to_offer_when_append_fails() {
        let test = PublicationTest::new();
        let active_index = log_buffer_descriptor::index_by_term(TERM_ID_1, TERM_ID_1);
        let initial_position = TERM_MIN_LENGTH;

        test.log_meta_data_buffer.put(
            term_tail_counter_offset(active_index),
            raw_tail_value(TERM_ID_1, initial_position as i64),
        );
        test.publication_limit.set(i64::max_value() as i64);

        let position = test.publication.position();
        assert!(position.is_ok());
        assert_eq!(position.unwrap(), initial_position as i64);

        let offer_result = test.publication.offer(test.src_buffer);
        assert!(offer_result.is_err());
        assert_eq!(offer_result.unwrap_err(), AeronError::AdminAction);
    }

    #[test]
    fn should_rotate_when_append_trips() {
        let test = PublicationTest::new();
        let active_index = log_buffer_descriptor::index_by_term(TERM_ID_1, TERM_ID_1);
        let initial_position = TERM_MIN_LENGTH - LENGTH;

        test.log_meta_data_buffer.put(
            term_tail_counter_offset(active_index),
            raw_tail_value(TERM_ID_1, initial_position as i64),
        );
        test.publication_limit.set(i32::max_value() as i64);

        let position = test.publication.position();
        assert!(position.is_ok());
        assert_eq!(position.unwrap(), initial_position as i64);

        let offer_result = test.publication.offer(test.src_buffer);
        assert!(offer_result.is_err());
        assert_eq!(offer_result.unwrap_err(), AeronError::AdminAction);

        let next_index = log_buffer_descriptor::index_by_term(TERM_ID_1, TERM_ID_1 + 1);
        assert_eq!(
            test.log_meta_data_buffer
                .get::<i32>(*log_buffer_descriptor::LOG_ACTIVE_TERM_COUNT_OFFSET),
            1
        );
        assert_eq!(
            test.log_meta_data_buffer.get::<i64>(term_tail_counter_offset(next_index)),
            ((TERM_ID_1 + 1) as i64) << 32
        );

        assert!(
            test.publication.offer(test.src_buffer).unwrap() as i64
                > (initial_position + LENGTH + test.src_buffer.capacity()) as i64
        );

        let position = test.publication.position();
        assert!(position.is_ok());
        assert!(position.unwrap() > (initial_position + LENGTH + test.src_buffer.capacity()) as i64);
    }

    #[test]
    fn should_rotate_when_claim_trips() {
        let mut test = PublicationTest::new();
        let active_index = log_buffer_descriptor::index_by_term(TERM_ID_1, TERM_ID_1);
        let initial_position = TERM_MIN_LENGTH - LENGTH;

        test.log_meta_data_buffer.put(
            term_tail_counter_offset(active_index),
            raw_tail_value(TERM_ID_1, initial_position as i64),
        );
        test.publication_limit.set(i32::max_value() as i64);

        let mut buffer_claim = BufferClaim::default();

        let position = test.publication.position();
        assert!(position.is_ok());
        assert_eq!(position.unwrap(), initial_position as i64);

        let claim_result = test.publication.try_claim(1024, &mut buffer_claim);
        assert!(claim_result.is_err());
        assert_eq!(claim_result.unwrap_err(), AeronError::AdminAction);

        let next_index = log_buffer_descriptor::index_by_term(TERM_ID_1, TERM_ID_1 + 1);
        assert_eq!(
            test.log_meta_data_buffer
                .get::<i32>(*log_buffer_descriptor::LOG_ACTIVE_TERM_COUNT_OFFSET),
            1
        );
        assert_eq!(
            test.log_meta_data_buffer.get::<i64>(term_tail_counter_offset(next_index)),
            ((TERM_ID_1 + 1) as i64) << 32
        );

        assert!(
            test.publication.try_claim(1024, &mut buffer_claim).unwrap() as i64
                > (initial_position + LENGTH + test.src_buffer.capacity()) as i64
        );

        let position = test.publication.position();
        assert!(position.is_ok());
        assert!(position.unwrap() > (initial_position + LENGTH + test.src_buffer.capacity()) as i64);
    }
}
