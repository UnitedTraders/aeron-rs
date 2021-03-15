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

use std::hash::{Hash, Hasher};
use std::io;

use thiserror::Error;

use crate::channel_uri::State;
use crate::client_conductor::RegistrationStatus;
use crate::concurrent::{broadcast::BroadcastTransmitError, ring_buffer::RingBufferError};
use crate::utils::types::{Index, Moment};

pub mod distinct_error_log;
pub mod error_log_descriptor;
pub mod error_log_reader;

#[derive(Debug, Error)]
pub enum AeronError {
    #[error("Aeron error: {0}")]
    Generic(#[from] GenericError),
    #[error("Subscription {0} not ready")]
    SubscriptionNotReady(i64),
    #[error("Publication {0} not ready")]
    PublicationNotReady(i64),
    #[error("Illegal argument: {0}")]
    IllegalArgument(#[from] IllegalArgumentError),
    #[error("Illegal state: {0}")]
    IllegalState(#[from] IllegalStateError),
    #[error("MemMappedFileError: {0}")]
    MemMappedFileError(io::Error),
    #[error("DriverTimeout: {0}")]
    DriverTimeout(#[from] DriverInteractionError),
    #[error("ReentrantException: Client cannot be invoked within callback")]
    ReentrantException,
    #[error("RegistrationException: {0}")]
    RegistrationException(i32, String),
    #[error("ChannelEndpointException. For msg with correlation ID = {0} error occurred: {1}")]
    ChannelEndpointException(i64, String), // correlation ID + error message
    #[error("ClientTimeoutException from driver")]
    ClientTimeoutException,
    #[error("BroadcastTransmitError: {0:?}")]
    BroadcastTransmitError(#[from] BroadcastTransmitError),
    #[error("RingBufferError: {0:?}")]
    RingBuffer(#[from] RingBufferError),
    #[error("Offer failed because publisher is not connected to subscriber")]
    NotConnected,
    #[error("Offer failed due to back pressure")]
    BackPressured,
    #[error("Offer failed because of an administration action in the system")]
    AdminAction,
    #[error("Offer failed publication is closed")]
    PublicationClosed,
    #[error("Max possible position exceeded")]
    MaxPositionExceeded,
    #[error("Unknown code {0} on getting position")]
    UnknownCode(i64),
}

#[derive(Error, Debug)]
pub enum IllegalStateError {
    #[error("Action possibly delayed: expected_term_id={expected_term_id} term_id={term_id}")]
    ActionPossiblyDelayed { term_id: i32, expected_term_id: i32 },
    #[error("Couldn't write command to driver")]
    CouldNotWriteCommandToDriver,
    #[error("Encountered '{c}' within media definition at index {index} in '{uri}'")]
    EncounteredCharacterWithinMediaDefinition { c: char, index: usize, uri: String },
    #[error("Empty key is not allowed at index {index} in '{uri}'")]
    EmptyKeyNotAllowed { index: usize, uri: String },
    #[error("Frame header length {length} must be equal to {data_offset}")]
    FrameHeaderLengthMustBeEqualToDataOffset { length: Index, data_offset: Index },
    #[error("Invalid end of key at index {index} in '{uri}'")]
    InvalidEndOfKey { index: usize, uri: String },
    #[error("Length overflow: {0}")]
    LengthOverflow(i32),
    #[error("Max capacity was reached: {0}")]
    MaxCapacityReached(Index),
    #[error("Max frame length must be a multiple of {frame_alignment} , length = {length}")]
    MaxFrameLengthMustBeMultipleOfFrameAlignment { length: i32, frame_alignment: Index },
    #[error("Page size is not a power of 2, length= {0}")]
    PageSizeIsNotPowerOfTwo(i32),
    #[error("Page size is greater than max size of {page_max_size}, size= {page_size}")]
    PageSizeGreaterThanMaxPossibleSize { page_size: i32, page_max_size: Index },
    #[error("Page size is less than min size of {page_min_size}, size= {page_size}")]
    PageSizeLessThanMinPossibleSize { page_size: i32, page_min_size: Index },
    #[error("Publication is closed")]
    PublicationClosed,
    #[error("Subscription is closed")]
    SubscriptionClosed,
    #[error("Term length is greater than max size of {term_max_length} , length= {term_length}")]
    TermLengthIsGreaterThanMaxPossibleSize { term_length: i32, term_max_length: Index },
    #[error("Term length is less than min size of {term_min_length} , length= {term_length}")]
    TermLengthIsLessThanMinPossibleSize { term_length: i32, term_min_length: Index },
    #[error("Term length is not a power of 2, length= {0}")]
    TermLengthIsNotPowerOfTwo(i32),
}

#[derive(Error, Debug)]
pub enum DriverInteractionError {
    #[error("CnC file is created but not initialised: {file_name}")]
    CncCreatedButNotInitialised { file_name: String },
    #[error("CnC file not created: {file_name}")]
    CncNotCreated { file_name: String },
    #[error("Driver is inactive, on checking the status")]
    Inactive,
    #[error("No driver heartbeat detected")]
    NoHeartbeatDetected,
    #[error("No response from driver in {0} ms, interaction time was too long")]
    NoResponse(u64),
    #[error("Driver has been inactive for over {0} ms, marking as inactive")]
    WasInactive(u64),
}

#[derive(Error, Debug)]
pub enum IllegalArgumentError {
    #[error("Allocate: key is ambiguous")]
    AllocateKeyIsAmbiguous,
    #[error("Allocate: key is too long")]
    AllocateKeyIsTooLong,
    #[error("Allocate: label can't be converted")]
    AllocateLabelCanNotBeConverted,
    #[error("Allocate: label is too long")]
    AllocateLabelTooLong,
    #[error("{filename}:{line}: counter id {counter_id} out of range: max_counter_id={max_counter_id}")]
    CounterIdOutOfRange {
        filename: String,
        line: u32,
        counter_id: i32,
        max_counter_id: i32,
    },
    #[error("Encoded message exceeds max_message_length of {max_message_length}, length={length}")]
    EncodedMessageExceedsMaxMessageLength { length: i32, max_message_length: i32 },
    #[error("Encoded message exceeds max_payload_length of {max_payload_length}, length={length}")]
    EncodedMessageExceedsMaxPayloadLength { length: i32, max_payload_length: i32 },
    #[error("Invalid control mode: {0}")]
    InvalidControlMode(String),
    #[error("Invalid media: {0}")]
    InvalidMedia(String),
    #[error("Invalid prefix: {0}")]
    InvalidPrefix(String),
    #[error("Key length is out of bounds: length= {key_length}, limit= {limit}")]
    KeyLengthIsOutOfBounds { key_length: usize, limit: Index },
    #[error("Label length is out of bounds: length= {label_length}, limit= {limit}")]
    LabelLengthIsOutOfBounds { label_length: usize, limit: Index },
    #[error("Limit outside range: capacity={capacity}  limit={limit}")]
    LimitOutsideRange { capacity: Index, limit: Index },
    #[error("Linger value cannot be negative: {0}")]
    LingerValueCannotBeNegative(i64),
    #[error("MTU is not in range {left_bound}-{right_bound}: {0}")]
    MtuIsNotInRange { mtu: u32, left_bound: i32, right_bound: i32 },
    #[error("MTU not a multiple of FRAME_ALIGNMENT= {frame_alignment}: mtu= {mtu}")]
    MtuNotMultipleOfFrameAlignment { mtu: u32, frame_alignment: Index },
    #[error("New_position {new_position} is not aligned to FRAME_ALIGNMENT= {frame_alignment}")]
    NewPositionNotAlignedToFrameAlignment { new_position: i64, frame_alignment: Index },
    #[error("New position {new_position} is out of range {left_bound} - {right_bound}")]
    NewPositionOutOfRange {
        new_position: i64,
        left_bound: i64,
        right_bound: i64,
    },
    #[error("No more input found, state={state:?}")]
    NoMoreInputFound { state: State },
    #[error("Term offset is not in range 0-1g: {0}")]
    TermOffsetNotInRange(u32),
    #[error("Term offset is not a multiple of FRAME_ALIGNMENT= {frame_alignment}: offset= {term_offset}")]
    TermOffsetNotMultipleOfFrameAlignment { term_offset: u32, frame_alignment: Index },
    #[error("Unable to allocate counter, metadata buffer is full")]
    UnableAllocateCounterBecauseMetadataBufferFull,
    #[error("Unable to allocate counter, values buffer is full")]
    UnableAllocateCounterBecauseValueBufferFull,
    #[error("Unknown media: {0}")]
    UnknownMedia(String),
    #[error("Aeron URIs must start with 'aeron:', found: '{uri}'")]
    UriMustStartWithAeron { uri: String },
}

#[derive(Error, Debug)]
pub enum GenericError {
    #[error("Agent start failed: {msg:?}")]
    AgentStartFailed { msg: Option<io::Error> },
    #[error("Buffers was not set for ExclusivePublication with registration_id {registration_id}")]
    BufferNotSetForExclusivePublication { registration_id: i64 },
    #[error("Buffers was not set for Publication with registration_id {registration_id}")]
    BufferNotSetForPublication { registration_id: i64 },
    #[error("Aeron client conductor is closed")]
    ClientConductorClosed,
    #[error("Client heartbeat timestamp not active")]
    ClientHeartbeatNotActive,
    #[error("Aeron CnC version does not match:  app={app_version} file={file_version}")]
    CncVersionDoesntMatch { app_version: String, file_version: String },
    #[error("Counter already dropped")]
    CounterAlreadyDropped,
    #[error("Counter not ready yet, status {status:?}")]
    CounterNotReadyYet { status: RegistrationStatus },
    #[error("Counter is not presented, because wasn't created before. Status {status:?}")]
    CounterWasNotCreatedBefore { status: RegistrationStatus },
    #[error("Counter not found")]
    CounterNotFound,
    #[error("{0}")]
    Custom(String),
    #[error("Exclusive publication already dropped")]
    ExclusivePublicationAlreadyDropped,
    #[error("Exclusive publication not found")]
    ExclusivePublicationNotFound,
    #[error("Exclusive publication not ready yet, status {status:?}")]
    ExclusivePublicationNotReadyYet { status: RegistrationStatus },
    #[error("Publication already dropped")]
    PublicationAlreadyDropped,
    #[error("Publication not found")]
    PublicationNotFound,
    #[error("String to CString conversion failed for endpoint_channel")]
    StringToCStringConversionFailed,
    #[error("Subscription already dropped")]
    SubscriptionAlreadyDropped,
    #[error("Subscription not found")]
    SubscriptionNotFound,
    #[error("Subscription is not presented, because wasn't created before. Status {status:?}")]
    SubscriptionWasNotCreatedBefore { status: RegistrationStatus },
    #[error("Timeout between service calls over {0} ms")]
    TimeoutBetweenServiceCallsOverTimeout(Moment),
    #[error("Unknown correlation_id: {0}")]
    UnknownCorrelationId(i64),
    #[error("Unknown registration_id: {0}")]
    UnknownRegistrationId(i64),
}

impl PartialEq for AeronError {
    fn eq(&self, other: &Self) -> bool {
        // Errors are equal if they have same type regardless the content of data (error message) inside
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

impl Eq for AeronError {}

impl Hash for AeronError {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            AeronError::MemMappedFileError(err) => err.kind().hash(state),
            err => err.hash(state),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub enum AeronErrorCode {
    GenericError = 0,
    InvalidChannel = 1,
    UnknownSubscription = 2,
    UnknownPublication = 3,
    ChannelEndpointError = 4,
    UnknownCounter = 5,
    UnknownCommandTypeId = 10,
    MalformedCommand = 11,
    ErrorNotSupplied = 12,
}
