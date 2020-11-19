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

use std::fmt::Display;
use std::{fmt, io};

use crate::concurrent::{self, broadcast::BroadcastTransmitError, ring_buffer::RingBufferError};

pub mod distinct_error_log;
pub mod error_log_descriptor;
pub mod error_log_reader;

#[derive(Debug)]
pub enum AeronError {
    GenericError(String),
    IllegalArgumentException(String),
    IllegalStateException(String),
    MemMappedFileError(io::Error),
    ConductorServiceTimeout(String),
    DriverTimeout(String),
    ReentrantException(String),
    RegistrationException(String),
    ChannelEndpointException((i64, String)), // correlation ID + error message
    ClientTimeoutException(String),
    BroadcastTransmitError(BroadcastTransmitError),
    RingBufferError(RingBufferError),
    NotConnected,
    BackPressured,
    AdminAction,
    PublicationClosed,
    MaxPositionExceeded,
    UnknownCode(i64),
}

impl Display for AeronError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AeronError::GenericError(msg) => write!(f, "Aeron error: {}", msg),
            AeronError::IllegalArgumentException(msg) => write!(f, "Illegal argument: {}", msg),
            AeronError::IllegalStateException(msg) => write!(f, "Illegal state: {}", msg),
            AeronError::MemMappedFileError(err) => write!(f, "MemMappedFileError: {:?}", err),
            AeronError::ConductorServiceTimeout(err) => write!(f, "ConductorServiceTimeout: {:?}", err),
            AeronError::DriverTimeout(err) => write!(f, "DriverTimeout: {:?}", err),
            AeronError::ReentrantException(err) => write!(f, "ReentrantException: {:?}", err),
            AeronError::RegistrationException(err) => write!(f, "RegistrationException: {:?}", err),
            AeronError::ChannelEndpointException(err) => write!(f, "ChannelEndpointException: {:?}", err),
            AeronError::ClientTimeoutException(err) => write!(f, "ClientTimeoutException: {:?}", err),
            AeronError::BroadcastTransmitError(err) => write!(f, "BroadcastTransmitError: {:?}", err),
            AeronError::RingBufferError(err) => write!(f, "RingBufferError: {:?}", err),
            AeronError::NotConnected => write!(f, "Offer failed because publisher is not connected to subscriber"),
            AeronError::BackPressured => write!(f, "Offer failed due to back pressure"),
            AeronError::AdminAction => write!(f, "Offer failed because of an administration action in the system"),
            AeronError::PublicationClosed => write!(f, "Offer failed publication is closed"),
            AeronError::MaxPositionExceeded => write!(f, "Max possible position exceeded"),
            AeronError::UnknownCode(code) => write!(f, "Unknown code {} on getting position", code),
        }
    }
}

impl std::convert::From<concurrent::ring_buffer::RingBufferError> for AeronError {
    fn from(err: RingBufferError) -> Self {
        AeronError::RingBufferError(err)
    }
}

impl std::convert::From<concurrent::broadcast::BroadcastTransmitError> for AeronError {
    fn from(err: BroadcastTransmitError) -> Self {
        AeronError::BroadcastTransmitError(err)
    }
}

impl PartialEq for AeronError {
    fn eq(&self, other: &Self) -> bool {
        // Errors are equal if they have same type regardless the content of data (error message) inside
        std::mem::discriminant(self) == std::mem::discriminant(other)
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
