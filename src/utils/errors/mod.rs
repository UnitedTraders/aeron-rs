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

use crate::concurrent::{broadcast::BroadcastTransmitError, ring_buffer::RingBufferError};

pub mod distinct_error_log;
pub mod error_log_descriptor;
pub mod error_log_reader;

#[derive(Debug, Error)]
pub enum AeronError {
    #[error("Aeron error: {0}")]
    GenericError(String),
    #[error("Subscription {0} not ready")]
    SubscriptionNotReady(i64),
    #[error("Publication {0} not ready")]
    PublicationNotReady(i64),
    #[error("Illegal argument: {0}")]
    IllegalArgumentException(String),
    #[error("Illegal state: {0}")]
    IllegalStateException(String),
    #[error("MemMappedFileError: {0}")]
    MemMappedFileError(io::Error),
    #[error("ConductorServiceTimeout: {0}")]
    ConductorServiceTimeout(String),
    #[error("DriverTimeout: {0}")]
    DriverTimeout(String),
    #[error("ReentrantException: {0}")]
    ReentrantException(String),
    #[error("RegistrationException: {0}")]
    RegistrationException(String),
    #[error("ChannelEndpointException. For msg with correlation ID = {0} error occurred: {1}")]
    ChannelEndpointException(i64, String), // correlation ID + error message
    #[error("ClientTimeoutException: {0}")]
    ClientTimeoutException(String),
    #[error("BroadcastTransmitError: {0:?}")]
    BroadcastTransmitError(#[from] BroadcastTransmitError),
    #[error("RingBufferError: {0:?}")]
    RingBufferError(#[from] RingBufferError),
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
