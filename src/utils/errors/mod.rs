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

use crate::client_conductor::RegistrationStatus;
use crate::concurrent::{broadcast::BroadcastTransmitError, ring_buffer::RingBufferError};

pub mod distinct_error_log;
pub mod error_log_descriptor;
pub mod error_log_reader;

#[derive(Debug, Error)]
pub enum AeronError {
    #[error("Aeron error: {0}")]
    GenericError(#[from] GenericError),
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
    DriverTimeout(#[from] DriverTimeoutError),
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

#[derive(Error, Debug)]
pub enum DriverTimeoutError {
    #[error("CnC file not created: {file_name}")]
    CncNotCreated { file_name: String },
    #[error("CnC file is created but not initialised: {file_name}")]
    CncCreatedButNotInitialised { file_name: String },
    #[error("No driver heartbeat detected")]
    NoHeartbeatDetected,
    #[error("Driver has been inactive for over {0} ms")]
    WasInactive(u64),
    #[error("Driver is inactive")]
    Inactive,
    #[error("No response from driver in {0} ms")]
    NoResponse(u64),
}

#[derive(Error, Debug)]
pub enum GenericError {
    #[error("Aeron CnC version does not match:  app={app_version} file={file_version}")]
    CncVersionDoesntMatch { app_version: String, file_version: String },
    #[error("Agent start failed: {msg:?}")]
    AgentStartFailed { msg: Option<io::Error> },
    #[error("Aeron client conductor is closed")]
    ClientConductorClosed,
    #[error("Client heartbeat timestamp not active")]
    ClientHeartbeatNotActive,
    #[error("Publication already dropped")]
    PublicationAlreadyDropped,
    #[error("Buffers was not set for Publication with registration_id {registration_id}")]
    BufferNotSetForPublication { registration_id: i64 },
    #[error("Publication not found")]
    PublicationNotFound,
    #[error("Unknown registration_id: {0}")]
    UnknownRegistrationId(i64),
    #[error("Exclusive publication already dropped")]
    ExclusivePublicationAlreadyDropped,
    #[error("Exclusive publication not ready yet, status {status:?}")]
    ExclusivePublicationNotReadyYet { status: RegistrationStatus },
    #[error("")]
    BufferNotSetForExclusivePublication { registartion_id: i64 },
    #[error("Exclusive publication not found")]
    ExclusivePublicationNotFound,
    #[error("Subscription already dropped")]
    SubscriptionAlreadyDropped,
    #[error("Subscription is not presented, because wasn't created before. Status {status:?}")]
    SubscriptionWasNotCreatedBefore { status: RegistrationStatus },
    #[error("Subscription not found")]
    SubscriptionNotFound,
    #[error("Counter already dropped")]
    CounterAlreadyDropped,
    #[error("Counter not ready yet, status {status:?}")]
    CounterNotReadyYet { status: RegistrationStatus },
    #[error("Counter is not presented, because wasn't created before. Status {status:?}")]
    CounterWasNotCreatedBefore { status: RegistrationStatus },
    #[error("Counter not found")]
    CounterNotFound,
    #[error("Unknown correlation_id: {0}")]
    UnknownCorrelationId(i64),
    #[error("String to CString conversion failed for endpoint_channel")]
    StringToCStringConversionFailed,
    #[error("{0}")]
    Custom(String),
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
