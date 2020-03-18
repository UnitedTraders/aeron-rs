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

use crate::concurrent::broadcast::BroadcastTransmitError;
use std::fmt::Display;
use std::{fmt, io};

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
