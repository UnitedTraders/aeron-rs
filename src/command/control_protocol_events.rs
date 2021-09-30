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

use std::fmt;

/**
* List of event types used in the control protocol between the media driver and the core.
*/
#[repr(C)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum AeronCommand {
    Padding = -0x01,

    AddPublication = 0x01,
    RemovePublication = 0x02,
    AddExclusivePublication = 0x03,
    AddSubscription = 0x04,
    RemoveSubscription = 0x05,
    ClientKeepAlive = 0x06,
    AddDestination = 0x07,
    RemoveDestination = 0x08,
    AddCounter = 0x09,
    RemoveCounter = 0x0A,
    ClientClose = 0x0B,
    AddRcvDestination = 0x0C,
    RemoveRcvDestination = 0x0D,
    TerminateDriver = 0x0E,

    ResponseOnError = 0xF01,
    ResponseOnAvailableImage = 0xF02,
    ResponseOnPublicationReady = 0xF03,
    ResponseOnOperationSuccess = 0xF04,
    ResponseOnUnavailableImage = 0xF05,
    ResponseOnExclusivePublicationReady = 0xF06,
    ResponseOnSubscriptionReady = 0xF07,
    ResponseOnCounterReady = 0xF08,
    ResponseOnUnavailableCounter = 0xF9,
    ResponseOnClientTimeout = 0xF0A,

    #[cfg(test)]
    UnitTestMessageTypeID = 0x65,
}

impl fmt::LowerHex for AeronCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let val = *self as u8;

        fmt::LowerHex::fmt(&val, f)
    }
}

impl AeronCommand {
    pub fn from_command_id(command_id: i32) -> Self {
        match command_id {
            -0x01 => Self::Padding,

            0x01 => Self::AddPublication,
            0x02 => Self::RemovePublication,
            0x03 => Self::AddExclusivePublication,
            0x04 => Self::AddSubscription,
            0x05 => Self::RemoveSubscription,
            0x06 => Self::ClientKeepAlive,
            0x07 => Self::AddDestination,
            0x08 => Self::RemoveDestination,
            0x09 => Self::AddCounter,
            0x0A => Self::RemoveCounter,
            0x0B => Self::ClientClose,
            0x0C => Self::AddRcvDestination,
            0x0D => Self::RemoveRcvDestination,
            0x0E => Self::TerminateDriver,

            0xF01 => Self::ResponseOnError,
            0xF02 => Self::ResponseOnAvailableImage,
            0xF03 => Self::ResponseOnPublicationReady,
            0xF04 => Self::ResponseOnOperationSuccess,
            0xF05 => Self::ResponseOnUnavailableImage,
            0xF06 => Self::ResponseOnExclusivePublicationReady,
            0xF07 => Self::ResponseOnSubscriptionReady,
            0xF08 => Self::ResponseOnCounterReady,
            0xF09 => Self::ResponseOnUnavailableCounter,
            0xF0A => Self::ResponseOnClientTimeout,

            #[cfg(test)]
            0x65 => Self::UnitTestMessageTypeID,
            _ => unreachable!("Unexpected control protocol event: {:x}", command_id),
        }
    }
}
