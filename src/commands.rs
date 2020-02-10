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

impl AeronCommand {
    pub fn from_header(f: i32) -> Option<Self> {
        match f {
            -0x01 => Some(Self::Padding),

            0x01 => Some(Self::AddPublication),
            0x02 => Some(Self::RemovePublication),
            0x03 => Some(Self::AddExclusivePublication),
            0x04 => Some(Self::AddSubscription),
            0x05 => Some(Self::RemoveSubscription),
            0x06 => Some(Self::ClientKeepAlive),
            0x07 => Some(Self::AddDestination),
            0x08 => Some(Self::RemoveDestination),
            0x09 => Some(Self::AddCounter),
            0x0A => Some(Self::RemoveCounter),
            0x0B => Some(Self::ClientClose),
            0x0C => Some(Self::AddRcvDestination),
            0x0D => Some(Self::RemoveRcvDestination),
            0x0E => Some(Self::TerminateDriver),

            0xF01 => Some(Self::ResponseOnError),
            0xF02 => Some(Self::ResponseOnAvailableImage),
            0xF03 => Some(Self::ResponseOnPublicationReady),
            0xF04 => Some(Self::ResponseOnOperationSuccess),
            0xF05 => Some(Self::ResponseOnUnavailableImage),
            0xF06 => Some(Self::ResponseOnExclusivePublicationReady),
            0xF07 => Some(Self::ResponseOnSubscriptionReady),
            0xF08 => Some(Self::ResponseOnCounterReady),
            0xF9 => Some(Self::ResponseOnUnavailableCounter),
            0xF0A => Some(Self::ResponseOnClientTimeout),
            #[cfg(test)]
            0x65 => Some(Self::UnitTestMessageTypeID),
            _ => None,
        }
    }
}

#[repr(C)]
#[allow(dead_code)]
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
