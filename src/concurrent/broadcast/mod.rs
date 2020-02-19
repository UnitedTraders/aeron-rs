use crate::utils::types::Index;

pub mod broadcast_buffer_descriptor;
pub mod broadcast_receiver;
pub mod record_descriptor;
pub mod broadcast_transmitter;
pub mod copy_broadcast_receiver;

#[derive(Debug, Eq, PartialEq)]
pub enum BroadcastTransmitError {
    EncodedMessageExceedsMaxMsgLength {
        max_msg_length: Index,
        length: Index,
    },
    NotPowerOfTwo(Index),
    MessageIdShouldBeGreaterThenZero(i32),
    UnableToKeepUpWithBroadcastBuffer,
    BufferTooSmall { need: i32, capacity: i32 },
}
