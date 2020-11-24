use thiserror::Error;

use crate::utils::types::Index;

pub mod broadcast_buffer_descriptor;
pub mod broadcast_receiver;
pub mod broadcast_transmitter;
pub mod copy_broadcast_receiver;
pub mod record_descriptor;

#[derive(Debug, Eq, PartialEq, Error)]
pub enum BroadcastTransmitError {
    #[error("Encoded message exceeds max_msg_length={max_msg_length}, msg_length={length}")]
    EncodedMessageExceedsMaxMsgLength { max_msg_length: Index, length: Index },
    #[error("{0} is not power of two")]
    NotPowerOfTwo(Index),
    #[error("Message id should be greater then zero, {0} provided")]
    MessageIdShouldBeGreaterThenZero(i32),
    #[error("Unable to keep up with broadcast buffer")]
    UnableToKeepUpWithBroadcastBuffer,
    #[error("Buffer is too small: Capacity = {capacity}, needed {need}")]
    BufferTooSmall { need: Index, capacity: Index },
}
