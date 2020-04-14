/*
 * Copyright 2020 UT OVERSEAS INC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::concurrent::{atomic_buffer::AtomicBuffer, counters::CountersReader};
use crate::utils::types::{Index, I64_SIZE};

pub const CHANNEL_ENDPOINT_INITIALIZING: i64 = 0;
pub const CHANNEL_ENDPOINT_ERRORED: i64 = -1;
pub const CHANNEL_ENDPOINT_ACTIVE: i64 = 1;
pub const CHANNEL_ENDPOINT_CLOSING: i64 = 2;

pub const NO_ID_ALLOCATED: i32 = -1;

static mut STATIC_BUFFER_SLICE: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 0];

pub fn channel_status_to_str(status_id: i64) -> String {
    match status_id {
        CHANNEL_ENDPOINT_INITIALIZING => String::from("Initializing"),
        CHANNEL_ENDPOINT_ERRORED => String::from("Errored"),
        CHANNEL_ENDPOINT_ACTIVE => String::from("Active"),
        CHANNEL_ENDPOINT_CLOSING => String::from("Closing"),
        _ => String::from("Unknown"),
    }
}

fn static_buffer() -> AtomicBuffer {
    let buffer = unsafe {
        assert_eq!(STATIC_BUFFER_SLICE.len(), I64_SIZE as usize);
        AtomicBuffer::wrap_slice(&mut STATIC_BUFFER_SLICE)
    };
    buffer.put_ordered::<i64>(0, CHANNEL_ENDPOINT_ACTIVE);
    buffer
}

#[derive(Debug)]
pub struct StatusIndicatorReader {
    buffer: AtomicBuffer,
    id: i32,
    offset: Index,
}

impl StatusIndicatorReader {
    pub fn new(input_buffer: AtomicBuffer, id: i32) -> Self {
        if NO_ID_ALLOCATED == id {
            Self {
                buffer: AtomicBuffer::wrap(static_buffer()),
                id,
                offset: 0,
            }
        } else {
            let offset = CountersReader::counter_offset(id);
            Self {
                buffer: AtomicBuffer::wrap(input_buffer),
                id,
                offset,
            }
        }
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn volatile(&self) -> i64 {
        self.buffer.get_volatile::<i64>(self.offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::concurrent::atomic_buffer::AlignedBuffer;

    #[test]
    fn test() {
        let buffer = AlignedBuffer::with_capacity(65536);
        let atomic_buffer = AtomicBuffer::from_aligned(&buffer);

        let reader = StatusIndicatorReader::new(atomic_buffer, 239);
        let counters_reader = CountersReader::counter_offset(239);

        assert_eq!(reader.id(), 239);
        assert_eq!(reader.offset, counters_reader);
        assert_eq!(reader.volatile(), reader.buffer.get_volatile::<i64>(counters_reader));
    }

    #[test]
    fn test_id_not_allocated() {
        let buffer = AlignedBuffer::with_capacity(1024);
        let atomic_buffer = AtomicBuffer::from_aligned(&buffer);

        let reader = StatusIndicatorReader::new(atomic_buffer, NO_ID_ALLOCATED);
        assert_eq!(reader.id(), NO_ID_ALLOCATED);
        assert_eq!(reader.volatile(), reader.buffer.get_volatile::<i64>(reader.offset));
        assert_eq!(reader.buffer.as_slice(), &[1, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(reader.buffer.as_slice(), static_buffer().as_slice());
    }
}
