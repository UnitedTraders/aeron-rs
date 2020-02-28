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

use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::concurrent::counters::CountersReader;
use crate::utils::types::Index;

const CHANNEL_ENDPOINT_INITIALIZING: i64 = 0;
const CHANNEL_ENDPOINT_ERRORED: i64 = -1;
const CHANNEL_ENDPOINT_ACTIVE: i64 = 1;
const CHANNEL_ENDPOINT_CLOSING: i64 = 2;

const NO_ID_ALLOCATED: i32 = -1;

struct StatusIndicatorReader {
    buffer: AtomicBuffer,
    id: i32,
    offset: Index,
    static_buffer: Option<[u8; 8]>,
}

impl StatusIndicatorReader {
    pub fn new(input_buffer: AtomicBuffer, id: i32) -> Self {
        if NO_ID_ALLOCATED == id {
            let mut static_buffer: [u8; 8] = [0; 8];
            let buffer = AtomicBuffer::wrap_slice(&mut static_buffer);
            buffer.put_ordered::<i64>(0, CHANNEL_ENDPOINT_ACTIVE);

            Self {
                static_buffer: Some(static_buffer),
                buffer: AtomicBuffer::wrap(input_buffer),
                id,
                offset: 0,
            }
        } else {
            Self {
                buffer: AtomicBuffer::wrap(input_buffer),
                id,
                offset: CountersReader::counter_offset(id),
                static_buffer: None,
            }
        }
    }

    // pub fn from_indicator_reader(reader: StatusIndicatorReader) {
    //     Self::new()
    //     AtomicBuffer::from_aligned()
    //     if NO_ID_ALLOCATED == m_id
    //     {
    //         m_buffer.wrap(m_staticBuffer);
    //     }
    //     else
    //     {
    //         m_buffer.wrap(indicatorReader.m_buffer);
    //     }
    //
    //     m_buffer.putInt64Ordered(m_offset, indicatorReader.m_buffer.getInt64Volatile(indicatorReader.m_offset));
    // }

    // StatusIndicatorReader& operator=(const StatusIndicatorReader& indicatorReader)
    // {
    // m_staticBuffer = indicatorReader.m_staticBuffer;
    // m_id = indicatorReader.m_id;
    // m_offset = indicatorReader.m_offset;
    //
    // if (ChannelEndpointStatus::NO_ID_ALLOCATED == m_id)
    // {
    // m_buffer.wrap(m_staticBuffer);
    // }
    // else
    // {
    // m_buffer.wrap(indicatorReader.m_buffer);
    // }
    //
    // m_buffer.putInt64Ordered(m_offset, indicatorReader.m_buffer.getInt64Volatile(indicatorReader.m_offset));
    //
    // return *this;
    // }

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
        let buffer = AlignedBuffer::with_capacity(16);
        let atomic_buffer = AtomicBuffer::from_aligned(&buffer);

        let reader = StatusIndicatorReader::new(atomic_buffer, 239);
        assert_eq!(reader.id(), 239);
        assert_eq!(reader.offset, CountersReader::counter_offset(239));
        assert_eq!(
            reader.volatile(),
            reader.buffer.get_volatile::<i64>(CountersReader::counter_offset(239))
        );
    }

    #[test]
    fn test_id_not_allocated() {
        let buffer = AlignedBuffer::with_capacity(16);
        let atomic_buffer = AtomicBuffer::from_aligned(&buffer);

        let reader = StatusIndicatorReader::new(atomic_buffer, NO_ID_ALLOCATED);
        assert_eq!(reader.id(), NO_ID_ALLOCATED);
        assert_eq!(reader.offset, 0);
        assert_eq!(
            reader.volatile(),
            reader.buffer.get_volatile::<i64>(CountersReader::counter_offset(239))
        );
        assert_eq!(reader.buffer.as_slice(), &[]);
        assert_eq!(reader.static_buffer, Some([1, 0, 0, 0, 0, 0, 0, 0]));
    }
}
