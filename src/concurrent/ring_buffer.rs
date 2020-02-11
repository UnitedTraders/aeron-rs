use cache_line_size::CACHE_LINE_SIZE;

use crate::commands::AeronCommand;
use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::utils::bit_utils::align;

use crate::utils::types::Index;

pub const CACHE_LINE_LENGTH: Index = CACHE_LINE_SIZE as Index;
pub const TAIL_POSITION_OFFSET: Index = CACHE_LINE_LENGTH * 2;
pub const HEAD_CACHE_POSITION_OFFSET: Index = CACHE_LINE_LENGTH * 4;
pub const HEAD_POSITION_OFFSET: Index = CACHE_LINE_LENGTH * 6;
pub const CORRELATION_COUNTER_OFFSET: Index = CACHE_LINE_LENGTH * 8;
pub const CONSUMER_HEARTBEAT_OFFSET: Index = CACHE_LINE_LENGTH * 10;
pub const TRAILER_LENGTH: Index = CACHE_LINE_LENGTH * 12;

//todo: rewrite all these index-based accessors using blitted structs + custom volatile cells?

/**
* Header length made up of fields for message length, message type, and then the encoded message.
* <p>
* Writing of the record length signals the message recording is complete.
* <pre>
*   0                   1                   2                   3
*   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
*  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*  |R|                       Record Length                         |
*  +-+-------------------------------------------------------------+
*  |                              Type                             |
*  +---------------------------------------------------------------+
*  |                       Encoded Message                        ...
* ...                                                              |
*  +---------------------------------------------------------------+
* </pre>
*/

struct RecordDescriptor {}

impl RecordDescriptor {
    pub const HEADER_LENGTH: Index = ::std::mem::size_of::<i32>() as Index * 2;
    pub const ALIGNMENT: Index = Self::HEADER_LENGTH;

    #[inline]
    pub fn make_header(len: Index, command: AeronCommand) -> i64 {
        (((command as i64) & 0xFFFF_FFFF) << 32) | ((len as i64) & 0xFFFF_FFFF)
    }

    #[inline]
    pub fn encoded_msg_offset(record_offset: Index) -> Index {
        record_offset + RecordDescriptor::HEADER_LENGTH
    }

    #[inline]
    pub fn length_offset(record_offset: Index) -> Index {
        record_offset
    }

    #[inline]
    pub fn type_offset(record_offset: Index) -> Index {
        record_offset + (::std::mem::size_of::<i32>() as Index)
    }

    #[inline]
    pub fn record_length(header: i64) -> Index {
        (header & 0xFFFF_FFFF) as Index
    }

    #[inline]
    pub fn message_type(header: i64) -> Result<AeronCommand, RingBufferError> {
        AeronCommand::from_header((header >> 32) as i32).ok_or_else(|| RingBufferError::UnknownCommand {
            cmd: (header >> 32) as i32,
        })
    }
}

#[derive(Debug, Copy, Clone)]
pub enum RingBufferError {
    InsufficientCapacity,
    MessageTooLong { msg: Index, max: Index },
    UnknownCommand { cmd: i32 },
}

pub struct MPSCProducer {
    buffer: AtomicBuffer,
    capacity: Index,
    max_msg_len: Index,
    tail_position: Index,
    head_cache_position: Index,
    head_position: Index,
}

pub struct MPSCConsumer {
    buffer: AtomicBuffer,
    capacity: Index,
    max_msg_len: Index,
    tail_position: Index,
    head_cache_position: Index,
    head_position: Index,
    correlation_id_counter: Index,
    consumer_heartbeat: Index,
}

impl MPSCProducer {
    pub fn new(buffer: AtomicBuffer) -> Self {
        let trailer_len = TRAILER_LENGTH;
        let capacity = buffer.capacity() - trailer_len;
        let max_msg_len = capacity / 8;
        let tail_position = capacity + TAIL_POSITION_OFFSET;
        let head_cache_position = capacity + HEAD_CACHE_POSITION_OFFSET;
        let head_position = capacity + HEAD_POSITION_OFFSET;
        MPSCProducer {
            buffer,
            capacity,
            max_msg_len,
            tail_position,
            head_cache_position,
            head_position,
        }
    }

    // todo: repace src+start/len with a single struct blitted over a buffer?
    pub fn write(&mut self, cmd: AeronCommand, src: &[u8]) -> Result<(), RingBufferError> {
        let mut record_len = src.len() as Index + RecordDescriptor::HEADER_LENGTH;
        if record_len > self.max_msg_len {
            return Err(RingBufferError::MessageTooLong {
                msg: record_len,
                max: self.max_msg_len,
            });
        }

        let required_capacity = align(record_len, RecordDescriptor::ALIGNMENT);
        // once we claim the required capacity we can write without conflicts
        let record_index = self.claim(required_capacity)?;

        self.buffer
            .put_ordered(record_index, &mut RecordDescriptor::make_header(-record_len, cmd));
        self.buffer.put_bytes(RecordDescriptor::encoded_msg_offset(record_index), src);
        self.buffer
            .put_ordered(RecordDescriptor::length_offset(record_index), &mut record_len);

        Ok(())
    }

    #[inline]
    pub fn claim(&self, required_capacity: Index) -> Result<Index, RingBufferError> {
        let mask = (self.capacity - 1) as i64;
        let mut head: i64 = self.buffer.get_volatile(self.head_cache_position);

        let (padding, tail_index) = loop {
            let tail: i64 = self.buffer.get_volatile(self.tail_position);
            let available_capacity = self.capacity - (tail - head) as Index;

            if required_capacity > available_capacity {
                head = self.buffer.get_volatile(self.head_position);
                if required_capacity > (self.capacity - (tail - head) as Index) {
                    return Err(RingBufferError::InsufficientCapacity);
                }
                self.buffer.put_ordered(self.head_cache_position, &mut head);
            }

            let mut padding = 0;
            let tail_index = (tail & mask) as Index;

            let len_to_buffer_end = self.capacity - tail_index;
            if required_capacity > len_to_buffer_end {
                let mut head_index = (head & mask) as Index;
                if required_capacity > head_index {
                    head = self.buffer.get_volatile(self.head_position);
                    head_index = (head & mask) as Index;
                    if required_capacity > head_index {
                        return Err(RingBufferError::InsufficientCapacity);
                    }
                    self.buffer.put_ordered(self.head_cache_position, &mut head)
                }
                padding = len_to_buffer_end;
            }
            let t2 = tail + required_capacity as i64 + padding as i64;
            if self.buffer.compare_and_set_i64(self.tail_position, tail, t2) {
                break (padding, tail_index);
            }
        };

        if padding != 0 {
            self.buffer
                .put_ordered(tail_index, &mut RecordDescriptor::make_header(padding, AeronCommand::Padding));
            Ok(0)
        } else {
            Ok(tail_index)
        }
    }
}

impl MPSCConsumer {
    pub fn new(buffer: AtomicBuffer) -> Self {
        let capacity = buffer.capacity() - TRAILER_LENGTH;
        let max_msg_len = capacity / 8;
        let tail_position = capacity + TAIL_POSITION_OFFSET;
        let head_cache_position = capacity + HEAD_CACHE_POSITION_OFFSET;
        let head_position = capacity + HEAD_POSITION_OFFSET;
        let correlation_id_counter = capacity + CORRELATION_COUNTER_OFFSET;
        let consumer_heartbeat = capacity + CONSUMER_HEARTBEAT_OFFSET;

        MPSCConsumer {
            buffer,
            capacity,
            max_msg_len,
            tail_position,
            head_cache_position,
            head_position,
            correlation_id_counter,
            consumer_heartbeat,
        }
    }

    /// Read from the ring buffer until either wrap-around or `msg_count_max` messages have been
    /// processed.
    /// Returns the number of messages processed.
    pub fn read<F: FnMut(AeronCommand, &AtomicBuffer)>(&self, msg_count_max: i32, mut handler: F) -> i32 {
        let head: i64 = self.buffer.get(self.head_position); // non - volatile read?
        let head_index = head as Index & (self.capacity - 1);
        let contiguous_block_len = self.capacity - head_index;

        let mut bytes_read = 0;
        let mut msg_read = 0;

        while bytes_read < contiguous_block_len && msg_read < msg_count_max {
            let record_index = head_index + bytes_read;
            let header: i64 = self.buffer.get_volatile(record_index);
            let record_len = RecordDescriptor::record_length(header);
            if record_len <= 0 {
                break;
            }

            bytes_read += align(record_len, RecordDescriptor::ALIGNMENT);

            if let Ok(msg_type) = RecordDescriptor::message_type(header) {
                if let AeronCommand::Padding = msg_type {
                    continue;
                }
                msg_read += 1;
                let view = self.buffer.view(
                    RecordDescriptor::encoded_msg_offset(record_index),
                    record_len - RecordDescriptor::HEADER_LENGTH,
                );
                handler(msg_type, &view)
            } else {
                break;
            }
        }
        // todo: move to a guard, or prevent corruption on panic
        if bytes_read != 0 {
            // zero-out memory and advance the reader
            self.buffer.set_memory(head_index, bytes_read as usize, 0);
            self.buffer.put_ordered::<i64>(self.head_position, head + bytes_read as i64)
        }
        msg_read
    }

    // Read all messages
    #[inline]
    pub fn read_all<F: FnMut(AeronCommand, &AtomicBuffer)>(&self, handler: F) -> i32 {
        self.read(::std::i32::MAX, handler)
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::AeronCommand;
    use crate::concurrent::atomic_buffer::{AlignedBuffer, AtomicBuffer};
    use crate::concurrent::ring_buffer::{
        MPSCConsumer, MPSCProducer, RecordDescriptor, RingBufferError, HEAD_POSITION_OFFSET, TAIL_POSITION_OFFSET, TRAILER_LENGTH,
    };
    use crate::utils::bit_utils::align;
    use crate::utils::types::Index;

    const CAPACITY: usize = 1024usize;
    const BUFFER_SZ: usize = CAPACITY + TRAILER_LENGTH as usize;

    const HEAD_COUNTER_INDEX: Index = 1024 + HEAD_POSITION_OFFSET;
    const TAIL_COUNTER_INDEX: Index = 1024 + TAIL_POSITION_OFFSET;

    const MSG_TYPE_ID: i32 = 101;

    struct Test {
        ab: AtomicBuffer,
        src_ab: AtomicBuffer,
        prod: MPSCProducer,
        buffer: AlignedBuffer,
        src_buffer: AlignedBuffer,
    }

    impl Test {
        pub fn new() -> Self {
            let buffer = AlignedBuffer::with_capacity(BUFFER_SZ);
            let ab = AtomicBuffer::from_aligned(&buffer);

            let src_buffer = AlignedBuffer::with_capacity(BUFFER_SZ);
            let src_ab = AtomicBuffer::from_aligned(&src_buffer);

            let prod = MPSCProducer::new(ab);

            Test {
                ab,
                src_ab,
                prod,
                buffer,
                src_buffer,
            }
        }
    }

    #[test]
    fn that_capacity_ok() {
        let b = AlignedBuffer::with_capacity(1024);
        let ab = AtomicBuffer::from_aligned(&b);
        let cap = ab.capacity();
        assert_eq!(cap, 1024);

        let p = MPSCProducer::new(ab);
        assert_eq!(p.capacity, cap - TRAILER_LENGTH)
    }

    #[test]
    fn that_writes_to_empty() {
        let mut test = Test::new();

        let src = [1, 2, 3];
        let result = test.prod.write(AeronCommand::UnitTestMessageTypeID, &src);
        assert!(result.is_ok())
    }

    #[test]
    fn should_reject_write_when_insufficient_space() {
        let mut test = Test::new();

        let length: Index = 100;
        let head: Index = 0;
        let tail: Index = head + (CAPACITY as Index - align(length - RecordDescriptor::ALIGNMENT, RecordDescriptor::ALIGNMENT));
        let _src_index: Index = 0;

        test.ab.put(HEAD_COUNTER_INDEX, head);
        test.ab.put(TAIL_COUNTER_INDEX, tail);

        let err = test.prod.write(AeronCommand::ClientKeepAlive, test.src_ab.as_mutable_slice());
        assert!(err.is_err(), "err is {:?}", err);
        assert_eq!(test.ab.get::<i64>(TAIL_COUNTER_INDEX), tail as i64);
    }

    #[test]
    fn should_read_single_message() -> Result<(), RingBufferError> {
        let test = Test::new();

        let length = 8 as Index;
        let head = 0 as Index;
        let record_length = length + RecordDescriptor::HEADER_LENGTH;
        let aligned_record_length = align(record_length, RecordDescriptor::ALIGNMENT);
        let tail = aligned_record_length;

        // simulate a write
        test.ab.put(HEAD_COUNTER_INDEX, head);
        test.ab.put(TAIL_COUNTER_INDEX, tail);
        test.ab.put(RecordDescriptor::type_offset(0), MSG_TYPE_ID);
        test.ab.put(RecordDescriptor::length_offset(0), record_length);

        let consumer = MPSCConsumer::new(test.ab);

        let mut times_called = 0;
        let messages_read = consumer.read_all(|_cmd, _b| {
            times_called += 1;
        });

        assert_eq!(messages_read, 1);
        assert_eq!(times_called, 1);
        assert_eq!(test.ab.get::<i64>(HEAD_COUNTER_INDEX), (head + aligned_record_length) as i64);

        for i in (0..RecordDescriptor::ALIGNMENT).step_by(4) {
            assert_eq!(
                test.ab.get::<i32>(i),
                0,
                "buffer has not be zeroed between indexes {} - {}",
                i,
                i + 3
            )
        }
        Ok(())
    }

    #[test]
    fn can_read_write() {
        let mut test = Test::new();

        let consumer = MPSCConsumer::new(test.ab);
        let result = test.prod.write(AeronCommand::UnitTestMessageTypeID, b"12345");

        assert!(result.is_ok());

        let mut data = Vec::new();
        consumer.read(1, |cmd, buf| {
            let msg = unsafe { String::from_raw_parts(buf.ptr, buf.capacity() as usize, buf.capacity() as usize) };
            data.push((cmd, msg));
        });

        assert_eq!(data.len(), 1);
        let (msg, string) = &data[0];
        assert_eq!(msg, &AeronCommand::UnitTestMessageTypeID);
        assert_eq!(string, "12345");
    }
}
