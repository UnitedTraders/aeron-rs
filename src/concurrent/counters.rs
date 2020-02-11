use std::collections::VecDeque;
use std::ffi::CString;
use std::time::SystemTime;

use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::concurrent::ring_buffer::CACHE_LINE_LENGTH;
use crate::offset_of;
use crate::utils::errors::*;
use crate::utils::misc::unix_time;
use crate::utils::types::{Index, Moment, MAX_MOMENT};

/**
 * Reads the counters metadata and values buffers.
 *
 * This code is threadsafe and can be used across threads.
 *
 * Values Buffer
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                        Counter Value                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                     120 bytes of padding                     ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                   Repeats to end of buffer                   ...
 *  |                                                               |
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *
 * Meta Data Buffer
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                        Record State                           |
 *  +---------------------------------------------------------------+
 *  |                          Type Id                              |
 *  +---------------------------------------------------------------+
 *  |                   Free-for-reuse Deadline                     |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                      112 bytes for key                       ...
 * ...                                                              |
 *  +-+-------------------------------------------------------------+
 *  |R|                      Label Length                           |
 *  +-+-------------------------------------------------------------+
 *  |                  380 bytes of Label in ASCII                 ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                   Repeats to end of buffer                   ...
 *  |                                                               |
 * ...                                                              |
 *  +---------------------------------------------------------------+
 */

const NULL_COUNTER_ID: i32 = -1;
const RECORD_UNUSED: i32 = 0;
const RECORD_ALLOCATED: i32 = 1;
const RECORD_RECLAIMED: i32 = -1;
const NOT_FREE_TO_REUSE: Moment = MAX_MOMENT;

const COUNTER_LENGTH: Index = std::mem::size_of::<CounterValueDefn>() as Index;
const METADATA_LENGTH: Index = std::mem::size_of::<CounterMetaDataDefn>() as Index;
const MAX_LABEL_LENGTH: Index = std::mem::size_of::<CounterMetaDataLabel>() as Index;
const MAX_KEY_LENGTH: Index = std::mem::size_of::<CounterMetaDataKey>() as Index;

// Original C++ alignment specification was #pragma pack(4)
#[repr(C, packed(4))]
struct CounterValueDefn {
    counter_value: u64,
    pad1: [i8; (2 * CACHE_LINE_LENGTH) as usize - std::mem::size_of::<u64>()],
}

// This type is needed just to be able get sizeof of this packed array
// May be its redundant - need to test
#[repr(C, packed(4))]
#[derive(Copy, Clone)]
struct CounterMetaDataKey {
    key: [i8; (2 * CACHE_LINE_LENGTH) as usize - (2 * std::mem::size_of::<i32>()) - std::mem::size_of::<u64>()],
}

// This type is needed just to be able get sizeof of this packed array
// May be its redundant - need to test
#[repr(C, packed(4))]
#[derive(Copy, Clone)]
struct CounterMetaDataLabel {
    key: [i8; (6 * CACHE_LINE_LENGTH) as usize - std::mem::size_of::<i32>()],
}

#[repr(C, packed(4))]
#[derive(Copy, Clone)]
pub struct CounterMetaDataDefn {
    state: i32,
    type_id: i32,
    free_to_reuse_deadline: Moment,
    key: CounterMetaDataKey,
    label_length: i32,
    label: CounterMetaDataLabel,
}

#[allow(non_snake_case)]
pub struct CountersReader {
    metadata_buffer: AtomicBuffer,
    values_buffer: AtomicBuffer,
    max_counter_id: i32,

    // It's a shame! But I failed to find working RUST offset_of! macro which works for
    // constants declared on module level. Therefore these constants live inside CountersReader
    // as mutable fields!!!
    FREE_TO_REUSE_DEADLINE_OFFSET: Index,
    LABEL_LENGTH_OFFSET: Index,
    KEY_OFFSET: Index,
}

impl CountersReader {
    pub fn new(metadata_buffer: AtomicBuffer, values_buffer: AtomicBuffer) -> Self {
        Self {
            metadata_buffer,
            values_buffer,
            max_counter_id: (values_buffer.capacity() / COUNTER_LENGTH) as i32,
            FREE_TO_REUSE_DEADLINE_OFFSET: offset_of!(CounterMetaDataDefn, free_to_reuse_deadline) as Index,
            LABEL_LENGTH_OFFSET: offset_of!(CounterMetaDataDefn, label_length) as Index,
            KEY_OFFSET: offset_of!(CounterMetaDataDefn, key) as Index,
        }
    }

    pub fn max_counter_id(&self) -> i32 {
        self.max_counter_id
    }

    pub fn get_counter_value(&self, id: i32) -> Result<u64, AeronError> {
        self.validate_counter_id(id)?;
        Ok(self.values_buffer.get_volatile::<u64>(Self::counter_offset(id)))
    }

    pub fn get_counter_state(&self, id: i32) -> Result<i32, AeronError> {
        self.validate_counter_id(id)?;
        Ok(self.metadata_buffer.get_volatile::<i32>(Self::metadata_offset(id)))
    }

    pub fn get_free_to_reuse_deadline(&self, id: i32) -> Result<u64, AeronError> {
        self.validate_counter_id(id)?;
        Ok(self
            .metadata_buffer
            .get_volatile::<u64>(Self::metadata_offset(id) + self.FREE_TO_REUSE_DEADLINE_OFFSET))
    }

    pub fn get_counter_label(&self, id: i32) -> Result<CString, AeronError> {
        self.validate_counter_id(id)?;

        Ok(self
            .metadata_buffer
            .get_string(Self::metadata_offset(id) + self.LABEL_LENGTH_OFFSET))
    }

    pub fn counter_offset(id: i32) -> Index {
        id as Index * COUNTER_LENGTH
    }

    pub fn metadata_offset(id: i32) -> Index {
        id as Index * METADATA_LENGTH
    }

    pub fn values_buffer(&self) -> AtomicBuffer {
        self.values_buffer.clone()
    }

    pub fn meta_data_buffer(&self) -> AtomicBuffer {
        self.metadata_buffer.clone()
    }

    fn validate_counter_id(&self, counter_id: i32) -> Result<(), AeronError> {
        if counter_id < 0 || counter_id > self.max_counter_id {
            let err_msg = format!(
                "{}:{}: counter id {} out of range: max_counter_id={}",
                file!(),
                line!(),
                counter_id,
                self.max_counter_id
            );
            Err(AeronError::IllegalArgumentException(err_msg))
        } else {
            Ok(())
        }
    }

    pub fn iter(&self) -> CountersReaderIter {
        CountersReaderIter { inner: self, pos: 0 }
    }
}

// This struct is needed to implement iterator for CountersReader
pub struct CountersReaderIter<'a> {
    inner: &'a CountersReader,
    pos: usize,
}

impl<'a> CountersReaderIter<'a> {
    pub fn new(inner: &'a CountersReader) -> Self {
        Self { inner, pos: 0 }
    }
}

impl<'a> Iterator for CountersReaderIter<'a> {
    type Item = &'a CounterMetaDataDefn;

    fn next(&mut self) -> Option<Self::Item> {
        let next_metadata_pos: Index = self.pos as Index * METADATA_LENGTH as Index;

        // Check bounds. End of the next Metadata struct to be read from buffer
        // should not be beyond the buffer end.
        if next_metadata_pos > self.inner.metadata_buffer.capacity() - METADATA_LENGTH {
            return None;
        }

        self.pos += 1;

        let record_status = self.inner.metadata_buffer.get_volatile::<i32>(next_metadata_pos);

        match record_status {
            RECORD_UNUSED | RECORD_RECLAIMED => None,
            RECORD_ALLOCATED => {
                let ret = self.inner.metadata_buffer.as_slice::<CounterMetaDataDefn>(next_metadata_pos);
                Some(ret)
            }
            _ => unreachable!("CountersReaderIter::next: unknown record status {}", record_status),
        }
    }
}

pub(crate) struct CountersManager {
    reader: CountersReader,
    free_list: VecDeque<i32>,
    clock: SystemTime,
    free_to_reuse_timeout_ms: u64,
    high_water_mark: Index,
}

impl CountersManager {
    pub fn new(
        metadata_buffer: AtomicBuffer,
        values_buffer: AtomicBuffer,
        clock: SystemTime,
        free_to_reuse_timeout_ms: u64,
    ) -> Self {
        Self {
            reader: CountersReader::new(metadata_buffer, values_buffer),
            free_list: VecDeque::default(),
            clock,
            free_to_reuse_timeout_ms,
            high_water_mark: 0,
        }
    }

    pub fn allocate(&mut self, type_id: i32, key: &[u8], label: CString) -> Result<i32, AeronError> {
        let counter_id = self.next_counter_id();

        if label.as_bytes_with_nul().len() > MAX_LABEL_LENGTH as usize {
            return Err(AeronError::IllegalArgumentException(String::from("allocate: label too long")));
        }

        self.check_counters_capacity(counter_id)?;

        let record_offset = CountersReader::metadata_offset(counter_id);
        self.check_meta_data_capacity(record_offset)?;

        let mut record: CounterMetaDataDefn = self.reader.metadata_buffer.get::<CounterMetaDataDefn>(record_offset);

        record.type_id = type_id;
        record.free_to_reuse_deadline = NOT_FREE_TO_REUSE;

        if key.len() > MAX_KEY_LENGTH as usize {
            return Err(AeronError::IllegalArgumentException(String::from("allocate: key too long")));
        }

        self.reader
            .metadata_buffer
            .put_bytes(record_offset + self.reader.KEY_OFFSET, key);
        self.reader
            .metadata_buffer
            .put_bytes(record_offset + self.reader.LABEL_LENGTH_OFFSET, label.as_bytes_with_nul());
        self.reader
            .metadata_buffer
            .put_ordered::<i32>(record_offset, RECORD_ALLOCATED);

        return Ok(counter_id);
    }

    pub fn free(&mut self, counter_id: i32) {
        let record_offset = CountersReader::metadata_offset(counter_id);
        self.reader.metadata_buffer.put::<Moment>(
            record_offset + self.reader.FREE_TO_REUSE_DEADLINE_OFFSET,
            unix_time()
                + self
                    .reader
                    .get_free_to_reuse_deadline(counter_id)
                    .expect("Error getting free to reuse deadline"),
        );
        self.reader
            .metadata_buffer
            .put_ordered::<i32>(record_offset, RECORD_RECLAIMED);
        self.free_list.push_back(counter_id);
    }

    pub fn set_counter_value(&mut self, counter_id: i32, value: u64) {
        self.reader
            .values_buffer
            .put_ordered::<u64>(CountersReader::counter_offset(counter_id), value);
    }

    fn next_counter_id(&mut self) -> i32 {
        let now_ms = unix_time();

        // Try to find counter ID which we allowed to reuse (based on reuse deadline)
        let search_result = self.free_list.iter().enumerate().find(|(_index, id)| {
            now_ms
                >= self
                    .reader
                    .metadata_buffer
                    .get_volatile::<u64>(CountersReader::metadata_offset(**id) + self.reader.FREE_TO_REUSE_DEADLINE_OFFSET)
        });

        if let Some((index, id)) = search_result {
            let counter_id = *id;
            self.free_list.remove(index); // A u sure that we need DQueue here???

            self.reader
                .values_buffer
                .put_ordered::<u64>(CountersReader::counter_offset(counter_id), 0);

            return counter_id;
        }

        self.high_water_mark += 1;

        self.high_water_mark as i32
    }

    fn check_counters_capacity(&self, counter_id: i32) -> Result<i32, AeronError> {
        if CountersReader::counter_offset(counter_id) + COUNTER_LENGTH > self.reader.values_buffer.capacity() {
            return Err(AeronError::IllegalArgumentException(String::from(
                "unable to allocated counter, values buffer is full",
            )));
        }

        Ok(0)
    }

    fn check_meta_data_capacity(&self, record_offset: Index) -> Result<i32, AeronError> {
        if record_offset + METADATA_LENGTH > self.reader.metadata_buffer.capacity() {
            return Err(AeronError::IllegalArgumentException(String::from(
                "unable to allocate counter, metadata buffer is full",
            )));
        }

        Ok(0)
    }
}
