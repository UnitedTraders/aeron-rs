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

use std::collections::VecDeque;
use std::ffi::CString;
use std::fmt;

use lazy_static::lazy_static;

use crate::{
    concurrent::atomic_buffer::AtomicBuffer,
    utils::{
        errors::*,
        misc::CACHE_LINE_LENGTH,
        types::{Index, Moment, I32_SIZE, I64_SIZE, MAX_MOMENT, U64_SIZE},
    },
};

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

pub const NULL_COUNTER_ID: i32 = -1;
pub const RECORD_UNUSED: i32 = 0;
pub const RECORD_ALLOCATED: i32 = 1;
pub const RECORD_RECLAIMED: i32 = -1;
pub const NOT_FREE_TO_REUSE: Moment = MAX_MOMENT;

pub const COUNTER_LENGTH: Index = std::mem::size_of::<CounterValueDefn>() as Index;
pub const METADATA_LENGTH: Index = std::mem::size_of::<CounterMetaDataDefn>() as Index;
pub const MAX_LABEL_LENGTH: Index = std::mem::size_of::<CounterMetaDataLabel>() as Index;
pub const MAX_KEY_LENGTH: Index = std::mem::size_of::<CounterMetaDataKey>() as Index;

// Original C++ alignment specification was #pragma pack(4)
#[repr(C, packed(4))]
struct CounterValueDefn {
    counter_value: u64,
    pad1: [i8; (2 * CACHE_LINE_LENGTH - I64_SIZE) as usize],
}

// This type is needed just to be able get sizeof of this packed array
// May be its redundant - need to test
#[repr(C, packed(4))]
#[derive(Copy, Clone)]
pub struct CounterMetaDataKey {
    pub key: [u8; (2 * CACHE_LINE_LENGTH - 2 * I32_SIZE - U64_SIZE) as usize],
}

// This type is needed just to be able get sizeof of this packed array
// May be its redundant - need to test
#[repr(C, packed(4))]
#[derive(Copy, Clone)]
pub struct CounterMetaDataLabel {
    pub val: [u8; (6 * CACHE_LINE_LENGTH - I32_SIZE) as usize],
}

#[repr(C, packed(4))]
#[derive(Copy, Clone)]
pub struct CounterMetaDataDefn {
    pub state: i32,
    pub type_id: i32,
    pub free_to_reuse_deadline: Moment,
    pub key: CounterMetaDataKey,
    pub label_length: i32,
    pub label: CounterMetaDataLabel,
}

impl fmt::Debug for CounterMetaDataDefn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Copy fields of packed structure to local variables to make sure we will not borrow them as refs (which is UB)
        let state = self.state;
        let type_id = self.type_id;
        let free_to_reuse_deadline = self.free_to_reuse_deadline;
        let key = self.key;
        let label_length = self.label_length;
        let label = self.label;

        write!(
            f,
            "CounterMetaDataDefn {{\n    state={} type_id={} free_to_reuse_deadline={} label_length={}",
            state, type_id, free_to_reuse_deadline, label_length
        )?;
        write!(f, "   key=\"")?;
        for ch in key.key.iter() {
            write!(f, "{}", ch)?;
        }
        write!(f, "\"\n    label=\"")?;
        for ch in label.val.iter() {
            write!(f, "{}", String::from_utf8_lossy(std::slice::from_ref::<u8>(ch)))?;
        }
        write!(f, "\" \n}}")?;
        Ok(())
    }
}

lazy_static! {
    pub static ref FREE_TO_REUSE_DEADLINE_OFFSET: Index = offset_of!(CounterMetaDataDefn, free_to_reuse_deadline) as Index;
    pub static ref LABEL_LENGTH_OFFSET: Index = offset_of!(CounterMetaDataDefn, label_length) as Index;
    pub static ref KEY_OFFSET: Index = offset_of!(CounterMetaDataDefn, key) as Index;
    pub static ref TYPE_ID_OFFSET: Index = offset_of!(CounterMetaDataDefn, type_id) as Index;
}

pub struct CountersReader {
    metadata_buffer: AtomicBuffer,
    values_buffer: AtomicBuffer,
    max_counter_id: i32,
}

impl CountersReader {
    pub fn new(metadata_buffer: AtomicBuffer, values_buffer: AtomicBuffer) -> Self {
        Self {
            metadata_buffer,
            values_buffer,
            max_counter_id: (values_buffer.capacity() / COUNTER_LENGTH),
        }
    }

    pub fn for_each<F>(&self, mut on_counters_metadata: F)
    where
        F: FnMut(i32, i32, &AtomicBuffer, CString),
    {
        for (id, i) in (0..self.metadata_buffer.capacity())
            .step_by(METADATA_LENGTH as usize)
            .enumerate()
        {
            let record_status = self.metadata_buffer.get_volatile::<i32>(i);
            if record_status == RECORD_UNUSED {
                break;
            } else if record_status == RECORD_ALLOCATED {
                let record = self.metadata_buffer.overlay_struct::<CounterMetaDataDefn>(i);

                let label = self.metadata_buffer.get_string(i + *LABEL_LENGTH_OFFSET);
                let key_buffer = AtomicBuffer::new(
                    unsafe { self.metadata_buffer.buffer().offset((i + *KEY_OFFSET) as isize) },
                    std::mem::size_of::<CounterMetaDataKey>() as i32,
                );

                on_counters_metadata(id as i32, (unsafe { *record }).type_id, &key_buffer, label);
            }
        }
    }

    pub fn max_counter_id(&self) -> i32 {
        self.max_counter_id
    }

    pub fn counter_value(&self, id: i32) -> Result<u64, AeronError> {
        self.validate_counter_id(id)?;
        Ok(self.values_buffer.get_volatile::<u64>(Self::counter_offset(id)))
    }

    pub fn counter_state(&self, id: i32) -> Result<i32, AeronError> {
        self.validate_counter_id(id)?;
        Ok(self.metadata_buffer.get_volatile::<i32>(Self::metadata_offset(id)))
    }

    pub fn free_to_reuse_deadline(&self, id: i32) -> Result<u64, AeronError> {
        self.validate_counter_id(id)?;
        Ok(self
            .metadata_buffer
            .get_volatile::<u64>(Self::metadata_offset(id) + *FREE_TO_REUSE_DEADLINE_OFFSET))
    }

    pub fn counter_label(&self, id: i32) -> Result<CString, AeronError> {
        self.validate_counter_id(id)?;

        Ok(self
            .metadata_buffer
            .get_string(Self::metadata_offset(id) + *LABEL_LENGTH_OFFSET))
    }

    pub fn counter_offset(id: i32) -> Index {
        id as Index * COUNTER_LENGTH
    }

    pub fn metadata_offset(id: i32) -> Index {
        id as Index * METADATA_LENGTH
    }

    pub fn values_buffer(&self) -> AtomicBuffer {
        self.values_buffer
    }

    pub fn meta_data_buffer(&self) -> AtomicBuffer {
        self.metadata_buffer
    }

    fn validate_counter_id(&self, counter_id: i32) -> Result<(), AeronError> {
        if counter_id < 0 || counter_id > self.max_counter_id {
            Err(IllegalArgumentError::CounterIdOutOfRange {
                filename: file!().to_string(),
                line: line!(),
                counter_id,
                max_counter_id: self.max_counter_id,
            }
            .into())
        } else {
            Ok(())
        }
    }

    pub fn iter(&self) -> CountersReaderIter {
        CountersReaderIter { inner: self, pos: 0 }
    }
}

/// This struct is needed to implement iterator for CountersReader
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
                let ret = self.inner.metadata_buffer.as_ref::<CounterMetaDataDefn>(next_metadata_pos);
                Some(ret)
            }
            _ => unreachable!("CountersReaderIter::next: unknown record status {}", record_status),
        }
    }
}

#[allow(dead_code)]
type KeyFunc = fn(&mut AtomicBuffer);
type SysTimeProvider = fn() -> u64;

pub fn default_sys_time_provider() -> u64 {
    0
}

pub struct CountersManager {
    reader: CountersReader,
    free_list: VecDeque<i32>,
    clock: SysTimeProvider,
    free_to_reuse_timeout_ms: u64,
    high_water_mark: Index,
}

impl CountersManager {
    pub fn new(metadata_buffer: AtomicBuffer, values_buffer: AtomicBuffer) -> Self {
        Self {
            reader: CountersReader::new(metadata_buffer, values_buffer),
            free_list: VecDeque::default(),
            clock: default_sys_time_provider,
            free_to_reuse_timeout_ms: 0,
            high_water_mark: 0,
        }
    }

    pub fn new_opt(
        metadata_buffer: AtomicBuffer,
        values_buffer: AtomicBuffer,
        clock: SysTimeProvider,
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

    // This fn is "inherited" from CountersReader just to keep CounterManager iface similar
    // to C++\Java iface (forEach method)
    pub fn iter(&self) -> CountersReaderIter {
        self.reader.iter()
    }

    /// This fn allocates counter with given type, key and label.
    /// The keys can be provided by two ways:
    /// 1. through key_opt param
    /// 2. could be generated and written in-place by key_func param
    /// If both key_opt and key_func are specified then AeronError is returned.
    pub fn allocate_opt(
        &mut self,
        type_id: i32,
        key_opt: Option<&[u8]>,
        key_func: Option<impl Fn(&mut AtomicBuffer)>,
        label_rs: &str,
    ) -> Result<i32, AeronError> {
        let counter_id = self.next_counter_id();

        // Try to convert Rust str in to C compatible string
        let conv_result = CString::new(label_rs);

        if conv_result.is_err() {
            return Err(IllegalArgumentError::AllocateLabelCanNotBeConverted.into());
        }

        let label = conv_result.unwrap();

        if label.as_bytes().len() > MAX_LABEL_LENGTH as usize {
            return Err(IllegalArgumentError::AllocateLabelTooLong.into());
        }

        self.check_counters_capacity(counter_id)?;

        let record_offset = CountersReader::metadata_offset(counter_id);
        self.check_meta_data_capacity(record_offset)?;

        let mut record = self.reader.metadata_buffer.get::<CounterMetaDataDefn>(record_offset);

        record.type_id = type_id;
        record.free_to_reuse_deadline = NOT_FREE_TO_REUSE;

        // Needed to put back changed fields.
        self.reader.metadata_buffer.put::<CounterMetaDataDefn>(record_offset, record);

        if key_opt.is_some() && key_func.is_some() {
            return Err(IllegalArgumentError::AllocateKeyIsAmbiguous.into());
        }

        if let Some(key) = key_opt {
            // In original code log key is truncated to MAX_KEY_LENGTH and no error produced.
            // In Rust implementation we'll return error in such situation.
            if key.len() > MAX_KEY_LENGTH as usize {
                return Err(IllegalArgumentError::AllocateKeyIsTooLong.into());
            }

            self.reader.metadata_buffer.put_bytes(record_offset + *KEY_OFFSET, key);
        }

        if let Some(key_fn) = key_func {
            let mut key_buffer = self.reader.metadata_buffer.view(record_offset + *KEY_OFFSET, MAX_KEY_LENGTH);
            key_fn(&mut key_buffer);
        }

        self.reader
            .metadata_buffer
            .put_string(record_offset + *LABEL_LENGTH_OFFSET, label.as_bytes());

        self.reader
            .metadata_buffer
            .put_ordered::<i32>(record_offset, RECORD_ALLOCATED);

        Ok(counter_id)
    }

    pub fn allocate(&mut self, label: &str) -> Result<i32, AeronError> {
        self.allocate_opt(0, None, Option::<fn(&mut AtomicBuffer)>::None, label)
    }

    pub fn free(&mut self, counter_id: i32) {
        let record_offset = CountersReader::metadata_offset(counter_id);
        self.reader.metadata_buffer.put::<Moment>(
            record_offset + *FREE_TO_REUSE_DEADLINE_OFFSET,
            (self.clock)() + self.free_to_reuse_timeout_ms,
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

    pub fn counter_value(&self, id: i32) -> Result<u64, AeronError> {
        self.reader.counter_value(id)
    }

    fn next_counter_id(&mut self) -> i32 {
        let now_ms = (self.clock)();

        // Try to find counter ID which we allowed to reuse (based on reuse deadline)
        let search_result = self.free_list.iter().enumerate().find(|(_index, id)| {
            now_ms as i64
                >= self
                    .reader
                    .metadata_buffer
                    .get_volatile::<i64>(CountersReader::metadata_offset(**id) + *FREE_TO_REUSE_DEADLINE_OFFSET)
        });

        if let Some((index, id)) = search_result {
            let counter_id = *id;
            self.free_list.remove(index); // A u sure that we need DQueue here???

            self.reader
                .values_buffer
                .put_ordered::<u64>(CountersReader::counter_offset(counter_id), 0);

            return counter_id;
        }

        let ret_id = self.high_water_mark;
        self.high_water_mark += 1;

        ret_id
    }

    fn check_counters_capacity(&self, counter_id: i32) -> Result<i32, AeronError> {
        if CountersReader::counter_offset(counter_id) + COUNTER_LENGTH > self.reader.values_buffer.capacity() {
            return Err(IllegalArgumentError::UnableAllocateCounterBecauseValueBufferFull.into());
        }

        Ok(0)
    }

    fn check_meta_data_capacity(&self, record_offset: Index) -> Result<i32, AeronError> {
        if record_offset + METADATA_LENGTH > self.reader.metadata_buffer.capacity() {
            return Err(IllegalArgumentError::UnableAllocateCounterBecauseMetadataBufferFull.into());
        }

        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;
    use crate::concurrent::{
        atomic_buffer::AlignedBuffer,
        counters,
        position::{ReadablePosition, UnsafeBufferPosition},
    };
    use crate::utils;

    const NUM_COUNTERS: Index = 4;
    // const INT_MAX: i32 = std::i32::MAX;
    const FREE_TO_REUSE_TIMEOUT: u64 = 1000;

    macro_rules! gen_counters_manager {
        ($counters_manager:ident) => {
            let m_buff = AlignedBuffer::with_capacity(NUM_COUNTERS * counters::METADATA_LENGTH);
            let v_buff = AlignedBuffer::with_capacity(NUM_COUNTERS * counters::COUNTER_LENGTH);

            let metadata_buffer = AtomicBuffer::from_aligned(&m_buff);
            let values_buffer = AtomicBuffer::from_aligned(&v_buff);

            let mut $counters_manager = CountersManager::new(metadata_buffer, values_buffer);
        };
    }

    macro_rules! gen_counters_manager_values {
        ($counters_manager:ident, $v_buff:ident) => {
            let m_buff = AlignedBuffer::with_capacity(NUM_COUNTERS * counters::METADATA_LENGTH);
            let $v_buff = AlignedBuffer::with_capacity(NUM_COUNTERS * counters::COUNTER_LENGTH);

            let metadata_buffer = AtomicBuffer::from_aligned(&m_buff);
            let values_buffer = AtomicBuffer::from_aligned(&$v_buff);

            let mut $counters_manager = CountersManager::new(metadata_buffer, values_buffer);
        };
    }

    macro_rules! gen_counters_manager_with_cool_down {
        ($counters_manager_with_cool_down:ident, $time_func:expr) => {
            let m_buff = AlignedBuffer::with_capacity(NUM_COUNTERS * counters::METADATA_LENGTH);
            let v_buff = AlignedBuffer::with_capacity(NUM_COUNTERS * counters::COUNTER_LENGTH);

            let metadata_buffer = AtomicBuffer::from_aligned(&m_buff);
            let values_buffer = AtomicBuffer::from_aligned(&v_buff);

            let mut $counters_manager_with_cool_down =
                CountersManager::new_opt(metadata_buffer, values_buffer, $time_func, FREE_TO_REUSE_TIMEOUT);
        };
    }

    #[test]
    fn test_counters_packed_struct_read_write() {
        let buff = AlignedBuffer::with_capacity(NUM_COUNTERS * counters::METADATA_LENGTH);
        let metadata_buffer = AtomicBuffer::from_aligned(&buff);

        let meta = CounterMetaDataDefn {
            state: 1,
            type_id: 2,
            free_to_reuse_deadline: 3,
            key: CounterMetaDataKey { key: [1; 112] },
            label_length: 4,
            label: CounterMetaDataLabel { val: [68; 380] },
        };

        metadata_buffer.put::<CounterMetaDataDefn>(0, meta);

        let read = metadata_buffer.get::<CounterMetaDataDefn>(0);

        println!("Put: {:?}", meta);
        println!("Read: {:?}", read);

        let state = meta.state;
        let read = read.state;
        assert_eq!(state, read);
    }

    #[test]
    #[allow(unused_mut)]
    #[allow(unused_must_use)]
    fn test_counters_check_empty() {
        gen_counters_manager!(counters_manager);

        for _i in counters_manager.iter() {
            panic!("Counters iterator should return nothing");
        }
    }

    #[test]
    #[allow(unused_mut)]
    #[allow(unused_must_use)]
    fn test_counters_check_overflow() {
        gen_counters_manager!(counters_manager);

        let labels = vec!["lab0", "lab1", "lab2", "lab3", "lab4"];

        let mut alloc_result: Result<i32, AeronError> = Ok(0);

        for label in labels {
            alloc_result = counters_manager.allocate(label);
        }

        if let Err(err) = alloc_result {
            println!("Counter alloc failed (by intention): {:?}", err);
        } else {
            panic!("Counter alloc must fail but didn't!");
        }
    }

    #[test]
    #[allow(unused_mut)]
    #[allow(unused_must_use)]
    fn test_counters_check_alloc() {
        gen_counters_manager!(counters_manager);

        let labels = vec!["lab0", "lab1", "lab2", "lab3"];
        let mut allocated: HashMap<i32, String> = HashMap::new();

        // Allocate 4 counters
        for label in labels {
            let alloc_result = counters_manager.allocate(label);

            if let Err(err) = alloc_result {
                panic!("Counter {:?} alloc failed: {:?}", label, err);
            } else {
                allocated.insert(alloc_result.unwrap(), label.to_string());
            }
        }

        // Check counters known by CountersManager
        for (id, counter) in counters_manager.iter().enumerate() {
            assert_eq!(
                unsafe { &utils::misc::aeron_str_to_rust(&counter.label.val[0] as *const u8, counter.label_length) },
                allocated.get(&(id as i32)).unwrap()
            );
            allocated.remove(&(id as i32));
        }

        assert_eq!(allocated.len(), 0);
    }

    #[test]
    #[allow(unused_mut)]
    #[allow(unused_must_use)]
    fn test_counters_recycle() {
        gen_counters_manager!(counters_manager);

        let labels = vec!["lab0", "lab1", "lab2", "lab3"];

        // Allocate 4 counters
        for label in labels {
            let alloc_result = counters_manager.allocate(label);

            if let Err(err) = alloc_result {
                panic!("Counter {:?} alloc failed: {:?}", label, err);
            }
        }

        counters_manager.free(2);
        assert_eq!(counters_manager.allocate("newLab2").unwrap(), 2);
    }

    #[test]
    #[allow(unused_mut)]
    #[allow(unused_must_use)]
    fn test_counters_free_and_reuse_counters() {
        gen_counters_manager!(counters_manager);

        counters_manager.allocate("abc");
        let def = counters_manager.allocate("def").unwrap();
        counters_manager.allocate("ghi");

        counters_manager.free(def);

        assert_eq!(counters_manager.allocate("next").unwrap(), def);
    }

    lazy_static! {
        static ref NOW_TIME: Mutex<u64> = Mutex::new(0);
    }

    fn test1_time_provider() -> u64 {
        *NOW_TIME.lock().unwrap()
    }

    #[test]
    #[allow(unused_mut)]
    #[allow(unused_must_use)]
    fn test_counters_free_and_not_reuse_counters_that_have_cool_down() {
        *NOW_TIME.lock().unwrap() = 0;
        gen_counters_manager_with_cool_down!(counters_manager_with_cool_down, test1_time_provider);

        // These counters are allocated at time=0 and FREE_TO_REUSE_TIMEOUT = 1000
        counters_manager_with_cool_down.allocate("abc");
        let def = counters_manager_with_cool_down.allocate("def").unwrap();
        let ghi = counters_manager_with_cool_down.allocate("ghi").unwrap();

        counters_manager_with_cool_down.free(def);

        // Set NOW to 999 - means that reuse timeout is not reached yet
        *NOW_TIME.lock().unwrap() = FREE_TO_REUSE_TIMEOUT - 1;

        let next_id = counters_manager_with_cool_down.allocate("the next label").unwrap();
        assert!(next_id > ghi);
    }

    #[test]
    #[allow(unused_mut)]
    #[allow(unused_must_use)]
    fn test_counters_free_and_reuse_counters_after_cool_down() {
        *NOW_TIME.lock().unwrap() = 0;
        gen_counters_manager_with_cool_down!(counters_manager_with_cool_down, test1_time_provider);

        counters_manager_with_cool_down.allocate("abc");
        let def = counters_manager_with_cool_down.allocate("def").unwrap();
        counters_manager_with_cool_down.allocate("ghi");

        counters_manager_with_cool_down.free(def);

        *NOW_TIME.lock().unwrap() = FREE_TO_REUSE_TIMEOUT;
        assert_eq!(counters_manager_with_cool_down.allocate("the next label").unwrap(), def);
    }

    #[test]
    #[allow(unused_mut)]
    #[allow(unused_must_use)]
    fn test_counters_map_position() {
        gen_counters_manager_values!(counters_manager, values_buffer);

        let reader_buffer = AtomicBuffer::from_aligned(&values_buffer);
        let writer_buffer = AtomicBuffer::from_aligned(&values_buffer);

        counters_manager.allocate("def");

        let counter_id = counters_manager.allocate("abc").unwrap();
        let reader = UnsafeBufferPosition::new(reader_buffer, counter_id);
        let writer = UnsafeBufferPosition::new(writer_buffer, counter_id);

        let expected_value = 0x000F_FFFF_FFFF;

        writer.set_ordered(expected_value);
        assert_eq!(reader.get_volatile(), expected_value);
    }

    fn test_key_func_777(buffer: &mut AtomicBuffer) {
        buffer.put::<i64>(0, 777)
    }

    fn test_key_func_444(buffer: &mut AtomicBuffer) {
        buffer.put::<i64>(0, 444)
    }

    #[test]
    #[allow(unused_mut)]
    #[allow(unused_must_use)]
    #[allow(clippy::cast_ptr_alignment)]
    fn test_counters_store_meta_data() {
        gen_counters_manager!(counters_manager);

        let labels = vec!["lab0", "lab1"];
        let type_ids = vec![333, 222];
        let keys = [777, 444];

        let counter_id0 = counters_manager
            .allocate_opt(type_ids[0], None, Some(test_key_func_777), labels[0])
            .unwrap();
        assert_eq!(counter_id0, 0);

        let counter_id1 = counters_manager
            .allocate_opt(type_ids[1], None, Some(test_key_func_444), labels[1])
            .unwrap();
        assert_eq!(counter_id1, 1);

        let mut num_counters: usize = 0;

        for (counter_id, counter) in counters_manager.iter().enumerate() {
            assert_eq!(counter_id, num_counters);
            assert_eq!(
                unsafe { &utils::misc::aeron_str_to_rust(&counter.label.val[0] as *const u8, counter.label_length) },
                labels[num_counters]
            );

            assert_eq!(counter.type_id, type_ids[num_counters]);

            // I know that there should be i64 value
            let key = &counter.key.key[0] as *const u8 as *const i64;

            unsafe {
                assert_eq!(*key, keys[num_counters]);
            }
            num_counters += 1;
        }

        assert_eq!(num_counters, 2);
    }

    #[test]
    #[allow(unused_mut)]
    #[allow(unused_must_use)]
    fn test_counters_store_and_load_value() {
        gen_counters_manager!(counters_manager);

        let counter_id = counters_manager.allocate("abc").unwrap();

        let value: u64 = 7;
        counters_manager.set_counter_value(counter_id, value);
        assert_eq!(counters_manager.counter_value(counter_id).unwrap(), value);
    }
}
