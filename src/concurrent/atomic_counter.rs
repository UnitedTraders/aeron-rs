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

use std::sync::{Arc, Mutex};

use crate::{
    concurrent::{
        atomic_buffer::AtomicBuffer,
        counters::{CountersManager, CountersReader},
    },
    utils::types::Index,
};

pub struct AtomicCounter {
    buffer: AtomicBuffer,
    counter_id: i32,
    counters_manager: Option<Arc<Mutex<CountersManager>>>, // TODO investigate do we really need shared ref here and mutex
    offset: Index,
}

impl AtomicCounter {
    pub fn new(buffer: AtomicBuffer, counter_id: i32) -> Self {
        let selfy = Self {
            buffer,
            counter_id,
            counters_manager: None,
            offset: CountersReader::counter_offset(counter_id),
        };

        selfy.buffer.put::<i64>(selfy.offset, 0);

        selfy
    }

    pub fn new_opt(buffer: AtomicBuffer, counter_id: i32, counters_mgr: Arc<Mutex<CountersManager>>) -> Self {
        let selfy = Self {
            buffer,
            counter_id,
            counters_manager: Some(counters_mgr),
            offset: CountersReader::counter_offset(counter_id),
        };

        selfy.buffer.put::<i64>(selfy.offset, 0);

        selfy
    }

    pub fn id(&self) -> i32 {
        self.counter_id
    }

    pub fn increment(&self) {
        self.buffer.get_and_add_i64(self.offset, 1);
    }

    pub fn increment_ordered(&self) {
        self.buffer.add_i64_ordered(self.offset, 1);
    }

    pub fn set(&self, value: i64) {
        self.buffer.put_atomic_i64(self.offset, value);
    }

    pub fn set_ordered(&self, value: i64) {
        self.buffer.put_ordered::<i64>(self.offset, value);
    }

    pub fn set_weak(&self, value: i64) {
        self.buffer.put::<i64>(self.offset, value);
    }

    pub fn get_and_add(&self, value: i64) -> i64 {
        self.buffer.get_and_add_i64(self.offset, value)
    }

    pub fn get_and_add_ordered(&self, increment: i64) -> i64 {
        let current_value = self.buffer.get::<i64>(self.offset);
        self.buffer.put_ordered::<i64>(self.offset, current_value + increment);
        current_value
    }

    pub fn get_and_set(&self, value: i64) -> i64 {
        let current_value = self.buffer.get::<i64>(self.offset);
        self.buffer.put_atomic_i64(self.offset, value);
        current_value
    }

    pub fn compare_and_set(&self, expected_value: i64, update_value: i64) -> bool {
        self.buffer.compare_and_set_i64(self.offset, expected_value, update_value)
    }

    pub fn get(&self) -> i64 {
        self.buffer.get_volatile::<i64>(self.offset)
    }

    pub fn get_weak(&self) -> i64 {
        self.buffer.get::<i64>(self.offset)
    }
}

impl Drop for AtomicCounter {
    fn drop(&mut self) {
        if let Some(mgr) = &self.counters_manager {
            mgr.lock().expect("Can't lock mutex on counters_mgr").free(self.counter_id);
        }
    }
}
