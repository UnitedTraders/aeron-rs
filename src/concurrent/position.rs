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

use crate::{
    concurrent::{atomic_buffer::AtomicBuffer, counters::CountersReader},
    utils::types::Index,
};

pub trait ReadablePosition {
    type P: ReadablePosition;

    fn wrap(&mut self, position: &Self::P);
    fn id(&self) -> i32;
    fn get(&self) -> i64;
    fn get_volatile(&self) -> i64;
    fn set(&self, value: i64);
    fn set_ordered(&self, value: i64);
}

#[derive(Clone)]
pub struct UnsafeBufferPosition {
    buffer: AtomicBuffer,
    id: i32,
    offset: Index,
}

impl UnsafeBufferPosition {
    pub fn new(buffer: AtomicBuffer, id: i32) -> Self {
        Self {
            buffer,
            id,
            offset: CountersReader::counter_offset(id),
        }
    }
}

impl ReadablePosition for UnsafeBufferPosition {
    type P = Self;
    fn wrap(&mut self, position: &Self::P) {
        self.buffer = position.buffer;
        self.id = position.id;
        self.offset = position.offset;
    }

    fn id(&self) -> i32 {
        self.id
    }

    fn get(&self) -> i64 {
        self.buffer.get(self.offset)
    }

    fn get_volatile(&self) -> i64 {
        self.buffer.get_volatile(self.offset)
    }

    fn set(&self, value: i64) {
        self.buffer.put_ordered::<i64>(self.offset, value)
    }

    fn set_ordered(&self, value: i64) {
        self.buffer.put_ordered::<i64>(self.offset, value)
    }
}
