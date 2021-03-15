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

use crate::utils::errors::{IllegalArgumentError, IllegalStateError};
use crate::{
    concurrent::{
        atomic_buffer::AtomicBuffer,
        logbuffer::{data_frame_header, header::Header},
    },
    utils::{
        bit_utils,
        errors::AeronError,
        misc::{alloc_buffer_aligned, dealloc_buffer_aligned},
        types::Index,
    },
};

const BUFFER_BUILDER_MAX_CAPACITY: Index = std::i32::MAX as Index - 8;

/// This type must not impl Copy! Only move semantics is allowed.
/// BufferBuilder owns memory (allocates / deallocates it)
pub struct BufferBuilder {
    capacity: Index,
    limit: Index,
    buffer: *mut u8,
}

impl Drop for BufferBuilder {
    fn drop(&mut self) {
        // Free the memory we own
        dealloc_buffer_aligned(self.buffer, self.capacity)
    }
}

impl BufferBuilder {
    pub fn new(initial_length: isize) -> Self {
        let len = bit_utils::find_next_power_of_two_i64(initial_length as i64) as Index;
        Self {
            capacity: len,
            limit: data_frame_header::LENGTH,
            buffer: alloc_buffer_aligned(len),
        }
    }

    pub fn buffer(&self) -> *mut u8 {
        self.buffer
    }

    pub fn limit(&self) -> Index {
        self.limit
    }

    pub fn set_limit(&mut self, limit: Index) -> Result<(), AeronError> {
        if limit >= self.capacity {
            return Err(IllegalArgumentError::LimitOutsideRange {
                capacity: self.capacity,
                limit,
            }
            .into());
        }

        self.limit = limit;

        Ok(())
    }

    pub fn reset(&mut self) -> &mut BufferBuilder {
        self.limit = data_frame_header::LENGTH;
        self
    }

    pub fn append(
        &mut self,
        buffer: &AtomicBuffer,
        offset: Index,
        length: Index,
        _header: &Header,
    ) -> Result<&BufferBuilder, AeronError> {
        self.ensure_capacity(length)?;

        unsafe {
            std::ptr::copy(
                buffer.buffer().offset(offset as isize),
                self.buffer.offset(self.limit as isize),
                length as usize,
            );
        }

        self.limit += length;

        Ok(self)
    }

    fn find_suitable_capacity(current_capacity: Index, required_capacity: Index) -> Result<Index, AeronError> {
        let mut capacity = current_capacity;

        loop {
            let new_capacity = capacity + (capacity >> 1);

            if new_capacity < capacity || new_capacity > BUFFER_BUILDER_MAX_CAPACITY {
                if capacity == BUFFER_BUILDER_MAX_CAPACITY {
                    return Err(IllegalStateError::MaxCapacityReached(BUFFER_BUILDER_MAX_CAPACITY).into());
                }

                capacity = BUFFER_BUILDER_MAX_CAPACITY;
            } else {
                capacity = new_capacity;
            }

            if capacity >= required_capacity {
                break;
            }
        }

        Ok(capacity)
    }

    /// This fn resizes (if needed) the buffer keeping all the data in it.
    fn ensure_capacity(&mut self, additional_capacity: Index) -> Result<(), AeronError> {
        let required_capacity = self.limit + additional_capacity;

        if required_capacity > self.capacity {
            let new_capacity = BufferBuilder::find_suitable_capacity(self.capacity, required_capacity)?;
            let new_buffer = alloc_buffer_aligned(new_capacity);

            unsafe {
                std::ptr::copy(self.buffer, new_buffer, self.limit as usize);
                dealloc_buffer_aligned(self.buffer, self.capacity)
            }

            self.buffer = new_buffer;
            self.capacity = new_capacity;
        }
        Ok(())
    }
}
