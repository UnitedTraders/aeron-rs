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

use crate::concurrent::{atomic_buffer::AtomicBuffer, logbuffer::data_frame_header};
use crate::utils::types::Index;

/**
 * Represents a claimed range in a buffer to be used for recording a message without copy semantics for later commit.
 * <p>
 * The claimed space is in {@link #buffer()} between {@link #offset()} and {@link #offset()} + {@link #length()}.
 * When the buffer is filled with message data, use {@link #commit()} to make it available to subscribers.
 */
#[derive(Default, Copy, Clone)]
pub struct BufferClaim {
    buffer: Option<AtomicBuffer>,
}

impl BufferClaim {
    pub fn wrap(&mut self, buffer: *mut u8, length: Index) {
        self.buffer = Some(AtomicBuffer::new(buffer, length));
    }

    pub fn wrap_with_offset(&mut self, buffer: &AtomicBuffer, offset: Index, length: Index) {
        self.buffer = Some(buffer.view(offset, length));
    }

    /**
     * The buffer to be used.
     *
     * @return the buffer to be used..
     */
    pub fn buffer(&self) -> AtomicBuffer {
        self.buffer.expect("No buffer")
    }

    /**
     * The offset in the buffer at which the claimed range begins.
     *
     * @return offset in the buffer at which the range begins.
     */
    pub const fn offset(&self) -> Index {
        data_frame_header::LENGTH
    }

    /**
     * The length of the claimed range in the buffer.
     *
     * @return length of the range in the buffer.
     */
    pub fn length(&self) -> Index {
        self.buffer.expect("No buffer").capacity() - data_frame_header::LENGTH
    }

    /**
     * Get the value of the flags field.
     *
     * @return the value of the header flags field.
     */
    pub fn flags(&self) -> u8 {
        self.buffer
            .expect("No buffer")
            .get::<u8>(*data_frame_header::FLAGS_FIELD_OFFSET)
    }

    /**
     * Set the value of the header flags field.
     *
     * @param flags value to be set in the header.
     * @return this for a fluent API.
     */
    pub fn set_flags(&mut self, flags: u8) -> &Self {
        self.buffer
            .expect("No buffer")
            .put::<u8>(*data_frame_header::FLAGS_FIELD_OFFSET, flags);
        self
    }

    /**
     * Get the value of the header type field.
     *
     * @return the value of the header type field.
     */
    pub fn header_type(&self) -> u16 {
        self.buffer
            .expect("No buffer")
            .get::<u16>(*data_frame_header::TYPE_FIELD_OFFSET)
    }

    /**
     * Set the value of the header type field.
     *
     * @param type value to be set in the header.
     * @return this for a fluent API.
     */
    pub fn set_header_type(&mut self, header_type: u16) -> &Self {
        self.buffer
            .expect("No buffer")
            .put::<u16>(*data_frame_header::TYPE_FIELD_OFFSET, header_type);
        self
    }

    /**
     * Get the value stored in the reserve space at the end of a data frame header.
     *
     * @return the value stored in the reserve space at the end of a data frame header.
     */
    pub fn reserved_value(&self) -> i64 {
        self.buffer
            .expect("No buffer")
            .get::<i64>(*data_frame_header::RESERVED_VALUE_FIELD_OFFSET)
    }

    /**
     * Write the provided value into the reserved space at the end of the data frame header.
     *
     * @param value to be stored in the reserve space at the end of a data frame header.
     * @return this for fluent API semantics.
     */
    pub fn set_reserved_value(&mut self, value: i64) -> &Self {
        self.buffer
            .expect("No buffer")
            .put::<i64>(*data_frame_header::RESERVED_VALUE_FIELD_OFFSET, value);
        self
    }

    /**
     * Commit the message to the log buffer so that is it available to subscribers.
     */
    pub fn commit(&mut self) {
        self.buffer
            .expect("No buffer")
            .put_ordered::<i32>(0, self.buffer.expect("No buffer").capacity());
    }

    /**
     * Abort a claim of the message space to the log buffer so that log can progress ignoring this claim.
     */
    pub fn abort(&mut self) {
        self.buffer
            .expect("No buffer")
            .put::<u16>(*data_frame_header::TYPE_FIELD_OFFSET, data_frame_header::HDR_TYPE_PAD);
        self.buffer
            .expect("No buffer")
            .put_ordered::<i32>(0, self.buffer.expect("No buffer").capacity());
    }
}
