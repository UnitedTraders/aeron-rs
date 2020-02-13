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

use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::concurrent::logbuffer::{data_frame_header, frame_descriptor, log_buffer_descriptor};
use crate::utils::bit_utils::align;
use crate::utils::bit_utils::number_of_trailing_zeroes;
use crate::utils::types::Index;

// FIXME: this is temporary stub
pub struct Context {}

/**
 * Represents the header of the data frame for accessing meta data fields.
 */
// TODO: originally there was pointer *void on context. Need to check all types
// which could be stored in this field and make it rusty.
pub struct Header {
    context: Context,
    buffer: AtomicBuffer,
    offset: Index,
    initial_term_id: i32,
    position_bits_to_shift: i32,
}

impl Header {
    pub fn new(initial_term_id: i32, capacity: Index, context: Context, buffer: AtomicBuffer) -> Self {
        Self {
            context,
            initial_term_id,
            offset: 0,
            position_bits_to_shift: number_of_trailing_zeroes(capacity as i32),
            buffer,
        }
    }

    /**
     * Get the initial term id this stream started at.
     *
     * @return the initial term id this stream started at.
     */
    pub fn initial_term_id(&self) -> i32 {
        self.initial_term_id
    }

    pub fn set_initial_term_id(&mut self, initial_term_id: i32) {
        self.initial_term_id = initial_term_id;
    }

    /**
     * The offset at which the frame begins.
     *
     * @return offset at which the frame begins.
     */
    pub fn offset(&self) -> Index {
        self.offset
    }

    pub fn set_offset(&mut self, offset: Index) {
        self.offset = offset;
    }

    /**
     * The AtomicBuffer containing the header.
     *
     * @return AtomicBuffer containing the header.
     */
    pub fn buffer(&self) -> &AtomicBuffer {
        &self.buffer
    }

    pub fn set_buffer(&mut self, buffer: AtomicBuffer) {
        self.buffer = buffer;
    }

    /**
     * The total length of the frame including the header.
     *
     * @return the total length of the frame including the header.
     */
    pub fn frame_length(&self) -> i32 {
        self.buffer.get::<i32>(self.offset)
    }

    /**
     * The session ID to which the frame belongs.
     *
     * @return the session ID to which the frame belongs.
     */
    pub fn session_id(&self) -> i32 {
        self.buffer
            .get::<i32>(self.offset + *data_frame_header::SESSION_ID_FIELD_OFFSET)
    }

    /**
     * The stream ID to which the frame belongs.
     *
     * @return the stream ID to which the frame belongs.
     */
    pub fn stream_id(&self) -> i32 {
        self.buffer
            .get::<i32>(self.offset + *data_frame_header::STREAM_ID_FIELD_OFFSET)
    }

    /**
     * The term ID to which the frame belongs.
     *
     * @return the term ID to which the frame belongs.
     */
    pub fn term_id(&self) -> i32 {
        self.buffer.get::<i32>(self.offset + *data_frame_header::TERM_ID_FIELD_OFFSET)
    }

    /**
     * The offset in the term at which the frame begins. This will be the same as {@link #offset()}
     *
     * @return the offset in the term at which the frame begins.
     */
    pub fn term_offset(&self) -> i32 {
        self.offset as i32
    }

    /**
     * The type of the the frame which should always be {@link DataFrameHeader::HDR_TYPE_DATA}
     *
     * @return type of the the frame which should always be {@link DataFrameHeader::HDR_TYPE_DATA}
     */
    pub fn frame_type(&self) -> u16 {
        self.buffer.get::<u16>(self.offset + *data_frame_header::TYPE_FIELD_OFFSET)
    }

    /**
     * The flags for this frame. Valid flags are {@link DataFrameHeader::BEGIN_FLAG}
     * and {@link DataFrameHeader::END_FLAG}. A convenience flag {@link DataFrameHeader::BEGIN_AND_END_FLAGS}
     * can be used for both flags.
     *
     * @return the flags for this frame.
     */
    pub fn flags(&self) -> u8 {
        self.buffer.get::<u8>(self.offset + *data_frame_header::FLAGS_FIELD_OFFSET)
    }

    /**
     * Get the current position to which the Image has advanced on reading this message.
     *
     * @return the current position to which the Image has advanced on reading this message.
     */
    pub fn position(&self) -> i64 {
        let resulting_offset = align(self.term_offset() + self.frame_length(), frame_descriptor::FRAME_ALIGNMENT);
        log_buffer_descriptor::compute_position(
            self.term_id(),
            resulting_offset,
            self.position_bits_to_shift,
            self.initial_term_id,
        )
    }

    /**
     * Get the value stored in the reserve space at the end of a data frame header.
     *
     * @return the value stored in the reserve space at the end of a data frame header.
     */
    pub fn reserved_value(&self) -> i64 {
        self.buffer
            .get::<i64>(self.offset + *data_frame_header::RESERVED_VALUE_FIELD_OFFSET)
    }

    /**
     * Get a pointer to the context associated with this message. Only valid during poll handling. Is normally a
     * pointer to an Image instance.
     *
     * @return a pointer to the context associated with this message.
     */
    pub fn context(&self) -> &Context {
        &self.context
    }
}
