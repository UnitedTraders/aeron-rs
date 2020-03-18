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

use std::collections::HashMap;

use aeron_rs::buffer_builder::BufferBuilder;
use aeron_rs::concurrent::atomic_buffer::AtomicBuffer;
use aeron_rs::concurrent::logbuffer::data_frame_header;
use aeron_rs::concurrent::logbuffer::frame_descriptor;
use aeron_rs::concurrent::logbuffer::header::Header;
use aeron_rs::utils::errors::AeronError;
use aeron_rs::utils::types::Index;

const DEFAULT_FRAGMENT_ASSEMBLY_BUFFER_LENGTH: isize = 4096;

trait Fragment: FnMut(&AtomicBuffer, Index, Index, &Header) -> Result<(), AeronError> {}

impl<T: FnMut(&AtomicBuffer, Index, Index, &Header) -> Result<(), AeronError>> Fragment for T {}

/**
 * A handler that sits in a chain-of-responsibility pattern that reassembles fragmented messages
 * so that the next handler in the chain only sees whole messages.
 * <p>
 * Unfragmented messages are delegated without copy. Fragmented messages are copied to a temporary
 * buffer for reassembly before delegation.
 * <p>
 * The Header passed to the delegate on assembling a message will be that of the last fragment.
 * <p>
 * Session based buffers will be allocated and grown as necessary based on the length of messages to be assembled.
 * When sessions go inactive see {@link on_unavailable_image_t}, it is possible to free the buffer by calling
 * {@link #deleteSessionBuffer(std::int32_t)}.
 */
struct FragmentAssembler {
    delegate: Box<dyn Fragment>,
    builder_by_session_id_map: HashMap<i32, BufferBuilder>,
    initial_buffer_length: isize,
}

impl FragmentAssembler {
    /**
     * Construct an adapter to reassembly message fragments and delegate on only whole messages.
     *
     * @param delegate            onto which whole messages are forwarded.
     * @param initialBufferLength to be used for each session.
     */
    pub fn new(delegate: Box<dyn Fragment>, initial_buffer_length: Option<isize>) -> Self {
        Self {
            delegate,
            builder_by_session_id_map: HashMap::new(),
            initial_buffer_length: initial_buffer_length.unwrap_or(DEFAULT_FRAGMENT_ASSEMBLY_BUFFER_LENGTH),
        }
    }

    /**
     * Compose a fragment_handler_t that calls the this FragmentAssembler instance for reassembly. Suitable for
     * passing to Subscription::poll(fragment_handler_t, int).
     *
     * @return fragment_handler_t composed with the FragmentAssembler instance
     */

    // FIXME: Handle lifetimes
    pub fn handler(&mut self) -> impl FnMut(&AtomicBuffer, Index, Index, &Header) -> Result<(), AeronError> + '_ {
        move |buffer, offset, length, header| self.on_fragment(buffer, offset, length, header)
    }

    /**
     * Free an existing session buffer to reduce memory pressure when an Image goes inactive or no more
     * large messages are expected.
     *
     * @param sessionId to have its buffer freed
     */
    pub fn delete_session_buffer(&mut self, session_id: i32) {
        self.builder_by_session_id_map.remove(&session_id);
    }

    #[inline]
    fn on_fragment(&mut self, buffer: &AtomicBuffer, offset: Index, length: Index, header: &Header) -> Result<(), AeronError> {
        let flags = header.flags();
        if (flags & frame_descriptor::UNFRAGMENTED) == frame_descriptor::UNFRAGMENTED {
            (*self.delegate)(buffer, offset, length, header)?;
        } else if (flags & frame_descriptor::BEGIN_FRAG) == frame_descriptor::BEGIN_FRAG {
            // FIXME: Check the logic to imitate C++ emplace
            let result = self
                .builder_by_session_id_map
                .insert(header.session_id(), BufferBuilder::new(self.initial_buffer_length));
            let mut builder = result.unwrap();
            builder.reset().append(buffer, offset, length, header)?;
        } else if let Some(builder) = self.builder_by_session_id_map.get_mut(&header.session_id()) {
            if builder.limit() != data_frame_header::LENGTH {
                builder.append(buffer, offset, length, header)?;
                if flags & frame_descriptor::END_FRAG == frame_descriptor::END_FRAG {
                    let msg_length = builder.limit() - data_frame_header::LENGTH;
                    let msg_buffer = AtomicBuffer::new(builder.buffer(), builder.limit());

                    (*self.delegate)(&msg_buffer, data_frame_header::LENGTH, msg_length, header)?;

                    builder.reset();
                }
            }
        }
        Ok(())
    }
}
