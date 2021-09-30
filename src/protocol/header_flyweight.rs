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

use crate::command::flyweight::Flyweight;
use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::utils::types::Index;

pub const HEADER_DEFN_SIZE: Index = std::mem::size_of::<HeaderDefn>() as Index;

/**
 * Flyweight for command header fields.
 */
#[repr(C, packed(4))]
#[derive(Copy, Clone)]
pub struct HeaderDefn {
    frame_length: i32,
    version: i8,
    flags: i8,
    h_type: i16,
}

pub(crate) struct HeaderFlyweight {
    pub flyweight: Flyweight<HeaderDefn>,
}

impl HeaderFlyweight {
    pub fn new(buffer: AtomicBuffer, offset: Index) -> Self {
        Self {
            flyweight: Flyweight::new(buffer, offset),
        }
    }
}
