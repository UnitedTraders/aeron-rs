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
use lazy_static::lazy_static;

use crate::offset_of;
use crate::utils::types::Index;

#[repr(C, packed(4))]
pub struct DataFrameHeaderDefn {
    frame_length: i32,
    version: u8,
    flags: u8,
    frame_type: u16,
    term_offset: i32,
    session_id: i32,
    stream_id: i32,
    term_id: i32,
    reserved_value: i64,
}

lazy_static! {
    pub static ref FRAME_LENGTH_FIELD_OFFSET: i32 = offset_of!(DataFrameHeaderDefn, frame_length);
    pub static ref VERSION_FIELD_OFFSET: i32 = offset_of!(DataFrameHeaderDefn, version);
    pub static ref FLAGS_FIELD_OFFSET: i32 = offset_of!(DataFrameHeaderDefn, flags);
    pub static ref TYPE_FIELD_OFFSET: i32 = offset_of!(DataFrameHeaderDefn, frame_type);
    pub static ref TERM_OFFSET_FIELD_OFFSET: i32 = offset_of!(DataFrameHeaderDefn, term_offset);
    pub static ref SESSION_ID_FIELD_OFFSET: i32 = offset_of!(DataFrameHeaderDefn, session_id);
    pub static ref STREAM_ID_FIELD_OFFSET: i32 = offset_of!(DataFrameHeaderDefn, stream_id);
    pub static ref TERM_ID_FIELD_OFFSET: i32 = offset_of!(DataFrameHeaderDefn, term_id);
    pub static ref RESERVED_VALUE_FIELD_OFFSET: i32 = offset_of!(DataFrameHeaderDefn, reserved_value);
}

pub const DATA_OFFSET: Index = std::mem::size_of::<DataFrameHeaderDefn>() as Index;
pub const LENGTH: Index = DATA_OFFSET;

pub const HDR_TYPE_PAD: u16 = 0x00;
pub const HDR_TYPE_DATA: u16 = 0x01;
pub const HDR_TYPE_NAK: u16 = 0x02;
pub const HDR_TYPE_SM: u16 = 0x03;
pub const HDR_TYPE_ERR: u16 = 0x04;
pub const HDR_TYPE_SETUP: u16 = 0x05;
pub const HDR_TYPE_EXT: u16 = 0xFFFF;

pub const CURRENT_VERSION: u8 = 0x0;
