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

use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::utils::{bit_utils, memory_mapped_file::MemoryMappedFile, misc, types::Index};

/**
* Description of the command and control file used between driver and clients
*
* File Layout
* <pre>
*  +-----------------------------+
*  |          Meta Data          |
*  +-----------------------------+
*  |      to-driver Buffer       |
*  +-----------------------------+
*  |      to-clients Buffer      |
*  +-----------------------------+
*  |   Counters Metadata Buffer  |
*  +-----------------------------+
*  |    Counters Values Buffer   |
*  +-----------------------------+
*  |          Error Log          |
*  +-----------------------------+
* </pre>
* <p>
* Meta Data Layout {@link #CNC_VERSION}
* <pre>
*   0                   1                   2                   3
*   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
*  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*  |                      Aeron CnC Version                        |
*  +---------------------------------------------------------------+
*  |                   to-driver buffer length                     |
*  +---------------------------------------------------------------+
*  |                  to-clients buffer length                     |
*  +---------------------------------------------------------------+
*  |               Counters Metadata buffer length                 |
*  +---------------------------------------------------------------+
*  |                Counters Values buffer length                  |
*  +---------------------------------------------------------------+
*  |                   Error Log buffer length                     |
*  +---------------------------------------------------------------+
*  |                   Client Liveness Timeout                     |
*  |                                                               |
*  +---------------------------------------------------------------+
*  |                    Driver Start Timestamp                     |
*  |                                                               |
*  +---------------------------------------------------------------+
*  |                         Driver PID                            |
*  |                                                               |
*  +---------------------------------------------------------------+
* </pre>
*/

pub static CNC_FILE: &str = "cnc.dat";
pub const CNC_VERSION: i32 = 16;

#[derive(Copy, Clone)]
#[repr(C, packed(4))]
struct MetaDataDefn {
    cnc_version: i32,
    to_driver_buffer_length: i32,
    to_clients_buffer_length: i32,
    counter_metadata_buffer_length: i32,
    counter_values_buffer_length: i32,
    error_log_buffer_length: i32,
    client_liveness_timeout: i64,
    start_timestamp: i64,
    pid: i64,
}

lazy_static! {
    pub static ref META_DATA_LENGTH: Index =
        bit_utils::align(std::mem::size_of::<MetaDataDefn>() as Index, misc::CACHE_LINE_LENGTH * 2);
}

pub fn cnc_version_volatile(cnc_file: &MemoryMappedFile) -> i32 {
    let meta_data_buffer = cnc_file.atomic_buffer(0, cnc_file.memory_size());

    meta_data_buffer.get_volatile::<i32>(offset_of!(MetaDataDefn, cnc_version) as Index)
}

pub fn create_to_driver_buffer(cnc_file: &MemoryMappedFile) -> AtomicBuffer {
    let meta_data_buffer = cnc_file.atomic_buffer(0, cnc_file.memory_size());

    let meta_data = meta_data_buffer.get::<MetaDataDefn>(0);

    cnc_file.atomic_buffer(*META_DATA_LENGTH, meta_data.to_driver_buffer_length as Index)
}

pub fn create_to_clients_buffer(cnc_file: &MemoryMappedFile) -> AtomicBuffer {
    let meta_data_buffer = cnc_file.atomic_buffer(0, cnc_file.memory_size());

    let meta_data = meta_data_buffer.get::<MetaDataDefn>(0);
    cnc_file.atomic_buffer(
        *META_DATA_LENGTH + meta_data.to_driver_buffer_length as Index,
        meta_data.to_clients_buffer_length as Index,
    )
}

pub fn create_counter_metadata_buffer(cnc_file: &MemoryMappedFile) -> AtomicBuffer {
    let meta_data_buffer = cnc_file.atomic_buffer(0, cnc_file.memory_size());

    let meta_data = meta_data_buffer.get::<MetaDataDefn>(0);

    cnc_file.atomic_buffer(
        *META_DATA_LENGTH + meta_data.to_driver_buffer_length as Index + meta_data.to_clients_buffer_length as Index,
        meta_data.counter_metadata_buffer_length as Index,
    )
}

pub fn create_counter_values_buffer(cnc_file: &MemoryMappedFile) -> AtomicBuffer {
    let meta_data_buffer = cnc_file.atomic_buffer(0, cnc_file.memory_size());

    let meta_data = meta_data_buffer.get::<MetaDataDefn>(0);
    cnc_file.atomic_buffer(
        *META_DATA_LENGTH
            + meta_data.to_driver_buffer_length as Index
            + meta_data.to_clients_buffer_length as Index
            + meta_data.counter_metadata_buffer_length as Index,
        meta_data.counter_values_buffer_length as Index,
    )
}

pub fn create_error_log_buffer(cnc_file: &MemoryMappedFile) -> AtomicBuffer {
    let meta_data_buffer = cnc_file.atomic_buffer(0, cnc_file.memory_size());

    let meta_data = meta_data_buffer.get::<MetaDataDefn>(0);
    cnc_file.atomic_buffer(
        *META_DATA_LENGTH
            + meta_data.to_driver_buffer_length as Index
            + meta_data.to_clients_buffer_length as Index
            + meta_data.counter_metadata_buffer_length as Index
            + meta_data.counter_values_buffer_length as Index,
        meta_data.error_log_buffer_length as Index,
    )
}

pub fn client_liveness_timeout(cnc_file: &MemoryMappedFile) -> i64 {
    let meta_data_buffer = cnc_file.atomic_buffer(0, cnc_file.memory_size());

    let meta_data = meta_data_buffer.get::<MetaDataDefn>(0);

    meta_data.client_liveness_timeout
}

pub fn start_timestamp(cnc_file: &MemoryMappedFile) -> i64 {
    let meta_data_buffer = cnc_file.atomic_buffer(0, cnc_file.memory_size());

    let meta_data = meta_data_buffer.get::<MetaDataDefn>(0);

    meta_data.start_timestamp
}

pub fn pid(cnc_file: &MemoryMappedFile) -> i64 {
    let meta_data_buffer = cnc_file.atomic_buffer(0, cnc_file.memory_size());

    let meta_data = meta_data_buffer.get::<MetaDataDefn>(0);

    meta_data.pid
}
