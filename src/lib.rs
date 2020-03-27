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

// TODO: Eliminate later
#![allow(dead_code)]
#![allow(clippy::cast_ptr_alignment)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::missing_safety_doc)]

pub mod aeron;
pub mod buffer_builder;
pub mod client_conductor;
pub mod cnc_file_descriptor;
pub mod command;
pub mod concurrent;
pub mod context;
pub mod counter;
pub mod driver_listener_adapter;
pub mod driver_proxy;
pub mod exclusive_publication;
pub mod fragment_assembler;
pub mod heartbeat_timestamp;
pub mod image;
pub mod protocol;
pub mod publication;
pub mod subscription;
pub mod utils;
