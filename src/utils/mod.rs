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

pub mod bit_utils;
pub mod errors;
pub mod log_buffers;
pub mod memory_mapped_file;
pub mod misc;
pub mod rate_reporter;
pub mod types;

#[macro_export]
macro_rules! ttrace {
        ($log_message:expr) => {
            if let Some(name) = std::thread::current().name() {
                log::trace!("({}) {}", name, $log_message);
            } else {
                log::trace!(concat!("(NoName) ", $log_message));
            }
        };
        ($log_message:expr, $($args:tt)*) => {
            if let Some(name) = std::thread::current().name() {
                log::trace!(concat!("({}) ", $log_message), name, $($args)*);
            } else {
                log::trace!(concat!("(NoName) ", $log_message), $($args)*);
            }
        };
    }
