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

use aeron_rs::cnc_file_descriptor::{self, CNC_FILE, CNC_VERSION};
use aeron_rs::context::Context;
use aeron_rs::utils::{
    errors::error_log_reader,
    memory_mapped_file::MemoryMappedFile,
    misc::{semantic_version_major, semantic_version_to_string},
};
use chrono::{Local, TimeZone};

struct CmdOpts {
    base_path: String,
}

impl Default for CmdOpts {
    fn default() -> Self {
        Self {
            base_path: Context::default_aeron_path(),
        }
    }
}

fn parse_cmd_line() -> CmdOpts {
    CmdOpts::default()
}

fn format_date(milliseconds_since_epoch: i64) -> String {
    // yyyy-MM-dd HH:mm:ss.SSSZ
    let time = Local.timestamp_millis(milliseconds_since_epoch);
    time.to_string()
}

fn main() {
    pretty_env_logger::init();
    let settings = parse_cmd_line();

    let cnc_file_name = settings.base_path + "/" + CNC_FILE;

    println!("Opening CnC file: {}", cnc_file_name);

    let cnc_file = MemoryMappedFile::map_existing(&cnc_file_name, false).expect("Cannot map file");
    let cnc_version = cnc_file_descriptor::cnc_version_volatile(&cnc_file);

    if semantic_version_major(cnc_version) != semantic_version_major(CNC_VERSION) {
        panic!(
            "CNC version is not supported:\n file={}\n app={}",
            semantic_version_to_string(cnc_version),
            semantic_version_to_string(CNC_VERSION)
        );
    }

    let error_buffer = cnc_file_descriptor::create_error_log_buffer(&cnc_file);

    let distinct_error_count = error_log_reader::read(
        error_buffer,
        |observation_count, first_observation_timestamp, last_observation_timestamp, encoded_exception| {
            println!(
                "***\n{} observations from {} to {} for:\n{}\n",
                observation_count,
                format_date(first_observation_timestamp),
                format_date(last_observation_timestamp),
                encoded_exception.into_string().expect("Cannot convert exception")
            );
        },
        0,
    );

    println!("\n{} distinct errors observed.\n", distinct_error_count);
}
