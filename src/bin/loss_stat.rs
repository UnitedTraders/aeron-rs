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

use aeron_rs::{
    concurrent::reports::{self, loss_report_descriptor},
    context::Context,
    utils::memory_mapped_file::MemoryMappedFile,
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
    let filename = loss_report_descriptor::file(&settings.base_path);

    println!("Using file: {}", filename);

    MemoryMappedFile::get_file_size(&filename).unwrap_or_else(|_| {
        panic!("Loss report does not exist: {}", filename);
    });

    let loss_report_file = MemoryMappedFile::map_existing(filename, false).expect("Cannot map file");
    let buffer = loss_report_file.atomic_buffer(0, loss_report_file.memory_size());

    println!("OBSERVATION_COUNT, TOTAL_BYTES_LOST, FIRST_OBSERVATION, LAST_OBSERVATION, SESSION_ID, STREAM_ID, CHANNEL, SOURCE");

    let entries_read = reports::read(&buffer, |observation_count, loss_report_entry, channel, source| {
        println!(
            "{},{},{},{},{},{},{:#?},{:#?}",
            observation_count,
            { loss_report_entry.total_bytes_lost },
            format_date(loss_report_entry.first_observation_timestamp),
            format_date(loss_report_entry.last_observation_timestamp),
            loss_report_entry.session_id,
            loss_report_entry.stream_id,
            channel,
            source
        );
    });

    println!("{} entries read", entries_read);
}
