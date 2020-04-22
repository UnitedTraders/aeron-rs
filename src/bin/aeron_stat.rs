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

use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};

use aeron_rs::{
    cnc_file_descriptor,
    concurrent::counters::CountersReader,
    context::Context,
    utils::{
        memory_mapped_file::MemoryMappedFile,
        misc::{semantic_version_major, semantic_version_to_string},
    },
};
use chrono::Local;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref RUNNING: AtomicBool = AtomicBool::from(true);
}

fn sig_int_handler() {
    RUNNING.store(false, Ordering::SeqCst);
}

struct CmdOpts {
    base_path: String,
    update_interval_ms: u64,
}

impl Default for CmdOpts {
    fn default() -> Self {
        Self {
            base_path: Context::default_aeron_path(),
            update_interval_ms: 1000,
        }
    }
}

fn parse_cmd_line() -> CmdOpts {
    CmdOpts::default()
}

fn get_date() -> String {
    // yyyy-MM-dd HH:mm:ss.SSSZ
    Local::now().to_string()
}

fn main() {
    pretty_env_logger::init();
    ctrlc::set_handler(move || {
        println!("received Ctrl+C!");
        sig_int_handler();
    })
    .expect("Error setting Ctrl-C handler");

    let settings = parse_cmd_line();
    let filename = settings.base_path + "/" + cnc_file_descriptor::CNC_FILE;

    println!("Using file: {}", filename);

    let cnc_file = MemoryMappedFile::map_existing(filename, false).expect("Can't map file");
    let cnc_version = cnc_file_descriptor::cnc_version_volatile(&cnc_file);

    if semantic_version_major(cnc_version) != semantic_version_major(cnc_file_descriptor::CNC_VERSION) {
        panic!(
            "CNC version is not supported: file={} app={}",
            semantic_version_to_string(cnc_version),
            semantic_version_to_string(cnc_file_descriptor::CNC_VERSION)
        );
    }

    let client_liveness_timeout_ns = cnc_file_descriptor::client_liveness_timeout(&cnc_file);
    let pid = cnc_file_descriptor::pid(&cnc_file);

    let metadata_buffer = cnc_file_descriptor::create_counter_metadata_buffer(&cnc_file);
    let values_buffer = cnc_file_descriptor::create_counter_values_buffer(&cnc_file);

    let counters = CountersReader::new(metadata_buffer, values_buffer);

    while RUNNING.load(Ordering::SeqCst) {
        let date = get_date();
        println!(
            "[{}] - Aeron Stat (CnC v{}), pid: {}, client liveness: {} ns",
            date,
            semantic_version_to_string(cnc_version),
            pid,
            client_liveness_timeout_ns
        );
        println!("===========================");

        counters.for_each(|counter_id, _, _buffer, l| {
            let value = counters.counter_value(counter_id).expect("Get an error from counter");

            println!("{:>3}:{:>20} - {}", counter_id, value, l.into_string().unwrap());
        });
        println!("===========================");
        thread::sleep(Duration::from_millis(settings.update_interval_ms));
    }

    println!("Exiting...")
}
