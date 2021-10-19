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
    ffi::CString,
    io::{stdout, Write},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use aeron_rs::{
    aeron::Aeron,
    concurrent::{
        atomic_buffer::{AlignedBuffer, AtomicBuffer},
        status::status_indicator_reader::channel_status_to_str,
    },
    context::Context,
    example_config::{DEFAULT_CHANNEL, DEFAULT_STREAM_ID},
    utils::errors::AeronError,
};
use lazy_static::lazy_static;
use nix::NixPath;

lazy_static! {
    pub static ref RUNNING: AtomicBool = AtomicBool::from(true);
}

fn sig_int_handler() {
    RUNNING.store(false, Ordering::SeqCst);
}

#[derive(Clone)]
struct Settings {
    dir_prefix: String,
    channel: String,
    stream_id: i32,
    number_of_messages: i64,
    linger_timeout_ms: u64,
}

impl Settings {
    pub fn new() -> Self {
        Self {
            dir_prefix: String::new(),
            channel: String::from(DEFAULT_CHANNEL),
            stream_id: DEFAULT_STREAM_ID.parse().unwrap(),
            number_of_messages: 10,
            linger_timeout_ms: 10000,
        }
    }
}

fn parse_cmd_line() -> Settings {
    Settings::new()
}

fn error_handler(error: AeronError) {
    println!("Error: {:?}", error);
}

fn on_new_publication_handler(channel: CString, stream_id: i32, session_id: i32, correlation_id: i64) {
    println!(
        "Publication: {} {} {} {}",
        channel.to_str().unwrap(),
        stream_id,
        session_id,
        correlation_id
    );
}

fn str_to_c(val: &str) -> CString {
    CString::new(val).expect("Error converting str to CString")
}

fn main() {
    pretty_env_logger::init();
    ctrlc::set_handler(move || {
        println!("received Ctrl+C!");
        sig_int_handler();
    })
    .expect("Error setting Ctrl-C handler");

    let settings = parse_cmd_line();

    println!(
        "Publishing to channel {} on Stream ID {}",
        settings.channel, settings.stream_id
    );

    let mut context = Context::new();

    if !settings.dir_prefix.is_empty() {
        context.set_aeron_dir(settings.dir_prefix.clone());
    }

    println!("Using CnC file: {}", context.cnc_file_name());

    context.set_new_publication_handler(Box::new(on_new_publication_handler));
    context.set_error_handler(Box::new(error_handler));
    context.set_pre_touch_mapped_memory(true);

    let aeron = Aeron::new(context);

    if aeron.is_err() {
        println!("Error creating Aeron instance: {:?}", aeron.err());
        return;
    }

    let mut aeron = aeron.unwrap();

    // add the publication to start the process
    let publication_id = aeron
        .add_publication(str_to_c(&settings.channel), settings.stream_id)
        .expect("Error adding publication");

    let mut publication = aeron.find_publication(publication_id);
    while publication.is_err() {
        std::thread::yield_now();
        publication = aeron.find_publication(publication_id);
    }

    let publication = publication.unwrap();

    let channel_status = publication.lock().unwrap().channel_status();

    println!(
        "Publication channel status {}: {} ",
        channel_status,
        channel_status_to_str(channel_status)
    );

    let buffer = AlignedBuffer::with_capacity(256);
    let src_buffer = AtomicBuffer::from_aligned(&buffer);

    for i in 0..settings.number_of_messages {
        if !RUNNING.load(Ordering::SeqCst) {
            break;
        }

        let str_msg = format!("Basic publisher msg #{}", i);
        let c_str_msg = CString::new(str_msg).unwrap();

        src_buffer.put_bytes(0, c_str_msg.as_bytes());

        println!("offering {}/{}", i + 1, settings.number_of_messages);
        let _unused = stdout().flush();

        let result = publication.lock().unwrap().offer_part(src_buffer, 0, c_str_msg.len() as i32);

        if let Ok(code) = result {
            println!("Sent with code {}!", code);
        } else {
            println!("Offer with error: {:?}", result.err());
        }

        if !publication.lock().unwrap().is_connected() {
            println!("No active subscribers detected");
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    println!("Done sending.");

    if settings.linger_timeout_ms > 0 {
        println!("Lingering for {} milliseconds.", settings.linger_timeout_ms);
        std::thread::sleep(Duration::from_millis(settings.linger_timeout_ms));
    }
}
