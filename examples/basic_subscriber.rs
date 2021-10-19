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
    slice,
    sync::atomic::{AtomicBool, AtomicI64, Ordering},
};

use aeron_rs::{
    aeron::Aeron,
    concurrent::{
        atomic_buffer::AtomicBuffer,
        logbuffer::header::Header,
        status::status_indicator_reader::channel_status_to_str,
        strategies::{SleepingIdleStrategy, Strategy},
    },
    context::Context,
    example_config::{DEFAULT_CHANNEL, DEFAULT_STREAM_ID},
    image::Image,
    utils::{errors::AeronError, types::Index},
};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref RUNNING: AtomicBool = AtomicBool::from(true);
    pub static ref SUBSCRIPTION_ID: AtomicI64 = AtomicI64::new(-1);
}

fn sig_int_handler() {
    RUNNING.store(false, Ordering::SeqCst);
}

#[derive(Clone)]
struct Settings {
    dir_prefix: String,
    channel: String,
    stream_id: i32,
}

impl Settings {
    pub fn new() -> Self {
        Self {
            dir_prefix: String::new(),
            channel: String::from(DEFAULT_CHANNEL),
            stream_id: DEFAULT_STREAM_ID.parse().unwrap(),
        }
    }
}

fn parse_cmd_line() -> Settings {
    Settings::new()
}

fn available_image_handler(image: &Image) {
    println!(
        "Available image correlation_id={} session_id={} at position={} from {}",
        image.correlation_id(),
        image.session_id(),
        image.position(),
        image.source_identity().to_str().unwrap()
    );
}

fn unavailable_image_handler(image: &Image) {
    println!(
        "Unavailable image correlation_id={} session_id={} at position={} from {}",
        image.correlation_id(),
        image.session_id(),
        image.position(),
        image.source_identity().to_str().unwrap()
    );
}

fn error_handler(error: AeronError) {
    println!("Error: {:?}", error);
}

fn on_new_fragment(buffer: &AtomicBuffer, offset: Index, length: Index, header: &Header) {
    unsafe {
        let slice_msg = slice::from_raw_parts_mut(buffer.buffer().offset(offset as isize), length as usize);
        let msg = CString::new(slice_msg).unwrap();
        println!(
            "Message to stream {} from session {} ({}@{}): <<{}>>",
            header.stream_id(),
            header.session_id(),
            length,
            offset,
            msg.to_str().unwrap()
        );
    }
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

    println!("Subscribing Pong at {} on Stream ID {}", settings.channel, settings.stream_id);

    let mut context = Context::new();

    if !settings.dir_prefix.is_empty() {
        context.set_aeron_dir(settings.dir_prefix.clone());
    }

    println!("Using CnC file: {}", context.cnc_file_name());

    context.set_new_subscription_handler(Box::new(|channel: CString, stream_id: i32, correlation_id: i64| {
        println!("Subscription: {} {} {}", channel.to_str().unwrap(), stream_id, correlation_id)
    }));
    context.set_available_image_handler(Box::new(available_image_handler));
    context.set_unavailable_image_handler(Box::new(unavailable_image_handler));
    context.set_error_handler(Box::new(error_handler));
    context.set_pre_touch_mapped_memory(true);

    let aeron = Aeron::new(context);

    if aeron.is_err() {
        println!("Error creating Aeron instance: {:?}", aeron.err());
        return;
    }

    let mut aeron = aeron.unwrap();

    let subscription_id = aeron
        .add_subscription(str_to_c(&settings.channel), settings.stream_id)
        .expect("Error adding subscription");

    SUBSCRIPTION_ID.store(subscription_id, Ordering::SeqCst);

    let mut subscription = aeron.find_subscription(subscription_id);
    while subscription.is_err() {
        std::thread::yield_now();
        subscription = aeron.find_subscription(subscription_id);
    }

    let subscription = subscription.unwrap();

    let channel_status = subscription.lock().expect("Fu").channel_status();

    println!(
        "Subscription channel status {}: {}",
        channel_status,
        channel_status_to_str(channel_status)
    );

    let idle_strategy = SleepingIdleStrategy::new(1000);

    while RUNNING.load(Ordering::SeqCst) {
        let fragments_read = subscription.lock().expect("Fu").poll(&mut on_new_fragment, 10);
        idle_strategy.idle_opt(fragments_read);
    }
}
