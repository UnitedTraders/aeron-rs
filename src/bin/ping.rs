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

use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use hdrhistogram::Histogram;

use aeron_rs::aeron::Aeron;
use aeron_rs::concurrent::atomic_buffer::{AlignedBuffer, AtomicBuffer};
use aeron_rs::concurrent::logbuffer::header::Header;
use aeron_rs::concurrent::strategies::{BusySpinIdleStrategy, Strategy};
use aeron_rs::context::Context;
use aeron_rs::example_config::{
    DEFAULT_FRAGMENT_COUNT_LIMIT, DEFAULT_MESSAGE_LENGTH, DEFAULT_PONG_CHANNEL, DEFAULT_PONG_STREAM_ID,
};
use aeron_rs::fragment_assembler::FragmentAssembler;
use aeron_rs::image::Image;
use aeron_rs::publication::Publication;
use aeron_rs::subscription::Subscription;
use aeron_rs::utils::errors::AeronError;
use aeron_rs::utils::types::Index;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref RUNNING: AtomicBool = AtomicBool::from(true);
    pub static ref COUNT_DOWN: AtomicI64 = AtomicI64::new(1);
    pub static ref SUBSCRIPTION_ID: AtomicI64 = AtomicI64::new(-1);
    pub static ref PUBLICATION_ID: AtomicI64 = AtomicI64::new(-1);
    pub static ref HISTOGRAMM: Arc<Mutex<Histogram::<u64>>> = Arc::new(Mutex::new(
        Histogram::<u64>::new_with_bounds(1, 10 * 1000 * 1000 * 1000, 3).unwrap()
    ));
}

fn sig_int_handler() {
    RUNNING.store(false, Ordering::SeqCst);
}

#[derive(Clone)]
struct Settings {
    dir_prefix: String,
    ping_channel: String,
    pong_channel: String,
    ping_stream_id: i32,
    pong_stream_id: i32,
    number_of_warmup_messages: i64,
    number_of_messages: i64,
    message_length: i32,
    fragment_count_limit: i32,
}

impl Settings {
    pub fn new() -> Self {
        Self {
            dir_prefix: String::new(),
            ping_channel: String::from(DEFAULT_PONG_CHANNEL),
            pong_channel: String::from(DEFAULT_PONG_CHANNEL),
            ping_stream_id: DEFAULT_PONG_STREAM_ID,
            pong_stream_id: DEFAULT_PONG_STREAM_ID,
            number_of_warmup_messages: 0, //DEFAULT_NUMBER_OF_WARM_UP_MESSAGES,
            number_of_messages: 1,        //DEFAULT_NUMBER_OF_MESSAGES,
            message_length: DEFAULT_MESSAGE_LENGTH,
            fragment_count_limit: DEFAULT_FRAGMENT_COUNT_LIMIT,
        }
    }
}

fn parse_cmd_line() -> Settings {
    Settings::new()
}

fn send_ping_and_receive_pong(
    fragment_handler: &mut impl FnMut(&AtomicBuffer, Index, Index, &Header),
    publication: Arc<Publication>,
    subscription: Arc<Mutex<Subscription>>,
    settings: &Settings,
) {
    let buffer = AlignedBuffer::with_capacity(settings.message_length);
    let src_buffer = AtomicBuffer::from_aligned(&buffer);
    let idle_strategy: BusySpinIdleStrategy = Default::default();
    let mut subscription = subscription.lock().unwrap();

    for _i in 0..settings.number_of_messages {
        let position = loop {
            // timestamps in the message are relative to this app, so just send the timepoint directly.
            let mut start = Instant::now();
            unsafe {
                let slice = ::std::slice::from_raw_parts(&mut start as *mut Instant as *mut u8, std::mem::size_of_val(&start));
                src_buffer.put_bytes(0, slice);
            }
            let position = publication.offer_part(src_buffer, 0, settings.message_length).unwrap();

            if position < 0 {
                break position; // One of control statuses e.g. BACK_PRESSURED
            }
        };

        println!("Position after offer: {}", position);

        let mut maybe_image = subscription.image_by_index(0);

        while maybe_image.is_none() {
            println!("Waiting for image...");
            maybe_image = subscription.image_by_index(0);
        }
        let image = maybe_image.unwrap();

        idle_strategy.reset();
        //loop {
        //while image.poll(fragment_handler, settings.fragment_count_limit) <= 0 {
        image.poll(fragment_handler, settings.fragment_count_limit);
        idle_strategy.idle();
        println!("Polling image. Positions is: {}", image.position());
        //}
        /*
            if image.position() >= position {
                println!("Polling image COMPLETED. Positions is: {}", image.position());
                break;
            }
        }
        */
    }
}

fn on_new_subscription_handler(channel: CString, stream_id: i32, correlation_id: i64) {
    println!("Subscription: {} {} {}", channel.to_str().unwrap(), stream_id, correlation_id);
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

fn available_image_handler(image: &Image) {
    println!(
        "Available image correlation_id={} session_id={} at position={} from {}",
        image.correlation_id(),
        image.session_id(),
        image.position(),
        image.source_identity().to_str().unwrap()
    );

    if image.subscription_registration_id() == SUBSCRIPTION_ID.load(Ordering::SeqCst) {
        let mut cnt = COUNT_DOWN.load(Ordering::SeqCst);
        cnt -= 1;
        COUNT_DOWN.store(cnt, Ordering::SeqCst);
    }
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

fn str_to_c(val: &str) -> CString {
    CString::new(val).expect("Error converting str to CString")
}

fn main() {
    ctrlc::set_handler(move || {
        println!("received Ctrl+C!");
        sig_int_handler();
    })
    .expect("Error setting Ctrl-C handler");

    let settings = parse_cmd_line();

    println!(
        "Subscribing Pong at {} on Stream ID {}",
        settings.pong_channel, settings.pong_stream_id
    );
    println!(
        "Publishing Ping at {} on Stream ID {}",
        settings.ping_channel, settings.ping_stream_id
    );

    let mut context = Context::new();

    if !settings.dir_prefix.is_empty() {
        context.set_aeron_dir(settings.dir_prefix.clone());
    }

    println!("Using CnC file: {}", context.cnc_file_name());

    context.set_new_subscription_handler(on_new_subscription_handler);
    context.set_new_publication_handler(on_new_publication_handler);
    context.set_available_image_handler(available_image_handler);
    context.set_unavailable_image_handler(unavailable_image_handler);
    context.set_error_handler(error_handler);
    context.set_pre_touch_mapped_memory(true);
    //context.set_use_conductor_agent_invoker(true); // start it in one thread for debugging

    let mut aeron = Aeron::new(context);

    let subscription_id = aeron
        .add_subscription(str_to_c(&settings.pong_channel), settings.pong_stream_id)
        .expect("Error adding subscription");
    let publication_id = aeron
        .add_publication(str_to_c(&settings.ping_channel), settings.ping_stream_id)
        .expect("Error adding publication");

    SUBSCRIPTION_ID.store(subscription_id, Ordering::SeqCst);
    PUBLICATION_ID.store(publication_id, Ordering::SeqCst);

    let mut pong_subscription = aeron.find_subscription(subscription_id);
    while pong_subscription.is_err() {
        std::thread::yield_now();
        pong_subscription = aeron.find_subscription(subscription_id);
    }

    let mut ping_publication = aeron.find_publication(publication_id);
    while ping_publication.is_err() {
        std::thread::yield_now();
        ping_publication = aeron.find_publication(publication_id);
    }

    let ping_publication = ping_publication.unwrap();
    let pong_subscription = pong_subscription.unwrap();

    //while COUNT_DOWN.load(Ordering::SeqCst) > 0 {
    //    std::thread::yield_now();
    //}

    if settings.number_of_warmup_messages > 0 {
        let mut warmup_settings = settings.clone();
        warmup_settings.number_of_messages = warmup_settings.number_of_warmup_messages;

        let wstart = Instant::now();

        println!(
            "Warming up the media driver with {} messages of length {}",
            warmup_settings.number_of_warmup_messages, warmup_settings.message_length
        );

        let mut fragment_assembler = FragmentAssembler::new(
            |_buffer: &AtomicBuffer, _offset, _length, _header: &Header| println!("fragment_assembler called"),
            None,
        );

        send_ping_and_receive_pong(
            &mut fragment_assembler.handler(),
            ping_publication.clone(),
            pong_subscription.clone(),
            &warmup_settings,
        );

        let duration = Instant::now() - wstart;

        println!("Warmed up the media driver in {} ns", duration.as_nanos());
    }

    loop {
        HISTOGRAMM.lock().unwrap().reset();

        let mut fragment_assembler = FragmentAssembler::new(
            |buffer: &AtomicBuffer, offset: Index, _length: Index, _header: &Header| {
                let end = Instant::now();
                let mut start = Instant::now(); // Just to init it

                buffer.get_bytes(
                    offset,
                    &mut start as *mut Instant as *mut u8,
                    std::mem::size_of_val(&start) as i32,
                );
                let nano_rtt = end - start;

                let _ignored = HISTOGRAMM.lock().unwrap().record(nano_rtt.as_nanos() as u64);
            },
            None,
        );

        println!(
            "Pinging {} messages of length {} bytes each",
            settings.number_of_messages, settings.message_length
        );

        send_ping_and_receive_pong(
            &mut fragment_assembler.handler(),
            ping_publication.clone(),
            pong_subscription.clone(),
            &settings,
        );

        let histogram = HISTOGRAMM.lock().unwrap();
        for v in histogram.iter_recorded() {
            println!("{} ns - {}", v.value_iterated_to(), v.count_at_value());
        }

        if !RUNNING.load(Ordering::SeqCst) {
            break;
        }
    }
}
