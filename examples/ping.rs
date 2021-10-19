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
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use aeron_rs::{
    aeron::Aeron,
    concurrent::{
        atomic_buffer::{AlignedBuffer, AtomicBuffer},
        logbuffer::header::Header,
        strategies::{BusySpinIdleStrategy, Strategy},
    },
    context::Context,
    example_config::{DEFAULT_FRAGMENT_COUNT_LIMIT, DEFAULT_MESSAGE_LENGTH, DEFAULT_PING_CHANNEL, DEFAULT_PING_STREAM_ID},
    fragment_assembler::FragmentAssembler,
    image::Image,
    publication::Publication,
    subscription::Subscription,
    utils::{errors::AeronError, types::Index},
};
use hdrhistogram::Histogram;
use lazy_static::lazy_static;
use structopt::StructOpt;

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

#[derive(StructOpt, Clone, Debug)]
#[structopt(name = "Aeron ping")]
struct CmdOpts {
    #[structopt(short = "p", long = "dir", default_value = "", help = "Prefix directory for aeron driver")]
    dir_prefix: String,
    #[structopt(short = "c", long = "ping_channel", default_value = DEFAULT_PING_CHANNEL, help = "Ping channel")]
    ping_channel: String,
    #[structopt(short = "C", long = "pong_channel", default_value = DEFAULT_PING_CHANNEL, help = "Pong channel")]
    pong_channel: String,
    #[structopt(short = "s", long, default_value = DEFAULT_PING_STREAM_ID, help = "Ping Stream ID")]
    ping_stream_id: i32,
    #[structopt(short = "S", long, default_value = DEFAULT_PING_STREAM_ID, help = "Pong Stream ID")]
    pong_stream_id: i32,
    #[structopt(short = "w", long, default_value = "0", help = "Number of Messages for warmup")]
    number_of_warmup_messages: i64,
    #[structopt(short = "m", long, default_value = "100", help = "Number of Messages")]
    number_of_messages: i64,
    #[structopt(short = "L", long, default_value = DEFAULT_MESSAGE_LENGTH, help = "Length of Messages")]
    message_length: i32,
    #[structopt(short = "f", long, default_value = DEFAULT_FRAGMENT_COUNT_LIMIT, help = "Fragment Count Limit")]
    fragment_count_limit: i32,
}

fn parse_cmd_line() -> CmdOpts {
    CmdOpts::from_args()
}

fn send_ping_and_receive_pong(
    mut fragment_handler: impl FnMut(&AtomicBuffer, Index, Index, &Header),
    publication: Arc<Mutex<Publication>>,
    subscription: Arc<Mutex<Subscription>>,
    settings: &CmdOpts,
) {
    let buffer = AlignedBuffer::with_capacity(settings.message_length);
    let src_buffer = AtomicBuffer::from_aligned(&buffer);
    let idle_strategy: BusySpinIdleStrategy = Default::default();

    for _i in 0..settings.number_of_messages {
        let position = loop {
            // timestamps in the message are relative to this app, so just send the timepoint directly.
            let mut start = Instant::now();
            unsafe {
                let slice = ::std::slice::from_raw_parts(&mut start as *mut Instant as *mut u8, std::mem::size_of_val(&start));
                src_buffer.put_bytes(0, slice);
            }
            let position = publication
                .lock()
                .unwrap()
                .offer_part(src_buffer, 0, settings.message_length)
                .unwrap();

            if position > 0 {
                break position;
            }
        };

        // Wait for image
        while subscription.lock().unwrap().image_by_index(0).is_none() {
            std::thread::sleep(Duration::from_millis(1000));
        }

        let mut subscription = subscription.lock().unwrap(); // Lock subscription. Means that it can't be changed by incoming messages (e.g. new images)
        let image = subscription.image_by_index(0).unwrap();

        idle_strategy.reset();

        loop {
            while image.poll(&mut fragment_handler, settings.fragment_count_limit) <= 0 {
                idle_strategy.idle();
            }

            if image.position() >= position as i64 {
                break;
            }
        }
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
    pretty_env_logger::init();
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

    context.set_new_subscription_handler(Box::new(on_new_subscription_handler));
    context.set_new_publication_handler(Box::new(on_new_publication_handler));
    context.set_available_image_handler(Box::new(available_image_handler));
    context.set_unavailable_image_handler(Box::new(unavailable_image_handler));
    context.set_error_handler(Box::new(error_handler));
    context.set_pre_touch_mapped_memory(true);
    //context.set_use_conductor_agent_invoker(true); // start it in one thread for debugging

    let aeron = Aeron::new(context);

    if aeron.is_err() {
        println!("Error creating Aeron instance: {:?}", aeron.err());
        return;
    }

    let mut aeron = aeron.unwrap();

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

        let mut handler_f = |_buffer: &AtomicBuffer, _offset, _length, _header: &Header| println!("fragment_assembler called");

        let mut fragment_assembler = FragmentAssembler::new(&mut handler_f, None);

        send_ping_and_receive_pong(
            fragment_assembler.handler(),
            ping_publication.clone(),
            pong_subscription.clone(),
            &warmup_settings,
        );

        let duration = Instant::now() - wstart;

        println!("Warmed up the media driver in {} ns", duration.as_nanos());
    }

    loop {
        HISTOGRAMM.lock().unwrap().reset();

        let mut handler_f = |buffer: &AtomicBuffer, offset: Index, _length: Index, _header: &Header| {
            let end = Instant::now();
            let mut start = Instant::now(); // Just to init it

            buffer.get_bytes(
                offset,
                &mut start as *mut Instant as *mut u8,
                std::mem::size_of_val(&start) as i32,
            );
            let nano_rtt = end - start;

            let _ignored = HISTOGRAMM.lock().unwrap().record(nano_rtt.as_nanos() as u64);
        };

        let mut fragment_assembler = FragmentAssembler::new(&mut handler_f, None);

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

        std::thread::sleep(Duration::from_millis(1000));
    }
}
