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

extern crate aeron_rs;

use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;
use std::{slice, thread};

use aeron_rs::{
    aeron::Aeron,
    concurrent::{
        atomic_buffer::{AlignedBuffer, AtomicBuffer},
        logbuffer::{buffer_claim::BufferClaim, header::Header},
        status::status_indicator_reader::CHANNEL_ENDPOINT_ACTIVE,
        strategies::{BusySpinIdleStrategy, SleepingIdleStrategy, Strategy},
    },
    context::Context,
    fragment_assembler::FragmentAssembler,
    utils::{
        errors::AeronError,
        types::{Index, I64_SIZE},
    },
};
use lazy_static::lazy_static;

use crate::common::{str_to_c, TEST_CHANNEL, TEST_STREAM_ID};

// IMPORTANT NOTICE: currently integration test can only work sequentially
// Run them with command: $ cargo test -- --test-threads=1
// Each test start Aeron media driver then stops it at the end. All driver runs are done
// with same /dev/shm/ directory thus parallel testing is not supported.
mod common;

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

#[test]
fn test_publication_create() {
    let md = common::start_aeron_md();

    let mut context = Context::new();

    context.set_new_publication_handler(Box::new(on_new_publication_handler));
    context.set_error_handler(Box::new(error_handler));
    context.set_pre_touch_mapped_memory(true);

    let mut aeron = Aeron::new(context).expect("Error creating Aeron instance");

    let publication_id = aeron
        .add_publication(str_to_c(TEST_CHANNEL), TEST_STREAM_ID)
        .expect("Error adding publication");

    let mut publication = aeron.find_publication(publication_id);

    if publication.is_err() {
        // Give driver 1 sec to serve our request about adding new publication
        std::thread::sleep(Duration::from_millis(1000));
        publication = aeron.find_publication(publication_id);
    }

    // At this point publication must be created and be available for publishing
    assert_eq!(publication.unwrap().lock().unwrap().channel_status(), CHANNEL_ENDPOINT_ACTIVE);

    common::stop_aeron_md(md);
}

fn on_new_subscription_handler(channel: CString, stream_id: i32, correlation_id: i64) {
    println!("Subscription: {} {} {}", channel.to_str().unwrap(), stream_id, correlation_id);
}

#[test]
fn test_subscription_create() {
    let md = common::start_aeron_md();

    let mut context = Context::new();

    context.set_new_subscription_handler(Box::new(on_new_subscription_handler));
    context.set_error_handler(Box::new(error_handler));
    context.set_pre_touch_mapped_memory(true);

    let mut aeron = Aeron::new(context).expect("Error creating Aeron instance");

    let subscription_id = aeron
        .add_subscription(str_to_c(TEST_CHANNEL), TEST_STREAM_ID)
        .expect("Error adding subscription");

    let mut subscription = aeron.find_subscription(subscription_id);

    if subscription.is_err() {
        // Give driver 1 sec to serve our request about adding new publication
        std::thread::sleep(Duration::from_millis(1000));
        subscription = aeron.find_subscription(subscription_id);
    }

    // At this point publication must be created and be available for publishing
    assert_eq!(
        subscription.unwrap().lock().unwrap().channel_status(),
        CHANNEL_ENDPOINT_ACTIVE
    );

    common::stop_aeron_md(md);
}

lazy_static! {
    pub static ref ON_NEW_FRAGMENT_CALLED: AtomicBool = AtomicBool::from(false);
}

fn on_new_fragment_check_payload(buffer: &AtomicBuffer, offset: Index, length: Index, _header: &Header) {
    ON_NEW_FRAGMENT_CALLED.store(true, Ordering::SeqCst);
    assert_eq!(length, 256);
    println!("on_new_fragment_check_payload: incoming fragment with {} length", length);
    unsafe {
        let slice_msg = slice::from_raw_parts_mut(buffer.buffer().offset(offset as isize), length as usize);
        for i in 0..length {
            assert_eq!(i as u8, slice_msg[i as usize]);
        }
    }
}

#[test]
fn test_unfragmented_msg() {
    let md = common::start_aeron_md();

    let mut context = Context::new();

    context.set_new_subscription_handler(Box::new(on_new_subscription_handler));
    context.set_error_handler(Box::new(error_handler));
    context.set_pre_touch_mapped_memory(true);

    let mut aeron = Aeron::new(context).expect("Error creating Aeron instance");

    let subscription_id = aeron
        .add_subscription(str_to_c(TEST_CHANNEL), TEST_STREAM_ID)
        .expect("Error adding subscription");

    let mut subscription = aeron.find_subscription(subscription_id);

    if subscription.is_err() {
        // Give driver 1 sec to serve our request about adding new publication
        std::thread::sleep(Duration::from_millis(1000));
        subscription = aeron.find_subscription(subscription_id);
    }

    let publication_id = aeron
        .add_publication(str_to_c(TEST_CHANNEL), TEST_STREAM_ID)
        .expect("Error adding publication");

    let mut publication = aeron.find_publication(publication_id);

    if publication.is_err() {
        // Give driver 1 sec to serve our request about adding new publication
        std::thread::sleep(Duration::from_millis(1000));
        publication = aeron.find_publication(publication_id);
    }

    let subscription = subscription.unwrap();
    let publication = publication.unwrap();

    // At this point publication must be created and be available for publishing
    assert_eq!(subscription.lock().unwrap().channel_status(), CHANNEL_ENDPOINT_ACTIVE);
    assert_eq!(publication.lock().unwrap().channel_status(), CHANNEL_ENDPOINT_ACTIVE);

    let buffer = AlignedBuffer::with_capacity(256);
    let src_buffer = AtomicBuffer::from_aligned(&buffer);

    // Fill in the buffer with exact data
    for i in 0..src_buffer.capacity() {
        src_buffer.put::<u8>(i, i as u8);
    }

    let result = publication.lock().unwrap().offer(src_buffer);

    if let Ok(code) = result {
        println!("Sent with code {}!", code);
    } else {
        panic!("Offer with error: {:?}", result.err());
    }

    let idle_strategy = SleepingIdleStrategy::new(1000);

    for _i in 0..3 {
        let fragments_read = subscription.lock().expect("Fu").poll(&mut on_new_fragment_check_payload, 10);
        if fragments_read > 0 {
            break;
        }
        idle_strategy.idle_opt(fragments_read);
    }

    assert!(ON_NEW_FRAGMENT_CALLED.load(Ordering::SeqCst));

    common::stop_aeron_md(md);
}

// Message in Aeron becomes fragmented when its length is grater than MTU. MTU is configured on driver
// level and can't be changed by client application.
// In this test we'll start aeronmd with MTU = 64 bytes.
// Also each fragment will have its own header.
// And also payload length is rounded to power of 2.
// All these gives real effective payload length of 32 bytes.
// Thus message with 256 body size will be split in to 8 fragments.
#[test]
fn test_fragmented_msg() {
    let md = common::start_aeron_md_mtu("64");

    let mut context = Context::new();

    context.set_new_subscription_handler(Box::new(on_new_subscription_handler));
    context.set_error_handler(Box::new(error_handler));
    context.set_pre_touch_mapped_memory(true);

    let mut aeron = Aeron::new(context).expect("Error creating Aeron instance");

    let subscription_id = aeron
        .add_subscription(str_to_c(TEST_CHANNEL), TEST_STREAM_ID)
        .expect("Error adding subscription");

    let mut subscription = aeron.find_subscription(subscription_id);

    if subscription.is_err() {
        // Give driver 1 sec to serve our request about adding new publication
        std::thread::sleep(Duration::from_millis(1000));
        subscription = aeron.find_subscription(subscription_id);
    }

    let publication_id = aeron
        .add_publication(str_to_c(TEST_CHANNEL), TEST_STREAM_ID)
        .expect("Error adding publication");

    let mut publication = aeron.find_publication(publication_id);

    if publication.is_err() {
        // Give driver 1 sec to serve our request about adding new publication
        std::thread::sleep(Duration::from_millis(1000));
        publication = aeron.find_publication(publication_id);
    }

    let subscription = subscription.unwrap();
    let publication = publication.unwrap();

    // At this point publication must be created and be available for publishing
    assert_eq!(subscription.lock().unwrap().channel_status(), CHANNEL_ENDPOINT_ACTIVE);
    assert_eq!(publication.lock().unwrap().channel_status(), CHANNEL_ENDPOINT_ACTIVE);

    let buffer = AlignedBuffer::with_capacity(256);
    let src_buffer = AtomicBuffer::from_aligned(&buffer);

    // Fill in the buffer with exact data
    for i in 0..src_buffer.capacity() {
        src_buffer.put::<u8>(i, i as u8);
    }

    let result = publication.lock().unwrap().offer(src_buffer);

    if let Ok(code) = result {
        println!("Sent with code {}!", code);
    } else {
        panic!("Offer with error: {:?}", result.err());
    }

    let mut handler_f = |buffer: &AtomicBuffer, offset, length, _header: &Header| {
        // This closure will be called when FragmentAssembler will assemble the full message for us
        println!("Checking reassembled message of length {}", length);
        assert_eq!(length, 256);
        unsafe {
            let slice_msg = slice::from_raw_parts_mut(buffer.buffer().offset(offset as isize), length as usize);
            for i in 0..length {
                assert_eq!(i as u8, slice_msg[i as usize]);
            }
        }
    };

    let mut fragment_assembler = FragmentAssembler::new(&mut handler_f, None);

    let idle_strategy = SleepingIdleStrategy::new(1000);

    let handler = &mut fragment_assembler.handler();

    for _i in 0..3 {
        let fragments_read = subscription.lock().expect("Fu").poll(handler, 10);
        idle_strategy.idle_opt(fragments_read);
    }

    common::stop_aeron_md(md);
}

lazy_static! {
    pub static ref SEQ_CHECK_FAILED: AtomicBool = AtomicBool::from(false);
    pub static ref LAST_RECEIVED_SEQ_NO: AtomicI64 = AtomicI64::from(-1);
}

#[allow(dead_code)]
#[allow(clippy::cast_ptr_alignment)]
fn on_new_fragment_check_seq_no(buffer: &AtomicBuffer, offset: Index, length: Index, _header: &Header) {
    assert_eq!(length, I64_SIZE);

    let prev_seq_no = LAST_RECEIVED_SEQ_NO.load(Ordering::SeqCst);

    unsafe {
        let this_seq_no = buffer.buffer().offset(offset as isize) as *mut i64;

        if *this_seq_no != prev_seq_no + 1 {
            SEQ_CHECK_FAILED.store(true, Ordering::SeqCst);
        }

        assert_eq!(*this_seq_no, prev_seq_no + 1);
    }

    LAST_RECEIVED_SEQ_NO.store(prev_seq_no + 1, Ordering::SeqCst);
}

// This test creates two threads: publisher and subscriber. They work simultaneously sending/receiving
// messages with monotonically growing sequence numbers.
#[test]
fn test_sequential_consistency() {
    let md = common::start_aeron_md();

    let messages_to_send: i64 = 10_000_000;

    let mut context = Context::new();

    context.set_new_subscription_handler(Box::new(on_new_subscription_handler));
    context.set_error_handler(Box::new(error_handler));
    context.set_pre_touch_mapped_memory(true);

    let mut aeron = Aeron::new(context).expect("Error creating Aeron instance");

    let subscription_id = aeron
        .add_subscription(str_to_c(TEST_CHANNEL), TEST_STREAM_ID)
        .expect("Error adding subscription");

    let mut subscription = aeron.find_subscription(subscription_id);

    if subscription.is_err() {
        // Give driver 1 sec to serve our request about adding new publication
        std::thread::sleep(Duration::from_millis(1000));
        subscription = aeron.find_subscription(subscription_id);
    }

    let publication_id = aeron
        .add_publication(str_to_c(TEST_CHANNEL), TEST_STREAM_ID)
        .expect("Error adding publication");

    let mut publication = aeron.find_publication(publication_id);

    if publication.is_err() {
        // Give driver 1 sec to serve our request about adding new publication
        std::thread::sleep(Duration::from_millis(1000));
        publication = aeron.find_publication(publication_id);
    }

    let subscription = subscription.unwrap();
    let publication = publication.unwrap();

    // At this point publication must be created and be available for publishing
    assert_eq!(subscription.lock().unwrap().channel_status(), CHANNEL_ENDPOINT_ACTIVE);
    assert_eq!(publication.lock().unwrap().channel_status(), CHANNEL_ENDPOINT_ACTIVE);

    let subscriber_thread = thread::Builder::new()
        .name(String::from("Subscriber thread"))
        .spawn(move || {
            let poll_idle_strategy = BusySpinIdleStrategy::default();
            for _messages_received in 0..messages_to_send {
                let fragments_read = subscription.lock().unwrap().poll(&mut on_new_fragment_check_seq_no, 100);

                poll_idle_strategy.idle_opt(fragments_read);
            }
        })
        .expect("Can't start Subscriber thread");

    let offer_idle_strategy = BusySpinIdleStrategy::default();
    let mut buffer_claim = BufferClaim::default();

    for seq_no in 0..messages_to_send {
        offer_idle_strategy.reset();

        while publication.lock().unwrap().try_claim(I64_SIZE, &mut buffer_claim).is_err() {
            offer_idle_strategy.idle();
        }

        buffer_claim.buffer().put::<i64>(buffer_claim.offset(), seq_no);
        buffer_claim.commit();
    }

    let _unused = subscriber_thread.join();

    common::stop_aeron_md(md);

    assert!(!SEQ_CHECK_FAILED.load(Ordering::SeqCst));
}
