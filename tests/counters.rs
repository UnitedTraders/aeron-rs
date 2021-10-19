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

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use aeron_rs::aeron::Aeron;
use aeron_rs::context::Context;
use lazy_static::lazy_static;

use aeron_rs::concurrent::counters::CountersReader;

// IMPORTANT NOTICE: currently integration test can only work sequentially
// Run them with command: $ cargo test -- --test-threads=1
// Each test start Aeron media driver then stops it at the end. All driver runs are done
// with same /dev/shm/ directory thus parallel testing is not supported.
pub mod common;

static COUNTER_TYPE_ID: i32 = 1101;
static COUNTER_LABEL: &str = "COUNTER LABEL";

lazy_static! {
    pub static ref HANDLER_A_CALLED: AtomicBool = AtomicBool::from(false);
    pub static ref HANDLER_B_CALLED: AtomicBool = AtomicBool::from(false);
    pub static ref GONE_HANDLER_A_CALLED: AtomicBool = AtomicBool::from(false);
    pub static ref GONE_HANDLER_B_CALLED: AtomicBool = AtomicBool::from(false);
}

fn counter_handler_a(counters_reader: &CountersReader, registration_id: i64, counter_id: i32) {
    HANDLER_A_CALLED.store(true, Ordering::SeqCst);
    println!(
        "counter_handler_a: counter allocated with registration_id {}, counter_id {}, label {}, value {}",
        registration_id,
        counter_id,
        counters_reader.counter_label(counter_id).unwrap().to_str().unwrap(),
        counters_reader.counter_value(counter_id).unwrap()
    );
}

fn gone_counter_handler_a(counters_reader: &CountersReader, registration_id: i64, counter_id: i32) {
    GONE_HANDLER_A_CALLED.store(true, Ordering::SeqCst);
    println!(
        "gone_counter_handler_a: counter deallocated with registration_id {}, counter_id {}, label {}, value {}",
        registration_id,
        counter_id,
        counters_reader.counter_label(counter_id).unwrap().to_str().unwrap(),
        counters_reader.counter_value(counter_id).unwrap()
    );
}

fn counter_handler_b(counters_reader: &CountersReader, registration_id: i64, counter_id: i32) {
    HANDLER_B_CALLED.store(true, Ordering::SeqCst);
    println!(
        "counter_handler_b: counter with registration_id {}, counter_id {}, label {}, value {}",
        registration_id,
        counter_id,
        counters_reader.counter_label(counter_id).unwrap().to_str().unwrap(),
        counters_reader.counter_value(counter_id).unwrap()
    );
}

fn gone_counter_handler_b(counters_reader: &CountersReader, registration_id: i64, counter_id: i32) {
    GONE_HANDLER_B_CALLED.store(true, Ordering::SeqCst);
    println!(
        "gone_counter_handler_a: counter deallocated with registration_id {}, counter_id {}, label {}, value {}",
        registration_id,
        counter_id,
        counters_reader.counter_label(counter_id).unwrap().to_str().unwrap(),
        counters_reader.counter_value(counter_id).unwrap()
    );
}

#[test]
fn test_counter_create() {
    pretty_env_logger::init();
    let md = common::start_aeron_md();

    let mut context_a = Context::new();
    let mut context_b = Context::new();

    context_a.set_agent_name("Client A");
    context_a.set_available_counter_handler(Box::new(counter_handler_a));
    context_a.set_unavailable_counter_handler(Box::new(gone_counter_handler_a));

    context_b.set_agent_name("Client B");
    context_b.set_available_counter_handler(Box::new(counter_handler_b));
    context_b.set_unavailable_counter_handler(Box::new(gone_counter_handler_b));

    let mut aeron_a = Aeron::new(context_a).expect("Error creating Aeron A instance");
    let aeron_b = Aeron::new(context_b).expect("Error creating Aeron B instance");

    assert!(!aeron_a.is_closed());
    assert!(!aeron_b.is_closed());

    let counter_key: [u8; 3] = [3, 3, 3];

    let counter_id = aeron_a.add_counter(COUNTER_TYPE_ID, &counter_key, COUNTER_LABEL).unwrap();

    // Find counter from A client
    let mut counter_on_a_side = aeron_a.find_counter(counter_id);

    if counter_on_a_side.is_err() {
        // Give driver 1 sec to serve our request about adding new publication
        std::thread::sleep(Duration::from_millis(1000));
        counter_on_a_side = aeron_a.find_counter(counter_id);
    }

    assert!(!counter_on_a_side.unwrap().is_closed());

    // Both instances informed about counter creation
    assert!(HANDLER_A_CALLED.load(Ordering::SeqCst));
    assert!(HANDLER_B_CALLED.load(Ordering::SeqCst));

    common::stop_aeron_md(md);
}
