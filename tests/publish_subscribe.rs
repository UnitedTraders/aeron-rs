extern crate aeron_rs;

use aeron_rs::context::Context;
use aeron_rs::aeron::Aeron;
use aeron_rs::utils::errors::AeronError;
use std::ffi::CString;
use crate::common::{TEST_CHANNEL, TEST_STREAM_ID, str_to_c};
use std::time::Duration;
use aeron_rs::concurrent::status::status_indicator_reader::CHANNEL_ENDPOINT_ACTIVE;

// IMPORTANT NOTICE: currently integration test can only work sequentially
// Run them with command: $ cargo test --integration -- --test-threads=1
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

    context.set_new_publication_handler(on_new_publication_handler);
    context.set_error_handler(error_handler);
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
    assert_eq!(publication.unwrap().channel_status(), CHANNEL_ENDPOINT_ACTIVE);

    common::stop_aeron_md(md);
}