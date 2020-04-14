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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub struct RateReporter {
    report_interval_ns: u64,
    on_report: fn(f64, f64, u64, u64),
    halt: AtomicBool,
    total_bytes: AtomicU64,
    total_messages: AtomicU64,
    last_total_bytes: u64,
    last_total_messages: u64,
    last_timestamp: Instant,
}
impl RateReporter {
    pub fn new(report_interval_ns: u64, on_report_handler: fn(f64, f64, u64, u64)) -> Self {
        Self {
            report_interval_ns,
            on_report: on_report_handler,
            halt: AtomicBool::from(false),
            total_bytes: AtomicU64::new(0),
            total_messages: AtomicU64::new(0),
            last_total_bytes: 0,
            last_total_messages: 0,
            last_timestamp: Instant::now(),
        }
    }

    pub fn run(&mut self) {
        while !self.halt.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_nanos(self.report_interval_ns));
            self.report();
        }
    }

    pub fn report(&mut self) {
        let current_total_bytes: u64 = self.total_bytes.load(Ordering::Relaxed);
        let current_total_messages: u64 = self.total_messages.load(Ordering::Relaxed);
        let current_timestamp = Instant::now();

        let time_span_sec = (current_timestamp - self.last_timestamp).as_secs_f64();
        let messages_per_sec: f64 = (current_total_messages - self.last_total_messages) as f64 / time_span_sec;
        let bytes_per_sec: f64 = (current_total_bytes - self.last_total_bytes) as f64 / time_span_sec;

        (self.on_report)(messages_per_sec, bytes_per_sec, current_total_messages, current_total_bytes);

        self.last_total_bytes = current_total_bytes;
        self.last_total_messages = current_total_messages;
        self.last_timestamp = current_timestamp;
    }

    pub fn reset(&mut self) {
        let current_total_bytes: u64 = self.total_bytes.load(Ordering::Relaxed);
        let current_total_messages: u64 = self.total_messages.load(Ordering::Relaxed);
        let current_timestamp = Instant::now();

        self.last_total_bytes = current_total_bytes;
        self.last_total_messages = current_total_messages;
        self.last_timestamp = current_timestamp;
    }

    pub fn halt(&mut self) {
        self.halt.store(true, Ordering::SeqCst);
    }

    pub fn on_message(&mut self, messages: u64, bytes: u64) {
        let current_total_bytes = self.total_bytes.load(Ordering::Relaxed);
        let current_total_messages = self.total_messages.load(Ordering::Relaxed);

        self.total_bytes.store(current_total_bytes + bytes, Ordering::Relaxed);
        self.total_messages
            .store(current_total_messages + messages, Ordering::Relaxed);
    }
}
