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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::client_conductor::ClientConductor;
use crate::concurrent::{atomic_buffer::AtomicBuffer, atomic_counter::AtomicCounter};
use crate::utils::errors::AeronError;

pub struct Counter {
    // inherits from AtomicCounter
    atomic_counter: AtomicCounter,
    client_conductor: Arc<Mutex<ClientConductor>>,
    registration_id: i64,
    is_closed: AtomicBool,
}

impl Counter {
    pub fn new(
        client_conductor: Arc<Mutex<ClientConductor>>,
        buffer: AtomicBuffer,
        registration_id: i64,
        counter_id: i32,
    ) -> Self {
        Self {
            atomic_counter: AtomicCounter::new(buffer, counter_id),
            client_conductor,
            registration_id,
            is_closed: AtomicBool::from(false),
        }
    }

    pub fn registration_id(&self) -> i64 {
        self.registration_id
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::SeqCst)
    }

    pub fn close(&self) {
        self.is_closed.store(true, Ordering::SeqCst);
    }

    pub fn state(&self) -> Result<i32, AeronError> {
        let cc = self.client_conductor.lock().expect("Mutex poisoned");
        let cr = cc.counters_reader()?;
        cr.counter_state(self.atomic_counter.id())
    }

    pub fn label(&self) -> Result<CString, AeronError> {
        let cc = self.client_conductor.lock().expect("Mutex poisoned");
        let cr = cc.counters_reader()?;
        cr.counter_label(self.atomic_counter.id())
    }

    /// Inherited from AtomicCounter
    pub fn id(&self) -> i32 {
        self.atomic_counter.id()
    }
}

impl Drop for Counter {
    fn drop(&mut self) {
        let _ignored = self
            .client_conductor
            .lock()
            .expect("Mutex poisoned")
            .release_counter(self.registration_id);
    }
}
