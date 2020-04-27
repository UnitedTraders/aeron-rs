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

use std::sync::atomic::{fence, AtomicI64, Ordering};

use crate::utils::types::Index;

/// AtomicVec stores elements of type T and provides thread safe operations on Vec.
/// Atomic behavior provided on AtomicVec as a whole.
#[derive(Default)]
pub struct AtomicVec<T: Clone> {
    buf: Vec<T>,
    begin_change: AtomicI64,
    end_change: AtomicI64,
}

impl<T: Clone> AtomicVec<T> {
    pub fn new() -> Self {
        Self {
            buf: vec![],
            begin_change: AtomicI64::from(-1),
            end_change: AtomicI64::from(-1),
        }
    }

    pub fn load(&self) -> &Vec<T> {
        loop {
            let change_number = self.end_change.load(Ordering::Acquire);

            fence(Ordering::Acquire);

            // Only return Vec when begin and end sequence numbers are the same meaning that
            // parallel store() operation is fully completed.
            if change_number == self.begin_change.load(Ordering::Acquire) {
                return &self.buf;
            }
        }
    }

    pub fn load_mut(&mut self) -> &mut Vec<T> {
        loop {
            let change_number = self.end_change.load(Ordering::Acquire);

            fence(Ordering::Acquire);

            if change_number == self.begin_change.load(Ordering::Acquire) {
                return &mut self.buf;
            }
        }
    }

    pub fn load_val(&mut self) -> Vec<T> {
        loop {
            let change_number = self.end_change.load(Ordering::Acquire);

            fence(Ordering::Acquire);

            if change_number == self.begin_change.load(Ordering::Acquire) {
                let tmp_vec = self.buf.clone();
                return tmp_vec;
            }
        }
    }

    pub fn store(&mut self, new_value: Vec<T>) {
        // Compute next change seq number
        let mut seq_no: i64 = self.begin_change.load(Ordering::Acquire) + 1;

        if seq_no == std::i64::MAX {
            seq_no = 0;
        }

        self.begin_change.store(seq_no, Ordering::Release);

        self.buf = new_value;

        self.end_change.store(seq_no, Ordering::Release);
    }

    pub fn add(&mut self, item: T) -> Vec<T> {
        let old_vec = self.load().clone();
        let mut new_vec = old_vec.clone();

        new_vec.push(item);

        self.store(new_vec);

        old_vec
    }

    /// Removes first element for which matching_fn returns true.
    /// Returns tuple (OldVector, position_of_deleted_item)
    pub fn remove(&mut self, matching_fn: impl Fn(&mut T) -> bool) -> Option<(Vec<T>, Index)> {
        let mut old_vec = self.load().clone();
        let mut new_vec = old_vec.clone();

        for (index, item) in old_vec.iter_mut().enumerate() {
            if matching_fn(item) {
                new_vec.remove(index);
                self.store(new_vec);
                return Some((old_vec, index as Index));
            }
        }

        None
    }
}
