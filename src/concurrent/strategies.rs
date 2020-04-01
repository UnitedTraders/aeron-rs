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

use std::time::Duration;

use crate::concurrent::atomics::cpu_pause;
use crate::utils::types::Moment;

pub trait Strategy {
    fn idle_opt(&self, work_count: i32);
    fn idle(&self);
    fn reset(&self);
}

pub trait StrategyMut {
    fn idle_opt(&mut self, work_count: i32);
    fn idle(&mut self);
    fn reset(&mut self);
}

const BACK_OFF_STATE_NOT_IDLE: u8 = 0;
const BACK_OFF_STATE_SPINNING: u8 = 1;
const BACK_OFF_STATE_YIELDING: u8 = 2;
const BACK_OFF_STATE_PARKING: u8 = 3;

pub struct BackOffIdleStrategy {
    state: u8,
    spins: i32,
    max_spins: i32,
    yields: i32,
    max_yields: i32,
    park_period_ns: u64,
    min_park_period_ns: u64,
    max_park_period_ns: u64,
}

impl StrategyMut for BackOffIdleStrategy {
    fn idle_opt(&mut self, work_count: i32) {
        if work_count > 0 {
            self.reset();
        } else {
            self.idle();
        }
    }

    fn idle(&mut self) {
        match self.state {
            BACK_OFF_STATE_NOT_IDLE => {
                self.state = BACK_OFF_STATE_SPINNING;
                self.spins += 1;
            }
            BACK_OFF_STATE_SPINNING => {
                cpu_pause();
                self.spins += 1;
                if self.spins > self.max_spins {
                    self.state = BACK_OFF_STATE_YIELDING;
                    self.yields = 0;
                }
            }
            BACK_OFF_STATE_YIELDING => {
                self.yields += 1;
                if self.yields > self.max_yields {
                    self.state = BACK_OFF_STATE_PARKING;
                    self.park_period_ns = self.min_park_period_ns;
                } else {
                    std::thread::yield_now();
                }
            }
            BACK_OFF_STATE_PARKING => {}
            _ => {
                std::thread::sleep(Duration::from_nanos(self.park_period_ns));
                self.park_period_ns = std::cmp::min(self.park_period_ns * 2, self.max_park_period_ns);
            }
        }
    }

    fn reset(&mut self) {
        self.spins = 0;
        self.yields = 0;
        self.park_period_ns = self.min_park_period_ns;
        self.state = BACK_OFF_STATE_NOT_IDLE;
    }
}

#[derive(Default)]
pub struct BusySpinIdleStrategy {}

impl BusySpinIdleStrategy {
    fn pause() {
        cpu_pause();
    }
}

impl Strategy for BusySpinIdleStrategy {
    fn idle_opt(&self, work_count: i32) {
        if work_count > 0 {
            return;
        }
        Self::pause();
    }

    fn idle(&self) {
        Self::pause();
    }

    fn reset(&self) {}
}

pub struct NoOpIdleStrategy {}

impl Strategy for NoOpIdleStrategy {
    fn idle_opt(&self, _work_count: i32) {
        unimplemented!();
    }

    fn idle(&self) {
        unimplemented!();
    }

    fn reset(&self) {
        unimplemented!()
    }
}

pub struct SleepingIdleStrategy {
    duration: Moment,
}

impl SleepingIdleStrategy {
    pub fn new(duration: Moment) -> Self {
        Self { duration }
    }
}

impl Strategy for SleepingIdleStrategy {
    fn idle_opt(&self, work_count: i32) {
        if 0 == work_count {
            std::thread::sleep(Duration::from_millis(self.duration));
        }
    }

    fn idle(&self) {
        unimplemented!();
    }

    fn reset(&self) {
        std::thread::sleep(Duration::from_millis(self.duration))
    }
}

pub struct YieldingIdleStrategy {}

impl Strategy for YieldingIdleStrategy {
    fn idle_opt(&self, work_count: i32) {
        if 0 == work_count {
            return;
        }
        std::thread::yield_now();
    }

    fn idle(&self) {
        unimplemented!();
    }

    fn reset(&self) {
        std::thread::yield_now();
    }
}
