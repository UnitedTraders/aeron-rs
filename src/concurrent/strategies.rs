use std::time::Duration;

use crate::concurrent::atomics::cpu_pause;

trait Strategy {
    fn set_idle(&mut self, work_count: i32);
    fn idle(&mut self);
    fn reset(&mut self);
}

const BACK_OFF_STATE_NOT_IDLE: u8 = 0;
const BACK_OFF_STATE_SPINNING: u8 = 1;
const BACK_OFF_STATE_YIELDING: u8 = 2;
const BACK_OFF_STATE_PARKING: u8 = 3;

struct BackOffIdleStrategy {
    state: u8,
    spins: i32,
    max_spins: i32,
    yields: i32,
    max_yields: i32,
    park_period_ns: u64,
    min_park_period_ns: u64,
    max_park_period_ns: u64,
}

impl Strategy for BackOffIdleStrategy {
    fn set_idle(&mut self, work_count: i32) {
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

struct BusySpinIdleStrategy {}

impl BusySpinIdleStrategy {
    fn pause() {
        cpu_pause();
    }
}

impl Strategy for BusySpinIdleStrategy {
    fn set_idle(&mut self, work_count: i32) {
        if work_count > 0 {
            return;
        }
        Self::pause();
    }

    fn idle(&mut self) {
        Self::pause();
    }

    fn reset(&mut self) {
        unimplemented!()
    }
}

struct NoOpIdleStrategy {}

impl Strategy for NoOpIdleStrategy {
    fn set_idle(&mut self, _work_count: i32) {
        unimplemented!();
    }

    fn idle(&mut self) {
        unimplemented!();
    }

    fn reset(&mut self) {
        unimplemented!()
    }
}

struct SleepingIdleStrategy {
    duration: Duration,
}

impl SleepingIdleStrategy {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

impl Strategy for SleepingIdleStrategy {
    fn set_idle(&mut self, work_count: i32) {
        if 0 == work_count {
            std::thread::sleep(self.duration);
        }
    }

    fn idle(&mut self) {
        unimplemented!();
    }

    fn reset(&mut self) {
        std::thread::sleep(self.duration)
    }
}

struct YieldingIdleStrategy {}

impl Strategy for YieldingIdleStrategy {
    fn set_idle(&mut self, work_count: i32) {
        if 0 == work_count {
            return;
        }
        std::thread::yield_now();
    }

    fn idle(&mut self) {
        unimplemented!();
    }

    fn reset(&mut self) {
        std::thread::yield_now();
    }
}
