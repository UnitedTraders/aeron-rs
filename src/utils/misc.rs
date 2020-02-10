use std::time::{SystemTime, UNIX_EPOCH};

use crate::utils::types::Moment;

#[inline]
// Get system time since start of UNIX epoch in milliseconds (ms)
pub fn unix_time() -> Moment {
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("Can't get UNIX epoch.");

    since_the_epoch.as_secs() * 1000 +
        since_the_epoch.subsec_nanos() as u64 / 1_000_000
}
