use std::time::{SystemTime, UNIX_EPOCH};

use crate::utils::types::Moment;
use std::ffi::CStr;

#[inline]
// Get system time since start of UNIX epoch in milliseconds (ms)
pub fn unix_time() -> Moment {
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("Can't get UNIX epoch.");

    since_the_epoch.as_secs() * 1000 + since_the_epoch.subsec_nanos() as u64 / 1_000_000
}

// Converts Aeron string in to Rust string
// Aeron string has first 4 bytes as its length, then N bytes of the string itself, then \0 termination byte.
// So it is C-style zero terminated string preceded with 4 bytes of its length
pub unsafe fn aeron_str_to_rust(raw_str: *const u8) -> String {
    let length: i32 = *(raw_str as *const i32);
    let ret = String::from(CStr::from_ptr(raw_str.offset(4) as *const i8).to_str().unwrap());

    assert_eq!((length - 1) as usize, ret.len());

    ret
}

// Accepts C-style zero terminated string
pub fn aeron_str_no_len_to_rust(raw_str: *const u8) -> String {
    unsafe { String::from(CStr::from_ptr(raw_str as *const i8).to_str().unwrap()) }
}
