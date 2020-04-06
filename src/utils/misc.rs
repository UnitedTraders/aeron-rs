use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::time::{SystemTime, UNIX_EPOCH};

use cache_line_size::CACHE_LINE_SIZE;

use crate::utils::types::{Index, Moment};

pub const CACHE_LINE_LENGTH: Index = CACHE_LINE_SIZE as Index;

#[inline]
// Get system time since start of UNIX epoch in milliseconds (ms) (10^-3 sec)
pub fn unix_time_ms() -> Moment {
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("Can't get UNIX epoch.");

    since_the_epoch.as_secs() * 1000 + since_the_epoch.subsec_nanos() as u64 / 1_000_000
}

// Get system time since start of UNIX epoch in nanoseconds (ns) (10^-9 sec)
pub fn unix_time_ns() -> Moment {
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("Can't get UNIX epoch.");

    since_the_epoch.as_secs() * 1_000_000_000 + since_the_epoch.subsec_nanos() as u64
}

// Accepts Aeron style ASCII string (without zero termination). Outputs Rust String.
pub fn aeron_str_to_rust(raw_str: *const u8, length: i32) -> String {
    unsafe {
        let str_slice = std::slice::from_raw_parts(raw_str, length as usize);
        let mut zero_terminated: Vec<u8> = Vec::with_capacity(length as usize + 1);
        zero_terminated.extend_from_slice(str_slice);

        String::from_utf8_unchecked(zero_terminated)
    }
}

pub fn semantic_version_compose(major: i32, minor: i32, patch: i32) -> i32 {
    (major << 16) | (minor << 8) | patch
}

pub fn semantic_version_major(version: i32) -> u8 {
    ((version >> 16) & 0xFF) as u8
}

pub fn semantic_version_minor(version: i32) -> u8 {
    ((version >> 8) & 0xFF) as u8
}

pub fn semantic_version_patch(version: i32) -> u8 {
    (version & 0xFF) as u8
}

pub fn semantic_version_to_string(version: i32) -> String {
    format!(
        "{}.{}.{}",
        semantic_version_major(version),
        semantic_version_minor(version),
        semantic_version_patch(version)
    )
}

#[allow(dead_code)]
/// Allocate a buffer aligned on the cache size
pub fn alloc_buffer_aligned(size: Index) -> *mut u8 {
    unsafe {
        let layout = Layout::from_size_align_unchecked(size as usize, CACHE_LINE_SIZE);
        alloc_zeroed(layout)
    }
}

/// Deallocate a buffer aligned on a cache size
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn dealloc_buffer_aligned(buff_ptr: *mut u8, len: Index) {
    unsafe {
        if cfg!(debug_assertions) {
            // dealloc markers for debug
            for i in 0..len as isize {
                *buff_ptr.offset(i) = 0xff;
            }
        }

        let layout = Layout::from_size_align_unchecked(len as usize, CACHE_LINE_SIZE);
        dealloc(buff_ptr, layout)
    }
}

// This struct is used to set bool flag to true till the end of scope and
// set the flag to false when dropped.
pub struct CallbackGuard<'a> {
    is_in_callback: &'a mut bool,
}
impl<'a> CallbackGuard<'a> {
    pub fn new(flag: &'a mut bool) -> Self {
        let selfy = Self { is_in_callback: flag };
        *selfy.is_in_callback = true;
        selfy
    }
}

impl<'a> Drop for CallbackGuard<'a> {
    fn drop(&mut self) {
        *self.is_in_callback = false;
    }
}
