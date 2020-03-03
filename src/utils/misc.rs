use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ffi::CStr;
use std::time::{SystemTime, UNIX_EPOCH};

use cache_line_size::CACHE_LINE_SIZE;

use crate::utils::types::{Index, Moment};

pub const CACHE_LINE_LENGTH: Index = CACHE_LINE_SIZE as Index;

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
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn aeron_str_to_rust(raw_str: *const u8) -> String {
    unsafe {
        let length: i32 = *(raw_str as *const i32);
        let ret = String::from(CStr::from_ptr(raw_str.offset(4) as *const i8).to_str().unwrap());

        assert_eq!((length - 1) as usize, ret.len());

        ret
    }
}

// Accepts C-style zero terminated string
pub fn aeron_str_no_len_to_rust(raw_str: *const u8) -> String {
    unsafe { String::from(CStr::from_ptr(raw_str as *const i8).to_str().unwrap()) }
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
