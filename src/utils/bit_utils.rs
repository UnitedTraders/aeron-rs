use cache_line_size::CACHE_LINE_SIZE;
use std::alloc::{alloc_zeroed, dealloc, Layout};

use crate::utils::types::Index;

pub fn align(value: Index, alignment: Index) -> Index {
    return (value + (alignment - 1)) & !(alignment - 1);
}

/// Allocate a buffer aligned on the cache size
pub fn alloc_buffer_aligned(size: usize) -> *mut u8 {
    unsafe {
        let layout = Layout::from_size_align_unchecked(size, CACHE_LINE_SIZE);
        alloc_zeroed(layout)
    }
}

/// Deallocate a buffer aligned on a cache size
pub fn dealloc_buffer_aligned(buff_ptr: *mut u8, len: usize) {
    unsafe {
        if cfg!(debug_assertions) {
            // dealloc markers for debug
            for i in 0..len as isize {
                *buff_ptr.offset(i) = 0xff;
            }
        }

        let layout = Layout::from_size_align_unchecked(len, CACHE_LINE_SIZE);
        dealloc(buff_ptr, layout)
    }
}
