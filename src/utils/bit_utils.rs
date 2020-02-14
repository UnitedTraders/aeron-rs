use cache_line_size::CACHE_LINE_SIZE;
use std::alloc::{alloc_zeroed, dealloc, Layout};

use crate::utils::types::Index;

pub const CACHE_LINE_LENGTH: Index = CACHE_LINE_SIZE as Index;

pub fn align(value: Index, alignment: Index) -> Index {
    (value + (alignment - 1)) & !(alignment - 1)
}

pub fn is_power_of_two(value: i32) -> bool {
    value > 0 && ((value & (!value + 1)) == value)
}

#[allow(dead_code)]
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

// Returns number of trailing bits which are set to 0
pub fn number_of_trailing_zeroes(value: i32) -> i32 {
    let table = [
        0, 1, 2, 24, 3, 19, 6, 25, 22, 4, 20, 10, 16, 7, 12, 26, 31, 23, 18, 5, 21, 9, 15, 11, 30, 17, 8, 14, 29, 13, 28, 27,
    ];

    if value == 0 {
        return 32;
    }

    // Use i64 inside the calculation to handle numbers close to i32::MAX without multiplication overflow
    let index = ((value as i64 & -value as i64) * 0x04D7651F) as u32;

    table[(index >> 27) as usize]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_number_of_trailing_zeroes() {
        assert_eq!(number_of_trailing_zeroes(0), 32);
        assert_eq!(number_of_trailing_zeroes(1), 0);
        assert_eq!(number_of_trailing_zeroes(2), 1);
        assert_eq!(number_of_trailing_zeroes(3), 0);
        assert_eq!(number_of_trailing_zeroes(4), 2);
        assert_eq!(number_of_trailing_zeroes(5), 0);
        assert_eq!(number_of_trailing_zeroes(6), 1);
        assert_eq!(number_of_trailing_zeroes(7), 0);
        assert_eq!(number_of_trailing_zeroes(512), 9);
        assert_eq!(number_of_trailing_zeroes(513), 0);
        assert_eq!(number_of_trailing_zeroes(1073741824), 30);
        assert_eq!(number_of_trailing_zeroes(1073741825), 0);
    }
}
