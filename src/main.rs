#![feature(core_intrinsics)]

extern crate cache_line_size;

use std::path::PathBuf;
use std::sync::atomic;

mod bit_utils;
mod concurrent;
mod commands;

pub struct Context {}

pub struct Aeron {
// random source
// random engine
// ctx
// cncBuffer

// atomic buffers (not backed by file??)
//toDriverAtomicBuffer
//toClientAtomicBuffer
//countersMetadataBuffer
//countersValueBuffer
}

pub fn main() {}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicPtr, Ordering};

    #[test]
    fn atomic_ptrs() {
        let mut vec = vec![0u8; 16];
        let mut ptr = vec.as_mut_ptr();
        let position = 1isize;
        let atomic_ptr = AtomicPtr::new(unsafe { ptr.offset(position) as *mut i32 });
        let mut val: i32 = 42;
        unsafe {
            atomic_ptr.store(&mut val, Ordering::Release)
        }

        let read = unsafe {
            // uncomment the following line. Should point to the same mem location
            // let atomic_ptr=AtomicPtr::new(unsafe{ptr.offset(position) as *mut i32});
            *atomic_ptr.load(Ordering::Acquire)
        };

        assert_eq!(read, 42);
    }

    #[test]
    fn it_works() {
        let len = 10;
        let mut vec = vec![0u8; len];
        let mut ptr = vec.as_mut_ptr();
        let position = 0isize;
        let mut val = 42;

        // store an int32 at index 1
        unsafe {
            std::sync::atomic::fence(Ordering::Release);
            *(ptr.offset(position) as *mut i32) = val
        };

        let read = unsafe {
            ::std::sync::atomic::fence(Ordering::Acquire);
            *(ptr as *mut i32)
        };
        assert_eq!(read, 42, "read");
    }
}
