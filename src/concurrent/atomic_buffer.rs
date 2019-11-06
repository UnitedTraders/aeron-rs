use std::intrinsics::atomic_cxchg;
use std::sync::atomic::{fence, Ordering};

use crate::bit_utils::{alloc_buffer_aligned, dealloc_buffer_aligned};
use crate::commands::AeronCommand::Padding;
use crate::concurrent::Index;

// Buffer allocated on cache-aligned memory boundaries. This struct owns the memory it is pointing to
pub struct AlignedBuffer {
    pub ptr: *mut u8,
    pub len: usize,
}

impl AlignedBuffer {
    pub(crate) fn with_capacity(len: usize) -> AlignedBuffer {
        AlignedBuffer { ptr: alloc_buffer_aligned(len), len }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        dealloc_buffer_aligned(self.ptr, self.len)
    }
}

// Wraps but does not own a region of shared memory
#[derive(Copy, Clone, Debug)]
pub struct AtomicBuffer {
    pub(crate) ptr: *mut u8,
    len: Index,
}

// todo: add bounds checks!!!
// todo: remove unsafe?
impl AtomicBuffer {
    pub fn from_aligned(aligned: &AlignedBuffer) -> AtomicBuffer {
        AtomicBuffer { ptr: aligned.ptr, len: aligned.len as Index }
    }
    //TODO: check that len is ok and ptr is aligned
    pub fn new(ptr: *mut u8, len: Index) -> AtomicBuffer {
        AtomicBuffer { ptr, len }
    }

    // Create a view on the contents of the buffer
    #[inline]
    pub fn view(&self, offset: Index, len: Index) -> Self {
        AtomicBuffer { ptr: unsafe { self.ptr.offset(offset as isize) }, len: self.len - len - offset }
    }

    pub const fn capacity(&self) -> Index { self.len }

    #[inline]
    fn bounds_check(&self, idx: Index) -> () {
        debug_assert!(idx < self.len)
    }

    #[inline]
    pub fn get<T: Copy>(&self, position: Index) -> T {
        unsafe {
            *(self.ptr.offset(position as isize) as *mut T)
        }
    }

    #[inline]
    pub fn set_memory(&self, position: Index, len: usize, value: u8) {
        unsafe {
            // poor man's memcp
            for mut i in ::std::slice::from_raw_parts_mut(self.ptr, len) {
                *i = value
            }
        }
    }

    #[inline]
    pub fn get_volatile<T: Copy>(&self, position: Index) -> T {
        let read = unsafe { *(self.ptr.offset(position as isize) as *mut T) };
        fence(Ordering::Acquire);
        read
    }

    #[inline]
    pub fn put_ordered<T>(&self, position: Index, val: T) {
        unsafe {
            fence(Ordering::Release);
            *(self.ptr.offset(position as isize) as *mut T) = val
        }
    }
    #[inline]
    pub fn put<T>(&self, position: Index, val: T) {
        unsafe {
            *(self.ptr.offset(position as isize) as *mut T) = val
        }
    }


    #[inline]
    pub fn compare_and_set<T>(&self, position: Index, expected: T, update: T) -> bool {
        unsafe {
            let ptr = self.ptr.offset(position as isize) as *mut T;
            let (_, ok) = unsafe { atomic_cxchg(ptr, expected, update) };
            ok
        }
    }

    #[inline]
    pub fn put_bytes(&self, offset: Index, src: &[u8]) {
        unsafe {
            let ptr = self.ptr.offset(offset as isize);
            let mut slice = ::std::slice::from_raw_parts_mut(ptr, src.len() as usize);
            slice.copy_from_slice(src)
        }
    }


    pub fn as_mutable_slice(&self) -> &mut [u8] {
        unsafe {
            ::std::slice::from_raw_parts_mut(self.ptr, self.len as usize)
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::concurrent::atomic_buffer::{AlignedBuffer, AtomicBuffer};
    use crate::concurrent::Index;

    #[test]
    fn that_buffer_can_be_created() {
        let capacity = 1024 << 2;
        let mut data = Vec::with_capacity(capacity);
        let mut buffer = AtomicBuffer::new(data.as_mut_ptr(), capacity as Index);
    }

    #[test]
    fn that_buffer_can_write() {
        let src = AlignedBuffer::with_capacity(1024 << 2);
        let mut buffer = AtomicBuffer::from_aligned(&src);
        let mut to_write = 1;
        buffer.put(0, to_write);
        let read: i32 = buffer.get(0);

        assert_eq!(read, to_write)
    }

    #[test]
    fn that_buffer_preserves_from_aligned() {}
}