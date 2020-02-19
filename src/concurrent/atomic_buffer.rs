use std::ffi::{CStr, CString};
use std::sync::atomic::{fence, AtomicI32, AtomicI64, Ordering};

use crate::utils::bit_utils::{alloc_buffer_aligned, dealloc_buffer_aligned};
use crate::utils::types::{Index, SZ_I32, SZ_I64};

// Buffer allocated on cache-aligned memory boundaries. This struct owns the memory it is pointing to
pub struct AlignedBuffer {
    pub ptr: *mut u8,
    pub len: Index,
}

impl AlignedBuffer {
    pub(crate) fn with_capacity(len: Index) -> AlignedBuffer {
        AlignedBuffer {
            ptr: alloc_buffer_aligned(len),
            len,
        }
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
        AtomicBuffer {
            ptr: aligned.ptr,
            len: aligned.len as Index,
        }
    }

    pub fn wrap(buffer: AtomicBuffer) -> Self {
        AtomicBuffer {
            ptr: buffer.ptr,
            len: buffer.len as Index,
        }
    }

    pub fn wrap_slice( slice: &mut [u8]) -> Self {
        AtomicBuffer {
            ptr: slice.as_mut_ptr(),
            len: slice.len() as isize,
        }
    }

    //TODO: check that len is ok and ptr is aligned
    pub fn new(ptr: *mut u8, len: Index) -> AtomicBuffer {
        AtomicBuffer { ptr, len }
    }

    // Create a view on the contents of the buffer starting from offset and spanning len bytes.
    // Sets length of the "view" buffer to "len"
    #[inline]
    pub fn view(&self, offset: Index, len: Index) -> Self {
        self.bounds_check(offset, len);

        AtomicBuffer {
            ptr: unsafe { self.ptr.offset(offset as isize) },
            len,
        }
    }

    pub const fn capacity(&self) -> Index {
        self.len
    }

    #[inline]
    pub fn bounds_check(&self, idx: Index, len: isize) {
        debug_assert!((idx + len as Index) < self.len)
    }

    #[inline]
    pub fn get<T: Copy>(&self, position: Index) -> T {
        unsafe { *(self.ptr.offset(position as isize) as *mut T) }
    }

    #[inline]
    pub fn as_slice<T: Copy>(&self, position: Index) -> &T {
        unsafe { &*(self.ptr.offset(position as isize) as *const T) }
    }

    #[inline]
    pub fn buffer(&self) -> *mut u8 {
        self.ptr
    }

    #[inline]
    pub fn set_memory(&self, _position: Index, len: Index, value: u8) {
        unsafe {
            // poor man's memcp
            for i in ::std::slice::from_raw_parts_mut(self.ptr, len as usize) {
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
        unsafe { *(self.ptr.offset(position as isize) as *mut T) = val }
    }

    #[inline]
    pub fn compare_and_set_i32(&self, position: Index, expected: i32, update: i32) -> bool {
        unsafe {
            let ptr = self.ptr.offset(position as isize) as *const AtomicI32;
            (&*ptr)
                .compare_exchange(expected, update, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        }
    }

    #[inline]
    pub fn compare_and_set_i64(&self, position: Index, expected: i64, update: i64) -> bool {
        unsafe {
            let ptr = self.ptr.offset(position as isize) as *const AtomicI64;
            (&*ptr)
                .compare_exchange(expected, update, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        }
    }

    // Put bytes in to this buffer at specified offset
    #[inline]
    pub fn put_bytes(&self, offset: Index, src: &[u8]) {
        unsafe {
            let ptr = self.ptr.offset(offset as isize);
            let slice = ::std::slice::from_raw_parts_mut(ptr, src.len() as usize);
            slice.copy_from_slice(src)
        }
    }

    // Copy "length" bytes from "src_buffer" starting from "src_offset" in to this buffer at given "offset"
    // offset - offset in current (self) buffer to start coping from
    // src_buffer - atomic buffer to copy data from
    // src_offset - offset in src_buffer to start coping from
    // length - number of bytes to copy
    #[inline]
    pub fn copy_from(&self, offset: Index, src_buffer: &AtomicBuffer, src_offset: Index, length: Index) {
        unsafe {
            let src_ptr = src_buffer.ptr.offset(src_offset as isize);
            let dest_ptr = self.ptr.offset(offset as isize);
            // TODO: check that memory regions are actually not overlapping, otherwise UB!
            std::ptr::copy_nonoverlapping(src_ptr, dest_ptr, length as usize);
        }
    }

    pub fn as_mutable_slice(&self) -> &mut [u8] {
        unsafe { ::std::slice::from_raw_parts_mut(self.ptr, self.len as usize) }
    }

    #[inline]
    pub fn get_string(&self, offset: Index) -> CString {
        self.bounds_check(offset, 4);

        // String in Aeron has first 4 bytes as length and rest "length" bytes is string body
        let length: i32 = self.get::<i32>(offset);
        self.get_string_without_length(offset + SZ_I32, length as isize)
    }

    #[inline]
    pub fn get_string_without_length(&self, offset: Index, length: isize) -> CString {
        self.bounds_check(offset, length);

        // Strings in Aeron are zero terminated and are not UTF-8 encoded.
        // We can't go with Rust UTF strings as Media Driver will not understand us.
        let ptr = unsafe { *(self.ptr.offset(offset as isize) as *const &[u8]) };
        CString::from(CStr::from_bytes_with_nul(ptr).expect("Error converting bytes in to CStr"))
    }

    #[inline]
    pub fn get_string_length(&self, offset: Index) -> Index {
        self.bounds_check(offset, 4);

        self.get::<i32>(offset) as Index
    }

    /**
     * Multi threaded increment.
     *
     * @param offset in the buffer of the word.
     * @param delta  for to be applied to the value.
     * @return the value before applying the delta.
     */
    pub fn get_and_add_i64(&self, offset: Index, delta: i64) -> i64 {
        self.bounds_check(offset, SZ_I64 as isize);
        unsafe {
            let atomic_ptr = self.ptr.offset(offset as isize) as *const AtomicI64;
            (&*atomic_ptr).fetch_add(delta, Ordering::SeqCst)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::concurrent::atomic_buffer::{AlignedBuffer, AtomicBuffer};
    use crate::utils::types::Index;

    #[test]
    fn that_buffer_can_be_created() {
        let capacity = 1024 << 2;
        let mut data = Vec::with_capacity(capacity);
        let _buffer = AtomicBuffer::new(data.as_mut_ptr(), capacity as Index);
    }

    #[test]
    fn that_buffer_can_write() {
        let src = AlignedBuffer::with_capacity(1024 << 2);
        let buffer = AtomicBuffer::from_aligned(&src);
        let to_write = 1;
        buffer.put(0, to_write);
        let read: i32 = buffer.get(0);

        assert_eq!(read, to_write)
    }

    #[test]
    fn that_buffer_preserves_from_aligned() {}
}
