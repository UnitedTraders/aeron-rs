use std::ffi::{CStr, CString};
use std::fmt::{Debug, Error, Formatter};
use std::slice;
use std::sync::atomic::{fence, AtomicI32, AtomicI64, Ordering};

use crate::utils::bit_utils::{alloc_buffer_aligned, dealloc_buffer_aligned};
use crate::utils::types::{Index, I32_SIZE, I64_SIZE};

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
#[derive(Copy, Clone)]
pub struct AtomicBuffer {
    pub(crate) ptr: *mut u8,
    len: Index,
}

impl Debug for AtomicBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let mut slice = self.as_slice();
        const TAKE_LIMIT: usize = 4;
        loop {
            let (head, tail) = slice.split_at(TAKE_LIMIT);
            if tail.len() > TAKE_LIMIT {
                writeln!(f, "{:?}", head)?;
                slice = tail;
            } else {
                write!(f, "{:?}", tail)?;
                break;
            }
        }
        Ok(())
    }
}

// todo: add bounds checks!!!
// todo: remove unsafe?
impl AtomicBuffer {
    #[cfg(test)]
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

    pub fn wrap_slice(slice: &mut [u8]) -> Self {
        AtomicBuffer {
            ptr: slice.as_mut_ptr(),
            len: slice.len() as isize,
        }
    }

    //TODO: check that len is ok and ptr is aligned
    pub fn new(ptr: *mut u8, len: Index) -> AtomicBuffer {
        AtomicBuffer { ptr, len }
    }

    #[inline]
    unsafe fn at(&self, offset: Index) -> *mut u8 {
        self.ptr.offset(offset as isize)
    }

    // Create a view on the contents of the buffer starting from offset and spanning len bytes.
    // Sets length of the "view" buffer to "len"
    #[inline]
    pub fn view(&self, offset: Index, len: Index) -> Self {
        self.bounds_check(offset, len);

        AtomicBuffer {
            ptr: unsafe { self.at(offset) },
            len,
        }
    }

    pub const fn capacity(&self) -> Index {
        self.len
    }

    #[inline]
    pub fn bounds_check(&self, idx: Index, len: isize) {
        assert!((idx + len as Index) <= self.len)
    }

    #[inline]
    pub fn get<T: Copy>(&self, position: Index) -> T {
        self.bounds_check(position, std::mem::size_of::<T>() as isize);
        unsafe { *(self.at(position) as *mut T) }
    }

    #[inline]
    pub fn overlay_struct<T>(&self, position: Index) -> *mut T {
        self.bounds_check(position, std::mem::size_of::<T>() as isize);
        unsafe { self.at(position) as *mut T }
    }

    #[inline]
    pub fn as_ref<T: Copy>(&self, position: Index) -> &T {
        self.bounds_check(position, std::mem::size_of::<T>() as isize);
        unsafe { &*(self.at(position) as *const T) }
    }

    #[inline]
    pub fn buffer(&self) -> *mut u8 {
        self.ptr
    }

    #[inline]
    pub fn set_memory(&self, position: Index, len: Index, value: u8) {
        self.bounds_check(position, len);
        let s = unsafe { slice::from_raw_parts_mut(self.ptr, len as usize) };

        // poor man's memcp
        for i in s {
            *i = value
        }
    }

    #[inline]
    pub fn get_volatile<T: Copy>(&self, position: Index) -> T {
        self.bounds_check(position, std::mem::size_of::<T>() as isize);
        let read = self.get(position);
        fence(Ordering::Acquire);
        read
    }

    #[inline]
    pub fn put_ordered<T>(&self, position: Index, val: T) {
        self.bounds_check(position, std::mem::size_of::<T>() as isize);
        fence(Ordering::Release);
        self.put(position, val);
    }

    #[inline]
    pub fn put<T>(&self, position: Index, val: T) {
        self.bounds_check(position, std::mem::size_of::<T>() as isize);
        unsafe { *(self.at(position) as *mut T) = val }
    }

    #[inline]
    pub fn compare_and_set_i32(&self, position: Index, expected: i32, update: i32) -> bool {
        self.bounds_check(position, I32_SIZE);
        unsafe {
            let ptr = self.at(position) as *const AtomicI32;
            (*ptr)
                .compare_exchange(expected, update, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        }
    }

    #[inline]
    pub fn compare_and_set_i64(&self, position: Index, expected: i64, update: i64) -> bool {
        self.bounds_check(position, I64_SIZE);
        unsafe {
            let ptr = self.at(position) as *const AtomicI64;
            (*ptr)
                .compare_exchange(expected, update, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        }
    }

    // Put bytes in to this buffer at specified offset
    #[inline]
    pub fn put_bytes(&self, offset: Index, src: &[u8]) {
        self.bounds_check(offset, src.len() as isize);

        let slice = unsafe {
            let ptr = self.at(offset);
            slice::from_raw_parts_mut(ptr, src.len() as usize)
        };
        slice.copy_from_slice(src)
    }

    // Copy "length" bytes from "src_buffer" starting from "src_offset" in to this buffer at given "offset"
    // offset - offset in current (self) buffer to start coping from
    // src_buffer - atomic buffer to copy data from
    // src_offset - offset in src_buffer to start coping from
    // length - number of bytes to copy
    #[inline]
    pub fn copy_from(&self, offset: Index, src_buffer: &AtomicBuffer, src_offset: Index, length: Index) {
        self.bounds_check(offset, length);
        src_buffer.bounds_check(src_offset, length);
        unsafe {
            let src_ptr = src_buffer.at(src_offset);
            let dest_ptr = self.at(offset);
            // TODO: check that memory regions are actually not overlapping, otherwise UB!
            std::ptr::copy_nonoverlapping(src_ptr, dest_ptr, length as usize);
        }
    }

    pub fn as_mutable_slice(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr, self.len as usize) }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr, self.len as usize) }
    }

    pub fn as_sub_slice(&self, index: Index, len: Index) -> &[u8] {
        self.bounds_check(index, len as isize);
        // self.view(index, len).as_slice()
        unsafe { slice::from_raw_parts(self.at(index), len as usize) }
    }

    #[inline]
    pub fn get_string(&self, offset: Index) -> CString {
        self.bounds_check(offset, 4);

        // String in Aeron has first 4 bytes as length and rest "length" bytes is string body
        let length: i32 = self.get::<i32>(offset);
        self.get_string_without_length(offset + I32_SIZE, length as isize)
    }

    #[inline]
    pub fn get_string_without_length(&self, offset: Index, length: isize) -> CString {
        self.bounds_check(offset, length);

        // Strings in Aeron are zero terminated and are not UTF-8 encoded.
        // We can't go with Rust UTF strings as Media Driver will not understand us.
        let c_str = unsafe {
            let ptr = self.at(offset) as *const i8;
            CStr::from_ptr(ptr)
        };

        CString::from(c_str)
    }

    #[inline]
    pub fn get_string_length(&self, offset: Index) -> Index {
        self.bounds_check(offset, 4);

        self.get::<i32>(offset) as Index
    }

    #[inline]
    pub fn put_string(&self, offset: Index, string: &[u8]) {
        self.bounds_check(offset, string.len() as isize + I32_SIZE);

        // String in Aeron has first 4 bytes as length and rest "length" bytes is string body
        self.put::<i32>(offset, string.len() as i32);
        self.put_bytes(offset + I32_SIZE, string);
    }

    /**
     * Multi threaded increment.
     *
     * @param offset in the buffer of the word.
     * @param delta  for to be applied to the value.
     * @return the value before applying the delta.
     */
    pub fn get_and_add_i64(&self, offset: Index, delta: i64) -> i64 {
        self.bounds_check(offset, I64_SIZE as isize);
        unsafe {
            let atomic_ptr = self.at(offset) as *const AtomicI64;
            (*atomic_ptr).fetch_add(delta, Ordering::SeqCst)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::concurrent::atomic_buffer::{AlignedBuffer, AtomicBuffer};
    use crate::utils::types::Index;

    #[test]
    fn atomic_buffer_can_be_created() {
        let capacity = 1024 << 2;
        let mut data = Vec::with_capacity(capacity);
        let _buffer = AtomicBuffer::new(data.as_mut_ptr(), capacity as Index);
    }

    #[test]
    fn atomic_buffer_aligned_buffer_create() {
        let src = AlignedBuffer::with_capacity(16);
        let atomic_buffer = AtomicBuffer::from_aligned(&src);

        //assert zeroed
        assert_eq!(atomic_buffer.as_slice(), &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,])
    }

    #[test]
    fn atomic_buffer_write_read() {
        let src = AlignedBuffer::with_capacity(1024 << 2);
        let buffer = AtomicBuffer::from_aligned(&src);
        let to_write = 1;
        buffer.put(0, to_write);
        let read: i32 = buffer.get(0);

        assert_eq!(read, to_write)
    }

    #[test]
    fn atomic_buffer_preserves_from_aligned() {
        let buffer = AlignedBuffer::with_capacity(8);
        let _atomic_buffer = AtomicBuffer::from_aligned(&buffer);
        // TODO: assert_eq!(atomic_buffer.as_slice(), buffer.)
    }

    #[test]
    fn atomic_buffer_put_bytes() {
        let mut data: Vec<u8> = (0u8..=7).map(|x| x).collect();
        assert_eq!(data.len(), 8);

        let buffer = AtomicBuffer::new(data.as_mut_ptr(), 8);
        buffer.put_bytes(4, &[0, 1, 2, 3]);

        assert_eq!(buffer.as_slice(), &[0, 1, 2, 3, 0, 1, 2, 3])
    }

    #[test]
    fn atomic_buffer_get_as_slice() {
        let mut data: Vec<u8> = (0u8..=7).map(|x| x).collect();
        assert_eq!(data.len(), 8);

        let buffer = AtomicBuffer::new(data.as_mut_ptr(), 8);
        let sub_slice = buffer.as_slice();

        assert_eq!(sub_slice, &[0, 1, 2, 3, 4, 5, 6, 7])
    }

    #[test]
    fn atomic_buffer_get_as_mut_slice() {
        let mut data: Vec<u8> = (0u8..=7).map(|x| x).collect();
        assert_eq!(data.len(), 8);

        let mut buffer = AtomicBuffer::new(data.as_mut_ptr(), 8);
        let sub_slice = buffer.as_mutable_slice();

        assert_eq!(sub_slice, &[0, 1, 2, 3, 4, 5, 6, 7])
    }

    #[test]
    fn atomic_buffer_get_sub_slice() {
        let mut data: Vec<u8> = (0u8..=7).map(|x| x).collect();
        assert_eq!(data.len(), 8);

        let buffer = AtomicBuffer::new(data.as_mut_ptr(), 8);
        let sub_slice = buffer.as_sub_slice(3, 2);

        assert_eq!(sub_slice, &[3, 4])
    }

    #[test]
    #[should_panic]
    fn atomic_buffer_get_sub_slice_out_of_bounds() {
        let mut data: Vec<u8> = (0u8..=7).map(|x| x).collect();
        assert_eq!(data.len(), 8);

        let x = AtomicBuffer::new(data.as_mut_ptr(), 8);
        let _sub_slice = x.as_sub_slice(7, 2);
    }

    #[test]
    fn atomic_buffer_put_and_get_string() {
        let src = AlignedBuffer::with_capacity(16);
        let atomic_buffer = AtomicBuffer::from_aligned(&src);

        let test_string = [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]; // zero terminated C-style

        atomic_buffer.put_string(2, &test_string);
        let read_str = atomic_buffer.get_string(2);

        assert_eq!(read_str.as_bytes().len(), 9);
        assert_eq!(read_str.as_bytes_with_nul(), test_string);
    }
}
