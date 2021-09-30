use std::{
    ffi::{CStr, CString},
    fmt::{Debug, Error, Formatter},
    io::Write,
    slice,
    sync::atomic::{fence, AtomicI32, AtomicI64, Ordering},
};

use crate::utils::{
    misc::{alloc_buffer_aligned, dealloc_buffer_aligned},
    types::{Index, I32_SIZE, I64_SIZE},
};

/// Buffer allocated on cache-aligned memory boundaries. This struct owns the memory it is pointing to
pub struct AlignedBuffer {
    pub ptr: *mut u8,
    pub len: Index,
}

impl AlignedBuffer {
    pub fn with_capacity(len: Index) -> AlignedBuffer {
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

/// Wraps but does not own a region of shared memory
#[derive(Copy, Clone)]
pub struct AtomicBuffer {
    pub(crate) ptr: *mut u8,
    len: Index,
}

impl Debug for AtomicBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let mut slice = self.as_slice();
        const TAKE_LIMIT: usize = 40;
        let mut bytes_counter = 0;
        loop {
            write!(f, "{}: ", bytes_counter)?;
            bytes_counter += TAKE_LIMIT;

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

impl Write for AtomicBuffer {
    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        self.put_bytes(0, buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

// Where needed AtomicBuffer is accessed through mem fences or atomic operations.
// Seems its safe to share AtomicBuffer between threads.
unsafe impl Send for AtomicBuffer {}
unsafe impl Sync for AtomicBuffer {}

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

    pub fn wrap_slice(slice: &mut [u8]) -> Self {
        AtomicBuffer {
            ptr: slice.as_mut_ptr(),
            len: slice.len() as Index,
        }
    }

    //TODO: check that len is ok and ptr is aligned
    pub(crate) fn new(ptr: *mut u8, len: Index) -> AtomicBuffer {
        AtomicBuffer { ptr, len }
    }

    #[inline]
    unsafe fn at(&self, offset: Index) -> *mut u8 {
        self.ptr.offset(offset as isize)
    }

    /// Create a view on the contents of the buffer starting from offset and spanning len bytes.
    /// Sets length of the "view" buffer to "len"
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
    pub fn bounds_check(&self, idx: Index, len: Index) {
        assert!((idx + len as Index) <= self.len)
    }

    #[inline]
    pub fn get<T: Copy>(&self, position: Index) -> T {
        self.bounds_check(position, std::mem::size_of::<T>() as Index);
        unsafe { *(self.at(position) as *mut T) }
    }

    #[inline]
    pub fn overlay_struct<T>(&self, position: Index) -> *mut T {
        self.bounds_check(position, std::mem::size_of::<T>() as Index);
        unsafe { self.at(position) as *mut T }
    }

    #[inline]
    pub fn as_ref<T: Copy>(&self, position: Index) -> &T {
        self.bounds_check(position, std::mem::size_of::<T>() as Index);
        unsafe { &*(self.at(position) as *const T) }
    }

    #[inline]
    pub fn buffer(&self) -> *mut u8 {
        self.ptr
    }

    #[inline]
    pub fn set_memory(&self, position: Index, len: Index, value: u8) {
        self.bounds_check(position, len);
        let s = unsafe { slice::from_raw_parts_mut(self.ptr.offset(position as isize), len as usize) };

        // poor man's memcp
        for i in s {
            *i = value
        }
    }

    #[inline]
    pub fn get_volatile<T: Copy>(&self, position: Index) -> T {
        self.bounds_check(position, std::mem::size_of::<T>() as Index);
        let read = self.get(position);
        fence(Ordering::Acquire);
        read
    }

    #[inline]
    pub fn put_ordered<T>(&self, position: Index, val: T) {
        self.bounds_check(position, std::mem::size_of::<T>() as Index);
        fence(Ordering::Release);
        self.put(position, val);
    }

    #[inline]
    pub fn put<T>(&self, position: Index, val: T) {
        self.bounds_check(position, std::mem::size_of::<T>() as Index);
        unsafe { *(self.at(position) as *mut T) = val }
    }

    #[inline]
    #[allow(clippy::cast_ptr_alignment)]
    pub fn put_atomic_i64(&self, offset: Index, val: i64) {
        self.bounds_check(offset, I64_SIZE);
        unsafe {
            let atomic_ptr = self.at(offset) as *const AtomicI64;
            (*atomic_ptr).store(val, Ordering::SeqCst);
        }
    }

    #[inline]
    #[allow(clippy::cast_ptr_alignment)]
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
    #[allow(clippy::cast_ptr_alignment)]
    pub fn compare_and_set_i64(&self, position: Index, expected: i64, update: i64) -> bool {
        self.bounds_check(position, I64_SIZE);
        unsafe {
            let ptr = self.at(position) as *const AtomicI64;
            (*ptr)
                .compare_exchange(expected, update, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        }
    }

    /**
     * Single threaded increment with release semantics.
     *
     * @param offset in the buffer of the word.
     * @param delta  for to be applied to the value.
     */
    pub fn add_i64_ordered(&self, offset: Index, delta: i64) {
        self.bounds_check(offset, I64_SIZE);

        let value = self.get::<i64>(offset);
        self.put_ordered::<i64>(offset, value + delta);
    }

    /// Put bytes in to this buffer at specified offset
    #[inline]
    pub fn put_bytes(&self, offset: Index, src: &[u8]) {
        self.bounds_check(offset, src.len() as Index);

        unsafe {
            let ptr = self.ptr.offset(offset as isize);
            ::std::ptr::copy(src.as_ptr(), ptr, src.len() as usize);
        }
    }

    #[inline]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn get_bytes(&self, offset: Index, dest: *mut u8, length: Index) {
        self.bounds_check(offset, length);

        unsafe {
            let ptr = self.at(offset);
            ::std::ptr::copy(ptr, dest, length as usize);
        }
    }

    /// Copy "length" bytes from "src_buffer" starting from "src_offset" in to this buffer at given "offset"
    /// offset - offset in current (self) buffer to start coping from
    /// src_buffer - atomic buffer to copy data from
    /// src_offset - offset in src_buffer to start coping from
    /// length - number of bytes to copy
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
        self.bounds_check(index, len);
        unsafe { slice::from_raw_parts(self.at(index), len as usize) }
    }

    #[inline]
    pub fn get_string(&self, offset: Index) -> CString {
        self.bounds_check(offset, 4);

        // String in Aeron has first 4 bytes as length and rest "length" bytes is string body in ASCII
        let length: i32 = self.get::<i32>(offset);
        self.get_string_without_length(offset + I32_SIZE, length)
    }

    #[inline]
    pub fn get_string_without_length(&self, offset: Index, length: Index) -> CString {
        self.bounds_check(offset, length);

        unsafe {
            // NOTE: we need to add trailing zero after the "length" bytes read from the buffer
            let str_slice = std::slice::from_raw_parts(self.at(offset) as *const u8, length as usize);
            let mut zero_terminated: Vec<u8> = Vec::with_capacity(length as usize + 1);
            zero_terminated.extend_from_slice(str_slice);
            zero_terminated.push(0);

            CString::from(CStr::from_bytes_with_nul_unchecked(&zero_terminated))
        }
    }

    #[inline]
    pub fn get_string_length(&self, offset: Index) -> Index {
        self.bounds_check(offset, 4);

        self.get::<i32>(offset) as Index
    }

    /// This function expects ASCII string WITHOUT trailing zero as its input.
    #[inline]
    pub fn put_string(&self, offset: Index, string: &[u8]) {
        self.bounds_check(offset, string.len() as Index + I32_SIZE);

        // String in Aeron has first 4 bytes as length and rest "length" bytes is string body
        self.put::<i32>(offset, string.len() as i32);

        self.put_bytes(offset + I32_SIZE, string);
    }

    #[inline]
    pub fn put_string_without_length(&self, offset: Index, string: &[u8]) -> Index {
        self.bounds_check(offset, string.len() as Index);

        self.put_bytes(offset + I32_SIZE, string);

        string.len() as Index
    }

    /**
     * Multi threaded increment.
     *
     * @param offset in the buffer of the word.
     * @param delta  for to be applied to the value.
     * @return the value before applying the delta.
     */
    #[allow(clippy::cast_ptr_alignment)]
    pub fn get_and_add_i64(&self, offset: Index, delta: i64) -> i64 {
        self.bounds_check(offset, I64_SIZE);
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
    use std::io::Write;

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
        let mut data: Vec<u8> = (0u8..=7).collect();
        assert_eq!(data.len(), 8);

        let buffer = AtomicBuffer::new(data.as_mut_ptr(), 8);

        buffer.put_bytes(4, &[0, 1, 2, 3]);

        assert_eq!(buffer.as_slice(), &[0, 1, 2, 3, 0, 1, 2, 3])
    }

    #[test]
    fn atomic_buffer_put_bytes_with_write_trait() {
        let mut data: Vec<u8> = (0u8..=7).collect();
        assert_eq!(data.len(), 8);

        let mut buffer = AtomicBuffer::new(data.as_mut_ptr(), 8);

        buffer.write_all(&[4, 5, 6, 7]).unwrap();

        assert_eq!(buffer.as_slice(), &[4, 5, 6, 7, 4, 5, 6, 7]);
    }

    #[test]
    fn atomic_buffer_get_as_slice() {
        let mut data: Vec<u8> = (0u8..=7).collect();
        assert_eq!(data.len(), 8);

        let buffer = AtomicBuffer::new(data.as_mut_ptr(), 8);
        let sub_slice = buffer.as_slice();

        assert_eq!(sub_slice, &[0, 1, 2, 3, 4, 5, 6, 7])
    }

    #[test]
    fn atomic_buffer_get_as_mut_slice() {
        let mut data: Vec<u8> = (0u8..=7).collect();
        assert_eq!(data.len(), 8);

        let mut buffer = AtomicBuffer::new(data.as_mut_ptr(), 8);
        let sub_slice = buffer.as_mutable_slice();

        assert_eq!(sub_slice, &[0, 1, 2, 3, 4, 5, 6, 7])
    }

    #[test]
    fn atomic_buffer_get_sub_slice() {
        let mut data: Vec<u8> = (0u8..=7).collect();
        assert_eq!(data.len(), 8);

        let buffer = AtomicBuffer::new(data.as_mut_ptr(), 8);
        let sub_slice = buffer.as_sub_slice(3, 2);

        assert_eq!(sub_slice, &[3, 4])
    }

    #[test]
    #[should_panic]
    fn atomic_buffer_get_sub_slice_out_of_bounds() {
        let mut data: Vec<u8> = (0u8..=7).collect();
        assert_eq!(data.len(), 8);

        let x = AtomicBuffer::new(data.as_mut_ptr(), 8);
        let _sub_slice = x.as_sub_slice(7, 2);
    }

    #[test]
    fn atomic_buffer_put_and_get_string() {
        let src = AlignedBuffer::with_capacity(16);
        let atomic_buffer = AtomicBuffer::from_aligned(&src);

        let test_string = [1, 2, 3, 4, 5, 6, 7, 8, 9]; // without trailing zero

        atomic_buffer.put_string(2, &test_string);
        let read_str = atomic_buffer.get_string(2); // trailing zero added here while reading from AB

        assert_eq!(read_str.as_bytes().len(), 9);
        assert_eq!(read_str.as_bytes(), test_string); // as_bytes() returns string body without trailing zero
    }
}
