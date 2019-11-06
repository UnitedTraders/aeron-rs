pub mod atomics;
pub mod atomic_buffer;
pub mod log_buffer;
pub mod ring_buffer;

pub type Index = i32;

const INDEX_MAX_USIZE: usize = ::std::i32::MAX as usize;

pub fn clamp_to_index(sz: usize) -> Index {
    sz.min(INDEX_MAX_USIZE) as Index
}