pub type Index = isize;

/** Length of the data blocks used by the CPU cache sub-system in bytes. */
pub const CACHE_LINE_LENGTH: i32 = 64;

const INDEX_MAX_USIZE: usize = ::std::isize::MAX as usize;

pub fn clamp_to_index(sz: usize) -> Index {
    sz.min(INDEX_MAX_USIZE) as Index
}

#[macro_export]
macro_rules! offset_of {
    ($Struct:path, $field:ident) => ({
        // Using a separate function to minimize unhygienic hazards
        // (e.g. unsafety of #[repr(packed)] field borrows).
        fn offset() -> usize {
            let u = core::mem::MaybeUninit::<$Struct>::uninit();
            // Use pattern-matching to avoid accidentally going through Deref.
            let &$Struct { $field: ref f, .. } = unsafe { &*u.as_ptr() };
            (f as *const _ as usize).wrapping_sub(&u as *const _ as usize)
        }
        offset()
    })
}