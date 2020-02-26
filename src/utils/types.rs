/*
 * Copyright 2020 UT OVERSEAS INC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Index type is used to express offset and size dimensions of data in buffers.
// Originally it was i32 but it'c more handy (less type casts) to have it as isize.
// We don't use usize as there are places where special "inverted" values with
// minus sign added are used.
// Also there is at least one place where Index is cased down to i32 to be written
// in to pack(4) structure.
pub type Index = isize;

// These are types USED inside "raw bytes packed" buffers and thus their size can't be changed
pub type Moment = u64;
pub const MAX_MOMENT: Moment = std::u64::MAX;

const INDEX_MAX_USIZE: usize = ::std::isize::MAX as usize;

pub fn clamp_to_index(sz: usize) -> Index {
    sz.min(INDEX_MAX_USIZE) as Index
}

// Define commonly used sizeoffs to shorten main code. i32 type is most commonly used in calculations
// with sizeof.
pub const I32_SIZE: Index = std::mem::size_of::<i32>() as Index;
pub const U32_SIZE: Index = std::mem::size_of::<u32>() as Index;
pub const I64_SIZE: Index = std::mem::size_of::<i64>() as Index;
pub const U64_SIZE: Index = std::mem::size_of::<u64>() as Index;

#[macro_export]
macro_rules! offset_of {
    ($Struct:path, $field:ident) => {{
        // Using a separate function to minimize unhygienic hazards
        // (e.g. unsafety of #[repr(packed)] field borrows).
        fn offset() -> usize {
            let u = core::mem::MaybeUninit::<$Struct>::uninit();
            // Use pattern-matching to avoid accidentally going through Deref.
            unsafe {
                let &$Struct { $field: ref f, .. } = &*u.as_ptr();
                (f as *const _ as usize).wrapping_sub(&u as *const _ as usize)
            }
        }
        offset() as Index
    }};
}

#[cfg(test)]
mod tests {
    use crate::utils::types::Index;

    #[repr(C, packed(4))]
    struct Foo {
        a: u8,
        b: i64,
        c: [u8; 3],
        d: i64,
    }

    #[test]
    fn offset_simple() {
        assert_eq!(offset_of!(Foo, a), 0);
        assert_eq!(offset_of!(Foo, b), 4);
        assert_eq!(offset_of!(Foo, c), 12);
        assert_eq!(offset_of!(Foo, d), 16);
    }
}
