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

// These are types not used inside "raw bytes packed" buffers and thus their definitions
// and size could be changed
pub type Index = i32; // As in C++ and Java implementations

// These are types USED inside "raw bytes packed" buffers and thus their size can't be changed
pub type Moment = u64;
pub const MAX_MOMENT: Moment = std::u64::MAX;

const INDEX_MAX_USIZE: usize = ::std::isize::MAX as usize;

pub fn clamp_to_index(sz: usize) -> Index {
    sz.min(INDEX_MAX_USIZE) as Index
}

// Define commonly used sizeoffs to shorten main code. i32 type is most commonly used in calculations
// with sizeof.
pub const SZ_I32: usize = std::mem::size_of::<i32>();
pub const SZ_U32: usize = std::mem::size_of::<u32>();
pub const SZ_I64: usize = std::mem::size_of::<i64>();
pub const SZ_U64: usize = std::mem::size_of::<u64>();

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
        offset() as i32
    }};
}

#[cfg(test)]
mod tests {
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
