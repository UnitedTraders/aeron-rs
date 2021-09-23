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

/// Index type is used to express offset and size dimensions of data in buffers.
/// It is i32 because there are many places where "length: Index" is written in to log file where it must be 32 bits long.
/// DON'T USE THIS TYPE INSIDE PACKED STRUCTS AS ITS SIZE MAY CHANGE!!!
pub type Index = i32;

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
