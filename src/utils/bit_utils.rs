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

use crate::utils::types::Index;

pub fn align(value: Index, alignment: Index) -> Index {
    (value + (alignment - 1)) & !(alignment - 1)
}

pub fn is_power_of_two(value: Index) -> bool {
    value > 0 && ((value & (!value + 1)) == value)
}

/// Returns number of trailing bits which are set to 0
pub fn number_of_trailing_zeroes(value: i32) -> i32 {
    let table = [
        0, 1, 2, 24, 3, 19, 6, 25, 22, 4, 20, 10, 16, 7, 12, 26, 31, 23, 18, 5, 21, 9, 15, 11, 30, 17, 8, 14, 29, 13, 28, 27,
    ];

    if value == 0 {
        return 32;
    }

    // Use i64 inside the calculation to handle numbers close to i32::MAX without multiplication overflow
    let index = ((value as i64 & -value as i64) * 0x04D7_651F) as u32;

    table[(index >> 27) as usize]
}

pub fn find_next_power_of_two_i64(mut value: i64) -> i64 {
    value -= 1;

    // Set all bits below the leading one using binary expansion
    // http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf
    let mut i: i64 = 1;

    loop {
        if i >= std::mem::size_of::<i64>() as i64 * 8 {
            break;
        }
        value |= value >> i;
        i *= 2;
    }

    value + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_number_of_trailing_zeroes() {
        assert_eq!(number_of_trailing_zeroes(0), 32);
        assert_eq!(number_of_trailing_zeroes(1), 0);
        assert_eq!(number_of_trailing_zeroes(2), 1);
        assert_eq!(number_of_trailing_zeroes(3), 0);
        assert_eq!(number_of_trailing_zeroes(4), 2);
        assert_eq!(number_of_trailing_zeroes(5), 0);
        assert_eq!(number_of_trailing_zeroes(6), 1);
        assert_eq!(number_of_trailing_zeroes(7), 0);
        assert_eq!(number_of_trailing_zeroes(512), 9);
        assert_eq!(number_of_trailing_zeroes(513), 0);
        assert_eq!(number_of_trailing_zeroes(1_073_741_824), 30);
        assert_eq!(number_of_trailing_zeroes(1_073_741_825), 0);
    }

    #[test]
    fn test_next_power_of_two() {
        assert_eq!(find_next_power_of_two_i64(0), 0);
        assert_eq!(find_next_power_of_two_i64(1), 1);
        assert_eq!(find_next_power_of_two_i64(2), 2);
        assert_eq!(find_next_power_of_two_i64(3), 4);
        assert_eq!(find_next_power_of_two_i64(4), 4);
        assert_eq!(find_next_power_of_two_i64(5), 8);
        assert_eq!(find_next_power_of_two_i64(6), 8);
        assert_eq!(find_next_power_of_two_i64(7), 8);
        assert_eq!(find_next_power_of_two_i64(8), 8);
        assert_eq!(find_next_power_of_two_i64(9), 16);
        assert_eq!(find_next_power_of_two_i64(10), 16);
        assert_eq!(find_next_power_of_two_i64(17), 32);
        assert_eq!(find_next_power_of_two_i64(12345), 16384);
    }
}
