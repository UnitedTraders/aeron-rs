// TODO: Eliminate later
#![allow(dead_code)]
#![feature(core_intrinsics)]
// Please someone give me normal offset_of macro!
#![feature(const_fn)] // Needed for offset_of! macro
#![feature(const_raw_ptr_deref)] // Needed for offset_of! macro
#![feature(const_raw_ptr_to_usize_cast)] // Needed for offset_of! macro

pub mod aeron;
pub mod commands;
pub mod concurrent;
pub mod context;
pub mod utils;
