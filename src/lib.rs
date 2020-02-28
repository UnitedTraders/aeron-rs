// TODO: Eliminate later
#![allow(dead_code)]
#![allow(clippy::cast_ptr_alignment)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::missing_safety_doc)]

pub mod aeron;
pub mod command;
pub mod concurrent;
pub mod context;
pub mod driver_listener_adapter;
//pub mod driver_proxy;
pub mod buffer_builder;
pub mod cnc_file_descriptor;
pub mod protocol;
pub mod utils;
