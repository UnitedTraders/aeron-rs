pub mod bit_utils;
pub mod errors;
pub mod log_buffers;
pub mod memory_mapped_file;
pub mod misc;
pub mod rate_reporter;
pub mod types;

#[macro_export]
macro_rules! ttrace {
        ($log_message:expr) => {
            if let Some(name) = std::thread::current().name() {
                log::trace!("({}) {}", name, $log_message);
            } else {
                log::trace!(concat!("(NoName) ", $log_message));
            }
        };
        ($log_message:expr, $($args:tt)*) => {
            if let Some(name) = std::thread::current().name() {
                log::trace!(concat!("({}) ", $log_message), name, $($args)*);
            } else {
                log::trace!(concat!("(NoName) ", $log_message), $($args)*);
            }
        };
    }
