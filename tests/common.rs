use std::time::Duration;
use std::process::{Command, Child};
use std::ffi::CString;

pub const TEST_CHANNEL: &str = "aeron:udp?endpoint=localhost:50000";
pub const TEST_STREAM_ID: i32 = 2000;

pub fn str_to_c(val: &str) -> CString {
    CString::new(val).expect("Error converting str to CString")
}

// This function starts Aeron media driver. The driver needs to be compiled prior the testing from
// official C distribution.
// It should be present in PATH env variable.
pub fn start_aeron_md() -> Child {
    let ret = Command::new("aeronmd")
        .env("AERON_DIR_DELETE_ON_SHUTDOWN", "1")
        .spawn()
        .expect("aeronmd failed to start");

    // Let some time for driver to start
    std::thread::sleep(Duration::from_millis(2000));

    ret
}

// Stop the driver at test end
pub fn stop_aeron_md(driver_proc: Child) {
    let pid = format!("{}", driver_proc.id()); // get UNIX pid

    println!("Killing aeronmd with pid {}", pid);

    Command::new("kill")
        .args(&["-2", &pid])  // Send SIGINT
        .spawn()
        .expect("problem while sending SIGINT to aeronmd");

    // Let some time for driver to shutdown prior startup in the next test
    std::thread::sleep(Duration::from_millis(1000));
}

