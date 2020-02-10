// Everything about error codes and messages

use std::fmt::Display;
use std::fmt;

pub enum AeronError {
    IllegalArgumentException(String),
}

impl Display for AeronError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AeronError::IllegalArgumentException(msg) => {
                write!(f, "Illegal argument: {}", msg)
            }
        }
    }
}