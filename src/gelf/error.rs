use std::error::Error;
use std::fmt::{Display, Formatter, Result};

/// Gelf error struct, which used to e-prints when something wrong.
#[derive(Debug)]
pub struct GelfError {
    message: String,
}

impl GelfError {
    pub fn new(message: &str) -> Self {
        GelfError {
            message: message.to_string(),
        }
    }
    pub fn from_err<T: Error>(message: &str, e: T) -> Self {
        GelfError {
            message: format!("{}: {:?}", message, e),
        }
    }
}

impl Display for GelfError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.message)
    }
}

impl Error for GelfError {}
