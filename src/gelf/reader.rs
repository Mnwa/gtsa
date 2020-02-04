use serde_json::{Map, Value};
use std::str::FromStr;

pub struct GelfReader {
    data: Map<String, Value>,
}

impl GelfReader {
    pub fn from_slice(buf: &[u8]) -> Result<GelfReader, serde_json::Error> {
        let data: Map<String, Value> = serde_json::from_slice(&buf)?;

        Ok(GelfReader{
            data
        })
    }

    pub fn print(&self) {
        println!("{}", self.to_string());
    }
}

impl ToString for GelfReader {
    fn to_string(&self) -> String {
        serde_json::to_string(&self.data).unwrap()
    }
}

impl FromStr for GelfReader {
    type Err = serde_json::error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let data: Map<String, Value> = serde_json::from_str(s)?;

        Ok(GelfReader{
            data
        })
    }
}
