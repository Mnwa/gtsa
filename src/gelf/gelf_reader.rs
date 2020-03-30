use actix::prelude::*;
use serde::de::{Error, Unexpected};
use serde::{Deserialize, Serialize};
use serde_json::{Error as JsonError, Map, Result as JsonResult, Value};
use std::str::FromStr;

/// Struct, which contains gelf data
pub struct GelfDataWrapper {
    data: GelfData,
}

impl GelfDataWrapper {
    /// Create gelf data wrapper from json slice
    pub fn from_slice(buf: &[u8]) -> JsonResult<GelfDataWrapper> {
        let data: Map<String, Value> = serde_json::from_slice(&buf)?;

        let data = to_gelf(data)?;

        Ok(GelfDataWrapper { data })
    }

    /// print gelf data to stdio
    pub fn print(&self) {
        println!("{}", self.to_string());
    }

    /// Returns a GelfData of this `String`'s contents.
    pub fn into_gelf(self) -> GelfData {
        self.data
    }
}

impl ToString for GelfDataWrapper {
    fn to_string(&self) -> String {
        serde_json::to_string(&self.data).unwrap()
    }
}

pub struct GelfMessage(pub Vec<u8>);

impl Message for GelfMessage {
    type Result = JsonResult<GelfDataWrapper>;
}

pub struct GelfReaderActor;

impl GelfReaderActor {
    pub fn new(threads: usize) -> Addr<GelfReaderActor> {
        SyncArbiter::start(threads, || GelfReaderActor)
    }
}

impl Actor for GelfReaderActor {
    type Context = SyncContext<Self>;
}

impl Handler<GelfMessage> for GelfReaderActor {
    type Result = JsonResult<GelfDataWrapper>;

    fn handle(&mut self, GelfMessage(msg): GelfMessage, _ctx: &mut Self::Context) -> Self::Result {
        GelfDataWrapper::from_slice(msg.as_slice())
    }
}

#[derive(Serialize, Deserialize)]
pub struct GelfData {
    pub host: String,
    pub level: GelfLevel,
    pub short_message: String,
    pub timestamp: f64,
    pub version: String,
    pub meta: Map<String, Value>,
    pub mechanism_data: Map<String, Value>,
}

fn to_gelf(data: Map<String, Value>) -> JsonResult<GelfData> {
    let mut meta = Map::new();
    let mut mechanism_data = Map::new();
    let gelf_fields: [String; 5] = [
        "host".to_string(),
        "level".to_string(),
        "short_message".to_string(),
        "timestamp".to_string(),
        "version".to_string(),
    ];
    data.iter().for_each(|(k, v)| {
        match k.split_at(1) {
            ("_", field) => meta.insert(field.to_string(), v.to_owned()),
            (_, _) if !gelf_fields.contains(k) => mechanism_data.insert(k.to_owned(), v.to_owned()),
            _ => None,
        };
    });

    Ok(GelfData {
        host: data
            .get("host")
            .ok_or_else(|| JsonError::missing_field("host"))?
            .to_string(),
        level: data
            .get("level")
            .ok_or_else(|| JsonError::missing_field("level"))?
            .to_string()
            .parse::<GelfLevel>()?,
        short_message: data
            .get("short_message")
            .ok_or_else(|| JsonError::missing_field("short_message"))?
            .to_string(),
        timestamp: data
            .get("timestamp")
            .ok_or_else(|| JsonError::missing_field("timestamp"))?
            .as_f64()
            .ok_or_else(|| JsonError::invalid_type(Unexpected::Other("timestamp"), &"u8"))?,
        version: data
            .get("version")
            .ok_or_else(|| JsonError::missing_field("version"))?
            .to_string(),
        meta,
        mechanism_data,
    })
}

#[derive(Serialize, Deserialize, Clone)]
pub enum GelfLevel {
    Emergency = 0,
    Alert = 1,
    Critical = 2,
    Error = 3,
    Warning = 4,
    Notice = 5,
    Informational = 6,
    Debug = 7,
}

impl FromStr for GelfLevel {
    type Err = JsonError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "0" => Ok(GelfLevel::Emergency),
            "1" => Ok(GelfLevel::Alert),
            "2" => Ok(GelfLevel::Critical),
            "3" => Ok(GelfLevel::Error),
            "4" => Ok(GelfLevel::Warning),
            "5" => Ok(GelfLevel::Notice),
            "6" => Ok(GelfLevel::Informational),
            "7" => Ok(GelfLevel::Debug),
            _ => Err(JsonError::invalid_value(
                Unexpected::Other("level"),
                &"integers from 0 to 7",
            )),
        }
    }
}
