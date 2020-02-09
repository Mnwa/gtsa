use actix::prelude::*;
use serde_json::{Map, Value, Result as JsonResult};
use serde::{Deserialize, Serialize};
use serde::de::Error;

pub struct GelfReader {
    data: GelfData,
}

impl GelfReader {
    pub fn from_slice(buf: &[u8]) -> JsonResult<GelfReader> {
        let data: Map<String, Value> = serde_json::from_slice(&buf)?;

        let data = match to_gelf(data) {
            Some(data) => data,
            None => return Err(serde_json::Error::missing_field("one of the gelf data struct"))
        };

        Ok(GelfReader {
            data
        })
    }

    pub fn print(&self) {
        println!("{}", self.to_string());
    }

    pub fn as_gelf(&self) -> &GelfData {
        &self.data
    }
}

impl ToString for GelfReader {
    fn to_string(&self) -> String {
        serde_json::to_string(&self.data).unwrap()
    }
}

pub struct GelfMessage(pub Vec<u8>);

impl Message for GelfMessage {
    type Result = JsonResult<GelfReader>;
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
    type Result = JsonResult<GelfReader>;

    fn handle(&mut self, msg: GelfMessage, _ctx: &mut Self::Context) -> Self::Result {
        GelfReader::from_slice(msg.0.as_slice())
    }
}

#[derive(Serialize, Deserialize)]
pub struct GelfData {
    pub host: String,
    pub level: u8,
    pub short_message: String,
    pub timestamp: f64,
    pub version: String,
    pub meta: Map<String, Value>,
}

fn to_gelf(data: Map<String, Value>) -> Option<GelfData> {
    let mut meta = Map::new();
    data
        .iter()
        .for_each(|(k, v)| {
            let str_k = k.as_str();
            if &str_k[..1] == "_" {
                meta.insert(
                    str_k[1..].to_owned(),
                    v.to_owned()
                );
            }
        });

    Some(GelfData {
        host: data.get("host")?.to_string(),
        level: data.get("level")?.as_u64()? as u8,
        short_message: data.get("short_message")?.to_string(),
        timestamp: data.get("timestamp")?.as_f64()?,
        version: data.get("version")?.to_string(),
        meta,
    })
}