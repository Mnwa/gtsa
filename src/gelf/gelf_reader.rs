use actix::prelude::*;
use serde_json::{Map, Value, Result as JsonResult};

pub struct GelfReader {
    data: Map<String, Value>,
}

impl GelfReader {
    pub fn from_slice(buf: &[u8]) -> JsonResult<GelfReader> {
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
