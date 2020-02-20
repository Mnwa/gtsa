use crate::gelf::gelf_reader::GelfReader;
use actix::prelude::*;
use serde_json::Value;

pub struct GelfProcessorMessage(pub GelfReader);
impl Message for GelfProcessorMessage {
    type Result = Option<Value>;
}

pub struct GelfPrinterActor;
impl GelfPrinterActor {
    #[allow(dead_code)]
    pub fn new() -> Addr<GelfPrinterActor> {
        SyncArbiter::start(2, || GelfPrinterActor)
    }
}

impl Actor for GelfPrinterActor {
    type Context = SyncContext<Self>;
}

impl Handler<GelfProcessorMessage> for GelfPrinterActor {
    type Result = Option<Value>;

    fn handle(
        &mut self,
        GelfProcessorMessage(msg): GelfProcessorMessage,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        msg.print();
        None
    }
}
