use actix::prelude::*;
use crate::gelf::gelf_reader::{GelfReader};

pub struct GelfProcessorMessage(pub GelfReader);
impl Message for GelfProcessorMessage {
    type Result = ();
}

pub struct GelfPrinterActor;
impl GelfPrinterActor {
    pub fn new() -> Addr<GelfPrinterActor> {
        GelfPrinterActor.start()
    }
}

impl Actor for GelfPrinterActor {
    type Context = Context<Self>;
}

impl Handler<GelfProcessorMessage> for GelfPrinterActor {
    type Result = ();

    fn handle(&mut self, msg: GelfProcessorMessage, _ctx: &mut Self::Context) -> Self::Result {
        msg.0.print()
    }
}