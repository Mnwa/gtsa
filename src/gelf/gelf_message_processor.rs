use crate::gelf::gelf_reader::GelfDataWrapper;
use crate::sentry::sentry_processor::SentryEvent;
use actix::prelude::*;

/// Message, which contains parsed gelf data
pub struct GelfProcessorMessage(pub GelfDataWrapper);
impl Message for GelfProcessorMessage {
    type Result = Option<SentryEvent>;
}

/// Simplest Gelf Processor Actor
/// Just prints gelf message to stdio
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
    type Result = Option<SentryEvent>;

    fn handle(
        &mut self,
        GelfProcessorMessage(msg): GelfProcessorMessage,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        msg.print();
        None
    }
}
