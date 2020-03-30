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

#[cfg(test)]
mod processor {
    use super::*;

    #[actix_rt::test]
    async fn test_actor() {
        let gelf_printer = GelfPrinterActor::new();

        let r = gelf_printer
            .send(GelfProcessorMessage(
                GelfDataWrapper::from_slice(
                    br#"{
                        "version":"1.1",
                        "host":"example.org",
                        "short_message":"A short message",
                        "level":5,
                        "_some_info":"foo",
                        "timestamp":1582213226
                    }"#,
                )
                .unwrap(),
            ))
            .await
            .unwrap()
            .is_none();

        assert!(r);
    }
}
