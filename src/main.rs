use actix::System;

mod gelf;
mod sentry;

use gelf::udp_acceptor;
use gelf::tcp_acceptor;
use gelf::gelf_reader::GelfReaderActor;
use gelf::unpacking::UnPackActor;
use crate::sentry::sentry_processor::SentryProcessorActor;
use crate::gelf::gelf_message_processor::GelfPrinterActor;

fn main() {
    let system = System::new("dada");
    let gelf_reader = GelfReaderActor::new(2);
    let gelf_unpacker = UnPackActor::new(2);
    let gelf_sentry_processor = SentryProcessorActor::new("https://883f3e4fc2f1461bbfaeb6dcb30bde85@sentry.io/2381673");
    let _gelf_printer = GelfPrinterActor::new();
    actix::spawn(udp_acceptor::new_udp_acceptor(
        "0.0.0.0:8080".to_string(),
        gelf_sentry_processor.clone(),
        gelf_reader.clone(),
        gelf_unpacker.clone(),
    ));
    actix::spawn(tcp_acceptor::new_tcp_acceptor(
        "0.0.0.0:8081".to_string(),
        gelf_sentry_processor.clone(),
        gelf_reader.clone(),
    ));
    system.run().unwrap();
}