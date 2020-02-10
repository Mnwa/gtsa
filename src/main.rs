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
    let dsn = std::option_env!("SENTRY_DSN").unwrap();
    let udp_addr = std::option_env!("UDP_ADDR").unwrap_or("0.0.0.0:8080");
    let tcp_addr = std::option_env!("TCP_ADDR").unwrap_or("0.0.0.0:8081");
    let system_name = std::option_env!("SYSTEM").unwrap_or("Gelf Mover");

    let reader_threads: usize = std::option_env!("READER_THREADS").unwrap_or("1").parse().unwrap();
    let unpacker_threads: usize = std::option_env!("UNPACKER_THREADS").unwrap_or("1").parse().unwrap();

    let system = System::new(system_name);
    let gelf_reader = GelfReaderActor::new(reader_threads);
    let gelf_unpacker = UnPackActor::new(unpacker_threads);
    let gelf_sentry_processor = SentryProcessorActor::new(dsn);
    let _gelf_printer = GelfPrinterActor::new();
    actix::spawn(udp_acceptor::new_udp_acceptor(
        udp_addr.to_string(),
        gelf_sentry_processor.clone(),
        gelf_reader.clone(),
        gelf_unpacker.clone(),
    ));
    actix::spawn(tcp_acceptor::new_tcp_acceptor(
        tcp_addr.to_string(),
        gelf_sentry_processor.clone(),
        gelf_reader.clone(),
    ));
    system.run().unwrap();
}