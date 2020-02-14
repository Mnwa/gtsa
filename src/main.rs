use actix::System;
use std::env;

mod gelf;
mod sentry;

use gelf::udp_acceptor;
use gelf::tcp_acceptor;
use gelf::gelf_reader::GelfReaderActor;
use gelf::unpacking::UnPackActor;
use crate::sentry::sentry_processor::SentryProcessorActor;
use crate::gelf::gelf_message_processor::GelfPrinterActor;

fn main() {
    let dsn = env::var("SENTRY_DSN").unwrap_or_else(|_e| {
        panic!("You must to pass SENTRY_DSN variable from env")
    });
    let udp_addr = env::var("UDP_ADDR").unwrap_or("0.0.0.0:8080".to_owned());
    let tcp_addr = env::var("TCP_ADDR").unwrap_or("0.0.0.0:8081".to_owned());
    let system_name = env::var("SYSTEM").unwrap_or("Gelf Mover".to_owned());

    let reader_threads: usize = env::var("READER_THREADS").unwrap_or("1".to_owned()).parse().unwrap();
    let unpacker_threads: usize = env::var("UNPACKER_THREADS").unwrap_or("1".to_owned()).parse().unwrap();

    let system = System::new(system_name);
    let gelf_reader = GelfReaderActor::new(reader_threads);
    let gelf_unpacker = UnPackActor::new(unpacker_threads);
    let gelf_sentry_processor = SentryProcessorActor::new(&dsn, reader_threads);
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