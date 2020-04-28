use actix::System;
use std::env;

mod gelf;
mod sentry;

use crate::gelf::gelf_message_processor::GelfPrinterActor;
use crate::sentry::sentry_processor::SentryProcessorActor;
use gelf::gelf_reader::GelfReaderActor;
use gelf::tcp_acceptor;
use gelf::udp_acceptor;
use gelf::unpacking::UnPackActor;
use std::sync::Arc;

fn main() {
    let dsn = env::var("SENTRY_DSN")
        .unwrap_or_else(|_e| panic!("You must to pass SENTRY_DSN variable from env"));
    let udp_addr = env::var("UDP_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let tcp_addr = env::var("TCP_ADDR").unwrap_or_else(|_| "0.0.0.0:8081".to_string());
    let system_name = env::var("SYSTEM").unwrap_or_else(|_| "Gelf Mover".to_string());

    let reader_threads: usize = env::var("READER_THREADS")
        .unwrap_or_else(|_| "1".to_string())
        .parse()
        .unwrap();
    let unpacker_threads: usize = env::var("UNPACKER_THREADS")
        .unwrap_or_else(|_| "1".to_string())
        .parse()
        .unwrap();
    let max_parallel_chunks: usize = std::env::var("MAX_PARALLEL_CHUNKS")
        .unwrap_or_else(|_| "500".to_string())
        .parse()
        .unwrap();

    let system = System::new(system_name);
    let gelf_reader = Arc::new(GelfReaderActor::new(reader_threads));
    let gelf_unpacker = Arc::new(UnPackActor::new(unpacker_threads));
    let gelf_sentry_processor = Arc::new(SentryProcessorActor::new(&dsn, reader_threads));
    let _gelf_printer = GelfPrinterActor::new();
    actix::spawn(udp_acceptor::new_udp_acceptor(
        udp_addr,
        Arc::clone(&gelf_sentry_processor),
        Arc::clone(&gelf_reader),
        gelf_unpacker,
        max_parallel_chunks,
    ));
    actix::spawn(tcp_acceptor::new_tcp_acceptor(
        tcp_addr,
        Arc::clone(&gelf_sentry_processor),
        Arc::clone(&gelf_reader),
    ));
    system.run().unwrap();
}
