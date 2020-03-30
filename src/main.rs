use std::env;

mod gelf;
mod sentry;

use crate::gelf::gelf_message_processor::GelfPrinterActor;
use crate::sentry::sentry_processor::SentryProcessorActor;
use gelf::gelf_reader::GelfReaderActor;
use gelf::tcp_acceptor;
use gelf::udp_acceptor;
use gelf::unpacking::UnPackActor;

#[actix_rt::main]
async fn main() {
    let dsn = env::var("SENTRY_DSN")
        .unwrap_or_else(|_e| panic!("You must to pass SENTRY_DSN variable from env"));
    let udp_addr = env::var("UDP_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let tcp_addr = env::var("TCP_ADDR").unwrap_or_else(|_| "0.0.0.0:8081".to_string());

    let reader_threads: usize = env::var("READER_THREADS")
        .unwrap_or_else(|_| "1".to_string())
        .parse()
        .unwrap();
    let unpacker_threads: usize = env::var("UNPACKER_THREADS")
        .unwrap_or_else(|_| "1".to_string())
        .parse()
        .unwrap();
    let max_parallel_chunks: usize = std::env::var("MAX_PARALLEL_CHUNKS")
        .unwrap_or_else(|_| "100000".to_string())
        .parse()
        .unwrap();

    let gelf_reader = GelfReaderActor::new(reader_threads);
    let gelf_unpacker = UnPackActor::new(unpacker_threads);
    let gelf_sentry_processor = SentryProcessorActor::new(&dsn, reader_threads);
    let _gelf_printer = GelfPrinterActor::new();
    actix::spawn(udp_acceptor::new_udp_acceptor(
        udp_addr,
        gelf_sentry_processor.clone(),
        gelf_reader.clone(),
        gelf_unpacker,
        max_parallel_chunks,
    ));
    actix::spawn(tcp_acceptor::new_tcp_acceptor(
        tcp_addr,
        gelf_sentry_processor,
        gelf_reader,
    ));
}
