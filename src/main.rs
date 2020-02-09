mod gelf;

use gelf::udp_acceptor;
use gelf::tcp_acceptor;
use actix::System;
use crate::gelf::gelf_message_processor::GelfPrinterActor;
use crate::gelf::gelf_reader::GelfReaderActor;
use crate::gelf::unpacking::UnPackActor;

fn main() {
    let system = System::new("dada");
    let gelf_reader = GelfReaderActor::new(2);
    let gelf_unpacker = UnPackActor::new(2);
    let gelf_printer = GelfPrinterActor::new();
    actix::spawn(udp_acceptor::new_udp_acceptor(
        "0.0.0.0:8080".to_string(),
        gelf_printer.clone(),
        gelf_reader.clone(),
        gelf_unpacker.clone(),
    ));
    actix::spawn(tcp_acceptor::new_tcp_acceptor(
        "0.0.0.0:8081".to_string(),
        gelf_printer.clone(),
        gelf_reader.clone(),
    ));
    system.run().unwrap();
}