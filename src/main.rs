mod gelf;

use gelf::udp_acceptor;
use gelf::tcp_acceptor;
use actix::System;
use crate::gelf::gelf_message_printer::GelfPrinterActor;

fn main() {
    let system = System::new("dada");
    actix::spawn(async move {
        udp_acceptor::new_udp_acceptor(
            "0.0.0.0:8080".to_string(),
            GelfPrinterActor::new(),
        ).await;
    });
    actix::spawn(async move {
        tcp_acceptor::new_tcp_acceptor(
            "0.0.0.0:8081".to_string(),
            GelfPrinterActor::new(),
        ).await;
    });
    system.run().unwrap();
}