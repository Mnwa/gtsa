mod gelf;

use gelf::tcp_acceptor;
use actix::System;

fn main() {
    let system = System::new("dada");
    actix::spawn(async move {
        tcp_acceptor::new_tcp_acceptor(
            "0.0.0.0:8080".to_string()
        ).await;
    });
    system.run().unwrap();
}