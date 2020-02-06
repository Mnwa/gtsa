use actix::prelude::*;
use futures::prelude::*;
use crate::gelf::reader::GelfReader;
use tokio::prelude::*;
use tokio::net::{TcpListener, ToSocketAddrs, TcpStream};
use tokio::net::tcp::Incoming;

pub async fn new_tcp_acceptor<T>(bind_addr: T) -> Addr<TcpActor>
where T: ToSocketAddrs{
    let mut listener = TcpListener::bind(bind_addr).await.unwrap();
    TcpActor::new(listener)
}

pub struct TcpActor;
impl TcpActor {
    pub fn new(listener: TcpListener) -> Addr<TcpActor> {
        TcpActor::create(|ctx| {
            ctx.add_stream(read_many(listener).map(|socket| {
                TcpPacket(socket)
            }));
            TcpActor
        })
    }
}
impl Actor for TcpActor {
    type Context = Context<Self>;
}

pub struct TcpPacket(TcpStream);
impl Message for TcpPacket {
    type Result = ();
}

impl StreamHandler<TcpPacket> for TcpActor {
    fn handle(&mut self, msg: TcpPacket, _ctx: &mut Context<Self>) {
        let mut socket = msg.0;

        actix::spawn(async move {
            loop {
                let mut buf = [0; 1024];
                let n = match socket.read(&mut buf).await {
                    // socket closed
                    Ok(n) if n == 0 => return,
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("failed to read from socket; err = {:?}", e);
                        return;
                    }
                };

                let reader = GelfReader::from_slice(&buf[0..n-1]);
                match reader {
                    Ok(reader) => {
                        reader.print();
                    }
                    Err(e) => {
                        eprintln!("udp parsing gelf error: {}\nOriginal response: {} {:?}", e, n, &buf[0..n])
                    }
                }
            }
        });
    }
}

fn read_many(listener: TcpListener) -> impl Stream<Item = TcpStream> {
    stream::unfold(listener, |mut listener| {
        async {
            match listener.accept().await {
                Ok((socket, _)) => Some((socket, listener)),
                Err(_e) => None
            }
        }
    })
}