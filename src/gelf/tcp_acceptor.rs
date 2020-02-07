use actix::prelude::*;
use futures::prelude::*;
use crate::gelf::gelf_reader::{GelfMessage, GelfReaderActor};
use tokio::prelude::*;
use tokio::net::{TcpListener, ToSocketAddrs, TcpStream};

pub async fn new_tcp_acceptor<T>(bind_addr: T) -> Addr<TcpActor>
where T: ToSocketAddrs{
    let listener = TcpListener::bind(bind_addr).await.unwrap();
    TcpActor::new(listener)
}

pub struct TcpActor {
    reader: Addr<GelfReaderActor>
}
impl TcpActor {
    pub fn new(listener: TcpListener) -> Addr<TcpActor> {
        TcpActor::create(|ctx| {
            ctx.add_stream(read_many(listener).map(|socket| {
                TcpPacket(socket)
            }));
            TcpActor{
                reader: GelfReaderActor::new()
            }
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
    fn handle(&mut self, msg: TcpPacket, ctx: &mut Context<Self>) {
        let mut socket = msg.0;
        let reader_actor = self.reader.clone();

        ctx.spawn(async move {
            loop {
                let mut buf = vec![0; 1024];
                let n = match socket.read(&mut buf).await {
                    // socket closed
                    Ok(n) if n == 0 => return,
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("failed to read from socket; err = {:?}", e);
                        return;
                    }
                };

                buf.truncate(n);

                let gelf_msg = GelfMessage{
                    0: buf.clone()
                };

                let reader = match reader_actor.send(gelf_msg).await {
                    Ok(ug) => ug,
                    Err(e) => {
                        eprintln!("gelf actor mailing error: {}", e);
                        return
                    }
                };

                match reader {
                    Ok(reader) => {
                        reader.print();
                    }
                    Err(e) => {
                        eprintln!("tcp parsing gelf error: {}\nOriginal response: {:?}", e, &buf)
                    }
                };
            }
        }.into_actor(self));
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