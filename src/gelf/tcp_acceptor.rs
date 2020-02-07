use actix::prelude::*;
use futures::prelude::*;
use crate::gelf::gelf_reader::{GelfMessage, GelfReaderActor};
use tokio::prelude::*;
use tokio::net::{TcpListener, ToSocketAddrs, TcpStream};
use crate::gelf::gelf_message_printer::GelfProcessorMessage;

pub async fn new_tcp_acceptor<T, A>(bind_addr: T, gelf_processor: Addr<A>) -> Addr<TcpActor<A>>
    where
        T: ToSocketAddrs,
        A: Actor<Context = Context<A>>,
        A: Handler<GelfProcessorMessage>,
{
    let listener = TcpListener::bind(bind_addr).await.unwrap();
    TcpActor::new(listener, gelf_processor)
}

pub struct TcpActor<T>
    where
        T: Actor<Context = Context<T>>,
        T: Handler<GelfProcessorMessage>,
{
    reader: Addr<GelfReaderActor>,
    gelf_processor: Addr<T>,
}

impl<T> TcpActor<T>
    where
        T: Actor<Context = Context<T>>,
        T: Handler<GelfProcessorMessage>,
{
    pub fn new(listener: TcpListener, gelf_processor: Addr<T>) -> Addr<TcpActor<T>> {
        TcpActor::create(|ctx| {
            ctx.add_stream(read_many(listener).map(|socket| {
                TcpPacket(socket)
            }));
            TcpActor {
                reader: GelfReaderActor::new(),
                gelf_processor,
            }
        })
    }
}

impl<T> Actor for TcpActor<T>
    where
        T: Actor<Context = Context<T>>,
        T: Handler<GelfProcessorMessage>,
{
    type Context = Context<Self>;
}

pub struct TcpPacket(TcpStream);

impl Message for TcpPacket {
    type Result = ();
}

impl<T: 'static> StreamHandler<TcpPacket> for TcpActor<T>
    where
        T: Actor<Context = Context<T>>,
        T: Handler<GelfProcessorMessage>,
{
    fn handle(&mut self, msg: TcpPacket, ctx: &mut Context<Self>) {
        let mut socket = msg.0;
        let reader_actor = self.reader.clone();
        let processor_actor = self.gelf_processor.clone();

        ctx.spawn(async move {
            let mut buf = Vec::new();
            loop {
                match socket.read_to_end(&mut buf).await {
                    // socket closed
                    Ok(n) if n == 0 => return,
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("failed to read from socket; err = {:?}", e);
                        return;
                    }
                };

                let buf_last_i = buf.len() - 1;
                if buf[buf_last_i] == 0 {
                    buf.truncate(buf_last_i);

                    let gelf_msg = GelfMessage {
                        0: buf.clone()
                    };

                    let reader = match reader_actor.send(gelf_msg).await {
                        Ok(ug) => ug,
                        Err(e) => {
                            eprintln!("gelf actor mailing error: {}", e);
                            return;
                        }
                    };

                    match reader {
                        Ok(reader) => {
                            let printer_message = GelfProcessorMessage {
                                0: reader
                            };
                            match processor_actor.send(printer_message).await {
                                Ok(ug) => ug,
                                Err(e) => {
                                    eprintln!("gelf actor processing error: {}", e);
                                    return;
                                }
                            };
                        }
                        Err(e) => {
                            match std::str::from_utf8(&buf) {
                                Ok(s) => eprintln!("tcp parsing gelf error: {}\nOriginal response: {:?}", e, s),
                                Err(_e) => eprintln!("tcp parsing gelf error: {}\nOriginal response: {:?}", e, &buf),
                            }
                        }
                    };

                    buf.clear()
                }
            }
        }.into_actor(self));
    }
}

fn read_many(listener: TcpListener) -> impl Stream<Item=TcpStream> {
    stream::unfold(listener, |mut listener| {
        async {
            match listener.accept().await {
                Ok((socket, _)) => Some((socket, listener)),
                Err(_e) => None
            }
        }
    })
}