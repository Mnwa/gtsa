use crate::gelf::error::GelfError;
use crate::gelf::gelf_message_processor::GelfProcessorMessage;
use crate::gelf::gelf_reader::{GelfMessage, GelfReaderActor};
use actix::dev::ToEnvelope;
use actix::prelude::*;
use futures::prelude::*;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::prelude::io::AsyncReadExt;

pub async fn new_tcp_acceptor<T, A>(
    bind_addr: T,
    gelf_processor: Addr<A>,
    reader: Addr<GelfReaderActor>,
) where
    T: ToSocketAddrs,
    A: Actor + Handler<GelfProcessorMessage>,
    A::Context: ToEnvelope<A, GelfProcessorMessage>,
{
    let listener = TcpListener::bind(bind_addr).await.unwrap();
    TcpActor::new(listener, gelf_processor, reader);
}

pub struct TcpActor<T>
where
    T: Actor + Handler<GelfProcessorMessage>,
    T::Context: ToEnvelope<T, GelfProcessorMessage>,
{
    reader: Addr<GelfReaderActor>,
    gelf_processor: Addr<T>,
}

impl<T> TcpActor<T>
where
    T: Actor + Handler<GelfProcessorMessage>,
    T::Context: ToEnvelope<T, GelfProcessorMessage>,
{
    pub fn new(
        listener: TcpListener,
        gelf_processor: Addr<T>,
        reader: Addr<GelfReaderActor>,
    ) -> Addr<TcpActor<T>> {
        TcpActor::create(|ctx| {
            ctx.add_stream(read_many(listener).map(TcpPacket));
            TcpActor {
                reader,
                gelf_processor,
            }
        })
    }
}

impl<T> Actor for TcpActor<T>
where
    T: Actor + Handler<GelfProcessorMessage>,
    T::Context: ToEnvelope<T, GelfProcessorMessage>,
{
    type Context = Context<Self>;
}

pub struct TcpPacket(TcpStream);

impl Message for TcpPacket {
    type Result = ();
}

impl<T: 'static> StreamHandler<TcpPacket> for TcpActor<T>
where
    T: Actor + Handler<GelfProcessorMessage>,
    T::Context: ToEnvelope<T, GelfProcessorMessage>,
{
    fn handle(&mut self, TcpPacket(mut socket): TcpPacket, ctx: &mut Context<Self>) {
        let reader_actor = self.reader.clone();
        let processor_actor = self.gelf_processor.clone();

        ctx.spawn(
            async move {
                let mut buf = Vec::new();
                loop {
                    let gelf_message = socket
                        .read_to_end(&mut buf)
                        .await
                        .map_err(|e| GelfError::from_err("failed to read from socket:", e))
                        .and_then(|n| match n {
                            0 => Err(GelfError::new("socket closed")),
                            _ => Ok(n),
                        })
                        .map(|n| {
                            buf.truncate(n - 1);
                            GelfMessage(buf.clone())
                        });

                    let gelf_message = match gelf_message {
                        Ok(m) => m,
                        Err(e) => {
                            eprintln!("{}", e);
                            return;
                        }
                    };

                    let reader = reader_actor
                        .send(gelf_message)
                        .await
                        .map_err(|e| GelfError::from_err("gelf actor mailing error:", e))
                        .and_then(|reader| {
                            reader.map_err(|e| {
                                println!("Original response: {}", String::from_utf8_lossy(&buf));
                                GelfError::from_err("tcp parsing gelf error:", e)
                            })
                        })
                        .map(GelfProcessorMessage);

                    let gelf_processor_message = match reader {
                        Ok(r) => r,
                        Err(e) => {
                            eprintln!("{}", e);
                            return;
                        }
                    };

                    if let Err(e) = processor_actor.send(gelf_processor_message).await {
                        eprintln!("{}", GelfError::from_err("gelf actor processing error:", e));
                        return;
                    }

                    buf.clear()
                }
            }
            .into_actor(self),
        );
    }
}

fn read_many(listener: TcpListener) -> impl Stream<Item = TcpStream> {
    stream::unfold(listener, |mut listener| async {
        match listener.accept().await {
            Ok((socket, _)) => Some((socket, listener)),
            Err(_e) => None,
        }
    })
}
