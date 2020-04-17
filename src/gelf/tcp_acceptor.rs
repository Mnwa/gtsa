use crate::gelf::error::GelfError;
use crate::gelf::gelf_message_processor::GelfProcessorMessage;
use crate::gelf::gelf_reader::{GelfMessage, GelfReaderActor};
use actix::dev::ToEnvelope;
use actix::prelude::*;
use futures::prelude::*;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::prelude::io::AsyncReadExt;

pub async fn new_tcp_acceptor<T, A>(
    bind_addr: T,
    gelf_processor: Arc<Addr<A>>,
    reader: Arc<Addr<GelfReaderActor>>,
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
    reader: Arc<Addr<GelfReaderActor>>,
    gelf_processor: Arc<Addr<T>>,
}

impl<T> TcpActor<T>
where
    T: Actor + Handler<GelfProcessorMessage>,
    T::Context: ToEnvelope<T, GelfProcessorMessage>,
{
    pub fn new(
        listener: TcpListener,
        gelf_processor: Arc<Addr<T>>,
        reader: Arc<Addr<GelfReaderActor>>,
    ) -> Addr<TcpActor<T>> {
        TcpActor::create(|ctx| {
            ctx.add_stream(read_tcp(listener));
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
        let reader_actor = Arc::clone(&self.reader);
        let processor_actor = Arc::clone(&self.gelf_processor);

        ctx.spawn(
            async move {
                loop {
                    let mut buf = Vec::with_capacity(8196);
                    let n = socket
                        .read_to_end(&mut buf)
                        .await
                        .map_err(|e| GelfError::from_err("failed to read from socket", e))
                        .and_then(|n| match n {
                            0 => Err(GelfError::new("socket closed")),
                            _ => Ok(n),
                        });

                    let gelf_message = match n {
                        Ok(n) => {
                            buf.truncate(n - 1);
                            GelfMessage(buf)
                        }
                        Err(e) => {
                            eprintln!("{}", e);
                            return;
                        }
                    };

                    let reader = reader_actor
                        .send(gelf_message)
                        .await
                        .map_err(|e| GelfError::from_err("gelf actor mailing error", e))
                        .and_then(|reader| {
                            reader.map_err(|e| GelfError::from_err("tcp parsing gelf error", e))
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
                        eprintln!("{}", GelfError::from_err("gelf actor processing error", e));
                        return;
                    }
                }
            }
            .into_actor(self),
        );
    }
}

fn read_tcp(listener: TcpListener) -> impl Stream<Item = TcpPacket> {
    stream::unfold(listener, |mut listener| async {
        match listener.accept().await {
            Ok((socket, _)) => Some((socket, listener)),
            Err(e) => panic!("tcp handling panic: {:?}", e),
        }
    })
    .map(TcpPacket)
}
