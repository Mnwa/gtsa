use actix::prelude::*;
use futures::prelude::*;
use tokio::net::udp::{RecvHalf};
use crate::gelf::unpacking::{UnPackActor, UnpackMessage};
use crate::gelf::gelf_reader::{GelfReaderActor, GelfMessage};
use tokio::net::{ToSocketAddrs, UdpSocket};
use std::net::SocketAddr;
use crate::gelf::gelf_message_printer::GelfProcessorMessage;

pub async fn new_udp_acceptor<T, A>(bind_addr: T, gelf_processor: Addr<A>) -> Addr<UdpActor<A>>
    where
        T: ToSocketAddrs,
        A: Actor<Context = Context<A>>,
        A: Handler<GelfProcessorMessage>,
{
    let socket = UdpSocket::bind(bind_addr).await.unwrap();
    let (recv, _) = socket.split();
    UdpActor::new(recv, gelf_processor)
}

pub struct UdpActor<T>
    where
        T: Actor<Context = Context<T>>,
        T: Handler<GelfProcessorMessage>,
{
    unpacker: Addr<UnPackActor>,
    reader: Addr<GelfReaderActor>,
    gelf_processor: Addr<T>,
}
impl <T>UdpActor<T>
    where
        T: Actor<Context = Context<T>>,
        T: Handler<GelfProcessorMessage>,
{
    pub fn new(recv: RecvHalf, gelf_processor: Addr<T>) -> Addr<UdpActor<T>> {
        UdpActor::create(|ctx| {
            ctx.add_stream(read_many(recv).map(|(buf, addr)| {
                UdpPacket(buf, addr)
            }));
            UdpActor{
                unpacker: UnPackActor::new(),
                reader: GelfReaderActor::new(),
                gelf_processor,
            }
        })
    }
}
impl <T>Actor for UdpActor<T>
    where
        T: Actor<Context = Context<T>>,
        T: Handler<GelfProcessorMessage>,
{
    type Context = Context<Self>;
}

pub struct UdpPacket(Vec<u8>, SocketAddr);
impl Message for UdpPacket {
    type Result = Vec<u8>;
}

impl <T: 'static>StreamHandler<UdpPacket> for UdpActor<T>
    where
        T: Actor<Context = Context<T>>,
        T: Handler<GelfProcessorMessage>,
{
    fn handle(&mut self, msg: UdpPacket, ctx: &mut Context<Self>) {
        let buf = msg.0;
        let packed_buf = UnpackMessage{
            0: buf
        };

        let requested_buf = self.unpacker.send(packed_buf);
        let reader_actor = self.reader.clone();
        let processor_actor = self.gelf_processor.clone();

        ctx.spawn(async move {
            let unpacked_buf = match requested_buf.await {
                Ok(pd) => pd,
                Err(e) => {
                    eprintln!("unpacker actor mailing error: {}", e);
                    return
                }
            };

            let parsed_data = match unpacked_buf {
                Ok(pd) => pd,
                Err(e) => {
                    if e.kind() != std::io::ErrorKind::WriteZero {
                        eprintln!("udp parsing data error: {}", e);
                    }
                    return
                }
            };

            let gelf_msg = GelfMessage{
                0: parsed_data.clone()
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
                    match std::str::from_utf8(&parsed_data) {
                        Ok(s) => eprintln!("udp parsing gelf error: {}\nOriginal response: {:?}", e, s),
                        Err(_e) => eprintln!("udp parsing gelf error: {}\nOriginal response: {:?}", e, &parsed_data),
                    }
                }
            };
        }.into_actor(self));
    }
}

type RecvData = (Vec<u8>, SocketAddr);

fn read_many(recv: RecvHalf) -> impl Stream<Item = RecvData> {
    stream::unfold(recv, |mut recv| {
        async {
            let mut buf = vec![0; 1024];
            let (n, addr) = match recv.recv_from(&mut buf).await {
                Ok((n, addr)) => (n, addr),
                Err(_e) => return None
            };
            buf.truncate(n);
            Some(((buf, addr), recv))
        }
    })
}