use actix::prelude::*;
use futures::prelude::*;
use tokio::net::udp::{RecvHalf};
use crate::gelf::unpacking::{UnPackActor, UnpackMessage};
use crate::gelf::gelf_reader::{GelfReaderActor, GelfMessage};
use tokio::net::{ToSocketAddrs, UdpSocket};
use std::net::SocketAddr;

pub async fn new_udp_acceptor<T>(bind_addr: T) -> Addr<UdpActor>
where T: ToSocketAddrs{
    let socket = UdpSocket::bind(bind_addr).await.unwrap();
    let (recv, _) = socket.split();
    UdpActor::new(recv)
}

pub struct UdpActor {
    unpacker: Addr<UnPackActor>,
    reader: Addr<GelfReaderActor>
}
impl UdpActor {
    pub fn new(recv: RecvHalf) -> Addr<UdpActor> {
        UdpActor::create(|ctx| {
            ctx.add_stream(read_many(recv).map(|(buf, addr)| {
                UdpPacket(buf, addr)
            }));
            UdpActor{
                unpacker: UnPackActor::new(),
                reader: GelfReaderActor::new(),
            }
        })
    }
}
impl Actor for UdpActor {
    type Context = Context<Self>;
}

pub struct UdpPacket(Vec<u8>, SocketAddr);
impl Message for UdpPacket {
    type Result = Vec<u8>;
}

impl StreamHandler<UdpPacket> for UdpActor {
    fn handle(&mut self, msg: UdpPacket, ctx: &mut Context<Self>) {
        let buf = msg.0;
        let packed_buf = UnpackMessage{
            0: buf
        };

        let requested_buf = self.unpacker.send(packed_buf);
        let reader_actor = self.reader.clone();

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
                    reader.print();
                }
                Err(e) => {
                    eprintln!("udp parsing gelf error: {}\nOriginal response: {:?}", e, &parsed_data)
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