use actix::prelude::*;
use futures::prelude::*;
use tokio::net::udp::{RecvHalf};
use crate::gelf::unpacking::{UnPackActor, UnpackMessage};
use crate::gelf::reader::GelfReader;
use tokio::net::{ToSocketAddrs, UdpSocket};
use std::net::SocketAddr;

pub async fn new_udp_acceptor<T>(bind_addr: T) -> Addr<UdpActor>
where T: ToSocketAddrs{
    let socket = UdpSocket::bind(bind_addr).await.unwrap();
    let (recv, _) = socket.split();
    UdpActor::new(recv)
}

pub struct UdpActor {
    unpacker: Addr<UnPackActor>
}
impl UdpActor {
    pub fn new(recv: RecvHalf) -> Addr<UdpActor> {
        UdpActor::create(|ctx| {
            ctx.add_stream(read_many(recv).map(|(buf, addr)| {
                UdpPacket(buf, addr)
            }));
            UdpActor{
                unpacker: UnPackActor::new()
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
    fn handle(&mut self, msg: UdpPacket, _ctx: &mut Context<Self>) {
        let buf = msg.0;
        let packed_buf = UnpackMessage{
            0: buf
        };

        let requested_buf = self.unpacker.send(packed_buf);

        actix::spawn(async move {
            let unpacked_buf = match requested_buf.await {
                Ok(pd) => pd,
                Err(e) => {
                    eprintln!("actor mailing error: {}", e);
                    return
                }
            };

            let parsed_data = match unpacked_buf {
                Ok(pd) => pd,
                Err(e) => {
                    eprintln!("udp parsing data error: {}", e);
                    return
                }
            };

            let reader = GelfReader::from_slice(&parsed_data);

            match reader {
                Ok(reader) => {
                    reader.print();
                }
                Err(e) => {
                    eprintln!("udp parsing gelf error: {}\nOriginal response: {:?}", e, &parsed_data)
                }
            };
        });
    }
}

type RecvData = (Vec<u8>, SocketAddr);

fn read_many(recv: RecvHalf) -> impl Stream<Item = RecvData> {
    stream::unfold(recv, |mut recv| {
        async {
            let mut buf = [0; 1024];
            match recv.recv_from(&mut buf).await {
                Ok((n, addr)) => Some(((Vec::from(&buf[0..n]), addr), recv)),
                Err(_e) => None
            }
        }
    })
}