use tokio::net::{UdpSocket, TcpListener, ToSocketAddrs};
use tokio::prelude::*;
use async_trait::async_trait;
use actix::prelude::*;

use crate::gelf::reader::GelfReader;
use crate::gelf::unpacking::{UnPackActor, UnpackMessage};

pub enum AcceptorType {
    UDP,
    TCP
}

#[async_trait]
pub trait Acceptor {
    async fn serve(&mut self);
    async fn listen(&mut self);
}

pub async fn create_acceptor<T>(bind_addr: T, acceptor_type: AcceptorType) -> Box<dyn Acceptor>
where T: ToSocketAddrs {
    match acceptor_type {
        AcceptorType::UDP => {
            let socket = UdpSocket::bind(bind_addr).await.unwrap();
            let unpack_actor = UnPackActor::new();

            Box::new(UdpAcceptor{
                socket,
                unpack_actor,
            })
        },
        AcceptorType::TCP => {
            let listener = TcpListener::bind(bind_addr).await.unwrap();

            Box::new(TcpAcceptor{
                listener
            })
        }
    }
}

struct UdpAcceptor {
    socket: UdpSocket,
    unpack_actor: Addr<UnPackActor>
}

#[async_trait]
impl Acceptor for UdpAcceptor {
    async fn serve(&mut self) {
        loop {
            self.listen().await
        }
    }

    async fn listen(&mut self) {
        let mut buf = [0; 1024];

        let n = match self.socket.recv_from(&mut buf).await {
            Ok((n, _addr)) => n,
            Err(e) => {
                eprintln!("failed to read from socket; err = {:?}", e);
                return
            }
        };

        let packed_buf = UnpackMessage{
            0: Vec::from(&buf[0..n])
        };

        let unpacked_buf = match self.unpack_actor.send(packed_buf).await {
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

        tokio::spawn(async move {
            let reader = GelfReader::from_slice(&parsed_data);

            match reader {
                Ok(reader) => {
                    reader.print();
                }
                Err(e) => {
                    eprintln!("udp parsing gelf error: {}\nOriginal response: {} {:?}", e, n, &buf[0..n])
                }
            }
        });
    }
}

struct TcpAcceptor {
    listener: TcpListener
}

#[async_trait]
impl Acceptor for TcpAcceptor {
    async fn serve(&mut self) {
        loop {
            self.listen().await
        }
    }

    async fn listen(&mut self) {
        let (mut socket, _) = self.listener.accept().await.unwrap();
        let mut buf = [0; 1024];

        tokio::spawn(async move {

            // In a loop, read data from the socket and write the data back.
            loop {
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

                // Write the data back
                if let Err(e) = socket.write_all(&buf[0..n]).await {
                    eprintln!("failed to write to socket; err = {:?}", e);
                    return;
                }
            }
        });
    }
}