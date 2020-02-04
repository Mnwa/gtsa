use tokio::net::{UdpSocket, TcpListener, ToSocketAddrs};
use tokio::prelude::*;
use async_trait::async_trait;
use flate2::read::{ZlibDecoder, GzDecoder};

use crate::gelf::reader::GelfReader;
use std::io::prelude::*;
use std::str::FromStr;
use std::collections::HashMap;
use std::hash::Hash;

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
            let mut chunked_messages: HashMap<String, Vec<MessageChunk>> = HashMap::new();

            Box::new(UdpAcceptor{
                socket,
                chunked_messages
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

#[derive(Debug)]
struct MessageChunk {
    message_id: String,
    sequence_number: u8,
    sequence_count: u8,
    message_chunk: String
}

impl MessageChunk {
    fn new(buf: &[u8]) -> Result<MessageChunk, std::str::Utf8Error> {
        Ok(MessageChunk {
            message_id: std::string::String::from_utf8_lossy(&buf[2..9]).to_string(),
            sequence_number: buf[10],
            sequence_count: buf[11],
            message_chunk: std::str::from_utf8(&buf[12..])?.to_string(),
        })
    }
}

struct UdpAcceptor {
    socket: UdpSocket,
    chunked_messages: HashMap<String, Vec<MessageChunk>>,
}

impl UdpAcceptor {
    fn parse_data(&mut self, buf: &[u8]) -> io::Result<String> {
        let mut parsed_buf = String::new();

        if is_chunk(&buf) {
            let chunk = match MessageChunk::new(buf) {
                Ok(s) => s,
                Err(e) => {
                    return Err(io::Error::from(io::ErrorKind::InvalidData))
                }
            };

            let message_id = chunk.message_id.clone();

            match self.chunked_messages.get_mut(&message_id) {
                Some(chunks) => {
                    let sequence_count = chunk.sequence_count.clone();
                    chunks.push(chunk);
                    if sequence_count == chunks.len() as u8 {
                        chunks.sort_by(|a,b| {
                            a.sequence_number.partial_cmp(&b.sequence_number).unwrap()
                        });

                        for chunk in chunks {
                            parsed_buf += chunk.message_chunk.as_str();
                        }

                        return Ok(parsed_buf);
                    }
                }
                None => {
                    self.chunked_messages.insert(message_id, vec![chunk]);
                }
            }

            return Err(io::Error::from(io::ErrorKind::WriteZero))
        }
        if is_zlib(&buf) {
            let mut zlib_decompressor = ZlibDecoder::new(buf);
            zlib_decompressor.read_to_string(&mut parsed_buf)?;
        } else if is_gz(buf) {
            let mut gzip_decompressor = GzDecoder::new(buf);
            gzip_decompressor.read_to_string(&mut parsed_buf)?;
        } else {
            parsed_buf = match std::str::from_utf8(buf) {
                Ok(s) => Ok(s.to_string()),
                Err(e) => {
                    eprintln!("{:?}", &buf);
                    Err(io::Error::from(io::ErrorKind::InvalidData))
                }
            }?;
        }
        Ok(parsed_buf)
    }
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
        let parsed_data = match self.parse_data(&buf[0..n]) {
            Ok(pd) => pd,
            Err(e) => {
                eprintln!("udp parsing data error: {}", e);
                return
            }
        };

        tokio::spawn(async move {
            let reader = GelfReader::from_str(&parsed_data);

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

fn is_chunk(buf: &[u8]) -> bool {
    if buf.len() <= 2 {
        return false
    }
    if !(buf[0] == 30 && buf[1] == 15) {
        return false
    }
    return true
}
fn is_gz(buf: &[u8]) -> bool {
    if buf.len() <= 2 {
        return false
    }
    if !(buf[0] == 31 && buf[1] == 139) {
        return false
    }
    return true
}
fn is_zlib(buf: &[u8]) -> bool {
    let compression_level_bytes = [1,94,156,218];
    if buf.len() <= 2 {
        return false
    }
    if buf[0] != 0x78 {
        return false
    }
    if !compression_level_bytes.contains(&buf[1]) {
        return false
    }
    return true
}