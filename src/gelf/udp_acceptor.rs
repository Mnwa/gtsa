use crate::gelf::gelf_message_processor::GelfProcessorMessage;
use crate::gelf::gelf_reader::{GelfMessage, GelfReaderActor};
use crate::gelf::unpacking::{UnPackActor, UnpackMessage};
use actix::dev::ToEnvelope;
use actix::prelude::*;
use futures::prelude::*;
use std::net::SocketAddr;
use tokio::net::udp::RecvHalf;
use tokio::net::{ToSocketAddrs, UdpSocket};

extern crate lru;
use crate::gelf::error::GelfError;
use lru::LruCache;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

pub async fn new_udp_acceptor<T, A>(
    bind_addr: T,
    gelf_processor: Addr<A>,
    reader: Addr<GelfReaderActor>,
    unpacker: Addr<UnPackActor>,
    max_parallel_chunks: usize,
) where
    T: ToSocketAddrs,
    A: Actor + Handler<GelfProcessorMessage>,
    A::Context: ToEnvelope<A, GelfProcessorMessage>,
{
    let socket = UdpSocket::bind(bind_addr).await.unwrap();
    let (recv, _) = socket.split();
    UdpActor::new(recv, gelf_processor, reader, unpacker, max_parallel_chunks);
}

pub struct UdpActor<T>
where
    T: Actor + Handler<GelfProcessorMessage>,
    T::Context: ToEnvelope<T, GelfProcessorMessage>,
{
    unpacker: Addr<UnPackActor>,
    unchanker: Addr<ChunkAcceptor>,
    reader: Addr<GelfReaderActor>,
    gelf_processor: Addr<T>,
}
impl<T> UdpActor<T>
where
    T: Actor + Handler<GelfProcessorMessage>,
    T::Context: ToEnvelope<T, GelfProcessorMessage>,
{
    pub fn new(
        recv: RecvHalf,
        gelf_processor: Addr<T>,
        reader: Addr<GelfReaderActor>,
        unpacker: Addr<UnPackActor>,
        max_parallel_chunks: usize,
    ) -> Addr<UdpActor<T>> {
        UdpActor::create(|ctx| {
            ctx.add_stream(read_udp(recv));
            UdpActor {
                unpacker,
                reader,
                gelf_processor,
                unchanker: ChunkAcceptor::new(max_parallel_chunks),
            }
        })
    }
}
impl<T> Actor for UdpActor<T>
where
    T: Actor + Handler<GelfProcessorMessage>,
    T::Context: ToEnvelope<T, GelfProcessorMessage>,
{
    type Context = Context<Self>;
}

pub struct UdpPacket(Vec<u8>, SocketAddr);
impl Message for UdpPacket {
    type Result = Vec<u8>;
}

impl<T: 'static> StreamHandler<UdpPacket> for UdpActor<T>
where
    T: Actor + Handler<GelfProcessorMessage>,
    T::Context: ToEnvelope<T, GelfProcessorMessage>,
{
    fn handle(&mut self, UdpPacket(buf, _addr): UdpPacket, ctx: &mut Context<Self>) {
        let reader_actor = self.reader.clone();
        let processor_actor = self.gelf_processor.clone();
        let unpacker_actor = self.unpacker.clone();
        let unchanker_actor = self.unchanker.clone();

        ctx.spawn(
            async move {
                let packed_buf_message = unchanker_actor
                    .send(UnpackMessage(buf))
                    .await
                    .map_err(|e| GelfError::from_err("unchunker actor mailing error:", e))
                    .and_then(|bu| {
                        bu.map_err(|e| GelfError::from_err("udp unchunking data error:", e))
                    })
                    .map(UnpackMessage);

                let packed_buf_message = match packed_buf_message {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("{}", e);
                        return;
                    }
                };

                let gelf_msg = unpacker_actor
                    .send(packed_buf_message)
                    .await
                    .map_err(|e| GelfError::from_err("unpacker actor mailing error:", e))
                    .and_then(|bm| {
                        bm.map_err(|e| GelfError::from_err("udp parsing data error:", e))
                    })
                    .map(GelfMessage);

                let gelf_msg = match gelf_msg {
                    Ok(g) => g,
                    Err(e) => {
                        eprintln!("{}", e);
                        return;
                    }
                };

                let original_message = gelf_msg.0.clone();

                let gelf_msg = reader_actor
                    .send(gelf_msg)
                    .await
                    .map_err(|e| GelfError::from_err("gelf actor mailing error:", e))
                    .and_then(|reader| {
                        reader.map_err(|e| {
                            println!(
                                "Original response: {}",
                                String::from_utf8_lossy(&original_message)
                            );
                            GelfError::from_err("udp parsing gelf error:", e)
                        })
                    })
                    .map(GelfProcessorMessage);

                let gelf_msg = match gelf_msg {
                    Ok(g) => g,
                    Err(e) => {
                        eprintln!("{}", e);
                        return;
                    }
                };

                if let Err(e) = processor_actor.send(gelf_msg).await {
                    eprintln!("gelf actor processing error: {}", e);
                    return;
                }
            }
            .into_actor(self),
        );
    }
}

fn read_udp(recv: RecvHalf) -> impl Stream<Item = UdpPacket> {
    stream::unfold(recv, |mut recv| async {
        let mut buf = vec![0; 8196];
        let (n, addr) = match recv.recv_from(&mut buf).await {
            Ok((n, addr)) => (n, addr),
            Err(e) => panic!("udp handling panic: {:?}", e),
        };
        buf.truncate(n);
        Some(((buf, addr), recv))
    })
    .map(|(buf, addr)| UdpPacket(buf, addr))
}

struct ChunkAcceptor {
    chunked_messages: LruCache<String, BinaryHeap<MessageChunk>>,
}

impl ChunkAcceptor {
    fn new(max_parallel_chunks: usize) -> Addr<ChunkAcceptor> {
        ChunkAcceptor::create(|_| ChunkAcceptor {
            chunked_messages: LruCache::new(max_parallel_chunks),
        })
    }
}

impl Actor for ChunkAcceptor {
    type Context = Context<Self>;
}

impl Handler<UnpackMessage> for ChunkAcceptor {
    type Result = std::io::Result<Vec<u8>>;

    fn handle(&mut self, msg: UnpackMessage, _ctx: &mut Self::Context) -> Self::Result {
        let UnpackMessage(buf) = msg;

        if is_chunk(&buf) {
            let chunk = MessageChunk::new(buf);

            let message_id = chunk.message_id.clone();

            match self.chunked_messages.get_mut(&message_id) {
                Some(chunks) => {
                    let sequence_count = chunk.sequence_count;
                    chunks.push(chunk);
                    if sequence_count == chunks.len() as u8 {
                        let parsed_buf = chunks
                            .clone()
                            .into_sorted_vec()
                            .into_iter()
                            .flat_map(|chunk| chunk.message_chunk.into_iter())
                            .collect();

                        self.chunked_messages.pop(&message_id);

                        return Ok(parsed_buf);
                    }
                }
                None => {
                    let b_h = {
                        let mut temp = BinaryHeap::with_capacity(1);
                        temp.push(chunk);
                        temp
                    };
                    self.chunked_messages.put(message_id, b_h);
                }
            }

            Err(std::io::Error::from(std::io::ErrorKind::WriteZero))
        } else {
            Ok(buf)
        }
    }
}

#[derive(Clone)]
struct MessageChunk {
    message_id: String,
    sequence_number: u8,
    sequence_count: u8,
    message_chunk: Vec<u8>,
}

impl MessageChunk {
    fn new(buf: Vec<u8>) -> MessageChunk {
        MessageChunk {
            message_id: std::string::String::from_utf8_lossy(&buf[2..9]).to_string(),
            sequence_number: buf[10],
            sequence_count: buf[11],
            message_chunk: buf[12..].to_vec(),
        }
    }
}

impl PartialEq for MessageChunk {
    fn eq(&self, other: &Self) -> bool {
        self.sequence_number.eq(&other.sequence_number)
    }
}
impl PartialOrd for MessageChunk {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.sequence_number.partial_cmp(&other.sequence_number)
    }
}

impl Eq for MessageChunk {}
impl Ord for MessageChunk {
    fn cmp(&self, other: &Self) -> Ordering {
        self.sequence_number.cmp(&other.sequence_number)
    }
}

fn is_chunk(buf: &[u8]) -> bool {
    if buf.len() <= 2 {
        return false;
    }
    if !(buf[0] == 30 && buf[1] == 15) {
        return false;
    }
    true
}
