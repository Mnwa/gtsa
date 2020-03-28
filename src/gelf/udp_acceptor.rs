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
            ctx.add_stream(read_many(recv).map(|(buf, addr)| UdpPacket(buf, addr)));
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
                let chunked_buf_message = UnpackMessage(buf);
                let unchanked_requested_buf = unchanker_actor.send(chunked_buf_message);
                let unchanked_result_buf = match unchanked_requested_buf.await {
                    Ok(pd) => pd,
                    Err(e) => {
                        eprintln!("unchunker actor mailing error: {}", e);
                        return;
                    }
                };
                let unchanked_buf = match unchanked_result_buf {
                    Ok(pd) => pd,
                    Err(e) => {
                        if e.kind() != std::io::ErrorKind::WriteZero {
                            eprintln!("udp unchunking data error: {}", e);
                        }
                        return;
                    }
                };

                let packed_buf_message = UnpackMessage(unchanked_buf);
                let unpacked_requested_buf = unpacker_actor.send(packed_buf_message);
                let unpacked_result_buf = match unpacked_requested_buf.await {
                    Ok(pd) => pd,
                    Err(e) => {
                        eprintln!("unpacker actor mailing error: {}", e);
                        return;
                    }
                };
                let parsed_data = match unpacked_result_buf {
                    Ok(pd) => pd,
                    Err(e) => {
                        eprintln!("udp parsing data error: {}", e);
                        return;
                    }
                };

                let gelf_msg = GelfMessage(parsed_data.clone());

                let reader = match reader_actor.send(gelf_msg).await {
                    Ok(ug) => ug,
                    Err(e) => {
                        eprintln!("gelf actor mailing error: {}", e);
                        return;
                    }
                };

                match reader {
                    Ok(reader) => {
                        let printer_message = GelfProcessorMessage(reader);
                        match processor_actor.send(printer_message).await {
                            Ok(ug) => ug,
                            Err(e) => {
                                eprintln!("gelf actor processing error: {}", e);
                                return;
                            }
                        };
                    }
                    Err(e) => match std::str::from_utf8(&parsed_data) {
                        Ok(s) => {
                            eprintln!("udp parsing gelf error: {}\nOriginal response: {:?}", e, s)
                        }
                        Err(_e) => eprintln!(
                            "udp parsing gelf error: {}\nOriginal response: {:?}",
                            e, &parsed_data
                        ),
                    },
                };
            }
            .into_actor(self),
        );
    }
}

type RecvData = (Vec<u8>, SocketAddr);

fn read_many(recv: RecvHalf) -> impl Stream<Item = RecvData> {
    stream::unfold(recv, |mut recv| async {
        let mut buf = vec![0; 8196];
        let (n, addr) = match recv.recv_from(&mut buf).await {
            Ok((n, addr)) => (n, addr),
            Err(_e) => return None,
        };
        buf.truncate(n);
        Some(((buf, addr), recv))
    })
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
                    let sequence_count = chunk.sequence_count.clone();
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
    return true;
}
