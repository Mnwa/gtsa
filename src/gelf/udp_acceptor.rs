use crate::gelf::gelf_message_processor::GelfProcessorMessage;
use crate::gelf::gelf_reader::{GelfMessage, GelfReaderActor};
use crate::gelf::unpacking::{UnPackActor, UnpackMessage};
use actix::dev::ToEnvelope;
use actix::prelude::*;
use futures::prelude::*;
use std::net::SocketAddr;
use tokio::net::udp::RecvHalf;
use tokio::net::{ToSocketAddrs, UdpSocket};

use crate::gelf::error::GelfError;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::SystemTime;

pub async fn new_udp_acceptor<T, A>(
    bind_addr: T,
    gelf_processor: Arc<Addr<A>>,
    reader: Arc<Addr<GelfReaderActor>>,
    unpacker: Arc<Addr<UnPackActor>>,
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
    unpacker: Arc<Addr<UnPackActor>>,
    unchanker: Arc<Addr<ChunkAcceptor>>,
    reader: Arc<Addr<GelfReaderActor>>,
    gelf_processor: Arc<Addr<T>>,
}
impl<T> UdpActor<T>
where
    T: Actor + Handler<GelfProcessorMessage>,
    T::Context: ToEnvelope<T, GelfProcessorMessage>,
{
    pub fn new(
        recv: RecvHalf,
        gelf_processor: Arc<Addr<T>>,
        reader: Arc<Addr<GelfReaderActor>>,
        unpacker: Arc<Addr<UnPackActor>>,
        max_parallel_chunks: usize,
    ) -> Addr<UdpActor<T>> {
        UdpActor::create(|ctx| {
            ctx.add_stream(read_udp(recv));
            UdpActor {
                unpacker,
                reader,
                gelf_processor,
                unchanker: Arc::new(ChunkAcceptor::new(max_parallel_chunks)),
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
        let reader_actor = Arc::clone(&self.reader);
        let processor_actor = Arc::clone(&self.gelf_processor);
        let unpacker_actor = Arc::clone(&self.unpacker);
        let unchanker_actor = Arc::clone(&self.unchanker);

        ctx.spawn(
            async move {
                let packed_buf_message = unchanker_actor
                    .send(UnchankMessage(buf))
                    .await
                    .map_err(|e| GelfError::from_err("unchunker actor mailing error", e))
                    .map_err(|e| {
                        eprintln!("{}", e);
                        e
                    })
                    .ok()
                    .and_then(|p| p)
                    .map(UnpackMessage);

                let packed_buf_message = match packed_buf_message {
                    Some(p) => p,
                    None => {
                        return;
                    }
                };

                let gelf_msg = unpacker_actor
                    .send(packed_buf_message)
                    .await
                    .map_err(|e| GelfError::from_err("unpacker actor mailing error", e))
                    .and_then(|bm| bm.map_err(|e| GelfError::from_err("udp parsing data error", e)))
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
                    .map_err(|e| GelfError::from_err("gelf actor mailing error", e))
                    .and_then(|reader| {
                        reader.map_err(|e| {
                            println!(
                                "Original response: {}",
                                String::from_utf8_lossy(&original_message)
                            );
                            GelfError::from_err("udp parsing gelf error", e)
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

pub struct UnchankMessage(pub Vec<u8>);

impl Message for UnchankMessage {
    type Result = Option<Vec<u8>>;
}

struct ChunkAcceptor {
    chunked_messages: HashMap<String, (SystemTime, BinaryHeap<MessageChunk>)>,
    max_parallel_chunks: usize,
}

impl ChunkAcceptor {
    fn new(max_parallel_chunks: usize) -> Addr<ChunkAcceptor> {
        ChunkAcceptor::create(|_| ChunkAcceptor {
            chunked_messages: HashMap::with_capacity(max_parallel_chunks),
            max_parallel_chunks,
        })
    }
}

impl Actor for ChunkAcceptor {
    type Context = Context<Self>;
}

impl Handler<UnchankMessage> for ChunkAcceptor {
    type Result = Option<Vec<u8>>;

    fn handle(
        &mut self,
        UnchankMessage(buf): UnchankMessage,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        if is_chunk(&buf) {
            let chunk = MessageChunk::new(buf);

            let message_id = chunk.message_id.clone();
            let sequence_count = chunk.sequence_count;

            let (_, chunks) = self
                .chunked_messages
                .entry(message_id.clone())
                .or_insert_with(|| (SystemTime::now(), BinaryHeap::with_capacity(128)));

            chunks.push(chunk);
            let chunks_len = { chunks.len() as u8 };

            if self.chunked_messages.len() == self.max_parallel_chunks {
                let invalid_keys: Vec<String> = self
                    .chunked_messages
                    .iter()
                    .filter_map(|(k, (t, _))| {
                        if t.elapsed().ok()?.as_secs() > 5 {
                            return Some(k.clone());
                        }
                        None
                    })
                    .collect();

                let invalid_keys: Option<Vec<BinaryHeap<MessageChunk>>> = invalid_keys
                    .iter()
                    .map(|k| self.chunked_messages.remove(k))
                    .map(|v| v.map(|(_, v)| v))
                    .collect();

                if let Some(invalid_keys) = invalid_keys {
                    println!("keys successfull cleared: {:#?}", invalid_keys)
                } else {
                    eprintln!("Fail to clear chunks hashmap")
                }
            }

            if sequence_count == chunks_len {
                let parsed_buf: Vec<u8> = self
                    .chunked_messages
                    .remove(&message_id)?
                    .1
                    .into_sorted_vec()
                    .into_iter()
                    .flat_map(|chunk| chunk.message_chunk.into_iter())
                    .collect();

                return Some(parsed_buf);
            }

            None
        } else {
            Some(buf)
        }
    }
}

#[derive(Clone, Debug)]
struct MessageChunk {
    message_id: String,
    sequence_number: u8,
    sequence_count: u8,
    message_chunk: Vec<u8>,
}

impl MessageChunk {
    fn new(buf: Vec<u8>) -> MessageChunk {
        MessageChunk {
            message_id: std::string::String::from_utf8_lossy(&buf[2..9]).into_owned(),
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

#[cfg(test)]
mod acceptor {
    use crate::gelf::udp_acceptor::{ChunkAcceptor, UnchankMessage};

    #[actix_rt::test]
    async fn test_unpacker() {
        let unpacker_actor = ChunkAcceptor::new(5);

        let message_1 = {
            let mut temp = vec![30, 15];
            temp.append(&mut vec![1, 2, 3, 4, 5, 6, 7, 8]);
            temp.append(&mut vec![0, 2]);
            temp.append(&mut b"test".to_vec());
            temp
        };
        let message_2 = {
            let mut temp = vec![30, 15];
            temp.append(&mut vec![1, 2, 3, 4, 5, 6, 7, 8]);
            temp.append(&mut vec![1, 2]);
            temp.append(&mut b"test".to_vec());
            temp
        };
        let response_1 = unpacker_actor
            .send(UnchankMessage(message_1))
            .await
            .unwrap();
        assert_eq!(response_1, None);
        let response_2 = unpacker_actor
            .send(UnchankMessage(message_2))
            .await
            .unwrap();
        assert_eq!(response_2, Some(b"testtest".to_vec()))
    }
}
