use std::io;
use std::io::prelude::*;
use actix::prelude::*;
use flate2::read::{ZlibDecoder, GzDecoder};
use std::collections::HashMap;

pub struct UnpackMessage(pub Vec<u8>);

impl Message for UnpackMessage {
    type Result = Result<Vec<u8>, io::Error>;
}

pub struct UnPackActor {
    chunked_messages: HashMap<String, Vec<MessageChunk>>,
}

impl UnPackActor {
    pub fn new() -> Addr<UnPackActor> {
        UnPackActor::create(|_| {
            UnPackActor{
                chunked_messages: HashMap::new()
            }
        })
    }
}

impl Actor for UnPackActor {
    type Context = Context<Self>;
}

impl Handler<UnpackMessage> for UnPackActor {
    type Result = Result<Vec<u8>, io::Error>;

    fn handle(&mut self, msg: UnpackMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let mut parsed_buf = Vec::new();
        let buf = msg.0.as_slice();

        if is_chunk(buf) {
            let chunk = MessageChunk::new(buf);

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
                            parsed_buf.append(&mut chunk.message_chunk);
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
        if is_zlib(buf) {
            let mut zlib_decompressor = ZlibDecoder::new(buf);
            zlib_decompressor.read_to_end(&mut parsed_buf)?;
        } else if is_gz(buf) {
            let mut gzip_decompressor = GzDecoder::new(buf);
            gzip_decompressor.read_to_end(&mut parsed_buf)?;
        } else {
            parsed_buf = Vec::from(buf);
        }
        Ok(parsed_buf)
    }
}

#[derive(Debug)]
struct MessageChunk {
    message_id: String,
    sequence_number: u8,
    sequence_count: u8,
    message_chunk: Vec<u8>
}

impl MessageChunk {
    fn new(buf: &[u8]) -> MessageChunk {
        MessageChunk {
            message_id: std::string::String::from_utf8_lossy(&buf[2..9]).to_string(),
            sequence_number: buf[10],
            sequence_count: buf[11],
            message_chunk: Vec::from(&buf[12..]),
        }
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