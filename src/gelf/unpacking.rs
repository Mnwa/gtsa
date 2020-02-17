use std::io;
use std::io::prelude::*;
use actix::prelude::*;
use flate2::read::{ZlibDecoder, GzDecoder};

pub struct UnpackMessage(pub Vec<u8>);

impl Message for UnpackMessage {
    type Result = Result<Vec<u8>, io::Error>;
}

pub struct UnPackActor;

impl UnPackActor {
    pub fn new(threads: usize) -> Addr<UnPackActor> {
        SyncArbiter::start(threads, || UnPackActor)
    }
}

impl Actor for UnPackActor {
    type Context = SyncContext<Self>;
}

impl Handler<UnpackMessage> for UnPackActor {
    type Result = std::io::Result<Vec<u8>>;

    fn handle(&mut self, msg: UnpackMessage, _ctx: &mut Self::Context) -> Self::Result {
        let buf = msg.0.as_slice();
        let mut parsed_buf = Vec::new();

        if is_zlib(buf) {
            let mut zlib_decompressor = ZlibDecoder::new(buf);
            let n = zlib_decompressor.read_to_end(&mut parsed_buf)?;
            parsed_buf.truncate(n)
        } else if is_gz(buf) {
            let mut gzip_decompressor = GzDecoder::new(buf);
            let n = gzip_decompressor.read_to_end(&mut parsed_buf)?;
            parsed_buf.truncate(n)
        } else {
            parsed_buf = Vec::from(buf);
        }
        Ok(parsed_buf)
    }
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