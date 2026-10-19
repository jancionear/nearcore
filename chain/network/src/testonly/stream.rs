//! Stream wrapper, which allows for custom interactions with the network protocol.
//! We might want to turn it into a fuzz testing framework for the network protocol.
use crate::network_protocol::{Encoding, PeerMessage, PeerMessageFrames, parse_borsh_header};
use crate::tcp;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct Stream {
    stream: tcp::Stream,
}

impl Stream {
    pub fn new(stream: tcp::Stream) -> Self {
        Self { stream }
    }

    async fn read_frame(&mut self) -> Result<Vec<u8>, std::io::Error> {
        let n = self.stream.stream.read_u32_le().await? as usize;
        let mut buf = BytesMut::new();
        buf.resize(n, 0);
        self.stream.stream.read_exact(&mut buf[..]).await?;
        Ok(buf.to_vec())
    }

    pub async fn read(&mut self) -> Result<PeerMessage, std::io::Error> {
        let first = self.read_frame().await?;
        let frames = match parse_borsh_header(&first) {
            Ok(_) => PeerMessageFrames { header: Some(first), body: self.read_frame().await? },
            Err(_) => PeerMessageFrames { header: None, body: first },
        };
        PeerMessage::deserialize(&frames).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to deserialize: {}", e),
            )
        })
    }

    pub async fn write(&mut self, msg: &PeerMessage) {
        self.write_encoded(&msg.serialize(Encoding::Proto)).await;
    }

    async fn write_encoded(&mut self, frames: &PeerMessageFrames) {
        for frame in frames.header.iter().chain([&frames.body]) {
            self.stream.stream.write_u32_le(frame.len() as u32).await.unwrap();
            self.stream.stream.write_all(frame).await.unwrap();
        }
        self.stream.stream.flush().await.unwrap();
    }
}
