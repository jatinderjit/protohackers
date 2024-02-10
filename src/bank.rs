use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU32, Ordering},
};

use anyhow::Result;
use futures::StreamExt;
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
};
use tokio_util::{
    bytes::Buf,
    codec::{Decoder, FramedRead},
};

use crate::config::ADDR;

struct Session {
    id: u32,
    prices: BTreeMap<i32, i32>,
}

impl Session {
    fn new() -> Session {
        static ID: AtomicU32 = AtomicU32::new(0);
        let id = ID.fetch_add(1, Ordering::Relaxed);
        let prices = BTreeMap::new();
        Session { id, prices }
    }
}

#[derive(Debug)]
enum Message {
    Insert { timestamp: i32, price: i32 },
    Query { start: i32, end: i32 },
    Invalid,
}

pub async fn run() -> Result<()> {
    let listener = TcpListener::bind(ADDR).await.unwrap();
    println!("Listening on {ADDR}...");

    loop {
        let (mut socket, addr) = listener.accept().await?;
        println!("Connected to {addr}");
        tokio::spawn(async move {
            let mut session = Session::new();
            let (reader, writer) = socket.split();
            session.start(reader, writer).await
        });
    }
}

impl Session {
    async fn start<R, W>(&mut self, reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        let mut messages = FramedRead::new(reader, MessageDecoder);
        while let Some(message) = messages.next().await {
            println!("id={}, {message:?}", self.id);
            match message {
                Ok(Message::Query { start, end }) => {
                    let mean = self.get_mean(start, end).await;
                    println!("id={}, mean={mean}", self.id);
                    writer.write_i32(mean).await?
                }
                Ok(Message::Insert { timestamp, price }) => {
                    self.prices.insert(timestamp, price);
                }
                Ok(Message::Invalid) => {
                    writer.write(b"undefined behavior").await?;
                }
                Err(e) => {
                    println!("Message error: {e:?}");
                    writer.write(b"undefined behavior").await?;
                }
            };
        }
        Ok(())
    }

    async fn get_mean(&mut self, start: i32, end: i32) -> i32 {
        if start > end {
            return 0;
        }
        let mut count = 0;
        let total = self
            .prices
            .range(start..=end)
            .map(|(_, p)| {
                count += 1;
                *p as i64
            })
            .sum::<i64>();
        if count == 0 {
            0
        } else {
            (total / count) as i32
        }
    }
}

fn to_num(bytes: &[u8]) -> i32 {
    assert_eq!(bytes.len(), 4);
    let mut b = [0u8; 4];
    b.copy_from_slice(bytes);
    i32::from_be_bytes(b)
}

struct MessageDecoder;

impl Decoder for MessageDecoder {
    type Item = Message;
    type Error = anyhow::Error;

    fn decode(
        &mut self,
        src: &mut tokio_util::bytes::BytesMut,
    ) -> std::prelude::v1::Result<Option<Self::Item>, Self::Error> {
        if src.len() < 9 {
            // Not enough data
            return Ok(None);
        }
        let msg_type = src[0];
        let num1 = to_num(&src[1..5]);
        let num2 = to_num(&src[5..9]);
        src.advance(9);
        match msg_type {
            b'I' => {
                let timestamp = num1;
                let price = num2;
                Ok(Some(Message::Insert { timestamp, price }))
            }
            b'Q' => {
                let start = num1;
                let end = num2;
                Ok(Some(Message::Query { start, end }))
            }
            _ => Ok(Some(Message::Invalid)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::Session;

    #[tokio::test]
    async fn transactions() {
        let reader = tokio_test::io::Builder::new()
            // I 12345 101
            .read(&[0x49, 0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x00, 0x65])
            // I 12346 102
            .read(&[0x49, 0x00, 0x00, 0x30, 0x3a, 0x00, 0x00, 0x00, 0x66])
            // I 12347 100
            .read(&[0x49, 0x00, 0x00, 0x30, 0x3b, 0x00, 0x00, 0x00, 0x64])
            // I 40960 5
            .read(&[0x49, 0x00, 0x00, 0xa0, 0x00, 0x00, 0x00, 0x00, 0x05])
            // Q 12288 16384
            .read(&[0x51, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x40, 0x00])
            .build();
        let writer = tokio_test::io::Builder::new()
            .write(&[0x00, 0x00, 0x00, 0x65]) // 101
            .build();
        let mut session = Session::new();
        let _ = session.start(reader, writer).await;
    }

    #[tokio::test]
    async fn query_empty_db() {
        let reader = tokio_test::io::Builder::new()
            // Q 12288 16384
            .read(&[0x51, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x40, 0x00])
            .build();
        let writer = tokio_test::io::Builder::new().write(&[0, 0, 0, 0]).build();
        let mut session = Session::new();
        let _ = session.start(reader, writer).await;
    }

    #[tokio::test]
    async fn query_min_after_max() {
        let reader = tokio_test::io::Builder::new()
            // I 12345 101
            .read(&[0x49, 0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x00, 0x65])
            // I 12346 102
            .read(&[0x49, 0x00, 0x00, 0x30, 0x3a, 0x00, 0x00, 0x00, 0x66])
            // I 12347 100
            .read(&[0x49, 0x00, 0x00, 0x30, 0x3b, 0x00, 0x00, 0x00, 0x64])
            // I 40960 5
            .read(&[0x49, 0x00, 0x00, 0xa0, 0x00, 0x00, 0x00, 0x00, 0x05])
            // Q 16384 12288
            .read(&[0x51, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x30, 0x00])
            .build();
        let writer = tokio_test::io::Builder::new().write(&[0, 0, 0, 0]).build();
        let mut session = Session::new();
        let _ = session.start(reader, writer).await;
    }

    #[tokio::test]
    async fn invalid_message_type() {
        let reader = tokio_test::io::Builder::new()
            // I 12345 101
            .read(&[0x49, 0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x00, 0x65])
            // I 12346 102
            .read(&[0x49, 0x00, 0x00, 0x30, 0x3a, 0x00, 0x00, 0x00, 0x66])
            // I 12347 100
            .read(&[0x49, 0x00, 0x00, 0x30, 0x3b, 0x00, 0x00, 0x00, 0x64])
            // I 40960 5
            .read(&[0x49, 0x00, 0x00, 0xa0, 0x00, 0x00, 0x00, 0x00, 0x05])
            // P 16384 12288 (invalid)
            .read(&[0x50, 0x00, 0x00, 0xa0, 0x00, 0x00, 0x00, 0x00, 0x05])
            // Q 16384 12288
            .read(&[0x51, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x40, 0x00])
            .build();
        let writer = tokio_test::io::Builder::new()
            .write(b"undefined behavior")
            .write(&[0x00, 0x00, 0x00, 0x65])
            .build();
        let mut session = Session::new();
        let _ = session.start(reader, writer).await;
    }

    #[tokio::test]
    async fn truncated_message() {
        let reader = tokio_test::io::Builder::new()
            // I 12345 101
            .read(&[0x49, 0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x00, 0x65])
            .read(&[0x49, 0x00, 0x00, 0xa0, 0x00])
            .build();
        let writer = tokio_test::io::Builder::new()
            .write(b"undefined behavior")
            .build();
        let mut session = Session::new();
        let _ = session.start(reader, writer).await;
    }

    #[tokio::test]
    async fn split_messages() {
        let reader = tokio_test::io::Builder::new()
            // I 12345 101
            .read(&[0x49, 0x00, 0x00, 0x30])
            .read(&[0x39, 0x00, 0x00, 0x00, 0x65])
            // I 12346 102
            .read(&[0x49, 0x00, 0x00, 0x30, 0x3a, 0x00, 0x00, 0x00, 0x66])
            // I 12347 100
            .read(&[0x49, 0x00, 0x00, 0x30, 0x3b, 0x00, 0x00, 0x00, 0x64])
            // I 40960 5
            .read(&[0x49, 0x00, 0x00, 0xa0, 0x00, 0x00, 0x00, 0x00, 0x05])
            // Q 16384 12288
            .read(&[0x51, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x40, 0x00])
            .build();
        let writer = tokio_test::io::Builder::new()
            .write(&[0x00, 0x00, 0x00, 0x65])
            .build();
        let mut session = Session::new();
        let _ = session.start(reader, writer).await;
    }

    #[tokio::test]
    async fn negative_timestamps() {
        let reader = tokio_test::io::Builder::new()
            .read(&[0x49, 0x00, 0x00, 0x00, 0x00, 0x77, 0x35, 0x94, 0x00])
            .read(&[0x49, 0x00, 0x00, 0x00, 0x01, 0x7a, 0x30, 0x84, 0x80])
            .read(&[0x49, 0x00, 0x00, 0x00, 0x02, 0x7d, 0x2b, 0x75, 0x00])
            .read(&[0x51, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02])
            .read(&[0x51, 0x82, 0xd4, 0x8b, 0x00, 0x7d, 0x2b, 0x75, 0x00])
            .build();
        let writer = tokio_test::io::Builder::new()
            .write(&[0x7a, 0x30, 0x84, 0x80])
            .write(&[0x7a, 0x30, 0x84, 0x80])
            .build();
        let mut session = Session::new();
        let _ = session.start(reader, writer).await;
    }
}
