use std::collections::BTreeMap;

use anyhow::Result;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
};

use crate::config::ADDR;

struct Session<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    prices: BTreeMap<u32, i32>,
    reader: R,
    writer: W,
}

pub async fn run() -> Result<()> {
    let listener = TcpListener::bind(ADDR).await.unwrap();
    println!("Listening on {ADDR}...");

    loop {
        let (mut socket, addr) = listener.accept().await?;
        println!("Connected to {addr}");
        tokio::spawn(async move {
            let (reader, writer) = socket.split();
            let mut session = Session {
                prices: BTreeMap::new(),
                reader,
                writer,
            };
            session.start().await
        });
    }
}

impl<R, W> Session<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    async fn start(&mut self) -> Result<()> {
        let mut msg = [0; 9];

        loop {
            let n = self.reader.read(&mut msg).await.expect("read error");
            if n == 0 {
                return Ok(());
            }
            if n == 9 {
                self.handle_message(&msg).await?
            } else {
                self.undefined_behavior().await?;
            }
        }
    }

    async fn handle_message(&mut self, msg: &[u8; 9]) -> Result<()> {
        match msg[0] {
            b'I' => Ok(self.insert(msg)),
            b'Q' => self.query(msg).await,
            _ => self.undefined_behavior().await,
        }
    }

    fn insert(&mut self, msg: &[u8; 9]) {
        let timestamp = read_uint(&msg[1..5]);
        let price = read_uint(&msg[5..9]) as i32;
        self.prices.insert(timestamp, price);
        println!("Inserted: {timestamp} => {price}");
    }

    async fn query(&mut self, msg: &[u8; 9]) -> Result<()> {
        let min_time = read_uint(&msg[1..5]);
        let max_time = read_uint(&msg[5..9]);
        if min_time > max_time {
            return Ok(self.writer.write_i32(0).await?);
        }
        let mut count = 0;
        let total = self
            .prices
            .range(min_time..=max_time)
            .map(|(_, p)| {
                count += 1;
                p
            })
            .sum::<i32>();
        let mean = if count == 0 { 0 } else { total / count };
        println!("{total} / {count} = {mean}");
        println!("query: mean between {min_time} to {max_time} = {mean}");
        Ok(self.writer.write_i32(mean).await?)
    }

    async fn undefined_behavior(&mut self) -> Result<()> {
        println!("prices: {:?}", self.prices);
        self.writer.write(b"undefined behavior").await?;
        Ok(())
    }
}

fn read_uint(bytes: &[u8]) -> u32 {
    assert_eq!(bytes.len(), 4);
    let mut num = 0;
    // Read number as Big-endian (network byte order)
    bytes.iter().for_each(|b| num = (num << 8) + (*b as u32));
    num
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

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
        let prices = BTreeMap::new();
        let mut session = Session {
            prices,
            reader,
            writer,
        };
        let _ = session.start().await;
    }

    #[tokio::test]
    async fn query_empty_db() {
        let reader = tokio_test::io::Builder::new()
            // Q 12288 16384
            .read(&[0x51, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x40, 0x00])
            .build();
        let writer = tokio_test::io::Builder::new().write(&[0, 0, 0, 0]).build();
        let prices = BTreeMap::new();
        let mut session = Session {
            prices,
            reader,
            writer,
        };
        let _ = session.start().await;
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
        let prices = BTreeMap::new();
        let mut session = Session {
            prices,
            reader,
            writer,
        };
        let _ = session.start().await;
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
        let prices = BTreeMap::new();
        let mut session = Session {
            prices,
            reader,
            writer,
        };
        let _ = session.start().await;
    }

    #[tokio::test]
    async fn truncated_message() {
        let reader = tokio_test::io::Builder::new()
            // I 12345 101
            .read(&[0x49, 0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x00, 0x65])
            // I 12346 102
            .read(&[0x49, 0x00, 0x00, 0x30, 0x3a, 0x00, 0x00, 0x00, 0x66])
            // I 12347 100
            .read(&[0x49, 0x00, 0x00, 0x30, 0x3b, 0x00, 0x00, 0x00, 0x64])
            // I 40960 5
            .read(&[0x49, 0x00, 0x00, 0xa0, 0x00, 0x00, 0x00, 0x00, 0x05])
            // I 16384 (invalid)
            .read(&[0x49, 0x00, 0x00, 0xa0, 0x00])
            // Q 16384 12288
            .read(&[0x51, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x40, 0x00])
            .build();
        let writer = tokio_test::io::Builder::new()
            .write(b"undefined behavior")
            .write(&[0x00, 0x00, 0x00, 0x65])
            .build();
        let prices = BTreeMap::new();
        let mut session = Session {
            prices,
            reader,
            writer,
        };
        let _ = session.start().await;
    }
}
