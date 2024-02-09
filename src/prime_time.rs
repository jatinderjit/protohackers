use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpListener,
};

use crate::config::ADDR;

#[derive(Deserialize)]
struct Request<'a> {
    method: &'a str,
    number: f64,
}

#[derive(Serialize)]
struct Response<'a> {
    method: &'a str,
    prime: bool,
}

pub async fn run() -> Result<()> {
    let listener = TcpListener::bind(ADDR).await.unwrap();
    println!("Listening on {ADDR}...");

    loop {
        let (mut socket, addr) = listener.accept().await?;
        println!("Connected to {addr}");
        tokio::spawn(async move {
            let (reader, writer) = socket.split();
            process(reader, writer).await.unwrap()
        });
    }
}

async fn process<R, W>(reader: R, mut writer: W) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    while let Some(line) = lines.next_line().await? {
        match serde_json::from_str::<Request>(&line) {
            Ok(req) => {
                let method = req.method;
                if method != "isPrime" {
                    malformed_response(writer).await?;
                    return Ok(());
                }
                let prime = if req.number.fract() == 0.0 {
                    is_prime(req.number as i64)
                } else {
                    false
                };
                let res = Response { method, prime };
                let res = serde_json::to_string(&res)?;
                writer.write(res.as_bytes()).await?;
                writer.write_u8(b'\n').await?;
            }
            Err(_) => {
                malformed_response(writer).await?;
                return Ok(());
            }
        }
    }
    Ok(())
}

fn is_prime(num: i64) -> bool {
    if num < 2 {
        return false;
    }
    for n in 2..=((num as f64).sqrt().floor() as i64) {
        if num % n == 0 {
            return false;
        }
    }
    true
}

async fn malformed_response<W>(mut writer: W) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write(b"Malformed Response").await?;
    writer.shutdown().await?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::process;

    #[tokio::test]
    async fn is_prime() {
        let reader = tokio_test::io::Builder::new()
            .read(b"{\"method\": \"isPrime\", \"number\": 1.23}\n")
            .read(b"{\"method\": \"isPrime\", \"number\": 1}\n")
            .read(b"{\"method\": \"isPrime\", \"number\": 2}\n")
            .read(b"{\"method\": \"isPrime\", \"number\": 2.0}\n")
            .read(b"{\"method\": \"isPrime\", \"number\": 13}\n")
            .read(b"{\"method\": \"isPrime\", \"number\": 15}\n")
            .read(b"{\"method\": \"isPrime\", \"number\": 13.0}\n")
            .read(b"{\"method\": \"isPrime\", \"number\": 15.0}\n")
            .build();
        let writer = tokio_test::io::Builder::new()
            .write(b"{\"method\":\"isPrime\",\"prime\":false}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":false}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":true}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":true}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":true}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":false}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":true}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":false}\n")
            .build();
        let _ = process(reader, writer).await;
    }

    #[tokio::test]
    async fn isnt_prime() {
        let reader = tokio_test::io::Builder::new()
            .read(b"{\"method\": \"isntPrime\", \"number\": 1.23}\n")
            .build();
        let writer = tokio_test::io::Builder::new()
            .write(b"Malformed Response")
            .build();
        let _ = process(reader, writer).await;
    }

    #[tokio::test]
    async fn extra_fields() {
        let reader = tokio_test::io::Builder::new()
            .read(b"{\"method\": \"isPrime\", \"number\": 1.23, \"extra\": true}\n")
            .read(b"{\"method\": \"isPrime\", \"number\": 19, \"extra\": true}\n")
            .build();
        let writer = tokio_test::io::Builder::new()
            .write(b"{\"method\":\"isPrime\",\"prime\":false}\n")
            .write(b"{\"method\":\"isPrime\",\"prime\":true}\n")
            .build();
        let _ = process(reader, writer).await;
    }
}
