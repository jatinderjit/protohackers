use anyhow::Result;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
};

use crate::config::ADDR;

pub async fn run() -> Result<()> {
    let listener = TcpListener::bind(ADDR).await.unwrap();
    println!("Listening on {ADDR}...");

    loop {
        let (mut socket, addr) = listener.accept().await?;
        println!("Connected to {addr}");
        tokio::spawn(async move {
            let (reader, writer) = socket.split();
            process(reader, writer).await
        });
    }
}

async fn process<R, W>(mut reader: R, mut writer: W)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = vec![0; 4096];

    loop {
        let n = reader.read(&mut buf).await.expect("read error");
        if n == 0 {
            println!("EOF");
            break;
        }
        println!("Received {n} bytes: {:?}", &buf[..n]);
        writer.write(&buf[..n]).await.expect("write error");
    }
}

#[cfg(test)]
mod test {
    use super::process;

    #[tokio::test]
    async fn echo() {
        let reader = tokio_test::io::Builder::new()
            .read(b"abc")
            .read(b"123")
            .build();
        let writer = tokio_test::io::Builder::new()
            .write(b"abc")
            .write(b"123")
            .build();
        let _ = process(reader, writer).await;
    }
}
