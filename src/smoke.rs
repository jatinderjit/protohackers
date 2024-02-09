use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const ADDR: &str = "127.0.0.1:10000";

pub async fn run() -> Result<()> {
    let listener = TcpListener::bind(ADDR).await.unwrap();
    println!("Listening on {ADDR}...");

    loop {
        let (socket, _) = listener.accept().await?;
        process(socket).await?;
    }
}

async fn process(mut socket: TcpStream) -> Result<()> {
    let mut data = Vec::new();
    let mut buf = vec![0; 1024];

    loop {
        let n = socket.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        data.extend(&buf[..n]);
    }
    socket.write_all(&data).await?;
    Ok(())
}
