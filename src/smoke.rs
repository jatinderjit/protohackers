use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const ADDR: &str = "0.0.0.0:10000";

pub async fn run() -> Result<()> {
    let listener = TcpListener::bind(ADDR).await.unwrap();
    println!("Listening on {ADDR}...");

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("Connected to {addr}");
        tokio::spawn(async move { process(socket).await }).await?;
    }
}

async fn process(mut socket: TcpStream) {
    let mut buf = vec![0; 4096];

    loop {
        let n = socket.read(&mut buf).await.expect("read error");
        if n == 0 {
            println!("EOF");
            break;
        }
        println!("Received {n} bytes: {:?}", &buf[..n]);
        socket.write(&buf[..n]).await.expect("write error");
    }
}
