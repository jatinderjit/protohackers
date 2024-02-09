use anyhow::Result;
use protohackers::smoke;

#[tokio::main]
async fn main() -> Result<()> {
    smoke::run().await?;
    Ok(())
}
