use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    protohackers::bank::run().await?;
    Ok(())
}
