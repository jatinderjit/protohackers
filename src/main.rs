use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    protohackers::smoke::run().await?;
    Ok(())
}
