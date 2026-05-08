use std::time::Duration;

use rr_config_etcd::{ConfigProvider, EtcdConfigProvider};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .try_init();

    let provider = EtcdConfigProvider::new(
        vec!["http://localhost:2379".to_string()],
        "default".to_string(),
    )
    .await?;

    let routing_table = provider.get_routing_table().await?;
    println!("routing table: {routing_table:#?}");

    let mut changes = provider.watch_changes().await?;
    let next = tokio::time::timeout(Duration::from_secs(30), changes.next()).await;

    match next {
        Ok(Some(change)) => println!("first change event: {change:?}"),
        Ok(None) => println!("watch stream ended before receiving an event"),
        Err(_) => println!("timed out waiting for first watch event"),
    }

    Ok(())
}
