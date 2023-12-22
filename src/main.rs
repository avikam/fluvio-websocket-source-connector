mod config;
mod websocket;

use config::WebSocketConfig;
use fluvio::{RecordKey, TopicProducer};
use fluvio_connector_common::{connector, tracing::{debug, trace}, Result, Source};
use futures::StreamExt;
use websocket::WebSocketSource;

#[connector(source)]
async fn start(config: WebSocketConfig, producer: TopicProducer) -> Result<()> {
    debug!(?config);
    let source = WebSocketSource::new(&config)?;
    let mut stream = source.connect(None).await?;

    while let Some(item) = stream.next().await {
        trace!(?item);
        producer.send(RecordKey::NULL, item).await?;
    }

    Ok(())
}
