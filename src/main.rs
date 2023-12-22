mod config;
mod source;
mod events;

use crate::config::WebSocketFluvioConfig;
use crate::source::WebSocketFluvioSource;
use fluvio::{RecordKey, TopicProducer};
use fluvio_connector_common::{connector, tracing::{debug, trace}, Result , Source};
use futures::StreamExt;

#[connector(source)]
async fn start(config: WebSocketFluvioConfig, producer: TopicProducer) -> Result<()> {
    debug!(?config);
    let source = WebSocketFluvioSource::new(&config)?;
    let mut stream = source.connect(None).await?;

    while let Some(item) = stream.next().await {
        trace!(?item);
        producer.send(RecordKey::NULL, item).await?;
    }

    Ok(())
}


