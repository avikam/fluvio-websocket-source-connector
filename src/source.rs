use crate::config::WebSocketFluvioConfig;
use anyhow::{Result,Context};
use async_trait::async_trait;
use fluvio::Offset;
use fluvio_connector_common::{tracing::{error, debug, info}, Source};
use futures::{self, stream::{LocalBoxStream, SplitSink}, SinkExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};
use tokio_stream::{wrappers::IntervalStream, StreamExt};
use tokio_tungstenite::{connect_async, MaybeTlsStream, tungstenite::protocol::Message, WebSocketStream};

pub(crate) struct WebSocketFluvioSource {
    config: WebSocketFluvioConfig,
}

type Transport = MaybeTlsStream<TcpStream>;

// Computes the backoff delay using an exponential strategy
fn compute_backoff(attempt: usize, base_delay_ms: usize, max_delay_ms: usize) -> usize {
    let exponent = 2usize.pow(attempt.min(31) as u32); // Prevent overflow, cap exponent at 2^31
    let delay = base_delay_ms.saturating_mul(exponent);
    delay.min(max_delay_ms)
}

fn max_retries(config: &WebSocketFluvioConfig) -> Option<usize> {
    config.reconnection_policy.as_ref().map(|rc| rc.max_retries)
}

async fn establish_connection(config: WebSocketFluvioConfig) -> Result<WebSocketStream<Transport>> {
    let mut attempt = 0;
    loop {
        match connect_async(config.uri.as_str()).await {
            Ok((mut ws_stream, _)) => {
                info!("WebSocket connected to {}", config.uri);
                if let Some(message) = config.subscription_message.as_ref() {
                    ws_stream.send(Message::Text(message.to_owned())).await?;
                }
                return Ok(ws_stream);
            }
            Err(e) => {
                error!("WebSocket connection error on attempt {}: {}", attempt, e);
                attempt += 1;

                if attempt >= max_retries(&config).unwrap_or(1) { 
                    break
                }

                if let Some(ref reconnection_policy) = config.reconnection_policy {
                    let delay = compute_backoff(attempt, reconnection_policy.base_delay_ms as usize, reconnection_policy.max_delay_ms as usize);
                    sleep(Duration::from_millis(delay as u64)).await;
                }
            }
        }
    }

    Err(anyhow::Error::new(
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to establish WebSocket connection after {} attempts", attempt),
        )
    ))
}

async fn ping(write_end: &mut SplitSink<WebSocketStream<Transport>, Message>) -> Result<()> {
    write_end.send(Message::Ping(Vec::new())).await
    .map_err(|e| {
        error!("Failed to send ping: {}", e);
        anyhow::Error::new(e)
    })?;

    debug!("Ping sent");
    Ok(())
}

async fn websocket_writer_and_stream<'a> (config: WebSocketFluvioConfig) -> Result<(
    SplitSink<WebSocketStream<Transport>, Message>, 
    LocalBoxStream<'a, String>
)> {
    let ws_stream = establish_connection(config).await
    .context("Failed to establish WebSocket connection")?;

    let (write_half, read_half) = futures::stream::StreamExt::split(ws_stream);
    let stream = futures::stream::StreamExt::filter_map(read_half, |message_result| {
        async move {
            match message_result {
                Ok(message) => {
                    match message {
                        Message::Text(text) => {
                            info!("Got message: {}", text);
                            Some(text)
                        }
                        Message::Binary(data) => {
                            if let Ok(text) = String::from_utf8(data) {
                                Some(text)
                            } else {
                                error!("Received binary data that could not be converted to UTF-8 text");
                                None
                            }
                        }

                        Message::Ping(_) | Message::Pong(_) => {
                            // upon receiving ping messages tungstenite queues pong replies automatically
                            debug!("Received ping/pong message, connection is alive");
                            None
                        }
                        Message::Close(_) => {
                            info!("Received WebSocket Close frame");
                            None
                        }
                        _ => {
                            // Ignore other message types
                            None
                        }
                    }
                }
                Err(e) => {
                    error!("WebSocket read error: {}", e);
                    // Depending on the error you may choose to stop and close or try to reconnect
                    None
                }
            }
        }
    });

    Ok((write_half, futures::stream::StreamExt::boxed_local(stream)))
}

impl WebSocketFluvioSource {
    pub(crate) fn new(config: &WebSocketFluvioConfig) -> Result<Self> {
        Ok(Self {
            config: config.clone()
        })
    }

    async fn reconnect_and_run<'a> (self) -> Result<LocalBoxStream<'a, String>> {
        enum StreamElement {
            Read(String),
            PingInterval
        }

        let config_clone = self.config.clone();

        let repeated_websocket = Box::pin(async_stream::stream! {
            loop {
                let ping_interval = config_clone.ping_interval_ms.map(|val| val as u64).unwrap_or(10_000);

                let ws_stream_result = websocket_writer_and_stream(config_clone.clone()).await;
                if ws_stream_result.is_err() {
                    break;
                }
                let (mut write_end, ws_stream) = ws_stream_result.unwrap();

                let mut ws_stream = ws_stream
                    .map(|s| StreamElement::Read(s))
                    .merge(IntervalStream::new(tokio::time::interval(Duration::from_millis(ping_interval))).map(|_| StreamElement::PingInterval));

                while let Some(item) = ws_stream.next().await {
                    match item {
                        StreamElement::Read(s) => yield s,
                        StreamElement::PingInterval => {
                            let ping_res = ping(&mut write_end).await;
                            if ping_res.is_err() { break; }
                        }
                    }
                }
            }
        });
        
        Ok(futures::stream::StreamExt::boxed_local(repeated_websocket))
    }
}

#[async_trait]
impl<'a> Source<'a, String> for WebSocketFluvioSource {
    async fn connect(self, _offset: Option<Offset>) -> Result<LocalBoxStream<'a, String>> {
        self
            .reconnect_and_run()
            .await
            .context("Failed to run WebSocket connection")
    }
}
