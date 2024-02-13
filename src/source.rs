use crate::config::WebSocketFluvioConfig;
use anyhow::{Result,Context};
use async_trait::async_trait;
use fluvio::Offset;
use fluvio_connector_common::{tracing::{error, debug, info}, Source};
use futures::{self, stream::LocalBoxStream, SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, MaybeTlsStream, tungstenite::protocol::Message, WebSocketStream};

pub(crate) struct WebSocketFluvioSource {
    config: WebSocketFluvioConfig,
}

// Computes the backoff delay using an exponential strategy
fn compute_backoff(attempt: usize, base_delay_ms: usize, max_delay_ms: usize) -> usize {
    let exponent = 2usize.pow(attempt.min(31) as u32); // Prevent overflow, cap exponent at 2^31
    let delay = base_delay_ms.saturating_mul(exponent);
    delay.min(max_delay_ms)
}

async fn establish_connection(config: WebSocketFluvioConfig) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    let mut attempt = 0;
    if let Some(ref reconnection_policy) = config.reconnection_policy {
        while attempt < reconnection_policy.max_retries {
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
                    let delay = compute_backoff(attempt, reconnection_policy.base_delay_ms as usize, reconnection_policy.max_delay_ms as usize);
                    sleep(Duration::from_millis(delay as u64)).await;
                    attempt += 1;
                }
            }
        }
    }

    Err(anyhow::Error::new(
        std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to establish WebSocket connection after several attempts",
        )
    ))
}

async fn websocket_stream<'a> (config: WebSocketFluvioConfig) -> Result<LocalBoxStream<'a, String>> {
    let ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>> = establish_connection(config).await
    .context("Failed to establish WebSocket connection")?;

    let (_write_half, read_half) = ws_stream.split();
    let stream = read_half.filter_map(|message_result| {
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
                            // Signal to stop and break the loop
                            // let _ = stop_tx.send(()).await;
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
                    Some("Error".to_string())
                }
            }
        }
    });
    Ok(stream.boxed_local())
}

impl WebSocketFluvioSource {
    pub(crate) fn new(config: &WebSocketFluvioConfig) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            // stop_tx: None,
        })
    }

    // async fn send_pong(&self, write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>) -> Result<()> {
    //     write.send(Message::Pong(Vec::new())).await.context("Failed to send pong")
    // }


    // async fn ping_interval_task(
    //     mut write: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    //     config: WebSocketFluvioConfig,
    //     // stop_tx: Sender<()>
    // ) -> LocalBoxStream<'static, ()> {
    //     let ping_interval = config.ping_interval_ms.unwrap_or(10_000);

    //     let mut interval = tokio::time::interval(Duration::from_millis(ping_interval as u64));
    //     interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    //     let interval_stream = IntervalStream::new(tokio::time::interval(Duration::from_millis(ping_interval as u64)));
    //     interval_stream.map(move |_| {
    //         async move {
    //             match write.send(Message::Ping(Vec::new())).await {
    //                 Ok(_) => {
    //                     debug!("Ping sent");
    //                 }
    //                 Err(e) => {
    //                     error!("Failed to send ping: {}", e);
    //                     // let _ = stop_tx.send(()).await; // Signal a stop due to ping failure
    //                     // break;
    //                 }
    //             };
    //         };
    //         ()
    //     }).boxed_local()
    // }

    async fn reconnect_and_run<'a> (self) -> Result<LocalBoxStream<'a, String>> {
        let config_clone = self.config.clone();

        let repeated_websocket = Box::pin(async_stream::stream! {
            loop {
                let mut ws_stream = websocket_stream(config_clone.clone())
                .await
                .unwrap();
                while let Some(item) = ws_stream.next().await {
                    yield item;
                }
            }
        });
        
        Ok(repeated_websocket.boxed_local())
    }
}



#[async_trait]
impl<'a> Source<'a, String> for WebSocketFluvioSource {
    async fn connect(mut self, _offset: Option<Offset>) -> Result<LocalBoxStream<'a, String>> {
        self
            .reconnect_and_run()
            .await
            .context("Failed to run WebSocket connection")
    }
}
