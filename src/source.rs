use crate::{config::WebSocketConfig, event::WebSocketEvent};
use anyhow::{Result, Context};
use async_trait::async_trait;
use fluvio_connector_common::{tracing::{debug, error, info, warn}, Source};
use fluvio::Offset;
use futures::{stream::{BoxStream, SplitStream}, StreamExt, SinkExt};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use tokio_tungstenite::{connect_async, MaybeTlsStream, tungstenite::{Error as WsError, protocol::{Message, WebSocketConfig as WsConfig}}};
use tokio::net::TcpStream;

pub(crate) struct WebSocketSource {
    config: WebSocketConfig,
    stop_tx: Option<Sender<()>>,
}

impl WebSocketSource {
    pub(crate) fn new(config: &WebSocketConfig) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            stop_tx: None,
        })
    }

    async fn establish_connection(&self) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let mut attempt = 0;
        while let Some(reconnection_policy) = &self.config.reconnection_policy {
            match connect_async(&self.config.uri).await {
                Ok((ws_stream, _)) => {
                    info!("WebSocket connected to {}", self.config.uri);
                    if let Some(message) = &self.config.subscription_message {
                        ws_stream.send(Message::Text(message.clone())).await?;
                    }
                    return Ok(ws_stream);
                }
                Err(e) => {
                    error!("WebSocket connection error on attempt {}: {}", attempt, e);
                    if attempt < reconnection_policy.max_retries {
                        let delay = compute_backoff(attempt, reconnection_policy.base_delay_ms as u64, reconnection_policy.max_delay_ms as u64);
                        sleep(Duration::from_millis(delay)).await;
                        attempt += 1;
                    } else {
                        break;
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

    async fn read_messages(&self, mut read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>, tx: Sender<String>) {
        while let Some(message_result) = read.next().await {
            match message_result {
                Ok(message) => {
                    match message {
                        Message::Text(text) => {
                            if let Err(send_error) = tx.send(text).await {
                                error!("Error sending text event to channel: {}", send_error);
                            }
                        }
                        Message::Binary(data) => {
                            if let Ok(text) = String::from_utf8(data) {
                                if let Err(send_error) = tx.send(text).await {
                                    error!("Error sending binary event to channel: {}", send_error);
                                }
                            } else {
                                error!("Received binary data that could not be converted to UTF-8 text");
                            }
                        }
                        Message::Pong(_pong) => {
                            debug!("Received pong message, connection is alive");
                        }
                        Message::Ping(ping) => {
                            if let Err(e) = self.send_pong(&mut write).await {
                                error!("Failed to respond to ping: {}", e);
                                break; // Exit the reading loop, should consider reconnecting
                            }
                        }
                        Message::Close(_) => {
                            info!("Received WebSocket Close frame");
                            // Signal to stop and break the loop
                            if let Some(stop_tx) = self.stop_tx.as_ref() {
                                let _ = stop_tx.send(()).await;
                                break;
                            }
                        }
                        _ => {
                            // Ignore other message types
                        }
                    }
                }
                Err(e) => {
                    error!("WebSocket read error: {}", e);
                    // Depending on the error you may choose to stop and close or try to reconnect
                    return;
                }
            }
        }
    }

    async fn send_pong(&self, write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>) -> Result<()> {
        write.send(Message::Pong(Vec::new())).await.context("Failed to send pong")
    }


    async fn ping_interval_task(&self, mut write: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>) -> Result<()> {
        if let Some(ping_interval) = self.config.ping_interval_ms {
            let mut interval = tokio::time::interval(Duration::from_millis(ping_interval as u64));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            while interval.tick().await.is_elapsed() {
                if write.send(Message::Ping(Vec::new())).await.is_err() {
                    error!("Failed to send ping");
                    break; // If sending ping fails, break the loop and indicate exit
                }
            }
        }
        Ok(())
    }

    async fn reconnect_and_run(&self) -> Result<Receiver<String>> {
        let (tx, rx) = mpsc::channel::<String>(self.config.max_message_size.unwrap_or(1024));
        let (stop_tx, stop_rx) = mpsc::channel(1);
        self.stop_tx = Some(stop_tx);

        loop {
            let ws_stream = self.establish_connection().await.context("Failed to establish WebSocket connection")?;
            let (write, read) = ws_stream.split();

            // Start read and ping tasks concurrently.
            let read_handle = tokio::spawn(async move {
                WebSocketSource::read_messages(read, tx.clone()).await;
            });
            let ping_handle = tokio::spawn(async move {
                WebSocketSource::ping_interval_task(write).await;
            });

            // Wait for any of the tasks to finish. If either finishes, we have to reconnect.
            tokio::select! {
                _ = read_handle => {
                    error!("WebSocket read task ended, reconnecting...");
                },
                _ = ping_handle => {
                    error!("WebSocket ping task ended, reconnecting...");
                },
                _ = stop_rx.recv() => {
                    debug!("Stop signal received, closing the WebSocket connection cleanly.");
                    break;
                },
            };

            // Should not reconnect if reconnection policy is not defined.
            if self.config.reconnection_policy.is_none() {
                debug!("Reconnection not configured, ending the source.");
                break;
            }

            // Apply the backoff strategy before attempting to reconnect.
            if let Some(ref policy) = self.config.reconnection_policy {
                let delay = compute_backoff(0 /* reset or implement retry counter */, policy.base_delay_ms as u64, policy.max_delay_ms as u64);
                sleep(Duration::from_millis(delay)).await;
            }
        }

        Ok(rx)
    }

    // Computes the backoff delay using an exponential strategy
    fn compute_backoff(attempt: u32, base_delay_ms: u64, max_delay_ms: u64) -> u64 {
        let exponent = 2u64.pow(attempt.min(31)); // Prevent overflow, cap exponent at 2^31
        let delay = base_delay_ms.checked_mul(exponent).unwrap_or(u64::MAX);
        delay.min(max_delay_ms)
    }
}

#[async_trait]
impl<'a> Source<'a, String> for WebSocketSource {
    async fn connect(mut self, _offset: Option<Offset>) -> Result<BoxStream<'a, String>> {
        let rx = self.reconnect_and_run().await.context("Failed to run WebSocket connection")?;
        let stream = rx.boxed();
        Ok(stream)
    }
}
