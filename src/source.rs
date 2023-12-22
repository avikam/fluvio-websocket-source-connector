use crate::config::WebSocketFluvioConfig;
use anyhow::{Result,Context};
use async_trait::async_trait;
use fluvio::Offset;
use fluvio_connector_common::{tracing::{error, debug, info}, Result as FluvioResult, Source};
use futures::stream::LocalBoxStream;
use futures::{stream::{ SplitStream, SplitSink}, StreamExt, SinkExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, MaybeTlsStream, tungstenite::protocol::Message, WebSocketStream};


pub(crate) struct WebSocketFluvioSource {
    config: WebSocketFluvioConfig,
    stop_tx: Option<Sender<()>>,
}

impl WebSocketFluvioSource {
    pub(crate) fn new(config: &WebSocketFluvioConfig) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            stop_tx: None,
        })
    }

    async fn establish_connection(&self) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let mut attempt = 0;
        while let Some(reconnection_policy) = &self.config.reconnection_policy {
            match connect_async(&self.config.uri).await {
                Ok((mut ws_stream, _)) => {
                    info!("WebSocket connected to {}", self.config.uri);
                    if let Some(message) = &self.config.subscription_message {
                        ws_stream.send(Message::Text(message.clone())).await?;
                    }
                    return Ok(ws_stream);
                }
                Err(e) => {
                    error!("WebSocket connection error on attempt {}: {}", attempt, e);
                    if attempt < reconnection_policy.max_retries {
                        let delay = Self::compute_backoff(attempt, reconnection_policy.base_delay_ms as usize, reconnection_policy.max_delay_ms as usize);
                        sleep(Duration::from_millis(delay as u64)).await;
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

    async fn read_messages(
        mut read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        tx: Sender<String>,
        stop_tx: Sender<()>
    ) {
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
                        Message::Pong(_) => {
                            debug!("Received pong message, connection is alive");
                        }
                        Message::Ping(_) => {
                            if let Err(e) = tx.send("Ping received".into()).await {
                                error!("Failed to send ping response: {}", e);
                            }
                        }
                        Message::Close(_) => {
                            info!("Received WebSocket Close frame");
                            // Signal to stop and break the loop
                            let _ = stop_tx.send(()).await;
                            break;
                        }
                        _ => {
                            // Ignore other message types
                        }
                    }
                }
                Err(e) => {
                    error!("WebSocket read error: {}", e);
                    // Depending on the error you may choose to stop and close or try to reconnect
                    break;
                }
            }
        }
    }

    // async fn send_pong(&self, write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>) -> Result<()> {
    //     write.send(Message::Pong(Vec::new())).await.context("Failed to send pong")
    // }


    async fn ping_interval_task(
        mut write: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        config: WebSocketFluvioConfig,
        stop_tx: Sender<()>
    ) {
        if let Some(ping_interval) = config.ping_interval_ms {
            let mut interval = tokio::time::interval(Duration::from_millis(ping_interval as u64));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                interval.tick().await;
                match write.send(Message::Ping(Vec::new())).await {
                    Ok(_) => {
                        debug!("Ping sent");
                    }
                    Err(e) => {
                        error!("Failed to send ping: {}", e);
                        let _ = stop_tx.send(()).await; // Signal a stop due to ping failure
                        break;
                    }
                }
            }
        }
    }


    async fn reconnect_and_run(&mut self) -> Result<Receiver<String>> {
        let (tx, rx) = mpsc::channel::<String>(self.config.max_message_size.unwrap_or(1024));

        let config = self.config.clone();

        loop {
            let ws_stream = self.establish_connection().await.context("Failed to establish WebSocket connection")?;
            let (write_half, read_half) = ws_stream.split();
            let read_tx = tx.clone();
            let read_stop_tx = self.stop_tx.clone().unwrap();
            let config_clone = config.clone();

            let read_handle = tokio::spawn(async move {
                WebSocketFluvioSource::read_messages(
                    read_half,
                    read_tx,
                    read_stop_tx,
                )
                .await;
            });

            let write_stop_tx = self.stop_tx.clone().unwrap();
            let ping_handle = tokio::spawn(async move {
                WebSocketFluvioSource::ping_interval_task(write_half, config_clone, write_stop_tx).await;
            });

            // Wait for any of the tasks to finish. If either finishes, we have to reconnect.
            tokio::select! {
                _ = read_handle => {
                    error!("WebSocket read task ended, reconnecting...");
                },
                _ = ping_handle => {
                    error!("WebSocket ping task ended, reconnecting...");
                },
            };

            // Should not reconnect if reconnection policy is not defined.
            if config.reconnection_policy.is_none() {
                debug!("Reconnection not configured, ending the source.");
                break;
            }

            // Apply the backoff strategy before attempting to reconnect.
            if let Some(ref policy) = config.reconnection_policy {
                let delay = Self::compute_backoff(0 /* reset or implement retry counter */, (policy.base_delay_ms as u64).try_into().unwrap(), (policy.max_delay_ms as u64).try_into().unwrap());
                sleep(Duration::from_millis(delay.try_into().unwrap())).await;
            }
        }

        Ok(rx)
    }

    // Computes the backoff delay using an exponential strategy
    fn compute_backoff(attempt: usize, base_delay_ms: usize, max_delay_ms: usize) -> usize {
        let exponent = 2usize.pow(attempt.min(31) as u32); // Prevent overflow, cap exponent at 2^31
        let delay = base_delay_ms.saturating_mul(exponent);
        delay.min(max_delay_ms)
    }
}



#[async_trait]
impl<'a> Source<'a, String> for WebSocketFluvioSource {
    async fn connect(mut self, _offset: Option<Offset>) -> FluvioResult<LocalBoxStream<'a, String>> {
        let rx = self
            .reconnect_and_run()
            .await
            .context("Failed to run WebSocket connection")?;
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx)
            .boxed();
        Ok(stream)
    }
}
