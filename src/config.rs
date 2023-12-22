use fluvio_connector_common::connector;
use serde::Deserialize;

#[connector(config, name = "websocket")]
#[derive(Debug, Deserialize)]
pub(crate) struct WebSocketConfig {
    pub uri: String,
    protocols: Vec<String>,
    connection_timeout_ms: Option<usize>,
    headers: Option<std::collections::HashMap<String, String>>,
    subscription_message: Option<String>,
    proxy: Option<String>,
    ssl: Option<SslConfig>,
    reconnection_policy: Option<ReconnectionPolicy>,
    max_message_size: Option<usize>,
    frame_size_limit: Option<usize>,
    ping_interval_ms: Option<usize>,
    pong_timeout_ms: Option<usize>,
    compression: Option<bool>,
    origin_header: Option<String>,
    binary_type: Option<String>,
    session_management: Option<bool>,
    throttling_policy: Option<String>,
    qos: Option<QosConfig>,
    ack_confirms: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SslConfig {
    verify: bool,
    ca_path: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ReconnectionPolicy {
    enabled: bool,
    max_retries: usize,
    backoff_strategy: String,
    base_delay_ms: usize,
    max_delay_ms: usize,
}

#[derive(Debug, Deserialize)]
pub(crate) struct QosConfig {
    delivery_guarantees: String,
}
