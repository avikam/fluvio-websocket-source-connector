use fluvio_connector_common::connector;
use serde::Deserialize;

#[connector(config, name = "websocket")]
#[derive(Debug, Clone)]
pub(crate) struct WebSocketFluvioConfig {
    pub(crate) uri: String,
    // pub(crate) protocols: Vec<String>,
    // pub(crate) connection_timeout_ms: Option<usize>,
    // pub(crate) headers: Option<std::collections::HashMap<String, String>>,
    pub(crate) subscription_message: Option<String>,
    // pub(crate) proxy: Option<String>,
    // pub(crate) ssl: Option<SslConfig>,
    pub(crate) reconnection_policy: Option<ReconnectionPolicy>,
    pub(crate) max_message_size: Option<usize>,
    // pub(crate) frame_size_limit: Option<usize>,
    pub(crate) ping_interval_ms: Option<usize>,
    // pub(crate) pong_timeout_ms: Option<usize>,
    // pub(crate) compression: Option<bool>,
    // pub(crate) origin_header: Option<String>,
    // pub(crate) binary_type: Option<String>,
    // pub(crate) session_management: Option<bool>,
    // pub(crate) throttling_policy: Option<String>,
    // pub(crate) qos: Option<QosConfig>,
    // pub(crate) ack_confirms: Option<bool>,
}

// #[derive(Debug, Deserialize, Clone)]
// pub(crate) struct SslConfig {
// pub(crate) verify: bool,
// pub(crate) ca_path: String,
// }

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct ReconnectionPolicy {
    // pub(crate) enabled: bool,
    pub(crate) max_retries: usize,
    // pub(crate) backoff_strategy: String,
    pub(crate) base_delay_ms: usize,
    pub(crate) max_delay_ms: usize,
}

// #[derive(Debug, Deserialize, Clone)]
// pub(crate) struct QosConfig {
//     pub(crate) delivery_guarantees: String,
// }
