use fluvio_connector_common::connector;
use serde::Deserialize;

#[connector(config, name = "websocket")]
#[derive(Debug, Clone)]
pub(crate) struct WebSocketFluvioConfig {
    pub(crate) uri: String,
    pub(crate) subscription_message: Option<String>,
    pub(crate) reconnection_policy: Option<ReconnectionPolicy>,
    pub(crate) max_message_size: Option<usize>,
    pub(crate) ping_interval_ms: Option<usize>,

}


#[derive(Debug, Deserialize, Clone)]
pub(crate) struct ReconnectionPolicy {
    pub(crate) max_retries: usize,
    pub(crate) base_delay_ms: usize,
    pub(crate) max_delay_ms: usize,
}
