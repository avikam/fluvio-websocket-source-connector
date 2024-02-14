use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WebSocketEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_bytes: Option<Vec<u8>>,
}

#[derive(Error, Debug)]
pub enum WebSocketEventError {
    #[error("Failed to process WebSocket message: {0}")]
    ProcessingError(String),

    #[error("Failed to establish a connection: {0}")]
    ConnectionError(String),
}

impl TryFrom<WebSocketEvent> for String {
    type Error = WebSocketEventError;

    fn try_from(event: WebSocketEvent) -> Result<Self, Self::Error> {
        if let Some(text) = event.message_text {
            Ok(text)
        } else if let Some(bytes) = event.message_bytes {
            String::from_utf8(bytes)
                .map_err(|e| WebSocketEventError::ProcessingError(e.to_string()))
        } else {
            Err(WebSocketEventError::ProcessingError(
                "No message content available".to_string(),
            ))
        }
    }
}
