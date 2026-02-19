use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};

use soul_core::error::{SoulError, SoulResult};
use soul_core::gateway::{Gateway, GatewayEvent, GatewayMessage};

/// WhatsApp Business Cloud API gateway.
///
/// Requires:
/// - Meta Business Account with WhatsApp Business API access
/// - Permanent access token
/// - Phone number ID
/// - Webhook verification token (for incoming messages)
pub struct WhatsAppGateway {
    access_token: String,
    phone_number_id: String,
    verify_token: String,
    /// Webhook listen port (incoming messages)
    webhook_port: u16,
    client: reqwest::Client,
    shutdown_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

impl WhatsAppGateway {
    pub fn new(
        access_token: impl Into<String>,
        phone_number_id: impl Into<String>,
        verify_token: impl Into<String>,
    ) -> Self {
        Self {
            access_token: access_token.into(),
            phone_number_id: phone_number_id.into(),
            verify_token: verify_token.into(),
            webhook_port: 3000,
            client: reqwest::Client::new(),
            shutdown_tx: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_webhook_port(mut self, port: u16) -> Self {
        self.webhook_port = port;
        self
    }

    /// Parse incoming webhook payload from WhatsApp
    fn parse_webhook_payload(
        body: &serde_json::Value,
    ) -> Vec<(String, String, String)> {
        let mut messages = Vec::new();

        if let Some(entries) = body.get("entry").and_then(|v| v.as_array()) {
            for entry in entries {
                if let Some(changes) = entry.get("changes").and_then(|v| v.as_array()) {
                    for change in changes {
                        if let Some(value) = change.get("value") {
                            if let Some(msgs) =
                                value.get("messages").and_then(|v| v.as_array())
                            {
                                for msg in msgs {
                                    let from = msg
                                        .get("from")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("unknown");
                                    let text = msg
                                        .get("text")
                                        .and_then(|v| v.get("body"))
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("");

                                    if !text.is_empty() {
                                        messages.push((
                                            from.to_string(),
                                            from.to_string(),
                                            text.to_string(),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        messages
    }
}

#[async_trait::async_trait]
impl Gateway for WhatsAppGateway {
    fn name(&self) -> &str {
        "whatsapp"
    }

    async fn start(&self, event_tx: mpsc::UnboundedSender<GatewayEvent>) -> SoulResult<()> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        {
            let mut lock = self.shutdown_tx.lock().await;
            *lock = Some(shutdown_tx);
        }

        let verify_token = self.verify_token.clone();
        let port = self.webhook_port;

        // Spawn a minimal webhook HTTP server
        tokio::spawn(async move {
            use tokio::net::TcpListener;
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let listener = match TcpListener::bind(format!("0.0.0.0:{port}")).await {
                Ok(l) => l,
                Err(e) => {
                    let _ = event_tx.send(GatewayEvent::Error {
                        source: "whatsapp".into(),
                        message: format!("Failed to bind webhook port {port}: {e}"),
                    });
                    return;
                }
            };

            info!("WhatsApp webhook listening on port {port}");

            let mut shutdown_rx = shutdown_rx;

            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        info!("WhatsApp gateway shutting down");
                        break;
                    }
                    result = listener.accept() => {
                        match result {
                            Ok((mut stream, _addr)) => {
                                let mut buf = vec![0u8; 65536];
                                let n = match stream.read(&mut buf).await {
                                    Ok(n) => n,
                                    Err(_) => continue,
                                };
                                let request = String::from_utf8_lossy(&buf[..n]);

                                // Handle GET (verification)
                                if request.starts_with("GET") {
                                    if request.contains(&verify_token) {
                                        // Extract hub.challenge
                                        let challenge = request
                                            .split("hub.challenge=")
                                            .nth(1)
                                            .and_then(|s| s.split(&[' ', '&', '\r', '\n'][..]).next())
                                            .unwrap_or("");
                                        let response = format!(
                                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                                            challenge.len(),
                                            challenge
                                        );
                                        let _ = stream.write_all(response.as_bytes()).await;
                                    } else {
                                        let _ = stream
                                            .write_all(b"HTTP/1.1 403 Forbidden\r\n\r\n")
                                            .await;
                                    }
                                }
                                // Handle POST (incoming messages)
                                else if request.starts_with("POST") {
                                    // Extract JSON body after \r\n\r\n
                                    if let Some(body_start) = request.find("\r\n\r\n") {
                                        let body_str = &request[body_start + 4..];
                                        if let Ok(body) =
                                            serde_json::from_str::<serde_json::Value>(body_str)
                                        {
                                            let messages =
                                                WhatsAppGateway::parse_webhook_payload(&body);
                                            for (channel_id, sender, text) in messages {
                                                let _ = event_tx.send(
                                                    GatewayEvent::MessageReceived {
                                                        channel_id,
                                                        sender,
                                                        text,
                                                        metadata: serde_json::json!({
                                                            "platform": "whatsapp",
                                                        }),
                                                    },
                                                );
                                            }
                                        }
                                    }
                                    let _ = stream
                                        .write_all(b"HTTP/1.1 200 OK\r\n\r\n")
                                        .await;
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "WhatsApp webhook accept error");
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn send(&self, channel_id: &str, message: GatewayMessage) -> SoulResult<()> {
        let url = format!(
            "https://graph.facebook.com/v21.0/{}/messages",
            self.phone_number_id
        );

        match message {
            GatewayMessage::Text { text } => {
                let body = serde_json::json!({
                    "messaging_product": "whatsapp",
                    "to": channel_id,
                    "type": "text",
                    "text": {
                        "body": text,
                    }
                });

                let response = self
                    .client
                    .post(&url)
                    .header("Authorization", format!("Bearer {}", self.access_token))
                    .header("Content-Type", "application/json")
                    .json(&body)
                    .send()
                    .await
                    .map_err(|e| SoulError::Provider(format!("WhatsApp send error: {e}")))?;

                if !response.status().is_success() {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    return Err(SoulError::Provider(format!(
                        "WhatsApp API error {status}: {body}"
                    )));
                }
            }
        }

        Ok(())
    }

    async fn stop(&self) -> SoulResult<()> {
        let tx = {
            let mut lock = self.shutdown_tx.lock().await;
            lock.take()
        };

        if let Some(tx) = tx {
            let _ = tx.send(());
        }

        info!("WhatsApp gateway stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_name() {
        let gw = WhatsAppGateway::new("token", "phone_id", "verify");
        assert_eq!(gw.name(), "whatsapp");
    }

    #[test]
    fn custom_webhook_port() {
        let gw = WhatsAppGateway::new("token", "phone_id", "verify").with_webhook_port(8888);
        assert_eq!(gw.webhook_port, 8888);
    }

    #[test]
    fn parse_webhook_empty() {
        let body = serde_json::json!({});
        let messages = WhatsAppGateway::parse_webhook_payload(&body);
        assert!(messages.is_empty());
    }

    #[test]
    fn parse_webhook_with_message() {
        let body = serde_json::json!({
            "entry": [{
                "changes": [{
                    "value": {
                        "messages": [{
                            "from": "+1234567890",
                            "type": "text",
                            "text": { "body": "hello agent" }
                        }]
                    }
                }]
            }]
        });
        let messages = WhatsAppGateway::parse_webhook_payload(&body);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].0, "+1234567890");
        assert_eq!(messages[0].2, "hello agent");
    }

    #[test]
    fn parse_webhook_ignores_non_text() {
        let body = serde_json::json!({
            "entry": [{
                "changes": [{
                    "value": {
                        "messages": [{
                            "from": "+1234567890",
                            "type": "image",
                        }]
                    }
                }]
            }]
        });
        let messages = WhatsAppGateway::parse_webhook_payload(&body);
        assert!(messages.is_empty());
    }
}
