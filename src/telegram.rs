use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info, warn};

use soul_core::error::{SoulError, SoulResult};
use soul_core::gateway::{Gateway, GatewayEvent, GatewayMessage};

const TELEGRAM_API: &str = "https://api.telegram.org";

/// Maximum retries for send operations (user-visible, must be reliable).
const SEND_MAX_RETRIES: u32 = 3;

/// Maximum retries for a single polling cycle before falling back to backoff sleep.
const POLL_MAX_RETRIES: u32 = 3;

/// Initial backoff delay for retries.
const INITIAL_BACKOFF_MS: u64 = 500;

/// Maximum backoff delay (cap for exponential growth).
const MAX_BACKOFF_MS: u64 = 30_000;

/// Telegram bot gateway using the Bot API directly via reqwest.
///
/// Uses long polling (`getUpdates`) for receiving messages and `sendMessage` for sending.
/// Both paths include exponential backoff retry logic that distinguishes retryable errors
/// (network failures, 429 rate limits, 5xx server errors) from fatal errors (4xx client errors).
pub struct TelegramGateway {
    token: String,
    /// Allowed usernames or user IDs (empty = allow all)
    allowed_users: Vec<String>,
    client: reqwest::Client,
    shutdown_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

impl TelegramGateway {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            allowed_users: Vec::new(),
            client: reqwest::Client::new(),
            shutdown_tx: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_allowed_users(mut self, users: Vec<String>) -> Self {
        self.allowed_users = users;
        self
    }

    pub fn is_user_allowed(&self, username: &str, user_id: &str) -> bool {
        self.allowed_users.is_empty()
            || self.allowed_users.contains(&username.to_string())
            || self.allowed_users.contains(&user_id.to_string())
    }

    fn api_url(&self, method: &str) -> String {
        format!("{}/bot{}/{}", TELEGRAM_API, self.token, method)
    }

    /// Send a single `sendChatAction typing` event to the given chat.
    /// Telegram shows the typing indicator for ~5s after each call.
    pub async fn send_typing(&self, channel_id: &str) -> SoulResult<()> {
        let chat_id = channel_id
            .parse::<i64>()
            .map_err(|e| SoulError::Provider(format!("Invalid chat_id: {e}")))?;
        let url = self.api_url("sendChatAction");
        let body = serde_json::json!({
            "chat_id": chat_id,
            "action": "typing",
        });
        // Best-effort — ignore errors (non-critical UX feature)
        let _ = post_with_retry(&self.client, &url, &body, 1, "sendChatAction").await;
        Ok(())
    }

    /// Spawn a background task that sends typing indicators every 4s until `cancel_rx` fires.
    /// Returns a `tokio::sync::oneshot::Sender` — drop or send on it to stop the loop.
    pub fn start_typing_loop(
        &self,
        channel_id: String,
    ) -> tokio::sync::oneshot::Sender<()> {
        let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let client = self.client.clone();
        let url = self.api_url("sendChatAction");

        tokio::spawn(async move {
            loop {
                let chat_id: i64 = match channel_id.parse() {
                    Ok(id) => id,
                    Err(_) => break,
                };
                let body = serde_json::json!({
                    "chat_id": chat_id,
                    "action": "typing",
                });
                let _ = post_with_retry(&client, &url, &body, 1, "sendChatAction").await;

                // Wait 4s or until cancelled
                tokio::select! {
                    _ = &mut cancel_rx => break,
                    _ = tokio::time::sleep(Duration::from_secs(4)) => {}
                }
            }
        });

        cancel_tx
    }
}

/// Whether an HTTP status code indicates a retryable error.
fn is_retryable_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS // 429
        || status.is_server_error() // 500-599
}

/// Compute exponential backoff delay for a given attempt (0-indexed).
/// Returns: INITIAL_BACKOFF_MS * 2^attempt, capped at MAX_BACKOFF_MS.
fn backoff_delay(attempt: u32) -> Duration {
    let ms = INITIAL_BACKOFF_MS.saturating_mul(1 << attempt.min(10));
    Duration::from_millis(ms.min(MAX_BACKOFF_MS))
}

/// Execute an HTTP POST with retry logic.
///
/// Retries on:
/// - Network/connection errors (reqwest::Error)
/// - HTTP 429 (rate limited)
/// - HTTP 5xx (server errors)
///
/// Does NOT retry on:
/// - HTTP 400 (bad request — will always fail)
/// - HTTP 401/403 (auth errors — token is wrong)
/// - HTTP 404 (endpoint doesn't exist)
///
/// Returns the successful response body as serde_json::Value, or the last error.
async fn post_with_retry(
    client: &reqwest::Client,
    url: &str,
    body: &serde_json::Value,
    max_retries: u32,
    context: &str,
) -> Result<serde_json::Value, String> {
    let mut last_error = String::new();

    for attempt in 0..=max_retries {
        if attempt > 0 {
            let delay = backoff_delay(attempt - 1);
            debug!(
                attempt,
                delay_ms = delay.as_millis() as u64,
                context,
                "Retrying Telegram API call"
            );
            tokio::time::sleep(delay).await;
        }

        let result = client.post(url).json(body).send().await;

        match result {
            Ok(response) => {
                let status = response.status();

                if status.is_success() {
                    match response.json::<serde_json::Value>().await {
                        Ok(data) => return Ok(data),
                        Err(e) => {
                            last_error = format!("JSON parse error: {e}");
                            warn!(
                                attempt,
                                error = %last_error,
                                context,
                                "Failed to parse Telegram response"
                            );
                            continue;
                        }
                    }
                }

                // Non-success status
                let response_body = response.text().await.unwrap_or_default();

                if is_retryable_status(status) {
                    last_error = format!("HTTP {status}: {response_body}");
                    warn!(
                        attempt,
                        status = %status,
                        context,
                        "Retryable Telegram API error"
                    );
                    continue;
                }

                // Fatal HTTP error — don't retry
                let msg = format!("Telegram API error {status}: {response_body}");
                error!(status = %status, context, "Fatal Telegram API error (not retrying)");
                return Err(msg);
            }
            Err(e) => {
                // Network error — always retryable
                last_error = format!("Network error: {e}");
                warn!(
                    attempt,
                    error = %e,
                    context,
                    "Telegram network error"
                );
                continue;
            }
        }
    }

    Err(format!(
        "{context}: all {max_retries} retries exhausted. Last error: {last_error}"
    ))
}

#[async_trait::async_trait]
impl Gateway for TelegramGateway {
    fn name(&self) -> &str {
        "telegram"
    }

    async fn start(&self, event_tx: mpsc::UnboundedSender<GatewayEvent>) -> SoulResult<()> {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        {
            let mut lock = self.shutdown_tx.lock().await;
            *lock = Some(shutdown_tx);
        }

        let client = self.client.clone();
        let token = self.token.clone();
        let allowed_users = self.allowed_users.clone();

        tokio::spawn(async move {
            let mut offset: i64 = 0;
            let base_url = format!("{}/bot{}", TELEGRAM_API, token);
            let mut consecutive_errors: u32 = 0;

            loop {
                let url = format!("{}/getUpdates", base_url);
                let body = serde_json::json!({
                    "offset": offset,
                    "timeout": 30,
                    "allowed_updates": ["message"],
                });

                tokio::select! {
                    _ = &mut shutdown_rx => {
                        info!("Telegram gateway shutting down");
                        break;
                    }
                    result = post_with_retry(&client, &url, &body, POLL_MAX_RETRIES, "getUpdates") => {
                        match result {
                            Ok(data) => {
                                consecutive_errors = 0;

                                if let Some(updates) = data.get("result").and_then(|v| v.as_array()) {
                                    for update in updates {
                                        if let Some(update_id) = update.get("update_id").and_then(|v| v.as_i64()) {
                                            offset = update_id + 1;
                                        }

                                        if let Some(message) = update.get("message") {
                                            let chat_id = message
                                                .get("chat")
                                                .and_then(|c| c.get("id"))
                                                .and_then(|v| v.as_i64())
                                                .map(|id| id.to_string())
                                                .unwrap_or_default();

                                            let username = message
                                                .get("from")
                                                .and_then(|f| f.get("username"))
                                                .and_then(|v| v.as_str())
                                                .unwrap_or("unknown");

                                            let user_id = message
                                                .get("from")
                                                .and_then(|f| f.get("id"))
                                                .and_then(|v| v.as_i64())
                                                .map(|id| id.to_string())
                                                .unwrap_or_default();

                                            if !allowed_users.is_empty()
                                                && !allowed_users.contains(&username.to_string())
                                                && !allowed_users.contains(&format!("@{}", username))
                                                && !allowed_users.contains(&user_id)
                                            {
                                                warn!(
                                                    username = %username,
                                                    user_id = %user_id,
                                                    "Ignoring message from unauthorized user"
                                                );
                                                continue;
                                            }

                                            if let Some(text) = message.get("text").and_then(|v| v.as_str()) {
                                                info!(
                                                    sender = %username,
                                                    chat_id = %chat_id,
                                                    text_len = text.len(),
                                                    "Message received"
                                                );
                                                let _ = event_tx.send(GatewayEvent::MessageReceived {
                                                    channel_id: chat_id,
                                                    sender: username.to_string(),
                                                    text: text.to_string(),
                                                    metadata: serde_json::json!({
                                                        "platform": "telegram",
                                                        "user_id": user_id,
                                                        "message_id": message.get("message_id"),
                                                    }),
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                consecutive_errors += 1;
                                error!(
                                    error = %e,
                                    consecutive_errors,
                                    "Telegram polling failed after retries"
                                );
                                let _ = event_tx.send(GatewayEvent::Error {
                                    source: "telegram".into(),
                                    message: e.clone(),
                                });

                                // Exponential backoff between polling cycles on persistent failure.
                                // Capped at 30s. Resets on first success.
                                let delay = backoff_delay(consecutive_errors.min(6));
                                warn!(
                                    delay_ms = delay.as_millis() as u64,
                                    consecutive_errors,
                                    "Backing off before next poll"
                                );
                                tokio::time::sleep(delay).await;
                            }
                        }
                    }
                }
            }
        });

        info!("Telegram gateway started");
        Ok(())
    }

    async fn send(&self, channel_id: &str, message: GatewayMessage) -> SoulResult<()> {
        let chat_id = channel_id
            .parse::<i64>()
            .map_err(|e| SoulError::Provider(format!("Invalid chat_id: {e}")))?;

        match message {
            GatewayMessage::Text { text } => {
                let chunks = split_message(&text, 4096);
                let total_chunks = chunks.len();

                for (i, chunk) in chunks.iter().enumerate() {
                    let url = self.api_url("sendMessage");
                    let body = serde_json::json!({
                        "chat_id": chat_id,
                        "text": chunk,
                    });

                    let context = if total_chunks > 1 {
                        format!("sendMessage chunk {}/{total_chunks}", i + 1)
                    } else {
                        "sendMessage".into()
                    };

                    post_with_retry(&self.client, &url, &body, SEND_MAX_RETRIES, &context)
                        .await
                        .map_err(|e| SoulError::Provider(e))?;

                    debug!(
                        chat_id,
                        chunk_index = i,
                        chunk_len = chunk.len(),
                        "Message chunk sent"
                    );
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

        info!("Telegram gateway stopped");
        Ok(())
    }
}

fn split_message(text: &str, max_len: usize) -> Vec<&str> {
    if text.len() <= max_len {
        return vec![text];
    }

    let mut chunks = Vec::new();
    let mut start = 0;

    while start < text.len() {
        let end = (start + max_len).min(text.len());
        let actual_end = if end < text.len() {
            text[start..end]
                .rfind('\n')
                .map(|pos| start + pos + 1)
                .unwrap_or(end)
        } else {
            end
        };
        chunks.push(&text[start..actual_end]);
        start = actual_end;
    }

    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_message_short() {
        let chunks = split_message("hello", 4096);
        assert_eq!(chunks, vec!["hello"]);
    }

    #[test]
    fn split_message_long() {
        let text = "a\nb\nc\nd\ne";
        let chunks = split_message(text, 4);
        assert!(!chunks.is_empty());
        for chunk in &chunks {
            assert!(chunk.len() <= 4);
        }
    }

    #[test]
    fn gateway_name() {
        let gw = TelegramGateway::new("test_token");
        assert_eq!(gw.name(), "telegram");
    }

    #[test]
    fn user_filtering_empty_allows_all() {
        let gw = TelegramGateway::new("token");
        assert!(gw.is_user_allowed("anyone", "123"));
    }

    #[test]
    fn user_filtering_by_username() {
        let gw = TelegramGateway::new("token")
            .with_allowed_users(vec!["@Gonzih".into(), "@maksim_amai".into()]);
        assert!(gw.is_user_allowed("@Gonzih", "123"));
        assert!(gw.is_user_allowed("@maksim_amai", "456"));
        assert!(!gw.is_user_allowed("@random", "789"));
    }

    #[test]
    fn user_filtering_by_id() {
        let gw = TelegramGateway::new("token")
            .with_allowed_users(vec!["12345".into()]);
        assert!(gw.is_user_allowed("unknown", "12345"));
        assert!(!gw.is_user_allowed("unknown", "99999"));
    }

    #[test]
    fn api_url_formats_correctly() {
        let gw = TelegramGateway::new("123:ABC");
        assert_eq!(
            gw.api_url("sendMessage"),
            "https://api.telegram.org/bot123:ABC/sendMessage"
        );
    }

    #[test]
    fn backoff_delay_exponential() {
        let d0 = backoff_delay(0);
        let d1 = backoff_delay(1);
        let d2 = backoff_delay(2);

        assert_eq!(d0.as_millis(), 500);
        assert_eq!(d1.as_millis(), 1000);
        assert_eq!(d2.as_millis(), 2000);
    }

    #[test]
    fn backoff_delay_caps_at_max() {
        let d_large = backoff_delay(20);
        assert!(d_large.as_millis() <= MAX_BACKOFF_MS as u128);
    }

    #[test]
    fn retryable_status_codes() {
        assert!(is_retryable_status(reqwest::StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retryable_status(reqwest::StatusCode::INTERNAL_SERVER_ERROR));
        assert!(is_retryable_status(reqwest::StatusCode::BAD_GATEWAY));
        assert!(is_retryable_status(reqwest::StatusCode::SERVICE_UNAVAILABLE));
        assert!(is_retryable_status(reqwest::StatusCode::GATEWAY_TIMEOUT));

        assert!(!is_retryable_status(reqwest::StatusCode::BAD_REQUEST));
        assert!(!is_retryable_status(reqwest::StatusCode::UNAUTHORIZED));
        assert!(!is_retryable_status(reqwest::StatusCode::FORBIDDEN));
        assert!(!is_retryable_status(reqwest::StatusCode::NOT_FOUND));
        assert!(!is_retryable_status(reqwest::StatusCode::OK));
    }
}
