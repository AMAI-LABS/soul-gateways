use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};

use soul_core::error::{SoulError, SoulResult};
use soul_core::gateway::{Gateway, GatewayEvent, GatewayMessage};

/// iMessage gateway for macOS — uses AppleScript for sending and chat.db polling for receiving.
///
/// Requires Full Disk Access for the process to read `~/Library/Messages/chat.db`.
pub struct IMessageGateway {
    /// Poll interval in seconds for checking new messages
    poll_interval_secs: u64,
    shutdown_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

impl IMessageGateway {
    pub fn new() -> Self {
        Self {
            poll_interval_secs: 2,
            shutdown_tx: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_poll_interval(mut self, secs: u64) -> Self {
        self.poll_interval_secs = secs;
        self
    }

    /// Send a message via AppleScript
    async fn send_via_applescript(recipient: &str, text: &str) -> SoulResult<()> {
        let script = format!(
            r#"tell application "Messages"
    set targetBuddy to buddy "{}" of service "iMessage"
    send "{}" to targetBuddy
end tell"#,
            recipient.replace('"', r#"\""#),
            text.replace('"', r#"\""#).replace('\n', r#"\n"#),
        );

        let output = tokio::process::Command::new("osascript")
            .arg("-e")
            .arg(&script)
            .output()
            .await
            .map_err(|e| SoulError::Provider(format!("osascript exec error: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SoulError::Provider(format!(
                "osascript error: {stderr}"
            )));
        }

        Ok(())
    }

    /// Read recent messages from chat.db
    /// Returns (rowid, chat_identifier, sender, text)
    #[cfg(feature = "imessage")]
    fn read_messages_since(
        last_rowid: i64,
    ) -> SoulResult<Vec<(i64, String, String, String)>> {
        let home = std::env::var("HOME")
            .map_err(|_| SoulError::Provider("HOME not set".into()))?;
        let db_path = format!("{}/Library/Messages/chat.db", home);

        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .map_err(|e| SoulError::Provider(format!("Cannot open chat.db: {e}")))?;

        let mut stmt = conn.prepare(
            "SELECT m.ROWID, c.chat_identifier, h.id, m.text
             FROM message m
             JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
             JOIN chat c ON c.ROWID = cmj.chat_id
             LEFT JOIN handle h ON h.ROWID = m.handle_id
             WHERE m.ROWID > ?1 AND m.is_from_me = 0 AND m.text IS NOT NULL
             ORDER BY m.ROWID ASC",
        )
        .map_err(|e| SoulError::Provider(format!("SQL prepare error: {e}")))?;

        let rows = stmt
            .query_map([last_rowid], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2).unwrap_or_else(|_| "unknown".into()),
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(|e| SoulError::Provider(format!("SQL query error: {e}")))?;

        let mut results = Vec::new();
        for row in rows {
            if let Ok(r) = row {
                results.push(r);
            }
        }

        Ok(results)
    }
}

impl Default for IMessageGateway {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Gateway for IMessageGateway {
    fn name(&self) -> &str {
        "imessage"
    }

    async fn start(&self, event_tx: mpsc::UnboundedSender<GatewayEvent>) -> SoulResult<()> {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        {
            let mut lock = self.shutdown_tx.lock().await;
            *lock = Some(shutdown_tx);
        }

        let poll_interval = self.poll_interval_secs;

        tokio::spawn(async move {
            let mut _last_rowid: i64 = 0;

            // Get the current max rowid to avoid processing old messages
            #[cfg(feature = "imessage")]
            {
                if let Ok(msgs) = IMessageGateway::read_messages_since(0) {
                    if let Some(last) = msgs.last() {
                        _last_rowid = last.0;
                    }
                }
            }

            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        info!("iMessage gateway shutting down");
                        break;
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_secs(poll_interval)) => {
                        #[cfg(feature = "imessage")]
                        {
                            match IMessageGateway::read_messages_since(_last_rowid) {
                                Ok(messages) => {
                                    for (rowid, chat_id, sender, text) in messages {
                                        _last_rowid = rowid;
                                        let _ = event_tx.send(GatewayEvent::MessageReceived {
                                            channel_id: chat_id,
                                            sender,
                                            text,
                                            metadata: serde_json::json!({
                                                "platform": "imessage",
                                                "rowid": rowid,
                                            }),
                                        });
                                    }
                                }
                                Err(e) => {
                                    warn!(error = %e, "iMessage polling error");
                                    let _ = event_tx.send(GatewayEvent::Error {
                                        source: "imessage".into(),
                                        message: e.to_string(),
                                    });
                                }
                            }
                        }

                        #[cfg(not(feature = "imessage"))]
                        {
                            // Without the imessage feature, this is a no-op poller
                            let _ = &event_tx;
                        }
                    }
                }
            }
        });

        info!("iMessage gateway started (poll interval: {}s)", poll_interval);
        Ok(())
    }

    async fn send(&self, channel_id: &str, message: GatewayMessage) -> SoulResult<()> {
        match message {
            GatewayMessage::Text { text } => {
                Self::send_via_applescript(channel_id, &text).await?;
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

        info!("iMessage gateway stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_name() {
        let gw = IMessageGateway::new();
        assert_eq!(gw.name(), "imessage");
    }

    #[test]
    fn custom_poll_interval() {
        let gw = IMessageGateway::new().with_poll_interval(10);
        assert_eq!(gw.poll_interval_secs, 10);
    }

    #[test]
    fn default_poll_interval() {
        let gw = IMessageGateway::default();
        assert_eq!(gw.poll_interval_secs, 2);
    }
}
