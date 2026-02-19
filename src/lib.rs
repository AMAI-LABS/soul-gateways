//! # soul-gateways
//!
//! Messaging gateway implementations for soul-core agents.
//!
//! Provides pluggable messaging backends:
//! - **Telegram** (default feature) — Bot API via teloxide
//! - **iMessage** (macOS only) — AppleScript + chat.db
//! - **WhatsApp** — Business Cloud API

#[cfg(feature = "telegram")]
pub mod telegram;

#[cfg(feature = "imessage")]
pub mod imessage;

#[cfg(feature = "whatsapp")]
pub mod whatsapp;
