//! xtalk-protocol — wire protocol encode/decode for the Rust runtime.

mod inbound;
mod outbound;

pub use inbound::parse_inbound_text;
pub use outbound::{
    outbound_action, ERROR, FINISH_ASR, FINISH_RESP, PONG, SESSION_ATTACHED, START_TTS, STOP_TTS,
    TTS_FINISHED, UPDATE_ASR, UPDATE_RESP,
};

use serde_json::Value;
use thiserror::Error;

/// Errors from protocol parse/encode.
#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("invalid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
}

/// Parsed inbound control message from the frontend.
#[derive(Debug, Clone, PartialEq)]
pub enum InboundMessage {
    Ping { timestamp: f64 },
    VadSpeechStart,
    VadSpeechEnd,
    TtsPlaybackFinished,
    TtsChunkPlayed {},
    SessionConfig(Value),
    ClockSync {
        client_send_ts: f64,
        server_recv_ts: f64,
        client_recv_ts: f64,
    },
    ChangeVoice { voice_name: String },
    ChangeEmotion {
        emotion_name: String,
        emotion_vector: Value,
    },
    ChangeTtsSpeed { speed: f64 },
    Unknown,
}

/// Crate identity helper used by workspace smoke checks.
pub fn crate_name() -> &'static str {
    "xtalk-protocol"
}
