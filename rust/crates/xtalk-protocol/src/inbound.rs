//! Inbound WebSocket JSON control-message parsing.

use serde_json::Value;

use crate::{InboundMessage, ProtocolError};

/// Parse a frontend text frame into an [`InboundMessage`].
///
/// Looks up `action`, falling back to `type` (Python InputGateway parity).
/// Unknown actions become [`InboundMessage::Unknown`] (not an error).
/// Only invalid JSON yields [`ProtocolError`].
pub fn parse_inbound_text(text: &str) -> Result<InboundMessage, ProtocolError> {
    let value: Value = serde_json::from_str(text)?;
    let action = value
        .get("action")
        .or_else(|| value.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    Ok(match action {
        "ping" => InboundMessage::Ping {
            timestamp: value
                .get("timestamp")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0),
        },
        "vad_speech_start" => InboundMessage::VadSpeechStart,
        "vad_speech_end" => InboundMessage::VadSpeechEnd,
        "tts_playback_finished" => InboundMessage::TtsPlaybackFinished,
        "tts_chunk_played" => InboundMessage::TtsChunkPlayed {},
        "session_config" => InboundMessage::SessionConfig(value),
        "clock_sync" => InboundMessage::ClockSync {
            client_send_ts: f64_field(&value, "client_send_ts"),
            server_recv_ts: f64_field(&value, "server_recv_ts"),
            client_recv_ts: f64_field(&value, "client_recv_ts"),
        },
        "change_voice" => InboundMessage::ChangeVoice {
            voice_name: string_field(&value, "voice_name"),
        },
        "change_emotion" => InboundMessage::ChangeEmotion {
            emotion_name: string_field(&value, "emotion_name"),
            emotion_vector: value
                .get("emotion_vector")
                .cloned()
                .unwrap_or_else(|| Value::Array(vec![])),
        },
        "change_tts_speed" => InboundMessage::ChangeTtsSpeed {
            speed: value.get("speed").and_then(|v| v.as_f64()).unwrap_or(1.0),
        },
        _ => InboundMessage::Unknown,
    })
}

fn f64_field(value: &Value, key: &str) -> f64 {
    value.get(key).and_then(|v| v.as_f64()).unwrap_or(0.0)
}

fn string_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}
