//! Outbound WebSocket JSON control-message encoding.

use serde::Serialize;

/// Well-known outbound action names (OutputGateway parity).
pub const SESSION_ATTACHED: &str = "session_attached";
pub const UPDATE_ASR: &str = "update_asr";
pub const FINISH_ASR: &str = "finish_asr";
pub const UPDATE_RESP: &str = "update_resp";
pub const FINISH_RESP: &str = "finish_resp";
pub const START_TTS: &str = "start_tts";
pub const STOP_TTS: &str = "stop_tts";
pub const TTS_FINISHED: &str = "tts_finished";
pub const ERROR: &str = "error";
pub const PONG: &str = "pong";

/// Encode an outbound control message as `{"action","data"}` JSON text.
pub fn outbound_action(action: &str, data: impl Serialize) -> String {
    let value = serde_json::json!({
        "action": action,
        "data": data,
    });
    value.to_string()
}
