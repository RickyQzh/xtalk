//! xtalk-events — session event types for the Rust runtime.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMeta {
    pub session_id: String,
    pub timestamp: f64,
}

impl EventMeta {
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            timestamp: now_ts(),
        }
    }
}

pub fn now_ts() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Event {
    #[serde(rename = "audio.frame_received")]
    AudioFrameReceived {
        meta: EventMeta,
        audio_data: Vec<u8>,
        sample_rate: u32,
    },
    #[serde(rename = "vad.speech_start")]
    VadSpeechStart { meta: EventMeta, origin: String },
    #[serde(rename = "vad.speech_end")]
    VadSpeechEnd { meta: EventMeta, origin: String },
    #[serde(rename = "asr.result_partial")]
    AsrResultPartial {
        meta: EventMeta,
        text: String,
        display_text: String,
        speech_pause: bool,
    },
    #[serde(rename = "asr.result_final")]
    AsrResultFinal {
        meta: EventMeta,
        text: String,
        display_text: String,
        speech_pause: bool,
    },
    #[serde(rename = "response.update")]
    ResponseUpdate { meta: EventMeta, text: String },
    #[serde(rename = "response.finish")]
    ResponseFinish { meta: EventMeta, text: String },
    #[serde(rename = "tts.started")]
    TtsStarted { meta: EventMeta },
    #[serde(rename = "tts.stopped")]
    TtsStopped { meta: EventMeta },
    #[serde(rename = "tts.finished")]
    TtsFinished { meta: EventMeta },
    #[serde(rename = "tts.chunk_ready")]
    TtsChunkReady {
        meta: EventMeta,
        audio_chunk: Vec<u8>,
        sample_rate: u32,
    },
    #[serde(rename = "tts.playback_finished")]
    TtsPlaybackFinished { meta: EventMeta },
    #[serde(rename = "turn.tts_stop_requested")]
    TurnTtsStopRequested { meta: EventMeta },
    #[serde(rename = "turn.llm_agent_stop_requested")]
    TurnLlmAgentStopRequested { meta: EventMeta },
    #[serde(rename = "turn.asr_start_requested")]
    TurnAsrStartRequested { meta: EventMeta },
    #[serde(rename = "turn.asr_end_requested")]
    TurnAsrEndRequested { meta: EventMeta },
    #[serde(rename = "llm_agent.consume_generation_requested")]
    LlmAgentConsumeGenerationRequested {
        meta: EventMeta,
        context_type: String,
        text: String,
    },
    #[serde(rename = "error.occurred")]
    ErrorOccurred {
        meta: EventMeta,
        error_message: String,
    },
    #[serde(rename = "session.config_received")]
    SessionConfigReceived { meta: EventMeta, config: Value },
    Extension {
        meta: EventMeta,
        type_name: String,
        payload: Value,
    },
}

impl Event {
    pub fn type_name(&self) -> &str {
        match self {
            Event::AudioFrameReceived { .. } => "audio.frame_received",
            Event::VadSpeechStart { .. } => "vad.speech_start",
            Event::VadSpeechEnd { .. } => "vad.speech_end",
            Event::AsrResultPartial { .. } => "asr.result_partial",
            Event::AsrResultFinal { .. } => "asr.result_final",
            Event::ResponseUpdate { .. } => "response.update",
            Event::ResponseFinish { .. } => "response.finish",
            Event::TtsStarted { .. } => "tts.started",
            Event::TtsStopped { .. } => "tts.stopped",
            Event::TtsFinished { .. } => "tts.finished",
            Event::TtsChunkReady { .. } => "tts.chunk_ready",
            Event::TtsPlaybackFinished { .. } => "tts.playback_finished",
            Event::TurnTtsStopRequested { .. } => "turn.tts_stop_requested",
            Event::TurnLlmAgentStopRequested { .. } => "turn.llm_agent_stop_requested",
            Event::TurnAsrStartRequested { .. } => "turn.asr_start_requested",
            Event::TurnAsrEndRequested { .. } => "turn.asr_end_requested",
            Event::LlmAgentConsumeGenerationRequested { .. } => {
                "llm_agent.consume_generation_requested"
            }
            Event::ErrorOccurred { .. } => "error.occurred",
            Event::SessionConfigReceived { .. } => "session.config_received",
            Event::Extension { type_name, .. } => type_name.as_str(),
        }
    }

    pub fn meta(&self) -> &EventMeta {
        match self {
            Event::AudioFrameReceived { meta, .. }
            | Event::VadSpeechStart { meta, .. }
            | Event::VadSpeechEnd { meta, .. }
            | Event::AsrResultPartial { meta, .. }
            | Event::AsrResultFinal { meta, .. }
            | Event::ResponseUpdate { meta, .. }
            | Event::ResponseFinish { meta, .. }
            | Event::TtsStarted { meta }
            | Event::TtsStopped { meta }
            | Event::TtsFinished { meta }
            | Event::TtsChunkReady { meta, .. }
            | Event::TtsPlaybackFinished { meta }
            | Event::TurnTtsStopRequested { meta }
            | Event::TurnLlmAgentStopRequested { meta }
            | Event::TurnAsrStartRequested { meta }
            | Event::TurnAsrEndRequested { meta }
            | Event::LlmAgentConsumeGenerationRequested { meta, .. }
            | Event::ErrorOccurred { meta, .. }
            | Event::SessionConfigReceived { meta, .. }
            | Event::Extension { meta, .. } => meta,
        }
    }
}
