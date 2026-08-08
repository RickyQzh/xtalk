//! Server JSON configuration and dummy pipeline construction.

use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;
use xtalk_models::{DummyAgent, DummyAsr, DummyTts, DummyVad};
use xtalk_pipeline::DefaultPipeline;

/// One model slot in the server config (`"type"` + optional `params`).
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ModelSpec {
    #[serde(rename = "type")]
    pub type_name: String,
    #[serde(default)]
    pub params: Value,
}

/// Top-level server configuration loaded from JSON.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ServerConfig {
    pub asr: ModelSpec,
    pub tts: ModelSpec,
    pub llm_agent: ModelSpec,
    #[serde(default)]
    pub vad: Option<ModelSpec>,
    #[serde(default = "default_max_sessions")]
    pub max_sessions: usize,
    #[serde(default = "default_listen")]
    pub listen: String,
}

fn default_max_sessions() -> usize {
    32
}

fn default_listen() -> String {
    "0.0.0.0:11995".into()
}

/// Errors while parsing config or building a pipeline from it.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid JSON config: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("failed to read config `{path}`: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("unsupported {slot} model type `{type_name}` (Phase 1 supports `dummy`)")]
    UnsupportedType {
        slot: &'static str,
        type_name: String,
    },
    #[error("missing or invalid param `{param}` for {slot}")]
    InvalidParam {
        slot: &'static str,
        param: &'static str,
    },
}

impl ServerConfig {
    /// Parse config from a JSON string.
    pub fn from_str(json: &str) -> Result<Self, ConfigError> {
        Ok(serde_json::from_str(json)?)
    }

    /// Load config from a filesystem path.
    pub fn from_path(path: impl AsRef<std::path::Path>) -> Result<Self, ConfigError> {
        let path_ref = path.as_ref();
        let text = std::fs::read_to_string(path_ref).map_err(|source| ConfigError::Io {
            path: path_ref.display().to_string(),
            source,
        })?;
        Self::from_str(&text)
    }
}

/// Build a [`DefaultPipeline`] from supported model specs (`dummy` only for now).
pub fn build_pipeline(cfg: &ServerConfig) -> Result<DefaultPipeline, ConfigError> {
    let asr = build_dummy_asr(&cfg.asr)?;
    let tts = build_dummy_tts(&cfg.tts)?;
    let agent = build_dummy_agent(&cfg.llm_agent)?;

    let mut builder = DefaultPipeline::builder()
        .asr(Box::new(asr))
        .tts(Box::new(tts))
        .agent(Box::new(agent));

    if let Some(vad_spec) = &cfg.vad {
        let vad = build_dummy_vad(vad_spec)?;
        builder = builder.vad(Box::new(vad));
    }

    Ok(builder.build())
}

fn require_dummy(slot: &'static str, spec: &ModelSpec) -> Result<(), ConfigError> {
    if spec.type_name == "dummy" {
        Ok(())
    } else {
        Err(ConfigError::UnsupportedType {
            slot,
            type_name: spec.type_name.clone(),
        })
    }
}

fn build_dummy_asr(spec: &ModelSpec) -> Result<DummyAsr, ConfigError> {
    require_dummy("asr", spec)?;
    let default_text = spec
        .params
        .get("default_text")
        .and_then(|v| v.as_str())
        .unwrap_or("This is a dummy ASR test text");
    Ok(DummyAsr::new(default_text))
}

fn build_dummy_tts(spec: &ModelSpec) -> Result<DummyTts, ConfigError> {
    require_dummy("tts", spec)?;
    let sample_rate = match spec.params.get("sample_rate") {
        None => 48_000u32,
        Some(Value::Number(n)) => n
            .as_u64()
            .ok_or(ConfigError::InvalidParam {
                slot: "tts",
                param: "sample_rate",
            })? as u32,
        Some(_) => {
            return Err(ConfigError::InvalidParam {
                slot: "tts",
                param: "sample_rate",
            })
        }
    };
    Ok(DummyTts::new(sample_rate))
}

fn build_dummy_agent(spec: &ModelSpec) -> Result<DummyAgent, ConfigError> {
    require_dummy("llm_agent", spec)?;
    let default_response = spec
        .params
        .get("default_response")
        .and_then(|v| v.as_str())
        .unwrap_or("Dummy agent reply.");
    Ok(DummyAgent::new(default_response))
}

fn build_dummy_vad(spec: &ModelSpec) -> Result<DummyVad, ConfigError> {
    require_dummy("vad", spec)?;
    Ok(DummyVad::new())
}

#[cfg(test)]
mod tests {
    use super::{build_pipeline, ServerConfig};
    use xtalk_pipeline::Pipeline;

    #[test]
    fn parses_dummy_config() {
        let cfg = ServerConfig::from_str(include_str!(
            "../../../examples/dummy_server/config.dummy.json"
        ))
        .unwrap();
        assert_eq!(cfg.asr.type_name, "dummy");
    }

    #[test]
    fn builds_dummy_pipeline() {
        let cfg = ServerConfig::from_str(include_str!(
            "../../../examples/dummy_server/config.dummy.json"
        ))
        .unwrap();
        let pipeline = build_pipeline(&cfg).expect("build pipeline");
        assert!(pipeline.asr().is_some());
        assert!(pipeline.tts().is_some());
        assert!(pipeline.agent().is_some());
        assert!(pipeline.vad().is_some());
    }

    #[test]
    fn rejects_unknown_type() {
        let json = r#"{
            "asr": { "type": "nope", "params": {} },
            "tts": { "type": "dummy", "params": {} },
            "llm_agent": { "type": "dummy", "params": {} }
        }"#;
        let cfg = ServerConfig::from_str(json).unwrap();
        match build_pipeline(&cfg) {
            Ok(_) => panic!("expected unsupported type error"),
            Err(err) => {
                let msg = err.to_string();
                assert!(msg.contains("nope"), "{msg}");
            }
        }
    }
}
