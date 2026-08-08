//! Server JSON configuration and pipeline construction.

use serde::Deserialize;
use serde_json::Value;
use std::str::FromStr;
use thiserror::Error;
use xtalk_models::{Agent, DummyAgent, DummyAsr, DummyTts, DummyVad, HttpChatAgent};
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
    #[error("unsupported {slot} model type `{type_name}`")]
    UnsupportedType {
        slot: &'static str,
        type_name: String,
    },
    #[error("missing or invalid param `{param}` for {slot}")]
    InvalidParam {
        slot: &'static str,
        param: &'static str,
    },
    #[error("environment variable `{name}` is not set (referenced as `${{{name}}}` in config)")]
    MissingEnv { name: String },
}

impl FromStr for ServerConfig {
    type Err = ConfigError;

    /// Parse config from a JSON string.
    fn from_str(json: &str) -> Result<Self, Self::Err> {
        Ok(serde_json::from_str(json)?)
    }
}

impl ServerConfig {
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

/// Expand `${VAR}` placeholders using process environment variables.
pub fn expand_env(input: &str) -> Result<String, ConfigError> {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            let start = i + 2;
            let Some(end_rel) = input[start..].find('}') else {
                out.push_str(&input[i..]);
                break;
            };
            let end = start + end_rel;
            let name = &input[start..end];
            if name.is_empty() {
                return Err(ConfigError::MissingEnv {
                    name: String::new(),
                });
            }
            let value = std::env::var(name).map_err(|_| ConfigError::MissingEnv {
                name: name.to_string(),
            })?;
            out.push_str(&value);
            i = end + 1;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    Ok(out)
}

fn param_string(
    spec: &ModelSpec,
    slot: &'static str,
    key: &'static str,
) -> Result<String, ConfigError> {
    let raw = spec
        .params
        .get(key)
        .and_then(|v| v.as_str())
        .ok_or(ConfigError::InvalidParam { slot, param: key })?;
    expand_env(raw)
}

/// Build a [`DefaultPipeline`] from supported model specs.
pub fn build_pipeline(cfg: &ServerConfig) -> Result<DefaultPipeline, ConfigError> {
    let asr = build_dummy_asr(&cfg.asr)?;
    let tts = build_dummy_tts(&cfg.tts)?;
    let agent = build_agent(&cfg.llm_agent)?;

    let mut builder = DefaultPipeline::builder()
        .asr(Box::new(asr))
        .tts(Box::new(tts))
        .agent(agent);

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
        Some(Value::Number(n)) => n.as_u64().ok_or(ConfigError::InvalidParam {
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

fn build_agent(spec: &ModelSpec) -> Result<Box<dyn Agent>, ConfigError> {
    match spec.type_name.as_str() {
        "dummy" => {
            let default_response = spec
                .params
                .get("default_response")
                .and_then(|v| v.as_str())
                .unwrap_or("Dummy agent reply.");
            Ok(Box::new(DummyAgent::new(default_response)))
        }
        "openai_compat" => {
            let base_url = param_string(spec, "llm_agent", "base_url")?;
            let api_key = param_string(spec, "llm_agent", "api_key")?;
            let model = param_string(spec, "llm_agent", "model")?;
            Ok(Box::new(HttpChatAgent::new(base_url, api_key, model)))
        }
        other => Err(ConfigError::UnsupportedType {
            slot: "llm_agent",
            type_name: other.to_string(),
        }),
    }
}

fn build_dummy_vad(spec: &ModelSpec) -> Result<DummyVad, ConfigError> {
    require_dummy("vad", spec)?;
    Ok(DummyVad::new())
}

#[cfg(test)]
mod tests {
    use super::{build_pipeline, expand_env, ConfigError, ServerConfig};
    use std::str::FromStr;
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

    #[test]
    fn expand_env_substitutes_and_errors_when_missing() {
        std::env::set_var("XTALK_TEST_EXPAND_ENV", "secret-value");
        assert_eq!(
            expand_env("Bearer ${XTALK_TEST_EXPAND_ENV}").unwrap(),
            "Bearer secret-value"
        );
        match expand_env("${XTALK_TEST_EXPAND_ENV_MISSING_XYZ}") {
            Err(ConfigError::MissingEnv { name }) => {
                assert_eq!(name, "XTALK_TEST_EXPAND_ENV_MISSING_XYZ");
            }
            other => panic!("expected MissingEnv, got {other:?}"),
        }
        std::env::remove_var("XTALK_TEST_EXPAND_ENV");
    }

    #[test]
    fn builds_openai_compat_agent_with_env_expansion() {
        std::env::set_var("OPENAI_API_KEY", "sk-test");
        let json = r#"{
            "asr": { "type": "dummy", "params": {} },
            "tts": { "type": "dummy", "params": {} },
            "llm_agent": {
                "type": "openai_compat",
                "params": {
                    "base_url": "https://api.openai.com/v1",
                    "api_key": "${OPENAI_API_KEY}",
                    "model": "gpt-4o-mini"
                }
            }
        }"#;
        let cfg = ServerConfig::from_str(json).unwrap();
        let pipeline = build_pipeline(&cfg).expect("openai_compat pipeline");
        assert!(pipeline.agent().is_some());
        std::env::remove_var("OPENAI_API_KEY");
    }

    #[test]
    fn openai_compat_missing_env_fails_clearly() {
        std::env::remove_var("OPENAI_API_KEY");
        let json = r#"{
            "asr": { "type": "dummy", "params": {} },
            "tts": { "type": "dummy", "params": {} },
            "llm_agent": {
                "type": "openai_compat",
                "params": {
                    "base_url": "https://api.openai.com/v1",
                    "api_key": "${OPENAI_API_KEY}",
                    "model": "gpt-4o-mini"
                }
            }
        }"#;
        let cfg = ServerConfig::from_str(json).unwrap();
        match build_pipeline(&cfg) {
            Err(ConfigError::MissingEnv { name }) => assert_eq!(name, "OPENAI_API_KEY"),
            Ok(_) => panic!("expected MissingEnv, got Ok(_)"),
            Err(err) => panic!("expected MissingEnv, got {err}"),
        }
    }
}
