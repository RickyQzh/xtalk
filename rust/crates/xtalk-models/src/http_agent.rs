//! OpenAI-compatible HTTP chat agent with SSE streaming.

use crate::agent::{Agent, AgentContext};
use crate::error::ModelError;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

/// Chat agent that calls an OpenAI-compatible `/chat/completions` endpoint.
pub struct HttpChatAgent {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    model: String,
}

impl HttpChatAgent {
    /// Create a new agent.
    ///
    /// `base_url` should look like `https://api.openai.com/v1` (no trailing slash required).
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: api_key.into(),
            model: model.into(),
        }
    }

    fn completions_url(&self) -> String {
        format!("{}/chat/completions", self.base_url)
    }

    async fn read_sse_deltas(
        &self,
        mut stream: impl StreamExt<Item = Result<Bytes, reqwest::Error>> + Unpin,
        cancel: &CancellationToken,
    ) -> Result<Vec<String>, ModelError> {
        let mut buffer = String::new();
        // Phase 2: coalesce all SSE content deltas into one response part so
        // LlmAgentGenerationManager emits a single ResponseUpdate (not token fragments).
        let mut joined = String::new();

        loop {
            if cancel.is_cancelled() {
                return Err(ModelError::Cancelled);
            }

            let next = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    return Err(ModelError::Cancelled);
                }
                chunk = stream.next() => chunk,
            };

            match next {
                None => break,
                Some(Err(err)) => {
                    return Err(ModelError::message(format!("SSE stream error: {err}")));
                }
                Some(Ok(bytes)) => {
                    buffer.push_str(&String::from_utf8_lossy(&bytes));
                    while let Some(idx) = buffer.find('\n') {
                        let mut line = buffer[..idx].to_string();
                        buffer = buffer[idx + 1..].to_string();
                        if line.ends_with('\r') {
                            line.pop();
                        }
                        if let Some(content) = parse_sse_data_line(&line)? {
                            joined.push_str(&content);
                        }
                    }
                }
            }
        }

        // Flush a trailing line without newline, if any.
        if !buffer.trim().is_empty() {
            if let Some(content) = parse_sse_data_line(buffer.trim_end_matches('\r'))? {
                joined.push_str(&content);
            }
        }

        if joined.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(vec![joined])
        }
    }
}

/// Parse one SSE line. Returns `Ok(None)` for keep-alives / `[DONE]` / non-content deltas.
fn parse_sse_data_line(line: &str) -> Result<Option<String>, ModelError> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with(':') {
        return Ok(None);
    }
    let Some(data) = trimmed.strip_prefix("data:") else {
        return Ok(None);
    };
    let data = data.trim();
    if data.is_empty() || data == "[DONE]" {
        return Ok(None);
    }

    let value: Value = serde_json::from_str(data)
        .map_err(|err| ModelError::message(format!("invalid SSE JSON: {err}")))?;
    let content = value
        .pointer("/choices/0/delta/content")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    Ok(content)
}

#[async_trait]
impl Agent for HttpChatAgent {
    async fn accept(
        &self,
        ctx: AgentContext,
        cancel: CancellationToken,
    ) -> Result<Vec<String>, ModelError> {
        if cancel.is_cancelled() {
            return Err(ModelError::Cancelled);
        }

        match ctx.context_type.as_str() {
            "asr_final" | "embedding" => {}
            _ => return Ok(Vec::new()),
        }

        let body = json!({
            "model": self.model,
            "stream": true,
            "messages": [
                {"role": "user", "content": ctx.text}
            ]
        });

        // Abortable TTFB: cancel must interrupt a slow/hung `.send()`, not only
        // subsequent SSE chunk reads.
        let response = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                return Err(ModelError::Cancelled);
            }
            result = self
                .client
                .post(self.completions_url())
                .bearer_auth(&self.api_key)
                .json(&body)
                .send() => {
                result.map_err(|err| {
                    ModelError::message(format!("chat completions request failed: {err}"))
                })?
            }
        };

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ModelError::message(format!(
                "chat completions HTTP {status}: {text}"
            )));
        }

        let stream = response.bytes_stream();
        self.read_sse_deltas(stream, &cancel).await
    }

    fn clone_box(&self) -> Box<dyn Agent> {
        Box::new(HttpChatAgent {
            client: self.client.clone(),
            base_url: self.base_url.clone(),
            api_key: self.api_key.clone(),
            model: self.model.clone(),
        })
    }
}
