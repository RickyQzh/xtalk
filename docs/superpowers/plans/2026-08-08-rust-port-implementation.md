# X-Talk Rust 迁移翻译版 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在仓库内交付可运行的 Rust X-Talk 运行时：EventBus + Pipeline + 主路径 Managers + axum WebSocket 服务，用 Dummy 模型跑通与现有 sample 前端兼容的会话闭环，并接入 OpenAI-compatible HTTP Agent。

**Architecture:** 新增 `rust/` Cargo workspace，按设计规格分层：`xtalk-events` → `xtalk-bus` → `xtalk-models` → `xtalk-pipeline` → `xtalk-protocol` → `xtalk-serving` → `xtalk-server`。每会话一个 `Service`（独立 EventBus + Pipeline clone）；Manager 只通过事件通信；前端 TypeScript 不改，线协议兼容 Python `InputGateway`/`OutputGateway`。

**Tech Stack:** Rust 1.83+、Tokio、Axum、Serde/JSON、Bytes、Tokio-Tungstenite（经 Axum）、Reqwest（HTTP Agent）、Tracing；测试用 `tokio::test` + `tower`/`axum` 测试工具。

**Spec:** `docs/superpowers/specs/2026-08-08-rust-port-design.md`

## Global Constraints

- 仓库约定 commit 前缀：`feature:` / `docs:` / `refactor:` / `fix:` / `chore:`
- Python 树（`src/xtalk/`、`examples/` 现有内容）本计划内只读对照，不删除、不破坏
- 前端 `frontend/` 本计划不改 API；若发现协议缺口，只在 Rust `OutputGateway` 填默认字段
- 上行音频：16 kHz PCM s16le mono；下行 TTS 二进制：48 kHz PCM s16le mono（Dummy 亦遵守）
- 事件 `TYPE` 字符串必须与 Python `serving/events.py` 一致（如 `asr.result_final`）
- WS 出站 JSON 形状：`{"action": "<name>", "data": ...}`（对齐 `OutputGateway._build_message`）
- WS 入站控制：JSON 含 `action` 或 `type`（对齐 `InputGateway` dispatch 表）
- Manager 之间禁止直接业务调用；只 `publish` / `subscribe`
- 公共 Rust API 写 `///` 文档注释；`cargo fmt` + `clippy -D warnings` 必须通过
- 本计划覆盖 Phase 0 + Phase 1 + Phase 2（HttpChatAgent）；Phase 3（native/FFI 深化）另开计划

---

## File Structure

```text
rust/
  Cargo.toml
  rust-toolchain.toml                    # pin stable
  .gitignore
  crates/
    xtalk-events/
      Cargo.toml
      src/lib.rs                         # Event enum, TYPE helpers, time helpers
    xtalk-bus/
      Cargo.toml
      src/lib.rs                         # EventBus
      tests/bus_tests.rs
    xtalk-models/
      Cargo.toml
      src/lib.rs
      src/asr.rs                         # Asr trait + DummyAsr
      src/tts.rs                         # Tts trait + DummyTts
      src/vad.rs                         # Vad trait + DummyVad
      src/agent.rs                       # Agent trait + DummyAgent + types
      src/http_agent.rs                  # HttpChatAgent (Phase 2)
      src/error.rs
    xtalk-pipeline/
      Cargo.toml
      src/lib.rs                         # Pipeline trait + DefaultPipeline
    xtalk-protocol/
      Cargo.toml
      src/lib.rs
      src/inbound.rs                     # Client JSON → internal enums
      src/outbound.rs                    # action/data builders
      src/fixtures/                      # recorded JSON fixtures
      tests/protocol_tests.rs
    xtalk-serving/
      Cargo.toml
      src/lib.rs
      src/manager.rs                     # Manager trait
      src/service.rs                     # Service
      src/service_manager.rs
      src/session_limiter.rs
      src/modules/
        mod.rs
        input_gateway.rs
        output_gateway.rs
        vad_manager.rs
        asr_manager.rs
        llm_agent_context_manager.rs
        llm_agent_generation_manager.rs
        tts_manager.rs
        turn_taking_manager.rs
      tests/pipeline_flow_tests.rs
      tests/interrupt_tests.rs
    xtalk-server/
      Cargo.toml
      src/main.rs
      src/config.rs                      # JSON config subset
      src/app.rs                         # axum Router + routes
      src/auth.rs                        # optional JWT stub / allow-all for dummy
  examples/
    dummy_server/
      Cargo.toml
      src/main.rs
      config.dummy.json
docs/tutorial/rust_runtime.md
docs/tutorial/rust_runtime.zh.md
.github/workflows/rust-check.yml
```

---

### Task 1: Cargo workspace 骨架

**Files:**
- Create: `rust/Cargo.toml`
- Create: `rust/rust-toolchain.toml`
- Create: `rust/.gitignore`
- Create: `rust/crates/xtalk-events/Cargo.toml`
- Create: `rust/crates/xtalk-events/src/lib.rs`
- Create: `rust/crates/xtalk-bus/Cargo.toml`
- Create: `rust/crates/xtalk-bus/src/lib.rs`
- Create: `rust/crates/xtalk-models/Cargo.toml`
- Create: `rust/crates/xtalk-models/src/lib.rs`
- Create: `rust/crates/xtalk-pipeline/Cargo.toml`
- Create: `rust/crates/xtalk-pipeline/src/lib.rs`
- Create: `rust/crates/xtalk-protocol/Cargo.toml`
- Create: `rust/crates/xtalk-protocol/src/lib.rs`
- Create: `rust/crates/xtalk-serving/Cargo.toml`
- Create: `rust/crates/xtalk-serving/src/lib.rs`
- Create: `rust/crates/xtalk-server/Cargo.toml`
- Create: `rust/crates/xtalk-server/src/main.rs`

**Interfaces:**
- Consumes: none
- Produces: workspace members compile; each lib exposes empty `pub fn crate_name() -> &'static str`

- [ ] **Step 1: Write workspace root**

```toml
# rust/Cargo.toml
[workspace]
resolver = "2"
members = [
  "crates/xtalk-events",
  "crates/xtalk-bus",
  "crates/xtalk-models",
  "crates/xtalk-pipeline",
  "crates/xtalk-protocol",
  "crates/xtalk-serving",
  "crates/xtalk-server",
]
[workspace.package]
edition = "2021"
license = "Apache-2.0"
version = "0.1.0"
[workspace.dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros", "sync", "time", "net"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
bytes = "1"
thiserror = "2"
async-trait = "0.1"
tracing = "0.1"
uuid = { version = "1", features = ["v4"] }
```

```toml
# rust/rust-toolchain.toml
[toolchain]
channel = "1.83.0"
components = ["rustfmt", "clippy"]
```

```gitignore
# rust/.gitignore
/target
**/*.rs.bk
Cargo.lock
```

- [ ] **Step 2: Create each crate with stub `lib.rs` / `main.rs`**

每个 lib crate：

```rust
//! xtalk-events — session event types for the Rust runtime.
pub fn crate_name() -> &'static str {
    "xtalk-events"
}
```

`xtalk-server/src/main.rs`：

```rust
fn main() {
    println!("xtalk-server stub");
}
```

依赖边（写进各自 `Cargo.toml`）：
- `xtalk-bus` → `xtalk-events`
- `xtalk-models` →（无内部依赖）
- `xtalk-pipeline` → `xtalk-models`
- `xtalk-protocol` → `serde`, `serde_json`, `bytes`
- `xtalk-serving` → `xtalk-events`, `xtalk-bus`, `xtalk-models`, `xtalk-pipeline`, `xtalk-protocol`
- `xtalk-server` → `xtalk-serving`, `xtalk-pipeline`, `xtalk-models`, `tokio`, later `axum`

- [ ] **Step 3: Verify build**

Run: `cd rust && cargo build --workspace`
Expected: SUCCESS

- [ ] **Step 4: Commit**

```bash
git add rust/
git commit -m "chore: scaffold rust workspace for xtalk-rs"
```

---

### Task 2: `xtalk-events` 主路径事件

**Files:**
- Modify: `rust/crates/xtalk-events/Cargo.toml`
- Modify: `rust/crates/xtalk-events/src/lib.rs`
- Create: `rust/crates/xtalk-events/tests/event_type_tests.rs`

**Interfaces:**
- Consumes: none
- Produces:
  - `pub struct EventMeta { pub session_id: String, pub timestamp: f64 }`
  - `pub enum Event { ... }` with methods `pub fn type_name(&self) -> &str`, `pub fn meta(&self) -> &EventMeta`
  - `pub fn now_ts() -> f64`

- [ ] **Step 1: Write failing test**

```rust
// rust/crates/xtalk-events/tests/event_type_tests.rs
use xtalk_events::{Event, EventMeta};

#[test]
fn asr_final_type_matches_python() {
    let ev = Event::AsrResultFinal {
        meta: EventMeta::new("s1"),
        text: "hi".into(),
        display_text: "hi".into(),
        speech_pause: false,
    };
    assert_eq!(ev.type_name(), "asr.result_final");
}

#[test]
fn extension_preserves_custom_type() {
    let ev = Event::Extension {
        meta: EventMeta::new("s1"),
        type_name: "custom.foo".into(),
        payload: serde_json::json!({"x": 1}),
    };
    assert_eq!(ev.type_name(), "custom.foo");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd rust && cargo test -p xtalk-events --test event_type_tests`
Expected: FAIL (type/module missing)

- [ ] **Step 3: Implement events**

`Cargo.toml` deps: `serde`, `serde_json`.

```rust
// rust/crates/xtalk-events/src/lib.rs (core shape)
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
    SessionConfigReceived {
        meta: EventMeta,
        config: Value,
    },
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
```

实现时若编译器要求更多变体字段对齐，保持与上表一致；后续 Task 可追加变体，但不得改已有 `type_name` 字符串。

- [ ] **Step 4: Run tests**

Run: `cd rust && cargo test -p xtalk-events`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add rust/crates/xtalk-events
git commit -m "feature: add xtalk-events core event types"
```

---

### Task 3: `xtalk-bus` EventBus

**Files:**
- Modify: `rust/crates/xtalk-bus/Cargo.toml`
- Modify: `rust/crates/xtalk-bus/src/lib.rs`
- Create: `rust/crates/xtalk-bus/tests/bus_tests.rs`

**Interfaces:**
- Consumes: `xtalk_events::Event`
- Produces:
  - `pub type Handler = Arc<dyn Fn(Event) -> BoxFuture<'static, ()> + Send + Sync>`
  - `pub struct EventBus`
  - `EventBus::new() -> Self`
  - `EventBus::subscribe(&self, type_name: &str, priority: i32, handler: Handler)`
  - `EventBus::publish(&self, event: Event) -> impl Future`
  - `EventBus::shutdown(&self)`
  - error-event rate limit constants: `MAX_ERROR_EVENT_DEPTH = 3`, `ERROR_EVENT_COOLDOWN = 1.0s`, `ERROR_EVENT_RATE_LIMIT = 10/s`

- [ ] **Step 1: Write failing tests**

```rust
// rust/crates/xtalk-bus/tests/bus_tests.rs
use std::sync::{Arc, Mutex};
use xtalk_bus::EventBus;
use xtalk_events::{Event, EventMeta};

#[tokio::test]
async fn handlers_run_by_priority_descending() {
    let bus = EventBus::new();
    let order = Arc::new(Mutex::new(Vec::new()));
    for (prio, label) in [(1, "low"), (10, "high"), (5, "mid")] {
        let order = Arc::clone(&order);
        bus.subscribe(
            "vad.speech_start",
            prio,
            Arc::new(move |_ev| {
                let order = Arc::clone(&order);
                let label = label;
                Box::pin(async move {
                    order.lock().unwrap().push(label);
                })
            }),
        );
    }
    bus.publish(Event::VadSpeechStart {
        meta: EventMeta::new("s"),
        origin: "client".into(),
    })
    .await;
    assert_eq!(*order.lock().unwrap(), vec!["high", "mid", "low"]);
}

#[tokio::test]
async fn handler_error_does_not_poison_bus() {
    let bus = EventBus::new();
    let hit = Arc::new(Mutex::new(false));
    bus.subscribe(
        "error.occurred",
        0,
        Arc::new(move |_ev| Box::pin(async { panic!("boom"); })),
    );
    let hit2 = Arc::clone(&hit);
    bus.subscribe(
        "vad.speech_end",
        0,
        Arc::new(move |_ev| {
            let hit2 = Arc::clone(&hit2);
            Box::pin(async move {
                *hit2.lock().unwrap() = true;
            })
        }),
    );
    // Swallow panics inside bus (catch_unwind / AssertUnwindSafe) OR convert handler to Result.
    // For this runtime, handlers must not panic; instead test that a handler returning after
    // publishing ErrorOccurred still allows subsequent publishes.
    bus.publish(Event::VadSpeechEnd {
        meta: EventMeta::new("s"),
        origin: "client".into(),
    })
    .await;
    assert!(*hit.lock().unwrap());
}
```

说明：实现时用 `tokio::sync::Mutex` 保护订阅表；`publish` 克隆当前 handler 列表后按 priority 降序 **顺序 await**（可预测）。handler 内若 `future` 返回错误，由包装层 `tracing::error` 并可选 `publish(ErrorOccurred)`，但必须有深度/频率限制避免递归打爆。

- [ ] **Step 2: Run test to verify it fails**

Run: `cd rust && cargo test -p xtalk-bus --test bus_tests`
Expected: FAIL

- [ ] **Step 3: Implement EventBus**

依赖：`xtalk-events`, `tokio`, `tracing`, `futures`（`BoxFuture`）。

核心 API 骨架：

```rust
use futures::future::BoxFuture;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use xtalk_events::Event;

pub type Handler = Arc<dyn Fn(Event) -> BoxFuture<'static, ()> + Send + Sync>;

struct Sub {
    priority: i32,
    handler: Handler,
}

pub struct EventBus {
    subs: Mutex<HashMap<String, Vec<Sub>>>,
    // error recursion / rate-limit state behind Mutex
}

impl EventBus {
    pub fn new() -> Self { /* ... */ }

    pub fn subscribe(&self, type_name: &str, priority: i32, handler: Handler) {
        // push + sort by priority desc
    }

    pub async fn publish(&self, event: Event) {
        let key = event.type_name().to_string();
        let handlers = {
            let guard = self.subs.lock().await;
            guard.get(&key).cloned().unwrap_or_default()
        };
        for sub in handlers {
            (sub.handler)(event.clone()).await;
        }
    }

    pub async fn shutdown(&self) {
        self.subs.lock().await.clear();
    }
}
```

为 `Sub` 实现 `Clone`（`Handler` 已是 `Arc`）。

另写单测：`error.occurred` 在冷却窗口内超速时被 drop（对照 Python `ERROR_EVENT_RATE_LIMIT`）。

- [ ] **Step 4: Run tests**

Run: `cd rust && cargo test -p xtalk-bus`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add rust/crates/xtalk-bus
git commit -m "feature: add async EventBus with priority dispatch"
```

---

### Task 4: `xtalk-models` traits + Dummy 实现

**Files:**
- Modify: `rust/crates/xtalk-models/Cargo.toml`
- Modify: `rust/crates/xtalk-models/src/lib.rs`
- Create: `rust/crates/xtalk-models/src/error.rs`
- Create: `rust/crates/xtalk-models/src/asr.rs`
- Create: `rust/crates/xtalk-models/src/tts.rs`
- Create: `rust/crates/xtalk-models/src/vad.rs`
- Create: `rust/crates/xtalk-models/src/agent.rs`
- Create: `rust/crates/xtalk-models/tests/dummy_tests.rs`

**Interfaces:**
- Consumes: none
- Produces:
  - `pub trait Asr: Send + Sync { async fn recognize_stream(&self, audio: &[u8], is_final: bool) -> Result<String, ModelError>; fn reset(&self); fn clone_box(&self) -> Box<dyn Asr>; }`
  - `pub trait Tts: Send + Sync { async fn synthesize_stream(&self, text: &str) -> Result<Vec<Vec<u8>>, ModelError>; fn sample_rate(&self) -> u32; fn clone_box(&self) -> Box<dyn Tts>; }`
  - `pub trait Vad: Send + Sync { async fn is_speech(&self, frame: &[u8]) -> Result<bool, ModelError>; fn clone_box(&self) -> Box<dyn Vad>; }`
  - `pub struct AgentContext { pub context_type: String, pub text: String }`
  - `pub trait Agent: Send + Sync { async fn accept(&self, ctx: AgentContext, cancel: CancellationToken) -> Result<Vec<String>, ModelError>; fn clone_box(&self) -> Box<dyn Agent>; }`
  - `DummyAsr { default_text }` / `DummyTts { sample_rate: 48000 }` / `DummyVad` / `DummyAgent { default_response }`

- [ ] **Step 1: Write failing tests**

```rust
use tokio_util::sync::CancellationToken;
use xtalk_models::{Agent, AgentContext, Asr, DummyAgent, DummyAsr, DummyTts, DummyVad, Tts, Vad};

#[tokio::test]
async fn dummy_asr_returns_default_on_final() {
    let asr = DummyAsr::new("hello");
    let text = asr.recognize_stream(&[], true).await.unwrap();
    assert_eq!(text, "hello");
}

#[tokio::test]
async fn dummy_tts_returns_pcm_at_48k() {
    let tts = DummyTts::new(48_000);
    assert_eq!(tts.sample_rate(), 48_000);
    let chunks = tts.synthesize_stream("hi").await.unwrap();
    assert!(!chunks.is_empty());
    // 100ms silence @ 48k s16le mono = 9600 bytes
    assert_eq!(chunks[0].len(), 48_000 / 10 * 2);
}

#[tokio::test]
async fn dummy_agent_only_answers_asr_final() {
    let agent = DummyAgent::new("ok");
    let cancel = CancellationToken::new();
    let out = agent
        .accept(
            AgentContext {
                context_type: "asr_final".into(),
                text: "x".into(),
            },
            cancel,
        )
        .await
        .unwrap();
    assert_eq!(out, vec!["ok".to_string()]);
}
```

- [ ] **Step 2: Run to verify fail**

Run: `cd rust && cargo test -p xtalk-models --test dummy_tests`
Expected: FAIL

- [ ] **Step 3: Implement traits + dummies**

依赖：`async-trait`, `thiserror`, `tokio-util`（CancellationToken）, `tokio`.

`DummyTts`：**不要**返回 WAV header；返回 raw PCM s16le 静音 chunk，便于 WS 二进制直出。

`DummyVad::is_speech`：若 `frame.len() >= 320`（10ms@16k）返回 `true`，否则 `false`（仅供单测；生产路径以客户端 VAD 事件为主）。

- [ ] **Step 4: Run tests**

Run: `cd rust && cargo test -p xtalk-models`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add rust/crates/xtalk-models
git commit -m "feature: add model traits and dummy ASR/TTS/VAD/Agent"
```

---

### Task 5: `xtalk-pipeline` DefaultPipeline

**Files:**
- Modify: `rust/crates/xtalk-pipeline/Cargo.toml`
- Modify: `rust/crates/xtalk-pipeline/src/lib.rs`
- Create: `rust/crates/xtalk-pipeline/tests/pipeline_tests.rs`

**Interfaces:**
- Consumes: `Box<dyn Asr>`, `Box<dyn Tts>`, `Box<dyn Vad>`, `Box<dyn Agent>`
- Produces:
  - `pub trait Pipeline: Send + Sync { fn clone_session(&self) -> Box<dyn Pipeline>; fn asr(&self) -> Option<&dyn Asr>; fn tts(&self) -> Option<&dyn Tts>; fn vad(&self) -> Option<&dyn Vad>; fn agent(&self) -> Option<&dyn Agent>; }`
  - `pub struct DefaultPipeline { ... }`
  - `DefaultPipeline::builder() -> DefaultPipelineBuilder`

- [ ] **Step 1: Failing test**

```rust
use xtalk_models::{DummyAgent, DummyAsr, DummyTts, DummyVad};
use xtalk_pipeline::DefaultPipeline;

#[test]
fn clone_session_isolates_asr_state() {
    let p = DefaultPipeline::builder()
        .asr(Box::new(DummyAsr::new("a")))
        .tts(Box::new(DummyTts::new(48_000)))
        .vad(Box::new(DummyVad::default()))
        .agent(Box::new(DummyAgent::new("r")))
        .build();
    let c1 = p.clone_session();
    let c2 = p.clone_session();
    // pointers / identity differ
    assert!(!std::ptr::eq(
        c1.asr().unwrap() as *const dyn xtalk_models::Asr,
        c2.asr().unwrap() as *const dyn xtalk_models::Asr
    ));
}
```

（若 fat pointer 比较不便，改为：对 c1 的 DummyAsr `recognize_stream` 后 `reset`，断言 c2 仍能独立返回 default_text——通过 interior `Mutex` 状态验证隔离。）

- [ ] **Step 2: Implement DefaultPipeline + builder**

```rust
pub struct DefaultPipeline {
    asr: Option<Box<dyn Asr>>,
    tts: Option<Box<dyn Tts>>,
    vad: Option<Box<dyn Vad>>,
    agent: Option<Box<dyn Agent>>,
}
```

`clone_session` 调用各模型 `clone_box()`。

- [ ] **Step 3: Test + commit**

```bash
cd rust && cargo test -p xtalk-pipeline
git add rust/crates/xtalk-pipeline
git commit -m "feature: add DefaultPipeline with session clone"
```

---

### Task 6: `xtalk-protocol` 线协议编解码

**Files:**
- Modify: `rust/crates/xtalk-protocol/src/lib.rs`
- Create: `rust/crates/xtalk-protocol/src/inbound.rs`
- Create: `rust/crates/xtalk-protocol/src/outbound.rs`
- Create: `rust/crates/xtalk-protocol/src/fixtures/vad_speech_start.json`
- Create: `rust/crates/xtalk-protocol/tests/protocol_tests.rs`

**Interfaces:**
- Consumes: `serde_json::Value`
- Produces:
  - `pub enum InboundMessage { Ping { timestamp: f64 }, VadSpeechStart, VadSpeechEnd, TtsPlaybackFinished, TtsChunkPlayed { .. }, SessionConfig(Value), Unknown }`
  - `pub fn parse_inbound_text(text: &str) -> Result<InboundMessage, ProtocolError>`
  - `pub fn outbound_action(action: &str, data: impl Serialize) -> String` → `{"action","data"}`
  - 常量 action 名：`session_attached`, `update_asr`, `finish_asr`, `update_resp`, `finish_resp`, `start_tts`, `stop_tts`, `tts_finished`, `error`, `pong`

- [ ] **Step 1: Fixture + failing tests**

```json
// fixtures/vad_speech_start.json
{"action":"vad_speech_start"}
```

```rust
#[test]
fn parse_vad_start_fixture() {
    let raw = include_str!("../src/fixtures/vad_speech_start.json");
    let msg = xtalk_protocol::parse_inbound_text(raw).unwrap();
    assert!(matches!(msg, xtalk_protocol::InboundMessage::VadSpeechStart));
}

#[test]
fn outbound_shape() {
    let s = xtalk_protocol::outbound_action("update_asr", serde_json::json!({"text":"hi"}));
    let v: serde_json::Value = serde_json::from_str(&s).unwrap();
    assert_eq!(v["action"], "update_asr");
    assert_eq!(v["data"]["text"], "hi");
}
```

对齐 Python dispatch 键：`ping`, `vad_speech_start`, `vad_speech_end`, `tts_playback_finished`, `tts_chunk_played`, `session_config`, `clock_sync`, `change_voice`, `change_emotion`, `change_tts_speed`（未知 action → `Unknown`，不报错）。

- [ ] **Step 2: Implement + test + commit**

```bash
cd rust && cargo test -p xtalk-protocol
git add rust/crates/xtalk-protocol
git commit -m "feature: add websocket protocol encode/decode"
```

---

### Task 7: Manager trait + Service 骨架

**Files:**
- Create: `rust/crates/xtalk-serving/src/manager.rs`
- Create: `rust/crates/xtalk-serving/src/service.rs`
- Modify: `rust/crates/xtalk-serving/src/lib.rs`
- Create: `rust/crates/xtalk-serving/tests/service_lifecycle_tests.rs`

**Interfaces:**
- Consumes: `EventBus`, `Box<dyn Pipeline>`
- Produces:
  - `pub trait Manager: Send + Sync { fn name(&self) -> &'static str; fn register(self: Arc<Self>, bus: Arc<EventBus>); }`
  - `pub struct Service { session_id, bus, pipeline, ... }`
  - `Service::new(session_id, pipeline, managers) -> Arc<Self>`
  - `Service::bus(&self) -> Arc<EventBus>`
  - `Service::shutdown(&self)`

- [ ] **Step 1: Failing lifecycle test**

```rust
#[tokio::test]
async fn service_registers_managers_and_shutdown_clears_bus() {
    // Use a test manager that sets a flag when its subscribed handler runs.
    // Service::new should call register on each manager.
    // After shutdown, publish should no-op (no handlers).
}
```

- [ ] **Step 2: Implement**

```rust
pub trait Manager: Send + Sync {
    fn name(&self) -> &'static str;
    fn register(self: Arc<Self>, bus: Arc<EventBus>);
}

pub struct Service {
    pub session_id: String,
    bus: Arc<EventBus>,
    pipeline: Box<dyn Pipeline>,
    _managers: Vec<Arc<dyn Manager>>,
}
```

`new`：创建 `EventBus`，对每个 manager `manager.register(bus.clone())`，把 `Arc<dyn Manager>` 存起来防 drop。

注意：Managers 需要读 Pipeline —— 用 `Arc<Mutex<Box<dyn Pipeline>>>` 或在构造 manager 时把所需模型 `clone_box` 注入 manager（**推荐注入**：避免 Manager 反查 Pipeline 造成生命周期复杂）。本计划采用 **构造时注入模型句柄**。

- [ ] **Step 3: Test + commit**

```bash
cd rust && cargo test -p xtalk-serving --test service_lifecycle_tests
git add rust/crates/xtalk-serving
git commit -m "feature: add Service and Manager registration skeleton"
```

---

### Task 8: InputGateway + OutputGateway

**Files:**
- Create: `rust/crates/xtalk-serving/src/modules/mod.rs`
- Create: `rust/crates/xtalk-serving/src/modules/input_gateway.rs`
- Create: `rust/crates/xtalk-serving/src/modules/output_gateway.rs`
- Create: `rust/crates/xtalk-serving/tests/gateway_tests.rs`

**Interfaces:**
- Consumes: `parse_inbound_text`, `outbound_action`, `EventBus`, outbound sink trait
- Produces:
  - `pub trait WsSink: Send + Sync { async fn send_text(&self, text: String); async fn send_bytes(&self, data: Bytes); }`
  - `InputGateway::handle_text(&self, text: &str)`
  - `InputGateway::handle_binary(&self, data: Bytes, sample_rate: u32)`
  - `OutputGateway` 订阅：`asr.*`, `response.*`, `tts.*`, `error.occurred` → 调 `WsSink`

- [ ] **Step 1: Failing tests with mock sink**

```rust
struct MockSink {
    texts: Mutex<Vec<String>>,
    bins: Mutex<Vec<Vec<u8>>>,
}
// implement WsSink

#[tokio::test]
async fn input_vad_start_publishes_event() {
    let bus = Arc::new(EventBus::new());
    let seen = Arc::new(Mutex::new(false));
    // subscribe vad.speech_start -> set seen
    let gw = InputGateway::new("s1".into(), bus.clone(), /* sink for pong */ sink);
    gw.handle_text(r#"{"action":"vad_speech_start"}"#).await.unwrap();
    assert!(*seen.lock().unwrap());
}

#[tokio::test]
async fn output_forwards_asr_final() {
    let bus = Arc::new(EventBus::new());
    let sink = Arc::new(MockSink::default());
    let out = OutputGateway::new("s1".into(), sink.clone());
    Arc::new(out).register(bus.clone());
    bus.publish(Event::AsrResultFinal { /* ... text: "hi" */ }).await;
    let texts = sink.texts.lock().unwrap();
    assert!(texts.iter().any(|t| t.contains("finish_asr") && t.contains("hi")));
}
```

- [ ] **Step 2: Implement gateways**

`InputGateway` 映射（最小集）：
- `ping` → 直接 `sink.send_text(pong JSON)`（含 `action: pong`）
- `vad_speech_start` / `vad_speech_end` → 对应事件，`origin: "client"`
- `tts_playback_finished` → `TtsPlaybackFinished`
- binary → `AudioFrameReceived { sample_rate: 16000 }`

`OutputGateway` 映射（最小集）：
- `AsrResultPartial` → `update_asr`
- `AsrResultFinal` → `finish_asr`
- `ResponseUpdate` / `ResponseFinish` → `update_resp` / `finish_resp`
- `TtsStarted` / `TtsStopped` / `TtsFinished` → `start_tts` / `stop_tts` / `tts_finished`
- `TtsChunkReady` → `send_bytes`
- `ErrorOccurred` → `error`
- 启动时由 Service 调 `send_session_attached`

- [ ] **Step 3: Test + commit**

```bash
cd rust && cargo test -p xtalk-serving --test gateway_tests
git commit -m "feature: add input/output websocket gateways"
```

---

### Task 9: VadManager + AsrManager（含预缓冲）

**Files:**
- Create: `rust/crates/xtalk-serving/src/modules/vad_manager.rs`
- Create: `rust/crates/xtalk-serving/src/modules/asr_manager.rs`
- Create: `rust/crates/xtalk-serving/tests/asr_manager_tests.rs`

**Interfaces:**
- Consumes: `Arc<dyn Asr>`, `Arc<dyn Vad>`（可选）, `EventBus`
- Produces: managers that emit `AsrResultPartial` / `AsrResultFinal`

行为（对齐 Python ASR 预缓冲语义，简化版）：
- 持续缓存最近 `PRE_ROLL_FRAMES = 20` 个 `AudioFrameReceived`
- 收到 `VadSpeechStart` 或 `TurnAsrStartRequested`：把 pre-roll 注入识别，进入 listening
- listening 期间音频喂给 `Asr::recognize_stream(..., is_final=false)`，publish partial
- `VadSpeechEnd` / `TurnAsrEndRequested`：`recognize_stream(..., is_final=true)`，publish final，`asr.reset()`

- [ ] **Step 1: Failing test**

```rust
#[tokio::test]
async fn asr_final_after_speech_end() {
    // publish 5 audio frames, then VadSpeechStart, more frames, VadSpeechEnd
    // expect one AsrResultFinal with DummyAsr default text
}
```

- [ ] **Step 2: Implement + test + commit**

```bash
cd rust && cargo test -p xtalk-serving --test asr_manager_tests
git commit -m "feature: add VAD and ASR managers with preroll"
```

`VadManager` Phase 1：若配置了服务端 Vad，可对帧跑 `is_speech` 发 origin=`server` 的 VAD 事件；默认关闭，依赖客户端 VAD。

---

### Task 10: LLM Agent Managers + TtsManager + TurnTakingManager

**Files:**
- Create: `rust/crates/xtalk-serving/src/modules/llm_agent_context_manager.rs`
- Create: `rust/crates/xtalk-serving/src/modules/llm_agent_generation_manager.rs`
- Create: `rust/crates/xtalk-serving/src/modules/tts_manager.rs`
- Create: `rust/crates/xtalk-serving/src/modules/turn_taking_manager.rs`
- Create: `rust/crates/xtalk-serving/tests/pipeline_flow_tests.rs`
- Create: `rust/crates/xtalk-serving/tests/interrupt_tests.rs`

**Interfaces:**
- Consumes: `Arc<dyn Agent>`, `Arc<dyn Tts>`, `CancellationToken` per generation
- Produces: end-to-end event chain ASR final → response → TTS chunks

编排：
1. `LlmAgentContextManager`：听 `AsrResultFinal` → publish `LlmAgentConsumeGenerationRequested { context_type: "asr_final", text }`
2. `LlmAgentGenerationManager`：听 consume 请求 → `agent.accept` 流式文本 → 每句/整段 `ResponseUpdate`，结束 `ResponseFinish`；同时把文本送给 TTS 路径（publish 内部或直接由同 manager 调合成——**必须经事件**：publish 一个 `turn.tts_text_append_requested` 或复用 `ResponseUpdate` 由 TtsManager 订阅）
3. `TtsManager`：订阅 `ResponseUpdate`（或专用 TTS 文本事件）→ `tts.synthesize_stream` → `TtsStarted` + N× `TtsChunkReady` + `TtsFinished`
4. `TurnTakingManager`：`VadSpeechStart`（client）在 TTS 播放中 → publish `TurnTtsStopRequested` + `TurnLlmAgentStopRequested`；Generation/TTS managers 取消 `CancellationToken` 并 publish `TtsStopped`

- [ ] **Step 1: Pipeline flow failing test**

```rust
#[tokio::test]
async fn asr_to_agent_to_tts_event_order() {
    // Wire Service with Dummy models + all managers + recording subscriber
    // Inject VadSpeechStart, AudioFrame, VadSpeechEnd
    // Assert type_name sequence contains:
    // asr.result_final → response.update/finish → tts.started → tts.chunk_ready → tts.finished
}
```

- [ ] **Step 2: Interrupt failing test**

```rust
#[tokio::test]
async fn speech_start_cancels_tts() {
    // Start a long DummyAgent response / slow TTS
    // Mid-flight publish VadSpeechStart
    // Expect turn.tts_stop_requested and tts.stopped; no crash
}
```

- [ ] **Step 3: Implement managers with cancel tokens**

每个生成任务：`let token = CancellationToken::new();` 存入 `Mutex<Option<CancellationToken>>`；stop 时 `token.cancel()`。

- [ ] **Step 4: Run both tests + commit**

```bash
cd rust && cargo test -p xtalk-serving --test pipeline_flow_tests --test interrupt_tests
git commit -m "feature: wire agent, tts, and turn-taking managers"
```

---

### Task 11: ServiceManager + SessionLimiter

**Files:**
- Create: `rust/crates/xtalk-serving/src/session_limiter.rs`
- Create: `rust/crates/xtalk-serving/src/service_manager.rs`
- Create: `rust/crates/xtalk-serving/tests/session_limiter_tests.rs`

**Interfaces:**
- Consumes: prototype `Pipeline` factory / `Arc<dyn Fn() -> Box<dyn Pipeline> + Send + Sync>`
- Produces:
  - `SessionLimiter::new(max_sessions: usize)`
  - `SessionLimiter::acquire() -> Result<SessionPermit, LimitError>`
  - `ServiceManager::connect(sink, user_id) -> Arc<Service>`
  - `ServiceManager::disconnect(session_id)`

- [ ] **Step 1: Test max sessions**

```rust
#[tokio::test]
async fn rejects_when_full() {
    let limiter = SessionLimiter::new(1);
    let _p = limiter.acquire().await.unwrap();
    assert!(limiter.try_acquire().is_err());
}
```

- [ ] **Step 2: Implement + commit**

```bash
git commit -m "feature: add ServiceManager and session limiter"
```

---

### Task 12: 配置加载 + axum `xtalk-server`

**Files:**
- Create: `rust/crates/xtalk-server/src/config.rs`
- Create: `rust/crates/xtalk-server/src/auth.rs`
- Create: `rust/crates/xtalk-server/src/app.rs`
- Modify: `rust/crates/xtalk-server/src/main.rs`
- Modify: `rust/crates/xtalk-server/Cargo.toml`
- Create: `rust/examples/dummy_server/config.dummy.json`
- Create: `rust/examples/dummy_server/Cargo.toml`
- Create: `rust/examples/dummy_server/src/main.rs`
- Modify: `rust/Cargo.toml`（members 加入 example）

**Interfaces:**
- Consumes: JSON config subset
- Produces:
  - `pub struct ServerConfig { pub asr: ModelSpec, pub tts: ModelSpec, pub llm_agent: ModelSpec, pub vad: Option<ModelSpec>, pub max_sessions: usize, pub listen: String }`
  - `pub struct ModelSpec { pub type_name: String, pub params: Value }`  （JSON 字段名用 `"type"`，Rust 侧 `#[serde(rename = "type")] type_name`）
  - `build_pipeline(cfg) -> Result<DefaultPipeline>`
  - 支持 type：`dummy`（asr/tts/vad/llm_agent）；未知 type → 启动错误
  - Routes（对齐 Python `Xtalk.mount_routes` 子集）：
    - `GET /` 可选静态提示
    - `WS /ws`
    - Phase 1 auth：**allow-all**（`auth.rs` 提供 `AuthMode::Disabled`）；保留 query `access_token` 解析钩子但不强制

`config.dummy.json`：

```json
{
  "listen": "0.0.0.0:11995",
  "max_sessions": 32,
  "asr": { "type": "dummy", "params": { "default_text": "hello from rust" } },
  "tts": { "type": "dummy", "params": { "sample_rate": 48000 } },
  "llm_agent": { "type": "dummy", "params": { "default_response": "Rust dummy reply." } },
  "vad": { "type": "dummy", "params": {} }
}
```

- [ ] **Step 1: Unit test config parse**

```rust
#[test]
fn parses_dummy_config() {
    let cfg = ServerConfig::from_str(include_str!("../../../examples/dummy_server/config.dummy.json")).unwrap();
    assert_eq!(cfg.asr.type_name, "dummy");
}
```

- [ ] **Step 2: Implement axum app**

依赖：`axum = { version = "0.7", features = ["ws"] }`, `tower-http`（可选 CORS/trace）, `tracing-subscriber`, `clap`。

`WS /ws` handler：
1. accept
2. `ServiceManager::connect` 得到 service
3. 发 `session_attached`
4. 循环读 Message::Text / Binary → InputGateway
5. 连接结束 → `disconnect` + `service.shutdown()`

`WsSink` 实现：包装 `Arc<Mutex<SplitSink<...>>>` 或 `tokio::sync::mpsc` 由写任务发送（推荐 mpsc，避免锁住 sink）。

- [ ] **Step 3: Manual smoke**

Run: `cd rust && cargo run -p dummy_server -- --config examples/dummy_server/config.dummy.json`
Expected: listens on `:11995`

用 websocat 或小脚本：连 `/ws`，发 `{"action":"ping"}`，收到 `pong`；发 vad start/end + 任意 binary，收到 `finish_asr` / `update_resp` / binary TTS。

- [ ] **Step 4: Commit**

```bash
git add rust/
git commit -m "feature: add axum xtalk-server and dummy_server example"
```

---

### Task 13: 前端联调兼容层与协议金丝雀

**Files:**
- Create: `rust/crates/xtalk-protocol/src/fixtures/ping.json`
- Create: `rust/crates/xtalk-protocol/src/fixtures/session_config.json`
- Create: `rust/crates/xtalk-serving/tests/frontend_compat_tests.rs`
- Modify: `rust/crates/xtalk-serving/src/modules/output_gateway.rs`（缺字段填默认）
- Create: `docs/tutorial/rust_runtime.md`
- Create: `docs/tutorial/rust_runtime.zh.md`

**Interfaces:**
- Produces: 文档说明如何用现有 `examples/sample_app` 静态页指向 Rust 端口；compat 测试固定出站 JSON 键

- [ ] **Step 1: Compat tests for outbound keys**

断言 `finish_asr` data 含 `text`；`update_resp` 含 `text`；`session_attached` 含 `session_id`；`latency_metrics` 若未实现则不发送（不要发残缺对象）。

- [ ] **Step 2: Write docs (EN + ZH)**

内容必须包含：
1. `cd rust && cargo run -p dummy_server -- --config examples/dummy_server/config.dummy.json`
2. 构建 frontend dist（若需要）并如何把 sample 页的 WS URL 指到 `ws://localhost:11995/ws`
3. 当前支持的模型 type 列表（仅 `dummy` + 下一 Task 的 `openai_compat`）
4. 与 Python 服务的差异（auth 默认关闭、未实现 captioner/recording 等）

- [ ] **Step 3: Commit**

```bash
git commit -m "docs: add rust runtime tutorial and protocol fixtures"
```

---

### Task 14: CI workflow

**Files:**
- Create: `.github/workflows/rust-check.yml`

- [ ] **Step 1: Add workflow**

```yaml
name: rust-check
on:
  push:
    paths: ["rust/**", ".github/workflows/rust-check.yml"]
  pull_request:
    paths: ["rust/**", ".github/workflows/rust-check.yml"]
jobs:
  check:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: rust
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@1.83.0
        with:
          components: rustfmt, clippy
      - run: cargo fmt --all -- --check
      - run: cargo clippy --workspace --all-targets -- -D warnings
      - run: cargo test --workspace
```

- [ ] **Step 2: Run locally equivalent + commit**

```bash
cd rust && cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
git add .github/workflows/rust-check.yml
git commit -m "chore: add rust fmt clippy test CI workflow"
```

---

### Task 15: `HttpChatAgent`（OpenAI-compatible）

**Files:**
- Create: `rust/crates/xtalk-models/src/http_agent.rs`
- Modify: `rust/crates/xtalk-models/src/lib.rs`
- Modify: `rust/crates/xtalk-models/Cargo.toml`（`reqwest` with `json`, `stream`）
- Create: `rust/crates/xtalk-models/tests/http_agent_tests.rs`
- Modify: `rust/crates/xtalk-server/src/config.rs`（识别 `type: openai_compat`）
- Create: `rust/examples/dummy_server/config.openai.json`

**Interfaces:**
- Consumes: `base_url`, `api_key`, `model`
- Produces: `HttpChatAgent` implementing `Agent`；SSE/stream chunks → `Vec<String>` 句子或 token 聚合列表
- `accept` 在 `cancel.is_cancelled()` 时停止读 stream

- [ ] **Step 1: Failing test with wiremock**

```rust
#[tokio::test]
async fn streams_chat_completion_deltas() {
    // wiremock: POST /v1/chat/completions returns SSE lines with two deltas then [DONE]
    // HttpChatAgent::accept returns concatenated / chunked strings
}
```

依赖测试：`wiremock`.

- [ ] **Step 2: Implement HttpChatAgent**

请求体：

```json
{
  "model": "<model>",
  "stream": true,
  "messages": [
    {"role": "user", "content": "<ctx.text>"}
  ]
}
```

解析 `data: {"choices":[{"delta":{"content":"..."}}]}`。

- [ ] **Step 3: Config wiring**

```json
{
  "llm_agent": {
    "type": "openai_compat",
    "params": {
      "base_url": "https://api.openai.com/v1",
      "api_key": "${OPENAI_API_KEY}",
      "model": "gpt-4o-mini"
    }
  }
}
```

`${ENV}` 展开在 `config.rs` 实现；缺省 env 时启动失败并给出清晰错误。

- [ ] **Step 4: Test + commit**

```bash
cd rust && cargo test -p xtalk-models --test http_agent_tests
git commit -m "feature: add OpenAI-compatible HttpChatAgent"
```

---

### Task 16: 集成验收清单（Phase 1/2 关门）

**Files:**
- Modify: `docs/superpowers/specs/2026-08-08-rust-port-design.md`（状态改为 `Accepted`，并链到本 plan）
- Modify: `docs/tutorial/rust_runtime.zh.md`（补充 HttpChatAgent 段落）

- [ ] **Step 1: Run full verification**

```bash
cd rust
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p dummy_server -- --config examples/dummy_server/config.dummy.json
```

- [ ] **Step 2: Manual E2E checklist（执行者勾选）**

- [ ] WS 连接后收到 `session_attached`
- [ ] `ping` → `pong`
- [ ] 客户端 `vad_speech_start` + binary PCM + `vad_speech_end` → `finish_asr` + `update_resp`/`finish_resp` + binary TTS + `tts_finished`
- [ ] TTS 播放中再发 `vad_speech_start` → 收到 `stop_tts`，服务不崩溃
- [ ] `config.openai.json` + 有效 API key 时，回复非 Dummy 固定文案

- [ ] **Step 3: Final commit**

```bash
git add docs/
git commit -m "docs: mark rust port phase1 acceptance checklist"
```

---

## Spec Coverage Check

| Spec 要求 | Task |
|-----------|------|
| Cargo workspace + crate 布局 | Task 1 |
| `xtalk-events` + Extension | Task 2 |
| EventBus 优先级/错误限流/shutdown | Task 3 |
| Model traits + Dummy* | Task 4 |
| DefaultPipeline + clone_session | Task 5 |
| 线协议编解码 + fixtures | Task 6, 13 |
| Manager/Service | Task 7 |
| Input/OutputGateway | Task 8 |
| ASR preroll + VAD | Task 9 |
| Agent/TTS/TurnTaking + 打断 | Task 10 |
| ServiceManager/SessionLimiter | Task 11 |
| 配置子集 + axum server + dummy_server | Task 12 |
| 前端兼容/文档 | Task 13 |
| CI | Task 14 |
| HttpChatAgent / openai_compat | Task 15 |
| Phase 1 成功标准验收 | Task 16 |
| Phase 3 native/FFI | **本计划不做**（另开计划） |
| captioner/recording/persistence 等 | **本计划不做**（Phase 2+ 后续） |

## Placeholder / Consistency Review

- 无 TBD/TODO 步骤；接口名在 Tasks 间保持一致：`clone_session`, `clone_box`, `WsSink`, `InboundMessage`, `ServerConfig.type_name`
- Dummy TTS 统一 48 kHz raw PCM（避免 WAV/PCM 混用）
- Commit 前缀使用仓库要求的 `feature:`/`docs:`/`chore:`

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-08-08-rust-port-implementation.md`.

**Two execution options:**

1. **Subagent-Driven (recommended)** — 每个 Task 派生子代理，Task 间审查，迭代快  
2. **Inline Execution** — 本会话按 `executing-plans` 批量执行并设检查点  

回复选项编号即可开始执行。
