# X-Talk Rust 迁移翻译版 — 设计规格

**日期:** 2026-08-08  
**状态:** Accepted  
**实现计划:** [2026-08-08-rust-port-implementation.md](../plans/2026-08-08-rust-port-implementation.md)  
**范围:** 在现有 Python X-Talk 之上，新增架构等价的 Rust 后端运行时（`xtalk-rs`），而非整仓机械翻译。

---

## 1. 背景与目标

X-Talk 是事件驱动的全双工级联语音对话框架。当前后端为纯 Python（FastAPI + asyncio），前端为 TypeScript SDK。核心价值不在某一具体 ASR/TTS 实现，而在：

- 会话级 Pipeline 组装
- EventBus 驱动的 Manager 编排
- 与前端兼容的 WebSocket 线协议
- 可插拔模型接口（ASR / TTS / VAD / Agent / TurnDetector …）

**迁移翻译版**的含义：

1. **翻译**：Rust 侧复刻同一套概念模型（Event / Bus / Manager / Pipeline / Service / Session）。
2. **迁移**：运行时中枢逐步切到 Rust；模型实现可先 Dummy / HTTP 适配，再按需原生化。
3. **非目标**：一次性把 FunASR、CosyVoice、LangChain Agent 等全部重写成 Rust。

成功标准（第一阶段）：

- Rust 服务可加载最小配置，用 Dummy ASR/TTS/Agent 跑通与现有 sample frontend 的 WebSocket 会话。
- 事件名、关键消息形状与现有前端兼容（允许显式兼容层做少量字段适配）。
- 核心 crate 可单测：EventBus 订阅/发布、会话生命周期、打断路径。

---

## 2. 方案对比与选定

| 方案 | 做法 | 优点 | 缺点 |
|------|------|------|------|
| A. 大爆炸全量重写 | Python + 全部模型一并 Rust 化 | 终点最干净 | 体量过大，长期不可交付 |
| B. 分层移植（选定） | Rust 运行时 + trait 模型 + 兼容现有前端；重模型走 sidecar/HTTP | 可增量交付，架构清晰 | 双栈过渡期 |
| C. Rust 壳调 Python | PyO3/子进程包一层 Python | 短期能跑 | 不是真正迁移，边界脏 |

**选定 B。** 仓库内新增 `rust/` Cargo workspace，与 Python 包并存；前端暂不动；Python 继续作为模型参考实现与对照基线。

---

## 3. 总体架构

```text
Browser / frontend createSession
        │  WebSocket + JSON/binary frames
        ▼
┌──────────────────────────────────────────┐
│  xtalk-server (axum)                     │
│  auth · session limiter · static/demo    │
└──────────────────┬───────────────────────┘
                   │ per-session
                   ▼
┌──────────────────────────────────────────┐
│  Service                                 │
│  ┌─────────────┐   ┌──────────────────┐  │
│  │ EventBus    │◄─►│ Managers         │  │
│  └─────────────┘   │ input/output     │  │
│         ▲          │ vad/asr/tts/...  │  │
│         │          │ llm_agent/...    │  │
│  ┌──────┴──────┐   └────────┬─────────┘  │
│  │ Pipeline    │            │            │
│  │ (trait objs)│◄───────────┘            │
│  └──────┬──────┘                         │
└─────────┼────────────────────────────────┘
          │
   ┌──────┴──────────────────────────┐
   │ Model backends                  │
   │ dummy · http/openai-compat      │
   │ (later) native / python-sidecar │
   └─────────────────────────────────┘
```

设计原则：

- **一个会话一个 Service**：拥有独立 EventBus、Pipeline clone、Manager 集合。
- **Manager 只通过事件通信**：禁止 Manager 之间直接持有可变引用调用业务方法。
- **模型是 Pipeline 上的 trait 对象**：Manager 从 Pipeline 取模型，不自己 new 具体实现。
- **前端平台代码不进 Rust**：继续用现有 `frontend/`；Rust 只保证线协议兼容。

---

## 4. 仓库与 crate 布局

```text
rust/
  Cargo.toml                 # workspace
  crates/
    xtalk-events/            # 事件类型、TYPE 字符串、序列化
    xtalk-bus/               # async EventBus
    xtalk-models/            # ASR/TTS/VAD/Agent/... traits + Dummy
    xtalk-pipeline/          # Pipeline / DefaultPipeline
    xtalk-serving/           # Service、Managers、ServiceManager
    xtalk-protocol/          # WS/HTTP 线协议编解码（与前端对齐）
    xtalk-server/            # axum 二进制：配置加载、挂路由、跑服务
  examples/
    dummy_server/            # 对标 examples/sample_app 最小可跑
```

Python 树保持不动。文档在 `docs/` 增加 Rust 章节（中英同步，遵循仓库文档规范）。

---

## 5. 核心组件设计

### 5.1 Events（`xtalk-events`）

- 用 `enum Event`（或带 `type` 标签的内部 tagged 结构）表达现有 `serving/events.py` 中的稳定事件。
- 每个变体携带 `session_id`、`timestamp`，以及与 Python `TYPE` 相同的字符串（如 `audio.frame_received`、`asr.result_final`）。
- 第一阶段覆盖主路径事件：音频进出、VAD、ASR partial/final、LLM/agent 响应、TTS chunk/控制、turn 请求、error、session config。
- 扩展事件用 `Event::Extension { type_name, payload: Value }`，对应 Python `create_event_class` 的动态能力，避免为每个实验事件改核心 enum。

### 5.2 EventBus（`xtalk-bus`）

语义对齐 Python `EventBus`：

- `subscribe(type, handler, priority)`
- `publish(event)`：按 priority 调度；handler 为 `async fn`
- 错误事件深度/频率限制（移植现有防递归逻辑）
- 可选 history（默认关）
- 会话结束时 cancel 未完成任务并清理订阅

实现建议：`tokio` + 每会话一条有序调度队列（先保证 handler 顺序可预测，再按需并行化同优先级无依赖 handler）。

### 5.3 Model traits（`xtalk-models`）

对齐 `speech/interfaces.py` 与 `llm_agent/interfaces.py` 的能力面，但用 Rust 习惯表达：

```rust
#[async_trait]
pub trait Asr: Send + Sync {
    async fn recognize_stream(&self, /* ... */) -> Result<()>;
    fn reset(&self);
    fn clone_box(&self) -> Box<dyn Asr>;
}

#[async_trait]
pub trait Tts: Send + Sync { /* synthesize / synthesize_stream */ }

#[async_trait]
pub trait Vad: Send + Sync { /* is_speech */ }

#[async_trait]
pub trait Agent: Send + Sync { /* stream generation + interrupt */ }

#[async_trait]
pub trait TurnDetector: Send + Sync { /* detect */ }
```

第一阶段必做实现：

- `DummyAsr` / `DummyTts` / `DummyVad` / `DummyAgent`
- `HttpChatAgent`（OpenAI-compatible chat completions streaming）作为第一个“真” LLM 后端

明确延后：本地 FunASR/CosyVoice/IndexTTS 原生绑定；需要时通过 `Http*Adapter` 或 Python sidecar 进程协议接入。

### 5.4 Pipeline（`xtalk-pipeline`）

- `trait Pipeline: Send + Sync { fn clone_session(&self) -> Box<dyn Pipeline>; getters... }`
- `DefaultPipeline`：持有 `Option<Box<dyn Asr>>` 等槽位，对应 Python `DefaultPipeline` 字段。
- 会话隔离规则：有状态模型必须 `clone_session`；无状态可共享 `Arc`。

### 5.5 Managers & Service（`xtalk-serving`）

Manager 用组合 + 显式注册替代 Python 的 decorator metaclass：

```rust
pub trait Manager: Send + Sync {
    fn name(&self) -> &'static str;
    fn register(&self, bus: &EventBus, pipeline: &dyn Pipeline);
}
```

第一阶段 Manager 集合（主路径最小闭环）：

1. `InputGateway` — WS → 事件
2. `OutputGateway` — 事件 → WS
3. `VadManager`
4. `AsrManager`（含预缓冲 padding 行为，对齐现有语义）
5. `LlmAgentContextManager` / `LlmAgentGenerationManager`
6. `TtsManager` + `TtsPlaybackManager`（可先合并简化，但事件面保持）
7. `TurnTakingManager`（打断/停止的最小集）

第二阶段再补：captioner、enhancer、speaker、recording、persistence、latency、embeddings、turn_detector。

`Service` 职责：

- 创建 session id、clone pipeline、建 bus、实例化 managers、register
- `run()` 直到 WS 关闭或主动 stop
- 暴露与 Python 相同的“原型 Service + 每连接 clone”模式

`ServiceManager`：会话表、并发上限（`SessionLimiter`）。

### 5.6 公共 API 面（对标 `Xtalk`）

Rust 侧提供构建器，而不是照搬类名魔法：

```rust
let app = XtalkBuilder::from_config("config.json")?
    .build()?;
// mount into axum Router; accept websocket sessions
```

配置 JSON 尽量兼容现有 sample config 的子集：`asr` / `tts` / `llm_agent` / `vad` 的 `type` + `params`。未知 type 在启动时明确报错。

---

## 6. 线协议与前端兼容

- 复用现有 `frontend` dist / sample_app 静态页做集成验证。
- `xtalk-protocol` 固化：
  - 二进制音频帧格式（采样率约定：上行 16 kHz PCM，下行常见 48 kHz）
  - JSON 控制消息（VAD start/end、playback confirm、session config、response update 等）
- 若 Rust 早期输出字段少于 Python，OutputGateway 填默认值，避免前端崩。
- **不在第一阶段改前端 API**；若必须改，单开兼容开关。

---

## 7. 并发、错误与打断

- 运行时：`tokio` 多线程。
- 打断路径：用户 VAD/speech 触发 → TurnTaking 发 stop/pause → ASR/Agent/TTS 协作取消；用 `CancellationToken`（或等价）贯穿生成与合成任务。
- Manager handler 内错误：记录日志并 `publish(ErrorOccurred)`；不得拖垮整个 bus。
- 会话结束：统一 `Drop`/显式 `shutdown`，取消任务、关模型流、移出会话表。

---

## 8. 测试策略

| 层级 | 内容 |
|------|------|
| 单测 | EventBus 优先级/错误限流；事件序列化往返；Dummy 模型 |
| 集成测 | 内存中启动 Service，注入音频帧事件，断言 ASR→Agent→TTS 事件序 |
| 协议测 | 对录制的前端消息 fixture 做编解码金丝雀 |
| 手工/E2E | `dummy_server` + 现有 sample 前端点通麦克风路径 |

CI：在 `.github/workflows` 增加 `rust/` 的 `cargo fmt` / `clippy` / `test`（不影响现有 Python 检查）。

---

## 9. 分阶段交付

### Phase 0 — 骨架（本设计落地后的第一实施计划）

- Cargo workspace + 空壳 crate
- `xtalk-events` + `xtalk-bus` + 单测
- Dummy Pipeline/Service
- axum WS echo → 再接到 Input/OutputGateway

### Phase 1 — 可演示闭环

- Dummy ASR/TTS/Agent 全路径
- 配置加载子集
- 与 sample 前端联调（打断的最小行为）

### Phase 2 — 真模型接入

- OpenAI-compatible Agent
- 至少一种远程 ASR/TTS HTTP 适配（或 Python sidecar）
- Turn detector / persistence 按需

### Phase 3 — 深化迁移

- 性能关键路径（音频缓冲、播放进度）Rust 原生优化
- 评估哪些 Python 模型值得 native/FFI
- 文档与示例全面切换“Rust 为推荐运行时”

---

## 10. 明确不做（YAGNI）

- 不翻译整个 `llm_agent/default.py` 的 LangChain 工具生态到 Phase 1。
- 不重写 frontend 为 Rust/WASM。
- 不删除或停止维护 Python 包，直到 Rust 运行时达到功能 parity 并由维护者宣布。
- 不做跨语言共享内存的复杂零拷贝方案（Phase 1 用 bytes/`Bytes` 即可）。

---

## 11. 风险与缓解

| 风险 | 缓解 |
|------|------|
| 线协议细节与前端耦合深 | 先录 fixture；OutputGateway 兼容层 |
| async 取消语义与 Python 不一致 | 用集成测试固定打断场景 |
| 模型生态仍在 Python | sidecar/HTTP 适配，避免阻塞运行时迁移 |
| 双栈维护成本 | 阶段门禁：每阶段必须有可演示产物 |

---

## 12. 文档与协作约定

- 设计与计划文档放在 `docs/superpowers/`。
- 用户可见教程后续补 `docs/tutorial/*` 的中英版本。
- Commit 前缀遵循仓库：`feature:` / `docs:` / `refactor:` / `fix:` / `chore:`。
- Rust 代码：`rustfmt` + `clippy`；公共 API 写文档注释。

---

## 13. 结论

在 fork 上以 **分层 Rust 运行时移植** 推进：先搬 EventBus / Pipeline / Service / 主路径 Managers 与线协议，用 Dummy + HTTP 模型跑通现有前端；Python 保留为对照与重模型后端。这是可交付的“迁移翻译版”，而不是不可完成的全量逐行翻译。
