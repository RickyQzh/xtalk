# Rust 运行时（Phase 1）

本教程说明如何启动实验性的 **Rust** X-Talk 服务（`dummy_server`），并把现有 sample 前端的 WebSocket 指向该端口。浏览器客户端 API 不变；Rust 只保证线协议兼容。

## 启动 Rust dummy 服务

在仓库根目录执行：

```bash
cd rust && cargo run -p dummy_server -- --config examples/dummy_server/config.dummy.json
```

默认监听 `0.0.0.0:11995`。WebSocket 路径为：

```text
ws://localhost:11995/ws
```

可选的 websocat 协议冒烟：

```bash
websocat ws://127.0.0.1:11995/ws
# 应先收到 session_attached，然后发送：
{"action":"ping","timestamp":1}
# 应收到 pong
```

## 把 sample 页的 WS URL 指到 Rust

1. **构建 frontend dist**（若希望使用本地 `xtalk-client`，而不是 sample 页的 CDN 回退）：

   ```bash
   cd frontend && npm install && npm run build
   ```

   `examples/sample_app/static/js/index.js` 会先尝试加载 `../../xtalk/index.js`，失败再回退到 unpkg CDN。

2. **覆盖 WebSocket URL**，让页面连到 Rust 而不是托管 HTML 的主机。在 `examples/sample_app/static/js/index.js` 中临时修改 `getWebSocketURL()`：

   ```js
   function getWebSocketURL() {
       return new URL("ws://localhost:11995/ws");
   }
   ```

   不要改动已发布的 `xtalk-client` API；这只是本地演示用的覆盖。

3. 按往常方式托管 sample 静态资源（例如 Python sample app 或静态文件服务器），打开页面并开始会话。Rust 服务会接受 `access_token` 查询参数，但**不会校验**（auth 默认关闭）。

> 说明：`createSession().open()` 仍需要成功的 `POST /api/auth/login`。Rust `dummy_server` **尚未**挂载该 HTTP 路由。完整浏览器联调时，请继续用登录 stub（或 Python auth 端点），只把 WS URL 指到 Rust；否则可用 websocat / 最小 WS 客户端做协议检查。

## 当前支持的模型 `type`

配置槽位（`asr` / `tts` / `llm_agent` / `vad`）目前接受：

| `type` | 状态 |
|--------|------|
| `dummy` | 现已支持（ASR / TTS / VAD / Agent） |
| `openai_compat` | 现已支持（仅 `llm_agent`：`HttpChatAgent`） |

未知 type 会在服务启动时报错。

### HttpChatAgent（`openai_compat`）

`llm_agent.type: openai_compat` 会构建 `HttpChatAgent`，向 OpenAI 兼容的 `/chat/completions` 发起 **SSE 流式**请求。示例配置：

```bash
cd rust && cargo run -p dummy_server -- --config examples/dummy_server/config.openai.json
```

`examples/dummy_server/config.openai.json` 要点：

- `params.base_url`：例如 `https://api.openai.com/v1`
- `params.api_key`：支持 `${OPENAI_API_KEY}` 环境变量展开；缺失变量时启动失败并报 `MissingEnv`
- `params.model`：例如 `gpt-4o-mini`
- ASR / TTS / VAD 仍可用 `dummy`

设置密钥后启动：

```bash
export OPENAI_API_KEY=sk-...
cd rust && cargo run -p dummy_server -- --config examples/dummy_server/config.openai.json
```

打断时（barge-in）会 cancel 进行中的 HTTP 请求；取消路径覆盖慢 TTFB 与中途流式输出。

## 与 Python 服务的差异

| 主题 | Python | Rust Phase 1 |
|------|--------|--------------|
| Auth | 登录 + token 校验 | 默认 **关闭**；忽略 token |
| Captioner | 可选 manager | **未实现** |
| Recording | `session_config` → RecordingManager | 可解析入站；**无 recording manager** |
| 延迟指标 | 出站 `latency_metrics` | **不发送**（避免残缺对象） |
| HTTP 面 | `/api/auth/login`、sessions、upload、静态页 | 仅 `GET /` 提示 + `WS /ws` |
| 模型 | 多种 ASR/TTS/LLM | `dummy` + `llm_agent` 的 `openai_compat` |

协议金丝雀使用的入站 fixture 位于 `rust/crates/xtalk-protocol/src/fixtures/`（例如 `ping.json`、`session_config.json`）。
