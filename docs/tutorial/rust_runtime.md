# Rust runtime (Phase 1)

This tutorial shows how to run the experimental **Rust** X-Talk server (`dummy_server`) and point the existing sample frontend at its WebSocket endpoint. The browser client API is unchanged; Rust only speaks the same wire protocol.

## Run the Rust dummy server

From the repository root:

```bash
cd rust && cargo run -p dummy_server -- --config examples/dummy_server/config.dummy.json
```

By default this listens on `0.0.0.0:11995`. The WebSocket path is:

```text
ws://localhost:11995/ws
```

A quick protocol check with websocat (optional):

```bash
websocat ws://127.0.0.1:11995/ws
# expect a session_attached frame, then send:
{"action":"ping","timestamp":1}
# expect a pong reply
```

## Point the sample page at the Rust WS URL

1. **Build the frontend dist** (if you want a local `xtalk-client` instead of the CDN fallback used by the sample page):

   ```bash
   cd frontend && npm install && npm run build
   ```

   The sample page at `examples/sample_app/static/js/index.js` tries `../../xtalk/index.js` first, then falls back to the unpkg CDN.

2. **Override the WebSocket URL** so the page talks to Rust instead of whatever host served the HTML. In `examples/sample_app/static/js/index.js`, change `getWebSocketURL()` temporarily to:

   ```js
   function getWebSocketURL() {
       return new URL("ws://localhost:11995/ws");
   }
   ```

   Do not change the published `xtalk-client` API; this is a local demo override only.

3. Serve the sample static assets however you normally do (for example via the Python sample app or a static file server), open the page, and start a session. The Rust server accepts the `access_token` query param but does not enforce it (auth is disabled).

> Note: `createSession().open()` still expects a successful `POST /api/auth/login`. The Rust `dummy_server` does **not** mount that HTTP route yet. For a full browser path you still need a login stub (or a Python auth endpoint) while pointing only the WS URL at Rust; otherwise use websocat / a minimal WS client for protocol checks.

## Supported model `type` values

Config slots (`asr` / `tts` / `llm_agent` / `vad`) currently accept:

| `type` | Status |
|--------|--------|
| `dummy` | Supported now |
| `openai_compat` | Coming next (HTTP chat agent) |

Unknown types fail at server startup.

## Differences vs the Python service

| Topic | Python | Rust Phase 1 |
|-------|--------|--------------|
| Auth | Login + token enforcement | Auth **disabled** by default; tokens ignored |
| Captioner | Optional managers | **Not implemented** |
| Recording | `session_config` → RecordingManager | Parsed inbound; **no recording manager** |
| Latency metrics | `latency_metrics` outbound frames | **Not sent** (avoids incomplete objects) |
| HTTP surface | `/api/auth/login`, sessions, upload, static | `GET /` hint + `WS /ws` only |
| Models | Many ASR/TTS/LLM backends | `dummy` only for now |

Inbound fixtures used by protocol canaries live under `rust/crates/xtalk-protocol/src/fixtures/` (for example `ping.json`, `session_config.json`).
