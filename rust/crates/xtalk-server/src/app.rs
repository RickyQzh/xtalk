//! Axum application: HTTP root + WebSocket `/ws` session loop.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Query, State, WebSocketUpgrade};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tracing::{info, warn};
use xtalk_bus::EventBus;
use xtalk_models::{Agent, Asr, Tts, Vad};
use xtalk_pipeline::Pipeline;
use xtalk_serving::{
    AsrManager, InputGateway, LimitError, LlmAgentContextManager, LlmAgentGenerationManager,
    Manager, ManagerBundle, OutputGateway, ServiceManager, TtsManager, TurnTakingManager,
    VadManager, WsSink,
};

use crate::auth::{AuthMode, WsAuthQuery};
use crate::config::{build_pipeline, ServerConfig};

/// Shared axum state.
#[derive(Clone)]
pub struct AppState {
    pub service_manager: Arc<ServiceManager>,
    pub auth: AuthMode,
}

/// Outbound frames queued for the WebSocket writer task.
enum WsOut {
    Text(String),
    Binary(Bytes),
}

/// [`WsSink`] backed by an mpsc channel (writer task owns the real sink).
struct MpscWsSink {
    tx: mpsc::Sender<WsOut>,
}

#[async_trait::async_trait]
impl WsSink for MpscWsSink {
    async fn send_text(&self, text: String) {
        let _ = self.tx.send(WsOut::Text(text)).await;
    }

    async fn send_bytes(&self, data: Bytes) {
        let _ = self.tx.send(WsOut::Binary(data)).await;
    }
}

/// Build the default per-session manager stack from a pipeline clone.
pub fn build_session_managers(
    session_id: &str,
    pipeline: &dyn Pipeline,
    bus: Arc<EventBus>,
    sink: Arc<dyn WsSink>,
) -> ManagerBundle {
    let input_gateway = Arc::new(InputGateway::new(
        session_id.to_string(),
        Arc::clone(&bus),
        Arc::clone(&sink),
    ));
    let output_gateway = Arc::new(OutputGateway::new(
        session_id.to_string(),
        Arc::clone(&sink),
    ));

    let mut managers: Vec<Arc<dyn Manager>> = vec![
        Arc::clone(&input_gateway) as Arc<dyn Manager>,
        Arc::clone(&output_gateway) as Arc<dyn Manager>,
    ];

    // Optional server VAD (DummyVad is fine; client VAD still drives the turn).
    let vad: Option<Arc<dyn Vad>> = pipeline.vad().map(|v| Arc::from(v.clone_box()));
    managers.push(Arc::new(VadManager::new(session_id, vad)));

    if let Some(asr) = pipeline.asr() {
        let asr: Arc<dyn Asr> = Arc::from(asr.clone_box());
        managers.push(Arc::new(AsrManager::new(session_id, asr)));
    }

    managers.push(Arc::new(LlmAgentContextManager::new(session_id)));

    if let Some(agent) = pipeline.agent() {
        let agent: Arc<dyn Agent> = Arc::from(agent.clone_box());
        managers.push(Arc::new(LlmAgentGenerationManager::new(session_id, agent)));
    }

    if let Some(tts) = pipeline.tts() {
        let tts: Arc<dyn Tts> = Arc::from(tts.clone_box());
        managers.push(Arc::new(TtsManager::new(session_id, tts)));
    }

    managers.push(Arc::new(TurnTakingManager::new(session_id)));

    ManagerBundle {
        managers,
        input_gateway,
        output_gateway,
    }
}

/// Construct [`AppState`] + router from a loaded [`ServerConfig`].
pub fn build_app(cfg: &ServerConfig) -> Result<(Router, AppState), crate::config::ConfigError> {
    let pipeline = build_pipeline(cfg)?;
    let pipeline = Arc::new(pipeline);
    let pipeline_factory: xtalk_serving::PipelineFactory = Arc::new({
        let pipeline = Arc::clone(&pipeline);
        move || pipeline.clone_session()
    });

    let service_manager = Arc::new(
        ServiceManager::new(cfg.max_sessions, pipeline_factory)
            .with_manager_factory(Arc::new(build_session_managers)),
    );

    let state = AppState {
        service_manager,
        auth: AuthMode::Disabled,
    };

    let router = Router::new()
        .route("/", get(root_handler))
        .route("/ws", get(ws_upgrade_handler))
        .with_state(state.clone());

    Ok((router, state))
}

async fn root_handler() -> Html<&'static str> {
    Html(
        "<!doctype html><html><body>\
         <h1>xtalk-server</h1>\
         <p>WebSocket endpoint: <code>/ws</code></p>\
         </body></html>",
    )
}

async fn ws_upgrade_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<WsAuthQuery>,
    State(state): State<AppState>,
) -> Response {
    let token = state.auth.extract_access_token(&query, None);
    if let Err(err) = state.auth.authorize_ws(token) {
        warn!(error = %err, "websocket auth rejected");
        return (
            axum::http::StatusCode::UNAUTHORIZED,
            format!("unauthorized: {err}"),
        )
            .into_response();
    }

    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::channel::<WsOut>(64);

    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let result = match msg {
                WsOut::Text(text) => sink.send(Message::Text(text)).await,
                WsOut::Binary(data) => sink.send(Message::Binary(data.to_vec())).await,
            };
            if result.is_err() {
                break;
            }
        }
    });

    let ws_sink: Arc<dyn WsSink> = Arc::new(MpscWsSink { tx });

    let service = match state.service_manager.connect(Some(ws_sink), None).await {
        Ok(service) => service,
        Err(LimitError::Full) => {
            warn!("session limit reached; rejecting websocket");
            writer.abort();
            return;
        }
    };

    let session_id = service.session_id.clone();
    info!(%session_id, "session connected");

    service.send_session_attached().await;

    let input = match service.input_gateway() {
        Some(gw) => gw,
        None => {
            warn!(%session_id, "missing input gateway");
            state.service_manager.disconnect(&session_id).await;
            writer.abort();
            return;
        }
    };

    while let Some(Ok(msg)) = stream.next().await {
        match msg {
            Message::Text(text) => {
                if let Err(err) = input.handle_text(&text).await {
                    warn!(%session_id, error = %err, "inbound text protocol error");
                }
            }
            Message::Binary(data) => {
                // Frontend streams 16 kHz PCM s16le mono.
                input.handle_binary(Bytes::from(data), 16_000).await;
            }
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Close(_) => break,
        }
    }

    info!(%session_id, "session disconnecting");
    state.service_manager.disconnect(&session_id).await;
    writer.abort();
}

/// Bind and serve the axum app until the process is stopped.
pub async fn serve(cfg: ServerConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listen = cfg.listen.clone();
    let (router, _state) = build_app(&cfg)?;
    let addr: SocketAddr = listen.parse()?;
    info!(%addr, "xtalk-server listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router).await?;
    Ok(())
}
