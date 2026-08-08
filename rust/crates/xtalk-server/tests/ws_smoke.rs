//! Integration smoke: ping → pong over `/ws`.

use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use xtalk_server::{build_app, ServerConfig};

#[tokio::test]
async fn ws_ping_receives_pong() {
    let cfg = ServerConfig::from_str(include_str!(
        "../../../examples/dummy_server/config.dummy.json"
    ))
    .expect("parse config");

    let (router, _state) = build_app(&cfg).expect("build app");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });

    // Give the server a moment to accept.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let url = format!("ws://{addr}/ws");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .unwrap_or_else(|err| panic!("connect {url}: {err}"));

    // session_attached first
    let first = recv_text(&mut ws).await;
    assert!(
        first.contains("session_attached"),
        "expected session_attached, got: {first}"
    );

    ws.send(Message::Text(r#"{"action":"ping","timestamp":1.5}"#.into()))
        .await
        .expect("send ping");

    let pong = recv_text(&mut ws).await;
    assert!(pong.contains("pong"), "expected pong, got: {pong}");

    let _ = ws.close(None).await;
}

async fn recv_text(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!("timed out waiting for text frame");
        }
        let next = tokio::time::timeout(remaining, ws.next())
            .await
            .expect("timeout")
            .expect("ws closed")
            .expect("ws error");
        match next {
            Message::Text(text) => return text,
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("unexpected frame: {other:?}"),
        }
    }
}

#[allow(dead_code)]
fn _addr_type_check(addr: SocketAddr) -> SocketAddr {
    addr
}
