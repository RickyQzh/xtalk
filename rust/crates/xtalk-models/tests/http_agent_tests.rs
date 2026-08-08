use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
use xtalk_models::{Agent, AgentContext, HttpChatAgent};

#[tokio::test]
async fn streams_chat_completion_deltas() {
    let server = MockServer::start().await;

    let body = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\" world\"}}]}\n\n",
        "data: [DONE]\n\n",
    );
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(body),
        )
        .mount(&server)
        .await;

    let agent = HttpChatAgent::new(format!("{}/v1", server.uri()), "test-key", "gpt-test");
    let cancel = CancellationToken::new();
    let out = agent
        .accept(
            AgentContext {
                context_type: "asr_final".into(),
                text: "hi".into(),
            },
            cancel,
        )
        .await
        .expect("accept should succeed");

    // Deltas are coalesced into one response part (not per-token fragments).
    assert_eq!(out, vec!["Hello world".to_string()]);
}

/// Minimal HTTP/1.1 server: responds with one SSE chunk, then stalls (never closes).
async fn spawn_stalling_sse_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stalling SSE server");
    let addr = listener.local_addr().expect("local_addr");

    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0u8; 8192];
        let mut req = Vec::new();
        loop {
            let n = sock.read(&mut buf).await.expect("read request");
            if n == 0 {
                return;
            }
            req.extend_from_slice(&buf[..n]);
            if req.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }

        // Oversized Content-Length so the client keeps waiting for more body bytes
        // after the first SSE event (mid-stream stall).
        let event = "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n";
        let header = format!(
            "HTTP/1.1 200 OK\r\n\
             content-type: text/event-stream\r\n\
             content-length: 1000000\r\n\
             connection: close\r\n\
             \r\n\
             {event}"
        );
        sock.write_all(header.as_bytes())
            .await
            .expect("write SSE headers+chunk");
        // Hold the connection open without sending further bytes.
        std::future::pending::<()>().await;
    });

    format!("http://{addr}/v1")
}

#[tokio::test]
async fn stops_streaming_on_cancel() {
    // Cancel *during* streaming (after first SSE chunk, while waiting for more),
    // not only before accept.
    let base_url = spawn_stalling_sse_server().await;

    let agent = HttpChatAgent::new(base_url, "k", "m");
    let cancel = CancellationToken::new();
    let cancel_mid = cancel.clone();

    let accept = tokio::spawn(async move {
        agent
            .accept(
                AgentContext {
                    context_type: "asr_final".into(),
                    text: "hi".into(),
                },
                cancel,
            )
            .await
    });

    // Allow connect + first chunk, then cancel while blocked on the next chunk.
    tokio::time::sleep(Duration::from_millis(150)).await;
    cancel_mid.cancel();

    let err = tokio::time::timeout(Duration::from_secs(2), accept)
        .await
        .expect("accept should return promptly after cancel")
        .expect("accept task should not panic")
        .expect_err("cancelled accept should error");
    assert!(
        matches!(err, xtalk_models::ModelError::Cancelled),
        "got {err:?}"
    );
}

#[tokio::test]
async fn stops_on_cancel_during_slow_send() {
    // Delayed wiremock body: cancel must abort `.send()` (slow TTFB), not hang.
    let server = MockServer::start().await;

    let body = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n",
        "data: [DONE]\n\n",
    );
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(body)
                .set_delay(Duration::from_secs(30)),
        )
        .mount(&server)
        .await;

    let agent = HttpChatAgent::new(format!("{}/v1", server.uri()), "k", "m");
    let cancel = CancellationToken::new();
    let cancel_mid = cancel.clone();

    let accept = tokio::spawn(async move {
        agent
            .accept(
                AgentContext {
                    context_type: "asr_final".into(),
                    text: "hi".into(),
                },
                cancel,
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    cancel_mid.cancel();

    let err = tokio::time::timeout(Duration::from_secs(2), accept)
        .await
        .expect("accept should return promptly after cancel")
        .expect("accept task should not panic")
        .expect_err("cancelled accept should error");
    assert!(
        matches!(err, xtalk_models::ModelError::Cancelled),
        "got {err:?}"
    );
}
