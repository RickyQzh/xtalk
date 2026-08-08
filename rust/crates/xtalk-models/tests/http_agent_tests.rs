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

    assert_eq!(out, vec!["Hello".to_string(), " world".to_string()]);
}

#[tokio::test]
async fn stops_streaming_on_cancel() {
    let server = MockServer::start().await;

    // Large / delayed-looking stream; cancel before accept finishes reading.
    let body = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\" more\"}}]}\n\n",
        "data: [DONE]\n\n",
    );
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(body),
        )
        .mount(&server)
        .await;

    let agent = HttpChatAgent::new(format!("{}/v1", server.uri()), "k", "m");
    let cancel = CancellationToken::new();
    cancel.cancel();
    let err = agent
        .accept(
            AgentContext {
                context_type: "asr_final".into(),
                text: "hi".into(),
            },
            cancel,
        )
        .await
        .expect_err("cancelled accept should error");
    assert!(
        matches!(err, xtalk_models::ModelError::Cancelled),
        "got {err:?}"
    );
}
