//! Frontend wire-protocol canaries: outbound JSON keys the browser client expects.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;
use xtalk_bus::EventBus;
use xtalk_events::{Event, EventMeta};
use xtalk_serving::{Manager, OutputGateway, WsSink};

#[derive(Default)]
struct MockSink {
    texts: Mutex<Vec<String>>,
}

#[async_trait]
impl WsSink for MockSink {
    async fn send_text(&self, text: String) {
        self.texts.lock().unwrap().push(text);
    }

    async fn send_bytes(&self, _data: Bytes) {}
}

fn parse_actions(texts: &[String]) -> Vec<Value> {
    texts
        .iter()
        .map(|t| serde_json::from_str(t).expect("outbound must be JSON"))
        .collect()
}

fn find_action<'a>(msgs: &'a [Value], action: &str) -> &'a Value {
    msgs.iter()
        .find(|v| v["action"] == action)
        .unwrap_or_else(|| panic!("missing action {action} in {msgs:?}"))
}

#[tokio::test]
async fn finish_asr_data_contains_text() {
    let bus = Arc::new(EventBus::new());
    let sink = Arc::new(MockSink::default());
    let sink_dyn: Arc<dyn WsSink> = sink.clone();
    Arc::new(OutputGateway::new("s1".into(), sink_dyn)).register(Arc::clone(&bus));

    bus.publish(Event::AsrResultFinal {
        meta: EventMeta::new("s1"),
        text: "hello".into(),
        display_text: String::new(), // empty display → default to text
        speech_pause: false,
    })
    .await;

    let msgs = parse_actions(&sink.texts.lock().unwrap());
    let msg = find_action(&msgs, "finish_asr");
    let data = &msg["data"];
    assert!(data.is_object(), "finish_asr data must be an object");
    assert!(
        data.get("text").and_then(|v| v.as_str()).is_some(),
        "finish_asr data must contain text key, got {data}"
    );
    assert_eq!(data["text"], "hello");
}

#[tokio::test]
async fn update_resp_data_contains_text() {
    let bus = Arc::new(EventBus::new());
    let sink = Arc::new(MockSink::default());
    let sink_dyn: Arc<dyn WsSink> = sink.clone();
    Arc::new(OutputGateway::new("s1".into(), sink_dyn)).register(Arc::clone(&bus));

    bus.publish(Event::ResponseUpdate {
        meta: EventMeta::new("s1"),
        text: "partial".into(),
    })
    .await;

    let msgs = parse_actions(&sink.texts.lock().unwrap());
    let msg = find_action(&msgs, "update_resp");
    let data = &msg["data"];
    assert!(data.is_object(), "update_resp data must be an object");
    assert!(
        data.get("text").and_then(|v| v.as_str()).is_some(),
        "update_resp data must contain text key, got {data}"
    );
    assert_eq!(data["text"], "partial");
}

#[tokio::test]
async fn session_attached_data_contains_session_id() {
    let sink = Arc::new(MockSink::default());
    let sink_dyn: Arc<dyn WsSink> = sink.clone();
    let out = OutputGateway::new("sess-42".into(), sink_dyn);
    out.send_session_attached().await;

    let msgs = parse_actions(&sink.texts.lock().unwrap());
    let msg = find_action(&msgs, "session_attached");
    let data = &msg["data"];
    assert!(data.is_object(), "session_attached data must be an object");
    assert!(
        data.get("session_id").and_then(|v| v.as_str()).is_some(),
        "session_attached data must contain session_id, got {data}"
    );
    assert_eq!(data["session_id"], "sess-42");
}

#[tokio::test]
async fn does_not_send_incomplete_latency_metrics() {
    // Latency metrics are not implemented in the Rust Phase-1 stack.
    // Do not emit a partial `latency_metrics` object (frontend expects all ms fields).
    let bus = Arc::new(EventBus::new());
    let sink = Arc::new(MockSink::default());
    let sink_dyn: Arc<dyn WsSink> = sink.clone();
    Arc::new(OutputGateway::new("s1".into(), sink_dyn)).register(Arc::clone(&bus));

    // Drive a representative turn so any accidental latency hook would fire.
    bus.publish(Event::AsrResultFinal {
        meta: EventMeta::new("s1"),
        text: "hi".into(),
        display_text: "hi".into(),
        speech_pause: false,
    })
    .await;
    bus.publish(Event::ResponseUpdate {
        meta: EventMeta::new("s1"),
        text: "yo".into(),
    })
    .await;
    bus.publish(Event::ResponseFinish {
        meta: EventMeta::new("s1"),
        text: "yo".into(),
    })
    .await;
    bus.publish(Event::TtsStarted {
        meta: EventMeta::new("s1"),
    })
    .await;
    bus.publish(Event::TtsFinished {
        meta: EventMeta::new("s1"),
    })
    .await;
    // Even an extension payload must not become a half-baked latency_metrics frame.
    bus.publish(Event::Extension {
        meta: EventMeta::new("s1"),
        type_name: "latency.metrics_updated".into(),
        payload: serde_json::json!({ "asr_latency_ms": 1 }),
    })
    .await;

    let msgs = parse_actions(&sink.texts.lock().unwrap());
    let leaked: Vec<_> = msgs
        .iter()
        .filter(|v| v["action"] == "latency_metrics")
        .collect();
    assert!(
        leaked.is_empty(),
        "must not send incomplete latency_metrics, got {leaked:?}"
    );
}
