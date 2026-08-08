use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use xtalk_bus::EventBus;
use xtalk_events::{Event, EventMeta};
use xtalk_serving::{InputGateway, Manager, OutputGateway, WsSink};

#[derive(Default)]
struct MockSink {
    texts: Mutex<Vec<String>>,
    bins: Mutex<Vec<Vec<u8>>>,
}

#[async_trait]
impl WsSink for MockSink {
    async fn send_text(&self, text: String) {
        self.texts.lock().unwrap().push(text);
    }

    async fn send_bytes(&self, data: Bytes) {
        self.bins.lock().unwrap().push(data.to_vec());
    }
}

#[tokio::test]
async fn input_vad_start_publishes_event() {
    let bus = Arc::new(EventBus::new());
    let seen = Arc::new(Mutex::new(false));
    let seen2 = Arc::clone(&seen);
    bus.subscribe(
        "vad.speech_start",
        0,
        Arc::new(move |ev| {
            let seen2 = Arc::clone(&seen2);
            Box::pin(async move {
                if let Event::VadSpeechStart { origin, .. } = ev {
                    assert_eq!(origin, "client");
                }
                *seen2.lock().unwrap() = true;
            })
        }),
    );

    let sink: Arc<dyn WsSink> = Arc::new(MockSink::default());
    let gw = InputGateway::new("s1".into(), Arc::clone(&bus), sink);
    gw.handle_text(r#"{"action":"vad_speech_start"}"#)
        .await
        .unwrap();
    assert!(*seen.lock().unwrap());
}

#[tokio::test]
async fn input_ping_sends_pong() {
    let bus = Arc::new(EventBus::new());
    let sink = Arc::new(MockSink::default());
    let sink_dyn: Arc<dyn WsSink> = sink.clone();
    let gw = InputGateway::new("s1".into(), bus, sink_dyn);
    gw.handle_text(r#"{"action":"ping","timestamp":1.5}"#)
        .await
        .unwrap();
    let texts = sink.texts.lock().unwrap();
    assert!(
        texts.iter().any(|t| t.contains("pong")),
        "expected pong in {texts:?}"
    );
}

#[tokio::test]
async fn input_binary_publishes_audio_frame() {
    let bus = Arc::new(EventBus::new());
    let got = Arc::new(Mutex::new(None));
    let got2 = Arc::clone(&got);
    bus.subscribe(
        "audio.frame_received",
        0,
        Arc::new(move |ev| {
            let got2 = Arc::clone(&got2);
            Box::pin(async move {
                if let Event::AudioFrameReceived {
                    audio_data,
                    sample_rate,
                    ..
                } = ev
                {
                    *got2.lock().unwrap() = Some((audio_data, sample_rate));
                }
            })
        }),
    );

    let sink: Arc<dyn WsSink> = Arc::new(MockSink::default());
    let gw = InputGateway::new("s1".into(), Arc::clone(&bus), sink);
    gw.handle_binary(Bytes::from_static(b"\x01\x02"), 16000)
        .await;
    let (data, rate) = got.lock().unwrap().clone().expect("audio frame");
    assert_eq!(data, vec![1, 2]);
    assert_eq!(rate, 16000);
}

#[tokio::test]
async fn output_forwards_asr_final() {
    let bus = Arc::new(EventBus::new());
    let sink = Arc::new(MockSink::default());
    let sink_dyn: Arc<dyn WsSink> = sink.clone();
    let out = OutputGateway::new("s1".into(), sink_dyn);
    Arc::new(out).register(Arc::clone(&bus));
    bus.publish(Event::AsrResultFinal {
        meta: EventMeta::new("s1"),
        text: "hi".into(),
        display_text: "hi".into(),
        speech_pause: false,
    })
    .await;
    let texts = sink.texts.lock().unwrap();
    assert!(texts
        .iter()
        .any(|t| t.contains("finish_asr") && t.contains("hi")));
}

#[tokio::test]
async fn output_send_session_attached() {
    let sink = Arc::new(MockSink::default());
    let sink_dyn: Arc<dyn WsSink> = sink.clone();
    let out = OutputGateway::new("s1".into(), sink_dyn);
    out.send_session_attached().await;
    let texts = sink.texts.lock().unwrap();
    assert!(
        texts
            .iter()
            .any(|t| t.contains("session_attached") && t.contains("s1")),
        "expected session_attached in {texts:?}"
    );
}

#[tokio::test]
async fn output_forwards_tts_chunk_as_bytes() {
    let bus = Arc::new(EventBus::new());
    let sink = Arc::new(MockSink::default());
    let sink_dyn: Arc<dyn WsSink> = sink.clone();
    let out = OutputGateway::new("s1".into(), sink_dyn);
    Arc::new(out).register(Arc::clone(&bus));
    bus.publish(Event::TtsChunkReady {
        meta: EventMeta::new("s1"),
        audio_chunk: vec![9, 8, 7],
        sample_rate: 48000,
    })
    .await;
    let bins = sink.bins.lock().unwrap();
    assert_eq!(*bins, vec![vec![9, 8, 7]]);
}
