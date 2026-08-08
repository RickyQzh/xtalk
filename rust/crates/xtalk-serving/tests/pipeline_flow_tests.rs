use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use xtalk_events::{Event, EventMeta};
use xtalk_models::{Agent, Asr, DummyAgent, DummyAsr, DummyTts, ModelError, Tts};
use xtalk_pipeline::DefaultPipeline;
use xtalk_serving::{
    AsrManager, LlmAgentContextManager, LlmAgentGenerationManager, Manager, Service, TtsManager,
    TurnTakingManager,
};

/// TTS that records synthesize inputs in order (for FIFO regression).
struct RecordingTts {
    sample_rate: u32,
    seen: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Tts for RecordingTts {
    async fn synthesize_stream(&self, text: &str) -> Result<Vec<Vec<u8>>, ModelError> {
        self.seen.lock().unwrap().push(text.to_string());
        let bytes = (self.sample_rate as usize) / 10 * 2;
        Ok(vec![vec![0u8; bytes]])
    }

    fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    fn clone_box(&self) -> Box<dyn Tts> {
        Box::new(RecordingTts {
            sample_rate: self.sample_rate,
            seen: Arc::clone(&self.seen),
        })
    }
}

/// TTS that fails after `tts.started` so error terminal-event accounting is testable.
struct FailingTts {
    sample_rate: u32,
}

#[async_trait]
impl Tts for FailingTts {
    async fn synthesize_stream(&self, text: &str) -> Result<Vec<Vec<u8>>, ModelError> {
        let _ = text;
        Err(ModelError::message("synth boom"))
    }

    fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    fn clone_box(&self) -> Box<dyn Tts> {
        Box::new(FailingTts {
            sample_rate: self.sample_rate,
        })
    }
}

fn record_type_names(bus: &xtalk_bus::EventBus, sink: Arc<Mutex<Vec<String>>>, types: &[&str]) {
    for type_name in types {
        let sink = Arc::clone(&sink);
        bus.subscribe(
            type_name,
            // Record before nested handlers so type_name order matches publish order.
            1000,
            Arc::new(move |ev| {
                let sink = Arc::clone(&sink);
                Box::pin(async move {
                    sink.lock().unwrap().push(ev.type_name().to_string());
                })
            }),
        );
    }
}

fn assert_subsequence(haystack: &[String], needle: &[&str]) {
    let mut from = 0;
    for expected in needle {
        let pos = haystack[from..]
            .iter()
            .position(|t| t == expected)
            .unwrap_or_else(|| {
                panic!(
                    "missing `{expected}` after index {from} in sequence:\n  {haystack:?}\nexpected subsequence:\n  {needle:?}"
                )
            });
        from += pos + 1;
    }
}

async fn wait_until(sink: &Mutex<Vec<String>>, predicate: impl Fn(&[String]) -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        {
            let names = sink.lock().unwrap().clone();
            if predicate(&names) {
                return;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            let names = sink.lock().unwrap().clone();
            panic!("timed out waiting for event sequence; got: {names:?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn asr_to_agent_to_tts_event_order() {
    let asr: Arc<dyn Asr> = Arc::new(DummyAsr::new("hello"));
    let agent: Arc<dyn Agent> = Arc::new(DummyAgent::new("reply text"));
    let tts: Arc<dyn Tts> = Arc::new(DummyTts::new(48_000));

    let managers: Vec<Arc<dyn Manager>> = vec![
        Arc::new(AsrManager::new("s1", Arc::clone(&asr))),
        Arc::new(LlmAgentContextManager::new("s1")),
        Arc::new(LlmAgentGenerationManager::new("s1", Arc::clone(&agent))),
        Arc::new(TtsManager::new("s1", Arc::clone(&tts))),
        Arc::new(TurnTakingManager::new("s1")),
    ];

    let pipeline = Box::new(
        DefaultPipeline::builder()
            .asr(asr.clone_box())
            .agent(agent.clone_box())
            .tts(tts.clone_box())
            .build(),
    );
    let service = Service::new("s1", pipeline, managers);
    let bus = service.bus();

    let names = Arc::new(Mutex::new(Vec::<String>::new()));
    record_type_names(
        &bus,
        Arc::clone(&names),
        &[
            "asr.result_final",
            "response.update",
            "response.finish",
            "tts.started",
            "tts.chunk_ready",
            "tts.finished",
        ],
    );

    let meta = EventMeta::new("s1");
    bus.publish(Event::VadSpeechStart {
        meta: meta.clone(),
        origin: "client".into(),
    })
    .await;
    bus.publish(Event::AudioFrameReceived {
        meta: meta.clone(),
        audio_data: vec![1; 320],
        sample_rate: 16_000,
    })
    .await;
    bus.publish(Event::VadSpeechEnd {
        meta: meta.clone(),
        origin: "client".into(),
    })
    .await;

    wait_until(&names, |n| n.iter().any(|t| t == "tts.finished")).await;

    let got = names.lock().unwrap().clone();
    assert_subsequence(
        &got,
        &[
            "asr.result_final",
            "response.update",
            "response.finish",
            "tts.started",
            "tts.chunk_ready",
            "tts.finished",
        ],
    );
}

#[tokio::test]
async fn multi_response_update_fifo_order() {
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let tts: Arc<dyn Tts> = Arc::new(RecordingTts {
        sample_rate: 48_000,
        seen: Arc::clone(&seen),
    });

    let managers: Vec<Arc<dyn Manager>> = vec![
        Arc::new(TtsManager::new("s1", Arc::clone(&tts))),
        Arc::new(TurnTakingManager::new("s1")),
    ];

    let pipeline = Box::new(DefaultPipeline::builder().tts(tts.clone_box()).build());
    let service = Service::new("s1", pipeline, managers);
    let bus = service.bus();

    let names = Arc::new(Mutex::new(Vec::<String>::new()));
    record_type_names(
        &bus,
        Arc::clone(&names),
        &[
            "tts.started",
            "tts.chunk_ready",
            "tts.finished",
            "tts.stopped",
        ],
    );

    let meta = EventMeta::new("s1");
    bus.publish(Event::ResponseUpdate {
        meta: meta.clone(),
        text: "one".into(),
    })
    .await;
    bus.publish(Event::ResponseUpdate {
        meta: meta.clone(),
        text: "two".into(),
    })
    .await;
    bus.publish(Event::ResponseUpdate {
        meta: meta.clone(),
        text: "three".into(),
    })
    .await;

    wait_until(&names, |n| n.iter().any(|t| t == "tts.finished")).await;

    let texts = seen.lock().unwrap().clone();
    assert_eq!(texts, vec!["one", "two", "three"]);

    let got = names.lock().unwrap().clone();
    assert_subsequence(
        &got,
        &[
            "tts.started",
            "tts.chunk_ready",
            "tts.chunk_ready",
            "tts.chunk_ready",
            "tts.finished",
        ],
    );
    assert!(
        !got.iter().any(|t| t == "tts.stopped"),
        "happy-path FIFO must not emit tts.stopped; got {got:?}"
    );
}

#[tokio::test]
async fn tts_error_emits_terminal_and_disarms_barge_in() {
    let tts: Arc<dyn Tts> = Arc::new(FailingTts {
        sample_rate: 48_000,
    });

    let managers: Vec<Arc<dyn Manager>> = vec![
        Arc::new(TtsManager::new("s1", Arc::clone(&tts))),
        Arc::new(TurnTakingManager::new("s1")),
    ];

    let pipeline = Box::new(DefaultPipeline::builder().tts(tts.clone_box()).build());
    let service = Service::new("s1", pipeline, managers);
    let bus = service.bus();

    let names = Arc::new(Mutex::new(Vec::<String>::new()));
    record_type_names(
        &bus,
        Arc::clone(&names),
        &[
            "tts.started",
            "error.occurred",
            "tts.stopped",
            "tts.finished",
            "turn.tts_stop_requested",
        ],
    );

    let meta = EventMeta::new("s1");
    bus.publish(Event::ResponseUpdate {
        meta: meta.clone(),
        text: "boom".into(),
    })
    .await;

    wait_until(&names, |n| {
        n.iter().any(|t| t == "error.occurred")
            && (n.iter().any(|t| t == "tts.stopped") || n.iter().any(|t| t == "tts.finished"))
    })
    .await;

    // Depth must be zero: barge-in after error should not fire stop.
    bus.publish(Event::VadSpeechStart {
        meta: meta.clone(),
        origin: "client".into(),
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let got = names.lock().unwrap().clone();
    assert!(
        got.iter().any(|t| t == "tts.started"),
        "expected tts.started; got {got:?}"
    );
    assert!(
        got.iter().any(|t| t == "error.occurred"),
        "expected error.occurred; got {got:?}"
    );
    assert!(
        got.iter().any(|t| t == "tts.stopped"),
        "expected tts.stopped terminal after synth error; got {got:?}"
    );
    assert!(
        !got.iter().any(|t| t == "turn.tts_stop_requested"),
        "barge-in must stay disarmed after error terminal; got {got:?}"
    );
}
