use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use xtalk_events::{Event, EventMeta};
use xtalk_models::{Agent, Asr, DummyAgent, DummyAsr, ModelError, Tts};
use xtalk_pipeline::DefaultPipeline;
use xtalk_serving::{
    AsrManager, LlmAgentContextManager, LlmAgentGenerationManager, Manager, Service, TtsManager,
    TurnTakingManager,
};

/// TTS that sleeps before returning silence — lets the interrupt race mid-flight.
struct SlowTts {
    delay: Duration,
    sample_rate: u32,
}

#[async_trait]
impl Tts for SlowTts {
    async fn synthesize_stream(&self, text: &str) -> Result<Vec<Vec<u8>>, ModelError> {
        let _ = text;
        tokio::time::sleep(self.delay).await;
        let bytes = (self.sample_rate as usize) / 10 * 2;
        Ok(vec![vec![0u8; bytes]])
    }

    fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    fn clone_box(&self) -> Box<dyn Tts> {
        Box::new(SlowTts {
            delay: self.delay,
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

async fn wait_until(sink: &Mutex<Vec<String>>, predicate: impl Fn(&[String]) -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
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
async fn speech_start_cancels_tts() {
    let asr: Arc<dyn Asr> = Arc::new(DummyAsr::new("hello"));
    let agent: Arc<dyn Agent> = Arc::new(DummyAgent::new("long reply"));
    let tts: Arc<dyn Tts> = Arc::new(SlowTts {
        delay: Duration::from_millis(500),
        sample_rate: 48_000,
    });

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
            "tts.started",
            "turn.tts_stop_requested",
            "turn.llm_agent_stop_requested",
            "tts.stopped",
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

    // Wait until TTS is active (slow synthesize in flight).
    wait_until(&names, |n| n.iter().any(|t| t == "tts.started")).await;

    // Barge-in while TTS is synthesizing.
    bus.publish(Event::VadSpeechStart {
        meta: meta.clone(),
        origin: "client".into(),
    })
    .await;

    wait_until(&names, |n| {
        n.iter().any(|t| t == "turn.tts_stop_requested") && n.iter().any(|t| t == "tts.stopped")
    })
    .await;

    let got = names.lock().unwrap().clone();
    assert!(
        got.iter().any(|t| t == "turn.tts_stop_requested"),
        "expected turn.tts_stop_requested; got {got:?}"
    );
    assert!(
        got.iter().any(|t| t == "tts.stopped"),
        "expected tts.stopped; got {got:?}"
    );
    // Interrupted path should not finish successfully.
    assert!(
        !got.iter().any(|t| t == "tts.finished"),
        "interrupted TTS should not emit tts.finished; got {got:?}"
    );
}
