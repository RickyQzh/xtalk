use std::sync::{Arc, Mutex};

use xtalk_bus::EventBus;
use xtalk_events::{Event, EventMeta};
use xtalk_models::{Asr, DummyAsr};
use xtalk_serving::{AsrManager, Manager, VadManager};

#[tokio::test]
async fn asr_final_after_speech_end() {
    let bus = Arc::new(EventBus::new());
    let finals = Arc::new(Mutex::new(Vec::<String>::new()));
    let finals2 = Arc::clone(&finals);
    bus.subscribe(
        "asr.result_final",
        0,
        Arc::new(move |ev| {
            let finals2 = Arc::clone(&finals2);
            Box::pin(async move {
                if let Event::AsrResultFinal { text, .. } = ev {
                    finals2.lock().unwrap().push(text);
                }
            })
        }),
    );

    let asr: Arc<dyn Asr> = Arc::new(DummyAsr::new("hello"));
    let mgr = AsrManager::new("s1", asr);
    Arc::new(mgr).register(Arc::clone(&bus));

    let meta = EventMeta::new("s1");
    for i in 0u8..5 {
        bus.publish(Event::AudioFrameReceived {
            meta: meta.clone(),
            audio_data: vec![i; 320],
            sample_rate: 16000,
        })
        .await;
    }

    bus.publish(Event::VadSpeechStart {
        meta: meta.clone(),
        origin: "client".into(),
    })
    .await;

    for i in 5u8..8 {
        bus.publish(Event::AudioFrameReceived {
            meta: meta.clone(),
            audio_data: vec![i; 320],
            sample_rate: 16000,
        })
        .await;
    }

    bus.publish(Event::VadSpeechEnd {
        meta: meta.clone(),
        origin: "client".into(),
    })
    .await;

    let got = finals.lock().unwrap().clone();
    assert_eq!(got, vec!["hello".to_string()]);
}

#[tokio::test]
async fn asr_start_end_via_turn_events() {
    let bus = Arc::new(EventBus::new());
    let finals = Arc::new(Mutex::new(Vec::<String>::new()));
    let finals2 = Arc::clone(&finals);
    bus.subscribe(
        "asr.result_final",
        0,
        Arc::new(move |ev| {
            let finals2 = Arc::clone(&finals2);
            Box::pin(async move {
                if let Event::AsrResultFinal { text, .. } = ev {
                    finals2.lock().unwrap().push(text);
                }
            })
        }),
    );

    let asr: Arc<dyn Asr> = Arc::new(DummyAsr::new("turn-ok"));
    Arc::new(AsrManager::new("s1", asr)).register(Arc::clone(&bus));

    let meta = EventMeta::new("s1");
    bus.publish(Event::AudioFrameReceived {
        meta: meta.clone(),
        audio_data: vec![1; 320],
        sample_rate: 16000,
    })
    .await;
    bus.publish(Event::TurnAsrStartRequested { meta: meta.clone() })
        .await;
    bus.publish(Event::AudioFrameReceived {
        meta: meta.clone(),
        audio_data: vec![2; 320],
        sample_rate: 16000,
    })
    .await;
    bus.publish(Event::TurnAsrEndRequested { meta: meta.clone() })
        .await;

    let got = finals.lock().unwrap().clone();
    assert_eq!(got, vec!["turn-ok".to_string()]);
}

#[tokio::test]
async fn vad_manager_disabled_by_default() {
    let bus = Arc::new(EventBus::new());
    let seen = Arc::new(Mutex::new(false));
    let seen2 = Arc::clone(&seen);
    bus.subscribe(
        "vad.speech_start",
        0,
        Arc::new(move |_ev| {
            let seen2 = Arc::clone(&seen2);
            Box::pin(async move {
                *seen2.lock().unwrap() = true;
            })
        }),
    );

    // No server-side VAD configured → should not emit.
    Arc::new(VadManager::new("s1", None)).register(Arc::clone(&bus));
    bus.publish(Event::AudioFrameReceived {
        meta: EventMeta::new("s1"),
        audio_data: vec![0; 320],
        sample_rate: 16000,
    })
    .await;
    assert!(!*seen.lock().unwrap());
}
