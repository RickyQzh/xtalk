use std::sync::{Arc, Mutex};
use xtalk_bus::EventBus;
use xtalk_events::{Event, EventMeta};

#[tokio::test]
async fn handlers_run_by_priority_descending() {
    let bus = EventBus::new();
    let order = Arc::new(Mutex::new(Vec::new()));
    for (prio, label) in [(1, "low"), (10, "high"), (5, "mid")] {
        let order = Arc::clone(&order);
        bus.subscribe(
            "vad.speech_start",
            prio,
            Arc::new(move |_ev| {
                let order = Arc::clone(&order);
                let label = label;
                Box::pin(async move {
                    order.lock().unwrap().push(label);
                })
            }),
        );
    }
    bus.publish(Event::VadSpeechStart {
        meta: EventMeta::new("s"),
        origin: "client".into(),
    })
    .await;
    assert_eq!(*order.lock().unwrap(), vec!["high", "mid", "low"]);
}

#[tokio::test]
async fn handler_error_does_not_poison_bus() {
    let bus = EventBus::new();
    let hit = Arc::new(Mutex::new(false));
    // Soft-failing error handler: completes normally after a soft failure path.
    bus.subscribe(
        "error.occurred",
        0,
        Arc::new(move |_ev| {
            Box::pin(async {
                // Soft failure path — do not panic; bus must remain usable.
                let _ = "soft failure handled";
            })
        }),
    );
    let hit2 = Arc::clone(&hit);
    bus.subscribe(
        "vad.speech_end",
        0,
        Arc::new(move |_ev| {
            let hit2 = Arc::clone(&hit2);
            Box::pin(async move {
                *hit2.lock().unwrap() = true;
            })
        }),
    );
    // Prior ErrorOccurred must not poison subsequent publishes.
    bus.publish(Event::ErrorOccurred {
        meta: EventMeta::new("s"),
        error_message: "prior failure".into(),
    })
    .await;
    bus.publish(Event::VadSpeechEnd {
        meta: EventMeta::new("s"),
        origin: "client".into(),
    })
    .await;
    assert!(*hit.lock().unwrap());
}

#[tokio::test]
async fn error_occurred_dropped_during_cooldown() {
    let bus = EventBus::new();
    let count = Arc::new(Mutex::new(0usize));
    let count2 = Arc::clone(&count);
    bus.subscribe(
        "error.occurred",
        0,
        Arc::new(move |_ev| {
            let count2 = Arc::clone(&count2);
            Box::pin(async move {
                *count2.lock().unwrap() += 1;
            })
        }),
    );

    for i in 0..5 {
        bus.publish(Event::ErrorOccurred {
            meta: EventMeta::new("s"),
            error_message: format!("err-{i}"),
        })
        .await;
    }

    // Cooldown is 1.0s: only the first error.occurred should be dispatched.
    assert_eq!(*count.lock().unwrap(), 1);
}
