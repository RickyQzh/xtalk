use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use xtalk_events::{Event, EventMeta};
use xtalk_pipeline::DefaultPipeline;
use xtalk_serving::{Manager, Service};

struct FlagManager {
    flag: Arc<AtomicBool>,
}

impl Manager for FlagManager {
    fn name(&self) -> &'static str {
        "flag"
    }

    fn register(self: Arc<Self>, bus: Arc<xtalk_bus::EventBus>) {
        let flag = Arc::clone(&self.flag);
        bus.subscribe(
            "vad.speech_start",
            0,
            Arc::new(move |_ev| {
                let flag = Arc::clone(&flag);
                Box::pin(async move {
                    flag.store(true, Ordering::SeqCst);
                })
            }),
        );
    }
}

#[tokio::test]
async fn service_registers_managers_and_shutdown_clears_bus() {
    let flag = Arc::new(AtomicBool::new(false));
    let manager: Arc<dyn Manager> = Arc::new(FlagManager {
        flag: Arc::clone(&flag),
    });
    let pipeline = Box::new(DefaultPipeline::builder().build());
    let service = Service::new("session-1", pipeline, vec![manager]);

    assert_eq!(service.session_id, "session-1");

    service
        .bus()
        .publish(Event::VadSpeechStart {
            meta: EventMeta::new("session-1"),
            origin: "client".into(),
        })
        .await;
    assert!(
        flag.load(Ordering::SeqCst),
        "Service::new should register managers so handlers run on publish"
    );

    flag.store(false, Ordering::SeqCst);
    service.shutdown().await;

    service
        .bus()
        .publish(Event::VadSpeechStart {
            meta: EventMeta::new("session-1"),
            origin: "client".into(),
        })
        .await;
    assert!(
        !flag.load(Ordering::SeqCst),
        "after shutdown, publish should no-op (no handlers)"
    );
}
