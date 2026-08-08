use xtalk_events::{Event, EventMeta};

#[test]
fn asr_final_type_matches_python() {
    let ev = Event::AsrResultFinal {
        meta: EventMeta::new("s1"),
        text: "hi".into(),
        display_text: "hi".into(),
        speech_pause: false,
    };
    assert_eq!(ev.type_name(), "asr.result_final");
}

#[test]
fn extension_preserves_custom_type() {
    let ev = Event::Extension {
        meta: EventMeta::new("s1"),
        type_name: "custom.foo".into(),
        payload: serde_json::json!({"x": 1}),
    };
    assert_eq!(ev.type_name(), "custom.foo");
}
