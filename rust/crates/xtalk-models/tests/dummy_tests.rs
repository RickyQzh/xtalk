use tokio_util::sync::CancellationToken;
use xtalk_models::{Agent, AgentContext, Asr, DummyAgent, DummyAsr, DummyTts, DummyVad, Tts, Vad};

#[tokio::test]
async fn dummy_asr_returns_default_on_final() {
    let asr = DummyAsr::new("hello");
    let text = asr.recognize_stream(&[], true).await.unwrap();
    assert_eq!(text, "hello");
}

#[tokio::test]
async fn dummy_tts_returns_pcm_at_48k() {
    let tts = DummyTts::new(48_000);
    assert_eq!(tts.sample_rate(), 48_000);
    let chunks = tts.synthesize_stream("hi").await.unwrap();
    assert!(!chunks.is_empty());
    // 100ms silence @ 48k s16le mono = 9600 bytes
    assert_eq!(chunks[0].len(), 48_000 / 10 * 2);
}

#[tokio::test]
async fn dummy_agent_only_answers_asr_final() {
    let agent = DummyAgent::new("ok");
    let cancel = CancellationToken::new();
    let out = agent
        .accept(
            AgentContext {
                context_type: "asr_final".into(),
                text: "x".into(),
            },
            cancel,
        )
        .await
        .unwrap();
    assert_eq!(out, vec!["ok".to_string()]);
}

#[tokio::test]
async fn dummy_vad_speech_threshold() {
    let vad = DummyVad::new();
    assert!(!vad.is_speech(&[]).await.unwrap());
    assert!(!vad.is_speech(&[0u8; 319]).await.unwrap());
    assert!(vad.is_speech(&[0u8; 320]).await.unwrap());
}
