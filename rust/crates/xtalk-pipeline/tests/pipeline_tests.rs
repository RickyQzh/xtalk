use xtalk_models::{DummyAgent, DummyAsr, DummyTts, DummyVad};
use xtalk_pipeline::{DefaultPipeline, Pipeline};

#[tokio::test]
async fn clone_session_isolates_asr_state() {
    let p = DefaultPipeline::builder()
        .asr(Box::new(DummyAsr::new("a")))
        .tts(Box::new(DummyTts::new(48_000)))
        .vad(Box::new(DummyVad))
        .agent(Box::new(DummyAgent::new("r")))
        .build();
    let c1 = p.clone_session();
    let c2 = p.clone_session();

    // Mutate c1 ASR interior state; c2 must remain independent.
    let t1 = c1.asr().unwrap().recognize_stream(&[], true).await.unwrap();
    assert_eq!(t1, "a");

    let t2 = c2
        .asr()
        .unwrap()
        .recognize_stream(&[], false)
        .await
        .unwrap();
    assert_eq!(t2, "");

    c1.asr().unwrap().reset();
    let t1_after = c1
        .asr()
        .unwrap()
        .recognize_stream(&[], false)
        .await
        .unwrap();
    assert_eq!(t1_after, "");

    let t2_final = c2.asr().unwrap().recognize_stream(&[], true).await.unwrap();
    assert_eq!(t2_final, "a");
}
