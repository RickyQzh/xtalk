#[test]
fn parse_vad_start_fixture() {
    let raw = include_str!("../src/fixtures/vad_speech_start.json");
    let msg = xtalk_protocol::parse_inbound_text(raw).unwrap();
    assert!(matches!(
        msg,
        xtalk_protocol::InboundMessage::VadSpeechStart
    ));
}

#[test]
fn parse_ping_fixture() {
    let raw = include_str!("../src/fixtures/ping.json");
    let msg = xtalk_protocol::parse_inbound_text(raw).unwrap();
    assert!(matches!(
        msg,
        xtalk_protocol::InboundMessage::Ping { timestamp } if (timestamp - 1.5).abs() < f64::EPSILON
    ));
}

#[test]
fn parse_session_config_fixture() {
    let raw = include_str!("../src/fixtures/session_config.json");
    let msg = xtalk_protocol::parse_inbound_text(raw).unwrap();
    match msg {
        xtalk_protocol::InboundMessage::SessionConfig(value) => {
            assert_eq!(value["action"], "session_config");
            assert_eq!(value["recording_path"], "logs/session_audio/demo.wav");
        }
        other => panic!("expected SessionConfig, got {other:?}"),
    }
}

#[test]
fn outbound_shape() {
    let s = xtalk_protocol::outbound_action("update_asr", serde_json::json!({"text":"hi"}));
    let v: serde_json::Value = serde_json::from_str(&s).unwrap();
    assert_eq!(v["action"], "update_asr");
    assert_eq!(v["data"]["text"], "hi");
}

#[test]
fn parse_ping() {
    let msg = xtalk_protocol::parse_inbound_text(r#"{"action":"ping","timestamp":1.5}"#).unwrap();
    assert!(matches!(
        msg,
        xtalk_protocol::InboundMessage::Ping { timestamp } if timestamp == 1.5
    ));
}

#[test]
fn parse_known_control_actions() {
    use xtalk_protocol::InboundMessage;

    assert!(matches!(
        xtalk_protocol::parse_inbound_text(r#"{"action":"vad_speech_end"}"#).unwrap(),
        InboundMessage::VadSpeechEnd
    ));
    assert!(matches!(
        xtalk_protocol::parse_inbound_text(r#"{"action":"tts_playback_finished"}"#).unwrap(),
        InboundMessage::TtsPlaybackFinished
    ));
    assert!(matches!(
        xtalk_protocol::parse_inbound_text(r#"{"action":"tts_chunk_played"}"#).unwrap(),
        InboundMessage::TtsChunkPlayed { .. }
    ));
    assert!(matches!(
        xtalk_protocol::parse_inbound_text(
            r#"{"action":"session_config","recording_path":"/tmp"}"#
        )
        .unwrap(),
        InboundMessage::SessionConfig(_)
    ));
    assert!(matches!(
        xtalk_protocol::parse_inbound_text(
            r#"{"action":"clock_sync","client_send_ts":1.0,"server_recv_ts":2.0,"client_recv_ts":3.0}"#
        )
        .unwrap(),
        InboundMessage::ClockSync { .. }
    ));
    assert!(matches!(
        xtalk_protocol::parse_inbound_text(r#"{"action":"change_voice","voice_name":"a"}"#)
            .unwrap(),
        InboundMessage::ChangeVoice { .. }
    ));
    assert!(matches!(
        xtalk_protocol::parse_inbound_text(r#"{"action":"change_emotion","emotion_name":"happy"}"#)
            .unwrap(),
        InboundMessage::ChangeEmotion { .. }
    ));
    assert!(matches!(
        xtalk_protocol::parse_inbound_text(r#"{"action":"change_tts_speed","speed":1.2}"#).unwrap(),
        InboundMessage::ChangeTtsSpeed { .. }
    ));
}

#[test]
fn unknown_action_is_unknown_not_error() {
    let msg = xtalk_protocol::parse_inbound_text(r#"{"action":"not_a_real_action"}"#).unwrap();
    assert!(matches!(msg, xtalk_protocol::InboundMessage::Unknown));
}

#[test]
fn invalid_json_is_error() {
    let err = xtalk_protocol::parse_inbound_text("not-json").unwrap_err();
    let _ = format!("{err}");
}
