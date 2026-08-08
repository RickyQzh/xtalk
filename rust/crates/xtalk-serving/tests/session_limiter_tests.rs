use std::sync::Arc;

use xtalk_pipeline::{DefaultPipeline, Pipeline};
use xtalk_serving::{LimitError, ServiceManager, SessionLimiter};

#[tokio::test]
async fn rejects_when_full() {
    let limiter = SessionLimiter::new(1);
    let _p = limiter.acquire().await.unwrap();
    assert!(limiter.try_acquire().is_err());
}

#[tokio::test]
async fn dropping_permit_frees_slot() {
    let limiter = SessionLimiter::new(1);
    {
        let _p = limiter.acquire().await.unwrap();
        assert!(limiter.try_acquire().is_err());
    }
    assert!(limiter.try_acquire().is_ok());
}

#[tokio::test]
async fn service_manager_connect_tracks_and_disconnect_releases() {
    let factory: Arc<dyn Fn() -> Box<dyn Pipeline> + Send + Sync> =
        Arc::new(|| Box::new(DefaultPipeline::builder().build()));
    let manager = ServiceManager::new(2, factory);

    let s1 = manager.connect(None, None).await.unwrap();
    assert!(!s1.session_id.is_empty());
    assert_eq!(manager.session_count(), 1);

    let s2 = manager.connect(None, Some("user-a")).await.unwrap();
    assert_ne!(s1.session_id, s2.session_id);
    assert_eq!(manager.session_count(), 2);

    let err = match manager.connect(None, None).await {
        Ok(_) => panic!("expected LimitError::Full when at capacity"),
        Err(e) => e,
    };
    assert!(matches!(err, LimitError::Full));

    manager.disconnect(&s1.session_id).await;
    assert_eq!(manager.session_count(), 1);

    let s3 = manager.connect(None, None).await.unwrap();
    assert_eq!(manager.session_count(), 2);
    assert_ne!(s3.session_id, s2.session_id);

    manager.disconnect(&s2.session_id).await;
    manager.disconnect(&s3.session_id).await;
    assert_eq!(manager.session_count(), 0);
}
