use super::*;
use std::{
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

fn unavailable() -> ClientError {
    ClientError::Transport(tonic::Status::unavailable(
        "temporary channel construction failure",
    ))
}

#[tokio::test(start_paused = true)]
async fn failed_initial_dial_recovers_on_resolve_without_cluster_update() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let count = attempts.clone();
    let pool = ConnectionPool::with_dial(ME, move |endpoint| {
        if count.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(unavailable())
        } else {
            default_dial(endpoint)
        }
    });
    pool.apply_cluster(&mk_cluster(vec![mk_instance(PEER_A, "peer-a:50052")]));
    assert!(pool.is_empty());
    for _ in 0..10 {
        assert!(pool.resolve(&PEER_A).is_err());
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(pool.resolve(&PEER_A).is_ok());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(pool.len(), 1);
    assert!(pool.resolve(&PEER_A).is_ok());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn failed_endpoint_replacement_never_returns_old_channel() {
    let fail = Arc::new(AtomicBool::new(true));
    let fail_dial = fail.clone();
    let endpoints = Arc::new(Mutex::new(Vec::new()));
    let dialed = endpoints.clone();
    let pool = ConnectionPool::with_dial(ME, move |endpoint| {
        dialed.lock().unwrap().push(endpoint.as_str().to_owned());
        if endpoint.as_str() == "new:50052" && fail_dial.load(Ordering::SeqCst) {
            Err(unavailable())
        } else {
            default_dial(endpoint)
        }
    });
    pool.apply_cluster(&mk_cluster(vec![mk_instance(PEER_A, "old:50052")]));
    assert!(pool.resolve(&PEER_A).is_ok());
    pool.apply_cluster(&mk_cluster(vec![mk_instance(PEER_A, "new:50052")]));
    assert!(
        matches!(pool.resolve(&PEER_A), Err(ClientError::Transport(status)) if status.code() == tonic::Code::Unavailable)
    );
    assert!(pool.is_empty());
    fail.store(false, Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(pool.resolve(&PEER_A).is_ok());
    assert_eq!(
        *endpoints.lock().unwrap(),
        ["old:50052", "new:50052", "new:50052"]
    );
}

#[tokio::test(start_paused = true)]
async fn repeated_updates_and_resolves_share_dial_backoff() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let count = attempts.clone();
    let pool = ConnectionPool::with_dial(ME, move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        Err(unavailable())
    });
    let cluster = mk_cluster(vec![mk_instance(PEER_A, "peer-a:50052")]);
    for _ in 0..10 {
        pool.apply_cluster(&cluster);
        assert!(pool.resolve(&PEER_A).is_err());
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(pool.resolve(&PEER_A).is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn departed_peer_is_not_redialed() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let count = attempts.clone();
    let pool = ConnectionPool::with_dial(ME, move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        Err(unavailable())
    });
    pool.apply_cluster(&mk_cluster(vec![mk_instance(PEER_A, "peer-a:50052")]));
    pool.apply_cluster(&mk_cluster(Vec::new()));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(pool.resolve(&PEER_A).is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn new_endpoint_is_tried_without_waiting_for_old_endpoint_backoff() {
    let pool = ConnectionPool::with_dial(ME, |endpoint| {
        if endpoint.as_str() == "bad:50052" {
            Err(unavailable())
        } else {
            default_dial(endpoint)
        }
    });
    pool.apply_cluster(&mk_cluster(vec![mk_instance(PEER_A, "bad:50052")]));
    assert!(pool.resolve(&PEER_A).is_err());
    pool.apply_cluster(&mk_cluster(vec![mk_instance(PEER_A, "good:50052")]));
    assert!(pool.resolve(&PEER_A).is_ok());
}
