use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

pub(super) fn empty_snapshot() -> JoinMessage {
    JoinMessage {
        msg: Some(join_message::Msg::Consumer(ConsumerMessage {
            msg: Some(consumer_message::Msg::Cluster(pb::ClusterMessage {
                version: 1,
                msg: Some(pb::cluster_message::Msg::Snapshot(
                    pb::cluster_message::Snapshot::default(),
                )),
                ..Default::default()
            })),
        })),
    }
}

fn ready_transport() -> (
    Transport,
    mpsc::Sender<std::result::Result<JoinMessage, tonic::Status>>,
    mpsc::Receiver<JoinMessage>,
) {
    let (incoming, receiver) = mpsc::channel(8);
    incoming.try_send(Ok(established())).unwrap();
    incoming.try_send(Ok(empty_snapshot())).unwrap();
    let (outbound, outgoing) = mpsc::channel(8);
    (
        Transport {
            outbound,
            inbound: Box::pin(ReceiverStream::new(receiver)),
        },
        incoming,
        outgoing,
    )
}

async fn snapshot_received(handle: &mut JoinHandle) {
    timeout(Duration::from_secs(10), async {
        loop {
            if handle.cluster.borrow_and_update().id().version == 1 {
                return;
            }
            handle.cluster.changed().await.unwrap();
        }
    })
    .await
    .expect("valid snapshot within the test deadline");
}

#[tokio::test(start_paused = true)]
async fn defaults_return_before_connecting_and_retry_initial_rpc_rejections() {
    let calls = Arc::new(AtomicUsize::new(0));
    let attempts = calls.clone();
    let (transport, _incoming, _outgoing) = ready_transport();
    let transport = Mutex::new(Some(transport));
    let connector: Connector = Arc::new(move |_| {
        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
        if attempt < 3 {
            Box::pin(async {
                Err(ClientError::Transport(tonic::Status::permission_denied(
                    "not yet permitted",
                )))
            })
        } else {
            let transport = transport.lock().unwrap().take().unwrap();
            Box::pin(async move { Ok(transport) })
        }
    });
    let mut handle = start_join(connector, ConsumerOptions::default())
        .await
        .unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "join returns a lifetime handle, not readiness"
    );
    snapshot_received(&mut handle).await;
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    handle.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn default_retries_have_no_total_outage_limit() {
    let calls = Arc::new(AtomicUsize::new(0));
    let attempts = calls.clone();
    let connector: Connector = Arc::new(move |_| {
        attempts.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(ClientError::Transport(tonic::Status::unavailable(
                "offline",
            )))
        })
    });
    let handle = start_join(connector, ConsumerOptions::default())
        .await
        .unwrap();
    tokio::task::yield_now().await;
    for _ in 0..3 {
        tokio::time::advance(Duration::from_secs(3600)).await;
        tokio::task::yield_now().await;
        assert!(!handle.closed().is_closed());
    }
    assert!(calls.load(Ordering::SeqCst) >= 4);
    handle.shutdown().await.unwrap();
}

struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn connection_and_handshake_attempts_are_individually_bounded() {
    for silent_handshake in [false, true] {
        let dropped = Arc::new(AtomicUsize::new(0));
        let drops = dropped.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let attempts = calls.clone();
        let (transport, _incoming, _outgoing) = ready_transport();
        let transport = Mutex::new(Some(transport));
        let connector: Connector = Arc::new(move |_| {
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                let guard = Dropped(drops.clone());
                Box::pin(async move {
                    if silent_handshake {
                        let (outbound, _) = mpsc::channel(1);
                        Ok(Transport {
                            outbound,
                            inbound: Box::pin(futures::stream::once(async move {
                                let _guard = guard;
                                std::future::pending().await
                            })),
                        })
                    } else {
                        let _guard = guard;
                        std::future::pending().await
                    }
                })
            } else {
                let transport = transport.lock().unwrap().take().unwrap();
                Box::pin(async move { Ok(transport) })
            }
        });
        let start = tokio::time::Instant::now();
        let mut handle = start_join(
            connector,
            ConsumerOptions::default().with_join_timeout(Duration::from_secs(2)),
        )
        .await
        .unwrap();
        snapshot_received(&mut handle).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(start.elapsed(), Duration::from_secs(3));
        handle.shutdown().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn server_closure_bad_handshakes_and_rpc_errors_all_retry() {
    let calls = Arc::new(AtomicUsize::new(0));
    let attempts = calls.clone();
    let (transport, _incoming, _outgoing) = ready_transport();
    let transport = Mutex::new(Some(transport));
    let connector: Connector = Arc::new(move |_| {
        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
        if attempt == 4 {
            let transport = transport.lock().unwrap().take().unwrap();
            return Box::pin(async move { Ok(transport) });
        }
        Box::pin(async move {
            let (outbound, _) = mpsc::channel(1);
            let messages = match attempt {
                0 => vec![Ok(session::closed_frame("coordinator rejected session"))],
                1 => vec![Ok(JoinMessage::default())],
                2 => vec![Err(tonic::Status::invalid_argument(
                    "registration rejected",
                ))],
                _ => vec![],
            };
            Ok(Transport {
                outbound,
                inbound: Box::pin(futures::stream::iter(messages)),
            })
        })
    });
    let mut handle = start_join(connector, ConsumerOptions::default())
        .await
        .unwrap();
    snapshot_received(&mut handle).await;
    assert_eq!(calls.load(Ordering::SeqCst), 5);
    handle.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn established_session_failures_all_reconnect() {
    for failure in [
        "closed",
        "rpc",
        "invalid-grant",
        "invalid-cluster",
        "eof",
        "send-dropped",
        "send-stalled",
    ] {
        let mut h = Harness::new();
        h.inbound
            .try_send(Ok(deadline::session_frame(
                SystemTime::now() + Duration::from_secs(600),
                false,
            )))
            .unwrap();
        let (transport, _incoming, _outgoing) = ready_transport();
        let transport = Mutex::new(Some(transport));
        h.driver.reconnect = Some((
            ReconnectOptions::default(),
            Arc::new(move |_| {
                let transport = transport
                    .lock()
                    .unwrap()
                    .take()
                    .expect("one successful reconnect");
                Box::pin(async move { Ok(transport) })
            }),
        ));
        match failure {
            "closed" => h
                .inbound
                .try_send(Ok(session::closed_frame("restart")))
                .unwrap(),
            "rpc" => h
                .inbound
                .try_send(Err(tonic::Status::permission_denied("permission changed")))
                .unwrap(),
            "invalid-grant" => h
                .inbound
                .try_send(Ok(wrap_client(client_message::Msg::Assign(
                    client_message::Assign {
                        grants: vec![pb::Grant::default()],
                    },
                ))))
                .unwrap(),
            "invalid-cluster" => h
                .inbound
                .try_send(Ok(JoinMessage {
                    msg: Some(join_message::Msg::Consumer(ConsumerMessage {
                        msg: Some(consumer_message::Msg::Cluster(pb::ClusterMessage::default())),
                    })),
                }))
                .unwrap(),
            "eof" => drop(h.inbound),
            "send-dropped" => h.outbound.close(),
            "send-stalled" => {
                while h
                    .driver
                    .transport
                    .as_ref()
                    .unwrap()
                    .outbound
                    .try_send(JoinMessage::default())
                    .is_ok()
                {}
            }
            _ => unreachable!(),
        }
        h.handle.task = Some(tokio::spawn(h.driver.run()));
        timeout(Duration::from_secs(45), async {
            loop {
                if h.handle.cluster.borrow_and_update().id().version == 1 {
                    break;
                }
                h.handle.cluster.changed().await.unwrap();
            }
        })
        .await
        .unwrap_or_else(|_| panic!("did not recover from {failure}"));
        h.handle.shutdown().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn disconnect_limit_cancels_initial_attempt_and_reports_terminal_reason() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let drops = dropped.clone();
    let connector: Connector = Arc::new(move |_| {
        let guard = Dropped(drops.clone());
        Box::pin(async move {
            let _guard = guard;
            std::future::pending().await
        })
    });
    let start = tokio::time::Instant::now();
    let handle = start_join(
        connector,
        ConsumerOptions::default().with_disconnect_timeout(Duration::from_secs(2)),
    )
    .await
    .unwrap();
    handle.closed().closed().await;
    assert_eq!(start.elapsed(), Duration::from_secs(2));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(matches!(
        handle.shutdown().await,
        Err(ClientError::DisconnectTimeout)
    ));
}

#[tokio::test(start_paused = true)]
async fn handshake_without_snapshot_does_not_reset_disconnect_budget() {
    let connector: Connector = Arc::new(|_| {
        Box::pin(async {
            let (outbound, _) = mpsc::channel(1);
            Ok(Transport {
                outbound,
                inbound: Box::pin(futures::stream::iter([Ok(established())])),
            })
        })
    });
    let start = tokio::time::Instant::now();
    let handle = start_join(
        connector,
        ConsumerOptions::default().with_disconnect_timeout(Duration::from_secs(3)),
    )
    .await
    .unwrap();
    handle.closed().closed().await;
    assert_eq!(start.elapsed(), Duration::from_secs(3));
    assert!(matches!(
        handle.shutdown().await,
        Err(ClientError::DisconnectTimeout)
    ));
}

#[tokio::test(start_paused = true)]
async fn shutdown_and_drop_cancel_pending_initial_connection() {
    for force in [false, true] {
        let dropped = Arc::new(AtomicUsize::new(0));
        let drops = dropped.clone();
        let connector: Connector = Arc::new(move |_| {
            let guard = Dropped(drops.clone());
            Box::pin(async move {
                let _guard = guard;
                std::future::pending().await
            })
        });
        let handle = start_join(connector, ConsumerOptions::default())
            .await
            .unwrap();
        let closed = handle.closed();
        tokio::task::yield_now().await;
        if force {
            drop(handle);
        } else {
            handle.shutdown().await.unwrap();
        }
        closed.closed().await;
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn invalid_controls_are_rejected_before_spawning() {
    for duration in [Duration::ZERO, Duration::MAX] {
        for opts in [
            ConsumerOptions::default().with_drain_timeout(duration),
            ConsumerOptions::default().with_disconnect_timeout(duration),
            ConsumerOptions::default().with_reconnect(ReconnectOptions {
                backoff: duration,
                ..Default::default()
            }),
        ] {
            let connector: Connector = Arc::new(|_| panic!("invalid options must not connect"));
            assert!(matches!(
                start_join(connector, opts).await,
                Err(ClientError::InvalidMessage(_))
            ));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn live_grants_survive_disconnect_and_local_actions_continue_without_coordinator() {
    let mut h = Harness::new();
    for id in ["active", "released", "expired", "loaded"] {
        h.driver
            .on_assign(grant(
                id,
                if id == "loaded" {
                    pb::GrantState::Allocated
                } else {
                    pb::GrantState::Active
                },
            ))
            .await
            .unwrap();
    }
    let ownership = |id: &str| {
        h.handle.assignments.borrow()[&GrantId(id.into())]
            .ownership
            .clone()
    };
    let active = ownership("active");
    let released = ownership("released");
    let expired = ownership("expired");
    let loaded = ownership("loaded");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let replay = seen.clone();
    h.driver.reconnect = Some((
        ReconnectOptions::default(),
        Arc::new(move |grants| {
            *replay.lock().unwrap() = grants;
            Box::pin(async {
                Err(ClientError::Transport(tonic::Status::unavailable(
                    "offline",
                )))
            })
        }),
    ));
    h.inbound
        .try_send(Ok(session::closed_frame("restart")))
        .unwrap();
    h.handle.task = Some(tokio::spawn(h.driver.run()));
    tokio::task::yield_now().await;
    assert!(!active.revoked().is_closed());
    active.wait_for_active().await.unwrap();
    released.release();
    expired.set_expiration(SystemTime::UNIX_EPOCH);
    loaded.loader().load();
    timeout(Duration::from_secs(1), async {
        while h.handle.assignments.borrow_and_update().len() != 2 {
            h.handle.assignments.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    let replay = seen.lock().unwrap().clone();
    assert_eq!(replay.len(), 2);
    assert!(replay.iter().all(|g| g.id == "active" || g.id == "loaded"));
    assert_eq!(
        replay.iter().find(|g| g.id == "loaded").unwrap().state,
        pb::GrantState::AllocatedLoaded as i32
    );
    assert!(!active.expired().is_closed());
    assert!(!active.revoked().is_closed());
    assert!(!h.handle.closed().is_closed());
    active.release();
    loaded.release();
    h.handle.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn offline_shutdown_revokes_then_waits_for_local_release() {
    let mut h = Harness::new();
    h.driver
        .on_assign(grant("g", pb::GrantState::Active))
        .await
        .unwrap();
    let ownership = h.handle.assignments.borrow()[&GrantId("g".into())]
        .ownership
        .clone();
    h.driver.reconnect = Some((
        ReconnectOptions::default(),
        Arc::new(|_| Box::pin(std::future::pending())),
    ));
    h.inbound
        .try_send(Ok(session::closed_frame("restart")))
        .unwrap();
    h.handle.task = Some(tokio::spawn(h.driver.run()));
    tokio::task::yield_now().await;
    let closed = h.handle.closed();
    let shutdown = tokio::spawn(h.handle.shutdown());
    ownership.revoked().closed().await;
    assert!(!ownership.expired().is_closed());
    assert!(!closed.is_closed());
    ownership.release();
    shutdown.await.unwrap().unwrap();
    assert!(closed.is_closed());
}

#[tokio::test(start_paused = true)]
async fn graceful_shutdown_is_bounded_even_with_a_stalled_outbound_stream() {
    let mut h = Harness::new();
    h.driver.drain_timeout = Duration::from_secs(2);
    h.driver
        .on_assign(grant("g", pb::GrantState::Active))
        .await
        .unwrap();
    let ownership = h.handle.assignments.borrow()[&GrantId("g".into())]
        .ownership
        .clone();
    while h
        .driver
        .transport
        .as_ref()
        .unwrap()
        .outbound
        .try_send(JoinMessage::default())
        .is_ok()
    {}
    h.handle.task = Some(tokio::spawn(h.driver.run()));
    let start = tokio::time::Instant::now();
    assert!(matches!(
        h.handle.shutdown().await,
        Err(ClientError::DrainTimeout)
    ));
    assert_eq!(start.elapsed(), Duration::from_secs(2));
    assert!(ownership.expired().is_closed());
}

#[tokio::test(start_paused = true)]
async fn disconnect_timeout_drains_locally_before_reporting_termination() {
    let mut h = Harness::new();
    h.driver
        .on_assign(grant("g", pb::GrantState::Active))
        .await
        .unwrap();
    let ownership = h.handle.assignments.borrow()[&GrantId("g".into())]
        .ownership
        .clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let attempts = calls.clone();
    h.driver.reconnect = Some((
        ReconnectOptions {
            timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        },
        Arc::new(move |_| {
            attempts.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::pending())
        }),
    ));
    h.inbound
        .try_send(Ok(session::closed_frame("restart")))
        .unwrap();
    h.handle.task = Some(tokio::spawn(h.driver.run()));
    tokio::task::yield_now().await;
    assert!(!ownership.revoked().is_closed());
    assert!(!ownership.expired().is_closed());
    ownership.revoked().closed().await;
    assert!(
        !h.handle.closed().is_closed(),
        "disconnect timeout begins drain, not immediate abort"
    );
    assert!(!ownership.expired().is_closed());
    ownership.release();
    h.handle.closed().closed().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        h.handle.shutdown().await,
        Err(ClientError::DisconnectTimeout)
    ));
}

#[tokio::test(start_paused = true)]
async fn disconnect_timeout_bounds_uncooperative_local_drain() {
    let mut h = Harness::new();
    h.driver
        .on_assign(grant("g", pb::GrantState::Active))
        .await
        .unwrap();
    let ownership = h.handle.assignments.borrow()[&GrantId("g".into())]
        .ownership
        .clone();
    h.driver.drain_timeout = Duration::from_secs(3);
    h.driver.reconnect = Some((
        ReconnectOptions {
            timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        },
        Arc::new(|_| Box::pin(std::future::pending())),
    ));
    h.inbound
        .try_send(Ok(session::closed_frame("restart")))
        .unwrap();
    h.handle.task = Some(tokio::spawn(h.driver.run()));
    let start = tokio::time::Instant::now();
    h.handle.closed().closed().await;
    assert_eq!(start.elapsed(), Duration::from_secs(5));
    assert!(ownership.expired().is_closed());
    assert!(h.handle.assignments.borrow().is_empty());
    assert!(matches!(
        h.handle.shutdown().await,
        Err(ClientError::DisconnectTimeout)
    ));
}

#[tokio::test(start_paused = true)]
async fn valid_snapshot_resets_disconnect_budget_for_next_outage() {
    let (transport, incoming, _outgoing) = ready_transport();
    let transport = Mutex::new(Some(transport));
    let connector: Connector = Arc::new(move |_| {
        let transport = transport.lock().unwrap().take();
        Box::pin(async move {
            match transport {
                Some(transport) => Ok(transport),
                None => Err(ClientError::Transport(tonic::Status::unavailable(
                    "offline",
                ))),
            }
        })
    });
    let mut handle = start_join(
        connector,
        ConsumerOptions::default().with_disconnect_timeout(Duration::from_secs(2)),
    )
    .await
    .unwrap();
    snapshot_received(&mut handle).await;
    tokio::time::advance(Duration::from_secs(3)).await;
    tokio::task::yield_now().await;
    assert!(
        !handle.closed().is_closed(),
        "connected lifetime is not bounded by disconnect timeout"
    );
    let start = tokio::time::Instant::now();
    drop(incoming);
    handle.closed().closed().await;
    assert_eq!(start.elapsed(), Duration::from_secs(2));
    assert!(matches!(
        handle.shutdown().await,
        Err(ClientError::DisconnectTimeout)
    ));
}

#[tokio::test]
async fn processor_releases_expired_range_while_consumer_keeps_reconnecting() {
    use crate::{
        ConnectionPool, DispatchFilter, FilterContext, InstanceId, Processor, Range, RangeFactory,
        recording_range::RecordingRange,
    };
    struct Factory;
    impl RangeFactory for Factory {
        type Range = RecordingRange;
        fn build(&self, _: GrantId, _: Shard, _: Arc<Ownership>) -> Option<RecordingRange> {
            Some(RecordingRange::new(
                "offline",
                Duration::ZERO,
                Duration::ZERO,
                Arc::new(Mutex::new(Vec::new())),
            ))
        }
    }
    let mut h = Harness::new();
    let id = GrantId("g".into());
    h.driver
        .on_assign(grant("g", pb::GrantState::Active))
        .await
        .unwrap();
    let assignment = h.handle.assignments.borrow()[&id].clone();
    let ownership = assignment.ownership.clone();
    let processor = Processor::new(Arc::new(Factory));
    processor.init(FilterContext {
        service: QualifiedServiceName::new("test", "lifecycle"),
        pool: ConnectionPool::new(InstanceId::new()),
        cluster: h.handle.cluster.clone(),
    });
    let worker = processor.clone();
    let handler = tokio::spawn(async move {
        worker
            .try_handle(id, assignment.shard, assignment.ownership)
            .await
    });
    let range = timeout(Duration::from_secs(1), async {
        loop {
            if let Some((_, GrantState::Active, range)) =
                processor.grants().get(&GrantId("g".into()))
            {
                return range;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    h.driver.reconnect = Some((
        ReconnectOptions::default(),
        Arc::new(|_| Box::pin(std::future::pending())),
    ));
    h.inbound
        .try_send(Ok(session::closed_frame("restart")))
        .unwrap();
    h.handle.task = Some(tokio::spawn(h.driver.run()));
    tokio::task::yield_now().await;
    assert!(!range.closed().is_closed());
    assert!(!ownership.revoked().is_closed());
    assert!(!ownership.expired().is_closed());
    ownership.set_expiration(SystemTime::UNIX_EPOCH);
    assert!(
        timeout(Duration::from_secs(1), handler)
            .await
            .unwrap()
            .unwrap()
    );
    timeout(Duration::from_secs(1), async {
        while !h.handle.assignments.borrow_and_update().is_empty() {
            h.handle.assignments.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(range.closed().is_closed());
    assert!(ownership.released.is_closed());
    assert!(processor.grants().is_empty());
    assert!(
        !h.handle.closed().is_closed(),
        "grant expiry must not stop the consumer"
    );
    h.handle.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn attempt_deadline_wins_over_simultaneously_ready_connection() {
    let (first, _first_incoming, _first_outgoing) = ready_transport();
    let (second, _incoming, _outgoing) = ready_transport();
    let transports = Mutex::new(std::collections::VecDeque::from([first, second]));
    let calls = Arc::new(AtomicUsize::new(0));
    let attempts = calls.clone();
    let connector: Connector = Arc::new(move |_| {
        let transport = transports.lock().unwrap().pop_front().unwrap();
        let first = attempts.fetch_add(1, Ordering::SeqCst) == 0;
        Box::pin(async move {
            if first {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok(transport)
        })
    });
    let mut handle = start_join(
        connector,
        ConsumerOptions::default().with_join_timeout(Duration::from_secs(1)),
    )
    .await
    .unwrap();
    snapshot_received(&mut handle).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "deadline must discard the first attempt"
    );
    handle.shutdown().await.unwrap();
}

#[test]
fn disconnect_limit_is_a_terminal_deadline_not_an_ownership_retry() {
    assert!(!ClientError::DisconnectTimeout.is_ownership());
    assert_eq!(
        ClientError::DisconnectTimeout.into_grpc().code(),
        tonic::Code::DeadlineExceeded
    );
}
