use super::*;
use crate::{
    ids::{DomainType, QualifiedDomainName},
    wrap::system_time_to_timestamp,
};
use tokio::time::timeout;

struct Harness {
    driver: Driver,
    handle: JoinHandle,
    inbound: mpsc::Sender<std::result::Result<JoinMessage, tonic::Status>>,
    outbound: mpsc::Receiver<JoinMessage>,
}

impl Harness {
    fn new() -> Self {
        let (inbound, receiver) = mpsc::channel(1024);
        let (outbound, outgoing) = mpsc::channel(1024);
        let (driver, handle) = Driver::new(Transport {
            outbound,
            inbound: Box::pin(ReceiverStream::new(receiver)),
        });
        Self {
            driver,
            handle,
            inbound,
            outbound: outgoing,
        }
    }
}

fn grant(id: &str, state: pb::GrantState) -> pb::Grant {
    pb::Grant {
        id: id.into(),
        state: state as i32,
        shard: Some(
            Shard {
                domain: QualifiedDomainName {
                    service: QualifiedServiceName::new("test", "lifecycle"),
                    name: "ranges".into(),
                },
                kind: DomainType::Global,
                region: Default::default(),
                from: String::new(),
                to: String::new(),
            }
            .into(),
        ),
        lease: Some(system_time_to_timestamp(
            SystemTime::now() + Duration::from_secs(60),
        )),
        ..Default::default()
    }
}

fn client_variant(message: JoinMessage) -> client_message::Msg {
    match message.msg.unwrap() {
        join_message::Msg::Consumer(ConsumerMessage {
            msg: Some(consumer_message::Msg::Client(ClientMessage { msg: Some(message) })),
        }) => message,
        _ => panic!("expected client frame"),
    }
}

async fn next(receiver: &mut mpsc::Receiver<JoinMessage>) -> JoinMessage {
    timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn initial_snapshot_survives_startup_and_broadcast_overflow() {
    let mut h = Harness::new();
    for n in 0..400 {
        h.driver
            .on_assign(grant(&n.to_string(), pb::GrantState::Active))
            .await
            .unwrap();
    }
    assert!(matches!(
        h.handle.grants.try_recv(),
        Err(broadcast::error::TryRecvError::Lagged(_))
    ));
    let late_subscriber = h.handle.assignments.clone();
    assert_eq!(late_subscriber.borrow().len(), 400);
}

#[tokio::test]
async fn session_closed_cleans_up_grants_and_closes_handle() {
    let mut h = Harness::new();
    h.driver
        .on_assign(grant("g", pb::GrantState::Active))
        .await
        .unwrap();
    let ownership = h.driver.grants[&GrantId("g".into())].ownership.clone();
    h.inbound
        .send(Ok(session::closed_frame("server stopped")))
        .await
        .unwrap();
    assert!(h.driver.run().await.is_err());
    assert!(h.handle.closed().is_closed());
    assert!(ownership.expired().is_closed());
    assert!(h.handle.assignments.borrow().is_empty());
}

#[tokio::test]
async fn shutdown_before_first_poll_sends_deregister_and_closed() {
    let mut h = Harness::new();
    h.handle.shutdown.close();
    h.driver.run().await.unwrap();
    assert!(matches!(
        client_variant(next(&mut h.outbound).await),
        client_message::Msg::Deregister(_)
    ));
    assert!(matches!(
        next(&mut h.outbound).await.msg,
        Some(join_message::Msg::Session(_))
    ));
    assert!(h.handle.closed().is_closed());
}

#[tokio::test]
async fn shutdown_waits_for_release_and_sends_latest_grant_state() {
    let mut h = Harness::new();
    let mut g = grant("g", pb::GrantState::Active);
    h.driver.on_assign(g.clone()).await.unwrap();
    let ownership = h.driver.grants[&GrantId("g".into())].ownership.clone();
    h.handle.task = Some(tokio::spawn(h.driver.run()));
    let shutdown = tokio::spawn(h.handle.shutdown());
    assert!(matches!(
        client_variant(next(&mut h.outbound).await),
        client_message::Msg::Deregister(_)
    ));
    assert!(!shutdown.is_finished());
    g.state = pb::GrantState::Revoked as i32;
    h.inbound
        .send(Ok(wrap_client(client_message::Msg::Revoke(
            client_message::Revoke { grants: vec![g] },
        ))))
        .await
        .unwrap();
    ownership.wait_for_revoke().await.unwrap().unload();
    let client_message::Msg::Update(update) = client_variant(next(&mut h.outbound).await) else {
        panic!("expected unload");
    };
    assert_eq!(
        update.grant.unwrap().state,
        pb::GrantState::RevokedUnloaded as i32
    );
    ownership.release();
    let client_message::Msg::Released(released) = client_variant(next(&mut h.outbound).await)
    else {
        panic!("expected release");
    };
    assert_eq!(
        released.grants[0].state,
        pb::GrantState::RevokedUnloaded as i32
    );
    assert!(matches!(
        next(&mut h.outbound).await.msg,
        Some(join_message::Msg::Session(_))
    ));
    timeout(Duration::from_secs(2), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn dropping_handle_aborts_driver_even_before_first_poll() {
    let mut h = Harness::new();
    h.driver
        .on_assign(grant("g", pb::GrantState::Active))
        .await
        .unwrap();
    let ownership = h.driver.grants[&GrantId("g".into())].ownership.clone();
    let closed = h.handle.closed();
    h.handle.task = Some(tokio::spawn(h.driver.run()));
    drop(h.handle);
    timeout(Duration::from_secs(2), closed.closed())
        .await
        .unwrap();
    assert!(ownership.expired().is_closed());
}

#[tokio::test]
async fn cancelling_shutdown_aborts_driver_and_its_grants() {
    let mut h = Harness::new();
    h.driver
        .on_assign(grant("g", pb::GrantState::Active))
        .await
        .unwrap();
    let ownership = h.driver.grants[&GrantId("g".into())].ownership.clone();
    let closed = h.handle.closed();
    h.handle.task = Some(tokio::spawn(h.driver.run()));
    assert!(
        timeout(Duration::from_millis(10), h.handle.shutdown())
            .await
            .is_err()
    );
    timeout(Duration::from_secs(2), closed.closed())
        .await
        .unwrap();
    assert!(ownership.expired().is_closed());
}

#[tokio::test]
async fn expiration_removes_grant_and_publishes_removal() {
    let mut h = Harness::new();
    h.driver
        .on_assign(grant("g", pb::GrantState::Active))
        .await
        .unwrap();
    h.driver.grants[&GrantId("g".into())]
        .ownership
        .set_expiration(SystemTime::UNIX_EPOCH);
    let action = timeout(Duration::from_secs(2), h.driver.actions.recv())
        .await
        .unwrap()
        .unwrap();
    h.driver.on_action(action).await.unwrap();
    assert!(h.driver.grants.is_empty());
    assert!(h.handle.assignments.borrow().is_empty());
    assert!(h.outbound.try_recv().is_err());
}

#[tokio::test]
async fn promotion_refreshes_lease_and_active_load_does_not_regress_state() {
    let mut h = Harness::new();
    let mut g = grant("g", pb::GrantState::Allocated);
    h.driver.on_assign(g.clone()).await.unwrap();
    let o = h.driver.grants[&GrantId("g".into())].ownership.clone();
    let deadline = SystemTime::now() + Duration::from_secs(120);
    g.lease = Some(system_time_to_timestamp(deadline));
    g.state = pb::GrantState::Active as i32;
    h.driver.on_promote(g);
    assert_eq!(o.expiration(), deadline);
    h.driver
        .on_action(GrantAction {
            id: GrantId("g".into()),
            ownership: o,
            action: Action::Load,
        })
        .await
        .unwrap();
    assert!(h.outbound.try_recv().is_err());
    assert_eq!(
        h.driver.grants[&GrantId("g".into())].pb_grant.state,
        pb::GrantState::Active as i32
    );
}

#[tokio::test]
async fn duplicate_assignment_preserves_world_identity_and_stale_release_is_ignored() {
    let mut h = Harness::new();
    let g = grant("g", pb::GrantState::Active);
    h.driver.on_assign(g.clone()).await.unwrap();
    let old = h.driver.grants[&GrantId("g".into())].ownership.clone();
    h.driver.on_assign(g.clone()).await.unwrap();
    assert!(Arc::ptr_eq(
        &old,
        &h.driver.grants[&GrantId("g".into())].ownership
    ));
    old.expire();
    h.driver.on_assign(g).await.unwrap();
    let new = h.driver.grants[&GrantId("g".into())].ownership.clone();
    assert!(!Arc::ptr_eq(&old, &new));
    h.driver
        .on_action(GrantAction {
            id: GrantId("g".into()),
            ownership: old,
            action: Action::Release,
        })
        .await
        .unwrap();
    assert_eq!(h.driver.grants.len(), 1);
    assert!(!new.expired().is_closed());
}

#[tokio::test]
async fn unconfirmed_and_revoked_grants_are_not_extended() {
    let mut h = Harness::new();
    h.driver
        .on_assign(grant("stale", pb::GrantState::Active))
        .await
        .unwrap();
    h.driver
        .on_assign(grant("revoked", pb::GrantState::Revoked))
        .await
        .unwrap();
    h.driver
        .grants
        .get_mut(&GrantId("stale".into()))
        .unwrap()
        .confirmed = false;
    let before: Vec<_> = h
        .driver
        .grants
        .values()
        .map(|entry| entry.ownership.expiration())
        .collect();
    h.driver.on_extend(client_message::Extend {
        lease: Some(system_time_to_timestamp(
            SystemTime::now() + Duration::from_secs(600),
        )),
    });
    let after: Vec<_> = h
        .driver
        .grants
        .values()
        .map(|entry| entry.ownership.expiration())
        .collect();
    assert_eq!(before, after);
}

#[tokio::test]
async fn rejects_assignment_without_lease() {
    let mut h = Harness::new();
    let mut g = grant("missing", pb::GrantState::Active);
    g.lease = None;
    assert!(matches!(
        h.driver.on_assign(g).await,
        Err(ClientError::InvalidMessage(_))
    ));
    assert!(h.driver.grants.is_empty());
}

#[tokio::test]
async fn reconnect_replays_unexpired_grants_and_preserves_identity() {
    let mut h = Harness::new();
    let g = grant("g", pb::GrantState::Active);
    h.driver.on_assign(g.clone()).await.unwrap();
    let ownership = h.driver.grants[&GrantId("g".into())].ownership.clone();
    let (transport_tx, transport_rx) = std::sync::mpsc::channel();
    let (incoming, receiver) = mpsc::channel(8);
    incoming.send(Ok(established())).await.unwrap();
    let (outbound, _outgoing) = mpsc::channel(8);
    assert!(
        transport_tx
            .send(Transport {
                outbound,
                inbound: Box::pin(ReceiverStream::new(receiver))
            })
            .is_ok()
    );
    let receiver = Arc::new(std::sync::Mutex::new(transport_rx));
    let replay = Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = replay.clone();
    h.driver.reconnect = Some((
        ReconnectOptions {
            timeout: Duration::from_secs(1),
            backoff: Duration::from_millis(1),
        },
        Arc::new(move |active| {
            *captured.lock().unwrap() = active;
            let transport = receiver.lock().unwrap().try_recv().unwrap();
            Box::pin(async move { Ok(transport) })
        }),
    ));
    h.driver.reconnect().await.unwrap();
    assert_eq!(replay.lock().unwrap().len(), 1);
    assert!(!h.driver.grants[&GrantId("g".into())].confirmed);
    h.driver.on_assign(g).await.unwrap();
    assert!(Arc::ptr_eq(
        &ownership,
        &h.driver.grants[&GrantId("g".into())].ownership
    ));
}

#[tokio::test]
async fn reconnect_is_bounded_and_shutdown_interrupts_it() {
    let mut h = Harness::new();
    h.driver.reconnect = Some((
        ReconnectOptions {
            timeout: Duration::from_millis(10),
            backoff: Duration::from_millis(1),
        },
        Arc::new(|_| Box::pin(std::future::pending())),
    ));
    assert!(matches!(
        h.driver.reconnect().await,
        Err(ClientError::SessionClosed(_))
    ));
    h.handle.shutdown.close();
    assert!(matches!(
        h.driver.reconnect().await,
        Err(ClientError::SessionClosed(_))
    ));
}

fn established() -> JoinMessage {
    use splitter_proto::session_pb;
    JoinMessage {
        msg: Some(join_message::Msg::Session(session_pb::Message {
            request: Some(session_pb::message::Request::Established(
                session_pb::message::Established {
                    ttl: Some(system_time_to_timestamp(
                        SystemTime::now() + Duration::from_secs(60),
                    )),
                    ..Default::default()
                },
            )),
        })),
    }
}

#[tokio::test]
async fn silent_reconnected_stream_does_not_reset_recovery_deadline() {
    let mut h = Harness::new();
    h.driver.reconnect = Some((
        ReconnectOptions {
            timeout: Duration::from_millis(10),
            backoff: Duration::from_millis(1),
        },
        Arc::new(|_| {
            Box::pin(async {
                let (outbound, _receiver) = mpsc::channel(1);
                Ok(Transport {
                    outbound,
                    inbound: Box::pin(futures::stream::pending()),
                })
            })
        }),
    ));
    assert!(matches!(
        h.driver.reconnect().await,
        Err(ClientError::SessionClosed(_))
    ));
}

#[tokio::test]
async fn two_consumers_complete_unload_load_and_release_handover() {
    use crate::{
        ConnectionPool, DispatchFilter, FilterContext, InstanceId, Processor, RangeFactory,
        RecordingRange,
    };
    struct Factory {
        label: &'static str,
        log: Arc<std::sync::Mutex<Vec<String>>>,
    }
    impl RangeFactory for Factory {
        type Range = RecordingRange;
        fn build(&self, _: GrantId, _: Shard, _: Arc<Ownership>) -> Option<RecordingRange> {
            self.log
                .lock()
                .unwrap()
                .push(format!("{} built", self.label));
            Some(RecordingRange::new(
                self.label,
                Duration::ZERO,
                Duration::ZERO,
                self.log.clone(),
            ))
        }
    }
    async fn active(p: &Processor<Factory>, id: &str) {
        timeout(Duration::from_secs(2), async {
            while !p
                .grants()
                .get(&GrantId(id.into()))
                .is_some_and(|(_, state, _)| state == GrantState::Active)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut old = Harness::new();
    let mut new = Harness::new();
    let old_grant = grant("old", pb::GrantState::Active);
    let new_grant = grant("new", pb::GrantState::Allocated);
    let old_processor = Processor::new(Arc::new(Factory {
        label: "old",
        log: log.clone(),
    }));
    let new_processor = Processor::new(Arc::new(Factory {
        label: "new",
        log: log.clone(),
    }));
    for (processor, h) in [(&old_processor, &old), (&new_processor, &new)] {
        processor.init(FilterContext {
            service: QualifiedServiceName::new("test", "lifecycle"),
            pool: ConnectionPool::new(InstanceId::new()),
            cluster: h.handle.cluster.clone(),
        });
    }
    old.driver.on_assign(old_grant.clone()).await.unwrap();
    new.driver.on_assign(new_grant.clone()).await.unwrap();
    let old_assignment = old.handle.assignments.borrow()[&GrantId("old".into())].clone();
    let new_assignment = new.handle.assignments.borrow()[&GrantId("new".into())].clone();
    let old_p = old_processor.clone();
    let old_task = tokio::spawn(async move {
        old_p
            .try_handle(
                GrantId("old".into()),
                old_assignment.shard,
                old_assignment.ownership,
            )
            .await
    });
    let new_p = new_processor.clone();
    let new_task = tokio::spawn(async move {
        new_p
            .try_handle(
                GrantId("new".into()),
                new_assignment.shard,
                new_assignment.ownership,
            )
            .await
    });
    old.handle.task = Some(tokio::spawn(old.driver.run()));
    new.handle.task = Some(tokio::spawn(new.driver.run()));
    active(&old_processor, "old").await;
    assert!(!log.lock().unwrap().iter().any(|line| line == "new built"));
    let revoked = pb::Grant {
        state: pb::GrantState::Revoked as i32,
        ..old_grant
    };
    old.inbound
        .send(Ok(wrap_client(client_message::Msg::Revoke(
            client_message::Revoke {
                grants: vec![revoked.clone()],
            },
        ))))
        .await
        .unwrap();
    let client_message::Msg::Update(unloaded) = client_variant(next(&mut old.outbound).await)
    else {
        panic!("expected unload");
    };
    assert_eq!(
        unloaded.grant.as_ref().unwrap().state,
        pb::GrantState::RevokedUnloaded as i32
    );
    new.inbound
        .send(Ok(wrap_client(client_message::Msg::Notify(
            client_message::Notify {
                target: Some(new_grant.clone()),
                update: unloaded.grant,
            },
        ))))
        .await
        .unwrap();
    let client_message::Msg::Update(loaded) = client_variant(next(&mut new.outbound).await) else {
        panic!("expected load");
    };
    assert_eq!(
        loaded.grant.as_ref().unwrap().state,
        pb::GrantState::AllocatedLoaded as i32
    );
    old.inbound
        .send(Ok(wrap_client(client_message::Msg::Notify(
            client_message::Notify {
                target: Some(revoked),
                update: loaded.grant,
            },
        ))))
        .await
        .unwrap();
    let client_message::Msg::Released(released) = client_variant(next(&mut old.outbound).await)
    else {
        panic!("expected release");
    };
    assert_eq!(released.grants[0].id, "old");
    new.inbound
        .send(Ok(wrap_client(client_message::Msg::Promote(
            client_message::Promote {
                grants: vec![pb::Grant {
                    state: pb::GrantState::Active as i32,
                    ..new_grant
                }],
            },
        ))))
        .await
        .unwrap();
    active(&new_processor, "new").await;
    timeout(Duration::from_secs(2), old_task)
        .await
        .unwrap()
        .unwrap();
    assert!(old_processor.grants().is_empty());
    let events = log.lock().unwrap().clone();
    assert!(
        events.iter().position(|e| e == "old drain done").unwrap()
            < events.iter().position(|e| e == "new built").unwrap()
    );
    old.handle.shutdown().await.unwrap();
    drop(new.handle);
    timeout(Duration::from_secs(2), new_task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn duplicate_revoke_preserves_and_replays_unloaded_state() {
    let mut h = Harness::new();
    let g = grant("g", pb::GrantState::Revoked);
    h.driver.on_assign(g.clone()).await.unwrap();
    let o = h.driver.grants[&GrantId("g".into())].ownership.clone();
    h.driver
        .on_action(GrantAction {
            id: GrantId("g".into()),
            ownership: o,
            action: Action::Unload,
        })
        .await
        .unwrap();
    let _ = next(&mut h.outbound).await;
    h.driver.on_revoke(g).await.unwrap();
    let client_message::Msg::Update(update) = client_variant(next(&mut h.outbound).await) else {
        panic!("expected unload replay");
    };
    assert_eq!(
        update.grant.unwrap().state,
        pb::GrantState::RevokedUnloaded as i32
    );
}

#[tokio::test]
async fn assignment_replay_resends_local_load_and_revoke_request() {
    let mut h = Harness::new();
    let g = grant("g", pb::GrantState::Allocated);
    h.driver.on_assign(g.clone()).await.unwrap();
    let o = h.driver.grants[&GrantId("g".into())].ownership.clone();
    h.driver
        .on_action(GrantAction {
            id: GrantId("g".into()),
            ownership: o.clone(),
            action: Action::Load,
        })
        .await
        .unwrap();
    let _ = next(&mut h.outbound).await;
    o.request_revoke();
    h.driver.on_assign(g).await.unwrap();
    let client_message::Msg::Update(update) = client_variant(next(&mut h.outbound).await) else {
        panic!("expected load replay");
    };
    assert_eq!(
        update.grant.unwrap().state,
        pb::GrantState::AllocatedLoaded as i32
    );
    assert!(matches!(
        client_variant(next(&mut h.outbound).await),
        client_message::Msg::Revoke(_)
    ));
}
