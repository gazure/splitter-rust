use super::*;
use splitter_proto::session_pb::{self, message::Request};

pub(super) fn session_frame(ttl: SystemTime, ack: bool) -> JoinMessage {
    let ttl = Some(system_time_to_timestamp(ttl));
    let request = if ack {
        Request::Ack(session_pb::message::HeartbeatAck { ttl })
    } else {
        Request::Established(session_pb::message::Established {
            ttl,
            ..Default::default()
        })
    };
    JoinMessage {
        msg: Some(join_message::Msg::Session(session_pb::Message {
            request: Some(request),
        })),
    }
}

async fn advance(duration: Duration) {
    tokio::time::advance(duration).await;
    tokio::task::yield_now().await;
}

async fn start(h: &mut Harness, ttl: Duration) {
    h.inbound
        .send(Ok(session_frame(SystemTime::now() + ttl, false)))
        .await
        .unwrap();
    // Move the driver without replacing its externally held channel endpoints.
    let replacement = Harness::new();
    let driver = std::mem::replace(&mut h.driver, replacement.driver);
    h.handle.task = Some(tokio::spawn(driver.run()));
    tokio::task::yield_now().await;
}

#[tokio::test(start_paused = true)]
async fn silent_session_expires_and_cleans_up_live_grants() {
    let mut h = Harness::new();
    h.driver
        .on_assign(grant("g", pb::GrantState::Active))
        .await
        .unwrap();
    let ownership = h.driver.grants[&GrantId("g".into())].ownership.clone();
    start(&mut h, Duration::from_secs(1)).await;

    advance(Duration::from_secs(2)).await;
    assert!(
        h.handle.closed().is_closed(),
        "silent session must expire without EOF"
    );
    assert!(ownership.expired().is_closed());
    assert!(h.handle.assignments.borrow().is_empty());
    assert!(matches!(
        h.handle.shutdown().await,
        Err(ClientError::SessionClosed(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn heartbeat_ack_renews_session_but_not_forever() {
    let mut h = Harness::new();
    start(&mut h, Duration::from_secs(1)).await;
    advance(Duration::from_millis(500)).await;
    h.inbound
        .send(Ok(session_frame(
            SystemTime::now() + Duration::from_secs(3),
            true,
        )))
        .await
        .unwrap();
    tokio::task::yield_now().await;

    advance(Duration::from_secs(1)).await;
    assert!(
        !h.handle.closed().is_closed(),
        "ack must replace the original deadline"
    );
    advance(Duration::from_secs(3)).await;
    assert!(
        h.handle.closed().is_closed(),
        "renewed session still expires without another ack"
    );
}

#[tokio::test(start_paused = true)]
async fn blocked_heartbeat_send_cannot_delay_session_expiration() {
    let mut h = Harness::new();
    while h
        .driver
        .transport
        .as_ref()
        .unwrap()
        .outbound
        .try_send(JoinMessage::default())
        .is_ok()
    {}
    start(&mut h, Duration::from_secs(1)).await;

    advance(Duration::from_millis(500)).await;
    assert!(!h.handle.closed().is_closed());
    advance(Duration::from_secs(1)).await;
    assert!(
        h.handle.closed().is_closed(),
        "session deadline must interrupt the five-second send wait"
    );
}

#[tokio::test(start_paused = true)]
async fn heartbeat_ack_can_shorten_the_session_deadline() {
    let mut h = Harness::new();
    start(&mut h, Duration::from_secs(30)).await;
    h.inbound
        .send(Ok(session_frame(
            SystemTime::now() + Duration::from_secs(1),
            true,
        )))
        .await
        .unwrap();
    tokio::task::yield_now().await;
    advance(Duration::from_secs(2)).await;
    assert!(h.handle.closed().is_closed());
}

#[tokio::test(start_paused = true)]
async fn unrelated_inbound_messages_do_not_extend_the_session() {
    let mut h = Harness::new();
    start(&mut h, Duration::from_secs(1)).await;
    advance(Duration::from_millis(500)).await;
    h.inbound.send(Ok(JoinMessage::default())).await.unwrap();
    tokio::task::yield_now().await;
    advance(Duration::from_millis(750)).await;
    assert!(h.handle.closed().is_closed());
}

#[tokio::test(start_paused = true)]
async fn queued_ack_cannot_revive_an_expired_session() {
    let mut h = Harness::new();
    h.inbound
        .send(Ok(session_frame(SystemTime::UNIX_EPOCH, false)))
        .await
        .unwrap();
    h.inbound
        .send(Ok(session_frame(
            SystemTime::now() + Duration::from_secs(60),
            true,
        )))
        .await
        .unwrap();
    h.handle.task = Some(tokio::spawn(h.driver.run()));
    tokio::task::yield_now().await;
    assert!(h.handle.closed().is_closed());
    assert!(matches!(
        h.handle.shutdown().await,
        Err(ClientError::SessionClosed(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn initial_stream_without_established_has_a_deadline() {
    let mut h = Harness::new();
    h.handle.task = Some(tokio::spawn(h.driver.run()));
    tokio::task::yield_now().await;
    advance(Duration::from_secs(31)).await;
    assert!(h.handle.closed().is_closed());
    assert!(matches!(
        h.handle.shutdown().await,
        Err(ClientError::SessionClosed(_))
    ));
}

fn pending_reconnect() -> (ReconnectOptions, Connector) {
    (
        ReconnectOptions {
            timeout: Some(Duration::from_secs(2)),
            backoff: Duration::from_millis(100),
        },
        Arc::new(|_| Box::pin(std::future::pending())),
    )
}

#[tokio::test(start_paused = true)]
async fn session_expiration_reconnects_and_preserves_valid_ownership() {
    let mut h = Harness::new();
    let g = grant("g", pb::GrantState::Active);
    h.driver.on_assign(g.clone()).await.unwrap();
    let ownership = h.driver.grants[&GrantId("g".into())].ownership.clone();
    let (incoming, receiver) = mpsc::channel(8);
    incoming
        .send(Ok(session_frame(
            SystemTime::now() + Duration::from_secs(10),
            false,
        )))
        .await
        .unwrap();
    incoming
        .send(Ok(reconnection::empty_snapshot()))
        .await
        .unwrap();
    incoming
        .send(Ok(wrap_client(client_message::Msg::Assign(
            client_message::Assign { grants: vec![g] },
        ))))
        .await
        .unwrap();
    let (outbound, mut outgoing) = mpsc::channel(8);
    let transport = std::sync::Mutex::new(Some(Transport {
        outbound,
        inbound: Box::pin(ReceiverStream::new(receiver)),
    }));
    let (replay_tx, mut replay_rx) = mpsc::channel(1);
    h.driver.reconnect = Some((
        pending_reconnect().0,
        Arc::new(move |active| {
            replay_tx.try_send(active).unwrap();
            let next = transport.lock().unwrap().take().expect("one reconnect");
            Box::pin(async move { Ok(next) })
        }),
    ));
    start(&mut h, Duration::from_secs(1)).await;
    advance(Duration::from_secs(1)).await;
    advance(Duration::from_millis(200)).await;

    let replay = replay_rx
        .try_recv()
        .expect("expired session must reconnect");
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].id, "g");
    let assignment = h.handle.assignments.borrow()[&GrantId("g".into())].clone();
    assert!(Arc::ptr_eq(&ownership, &assignment.ownership));
    assert!(!ownership.expired().is_closed());
    advance(Duration::from_secs(2)).await;
    assert!(
        !h.handle.closed().is_closed(),
        "recovery must install the new session deadline"
    );
    assert!(
        outgoing.try_recv().is_err(),
        "old heartbeat interval must also be replaced"
    );
    ownership.release();
    h.handle.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn failed_recovery_after_session_expiration_is_bounded() {
    let mut h = Harness::new();
    h.driver.reconnect = Some(pending_reconnect());
    start(&mut h, Duration::from_secs(1)).await;
    advance(Duration::from_secs(1)).await;
    assert!(
        !h.handle.closed().is_closed(),
        "recovery gets its own bounded window"
    );
    advance(Duration::from_secs(3)).await;
    assert!(h.handle.closed().is_closed());
    assert!(matches!(
        h.handle.shutdown().await,
        Err(ClientError::DisconnectTimeout)
    ));
}

#[tokio::test(start_paused = true)]
async fn shutdown_interrupts_recovery_triggered_by_session_expiration() {
    let mut h = Harness::new();
    h.driver.reconnect = Some(pending_reconnect());
    start(&mut h, Duration::from_secs(1)).await;
    advance(Duration::from_secs(1)).await;
    h.handle.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn draining_session_expires_without_reconnecting() {
    let mut h = Harness::new();
    h.driver
        .on_assign(grant("g", pb::GrantState::Active))
        .await
        .unwrap();
    h.driver.reconnect = Some((
        pending_reconnect().0,
        Arc::new(|_| {
            panic!("shutdown must not start a new session");
        }),
    ));
    start(&mut h, Duration::from_secs(1)).await;
    h.handle.shutdown.close();
    tokio::task::yield_now().await;
    advance(Duration::from_secs(2)).await;
    assert!(
        !h.handle.closed().is_closed(),
        "offline drain retains grants until completion or timeout"
    );
    advance(Duration::from_secs(30)).await;
    assert!(h.handle.closed().is_closed());
    assert!(h.handle.assignments.borrow().is_empty());
    assert!(matches!(
        h.handle.shutdown().await,
        Err(ClientError::DrainTimeout)
    ));
}

#[tokio::test(start_paused = true)]
async fn expired_reconnect_handshakes_do_not_reset_the_recovery_budget() {
    let mut h = Harness::new();
    h.driver.reconnect = Some((
        pending_reconnect().0,
        Arc::new(|_| {
            Box::pin(async {
                let (outbound, _receiver) = mpsc::channel(1);
                Ok(Transport {
                    outbound,
                    inbound: Box::pin(futures::stream::iter([Ok(session_frame(
                        SystemTime::UNIX_EPOCH,
                        false,
                    ))])),
                })
            })
        }),
    ));
    let started = tokio::time::Instant::now();
    assert!(matches!(
        h.driver.reconnect().await,
        Err(ClientError::DisconnectTimeout)
    ));
    assert!(started.elapsed() < Duration::from_secs(3));
}
