use super::*;
use crate::ids::{DomainKey, Endpoint, Instance, InstanceId, Location, QualifiedDomainKey};
use splitter_proto::pb::cluster_message as cm;

fn snapshot(assignment: bool) -> JoinMessage {
    let grant = grant("peer-grant", pb::GrantState::Active);
    let shard = grant.shard.unwrap();
    let consumer = Instance {
        id: InstanceId::new(),
        location: Location::new("region", "peer"),
        name: "peer".into(),
        created: SystemTime::UNIX_EPOCH,
        endpoint: Endpoint::new("peer:8080"),
    };
    JoinMessage {
        msg: Some(join_message::Msg::Consumer(ConsumerMessage {
            msg: Some(consumer_message::Msg::Cluster(pb::ClusterMessage {
                version: if assignment { 1 } else { 2 },
                msg: Some(cm::Msg::Snapshot(cm::Snapshot {
                    origin: Some(consumer.clone().into()),
                    shards: vec![shard.clone()],
                    assignments: if assignment {
                        vec![cm::Assignment {
                            consumer: Some(consumer.into()),
                            grants: vec![cm::GrantInfo {
                                id: grant.id,
                                shard: Some(shard),
                                state: grant.state,
                            }],
                        }]
                    } else {
                        vec![]
                    },
                })),
                ..Default::default()
            })),
        })),
    }
}

fn assert_peer_route(cluster: &ClusterMap) {
    let owner = cluster
        .lookup(
            &QualifiedDomainKey {
                domain: QualifiedDomainName {
                    service: QualifiedServiceName::new("test", "lifecycle"),
                    name: "ranges".into(),
                },
                key: DomainKey {
                    key: "key".into(),
                    ..Default::default()
                },
            },
            &[],
        )
        .expect("peer route must survive recovery");
    assert_eq!(owner.id.as_str(), "peer-grant");
    assert_eq!(
        cluster.consumers[&owner.consumer]
            .consumer
            .endpoint
            .as_str(),
        "peer:8080"
    );
}

#[tokio::test(start_paused = true)]
async fn reconnect_preserves_routes_until_partial_snapshot_and_clears_on_close() {
    let mut h = Harness::new();
    h.driver.handle_inbound(snapshot(true)).await.unwrap();
    let cluster = h.handle.cluster.clone();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let captured_calls = calls.clone();
    let (incoming, receiver) = mpsc::channel(8);
    incoming.send(Ok(established())).await.unwrap();
    let (outbound, _outgoing) = mpsc::channel(8);
    let transport = Arc::new(std::sync::Mutex::new(Some(Transport {
        outbound,
        inbound: Box::pin(ReceiverStream::new(receiver)),
    })));
    h.driver.reconnect = Some((
        ReconnectOptions {
            timeout: Duration::from_secs(10),
            backoff: Duration::from_secs(1),
        },
        Arc::new(move |_| {
            assert_peer_route(&cluster.borrow());
            if captured_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                Box::pin(async { Err(ClientError::Transport(tonic::Status::unavailable("retry"))) })
            } else {
                let transport = transport.lock().unwrap().take().unwrap();
                Box::pin(async move { Ok(transport) })
            }
        }),
    ));
    h.driver.reconnect().await.unwrap();
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_peer_route(&h.handle.cluster.borrow());
    h.driver.handle_inbound(snapshot(false)).await.unwrap();
    assert_eq!(h.handle.cluster.borrow().id.version, 2);
    assert_peer_route(&h.handle.cluster.borrow());
    drop(h.driver);
    assert!(h.handle.cluster.borrow().grants.is_empty());
    assert!(h.handle.cluster.borrow().consumers.is_empty());
    assert!(h.handle.closed().is_closed());
}

#[tokio::test(start_paused = true)]
async fn failed_recovery_clears_retained_routes() {
    let mut h = Harness::new();
    h.driver.handle_inbound(snapshot(true)).await.unwrap();
    let cluster = h.handle.cluster.clone();
    h.driver.reconnect = Some((
        ReconnectOptions {
            timeout: Duration::from_secs(2),
            backoff: Duration::from_secs(1),
        },
        Arc::new(move |_| {
            assert_peer_route(&cluster.borrow());
            Box::pin(std::future::pending())
        }),
    ));
    h.inbound
        .send(Err(tonic::Status::unavailable("restart")))
        .await
        .unwrap();
    assert!(matches!(
        h.driver.run().await,
        Err(ClientError::SessionClosed(_))
    ));
    assert!(h.handle.closed().is_closed());
    assert!(h.handle.cluster.borrow().grants.is_empty());
    assert!(h.handle.cluster.borrow().consumers.is_empty());
}

fn incremental(cluster: &ClusterMap, version: i64) -> JoinMessage {
    JoinMessage {
        msg: Some(join_message::Msg::Consumer(ConsumerMessage {
            msg: Some(consumer_message::Msg::Cluster(pb::ClusterMessage {
                id: cluster.id.origin.as_ref().unwrap().id.to_string(),
                version,
                msg: Some(cm::Msg::Change(cm::Change::default())),
                ..Default::default()
            })),
        })),
    }
}

#[tokio::test]
async fn invalid_cluster_update_preserves_map_and_requires_snapshot() {
    let mut h = Harness::new();
    h.driver.handle_inbound(snapshot(true)).await.unwrap();
    let previous = h.handle.cluster.borrow().clone();
    assert!(matches!(
        h.driver.handle_inbound(incremental(&previous, 3)).await,
        Err(ClientError::InvalidCluster(_))
    ));
    assert!(Arc::ptr_eq(&previous, &h.handle.cluster.borrow()));
    // Even a correctly sequenced delta cannot repair a broken stream history.
    assert!(matches!(
        h.driver.handle_inbound(incremental(&previous, 2)).await,
        Err(ClientError::InvalidCluster(_))
    ));
    h.driver.handle_inbound(snapshot(false)).await.unwrap();
    let repaired = h.handle.cluster.borrow().clone();
    h.driver
        .handle_inbound(incremental(&repaired, 3))
        .await
        .unwrap();
    assert_eq!(h.handle.cluster.borrow().id.version, 3);
    assert_peer_route(&h.handle.cluster.borrow());
}

#[tokio::test(start_paused = true)]
async fn invalid_cluster_update_reconnects_and_accepts_fresh_snapshot() {
    let mut h = Harness::new();
    h.driver.handle_inbound(snapshot(true)).await.unwrap();
    let previous = h.handle.cluster.borrow().clone();
    let (incoming, receiver) = mpsc::channel(8);
    incoming.send(Ok(established())).await.unwrap();
    incoming.send(Ok(snapshot(false))).await.unwrap();
    let (outbound, _outgoing) = mpsc::channel(8);
    let transport = Arc::new(std::sync::Mutex::new(Some(Transport {
        outbound,
        inbound: Box::pin(ReceiverStream::new(receiver)),
    })));
    h.driver.reconnect = Some((
        ReconnectOptions {
            timeout: Duration::from_secs(10),
            backoff: Duration::from_secs(1),
        },
        Arc::new(move |_| {
            let transport = transport
                .lock()
                .unwrap()
                .take()
                .expect("one recovery attempt");
            Box::pin(async move { Ok(transport) })
        }),
    ));
    h.inbound
        .send(Ok(incremental(&previous, 99)))
        .await
        .unwrap();
    h.handle.task = Some(tokio::spawn(h.driver.run()));
    timeout(Duration::from_secs(5), async {
        while h.handle.cluster.borrow().id.version != 2 {
            h.handle.cluster.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_peer_route(&h.handle.cluster.borrow());
    assert!(!h.handle.closed().is_closed());
    h.handle.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn invalid_cluster_update_without_recovery_closes_and_clears_routes() {
    let mut h = Harness::new();
    h.driver.handle_inbound(snapshot(true)).await.unwrap();
    let previous = h.handle.cluster.borrow().clone();
    h.inbound
        .send(Ok(incremental(&previous, 99)))
        .await
        .unwrap();
    assert!(matches!(
        h.driver.run().await,
        Err(ClientError::InvalidCluster(_))
    ));
    assert!(h.handle.closed().is_closed());
    assert!(h.handle.cluster.borrow().grants.is_empty());
}

#[tokio::test(start_paused = true)]
async fn cluster_recovery_requires_snapshot_within_total_recovery_budget() {
    let mut h = Harness::new();
    h.driver.handle_inbound(snapshot(true)).await.unwrap();
    let previous = h.handle.cluster.borrow().clone();
    let (incoming, receiver) = mpsc::channel(8);
    incoming.send(Ok(established())).await.unwrap();
    let (outbound, _outgoing) = mpsc::channel(8);
    let transport = Arc::new(std::sync::Mutex::new(Some(Transport {
        outbound,
        inbound: Box::pin(ReceiverStream::new(receiver)),
    })));
    h.driver.reconnect = Some((
        ReconnectOptions {
            timeout: Duration::from_secs(3),
            backoff: Duration::from_secs(1),
        },
        Arc::new(move |_| {
            let transport = transport.lock().unwrap().take().unwrap();
            Box::pin(async move { Ok(transport) })
        }),
    ));
    h.inbound
        .send(Ok(incremental(&previous, 99)))
        .await
        .unwrap();
    let start = tokio::time::Instant::now();
    let error = h.driver.run().await.unwrap_err();
    assert!(matches!(error, ClientError::SessionClosed(ref s) if s == "cluster recovery timeout"));
    assert_eq!(start.elapsed(), Duration::from_secs(3));
    assert!(h.handle.cluster.borrow().grants.is_empty());
}
