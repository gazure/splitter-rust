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
                    ..Default::default()
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
