//! Live Join-protocol tests. Run against an isolated service with a global domain
//! and at least two shards, with no other consumers attached to that service.
//! Set `SPLITTER_REGION` if the service region differs from `region`.
//! The UNIT routing test requires a separate service with a `leader` UNIT domain.
//! Run `python3 tools/live_splitter.py` from the workspace root to provision a
//! disposable Docker server and run all tests. To use an existing server:
//!
//! ```sh
//! splitterctl --insecure -e localhost:8500 tenants new client-test
//! splitterctl --insecure -e localhost:8500 services new client-test/ranges region
//! splitterctl --insecure -e localhost:8500 domains new global client-test/ranges/entities --shards 4
//! splitterctl --insecure -e localhost:8500 services new client-test/unit region
//! splitterctl --insecure -e localhost:8500 domains new unit client-test/unit/leader
//! SPLITTER_ENDPOINT=http://localhost:8500 SPLITTER_SERVICE=client-test/ranges \
//!   SPLITTER_UNIT_SERVICE=client-test/unit SPLITTER_SHARDS=4 \
//!   cargo test -p splitter-client --test e2e -- --ignored --test-threads=1
//! ```
//!
//! These tests are explicitly ignored by default. Selecting them requires valid
//! environment settings and a reachable server; missing setup never passes.
//! They exercise lifecycle convergence, not application-state durability.

#[path = "../support/recording_range.rs"]
mod recording_range;

use recording_range::RecordingRange;

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use splitter_client::{
    ConsumerClient, ConsumerOptions, Dispatcher, DispatcherBuilder, DomainKey, DomainType,
    Endpoint, GrantId, GrantState, Instance, InstanceId, Location, Ownership, Processor,
    QualifiedDomainKey, QualifiedDomainName, QualifiedServiceName, RangeFactory, Shard,
};
use tonic::transport::Channel;

struct Env {
    endpoint: String,
    service: QualifiedServiceName,
}

fn read_env() -> Env {
    let endpoint =
        std::env::var("SPLITTER_ENDPOINT").expect("set SPLITTER_ENDPOINT for live tests");
    let service = QualifiedServiceName::parse(
        &std::env::var("SPLITTER_SERVICE")
            .expect("set SPLITTER_SERVICE to an isolated test service"),
    )
    .expect("SPLITTER_SERVICE must be tenant/service");
    Env { endpoint, service }
}

fn test_instance() -> Instance {
    Instance {
        id: InstanceId::new(),
        location: Location::new(
            std::env::var("SPLITTER_REGION").unwrap_or_else(|_| "region".into()),
            "test",
        ),
        name: "splitter-rs-e2e".into(),
        created: SystemTime::now(),
        endpoint: Endpoint::default(),
    }
}

async fn connect(endpoint: &str) -> Channel {
    Channel::from_shared(endpoint.to_string())
        .expect("valid URL")
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .expect("server reachable")
}

#[tokio::test]
#[ignore = "requires an isolated live Splitter service; see module instructions"]
async fn join_receives_initial_cluster_update() {
    let env = read_env();
    let client = ConsumerClient::new(connect(&env.endpoint).await);
    let handle = client
        .join(test_instance(), env.service, ConsumerOptions::default())
        .await
        .expect("join succeeds");
    let mut cluster = handle.cluster.clone();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if cluster.borrow_and_update().id().version > 0 {
                break;
            }
            cluster
                .changed()
                .await
                .expect("session remains open until initial snapshot");
        }
    })
    .await
    .expect("initial cluster snapshot arrived within 5s");

    // This test deliberately has no range handlers. Force-close instead of
    // waiting for a graceful unload handshake no handler can complete.
    let closed = handle.closed();
    drop(handle);
    tokio::time::timeout(Duration::from_secs(5), closed.closed())
        .await
        .expect("dropping the handle stops the driver");
}

#[tokio::test]
#[ignore = "requires an isolated live Splitter service; see module instructions"]
async fn shutdown_closes_driver_cleanly() {
    let env = read_env();
    let client = ConsumerClient::new(connect(&env.endpoint).await);
    let handle = client
        .join(test_instance(), env.service, ConsumerOptions::default())
        .await
        .expect("join succeeds");
    let closed = handle.closed();
    // Assignments can arrive before shutdown is polled. There is no application
    // state in this test, so release every observed assignment immediately.
    let mut assignments = handle.assignments.clone();
    let releaser = tokio::spawn(async move {
        loop {
            for assignment in assignments.borrow_and_update().values() {
                assignment.ownership.release();
            }
            if assignments.changed().await.is_err() {
                break;
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), handle.shutdown())
        .await
        .expect("immediate shutdown completes in 5s")
        .expect("no shutdown error");
    releaser
        .await
        .expect("assignment releaser exits with the driver");
    assert!(closed.is_closed(), "closed latch fires after shutdown");
}

#[tokio::test]
#[ignore = "requires a live Splitter endpoint; see module instructions"]
async fn join_returns_error_when_service_missing() {
    let env = read_env();
    let client = ConsumerClient::new(connect(&env.endpoint).await);
    let bogus = QualifiedServiceName::new(
        env.service.tenant,
        format!("does-not-exist-{}", uuid::Uuid::new_v4()),
    );
    let handle = client
        .join(
            test_instance(),
            bogus,
            ConsumerOptions::default().without_reconnect(),
        )
        .await
        .expect("Join establishes the stream before asynchronous registration rejection");
    tokio::time::timeout(Duration::from_secs(5), handle.closed().closed())
        .await
        .expect("driver exits within 5s for unknown service");
    assert!(
        handle.shutdown().await.is_err(),
        "registration rejection is surfaced to the caller"
    );
}

#[tokio::test]
#[ignore = "requires a live Splitter endpoint; see module instructions"]
async fn missing_service_retries_until_configured_disconnect_timeout() {
    let env = read_env();
    let client = ConsumerClient::new(connect(&env.endpoint).await);
    let bogus = QualifiedServiceName::new(
        env.service.tenant,
        format!("does-not-exist-{}", uuid::Uuid::new_v4()),
    );
    let started = tokio::time::Instant::now();
    let handle = client
        .join(
            test_instance(),
            bogus,
            ConsumerOptions::default().with_disconnect_timeout(Duration::from_secs(3)),
        )
        .await
        .expect("consumer starts before registration succeeds");
    tokio::time::timeout(Duration::from_secs(10), handle.closed().closed())
        .await
        .expect("disconnect timeout stops repeated registration attempts");
    assert!(started.elapsed() >= Duration::from_secs(3));
    assert!(matches!(
        handle.shutdown().await,
        Err(splitter_client::ClientError::DisconnectTimeout)
    ));
}

struct TestFactory;

impl RangeFactory for TestFactory {
    type Range = RecordingRange;

    fn build(&self, id: GrantId, _shard: Shard, _ownership: Arc<Ownership>) -> Option<Self::Range> {
        Some(RecordingRange::new(
            id.to_string(),
            Duration::from_millis(5),
            Duration::from_millis(5),
            Arc::new(Mutex::new(Vec::new())),
        ))
    }
}

async fn dispatcher(env: &Env) -> (Dispatcher, Arc<Processor<TestFactory>>) {
    let processor = Processor::new(Arc::new(TestFactory));
    let dispatcher = DispatcherBuilder::new(env.service.clone(), test_instance())
        .filter(processor.clone())
        .build(connect(&env.endpoint).await)
        .await
        .expect("dispatcher joins isolated test service");
    (dispatcher, processor)
}

fn active_shards(processor: &Processor<TestFactory>) -> HashSet<Shard> {
    processor
        .grants()
        .ids()
        .iter()
        .filter_map(|id| {
            processor
                .grants()
                .get(id)
                .and_then(|(shard, state, _)| (state == GrantState::Active).then_some(shard))
        })
        .collect()
}

async fn eventually(description: &str, condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out: {description}"));
}

#[tokio::test]
#[ignore = "requires a live Splitter service with a leader UNIT domain; see module instructions"]
async fn unit_domain_routes_keys_from_live_cluster_snapshot() {
    let mut env = read_env();
    env.service = QualifiedServiceName::parse(
        &std::env::var("SPLITTER_UNIT_SERVICE")
            .expect("set SPLITTER_UNIT_SERVICE to an isolated service with a leader UNIT domain"),
    )
    .expect("SPLITTER_UNIT_SERVICE must be tenant/service");
    let domain = QualifiedDomainName {
        service: env.service.clone(),
        name: "leader".into(),
    };
    let (dispatcher, ranges) = dispatcher(&env).await;
    eventually(
        "UNIT grant becomes active locally and in the cluster",
        || {
            active_shards(&ranges)
                .iter()
                .any(|shard| shard.kind == DomainType::Unit && shard.domain == domain)
                && dispatcher
                    .cluster()
                    .grants()
                    .values()
                    .any(|grant| grant.shard.domain == domain && grant.state == GrantState::Active)
        },
    )
    .await;

    let cluster = dispatcher.cluster();
    for value in [
        "",
        "00000000-0000-0000-0000-000000000000",
        "ffffffff-ffff-ffff-ffff-ffffffffffff",
    ] {
        for region in ["", "another-region"] {
            let key = QualifiedDomainKey {
                domain: domain.clone(),
                key: DomainKey {
                    key: value.into(),
                    region: region.into(),
                },
            };
            let owner = cluster
                .lookup(&key, &[GrantState::Active])
                .expect("live UNIT owner covers every key and region");
            assert_eq!(owner.consumer, dispatcher.id().id);
        }
    }
    dispatcher
        .drain(Duration::from_secs(30))
        .await
        .expect("UNIT consumer drains");
    assert!(ranges.grants().is_empty());
}

#[tokio::test]
#[ignore = "requires an isolated live Splitter service with >=2 shards; see module instructions"]
async fn two_consumers_rebalance_and_gracefully_transfer_all_ranges() {
    let env = read_env();
    let expected: usize = std::env::var("SPLITTER_SHARDS")
        .expect("set SPLITTER_SHARDS")
        .parse()
        .expect("SPLITTER_SHARDS must be an integer");
    assert!(expected >= 2, "handover test requires at least two shards");
    let (first, first_ranges) = dispatcher(&env).await;
    eventually("first consumer owns all configured shards", || {
        active_shards(&first_ranges).len() == expected
    })
    .await;
    let original = active_shards(&first_ranges);
    let (second, second_ranges) = dispatcher(&env).await;
    eventually(
        "both consumers own disjoint subsets of the original shards",
        || {
            let a = active_shards(&first_ranges);
            let b = active_shards(&second_ranges);
            !a.is_empty()
                && !b.is_empty()
                && a.is_disjoint(&b)
                && a.union(&b).cloned().collect::<HashSet<_>>() == original
        },
    )
    .await;

    first
        .drain(Duration::from_secs(30))
        .await
        .expect("first consumer drains cleanly");
    assert!(
        first_ranges.grants().is_empty(),
        "drain joins range handlers"
    );
    eventually(
        "remaining consumer owns exactly the original shard set",
        || active_shards(&second_ranges) == original,
    )
    .await;
    second
        .drain(Duration::from_secs(30))
        .await
        .expect("last consumer drains cleanly");
    assert!(
        second_ranges.grants().is_empty(),
        "last drain clears range handlers"
    );
}
