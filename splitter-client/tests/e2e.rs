//! End-to-end integration test, gated on a running splitter server.
//!
//! These tests are skipped (returning OK) unless the caller supplies both
//! `SPLITTER_ENDPOINT` (e.g. `http://localhost:50051`) and `SPLITTER_SERVICE`
//! (e.g. `t/s`). The expected setup is:
//!
//! ```sh
//! # Terminal 1: start a splitter
//! ./splitter --port 50051
//!
//! # Terminal 2: create a tenant/service/domain
//! ./splitterctl tenants new t
//! ./splitterctl services new t/s
//! ./splitterctl domains new unit t/s/leader
//!
//! # Terminal 3: run the e2e tests
//! SPLITTER_ENDPOINT=http://localhost:50051 SPLITTER_SERVICE=t/s \
//!   cargo test --test e2e -- --nocapture
//! ```
//!
//! Running against a fresh splitter with no created service will surface
//! a gRPC `NotFound` error on join — that path is covered by
//! [`join_returns_error_when_service_missing`].

use std::time::{Duration, SystemTime};

use splitter_client::{
    ConsumerClient, ConsumerOptions, Endpoint, Instance, InstanceId, Location, QualifiedServiceName
};
use tonic::transport::Channel;

struct Env {
    endpoint: String,
    service: QualifiedServiceName,
}

fn read_env() -> Option<Env> {
    let endpoint = std::env::var("SPLITTER_ENDPOINT").ok()?;
    let service_raw = std::env::var("SPLITTER_SERVICE").ok()?;
    let service = QualifiedServiceName::parse(&service_raw)?;
    Some(Env { endpoint, service })
}

fn test_instance() -> Instance {
    Instance {
        id: InstanceId::new(),
        location: Location::new("local", "test"),
        name: "splitter-rs-e2e".into(),
        created: SystemTime::now(),
        endpoint: Endpoint::default(),
    }
}

async fn connect(endpoint: &str) -> Channel {
    Channel::from_shared(endpoint.to_string())
        .expect("valid URL")
        .connect()
        .await
        .expect("server reachable")
}

#[tokio::test]
async fn join_receives_initial_cluster_update() {
    let Some(env) = read_env() else {
        eprintln!("SPLITTER_ENDPOINT or SPLITTER_SERVICE not set; skipping");
        return;
    };
    let channel = connect(&env.endpoint).await;
    let client = ConsumerClient::new(channel);

    let handle = client
        .join(test_instance(), env.service, ConsumerOptions::default())
        .await
        .expect("join succeeds");

    let mut cluster = handle.cluster.clone();
    // First change is the initial snapshot from the server. Bound to 5s so
    // the test fails loudly instead of hanging if the coordinator is stuck.
    tokio::time::timeout(Duration::from_secs(5), cluster.changed())
        .await
        .expect("cluster update arrived within 5s")
        .expect("sender not dropped");

    let snap = cluster.borrow_and_update().clone();
    assert!(
        snap.id.version > 0,
        "cluster snapshot should have version > 0; got {:?}",
        snap.id
    );

    handle.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn shutdown_closes_driver_cleanly() {
    let Some(env) = read_env() else {
        eprintln!("SPLITTER_ENDPOINT or SPLITTER_SERVICE not set; skipping");
        return;
    };
    let channel = connect(&env.endpoint).await;
    let client = ConsumerClient::new(channel);

    let handle = client
        .join(test_instance(), env.service, ConsumerOptions::default())
        .await
        .expect("join succeeds");

    let closed = handle.closed();
    assert!(!closed.is_closed(), "freshly-joined handle shouldn't be closed");

    // Graceful shutdown should both send Deregister+Closed and settle within a
    // reasonable timeout.
    tokio::time::timeout(Duration::from_secs(5), handle.shutdown())
        .await
        .expect("shutdown completes in <5s")
        .expect("no shutdown error");

    assert!(closed.is_closed(), "closed latch should fire after shutdown");
}

#[tokio::test]
async fn join_returns_error_when_service_missing() {
    let Some(env) = read_env() else {
        eprintln!("SPLITTER_ENDPOINT not set; skipping");
        return;
    };
    let channel = connect(&env.endpoint).await;
    let client = ConsumerClient::new(channel);

    // A random service under the same tenant that certainly does not exist.
    let bogus = QualifiedServiceName::new(
        env.service.tenant.clone(),
        format!("does-not-exist-{}", uuid::Uuid::new_v4()),
    );

    let handle = client
        .join(test_instance(), bogus, ConsumerOptions::default())
        .await
        .expect("join call returns a handle even for unknown service");

    // The server should surface an error on the inbound stream within a
    // short window. We observe it via the `closed` latch and the driver task
    // returning an error on shutdown.
    let closed = handle.closed();
    tokio::time::timeout(Duration::from_secs(5), closed.closed())
        .await
        .expect("driver exits within 5s for unknown service");
}
