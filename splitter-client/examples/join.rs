//! Phase-2 smoke: connect to a splitter, join a service, and drive the
//! ownership lifecycle for each assigned grant.
//!
//! For every grant we receive, we spawn a task that:
//!   1. waits for counterpart unload, then calls `loader.load()`
//!   2. waits for activation
//!   3. waits for revocation, then calls `unloader.unload()`
//!   4. waits for counterpart load
//!
//! This is a minimal but complete lifecycle driver — Phase 3's `Processor`
//! generalizes this over a user-supplied `Range`.
//!
//! Usage:
//!     cargo run --example join -- <endpoint> <tenant/service> [region] [node]

use std::{collections::HashMap, sync::Arc, time::SystemTime};

use splitter_client::{
    ConsumerClient, ConsumerOptions, GrantId, Instance, InstanceId, Location, Ownership,
    QualifiedServiceName, Shard,
};
use tonic::transport::Channel;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,splitter_client=debug".into()),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let endpoint = args
        .next()
        .unwrap_or_else(|| "http://localhost:50051".to_string());
    let svc_arg = args.next().ok_or("second arg must be tenant/service")?;
    let service = QualifiedServiceName::parse(&svc_arg)
        .ok_or("tenant/service format required, e.g. acme/widgets")?;
    let region = args.next().unwrap_or_else(|| "local".to_string());
    let node = args.next().unwrap_or_else(|| "local".to_string());

    tracing::info!(%endpoint, %service, %region, %node, "connecting");

    let channel = Channel::from_shared(endpoint.clone())?.connect().await?;
    let client = ConsumerClient::new(channel);

    let consumer = Instance {
        id: InstanceId::new(),
        location: Location::new(region, node),
        name: "splitter-rs/join-example".into(),
        created: SystemTime::now(),
        endpoint: splitter_client::Endpoint::default(),
    };

    let handle = client
        .join(consumer, service, ConsumerOptions::default())
        .await?;

    let mut cluster = handle.cluster.clone();
    let mut grants = handle.assignments.clone();
    let mut known: HashMap<GrantId, Arc<Ownership>> = HashMap::new();
    let mut tasks = tokio::task::JoinSet::new();
    let closed = handle.closed();

    loop {
        let snapshot = grants.borrow_and_update().clone();
        known.retain(|id, ownership| {
            snapshot
                .get(id)
                .is_some_and(|entry| Arc::ptr_eq(ownership, &entry.ownership))
        });
        for (id, assignment) in snapshot.iter() {
            if !known.contains_key(id) {
                known.insert(id.clone(), assignment.ownership.clone());
                tasks.spawn(run_lifecycle(
                    id.clone(),
                    assignment.shard.clone(),
                    assignment.ownership.clone(),
                ));
            }
        }
        tokio::select! {
            res = cluster.changed() => {
                if res.is_err() { break; }
                let snap = cluster.borrow_and_update();
                tracing::info!(
                    version = snap.id.version,
                    consumers = snap.consumers.len(),
                    grants = snap.grants.len(),
                    shards = snap.shards.len(),
                    "cluster update"
                );
            }
            result = grants.changed() => if result.is_err() { break; },
            _ = tasks.join_next(), if !tasks.is_empty() => {},
            () = closed.closed() => {
                tracing::info!("join closed");
                break;
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("SIGINT received, draining");
                let result = tokio::time::timeout(std::time::Duration::from_secs(10), handle.shutdown()).await;
                tasks.shutdown().await;
                result??;
                return Ok(());
            }
        }
    }

    tasks.shutdown().await;
    Ok(())
}

struct Release(Arc<Ownership>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

async fn run_lifecycle(id: GrantId, shard: Shard, o: Arc<Ownership>) {
    let _release = Release(o.clone());
    let loader = match o.wait_for_unload().await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(%id, error = %e, "wait_for_unload bailed");
            return;
        }
    };
    tracing::info!(%id, shard = %shard, "counterpart unloaded, signalling load");
    loader.load();

    if let Err(e) = o.wait_for_active().await {
        tracing::warn!(%id, error = %e, "wait_for_active bailed");
        return;
    }
    tracing::info!(%id, shard = %shard, "ACTIVE");

    let unloader = match o.wait_for_revoke().await {
        Ok(u) => u,
        Err(e) => {
            tracing::warn!(%id, error = %e, "wait_for_revoke bailed");
            return;
        }
    };
    tracing::info!(%id, shard = %shard, "revoked, draining");
    unloader.unload();

    if let Err(e) = o.wait_for_load().await {
        tracing::warn!(%id, error = %e, "wait_for_load bailed");
        return;
    }
    tracing::info!(%id, shard = %shard, "counterpart loaded, exiting");
}
