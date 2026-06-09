//! Dispatcher: orchestrates the `ConsumerClient` stream, a `ConnectionPool`,
//! and a chain of `DispatchFilter`s.
//!
//! Port of `pkg/model/dispatcher.go`. Filters run sequentially per assigned
//! grant; the first filter that returns `true` from `try_handle` owns the
//! grant for its lifetime. If no filter accepts a grant, the dispatcher asks
//! the coordinator to revoke it via `Ownership::request_revoke`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;
use tokio::task::JoinHandle as TaskHandle;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use crate::cluster::ClusterMap;
use crate::consumer::{ConsumerClient, ConsumerOptions, GrantEvent, JoinHandle};
use crate::error::Result;
use crate::ids::{GrantId, Instance, QualifiedServiceName, Shard};
use crate::latch::{Latch, LatchReader};
use crate::ownership::Ownership;
use crate::pool::ConnectionPool;

/// Context passed to a filter's `init`.
#[derive(Clone)]
pub struct FilterContext {
    pub service: QualifiedServiceName,
    pub pool: Arc<ConnectionPool>,
    pub cluster: watch::Receiver<Arc<ClusterMap>>,
}

#[async_trait]
pub trait DispatchFilter: Send + Sync + 'static {
    /// Called once after the dispatcher has joined and the pool is wired up.
    /// May be called from any task; implementations should not block.
    fn init(&self, ctx: FilterContext);

    /// Attempt to claim a grant. Return `true` to own the grant for its full
    /// lifecycle (blocking until the grant is revoked/expired). Return
    /// `false` to pass it to the next filter.
    async fn try_handle(&self, id: GrantId, shard: Shard, ownership: Arc<Ownership>) -> bool;
}

pub struct DispatcherBuilder {
    service: QualifiedServiceName,
    instance: Instance,
    chain: Vec<Arc<dyn DispatchFilter>>,
    options: ConsumerOptions,
    pool: Option<Arc<ConnectionPool>>,
}

impl DispatcherBuilder {
    pub fn new(service: QualifiedServiceName, instance: Instance) -> Self {
        Self {
            service,
            instance,
            chain: Vec::new(),
            options: ConsumerOptions::default(),
            pool: None,
        }
    }

    pub fn filter(mut self, f: Arc<dyn DispatchFilter>) -> Self {
        self.chain.push(f);
        self
    }

    pub fn options(mut self, opts: ConsumerOptions) -> Self {
        self.options = opts;
        self
    }

    /// Inject a pre-built pool (e.g. one configured with TLS). Defaults to an
    /// insecure HTTP pool keyed on this consumer's instance id.
    pub fn pool(mut self, pool: Arc<ConnectionPool>) -> Self {
        self.pool = Some(pool);
        self
    }

    pub async fn build(self, channel: Channel) -> Result<Dispatcher> {
        let client = ConsumerClient::new(channel);
        let join = client
            .join(self.instance.clone(), self.service.clone(), self.options)
            .await?;

        let pool = self
            .pool
            .unwrap_or_else(|| ConnectionPool::new(self.instance.id));

        // Seed pool with current cluster, then keep it in sync.
        pool.apply_cluster(&join.cluster.borrow().clone());

        let chain = Arc::new(self.chain);
        let ctx = FilterContext {
            service: self.service.clone(),
            pool: pool.clone(),
            cluster: join.cluster.clone(),
        };
        for f in chain.iter() {
            f.init(ctx.clone());
        }

        let closed = Latch::new();
        let grants_task = spawn_grants_router(join.subscribe_grants(), chain.clone());
        let pool_task = spawn_pool_updater(pool.clone(), join.cluster.clone());
        let closed_watcher = spawn_closed_watcher(join.closed(), closed.clone());

        Ok(Dispatcher {
            id: self.instance,
            service: self.service,
            pool,
            cluster: join.cluster.clone(),
            chain,
            closed,
            join: Some(join),
            tasks: vec![grants_task, pool_task, closed_watcher],
        })
    }
}

pub struct Dispatcher {
    id: Instance,
    service: QualifiedServiceName,
    pool: Arc<ConnectionPool>,
    cluster: watch::Receiver<Arc<ClusterMap>>,
    chain: Arc<Vec<Arc<dyn DispatchFilter>>>,
    closed: Latch,
    join: Option<JoinHandle>,
    tasks: Vec<TaskHandle<()>>,
}

impl Dispatcher {
    pub fn id(&self) -> &Instance {
        &self.id
    }
    pub fn service(&self) -> &QualifiedServiceName {
        &self.service
    }
    pub fn pool(&self) -> &Arc<ConnectionPool> {
        &self.pool
    }
    pub fn cluster(&self) -> Arc<ClusterMap> {
        self.cluster.borrow().clone()
    }
    pub fn closed(&self) -> LatchReader {
        self.closed.reader()
    }
    pub fn chain(&self) -> &Arc<Vec<Arc<dyn DispatchFilter>>> {
        &self.chain
    }

    /// Initiate graceful shutdown. Sends `Deregister`, then waits up to
    /// `timeout` for the stream to close. Aborts background tasks on return.
    pub async fn drain(mut self, timeout: Duration) -> Result<()> {
        info!(id = %self.id, "dispatcher draining");
        let res = if let Some(join) = self.join.take() {
            match tokio::time::timeout(timeout, join.shutdown()).await {
                Ok(r) => r,
                Err(_) => {
                    warn!(?timeout, "dispatcher drain timed out");
                    Ok(())
                }
            }
        } else {
            Ok(())
        };
        for t in self.tasks.drain(..) {
            t.abort();
        }
        self.closed.close();
        res
    }
}

impl Drop for Dispatcher {
    fn drop(&mut self) {
        for t in self.tasks.drain(..) {
            t.abort();
        }
        self.closed.close();
    }
}

fn spawn_grants_router(
    mut grants: tokio::sync::broadcast::Receiver<GrantEvent>,
    chain: Arc<Vec<Arc<dyn DispatchFilter>>>,
) -> TaskHandle<()> {
    tokio::spawn(async move {
        loop {
            match grants.recv().await {
                Ok(GrantEvent::Assigned {
                    id,
                    shard,
                    ownership,
                }) => {
                    let chain = chain.clone();
                    tokio::spawn(run_chain(chain, id, shard, ownership));
                }
                Ok(GrantEvent::Removed { id }) => debug!(%id, "grant removed"),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!(dropped = n, "grants broadcast lagged");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    })
}

async fn run_chain(
    chain: Arc<Vec<Arc<dyn DispatchFilter>>>,
    id: GrantId,
    shard: Shard,
    ownership: Arc<Ownership>,
) {
    if ownership.expired().is_closed() {
        debug!(%id, "grant already expired before dispatch");
        return;
    }
    for filter in chain.iter() {
        if filter
            .try_handle(id.clone(), shard.clone(), ownership.clone())
            .await
        {
            return;
        }
    }
    warn!(%id, shard = %shard, "no filter accepted grant, relinquishing");
    ownership.request_revoke();
}

fn spawn_pool_updater(
    pool: Arc<ConnectionPool>,
    mut cluster: watch::Receiver<Arc<ClusterMap>>,
) -> TaskHandle<()> {
    tokio::spawn(async move {
        while cluster.changed().await.is_ok() {
            let snap = cluster.borrow_and_update().clone();
            pool.apply_cluster(&snap);
        }
    })
}

fn spawn_closed_watcher(inner: LatchReader, closed: Latch) -> TaskHandle<()> {
    tokio::spawn(async move {
        inner.closed().await;
        closed.close();
    })
}
