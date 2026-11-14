//! Dispatcher: orchestrates the `ConsumerClient` stream, a `ConnectionPool`,
//! and a chain of `DispatchFilter`s.
//!
//! Port of `pkg/model/dispatcher.go`. Filters run sequentially per assigned
//! grant; the first filter that returns `true` from `try_handle` owns the
//! grant for its lifetime. If no filter accepts a grant, the dispatcher asks
//! the coordinator to revoke it via `Ownership::request_revoke`.

use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio::{
    sync::watch,
    task::{AbortHandle, JoinHandle as TaskHandle, JoinSet},
};
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use crate::{
    cluster::ClusterMap,
    consumer::{AssignmentSnapshot, ConsumerClient, ConsumerOptions, JoinHandle},
    error::{ClientError, Result},
    ids::{GrantId, Instance, QualifiedServiceName, Shard},
    latch::{Latch, LatchReader},
    ownership::Ownership,
    pool::ConnectionPool,
};

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

#[must_use]
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
        let force = Latch::new();
        let grants_task =
            spawn_grants_router(join.assignments.clone(), chain.clone(), force.reader());
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
            force,
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
    force: Latch,
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

    /// Deregisters, waits for range handover, and joins all dispatcher tasks.
    ///
    /// If `timeout` elapses, aborts the remaining tasks and returns
    /// `ClientError::DrainTimeout`. Dropping this future also cancels shutdown.
    pub async fn drain(mut self, timeout: Duration) -> Result<()> {
        info!(id = %self.id, "dispatcher draining");
        let result = tokio::time::timeout(timeout, async {
            let result = match self.join.take() {
                Some(join) => join.shutdown().await,
                None => Ok(()),
            };
            while let Some(task) = self.tasks.first_mut() {
                let _ = task.await;
                self.tasks.remove(0);
            }
            result
        })
        .await;
        if result.is_err() {
            self.force.close();
            // The router shuts down and joins its child tasks. The cancelled
            // JoinHandle aborts the driver, waking the pool and closed watcher.
            for task in self.tasks.drain(..) {
                let _ = task.await;
            }
        }
        self.closed.close();
        result.unwrap_or(Err(ClientError::DrainTimeout))
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
    mut assignments: watch::Receiver<AssignmentSnapshot>,
    chain: Arc<Vec<Arc<dyn DispatchFilter>>>,
    force: LatchReader,
) -> TaskHandle<()> {
    tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        let mut running: HashMap<GrantId, (Arc<Ownership>, AbortHandle)> = HashMap::new();
        loop {
            let snapshot = assignments.borrow_and_update().clone();
            running.retain(|id, (ownership, task)| {
                let current = snapshot
                    .get(id)
                    .is_some_and(|entry| Arc::ptr_eq(ownership, &entry.ownership));
                if !current {
                    task.abort();
                }
                current
            });
            for (id, assignment) in snapshot.iter() {
                if !running.contains_key(id) && !assignment.ownership.expired().is_closed() {
                    let abort = tasks.spawn(run_chain(
                        chain.clone(),
                        id.clone(),
                        assignment.shard.clone(),
                        assignment.ownership.clone(),
                    ));
                    // Retain finished identities until removal so snapshot
                    // updates cannot restart a completed handler.
                    running.insert(id.clone(), (assignment.ownership.clone(), abort));
                }
            }
            tokio::select! {
                biased;
                () = force.closed() => break,
                changed = assignments.changed() => if changed.is_err() { break; },
                result = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(Err(error)) = result { debug!(%error, "grant task exited"); }
                }
            }
        }
        tasks.shutdown().await;
    })
}

struct ReleaseOnExit(Arc<Ownership>);
impl Drop for ReleaseOnExit {
    fn drop(&mut self) {
        self.0.release();
    }
}

async fn run_chain(
    chain: Arc<Vec<Arc<dyn DispatchFilter>>>,
    id: GrantId,
    shard: Shard,
    ownership: Arc<Ownership>,
) {
    let _release = ReleaseOnExit(ownership.clone());
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

#[cfg(test)]
mod tests;
