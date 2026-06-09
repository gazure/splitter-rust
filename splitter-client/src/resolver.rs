//! Domain resolver: cluster lookup + pool dial.
//!
//! Port of `pkg/model/proxy.go`'s `Resolver`. Given a `QualifiedDomainKey`,
//! finds the owning consumer in the latest cluster snapshot and returns a
//! user-typed gRPC client over the peer `Channel`.
//!
//! Full `Proxy`/`handle`/`handle_with_retry` machinery lives in Phase 4.

use std::sync::Arc;

use tokio::sync::watch;
use tonic::transport::Channel;

use crate::cluster::ClusterMap;
use crate::error::ClientError;
use crate::ids::{GrantState, Location, QualifiedDomainKey, QualifiedDomainName};
use crate::pool::ConnectionPool;

/// Map a peer `Channel` into a user-defined gRPC client.
pub type RemoteFn<C> = Arc<dyn Fn(Channel) -> C + Send + Sync>;

pub struct DomainResolver<C> {
    pool: Arc<ConnectionPool>,
    cluster: watch::Receiver<Arc<ClusterMap>>,
    domain: QualifiedDomainName,
    remote_fn: RemoteFn<C>,
    /// Grant states to consider during lookup, in priority order.
    preferred_states: Vec<GrantState>,
}

impl<C> DomainResolver<C> {
    pub fn new<F>(
        pool: Arc<ConnectionPool>,
        cluster: watch::Receiver<Arc<ClusterMap>>,
        domain: QualifiedDomainName,
        remote_fn: F,
    ) -> Self
    where
        F: Fn(Channel) -> C + Send + Sync + 'static,
    {
        Self {
            pool,
            cluster,
            domain,
            remote_fn: Arc::new(remote_fn),
            preferred_states: vec![
                GrantState::Active,
                GrantState::AllocatedLoaded,
                GrantState::Revoked,
            ],
        }
    }

    pub fn domain(&self) -> &QualifiedDomainName {
        &self.domain
    }

    /// Look up the owning consumer's location without dialing.
    pub fn location(&self, key: &QualifiedDomainKey) -> Option<Location> {
        let cluster = self.cluster.borrow();
        cluster
            .lookup(key, &self.preferred_states)
            .and_then(|g| cluster.consumers.get(&g.consumer))
            .map(|c| c.consumer.location.clone())
    }

    /// Resolve to the owner's gRPC client. Returns `NoResolution` if the key
    /// is not owned, is owned by this consumer (caller should use the local
    /// path), or the peer hasn't advertised a server endpoint.
    pub fn resolve(&self, key: &QualifiedDomainKey) -> Result<C, ClientError> {
        if key.domain != self.domain {
            return Err(ClientError::InvalidMessage(format!(
                "resolver domain mismatch: {} vs {}",
                self.domain, key.domain
            )));
        }
        let cluster = self.cluster.borrow();
        let grant = cluster
            .lookup(key, &self.preferred_states)
            .ok_or(ClientError::NoResolution)?;
        let channel = self.pool.resolve(&grant.consumer)?;
        Ok((self.remote_fn)(channel))
    }
}
