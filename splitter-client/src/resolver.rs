//! Domain resolver: cluster lookup + pool dial.
//!
//! Port of `pkg/model/proxy.go`'s `Resolver`. Given a `QualifiedDomainKey`,
//! finds the owning consumer in the latest cluster snapshot and returns a
//! user-typed gRPC client over the peer `Channel`.
//!
//! Uses the cluster lookup order: active, revoked, loaded, then unloaded.

use std::sync::Arc;

use tokio::sync::watch;
use tonic::transport::Channel;

use crate::{
    cluster::ClusterMap,
    error::ClientError,
    ids::{Location, QualifiedDomainKey, QualifiedDomainName},
    pool::ConnectionPool,
};

#[cfg(test)]
mod tests;

/// Map a peer `Channel` into a user-defined gRPC client.
pub type RemoteFn<C> = Arc<dyn Fn(Channel) -> C + Send + Sync>;

pub struct DomainResolver<C> {
    pool: Arc<ConnectionPool>,
    cluster: watch::Receiver<Arc<ClusterMap>>,
    domain: QualifiedDomainName,
    remote_fn: RemoteFn<C>,
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
        }
    }

    pub fn domain(&self) -> &QualifiedDomainName {
        &self.domain
    }

    /// Look up the owning consumer's location without dialing.
    pub fn location(&self, key: &QualifiedDomainKey) -> Option<Location> {
        if key.domain != self.domain {
            return None;
        }
        let cluster = self.cluster.borrow();
        cluster
            .lookup(key, &[])
            .and_then(|g| cluster.consumers().get(&g.consumer))
            .map(|c| c.consumer.location.clone())
    }

    /// Resolves the owner's gRPC client for `key`.
    ///
    /// Returns `NoResolution` for a local owner, `NotOwned` for an unknown owner,
    /// and a retryable transport error when the remote peer is unavailable.
    /// Returns `InvalidMessage` if the key belongs to another domain.
    pub fn resolve(&self, key: &QualifiedDomainKey) -> Result<C, ClientError> {
        if key.domain != self.domain {
            return Err(ClientError::InvalidMessage(format!(
                "resolver domain mismatch: {} vs {}",
                self.domain, key.domain
            )));
        }
        let cluster = self.cluster.borrow();
        let grant = cluster.lookup(key, &[]).ok_or(ClientError::NotOwned)?;
        self.pool
            .resolve(&grant.consumer)
            .map(|channel| (self.remote_fn)(channel))
    }
}
