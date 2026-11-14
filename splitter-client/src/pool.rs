//! Peer connection pool.
//!
//! Port of `pkg/model/pool.go`, simplified to lean on tonic's already-lazy
//! `Channel::connect_lazy()` for dial/reconnect. The pool's job is to:
//!
//! 1. Keep a `HashMap<InstanceId, Channel>` in sync with the set of consumer
//!    instances visible in the latest `ClusterMap`.
//! 2. Reject self-lookups via `ClientError::NoResolution` (pool.go:59).
//! 3. Expose cheap `resolve(id)` for the forwarding path.
//!
//! We skip Go's 19s dial-delay / 2min idle-eviction machinery — tonic's
//! `Channel` already handles lazy connect + reconnect; when a consumer leaves
//! the cluster we drop the channel, and tonic tears down the underlying
//! connection on the last clone's drop.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::Duration,
};

use tokio::time::Instant;
use tonic::transport::Channel;
use tracing::warn;

use crate::{
    cluster::ClusterMap,
    error::ClientError,
    ids::{Endpoint, InstanceId},
};

struct PoolEntry {
    endpoint: Endpoint,
    channel: Option<Channel>,
    retry_at: Instant,
}

const DIAL_RETRY_DELAY: Duration = Duration::from_secs(1);

impl PoolEntry {
    fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            channel: None,
            retry_at: Instant::now(),
        }
    }

    fn try_dial(&mut self, dial: &DialFn) {
        if self.channel.is_some() || Instant::now() < self.retry_at {
            return;
        }
        match dial(&self.endpoint) {
            Ok(channel) => self.channel = Some(channel),
            Err(error) => {
                self.retry_at = Instant::now() + DIAL_RETRY_DELAY;
                warn!(peer = %self.endpoint, %error, "failed to construct peer channel; retry pending");
            }
        }
    }
}

/// Dial configuration: accepts an [`Endpoint`] (host:port or full URL) and
/// returns a tonic [`Channel`]. Defaults to insecure HTTP/2. Users can
/// customize via [`ConnectionPool::with_dial`].
type DialFn = dyn Fn(&Endpoint) -> Result<Channel, ClientError> + Send + Sync + 'static;

pub struct ConnectionPool {
    self_id: InstanceId,
    entries: RwLock<HashMap<InstanceId, PoolEntry>>,
    dial: Arc<DialFn>,
}

impl ConnectionPool {
    pub fn new(self_id: InstanceId) -> Arc<Self> {
        Arc::new(Self {
            self_id,
            entries: RwLock::new(HashMap::new()),
            dial: Arc::new(default_dial),
        })
    }

    /// Build a pool with a user-supplied dial function. Useful for TLS /
    /// custom interceptors; the default dials insecure HTTP/2.
    /// The callback must be nonblocking and must not access this pool: it runs
    /// under the pool's write lock to serialize channel construction.
    pub fn with_dial<F>(self_id: InstanceId, dial: F) -> Arc<Self>
    where
        F: Fn(&Endpoint) -> Result<Channel, ClientError> + Send + Sync + 'static,
    {
        Arc::new(Self {
            self_id,
            entries: RwLock::new(HashMap::new()),
            dial: Arc::new(dial),
        })
    }

    pub fn self_id(&self) -> &InstanceId {
        &self.self_id
    }

    /// Resolves a peer by instance ID.
    /// Returns `NoResolution` for this instance and retryable `Unavailable` for missing peers.
    /// Retries failed channel construction after the one-second backoff, even
    /// without another cluster update.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer poisoned the connection pool lock.
    pub fn resolve(&self, id: &InstanceId) -> Result<Channel, ClientError> {
        if id == &self.self_id {
            return Err(ClientError::NoResolution);
        }
        {
            let entries = self.entries.read().expect("pool poisoned");
            let entry = entries.get(id).ok_or_else(peer_unavailable)?;
            if let Some(channel) = &entry.channel {
                return Ok(channel.clone());
            }
            if Instant::now() < entry.retry_at {
                return Err(peer_unavailable());
            }
        }
        // Recheck under the write lock: another caller can dial or replace the
        // endpoint between the read and write, and departed peers stay removed.
        let mut entries = self.entries.write().expect("pool poisoned");
        let entry = entries.get_mut(id).ok_or_else(peer_unavailable)?;
        entry.try_dial(self.dial.as_ref());
        entry.channel.clone().ok_or_else(peer_unavailable)
    }

    /// Applies membership changes and attempts channel construction for new peers.
    /// Endpoint changes discard the previous channel before dialing the new endpoint.
    /// Failed construction is retried by later updates or lookups, at most once
    /// per second for the same endpoint.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer poisoned the connection pool lock.
    pub fn apply_cluster(&self, cluster: &ClusterMap) {
        let mut seen = HashSet::new();
        let mut entries = self.entries.write().expect("pool poisoned");
        for consumer in cluster.consumers().values() {
            let id = consumer.consumer.id;
            let endpoint = &consumer.consumer.endpoint;
            if id == self.self_id || endpoint.is_empty() {
                continue;
            }
            seen.insert(id);
            let entry = entries
                .entry(id)
                .or_insert_with(|| PoolEntry::new(endpoint.clone()));
            if entry.endpoint != *endpoint {
                *entry = PoolEntry::new(endpoint.clone());
            }
            entry.try_dial(self.dial.as_ref());
        }
        entries.retain(|id, _| seen.contains(id));
    }

    /// Returns the number of peers with constructed channels, excluding pending retries.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer poisoned the connection pool lock.
    pub fn len(&self) -> usize {
        self.entries
            .read()
            .expect("pool poisoned")
            .values()
            .filter(|entry| entry.channel.is_some())
            .count()
    }

    /// Returns whether the pool has no constructed peer channels.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn peer_unavailable() -> ClientError {
    ClientError::Transport(tonic::Status::unavailable(
        "peer is not available in the connection pool",
    ))
}

fn default_dial(endpoint: &Endpoint) -> Result<Channel, ClientError> {
    tonic::transport::Endpoint::from_shared(normalize_endpoint(endpoint.as_str()))
        .map(|ep| ep.connect_lazy())
        .map_err(ClientError::TransportSetup)
}

fn normalize_endpoint(s: &str) -> String {
    if s.starts_with("http://") || s.starts_with("https://") {
        s.to_string()
    } else {
        format!("http://{s}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    mod recovery;
    use crate::ids::Location;
    use splitter_proto::pb::{ClusterMessage, cluster_message};
    use std::time::SystemTime;

    /// Deterministic test IDs from a u128 so we can refer to them by short
    /// labels without generating random UUIDs.
    const ME: InstanceId = InstanceId(uuid::Uuid::from_u128(0x0000_0000_0000_0001));
    const OTHER: InstanceId = InstanceId(uuid::Uuid::from_u128(0x0000_0000_0000_0002));
    const PEER_A: InstanceId = InstanceId(uuid::Uuid::from_u128(0x0000_0000_0000_000A));
    const PEER_B: InstanceId = InstanceId(uuid::Uuid::from_u128(0x0000_0000_0000_000B));

    fn mk_instance(id: InstanceId, endpoint: &str) -> crate::ids::Instance {
        crate::ids::Instance {
            id,
            location: Location::default(),
            name: "test".into(),
            created: SystemTime::UNIX_EPOCH,
            endpoint: endpoint.into(),
        }
    }

    fn mk_cluster(consumers: Vec<crate::ids::Instance>) -> ClusterMap {
        ClusterMap::empty()
            .apply(ClusterMessage {
                version: 1,
                msg: Some(cluster_message::Msg::Snapshot(cluster_message::Snapshot {
                    assignments: consumers
                        .into_iter()
                        .map(|consumer| cluster_message::Assignment {
                            consumer: Some(consumer.into()),
                            grants: Vec::new(),
                        })
                        .collect(),
                    ..Default::default()
                })),
                ..Default::default()
            })
            .unwrap()
    }

    #[test]
    fn self_resolve_returns_no_resolution() {
        let pool = ConnectionPool::new(ME);
        assert!(matches!(pool.resolve(&ME), Err(ClientError::NoResolution)));
    }

    #[test]
    fn unknown_id_returns_retryable_unavailable() {
        let pool = ConnectionPool::new(ME);
        assert!(matches!(
            pool.resolve(&OTHER),
            Err(ClientError::Transport(status)) if status.code() == tonic::Code::Unavailable
        ));
    }

    #[tokio::test]
    async fn apply_cluster_adds_known_peers() {
        let pool = ConnectionPool::new(ME);
        let cluster = mk_cluster(vec![
            mk_instance(ME, "1.1.1.1:50052"),
            mk_instance(PEER_A, "2.2.2.2:50052"),
            mk_instance(PEER_B, "3.3.3.3:50052"),
        ]);
        pool.apply_cluster(&cluster);
        assert_eq!(pool.len(), 2);
        assert!(pool.resolve(&PEER_A).is_ok());
        assert!(pool.resolve(&PEER_B).is_ok());
        // self still rejects
        assert!(matches!(pool.resolve(&ME), Err(ClientError::NoResolution)));
    }

    #[tokio::test]
    async fn apply_cluster_drops_departed_peers() {
        let pool = ConnectionPool::new(ME);
        pool.apply_cluster(&mk_cluster(vec![
            mk_instance(PEER_A, "2.2.2.2:50052"),
            mk_instance(PEER_B, "3.3.3.3:50052"),
        ]));
        assert_eq!(pool.len(), 2);

        pool.apply_cluster(&mk_cluster(vec![mk_instance(PEER_A, "2.2.2.2:50052")]));
        assert_eq!(pool.len(), 1);
        assert!(pool.resolve(&PEER_A).is_ok());
        assert!(matches!(
            pool.resolve(&PEER_B),
            Err(ClientError::Transport(status)) if status.code() == tonic::Code::Unavailable
        ));
    }

    #[tokio::test]
    async fn apply_cluster_skips_empty_endpoints() {
        let pool = ConnectionPool::new(ME);
        pool.apply_cluster(&mk_cluster(vec![mk_instance(PEER_A, "")]));
        assert_eq!(pool.len(), 0);
    }
}
