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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ids::{
            DomainKey, DomainType, GrantState, Instance, InstanceId, QualifiedServiceName, Shard,
        },
        proxy::{Proxy, handle, handle_with_retry},
    };
    use splitter_proto::pb::{self, cluster_message as cm};
    use std::time::SystemTime;

    fn peer(name: &str) -> Instance {
        Instance {
            id: InstanceId::new(),
            location: Location::new("region", name),
            name: name.into(),
            created: SystemTime::UNIX_EPOCH,
            endpoint: "peer:8080".into(),
        }
    }

    fn key() -> QualifiedDomainKey {
        QualifiedDomainKey {
            domain: QualifiedDomainName {
                service: QualifiedServiceName::new("test", "resolver"),
                name: "unit".into(),
            },
            key: DomainKey::default(),
        }
    }

    fn cluster(owners: &[(&Instance, pb::GrantState)]) -> Arc<ClusterMap> {
        let shard: pb::Shard = Shard {
            domain: key().domain,
            kind: DomainType::Unit,
            region: crate::ids::Region::default(),
            from: String::new(),
            to: String::new(),
        }
        .into();
        Arc::new(
            ClusterMap::empty()
                .apply(pb::ClusterMessage {
                    version: 1,
                    msg: Some(cm::Msg::Snapshot(cm::Snapshot {
                        shards: vec![shard.clone()],
                        assignments: owners
                            .iter()
                            .enumerate()
                            .map(|(i, (owner, state))| cm::Assignment {
                                consumer: Some((*owner).clone().into()),
                                grants: vec![cm::GrantInfo {
                                    id: i.to_string(),
                                    shard: Some(shard.clone()),
                                    state: *state as i32,
                                }],
                            })
                            .collect(),
                        ..Default::default()
                    })),
                    ..Default::default()
                })
                .unwrap(),
        )
    }

    fn resolver(pool: Arc<ConnectionPool>, cluster: Arc<ClusterMap>) -> DomainResolver<()> {
        let (_, receiver) = watch::channel(cluster);
        DomainResolver::new(pool, receiver, key().domain, |_| ())
    }

    #[tokio::test]
    async fn unknown_owner_is_not_owned_and_can_be_retried() {
        let r = resolver(ConnectionPool::new(InstanceId::new()), cluster(&[]));
        let error = r.resolve(&key()).unwrap_err();
        assert!(error.is_ownership());
        assert!(matches!(error, ClientError::NotOwned));
    }

    #[tokio::test]
    async fn default_handover_priority_matches_cluster_lookup_and_go() {
        let revoked = peer("revoked");
        let loaded = peer("loaded");
        let r = resolver(
            ConnectionPool::new(InstanceId::new()),
            cluster(&[
                (&revoked, pb::GrantState::Revoked),
                (&loaded, pb::GrantState::AllocatedLoaded),
            ]),
        );
        assert_eq!(r.location(&key()), Some(revoked.location));
    }

    #[tokio::test]
    async fn unloaded_owner_remains_resolvable() {
        let owner = peer("unloaded");
        let map = cluster(&[(&owner, pb::GrantState::RevokedUnloaded)]);
        let pool = ConnectionPool::new(InstanceId::new());
        pool.apply_cluster(&map);
        let r = resolver(pool, map);
        assert_eq!(r.location(&key()), Some(owner.location));
        r.resolve(&key()).unwrap();
    }

    #[tokio::test]
    async fn remote_peer_missing_from_pool_is_retryable_not_local_fallback() {
        let owner = peer("remote");
        let r = resolver(
            ConnectionPool::new(InstanceId::new()),
            cluster(&[(&owner, pb::GrantState::Active)]),
        );
        let error = r.resolve(&key()).unwrap_err();
        assert!(error.is_ownership());
        assert!(!matches!(error, ClientError::NoResolution));
    }

    #[tokio::test]
    async fn only_local_owner_requests_local_fallback_and_domain_mismatch_is_rejected() {
        let owner = peer("self");
        let r = resolver(
            ConnectionPool::new(owner.id),
            cluster(&[(&owner, pb::GrantState::Active)]),
        );
        assert!(matches!(r.resolve(&key()), Err(ClientError::NoResolution)));
        let mut wrong = key();
        wrong.domain.name = "other".into();
        assert!(matches!(
            r.resolve(&wrong),
            Err(ClientError::InvalidMessage(_))
        ));
        assert_eq!(r.location(&wrong), None);
    }

    struct LocalFallbackProxy(DomainResolver<()>);

    impl Proxy for LocalFallbackProxy {
        type Key = QualifiedDomainKey;
        type Client = ();
        type Value = ();
        fn resolve(&self, key: &Self::Key) -> Result<(), ClientError> {
            self.0.resolve(key)
        }
        fn lookup(&self, _: &Self::Key, states: &[GrantState]) -> Option<()> {
            // A non-active range remains locally available during handover.
            states.is_empty().then_some(())
        }
        fn domain_key(&self, key: &Self::Key) -> QualifiedDomainKey {
            key.clone()
        }
        fn location(&self, key: &Self::Key) -> Option<Location> {
            self.0.location(key)
        }
        fn cluster(&self) -> Arc<ClusterMap> {
            self.0.cluster.borrow().clone()
        }
    }

    #[tokio::test]
    async fn missing_owner_does_not_invoke_non_active_local_handler() {
        let proxy = LocalFallbackProxy(resolver(
            ConnectionPool::new(InstanceId::new()),
            cluster(&[]),
        ));
        let result = handle(
            &proxy,
            &key(),
            (),
            |(), ()| async { panic!("no remote owner") },
            |(), ()| async { Ok::<(), ClientError>(()) },
        )
        .await;
        assert!(matches!(result, Err(ClientError::NotOwned)));
    }

    #[tokio::test]
    async fn known_local_owner_still_invokes_local_fallback() {
        let owner = peer("self");
        let proxy = LocalFallbackProxy(resolver(
            ConnectionPool::new(owner.id),
            cluster(&[(&owner, pb::GrantState::Revoked)]),
        ));
        let result = handle(
            &proxy,
            &key(),
            (),
            |(), ()| async { panic!("owner is local") },
            |(), ()| async { Ok::<_, ClientError>(42) },
        )
        .await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test(start_paused = true)]
    async fn unavailable_peer_retries_until_pool_catches_up() {
        let owner = peer("remote");
        let map = cluster(&[(&owner, pb::GrantState::Active)]);
        let pool = ConnectionPool::new(InstanceId::new());
        let proxy = LocalFallbackProxy(resolver(pool.clone(), map.clone()));
        let task = tokio::spawn(async move {
            handle_with_retry(
                &proxy,
                &key(),
                std::time::Duration::from_secs(5),
                (),
                |(), ()| async { Ok(42) },
                |(), ()| async { panic!("missing remote peer must not invoke local fallback") },
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        pool.apply_cluster(&map);
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        assert_eq!(task.await.unwrap().unwrap(), 42);
    }
}
