//! Proxy trait and `handle` / `handle_with_retry` helpers.
//!
//! Port of `pkg/model/proxy.go` (`Proxy`, `Resolver`, `GrantResolver`,
//! `Handle`, `HandleWithRetry`). The routing contract implemented by `handle`:
//!
//!   1. Fast path — if we locally own the key in `Active` state, invoke the
//!      caller-supplied `local` closure against that value.
//!   2. Otherwise call `Proxy::resolve`. A successful resolution yields a
//!      gRPC client for the remote owner; we invoke `remote` against it and
//!      translate any `tonic::Status` back into a [`ClientError`] via
//!      [`ClientError::from_grpc`].
//!   3. If `resolve` returns `NoResolution` (no remote owner known), fall
//!      back to a local lookup across *any* state. If still empty, return
//!      `NotOwned`.
//!   4. Other resolve errors propagate.
//!
//! `handle_with_retry` wraps `handle` with [`crate::retry::retry_ownership`]
//! so transient `NotOwned` / `Draining` / `Unavailable` failures are retried.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::cluster::ClusterMap;
use crate::error::{ClientError, Result};
use crate::ids::{GrantState, Location, QualifiedDomainKey};
use crate::retry::{retry_ownership, OwnershipBackoff};

/// Unified proxy over local + remote resolution for a domain.
///
/// Implementors decide how to convert their user-level key into a
/// `QualifiedDomainKey`, how to find the remote owner (usually via a
/// [`crate::resolver::DomainResolver`]), and how to look up local ownership
/// (usually via [`crate::grant_map::GrantMap`]).
pub trait Proxy: Send + Sync + 'static {
    type Key: Send + Sync;
    type Client: Send;
    type Value: Send + Sync + Clone;

    /// Return a gRPC client for the remote owner, or `Err(NoResolution)` if
    /// this key is not owned by a reachable peer.
    fn resolve(&self, key: &Self::Key) -> Result<Self::Client>;

    /// Look up a locally-owned value by key, preferring the given grant
    /// states in order. Pass `&[]` to consider any ownership state.
    fn lookup(&self, key: &Self::Key, states: &[GrantState]) -> Option<Self::Value>;

    fn domain_key(&self, key: &Self::Key) -> QualifiedDomainKey;

    fn location(&self, key: &Self::Key) -> Option<Location>;

    fn cluster(&self) -> Arc<ClusterMap>;
}

/// Single-attempt proxy routing. See module docs for the decision tree.
pub async fn handle<P, Req, Resp, Rem, Loc, RFut, LFut>(
    proxy: &P,
    key: &P::Key,
    req: Req,
    remote: Rem,
    local: Loc,
) -> Result<Resp>
where
    P: Proxy,
    Rem: FnOnce(P::Client, Req) -> RFut,
    Loc: FnOnce(P::Value, Req) -> LFut,
    RFut: Future<Output = core::result::Result<Resp, tonic::Status>>,
    LFut: Future<Output = Result<Resp>>,
{
    if let Some(v) = proxy.lookup(key, &[GrantState::Active]) {
        return local(v, req).await;
    }
    match proxy.resolve(key) {
        Ok(client) => remote(client, req).await.map_err(ClientError::from_grpc),
        Err(ClientError::NoResolution) => {
            if let Some(v) = proxy.lookup(key, &[]) {
                return local(v, req).await;
            }
            Err(ClientError::NotOwned)
        }
        Err(e) => Err(e),
    }
}

/// Retrying proxy routing. Uses [`OwnershipBackoff`] defaults (1s→5s) bounded
/// by `timeout`. Retries on ownership-class errors
/// (`NotOwned`/`Draining`/`Unavailable`); other errors short-circuit.
///
/// Because the request must be retried on each attempt, `Req: Clone` and the
/// closures are `Fn` (not `FnOnce`).
pub async fn handle_with_retry<P, Req, Resp, Rem, Loc, RFut, LFut>(
    proxy: &P,
    key: &P::Key,
    timeout: Duration,
    req: Req,
    remote: Rem,
    local: Loc,
) -> Result<Resp>
where
    P: Proxy,
    Req: Clone,
    Rem: Fn(P::Client, Req) -> RFut,
    Loc: Fn(P::Value, Req) -> LFut,
    RFut: Future<Output = core::result::Result<Resp, tonic::Status>>,
    LFut: Future<Output = Result<Resp>>,
{
    let backoff = OwnershipBackoff::new(timeout);
    // &remote / &local each satisfy FnOnce via the blanket `&F: Fn` impl,
    // so one `handle` call per attempt consumes nothing of the outer closures.
    retry_ownership(&backoff, || handle(proxy, key, req.clone(), &remote, &local)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{DomainKey, QualifiedDomainName, QualifiedServiceName};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex;
    use tonic::{Code, Status};

    type ResolveFn = Box<dyn FnMut() -> Result<&'static str> + Send>;
    type LookupFn = Box<dyn FnMut(&[GrantState]) -> Option<u32> + Send>;
    type RemoteFut = Pin<Box<dyn Future<Output = core::result::Result<String, Status>> + Send>>;
    type LocalFut = Pin<Box<dyn Future<Output = Result<String>> + Send>>;

    /// Test proxy whose behavior is driven by test-supplied closures.
    struct MockProxy {
        on_resolve: Mutex<ResolveFn>,
        on_lookup: Mutex<LookupFn>,
        cluster: Arc<ClusterMap>,
    }

    impl MockProxy {
        fn new<R, L>(on_resolve: R, on_lookup: L) -> Self
        where
            R: FnMut() -> Result<&'static str> + Send + 'static,
            L: FnMut(&[GrantState]) -> Option<u32> + Send + 'static,
        {
            Self {
                on_resolve: Mutex::new(Box::new(on_resolve)),
                on_lookup: Mutex::new(Box::new(on_lookup)),
                cluster: Arc::new(ClusterMap::empty()),
            }
        }
    }

    impl Proxy for MockProxy {
        type Key = u32;
        type Client = &'static str;
        type Value = u32;

        fn resolve(&self, _: &u32) -> Result<&'static str> {
            (self.on_resolve.lock().unwrap())()
        }
        fn lookup(&self, _: &u32, states: &[GrantState]) -> Option<u32> {
            (self.on_lookup.lock().unwrap())(states)
        }
        fn domain_key(&self, key: &u32) -> QualifiedDomainKey {
            QualifiedDomainKey {
                domain: QualifiedDomainName {
                    service: QualifiedServiceName::new("t", "s"),
                    name: "d".into(),
                },
                key: DomainKey {
                    region: Default::default(),
                    key: key.to_string(),
                },
            }
        }
        fn location(&self, _: &u32) -> Option<Location> {
            None
        }
        fn cluster(&self) -> Arc<ClusterMap> {
            self.cluster.clone()
        }
    }

    fn remote_ok() -> impl Fn(&'static str, u32) -> RemoteFut {
        |client, req| Box::pin(async move { Ok(format!("remote:{client}:{req}")) })
    }

    fn local_ok() -> impl Fn(u32, u32) -> LocalFut {
        |v, req| Box::pin(async move { Ok(format!("local:{v}:{req}")) })
    }

    #[tokio::test]
    async fn active_local_grant_takes_fast_path() {
        let p = MockProxy::new(
            || panic!("resolve should not be called"),
            |states| {
                assert_eq!(states, &[GrantState::Active]);
                Some(99)
            },
        );
        let r = handle(&p, &7, 42, remote_ok(), local_ok()).await.unwrap();
        assert_eq!(r, "local:99:42");
    }

    #[tokio::test]
    async fn no_local_active_resolves_remote() {
        let p = MockProxy::new(|| Ok("peer-a"), |_| None);
        let r = handle(&p, &7, 42, remote_ok(), local_ok()).await.unwrap();
        assert_eq!(r, "remote:peer-a:42");
    }

    #[tokio::test]
    async fn no_resolution_falls_back_to_any_state_local() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = attempts.clone();
        let p = MockProxy::new(
            || Err(ClientError::NoResolution),
            move |states| {
                let n = a.fetch_add(1, Ordering::SeqCst);
                // First call filters to Active → None; second uses any state.
                if n == 0 {
                    assert_eq!(states, &[GrantState::Active]);
                    None
                } else {
                    assert!(states.is_empty());
                    Some(5)
                }
            },
        );
        let r = handle(&p, &7, 42, remote_ok(), local_ok()).await.unwrap();
        assert_eq!(r, "local:5:42");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn no_resolution_no_local_is_not_owned() {
        let p = MockProxy::new(|| Err(ClientError::NoResolution), |_| None);
        let err = handle(&p, &7, 42, remote_ok(), local_ok()).await.unwrap_err();
        assert!(matches!(err, ClientError::NotOwned));
    }

    #[tokio::test]
    async fn remote_status_translates_to_client_error() {
        let p = MockProxy::new(|| Ok("peer-a"), |_| None);
        let remote = |_: &'static str, _: u32| {
            Box::pin(async move {
                Err::<String, _>(Status::new(Code::OutOfRange, "not owned"))
            })
        };
        let err = handle(&p, &7, 42, remote, local_ok()).await.unwrap_err();
        assert!(matches!(err, ClientError::NotOwned));
    }

    #[tokio::test]
    async fn resolve_returning_unavailable_propagates() {
        let p = MockProxy::new(
            || Err(ClientError::Transport(Status::new(Code::Unavailable, "down"))),
            |_| None,
        );
        let err = handle(&p, &7, 42, remote_ok(), local_ok()).await.unwrap_err();
        assert!(matches!(err, ClientError::Transport(_)));
    }

    #[tokio::test]
    async fn retry_recovers_after_transient_not_owned() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = attempts.clone();
        let p = MockProxy::new(
            move || {
                let n = a.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err(ClientError::NoResolution)
                } else {
                    Ok("peer-a")
                }
            },
            |_| None,
        );
        // NoResolution + no local value = NotOwned (ownership error → retried).
        tokio::time::pause();
        let task = tokio::spawn(async move {
            handle_with_retry(&p, &7, Duration::from_secs(30), 42u32, remote_ok(), local_ok()).await
        });
        tokio::time::advance(Duration::from_secs(10)).await;
        let r = task.await.unwrap().unwrap();
        assert_eq!(r, "remote:peer-a:42");
        assert!(attempts.load(Ordering::SeqCst) >= 3);
    }

    #[tokio::test]
    async fn retry_does_not_retry_non_ownership_errors() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = attempts.clone();
        let p = MockProxy::new(
            move || {
                a.fetch_add(1, Ordering::SeqCst);
                Err(ClientError::InvalidMessage("bad".into()))
            },
            |_| None,
        );
        let r: Result<String> =
            handle_with_retry(&p, &7, Duration::from_secs(1), 42u32, remote_ok(), local_ok()).await;
        assert!(matches!(r, Err(ClientError::InvalidMessage(_))));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
