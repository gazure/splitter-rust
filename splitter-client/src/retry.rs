//! Ownership-retry backoff.
//!
//! Port of `pkg/model/proxy.go:51-68` (`RetryOwnership1`). Exponential
//! backoff starting at 1s, capped at 5s, bounded by a total timeout. Only
//! retries on errors classified as ownership failures (see
//! [`ClientError::is_ownership`]); all others short-circuit.

use std::future::Future;
use std::time::Duration;

use crate::error::Result;

/// Defaults match `proxy.go:55` (`InitialInterval=1s`, `MaxInterval=5s`).
#[derive(Clone, Debug)]
pub struct OwnershipBackoff {
    pub initial: Duration,
    pub max: Duration,
    pub timeout: Duration,
}

impl OwnershipBackoff {
    pub fn new(timeout: Duration) -> Self {
        Self {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(5),
            timeout,
        }
    }
}

/// Repeatedly run `op` while it returns an ownership-class error, until it
/// succeeds, returns a non-ownership error, or the backoff's total timeout
/// elapses.
pub async fn retry_ownership<T, Op, Fut>(backoff: &OwnershipBackoff, mut op: Op) -> Result<T>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let deadline = tokio::time::Instant::now() + backoff.timeout;
    let mut delay = backoff.initial;
    loop {
        let err = match op().await {
            Ok(v) => return Ok(v),
            Err(e) if e.is_ownership() => e,
            Err(e) => return Err(e),
        };
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(err);
        }
        let remaining = deadline - now;
        let sleep_for = delay.min(remaining);
        tokio::time::sleep(sleep_for).await;
        delay = (delay * 2).min(backoff.max);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ClientError;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use tonic::{Code, Status};

    fn fast_backoff(timeout: Duration) -> OwnershipBackoff {
        OwnershipBackoff {
            initial: Duration::from_millis(1),
            max: Duration::from_millis(4),
            timeout,
        }
    }

    #[tokio::test]
    async fn succeeds_immediately_without_retry() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = attempts.clone();
        let r: Result<i32> = retry_ownership(&fast_backoff(Duration::from_secs(1)), || {
            let a = a.clone();
            async move {
                a.fetch_add(1, Ordering::SeqCst);
                Ok(42)
            }
        })
        .await;
        assert_eq!(r.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_ownership_errors_until_success() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = attempts.clone();
        let r: Result<&'static str> =
            retry_ownership(&fast_backoff(Duration::from_millis(500)), || {
                let a = a.clone();
                async move {
                    let n = a.fetch_add(1, Ordering::SeqCst);
                    if n < 3 {
                        Err(ClientError::NotOwned)
                    } else {
                        Ok("ok")
                    }
                }
            })
            .await;
        assert_eq!(r.unwrap(), "ok");
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn non_ownership_errors_are_not_retried() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = attempts.clone();
        let r: Result<()> = retry_ownership(&fast_backoff(Duration::from_secs(1)), || {
            let a = a.clone();
            async move {
                a.fetch_add(1, Ordering::SeqCst);
                Err(ClientError::InvalidMessage("nope".into()))
            }
        })
        .await;
        assert!(matches!(r, Err(ClientError::InvalidMessage(_))));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unavailable_transport_is_ownership_and_retried() {
        let attempts = Arc::new(AtomicU32::new(0));
        let a = attempts.clone();
        let _: Result<()> = retry_ownership(&fast_backoff(Duration::from_millis(50)), || {
            let a = a.clone();
            async move {
                a.fetch_add(1, Ordering::SeqCst);
                Err(ClientError::Transport(Status::new(Code::Unavailable, "peer down")))
            }
        })
        .await;
        assert!(attempts.load(Ordering::SeqCst) > 1);
    }

    #[tokio::test]
    async fn timeout_surfaces_last_ownership_error() {
        let r: Result<()> = retry_ownership(&fast_backoff(Duration::from_millis(20)), || async {
            Err(ClientError::Draining)
        })
        .await;
        assert!(matches!(r, Err(ClientError::Draining)));
    }
}
