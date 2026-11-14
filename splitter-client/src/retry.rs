//! Ownership-retry backoff.
//!
//! Port of `pkg/model/proxy.go:51-68` (`RetryOwnership1`). Exponential
//! backoff starting at 1s, capped at 5s, bounded by a total timeout. Only
//! retries on errors classified as ownership failures (see
//! [`crate::ClientError::is_ownership`]); all others short-circuit.

use std::{future::Future, time::Duration};

use crate::error::{ClientError, Result};

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
///
/// The budget includes attempts and backoff. Expiration during an attempt, or a
/// zero budget, returns [`ClientError::RetryTimeout`]. Expiration during backoff
/// returns the last ownership error without starting another attempt.
///
/// Timeouts drop the in-flight future. Operations must yield for cancellation;
/// dropping a future does not undo effects that have already reached a peer.
pub async fn retry_ownership<T, Op, Fut>(backoff: &OwnershipBackoff, mut op: Op) -> Result<T>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let deadline = tokio::time::Instant::now() + backoff.timeout;
    let mut delay = backoff.initial;
    let mut last_error = None;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(last_error.unwrap_or(ClientError::RetryTimeout));
        }
        let result = tokio::select! {
            biased;
            () = tokio::time::sleep_until(deadline) => return Err(ClientError::RetryTimeout),
            result = async { op().await } => result,
        };
        let err = match result {
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
        last_error = Some(err);
        tokio::time::sleep(sleep_for).await;
        delay = delay.saturating_mul(2).min(backoff.max);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ClientError;
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };
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
                Err(ClientError::Transport(Status::new(
                    Code::Unavailable,
                    "peer down",
                )))
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

    #[tokio::test(start_paused = true)]
    async fn pending_request_cannot_outlive_total_timeout() {
        let backoff = OwnershipBackoff::new(Duration::from_secs(1));
        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            retry_ownership::<(), _, _>(&backoff, std::future::pending),
        )
        .await
        .expect("retry deadline must terminate a pending request");
        assert!(matches!(result, Err(ClientError::RetryTimeout)));
        assert_eq!(started.elapsed(), backoff.timeout);
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_cannot_start_an_attempt_at_the_deadline() {
        let backoff = OwnershipBackoff {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(1),
            timeout: Duration::from_secs(1),
        };
        let mut attempts = 0;
        let result = retry_ownership(&backoff, || {
            attempts += 1;
            let attempt = attempts;
            async move {
                if attempt == 1 {
                    Err(ClientError::NotOwned)
                } else {
                    Ok(42)
                }
            }
        })
        .await;
        assert!(matches!(result, Err(ClientError::NotOwned)));
        assert_eq!(attempts, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn zero_budget_does_not_invoke_the_operation() {
        let mut called = false;
        let result = retry_ownership::<(), _, _>(&OwnershipBackoff::new(Duration::ZERO), || {
            called = true;
            std::future::ready(Ok(()))
        })
        .await;
        assert!(!called);
        assert!(matches!(result, Err(ClientError::RetryTimeout)));
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_drops_pending_attempt_after_an_ownership_retry() {
        struct Dropped(Arc<AtomicU32>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let attempts = Arc::new(AtomicU32::new(0));
        let dropped = Arc::new(AtomicU32::new(0));
        let backoff = OwnershipBackoff::new(Duration::from_secs(2));
        let started = tokio::time::Instant::now();
        let result: Result<()> = retry_ownership(&backoff, || {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            let dropped = dropped.clone();
            async move {
                if attempt == 0 {
                    return Err(ClientError::Draining);
                }
                let _guard = Dropped(dropped);
                std::future::pending().await
            }
        })
        .await;
        assert!(matches!(result, Err(ClientError::RetryTimeout)));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(started.elapsed(), backoff.timeout);
    }

    #[test]
    fn retry_timeout_is_terminal_and_maps_to_deadline_exceeded() {
        assert!(!ClientError::RetryTimeout.is_ownership());
        assert_eq!(
            ClientError::RetryTimeout.into_grpc().code(),
            Code::DeadlineExceeded
        );
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_wins_over_an_attempt_ready_at_the_same_time() {
        let backoff = OwnershipBackoff::new(Duration::from_secs(1));
        let result = retry_ownership(&backoff, || async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(42)
        })
        .await;
        assert!(matches!(result, Err(ClientError::RetryTimeout)));
    }
}
