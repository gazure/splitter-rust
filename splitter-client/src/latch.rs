//! One-shot idempotent signal, analogue of Go's `iox.AsyncCloser` / `RAsyncCloser`.
//!
//! Multiple observers can await the same close. Backed by `tokio::sync::watch`
//! so readers can `.await` (via `Latch::closed`) and the fast path is a simple
//! atomic load.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::watch;

struct Inner {
    closed: AtomicBool,
    tx: watch::Sender<bool>,
}

#[derive(Clone)]
pub struct Latch {
    inner: Arc<Inner>,
}

#[derive(Clone)]
pub struct LatchReader {
    inner: Arc<Inner>,
    rx: watch::Receiver<bool>,
}

impl Latch {
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(false);
        Self {
            inner: Arc::new(Inner {
                closed: AtomicBool::new(false),
                tx,
            }),
        }
    }

    pub fn close(&self) {
        if !self.inner.closed.swap(true, Ordering::SeqCst) {
            // Ignore send error — absence of receivers is fine, the bool is authoritative.
            let _ = self.inner.tx.send(true);
        }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    pub fn reader(&self) -> LatchReader {
        LatchReader {
            inner: self.inner.clone(),
            rx: self.inner.tx.subscribe(),
        }
    }

    pub async fn closed(&self) {
        self.reader().closed().await
    }
}

impl Default for Latch {
    fn default() -> Self {
        Self::new()
    }
}

impl LatchReader {
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    pub async fn closed(&self) {
        if self.is_closed() {
            return;
        }
        let mut rx = self.rx.clone();
        while !*rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                // Sender dropped. Arc<Inner> keeps the sender alive for as long
                // as any reader exists, so this branch is unreachable through
                // the public API, but we bail out rather than spin.
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn close_before_await_resolves_immediately() {
        let l = Latch::new();
        l.close();
        timeout(Duration::from_millis(50), l.closed())
            .await
            .expect("should resolve immediately");
        assert!(l.is_closed());
    }

    #[tokio::test]
    async fn close_after_await_wakes_observers() {
        let l = Latch::new();
        let r1 = l.reader();
        let r2 = l.reader();
        let h1 = tokio::spawn(async move { r1.closed().await });
        let h2 = tokio::spawn(async move { r2.closed().await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        l.close();
        timeout(Duration::from_millis(50), h1)
            .await
            .expect("h1 completes")
            .unwrap();
        timeout(Duration::from_millis(50), h2)
            .await
            .expect("h2 completes")
            .unwrap();
    }

    #[tokio::test]
    async fn multiple_close_is_idempotent() {
        let l = Latch::new();
        l.close();
        l.close();
        l.close();
        assert!(l.is_closed());
        timeout(Duration::from_millis(50), l.closed())
            .await
            .expect("still resolves");
    }
}
