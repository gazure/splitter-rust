//! Grant-ownership state machine signals.
//!
//! Port of `pkg/model/client.go` (`Ownership`, `Loader`, `Unloader`,
//! `WaitFor*`) and `pkg/model/grant.go`. A user-facing `Ownership` bundles
//! three life-cycle latches (active / revoked / expired) plus `Loader` and
//! `Unloader` handles used to coordinate graceful handover between the
//! previous and next owners of the same shard.
//!
//! ## Naming
//!
//! - `Loader` is held by a grant transitioning *into* ownership (ALLOCATED).
//!   - [`Loader::unloaded`] closes when the previous owner has unloaded.
//!   - [`Loader::load`] is called by the user once initialization finishes.
//! - `Unloader` is held by a grant transitioning *out of* ownership (REVOKED).
//!   - [`Unloader::loaded`] closes when the next owner has loaded.
//!   - [`Unloader::unload`] is called by the user once drain finishes.
//!
//! ## Driver responsibilities
//!
//! The consumer driver in [`crate::consumer`] owns the `Arc<Ownership>` and is
//! the only component that closes `active` / `revoked` / `expired` or the
//! counterpart-signalling latches (`loader.unloaded`, `unloader.loaded`).
//! The user only ever closes `loader.load`, `unloader.unload`, and the
//! `request_revoke` signal.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use tokio::sync::Notify;

use crate::error::ClientError;
use crate::ids::GrantState;
use crate::latch::{Latch, LatchReader};

/// Handover controls for the "taking over" (ALLOCATED) phase.
pub struct Loader {
    pub(crate) unloaded: Latch,
    pub(crate) load: Latch,
}

impl Loader {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            unloaded: Latch::new(),
            load: Latch::new(),
        })
    }

    /// Signal that this consumer has finished loading and is ready to take
    /// over. Idempotent; the consumer driver forwards a single Update to the
    /// coordinator the first time this closes.
    pub fn load(&self) {
        self.load.close();
    }

    /// Reader that closes when the previous owner has unloaded.
    pub fn unloaded(&self) -> LatchReader {
        self.unloaded.reader()
    }
}

/// Handover controls for the "giving up" (REVOKED) phase.
pub struct Unloader {
    pub(crate) loaded: Latch,
    pub(crate) unload: Latch,
}

impl Unloader {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            loaded: Latch::new(),
            unload: Latch::new(),
        })
    }

    /// Signal that this consumer has finished unloading. Idempotent.
    pub fn unload(&self) {
        self.unload.close();
    }

    /// Reader that closes when the next owner has loaded.
    pub fn loaded(&self) -> LatchReader {
        self.loaded.reader()
    }
}

/// Grant-lifecycle signal bundle (see module docs).
pub struct Ownership {
    pub(crate) active: Latch,
    pub(crate) revoked: Latch,
    pub(crate) expired: Latch,
    pub(crate) request_revoke_signal: Latch,
    pub(crate) loader: Arc<Loader>,
    pub(crate) unloader: Arc<Unloader>,
    pub(crate) expiration: Mutex<SystemTime>,
    /// Notified when `expiration` is updated so the per-grant expiration timer
    /// can re-read the deadline.
    pub(crate) expiration_changed: Notify,
}

impl fmt::Debug for Ownership {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ownership")
            .field("active", &self.active.is_closed())
            .field("revoked", &self.revoked.is_closed())
            .field("expired", &self.expired.is_closed())
            .field("expiration", &self.expiration())
            .finish()
    }
}

impl Ownership {
    /// Construct a fresh Ownership in a given initial state with a given lease
    /// expiration. Called by the consumer driver on Assign.
    pub(crate) fn new(initial_state: GrantState, expiration: SystemTime) -> Arc<Self> {
        let o = Arc::new(Self {
            active: Latch::new(),
            revoked: Latch::new(),
            expired: Latch::new(),
            request_revoke_signal: Latch::new(),
            loader: Loader::new(),
            unloader: Unloader::new(),
            expiration: Mutex::new(expiration),
            expiration_changed: Notify::new(),
        });
        // Mirror grant.go:107 — pre-close latches for already-reached states.
        match initial_state {
            GrantState::Active => o.active.close(),
            GrantState::Revoked | GrantState::RevokedUnloaded => o.revoked.close(),
            _ => {}
        }
        if SystemTime::now() > expiration {
            o.expired.close();
        }
        o
    }

    pub fn active(&self) -> LatchReader {
        self.active.reader()
    }
    pub fn revoked(&self) -> LatchReader {
        self.revoked.reader()
    }
    pub fn expired(&self) -> LatchReader {
        self.expired.reader()
    }
    pub fn loader(&self) -> Arc<Loader> {
        self.loader.clone()
    }
    pub fn unloader(&self) -> Arc<Unloader> {
        self.unloader.clone()
    }

    pub fn expiration(&self) -> SystemTime {
        *self.expiration.lock().expect("expiration mutex poisoned")
    }

    /// Request that the coordinator revoke this grant. Does NOT block on the
    /// revoke landing — the consumer should continue to honour ownership until
    /// `revoked()` fires (or the lease lapses).
    pub fn request_revoke(&self) {
        self.request_revoke_signal.close();
    }

    /// Driver-side updates. Not part of the public API.
    pub(crate) fn set_expiration(&self, deadline: SystemTime) {
        *self.expiration.lock().expect("expiration mutex poisoned") = deadline;
        self.expiration_changed.notify_waiters();
    }

    pub(crate) fn activate(&self) {
        self.active.close();
    }
    pub(crate) fn revoke(&self) {
        self.revoked.close();
    }
    pub(crate) fn expire(&self) {
        self.expired.close();
    }
    pub(crate) fn counterpart_loaded(&self) {
        self.unloader.loaded.close();
    }
    pub(crate) fn counterpart_unloaded(&self) {
        self.loader.unloaded.close();
    }
}

// ---------- WaitFor helpers (client.go:106-175) ----------

impl Ownership {
    /// Block until the previous counterpart has unloaded. If the grant becomes
    /// active before the counterpart unloads we consider it unloaded too (the
    /// grant has been activated outright). Returns the loader on success, or
    /// an ownership error if the grant is revoked or expires first.
    pub async fn wait_for_unload(self: &Arc<Self>) -> Result<Arc<Loader>, ClientError> {
        let active = self.active.reader();
        let unloaded = self.loader.unloaded.reader();
        let revoked = self.revoked.reader();
        let expired = self.expired.reader();

        tokio::select! {
            _ = active.closed() => Ok(self.loader.clone()),
            _ = unloaded.closed() => Ok(self.loader.clone()),
            _ = revoked.closed() => Err(ClientError::Revoked),
            _ = expired.closed() => Err(ClientError::Expired),
        }
    }

    /// Block until the grant is activated. Errors on revoke or expiration.
    pub async fn wait_for_active(&self) -> Result<(), ClientError> {
        let active = self.active.reader();
        let revoked = self.revoked.reader();
        let expired = self.expired.reader();
        tokio::select! {
            _ = active.closed() => Ok(()),
            _ = revoked.closed() => Err(ClientError::Revoked),
            _ = expired.closed() => Err(ClientError::Expired),
        }
    }

    /// Block until the grant is revoked. Returns the unloader. Errors on
    /// expiration.
    pub async fn wait_for_revoke(self: &Arc<Self>) -> Result<Arc<Unloader>, ClientError> {
        let revoked = self.revoked.reader();
        let expired = self.expired.reader();
        tokio::select! {
            _ = revoked.closed() => Ok(self.unloader.clone()),
            _ = expired.closed() => Err(ClientError::Expired),
        }
    }

    /// Block until the next counterpart has loaded. Errors on expiration.
    pub async fn wait_for_load(&self) -> Result<(), ClientError> {
        let loaded = self.unloader.loaded.reader();
        let expired = self.expired.reader();
        tokio::select! {
            _ = loaded.closed() => Ok(()),
            _ = expired.closed() => Err(ClientError::Expired),
        }
    }

    /// Block on a user-supplied action latch (e.g. `Range::initialized()` or
    /// the drain completion latch). Errors on expiration.
    pub async fn wait_for_action(&self, action: LatchReader) -> Result<(), ClientError> {
        let expired = self.expired.reader();
        tokio::select! {
            _ = action.closed() => Ok(()),
            _ = expired.closed() => Err(ClientError::Expired),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    fn far_future() -> SystemTime {
        SystemTime::now() + Duration::from_secs(3600)
    }

    #[tokio::test]
    async fn initial_active_state_fires_active_immediately() {
        let o = Ownership::new(GrantState::Active, far_future());
        timeout(Duration::from_millis(50), o.wait_for_active())
            .await
            .expect("already active")
            .expect("ok");
    }

    #[tokio::test]
    async fn initial_revoked_state_fires_revoked() {
        let o = Ownership::new(GrantState::Revoked, far_future());
        let err = timeout(Duration::from_millis(50), o.wait_for_active())
            .await
            .expect("resolves")
            .unwrap_err();
        assert!(matches!(err, ClientError::Revoked));
    }

    #[tokio::test]
    async fn expiration_in_past_pre_closes_expired() {
        let o = Ownership::new(
            GrantState::Allocated,
            SystemTime::now() - Duration::from_secs(1),
        );
        let err = timeout(Duration::from_millis(50), o.wait_for_active())
            .await
            .expect("resolves")
            .unwrap_err();
        assert!(matches!(err, ClientError::Expired));
    }

    #[tokio::test]
    async fn wait_for_unload_returns_loader_when_activated() {
        let o = Ownership::new(GrantState::Allocated, far_future());
        let o2 = o.clone();
        tokio::spawn(async move { o2.activate() });
        let loader = timeout(Duration::from_millis(100), o.wait_for_unload())
            .await
            .expect("resolves")
            .expect("ok");
        assert!(Arc::ptr_eq(&loader, &o.loader));
    }

    #[tokio::test]
    async fn wait_for_unload_returns_loader_when_counterpart_unloaded() {
        let o = Ownership::new(GrantState::Allocated, far_future());
        let o2 = o.clone();
        tokio::spawn(async move { o2.counterpart_unloaded() });
        timeout(Duration::from_millis(100), o.wait_for_unload())
            .await
            .expect("resolves")
            .expect("ok");
    }

    #[tokio::test]
    async fn wait_for_revoke_returns_unloader() {
        let o = Ownership::new(GrantState::Active, far_future());
        let o2 = o.clone();
        tokio::spawn(async move { o2.revoke() });
        let unloader = timeout(Duration::from_millis(100), o.wait_for_revoke())
            .await
            .expect("resolves")
            .expect("ok");
        assert!(Arc::ptr_eq(&unloader, &o.unloader));
    }

    #[tokio::test]
    async fn wait_for_load_resolves_on_counterpart_loaded() {
        let o = Ownership::new(GrantState::Revoked, far_future());
        let o2 = o.clone();
        tokio::spawn(async move { o2.counterpart_loaded() });
        timeout(Duration::from_millis(100), o.wait_for_load())
            .await
            .expect("resolves")
            .expect("ok");
    }

    #[tokio::test]
    async fn loader_load_and_unloader_unload_are_idempotent() {
        let o = Ownership::new(GrantState::Allocated, far_future());
        let l = o.loader();
        l.load();
        l.load();
        assert!(l.load.is_closed());
        let u = o.unloader();
        u.unload();
        u.unload();
        assert!(u.unload.is_closed());
    }

    #[tokio::test]
    async fn request_revoke_closes_signal() {
        let o = Ownership::new(GrantState::Active, far_future());
        assert!(!o.request_revoke_signal.is_closed());
        o.request_revoke();
        assert!(o.request_revoke_signal.is_closed());
    }

    #[tokio::test]
    async fn set_expiration_wakes_waiters() {
        let o = Ownership::new(GrantState::Active, far_future());
        let o2 = o.clone();
        let waiter = tokio::spawn(async move { o2.expiration_changed.notified().await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        o.set_expiration(SystemTime::now() + Duration::from_secs(10));
        timeout(Duration::from_millis(50), waiter)
            .await
            .expect("notified")
            .expect("task ok");
    }
}
