//! Processor: a `DispatchFilter` that manages a generic [`Range`] per grant.
//!
//! Port of `pkg/model/dispatcher.go:179-302`. For every grant the factory
//! accepts, the Processor runs the 5-step ownership state machine:
//!
//!   1. Wait for counterpart unload (or direct activation) → get `Loader`.
//!   2. Build a `Range`, wait for it to initialize, call `loader.load()`.
//!   3. Wait for activation.
//!   4. Wait for revocation → get `Unloader`; drain the Range with a timeout.
//!   5. Signal unload via `unloader.unload()`, wait for counterpart load.
//!
//! Any step may abort on `Ownership::expired()` firing, at which point the
//! Range is closed and cleaned up via the RAII `GrantCleanup` guard.

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use tracing::debug;

use crate::{
    dispatcher::{DispatchFilter, FilterContext},
    grant_map::{GrantMap, GrantMapReader},
    ids::{GrantId, Shard},
    latch::{Latch, LatchReader},
    ownership::{Loader, Ownership},
};

/// A user-supplied range-of-ownership. The `Range` governs whatever state
/// lives behind a single assigned shard (for example, a per-range `HashMap` of actors,
/// an in-memory cache, or a subscription to an external data feed).
#[async_trait]
pub trait Range: Send + Sync + 'static {
    /// Closes once the Range has finished initialization.
    fn initialized(&self) -> LatchReader;

    /// Called when the grant is revoked. Returns a `LatchReader` that closes
    /// after drain completes. The `timeout` is the remaining lease (after
    /// which the coordinator will hard-expire the grant anyway).
    async fn drain(&self, timeout: Duration) -> LatchReader;

    /// Closes if the Range terminates on its own (e.g. internal failure). The
    /// Processor aborts the state machine when this fires.
    fn closed(&self) -> LatchReader;

    /// Force-close the Range. The Processor calls this on state-machine exit
    /// (via the `GrantCleanup` RAII guard).
    fn close(&self);
}

/// Factory that builds `Range`s for shards this processor owns.
pub trait RangeFactory: Send + Sync + 'static {
    type Range: Range;

    /// Build a `Range` for `shard`, or return `None` if this factory doesn't
    /// handle that shard's domain. Returning `None` causes the Processor to
    /// delegate to the next `DispatchFilter` in the chain.
    fn build(&self, id: GrantId, shard: Shard, ownership: Arc<Ownership>) -> Option<Self::Range>;
}

pub struct Processor<F: RangeFactory> {
    factory: Arc<F>,
    grants: Arc<GrantMap<Arc<F::Range>>>,
    initialized: Latch,
}

impl<F: RangeFactory> Processor<F> {
    pub fn new(factory: Arc<F>) -> Arc<Self> {
        Arc::new(Self {
            factory,
            grants: Arc::new(GrantMap::new()),
            initialized: Latch::new(),
        })
    }

    /// Returns a cloneable, read-only handle to the live range registry.
    ///
    /// Callers can inspect and retrieve ranges but cannot remove entries:
    ///
    /// ```compile_fail
    /// use splitter_client::{GrantId, Processor, RangeFactory};
    ///
    /// fn remove<F: RangeFactory>(processor: &Processor<F>, id: &GrantId) {
    ///     processor.grants().delete(id);
    /// }
    /// ```
    ///
    /// Only the processor can publish a range as active:
    ///
    /// ```compile_fail
    /// use std::sync::Arc;
    /// use splitter_client::{GrantId, Processor, RangeFactory, Shard};
    ///
    /// fn activate<F: RangeFactory>(
    ///     processor: &Processor<F>, id: GrantId, shard: Shard, range: Arc<F::Range>,
    /// ) {
    ///     processor.grants().activate(id, shard, range);
    /// }
    /// ```
    pub fn grants(&self) -> GrantMapReader<Arc<F::Range>> {
        GrantMapReader::new(self.grants.clone())
    }
}

#[async_trait]
impl<F: RangeFactory> DispatchFilter for Processor<F> {
    fn init(&self, _ctx: FilterContext) {
        self.initialized.close();
    }

    async fn try_handle(&self, id: GrantId, shard: Shard, ownership: Arc<Ownership>) -> bool {
        self.initialized.closed().await;
        let Ok(loader) = ownership.wait_for_unload().await else {
            return true;
        };
        let Some(range) = self
            .factory
            .build(id.clone(), shard.clone(), ownership.clone())
        else {
            return false;
        };
        let range = Arc::new(range);
        self.run_grant(id, shard, ownership, range, loader).await;
        true
    }
}

impl<F: RangeFactory> Processor<F> {
    async fn run_grant(
        &self,
        id: GrantId,
        shard: Shard,
        o: Arc<Ownership>,
        range: Arc<F::Range>,
        loader: Arc<Loader>,
    ) {
        let _cleanup = GrantCleanup::<F::Range> {
            id: id.clone(),
            grants: self.grants.clone(),
            range: range.clone(),
            ownership: o.clone(),
        };

        // Step 2: Record the grant and wait for Range initialization. Race
        // against Range::closed — if the user's Range self-destructs during
        // init we abandon the grant.
        self.grants
            .allocated(id.clone(), shard.clone(), range.clone());
        let init = range.initialized();
        let closed = range.closed();
        tokio::select! {
            biased;
            () = closed.closed() => return,
            () = o.revoked.closed() => return,
            res = o.wait_for_action(init) => {
                if let Err(e) = res {
                    debug!(%id, error = %e, "step 2 init bail");
                    return;
                }
            }
        }
        loader.load();
        self.grants.loaded(id.clone(), shard.clone(), range.clone());

        // Step 3: Wait for activation.
        tokio::select! {
            biased;
            () = closed.closed() => return,
            result = o.wait_for_active() => if result.is_err() { return; },
        }
        self.grants
            .activate(id.clone(), shard.clone(), range.clone());

        // Step 4: Wait for revocation, then drain.
        let unloader = tokio::select! {
            biased;
            () = closed.closed() => return,
            result = o.wait_for_revoke() => match result {
                Ok(unloader) => unloader,
                Err(_) => return,
            },
        };
        self.grants.revoke(id.clone(), shard.clone(), range.clone());
        let timeout = o
            .expiration()
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO);
        let drain_complete = tokio::select! {
            biased;
            () = closed.closed() => return,
            () = o.wait_for_expiration() => return,
            result = tokio::time::timeout(timeout, range.drain(timeout)) => match result {
                Ok(completed) => completed,
                Err(_) => return,
            },
        };

        // Step 5: Wait for drain to complete, signal unload, wait for
        // counterpart load.
        tokio::select! {
            biased;
            () = closed.closed() => return,
            result = o.wait_for_action(drain_complete) => if result.is_err() { return; },
        }
        unloader.unload();
        self.grants
            .unloaded(id.clone(), shard.clone(), range.clone());
        tokio::select! {
            biased;
            () = closed.closed() => {},
            _ = o.wait_for_load() => {},
        }
    }
}

/// RAII guard: remove the grant from the processor's map and force-close the
/// Range on drop. Runs on every `run_grant` exit path, including early
/// returns and panics.
struct GrantCleanup<R: Range + ?Sized> {
    id: GrantId,
    grants: Arc<GrantMap<Arc<R>>>,
    range: Arc<R>,
    ownership: Arc<Ownership>,
}

impl<R: Range + ?Sized> Drop for GrantCleanup<R> {
    fn drop(&mut self) {
        self.grants
            .delete_if(&self.id, |current| Arc::ptr_eq(current, &self.range));
        self.range.close();
        self.ownership.release();
    }
}

// ---------- Helper: minimal Range impls for examples / tests ----------

/// A `Range` that records lifecycle events for inspection. Useful in tests
/// and in the `processor` example. `drain` closes its latch after `drain_delay`
/// (or the timeout, whichever is sooner).
pub struct RecordingRange {
    initialized: Latch,
    closed: Latch,
    drain_delay: Duration,
    log: Arc<std::sync::Mutex<Vec<String>>>,
    label: String,
}

impl RecordingRange {
    /// Creates a range that records initialization and drain events.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime.
    pub fn new(
        label: impl Into<String>,
        init_delay: Duration,
        drain_delay: Duration,
        log: Arc<std::sync::Mutex<Vec<String>>>,
    ) -> Self {
        let label = label.into();
        let initialized = Latch::new();
        let log_init = log.clone();
        let label_init = label.clone();
        let init = initialized.clone();
        tokio::spawn(async move {
            tokio::time::sleep(init_delay).await;
            log_init
                .lock()
                .expect("log poisoned")
                .push(format!("{label_init} initialized"));
            init.close();
        });
        Self {
            initialized,
            closed: Latch::new(),
            drain_delay,
            log,
            label,
        }
    }

    /// Appends a labeled message to the event log.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer poisoned the log mutex.
    pub fn log(&self, msg: &str) {
        self.log
            .lock()
            .expect("log poisoned")
            .push(format!("{} {msg}", self.label));
    }
}

#[async_trait]
impl Range for RecordingRange {
    fn initialized(&self) -> LatchReader {
        self.initialized.reader()
    }

    async fn drain(&self, timeout: Duration) -> LatchReader {
        self.log("drain start");
        let done = Latch::new();
        let log = self.log.clone();
        let label = self.label.clone();
        let delay = self.drain_delay.min(timeout);
        let d = done.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            log.lock()
                .expect("log poisoned")
                .push(format!("{label} drain done"));
            d.close();
        });
        done.reader()
    }

    fn closed(&self) -> LatchReader {
        self.closed.reader()
    }

    fn close(&self) {
        if !self.closed.is_closed() {
            self.log("closed");
            self.closed.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{DomainType, GrantState, QualifiedDomainName, QualifiedServiceName};
    use std::sync::Mutex;

    struct FixedFactory {
        log: Arc<Mutex<Vec<String>>>,
    }
    impl RangeFactory for FixedFactory {
        type Range = RecordingRange;
        fn build(&self, _id: GrantId, _shard: Shard, _o: Arc<Ownership>) -> Option<RecordingRange> {
            Some(RecordingRange::new(
                "r",
                Duration::from_millis(10),
                Duration::from_millis(10),
                self.log.clone(),
            ))
        }
    }

    struct RejectFactory;
    impl RangeFactory for RejectFactory {
        type Range = RecordingRange;
        fn build(&self, _id: GrantId, _shard: Shard, _o: Arc<Ownership>) -> Option<RecordingRange> {
            None
        }
    }

    fn mk_shard() -> Shard {
        Shard {
            domain: QualifiedDomainName {
                service: QualifiedServiceName::new("t", "s"),
                name: "d".into(),
            },
            kind: DomainType::Global,
            region: crate::ids::Region::default(),
            from: String::new(),
            to: String::new(),
        }
    }

    fn far_future() -> SystemTime {
        SystemTime::now() + Duration::from_secs(3600)
    }

    #[tokio::test]
    async fn rejecting_factory_returns_false() {
        let p = Processor::new(Arc::new(RejectFactory));
        p.initialized.close();
        let o = Ownership::new(GrantState::Active, far_future());
        // Note: try_handle takes &self; processor is still borrowed here, that
        // is fine within a single async fn (no task spawn).
        let took = p.try_handle(GrantId("g".into()), mk_shard(), o).await;
        assert!(!took);
    }

    #[tokio::test]
    async fn full_lifecycle_records_all_phases() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let p = Processor::new(Arc::new(FixedFactory { log: log.clone() }));
        p.initialized.close();
        let o = Ownership::new(GrantState::Allocated, far_future());
        let o_driver = o.clone();

        let p2 = p.clone();
        let task =
            tokio::spawn(async move { p2.try_handle(GrantId("g".into()), mk_shard(), o).await });

        // Simulate the driver: init is automatic via spawn in RecordingRange.
        // Step 1 needs counterpart-unloaded:
        tokio::time::sleep(Duration::from_millis(20)).await;
        o_driver.counterpart_unloaded();
        // Step 3 needs active:
        tokio::time::sleep(Duration::from_millis(20)).await;
        o_driver.activate();
        // Step 4 needs revoke:
        tokio::time::sleep(Duration::from_millis(20)).await;
        o_driver.revoke();
        // Step 5 needs counterpart-loaded:
        tokio::time::sleep(Duration::from_millis(40)).await;
        o_driver.counterpart_loaded();

        let took = task.await.expect("task joined");
        assert!(took);

        let events = log.lock().unwrap().clone();
        let joined = events.join("|");
        assert!(
            joined.contains("r initialized")
                && joined.contains("r drain start")
                && joined.contains("r drain done")
                && joined.contains("r closed"),
            "unexpected log: {joined}"
        );
        // Verify the Loader/Unloader signals fired (i.e. steps 2 & 5 reached).
        assert!(o_driver.loader.load.is_closed(), "loader.load should fire");
        assert!(
            o_driver.unloader.unload.is_closed(),
            "unloader.unload should fire"
        );
        // Grant map cleaned up on exit.
        assert!(p.grants().is_empty());
    }

    #[tokio::test]
    async fn exit_on_expiration_cleans_up() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let p = Processor::new(Arc::new(FixedFactory { log: log.clone() }));
        p.initialized.close();
        let o = Ownership::new(GrantState::Allocated, far_future());
        let o_driver = o.clone();
        let p2 = p.clone();
        let task =
            tokio::spawn(async move { p2.try_handle(GrantId("g".into()), mk_shard(), o).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Expire before any progress.
        o_driver.expire();
        let took = task.await.expect("task joined");
        assert!(took);
        assert!(p.grants().is_empty());
        // No application state is created before counterpart unload.
        assert!(log.lock().unwrap().is_empty());
    }
    #[derive(Clone)]
    struct ControlledRange {
        initialized: Latch,
        closed: Latch,
        draining: Latch,
        drained: Latch,
        hang_in_drain: bool,
    }
    #[async_trait]
    impl Range for ControlledRange {
        fn initialized(&self) -> LatchReader {
            self.initialized.reader()
        }
        fn closed(&self) -> LatchReader {
            self.closed.reader()
        }
        fn close(&self) {
            self.closed.close();
        }
        async fn drain(&self, _: Duration) -> LatchReader {
            self.draining.close();
            if self.hang_in_drain {
                std::future::pending::<()>().await;
            }
            self.drained.reader()
        }
    }
    struct ControlledFactory {
        range: ControlledRange,
        built: Latch,
    }
    impl RangeFactory for ControlledFactory {
        type Range = ControlledRange;
        fn build(&self, _: GrantId, _: Shard, _: Arc<Ownership>) -> Option<ControlledRange> {
            self.built.close();
            Some(self.range.clone())
        }
    }
    fn controlled(
        hang_in_drain: bool,
    ) -> (Arc<Processor<ControlledFactory>>, ControlledRange, Latch) {
        let range = ControlledRange {
            initialized: Latch::new(),
            closed: Latch::new(),
            draining: Latch::new(),
            drained: Latch::new(),
            hang_in_drain,
        };
        let built = Latch::new();
        let processor = Processor::new(Arc::new(ControlledFactory {
            range: range.clone(),
            built: built.clone(),
        }));
        processor.initialized.close();
        (processor, range, built)
    }
    async fn state(processor: &Processor<ControlledFactory>, expected: GrantState) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !processor
                .grants
                .get(&GrantId("g".into()))
                .is_some_and(|(_, state, _)| state == expected)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn factory_waits_until_previous_owner_unloads() {
        let (processor, range, built) = controlled(false);
        let ownership = Ownership::new(GrantState::Allocated, far_future());
        let p = processor.clone();
        let o = ownership.clone();
        let task =
            tokio::spawn(async move { p.try_handle(GrantId("g".into()), mk_shard(), o).await });
        tokio::task::yield_now().await;
        assert!(!built.is_closed());
        ownership.counterpart_unloaded();
        tokio::time::timeout(Duration::from_secs(2), built.closed())
            .await
            .unwrap();
        range.close();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn self_close_aborts_every_lifecycle_wait_and_releases_grant() {
        for phase in 0..5 {
            let (processor, range, built) = controlled(false);
            let ownership = Ownership::new(GrantState::Allocated, far_future());
            ownership.counterpart_unloaded();
            let p = processor.clone();
            let o = ownership.clone();
            let task =
                tokio::spawn(async move { p.try_handle(GrantId("g".into()), mk_shard(), o).await });
            tokio::time::timeout(Duration::from_secs(2), built.closed())
                .await
                .unwrap();
            if phase >= 1 {
                range.initialized.close();
                state(&processor, GrantState::AllocatedLoaded).await;
            }
            if phase >= 2 {
                ownership.activate();
                state(&processor, GrantState::Active).await;
            }
            if phase >= 3 {
                ownership.revoke();
                state(&processor, GrantState::Revoked).await;
            }
            if phase >= 4 {
                range.drained.close();
                state(&processor, GrantState::RevokedUnloaded).await;
            }
            range.close();
            tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
            assert!(ownership.released.is_closed());
            assert!(processor.grants.is_empty());
        }
    }

    #[tokio::test]
    async fn hung_drain_call_is_cancelled_when_lease_expires() {
        let (processor, range, _) = controlled(true);
        range.initialized.close();
        let ownership = Ownership::new(GrantState::Active, far_future());
        let p = processor.clone();
        let o = ownership.clone();
        let task =
            tokio::spawn(async move { p.try_handle(GrantId("g".into()), mk_shard(), o).await });
        state(&processor, GrantState::Active).await;
        ownership.revoke();
        tokio::time::timeout(Duration::from_secs(2), range.draining.closed())
            .await
            .unwrap();
        ownership.set_expiration(SystemTime::UNIX_EPOCH);
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert!(range.closed.is_closed());
        assert!(!ownership.unloader.unload.is_closed());
        assert!(processor.grants.is_empty());
    }

    #[tokio::test]
    async fn old_cleanup_cannot_delete_replacement_grant() {
        let (processor, old, _) = controlled(false);
        let replacement = Arc::new(controlled(false).1);
        let id = GrantId("g".into());
        processor
            .grants
            .activate(id.clone(), mk_shard(), replacement.clone());
        drop(GrantCleanup {
            id: id.clone(),
            grants: processor.grants.clone(),
            range: Arc::new(old),
            ownership: Ownership::new(GrantState::Active, far_future()),
        });
        assert!(Arc::ptr_eq(
            &processor.grants.get(&id).unwrap().2,
            &replacement
        ));
    }
}
