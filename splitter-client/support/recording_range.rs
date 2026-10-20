//! Shared lifecycle fixture for unit tests, integration tests, and examples.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use splitter_client::{Latch, LatchReader, Range};

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
