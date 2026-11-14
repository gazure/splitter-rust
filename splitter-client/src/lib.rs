//! Rust client for the splitter data plane.
//!
//! # Module map
//!
//! - [`consumer`] — drives the `ConsumerService.Join` bidi stream.
//! - [`ownership`] — per-grant lifecycle signals ([`Ownership`],
//!   [`Loader`], [`Unloader`]) and the `wait_for_*` helpers.
//! - [`cluster`] — immutable cluster-view snapshots ([`ClusterMap`]).
//! - [`pool`] — peer [`ConnectionPool`] keyed on `InstanceId`.
//! - [`dispatcher`] — filter-chain orchestration around a running
//!   consumer ([`Dispatcher`], [`DispatchFilter`]).
//! - [`processor`] — a [`DispatchFilter`] that manages a user-supplied
//!   [`Range`] per grant via the 5-step handover state machine.
//! - [`proxy`] + [`resolver`] + [`retry`] — local+remote request routing
//!   with ownership-aware retries.
//!
//! # Naming conventions
//!
//! Graceful-stop methods follow a consistent rule across the API:
//!
//! | method             | semantics                                      |
//! |--------------------|------------------------------------------------|
//! | `close()`          | fire a one-shot signal; does not block         |
//! | `shutdown()`       | trigger graceful stop and await completion     |
//! | `drain(timeout)`   | trigger graceful stop, bounded by the timeout  |
//!
//! # Wire conversions and API boundaries
//!
//! Convert between protocol messages and client types with `From` and `TryFrom`:
//!
//! ```
//! use splitter_client::{DomainType, QualifiedDomainName, QualifiedServiceName, Shard};
//! use splitter_proto::pb;
//!
//! let shard = Shard {
//!     domain: QualifiedDomainName {
//!         service: QualifiedServiceName::new("tenant", "service"),
//!         name: "ranges".into(),
//!     },
//!     kind: DomainType::Global,
//!     region: Default::default(),
//!     from: "a".into(),
//!     to: "z".into(),
//! };
//! let message: pb::Shard = shard.clone().into();
//! assert_eq!(Shard::try_from(message).unwrap(), shard);
//! ```
//!
//! Session framing and conversion helper modules are internal:
//!
//! ```compile_fail,E0603
//! use splitter_client::session::heartbeat_frame;
//! ```
//!
//! ```compile_fail,E0603
//! use splitter_client::wrap::timestamp_to_system_time;
//! ```
//!
//! Example and test fixtures are excluded from the library API:
//!
//! ```compile_fail,E0432
//! use splitter_client::RecordingRange;
//! ```
//!
//! ```compile_fail,E0432
//! use splitter_client::processor::RecordingRange;
//! ```

// ClientError carries a tonic::Status which is ~176 bytes; boxing it would
// complicate the #[from] conversions without real benefit. Results carrying
// ClientError are our standard return type, so accept the size.
#![allow(clippy::result_large_err)]

pub mod cluster;
pub mod consumer;
pub mod dispatcher;
pub mod error;
pub mod grant_map;
pub mod ids;
pub mod latch;
pub mod ownership;
pub mod pool;
pub mod processor;
pub mod proxy;
pub mod resolver;
pub mod retry;
mod session;
mod wrap;

pub use cluster::{ClusterId, ClusterMap, ConsumerEntry, GrantEntry};
pub use consumer::{
    AssignmentSnapshot, ConsumerClient, ConsumerOptions, GrantAssignment, GrantEvent, JoinHandle,
    ReconnectOptions,
};
pub use dispatcher::{DispatchFilter, Dispatcher, DispatcherBuilder, FilterContext};
pub use error::{ClientError, Result};
pub use grant_map::{GrantMap, GrantMapReader};
pub use ids::{
    ConsumerId, DomainKey, DomainKeyName, DomainName, DomainType, Endpoint, GrantId, GrantState,
    Instance, InstanceId, InstanceName, Location, NodeName, QualifiedDomainKey,
    QualifiedDomainName, QualifiedServiceName, Region, ServiceName, Shard, TenantName,
};
pub use latch::{Latch, LatchReader};
pub use ownership::{Loader, Ownership, Unloader};
pub use pool::ConnectionPool;
pub use processor::{Processor, Range, RangeFactory};
pub use proxy::{Proxy, handle, handle_with_retry};
pub use resolver::{DomainResolver, RemoteFn};
pub use retry::{OwnershipBackoff, retry_ownership};

#[cfg(test)]
extern crate self as splitter_client;

#[cfg(test)]
#[path = "../support/recording_range.rs"]
mod recording_range;
