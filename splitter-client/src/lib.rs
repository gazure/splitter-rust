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
pub mod session;
pub mod wrap;

pub use cluster::{ClusterId, ClusterMap, ConsumerEntry, GrantEntry};
pub use consumer::{ConsumerClient, ConsumerOptions, GrantEvent, JoinHandle};
pub use dispatcher::{DispatchFilter, Dispatcher, DispatcherBuilder, FilterContext};
pub use error::{ClientError, Result};
pub use grant_map::GrantMap;
pub use ids::{
    ConsumerId, DomainKey, DomainKeyName, DomainName, DomainType, Endpoint, GrantId, GrantState,
    Instance, InstanceId, InstanceName, Location, NodeName, QualifiedDomainKey,
    QualifiedDomainName, QualifiedServiceName, Region, ServiceName, Shard, TenantName,
};
pub use latch::{Latch, LatchReader};
pub use ownership::{Loader, Ownership, Unloader};
pub use pool::ConnectionPool;
pub use processor::{Processor, Range, RangeFactory, RecordingRange};
pub use proxy::{handle, handle_with_retry, Proxy};
pub use resolver::{DomainResolver, RemoteFn};
pub use retry::{retry_ownership, OwnershipBackoff};
