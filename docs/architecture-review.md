**Splitter Rust client architecture review**

Updated October 20, 2026. This backlog records the architecture review and the
decisions reached in follow-up discussion. Review candidates are not confirmed
bugs or approved redesigns. The current behavior is described in the
[lifecycle guide](lifecycle.md).

Preserve the single-owner session driver, authoritative snapshots, typed
identifiers, RAII cleanup, and the existing client/proto/peer crate boundaries.
Preserve observable protocol compatibility with Go without requiring identical
internal structures. Base proposals on the core client's contracts and supported
callers.

**Closure decision**

The agreed responsibility boundary is:

- Closed ranges remain callable. Their application request handlers must return
  `NotOwned` errors.
- The proxy can invoke a local handler through either its active fast path or
  fallback path without checking range closure.
- Registry cleanup removes the range from subsequent lookups. Retained
  references remain subject to the range's request-handling contract.
- `Drain` remains the application mechanism for graceful handover. `Close`
  remains forceful termination of the lifecycle.

Proxy closure enforcement is withdrawn. This decision does not require operation
counters, per-action reporting, joining every application worker, or changes to
protocol release ordering.
The original proposal to redesign completion/release was withdrawn: a concrete
defect in the lifecycle contract was not established.

The Rust proxy's lack of an explicit closure check follows this contract.
The local Go checkout also does not check `IsClosed()` in `HandleEx`,
`HandleLocalEx`, or `GrantMap.Lookup`; it relies on removal and application
handling. Compare additional client implementations and usage patterns when the
work sources become available. Those sources are not assumed to contain a
different version of the Go library.

Relevant code: [Rust proxy](../splitter-client/src/proxy.rs),
[Rust processor](../splitter-client/src/processor.rs),
[Go proxy](../../splitter/pkg/model/proxy.go), and
[Go grant map](../../splitter/pkg/model/cluster.go).

**Review backlog**

The mutation boundaries and public API surface cleanup in item 2 are implemented.
Item 3 has an initial caller inventory and assessment below; its API
recommendations remain proposals. The suggested next review is item 4: protocol
state and asynchronous effects. The remaining order is provisional; establish a
caller problem or correctness invariant before choosing a refactor.

1. **Range closure contract — resolved; proxy enforcement withdrawn.**
   Closed ranges remain callable and their request handlers return `NotOwned`.
   Preserve this responsibility in range implementations, including when callers
   retain references after registry removal. No proxy closure interface or
   admission check is required. Source: [proxy](../splitter-client/src/proxy.rs).

2. **Public API boundaries and invariant protection — implemented.**
   `Processor::grants()` returns a cloneable `GrantMapReader` with read methods;
   the processor retains the mutable registry. `ClusterMap` fields are private,
   with read-only accessors and validated updates through `apply()`. Compile-fail
   tests protect both boundaries. Custom processors can still own a mutable
   `GrantMap`, and retrieved application range handles retain their lifecycle
   responsibilities. The changes are stacked locally as
   `refactor/processor-grant-view` and `refactor/cluster-snapshot-access`.
   `session` and `wrap` are internal. `RecordingRange` lives in shared
   test/example support, with both public export paths removed. Application
   module paths and wire conversion trait implementations remain available.
   This cleanup is on `refactor/client-public-surface`. Sources:
   [cluster](../splitter-client/src/cluster.rs),
   [grant map](../splitter-client/src/grant_map.rs), and
   [exports](../splitter-client/src/lib.rs).

3. **Observation versus ownership authority — initial review complete, design open.**
   Assignment snapshots expose `Arc<Ownership>`, which combines observation
   with loading, unloading, revocation requests, and release. Inventory callers
   before proposing read-only views and a separate controller. Evaluate an
   explicit local assignment generation in place of exposed pointer-identity
   conventions. A local generation is not a distributed fencing token. Test
   stale acknowledgments against replacement assignments. Sources:
   [ownership](../splitter-client/src/ownership.rs),
   [consumer](../splitter-client/src/consumer.rs), and
   [dispatcher](../splitter-client/src/dispatcher.rs).

    The October 20 assessment below identifies an observation API boundary worth
    narrowing. It does not establish a stale-action correctness defect or a need
    to replace pointer identity with a generation counter.

4. **Protocol state and asynchronous effects — review candidate.**
   The driver combines wire state, confirmation and revocation flags, leases,
   transport operations, and recovery. Map the authoritative owner of each
   fact and the cancellation boundaries around sends. Different representations
   can serve different purposes; duplication alone is not a defect. Consider a
   deterministic transition core with explicit effects only where it makes
   ordering and recovery easier to verify. Preserve partial snapshots, replay,
   shared leases, and detached revoked leases. Require transition and
   cancellation tests before changing structure. Source:
   [consumer](../splitter-client/src/consumer.rs).

5. **Factory selection and application lifecycle extensibility — review candidate.**
   `RangeFactory::build()` combines selection and construction through
   `Option<Range>`. `DispatchFilter::try_handle()` combines selection with a
   long-running lifecycle. Evaluate separate selection and fallible construction
   if supported callers need them. Establish a concrete lifecycle customization
   need before generalizing orchestration. Preserve legal handover ordering.
   Sources: [processor](../splitter-client/src/processor.rs) and
   [dispatcher](../splitter-client/src/dispatcher.rs).

6. **Consumer lifetime — default reconnection implemented; broader supervision review open.**
   The consumer owns an ongoing reconnect loop, following the Go client's
   lifetime model. Session failures do not require an application restart loop.
   Reconnection is enabled by default, including initial connection failures.
   Controls bound each connection attempt, optionally bound continuous
   disconnection, and bound graceful drain. Dropping the handle force-stops it.
   Existing grants retain their current leases during outages, and local
   completion does not require a coordinator acknowledgment. At hard expiry,
   the processor closes the range and releases locally; graceful drain remains
   bounded by the lease during revocation.
   This change is on `fix/consumer-reconnect-default`. Validation passed 163
   workspace tests and 15 doc tests, formatting, Clippy, and documentation builds.
   The live registration retry test passes. Delayed promotion, two-consumer
   handover, and final UNIT-consumer drain fail against the local coordinator
   image; all three failures reproduce on the unchanged parent branch.
   A returned handle is not a readiness signal. Broader readiness information,
   panic handling, and peer-server supervision remain review candidates.
   Zero assignments remain valid. Sources:
   [consumer](../splitter-client/src/consumer.rs),
   [dispatcher](../splitter-client/src/dispatcher.rs), and
   [peer crate](../splitter-peer).

7. **Routing outcomes, errors, and retry contracts — review candidate.**
   Local ownership currently appears as `NoResolution`, an error used for normal
   routing control flow. Evaluate explicit local, remote, and unassigned
   outcomes. Identify recovery decisions obscured by string-based session
   errors. Distinguish ownership rejection from ambiguous transport failure in
   retry policy; a clonable request is not proof that repetition is safe.
   Preserve Go-compatible gRPC status mappings. Sources:
   [resolver](../splitter-client/src/resolver.rs),
   [errors](../splitter-client/src/error.rs), and
   [retry](../splitter-client/src/retry.rs).

8. **Lease time and validated boundary types — review candidate.**
   Audit wall-clock expiration, monotonic scheduling, checked timestamp
   conversion, and renewal behavior. Consider an injected clock for deterministic
   forward/backward clock tests. Review fallible parsing and shard/domain
   representations for invalid combinations. Validate against the wire contract
   rather than inventing stricter name or key restrictions. Sources:
   [ownership](../splitter-client/src/ownership.rs),
   [consumer](../splitter-client/src/consumer.rs),
   [wire conversions](../splitter-client/src/wrap.rs), and
   [identifiers](../splitter-client/src/ids.rs).

9. **Callback boundaries and scaling — audit first, benchmark before redesign.**
   Pool dial callbacks execute under a write lock; resolution retains a snapshot
   borrow through callback execution. Assess reentrancy and lock duration.
   Moving construction outside a lock requires checking endpoint identity before
   publishing the result. Measure routing scans, snapshot copying, per-grant
   watchers, and action-channel growth at realistic grant counts. Keep short
   synchronous locks where appropriate. Sources:
   [pool](../splitter-client/src/pool.rs),
   [resolver](../splitter-client/src/resolver.rs), and
   [cluster](../splitter-client/src/cluster.rs).

10. **Validation strategy — applies to each accepted change.**
    Use deterministic protocol tests for transitions and cancellation, in-process
    transport tests for message behavior, and the live harness for coordinator
    compatibility. Add model/property tests or benchmarks where they establish
    a specific invariant or performance claim. Compare Go behavior as evidence,
    not as an unquestionable oracle. Keep nightly formatting and strict workspace
    Clippy checks. See [live testing](live-testing.md).

**Public API surface assessment — October 20**

Before this cleanup, the crate exported all 15 source modules and re-exported its
main application types at the root. Module visibility alone does not expose
private state. The driver,
grant action queue, cleanup guards, and ownership signal writers remain private
or crate-visible. The implemented registry and cluster boundaries continue to
protect their invariants.

The implemented changes narrow the supported API contract:

| Surface | Use | Implemented boundary |
|---|---|---|
| `session` | Frame construction, inbound classification, and heartbeat scheduling used by the consumer and its unit tests | Private module with crate-visible shared helpers |
| `wrap` | Wire conversion helpers used inside the client; `From` and `TryFrom` implementations also serve external callers | Private module with trait implementations retained |
| `RecordingRange` | Used by processor/consumer unit tests, two examples, and live integration tests | Shared support source with both public paths removed |
| Application modules and root re-exports | Expose consumer, processor, routing, ownership, identifiers, and lifecycle primitives | Existing paths retained |

Sources: [exports](../splitter-client/src/lib.rs),
[session helpers](../splitter-client/src/session.rs),
[wire conversions](../splitter-client/src/wrap.rs),
[recording range](../splitter-client/support/recording_range.rs),
[processor example](../splitter-client/examples/processor.rs),
[peer example](../splitter-client/examples/peer_server.rs), and
[live tests](../splitter-client/tests/e2e.rs).

No external use of the named `session` or `wrap` helper functions was found in
the inspected workspace examples or integration tests. This does not establish
that downstream applications never use them. Hiding these modules removes
public import paths and is an API change. The wire conversion trait
implementations remain usable through `into()` and `try_into()` even when their
implementation module is private. Timestamp conversion behavior remains a
separate boundary-validation review topic.

`RecordingRange` simulates initialization and drain with timers and records a
string log. It is a fixture, not an application request handler. Removing only
the root re-export leaves `processor::RecordingRange` public. Gating it only with
`cfg(test)` would break examples and integration tests, which compile against
the normal library. A shared support source module serves those targets and
the unit tests without adding a production export or a new crate.

The public `Latch` and `LatchReader` types support application implementations of
`Range`; their visibility is intentional. Likewise, custom processors can own a
mutable `GrantMap`, while `Processor::grants()` exposes only a reader. Broadly
hiding application modules would add import churn without demonstrating another
invariant improvement.

Compile-fail documentation tests cover the two internal helper modules and both
removed fixture paths. An executable documentation example verifies external
`From` and `TryFrom` conversions for a shard. Validation passed 145 workspace
tests and 15 documentation tests. Six live tests and one peer documentation
example remain ignored. The peer tests required a rerun with loopback socket
access after the sandbox denied their binds. Documentation builds with warnings
denied, nightly formatting, and strict workspace Clippy checks passed.

**Observation versus ownership assessment — October 20**

The assessment uses the Rust client, its examples and tests, and the Go model
interfaces. No production caller inventory is available from these sources. The
following recommendations are not accepted API changes.

The current callers need different capabilities:

| Caller | Observed use | Required authority |
|---|---|---|
| Consumer driver | Maintains protocol state, leases, and counterpart signals | Internal lifecycle transitions |
| Dispatcher | Reconciles assignments, runs filters, and releases on handler exit | Assignment identity, revocation requests, and release |
| Processor and custom `DispatchFilter` | Coordinate initialization, activation, drain, and completion | Full application lifecycle controls |
| Direct consumer example and live tests | Drive or terminate assignments from the assignment snapshot | Full application lifecycle controls |
| Example range factories | Ignore the ownership argument | None in these examples |
| Broadcast event subscribers | No application subscribers found in the inspected Rust callers | Intended for observation by the public documentation |

Sources: [dispatcher](../splitter-client/src/dispatcher.rs),
[processor](../splitter-client/src/processor.rs),
[direct consumer example](../splitter-client/examples/join.rs),
[live tests](../splitter-client/tests/e2e.rs), and
[Go ownership interface](../../splitter/pkg/model/client.go).

The concrete API concern is that `GrantEvent::Assigned`, documented as
observational, carries the same `Arc<Ownership>` used by lifecycle handlers.
A subscriber can call `loader().load()`, `unloader().unload()`, or `release()`.
`release()` immediately expires the shared ownership and asks the driver to
release the grant; it does not verify that the application has stopped. This is
an authority boundary that callers must enforce by convention. No misuse was
found in the inspected callers.

The assignment snapshot has a different purpose: it is the recoverable delivery
mechanism for lifecycle handlers. Making it read-only without another control
delivery mechanism would break direct consumer orchestration. Also, only the map
membership is a snapshot. The shared ownership latches and expiration continue
to change; `assignments.changed()` is not a notification for every lifecycle or
lease update.

The smallest proposed capability change is a cloneable observation handle with
private storage and no access back to `Ownership`, `Loader`, or `Unloader`.
It would expose lifecycle readers and the current expiration. An additive
`Ownership` method could create it so applications can pass a narrower handle
to observers. Using it in broadcast events would enforce their documented
observation role, but changing the existing event field requires an API migration.
An additive view alone does not narrow the existing event API.

The inspected Rust range factories do not establish a need for a separate
range-level revocation capability. The Go `Ownership` interface exposes
`RequestRevoke` to range factories, but API availability alone does not establish
a caller requirement. Keep `request_revoke()` available to lifecycle handlers;
defer changes to factory capabilities until a supported caller or a core
lifecycle invariant justifies them. There is no caller evidence here for
introducing a non-cloneable controller.

Replacement safety already has a concrete mechanism. Each queued `GrantAction`
retains the originating `Arc<Ownership>`, and `Driver::on_action()` compares it
with the current instance before processing any action. The retained `Arc`
keeps the old allocation alive, so allocator address reuse cannot make an old
action match a replacement. Compatible replay preserves identity; replacement
creates a new instance. The dispatcher uses the same identity to reconcile tasks,
and processor cleanup checks range identity before deleting a registry entry.

An explicit local generation could make identity easier to name, log, or carry
outside an ownership handle. It would not strengthen the current comparison by
itself. A generation design would need session scoping, replay stability,
replacement changes, and a wraparound policy. Defer it until a caller needs those
properties. If callers only need to compare assignments, a named comparison
method can hide the pointer convention with less change. Neither approach
provides distributed fencing.

Existing tests cover compatible duplicate assignment, stale release after
replacement, and dispatcher replacement with old-handler cancellation. Before
changing identity or capability delivery, extend coverage to queued stale load,
unload, revoke, and expiration actions. Verify both unchanged replacement state
and absence of outbound messages, with current-instance actions as positive
controls. A narrowed observation API also needs compile-fail checks for all four
controls and runtime checks that retained views observe lease updates and
expiration after replacement.

Validation for the initial observation review:
`cargo test -p splitter-client --all-targets --locked`
passed 142 tests. Six live-server tests were ignored by default and were not run.
The observation recommendations remain unimplemented.

**Implementation discipline**

Before implementing a review candidate, record the concrete problem, current
behavior, intended contract, and smallest useful change. Identify the tests that
distinguish the old and new behavior. Keep scoped changes on separate Graphite
branches, stacking only where dependencies require it. Prefer incremental API
migration over a client rewrite. Reassess proposals when caller evidence does
not support the original concern.
