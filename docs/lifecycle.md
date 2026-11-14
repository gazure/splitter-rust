# Range lifecycle and session recovery

The dispatcher reconciles current ownership, stops application ranges when their
leases end, and waits for handover during graceful shutdown. This behavior
supports adapters that associate a stateful runtime, such as a Bevy ECS world,
with each grant.

## Assignment delivery

`JoinHandle::assignments` is a watch receiver containing the current assignment
map. Read its initial value before awaiting `changed()`. Reconcile by grant ID
and `Arc<Ownership>` identity: replacing a grant expires the old ownership
instance, even if the ID stays the same.

The dispatcher uses this snapshot and tracks its range tasks in a `JoinSet`.
Assignments received during startup or during bursts larger than the broadcast
buffer remain discoverable. Completed handlers are not restarted by unrelated
snapshot changes. Removing an assignment cancels its handler, and dispatcher
shutdown joins the remaining handlers.

`GrantEvent` broadcasts remain available for observation. They are lossy and do
not replay earlier assignments. Use the snapshot to drive application ownership.
The `join` example demonstrates this pattern.

## Range ordering and release

`Processor` waits for counterpart unload or direct activation before calling
`RangeFactory::build`. Initialization can therefore load application state after
the previous owner has stopped updating it. A rejecting factory still returns
`None` to delegate to the next filter, but rejection now happens after that wait.

Range closure cancels initialization, activation, steady-state ownership, drain,
and counterpart-load waits. Both the `drain()` call and its completion latch are
bounded by ownership expiration. Expiration wins over ready success signals;
revocation wins over activation and counterpart-unload signals. Deadline changes
use a watch channel, including when the coordinator shortens a lease.

When a handler finishes, the client sends `Released` with the latest grant state
and removes the assignment. An old handler cannot remove or release a replacement
instance. Load acknowledgements cannot regress an active grant to allocated, and
duplicate revocation preserves an already-unloaded state.

Direct `ConsumerClient` users must call `Ownership::release()` after stopping all
application work. Use an exit guard so cancellation and failure also release the
grant. `Processor` and `Dispatcher` do this automatically. `request_revoke()` has
a different purpose: it asks the coordinator to begin graceful handover while
the handler continues to honor its existing lease.

Closed ranges remain callable. Application request handlers must return
`ClientError::NotOwned` when the range is closed, including calls through retained
references after registry removal. The proxy delegates this check to the range
on both its active local path and local fallback path.

## Shutdown

`JoinHandle::shutdown()` stops reconnecting and sends `Deregister` when connected.
It continues processing handover and lease expiration until the grant table is
empty. If disconnected, it revokes grants locally and waits for release or expiry
without contacting the coordinator. `ConsumerOptions::with_drain_timeout(duration)`
bounds both paths; the default is 30 seconds. Reaching that deadline force-closes
remaining grants and returns `ClientError::DrainTimeout`.

Dropping a `JoinHandle`, including cancellation of `shutdown()`, aborts the
session driver. Final driver cleanup expires every grant, clears the assignment
and cluster snapshots, and closes the handle's completion signal. Ordinary
session failures enter recovery instead of closing the consumer.

`Dispatcher::drain(timeout)` waits for the session and its range tasks. On timeout,
it cancels and joins remaining dispatcher work and returns
`ClientError::DrainTimeout`. A timeout no longer reports success.

Cancellation is cooperative. Synchronous application code that blocks a runtime
worker cannot be preempted by an async timeout. Ranges must stop side effects when
closed; storage-level fencing is still required for durable external writes.

## Connection lifetime and termination

`ConsumerClient::join()` starts a background driver and returns its handle before
connecting. The handle represents the consumer lifetime; it does not signal
successful registration or readiness. The driver retries initial connection
failures and subsequent session failures with a one-second backoff by default.
RPC rejection, explicit server closure, malformed protocol messages, EOF,
expired sessions, and failed or stalled outbound sends all return to that loop.
Failures are logged; they do not require an application restart loop.

To let the consumer connect when the coordinator starts later, construct the
Tonic channel with `connect_lazy()`. An application's earlier call to
`Channel::connect().await` can still fail before the consumer starts.

Each connection attempt, including receipt of an unexpired `Established` frame,
has a 30-second timeout by default. Set
`ConsumerOptions::with_join_timeout(duration)` to change it. A timed-out attempt
is dropped and retried. This timeout does not limit the consumer's lifetime.
Zero or unrepresentable durations are rejected before the driver starts.

Each `Established` or heartbeat acknowledgement replaces the session deadline
with the server's absolute expiration, converted to a monotonic timer when
received. Acknowledgements can extend or shorten the deadline. Other inbound
messages and outbound heartbeats do not renew it. The deadline covers message
handling and outbound sends, so a blocked send cannot keep an expired session
alive. An outbound send also has its own five-second timeout.

There is no continuous disconnect limit by default. To terminate after a long
outage, configure a disconnect timeout and a drain timeout:

```rust
use std::time::Duration;
use splitter_client::ConsumerOptions;

let options = ConsumerOptions::default()
    .with_disconnect_timeout(Duration::from_secs(60))
    .with_drain_timeout(Duration::from_secs(10));
```

The disconnect timer starts at initial connection or after losing a session.
It includes backoff, connection attempts, and receipt of a valid cluster snapshot.
A handshake or heartbeat alone does not reset it. A valid snapshot, including one
with zero assignments, resets the timer for the next outage. Expiration stops
retries, starts bounded local drain, and ultimately returns
`ClientError::DisconnectTimeout`. The total bound is the disconnect timeout plus
the drain timeout. Explicit shutdown and dropping the handle interrupt recovery.

`ReconnectOptions` controls the retry backoff and optional disconnect `timeout`.
Its default `timeout: None` retries indefinitely. To opt out of recovery entirely,
use `ConsumerOptions::without_reconnect()`. In that mode, `join()` waits for the
initial RPC response and returns connection errors or `ClientError::JoinTimeout`
directly. After it returns a stream, session establishment has a separate
30-second deadline. Subsequent session failures terminate the driver.

## Assignment recovery

Disconnection does not revoke existing grants or extend their leases. Application
handlers can continue using ownership until its existing expiration. The driver
continues processing local load, unload, release, and expiration signals while
waiting to reconnect. Local completion does not depend on an outbound report to
the coordinator. At the hard lease deadline, the processor closes the range and
releases it locally. Graceful `Range::drain()` runs on revocation while lease time
remains; expiry does not grant additional ownership time.

Registration replays non-revoked grants whose leases remain valid. The existing
range and ownership identity survive compatible reassignment. Unconfirmed grants
keep their old deadlines and do not receive lease extensions from the new
session. Confirmed replay resends locally advanced load state and outstanding
revoke requests. Repeated revocation resends unload state. Expired ownership is
never revived; a later assignment creates a new ownership instance.

The routing map survives reconnect so a partial coordinator snapshot can retain
valid peer routes. Like the Go client, the Rust client keeps omitted grants only
while their shards remain present and their states are compatible with the new
assignments. An allocated or loaded grant can coexist with a revoked or unloaded
grant for the same shard. New assignments and state updates take precedence over
retained grants, so activation removes a conflicting former owner. Snapshots drop
omitted consumers without surviving grants; incremental changes keep consumers
until explicitly removed. Final consumer termination clears the routing map.
Local offline drain also clears stale routes.

Cluster deltas must match the current coordinator and the next version. Invalid
assignments, unknown updates, and invalid state transitions leave the previous map
unchanged. Grants and consumers retained from an older snapshot must be reported
by the current coordinator before they can be updated or explicitly removed.
An invalid cluster message triggers recovery; the optional disconnect timeout
includes receiving a valid replacement snapshot. Without recovery, the session
closes with `ClientError::InvalidCluster`. Deltas cannot resume before a snapshot.

## Request retry deadlines

The domain resolver uses Go's default routing priority: active, revoked, loaded,
then unloaded. Unknown ownership returns `NotOwned`. A remote peer missing from
the connection pool returns retryable `Unavailable`. Only a local owner returns
`NoResolution`, which permits `handle` to fall back to a non-active local range.

`retry_ownership` and `handle_with_retry` include both attempts and backoff in
their timeout budget. If an in-flight attempt reaches the deadline, the helper
drops its future and returns `ClientError::RetryTimeout`, which maps to gRPC
`DeadlineExceeded`. A zero timeout returns that error without invoking the operation.
If the budget expires during backoff, the helper returns the last ownership error
without starting another attempt.

Cancellation requires the operation to yield. Dropping an RPC future does not
undo effects that have already reached a peer.

## Compatibility and validation

The `session` and `wrap` modules are internal. Use `From` and `TryFrom` to convert
between protocol messages and client types. `RecordingRange` is shared source
for the repository's examples and tests; neither `splitter_client::RecordingRange`
nor `splitter_client::processor::RecordingRange` is exported. Applications
implement `Range` for their own state.

`Processor::grants()` returns a cloneable `GrantMapReader` with `get`, `ids`,
`len`, and `is_empty` methods. Readers observe the processor's live registry but
cannot insert, transition, or remove its entries. Retrieved range handles keep
their existing lifecycle responsibilities. Custom processors can still own a
mutable `GrantMap`.

`Range` and `RangeFactory` signatures are unchanged. Direct `ConsumerOptions`
struct literals need `reconnect`, `join_timeout`, and `drain_timeout`, or
`..Default::default()`. `ReconnectOptions::timeout` is now `Option<Duration>`:
use `Some(duration)` for a disconnect limit or `None` for unlimited recovery.
Construct cluster maps with `ClusterMap::empty()` and validated `apply()` updates.
Read metadata and routing entries through `id()`, `consumers()`, `grants()`, and
`shards()`. All snapshot fields are private, including retention metadata, so
callers cannot edit relationships or advance versions outside validation.
Dropping a Join handle now cancels its driver, so retain the handle for the desired
session lifetime. The lockfile updates Tokio to 1.53.1 and `async-trait` to 0.1.92;
the latter avoids a generated `double_must_use` lint on the installed nightly.
The existing `result_large_err` Clippy allowance is unchanged.

The deterministic tests use in-memory protocol streams and cover startup bursts,
assignment replacement, session cleanup, cancelled shutdown, lease changes,
handover acknowledgements, release, reconnect replay, and recovery deadlines.
Paused-time tests also exercise silent sessions, heartbeat renewal and shortening,
blocked outbound sends, and recovery after session expiration.
The two-consumer protocol test checks that the new range is built only after the
old range drains and that the old grant is released after counterpart load.

Run the client suite with:

```sh
cargo test -p splitter-client --all-targets --locked
cargo +nightly clippy --workspace --all-targets --locked -- -D warnings
```

Use the checked-in `rustfmt.toml` with nightly rustfmt. The live tests in
`splitter-client/tests/e2e.rs` are explicitly ignored by default and contain
setup instructions. When selected with `--ignored`, missing configuration fails
instead of silently passing. They include a two-consumer rebalance and shutdown
scenario. In-memory tests do not replace that live-server validation or establish
application-state durability.

Run `python3 tools/live_splitter.py` to build and provision a disposable Docker
server for the live suite. See [Live Splitter tests](live-testing.md) for
requirements, diagnostics, and the limits of the single-node deployment.
