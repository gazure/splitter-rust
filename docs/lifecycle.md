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

## Shutdown

`JoinHandle::shutdown()` sends `Deregister`, continues processing handover and
lease expiration, and closes the session after its grant table becomes empty.
It can wait until lease expiration if direct consumer code never releases its
grants. Use a timeout or `Dispatcher::drain(timeout)` to bound the wait.

Dropping a `JoinHandle`, including cancellation of `shutdown()`, aborts the
session driver. Driver cleanup expires every grant, clears the assignment and
cluster snapshots, and closes the handle's completion signal. This also happens
on server session closure, transport errors, and driver cancellation.

`Dispatcher::drain(timeout)` waits for the session and its range tasks. On timeout,
it cancels and joins remaining dispatcher work and returns
`ClientError::DrainTimeout`. A timeout no longer reports success.

Cancellation is cooperative. Synchronous application code that blocks a runtime
worker cannot be preempted by an async timeout. Ranges must stop side effects when
closed; storage-level fencing is still required for durable external writes.

## Optional reconnect

Reconnect is disabled by default. Enable bounded recovery for transient stream
failures or EOF with these options:

```rust
use std::time::Duration;
use splitter_client::{ConsumerOptions, ReconnectOptions};

let options = ConsumerOptions::default().with_reconnect(ReconnectOptions {
    timeout: Duration::from_secs(30),
    backoff: Duration::from_secs(1),
});
```

The timeout includes connection attempts and the server's `Established` response.
A silent or repeatedly failing establishment cannot reset the deadline. Shutdown
interrupts recovery. Explicit session closure and permanent RPC errors remain
terminal; an initial `join()` connection error still returns to its caller.

Registration replays non-revoked grants whose leases remain valid. The existing
range and ownership identity survive compatible reassignment. Unconfirmed grants
keep their old deadlines and do not receive lease extensions from the new
session. Confirmed replay resends locally advanced load state and outstanding
revoke requests. Repeated revocation resends unload state. Expired ownership is
never revived; a later assignment creates a new ownership instance.

An outbound stream that remains blocked for five seconds terminates the session.
Automatic recovery currently applies to inbound transient failures and EOF;
callers still need to supervise terminal session failures.

## Compatibility and validation

`Range` and `RangeFactory` signatures are unchanged. Direct `ConsumerOptions`
struct literals need the new `reconnect` field, or `..Default::default()`.
Dropping a Join handle now cancels its driver, so retain the handle for the desired
session lifetime. The lockfile updates Tokio to 1.53.1 and `async-trait` to 0.1.92;
the latter avoids a generated `double_must_use` lint on the installed nightly.
The existing `result_large_err` Clippy allowance is unchanged.

The deterministic tests use in-memory protocol streams and cover startup bursts,
assignment replacement, session cleanup, cancelled shutdown, lease changes,
handover acknowledgements, release, reconnect replay, and recovery deadlines.
The two-consumer protocol test checks that the new range is built only after the
old range drains and that the old grant is released after counterpart load.

Run the client suite with:

```sh
cargo test -p splitter-client --all-targets --locked
cargo +nightly clippy --workspace --all-targets --locked -- -D warnings
```

Use the checked-in `rustfmt.toml` with nightly rustfmt. In-memory tests do not
replace live-server validation or establish application-state durability.
