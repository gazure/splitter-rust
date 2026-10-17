# Live Splitter tests

Run the Rust client against a disposable, single-node Go Splitter server:

```sh
python3 tools/live_splitter.py
```

Run this command from the `splitter-rust` checkout on macOS or Linux. It requires Python 3, Docker
with a running local daemon, the Rust build prerequisites (including `protoc`),
and the sibling `../splitter` checkout. Go builds inside Docker; the image pins
Go 1.26.2 to match the server's `go.mod`. The first build requires network access
for the Go image and dependencies. Cargo also downloads missing dependencies.

The harness builds both `splitter` and `splitterctl` from the same source tree.
It starts one Raft node with fast bootstrap and activation, temporary storage,
and a unique container name. Docker assigns an available host port, bound to
loopback. See Docker's [port publishing documentation](https://docs.docker.com/engine/network/port-publishing/).

Readiness requires a successful management API request, not just an open TCP
port. The harness then creates these isolated fixtures:

| Service | Domain | Purpose |
| --- | --- | --- |
| `rust-client/ranges` | `entities`, GLOBAL, four shards | Join, shutdown, and two-consumer handover |
| `rust-client/unit` | `leader`, UNIT | Routing from a real server snapshot |

The five live tests run serially on the host against the published gRPC endpoint.
They cover initial cluster delivery, missing-service rejection, shutdown,
two-consumer rebalance and drain, and UNIT-domain lookup. Ordinary `cargo test`
keeps these tests ignored; explicitly running them without configuration fails.

## Options and diagnostics

Use another source checkout or an existing image:

```sh
python3 tools/live_splitter.py --source /path/to/splitter
python3 tools/live_splitter.py --image atoms.co/splitter:latest
```

The existing image must provide `/usr/local/bin/splitter` and
`/usr/local/bin/splitterctl` with the current server flags. The server repository's
`bazel run //:load` target produces that image. No custom Testcontainers module
is required for this harness; Docker lifecycle management lives in the runner.

Use `--offline` to prevent Cargo downloads; this does not disable Docker build
downloads. `--startup-timeout` bounds readiness polling, and `--test-timeout`
bounds the Cargo build and test process. Individual management requests also
have deadlines.

The runner captures server logs in `target/live-splitter/<container>/server.log`
and records test environment settings in `environment.json` after provisioning.
It removes its container on completion, failure, or Ctrl-C. To retain a failed
server for inspection:

```sh
python3 tools/live_splitter.py --keep-on-failure
```

The runner prints the retained container name and its removal command. Storage
is a container-local tmpfs and disappears when the container stops. The harness
requires a local Docker daemon because Rust tests connect through host loopback.

## Scope

This deployment exercises the real Go coordinator, Raft storage, gRPC transport,
and Rust lifecycle handlers. It does not test multi-node Raft failover, network
partitions, application-state durability, or application RPCs between consumers.
The deterministic in-memory tests remain responsible for scripted lifecycle
races and transport failures.

To use an existing server instead, follow the provisioning commands at the top
of [e2e.rs](../splitter-client/tests/e2e.rs). Use dedicated services with no other
consumers attached.

Runner tests exercise provisioning, failure propagation, log capture, and cleanup
with a simulated Docker CLI:

```sh
python3 -B -m unittest discover -s tools -p 'test_live_splitter.py'
```

These runner tests do not validate the Docker image or server bootstrap.
