#!/usr/bin/env python3
"""Build a disposable Splitter server and run the Rust live integration tests."""

import argparse
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import time
import uuid


ROOT = Path(__file__).resolve().parents[1]


def run(command, *, timeout=30, check=True, capture=True, **kwargs):
    print(f"+ {shlex.join(str(part) for part in command)}", flush=True)
    with subprocess.Popen(
        command, text=True, start_new_session=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None, **kwargs,
    ) as process:
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            # Cargo can leave test executables running if only Cargo is killed.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.communicate()
            raise
        result = subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
        if check:
            result.check_returncode()
        return result


def ctl(container, *arguments, check=True):
    # The multi-endpoint CLI path bounds the RPC as well as connection setup.
    return run([
        "docker", "exec", container, "/usr/local/bin/splitterctl",
        "--insecure", "--endpoints=127.0.0.1:50051", "--dial-timeout=3s",
        *arguments,
    ], timeout=10, check=check)


def wait_ready(container, timeout):
    deadline = time.monotonic() + timeout
    last_error = "no response"
    while time.monotonic() < deadline:
        state = run(["docker", "inspect", "--format", "{{.State.Running}}", container])
        if state.stdout.strip() != "true":
            raise RuntimeError("Splitter exited before becoming ready")
        result = ctl(container, "tenants", "list", check=False)
        if result.returncode == 0:
            return
        last_error = result.stderr or result.stdout
        time.sleep(0.5)
    raise RuntimeError(f"Splitter management API did not become ready: {last_error}")


def provision(container):
    ctl(container, "tenants", "new", "rust-client")
    ctl(container, "services", "new", "rust-client/ranges", "region")
    ctl(container, "domains", "new", "global", "rust-client/ranges/entities", "--shards=4")
    ctl(container, "services", "new", "rust-client/unit", "region")
    ctl(container, "domains", "new", "unit", "rust-client/unit/leader")


def exercise(args):
    run(["docker", "info", "--format", "{{.ServerVersion}}"])
    run(["cargo", "--version"])
    image = args.image
    if image is None:
        source = args.source.resolve()
        if not (source / "cmd/splitter/main.go").is_file():
            raise RuntimeError(f"Splitter source not found at {source}; set --source or --image")
        image = "splitter-rust-test:local"
        run([
            "docker", "build", "--file", str(ROOT / "testing/splitter/Dockerfile"),
            "--tag", image, str(source),
        ], timeout=900, capture=False)

    container = f"splitter-rust-test-{uuid.uuid4().hex[:12]}"
    artifacts = ROOT / "target/live-splitter" / container
    artifacts.mkdir(parents=True)
    print(f"Artifacts: {artifacts}", flush=True)
    succeeded = False
    try:
        run([
            "docker", "run", "--detach", "--name", container,
            "--hostname", "splitter.test", "--env", "APP_REGION=region",
            "--publish", "127.0.0.1::50051", "--tmpfs", "/data:rw,size=256m",
            "--entrypoint", "/usr/local/bin/splitter", image, "start",
            "--data_path=/data", "--raft_id=splitter",
            "--raft_server=splitter.test:50053", "--raft_join_peers=splitter.test:50052",
            "--splitter_server=splitter.test:50052",
            "--fast_activation", "--eager_assignment", "--raft_fast_bootstrap",
        ], timeout=120)
        wait_ready(container, args.startup_timeout)
        address = run(["docker", "port", container, "50051/tcp"]).stdout.strip()
        if not address.startswith("127.0.0.1:") or "\n" in address:
            raise RuntimeError(f"Unexpected published endpoint: {address!r}")
        provision(container)
        settings = {
            "SPLITTER_ENDPOINT": f"http://{address}",
            "SPLITTER_SERVICE": "rust-client/ranges",
            "SPLITTER_UNIT_SERVICE": "rust-client/unit",
            "SPLITTER_REGION": "region",
            "SPLITTER_SHARDS": "4",
        }
        (artifacts / "environment.json").write_text(json.dumps(settings, indent=2) + "\n")
        command = ["cargo", "test", "-p", "splitter-client", "--lib", "--test", "e2e", "--locked"]
        if args.offline:
            command.append("--offline")
        command.extend(["--", "--ignored", "--test-threads=1", "--nocapture"])
        run(command, cwd=ROOT, env={**os.environ, **settings},
            timeout=args.test_timeout, capture=False)
        succeeded = True
    finally:
        # Capture logs even after a failed startup or interrupted test run.
        try:
            logs = run(["docker", "logs", "--timestamps", container], check=False)
            (artifacts / "server.log").write_text(logs.stdout + logs.stderr)
        except (OSError, subprocess.SubprocessError) as error:
            print(f"Could not collect server logs: {error}", file=sys.stderr)
        if args.keep_on_failure and not succeeded:
            print(f"Container retained: {container}\nRemove with: docker rm -f {container}")
        else:
            try:
                result = run(["docker", "rm", "--force", "--volumes", container], check=False)
                if result.returncode:
                    print(result.stderr, file=sys.stderr)
            except (OSError, subprocess.SubprocessError) as error:
                print(f"Cleanup failed; remove {container} manually: {error}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT.parent / "splitter",
                        help="Go Splitter checkout used to build the image (default: ../splitter)")
    parser.add_argument("--image", help="Use an existing image with splitter and splitterctl; skip build")
    parser.add_argument("--offline", action="store_true", help="Run Cargo offline (not the Docker build)")
    parser.add_argument("--keep-on-failure", action="store_true", help="Keep a failed server for inspection")
    parser.add_argument("--startup-timeout", type=float, default=90, help="Readiness timeout in seconds")
    parser.add_argument("--test-timeout", type=float, default=600, help="Cargo test timeout in seconds")
    args = parser.parse_args()
    if args.startup_timeout <= 0 or args.test_timeout <= 0:
        parser.error("timeouts must be positive")
    try:
        exercise(args)
    except subprocess.CalledProcessError as error:
        print(error.stderr or str(error), file=sys.stderr)
        return 1
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(str(error), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
