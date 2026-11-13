"""Check runner cleanup and failure propagation without requiring Docker."""

import argparse
import contextlib
import io
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import live_splitter


class HarnessTests(unittest.TestCase):
    def exercise(self, *, cargo_error=None, running=True, keep=False):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        calls = []

        def run(command, **kwargs):
            calls.append((command, kwargs))
            output = ""
            if command[:2] == ["docker", "inspect"]:
                output = "true" if running else "false"
            elif command[:2] == ["docker", "port"]:
                output = "127.0.0.1:49152\n"
            elif command[:2] == ["docker", "logs"]:
                output = "server log\n"
            elif command[:2] == ["cargo", "test"] and cargo_error:
                raise cargo_error
            return subprocess.CompletedProcess(command, 0, output, "")

        args = argparse.Namespace(
            image="splitter:test", source=root, offline=True,
            startup_timeout=1, test_timeout=10, keep_on_failure=keep,
        )
        with patch.object(live_splitter, "ROOT", root), \
                patch.object(live_splitter, "run", side_effect=run), \
                contextlib.redirect_stdout(io.StringIO()):
            error = None
            try:
                live_splitter.exercise(args)
            except (RuntimeError, subprocess.SubprocessError) as caught:
                error = caught
        logs = list(root.glob("target/live-splitter/*/server.log"))
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0].read_text(), "server log\n")
        return calls, error

    def test_success_provisions_both_services_and_cleans_up(self):
        calls, error = self.exercise()
        self.assertIsNone(error)
        command, options = next(call for call in calls if call[0][:2] == ["cargo", "test"])
        self.assertIn("--ignored", command)
        self.assertIn("--lib", command)
        self.assertIn("--test-threads=1", command)
        self.assertEqual(options["env"]["SPLITTER_ENDPOINT"], "http://127.0.0.1:49152")
        self.assertEqual(options["env"]["SPLITTER_UNIT_SERVICE"], "rust-client/unit")
        self.assertTrue(any(c[-3:] == ["new", "unit", "rust-client/unit/leader"] for c, _ in calls))
        self.assertEqual(calls[-1][0][:4], ["docker", "rm", "--force", "--volumes"])

    def test_failed_tests_propagate_and_cleanup(self):
        failure = subprocess.CalledProcessError(101, ["cargo", "test"])
        calls, error = self.exercise(cargo_error=failure)
        self.assertIs(error, failure)
        self.assertEqual(calls[-1][0][:2], ["docker", "rm"])

    def test_test_timeout_propagates_and_cleans_up(self):
        failure = subprocess.TimeoutExpired(["cargo", "test"], 10)
        calls, error = self.exercise(cargo_error=failure)
        self.assertIs(error, failure)
        self.assertEqual(calls[-1][0][:2], ["docker", "rm"])

    def test_early_server_exit_cleans_up_without_running_tests(self):
        calls, error = self.exercise(running=False)
        self.assertIsInstance(error, RuntimeError)
        self.assertFalse(any(c[:2] == ["cargo", "test"] for c, _ in calls))
        self.assertEqual(calls[-1][0][:2], ["docker", "rm"])

    def test_keep_on_failure_retains_container_and_logs(self):
        calls, error = self.exercise(running=False, keep=True)
        self.assertIsInstance(error, RuntimeError)
        self.assertFalse(any(c[:2] == ["docker", "rm"] for c, _ in calls))


if __name__ == "__main__":
    unittest.main()
