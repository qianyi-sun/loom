import subprocess
import tempfile
import unittest
from pathlib import Path

from agentic_data_platform.domain.run_records import TerminalTurn
from agentic_data_platform.sandbox.docker_terminal import (
    DockerTerminalSandbox,
    DockerTerminalSandboxConfig,
)


class FakeRunner:
    def __init__(self):
        self.calls = []
        self.behaviors = []

    def add_completed(self, *, returncode=0, stdout="", stderr="", write_files=None):
        self.behaviors.append(
            {
                "type": "completed",
                "returncode": returncode,
                "stdout": stdout,
                "stderr": stderr,
                "write_files": write_files or {},
            }
        )

    def add_timeout(self):
        self.behaviors.append({"type": "timeout"})

    def run(self, args, *, timeout):
        self.calls.append({"args": args, "timeout": timeout})
        behavior = self.behaviors.pop(0)

        if behavior["type"] == "timeout":
            raise subprocess.TimeoutExpired(args, timeout, output="partial out", stderr="partial err")

        workspace = _workspace_from_args(args)
        for relative_path, content in behavior["write_files"].items():
            target = workspace / relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)

        return subprocess.CompletedProcess(
            args=args,
            returncode=behavior["returncode"],
            stdout=behavior["stdout"],
            stderr=behavior["stderr"],
        )


class DockerTerminalSandboxTest(unittest.TestCase):
    def test_successful_command_captures_stdout_and_changed_paths(self):
        runner = FakeRunner()
        runner.add_completed(
            stdout="created output\n",
            write_files={"output.txt": "hello\n"},
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            sandbox = DockerTerminalSandbox(
                DockerTerminalSandboxConfig(
                    run_id="run_001",
                    image="python:3.12-slim",
                    workspace_root=Path(temp_dir),
                    cpu_limit=2,
                    memory_mb=512,
                    pids_limit=128,
                    timeout_seconds=30,
                    internet_access=True,
                ),
                runner=runner,
            )

            result = sandbox.execute("printf hello > output.txt")

        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.stdout, "created output\n")
        self.assertEqual(result.stderr, "")
        self.assertEqual(result.changed_paths, ["output.txt"])
        self.assertFalse(result.timed_out)

        args = runner.calls[0]["args"]
        self.assertEqual(args[:3], ["docker", "run", "--rm"])
        self.assertIn("--cpus", args)
        self.assertIn("--memory", args)
        self.assertIn("python:3.12-slim", args)

    def test_failing_command_captures_exit_code_and_stderr(self):
        runner = FakeRunner()
        runner.add_completed(returncode=7, stdout="", stderr="missing file\n")

        with tempfile.TemporaryDirectory() as temp_dir:
            sandbox = DockerTerminalSandbox(
                DockerTerminalSandboxConfig(
                    run_id="run_002",
                    image="python:3.12-slim",
                    workspace_root=Path(temp_dir),
                    timeout_seconds=30,
                ),
                runner=runner,
            )

            result = sandbox.execute("cat missing.txt")

        self.assertEqual(result.exit_code, 7)
        self.assertEqual(result.stderr, "missing file\n")
        self.assertEqual(result.changed_paths, [])

    def test_timeout_records_limit_metadata(self):
        runner = FakeRunner()
        runner.add_timeout()

        with tempfile.TemporaryDirectory() as temp_dir:
            sandbox = DockerTerminalSandbox(
                DockerTerminalSandboxConfig(
                    run_id="run_003",
                    image="python:3.12-slim",
                    workspace_root=Path(temp_dir),
                    timeout_seconds=1,
                ),
                runner=runner,
            )

            result = sandbox.execute("sleep 60")

        self.assertEqual(result.exit_code, 124)
        self.assertTrue(result.timed_out)
        self.assertEqual(result.metadata["timeout_seconds"], 1)
        self.assertEqual(result.metadata["resource_limits"]["memory_mb"], None)
        self.assertEqual(result.metadata["resource_limits"]["cpu_limit"], None)
        self.assertIn("timed out", result.stderr)

    def test_workspace_snapshot_captures_final_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir)
            workspace = workspace_root / "run_004"
            workspace.mkdir()
            (workspace / "nested").mkdir()
            (workspace / "nested" / "answer.txt").write_text("42\n")

            sandbox = DockerTerminalSandbox(
                DockerTerminalSandboxConfig(
                    run_id="run_004",
                    image="python:3.12-slim",
                    workspace_root=workspace_root,
                    timeout_seconds=30,
                ),
                runner=FakeRunner(),
            )

            snapshot = sandbox.capture_workspace()

        self.assertEqual(snapshot.run_id, "run_004")
        self.assertEqual([item.path for item in snapshot.files], ["nested/answer.txt"])
        self.assertEqual(snapshot.files[0].size_bytes, 3)
        self.assertEqual(len(snapshot.files[0].sha256), 64)

    def test_result_converts_to_terminal_turn(self):
        runner = FakeRunner()
        runner.add_completed(stdout="ok\n", write_files={"done.txt": "ok\n"})

        with tempfile.TemporaryDirectory() as temp_dir:
            sandbox = DockerTerminalSandbox(
                DockerTerminalSandboxConfig(
                    run_id="run_005",
                    image="python:3.12-slim",
                    workspace_root=Path(temp_dir),
                    timeout_seconds=30,
                ),
                runner=runner,
            )

            result = sandbox.execute("touch done.txt")
            turn = result.to_terminal_turn(turn_index=0, model_call_id="call_001")

        self.assertIsInstance(turn, TerminalTurn)
        self.assertEqual(turn.turn_index, 0)
        self.assertEqual(turn.command, "touch done.txt")
        self.assertEqual(turn.cwd, "/workspace")
        self.assertEqual(turn.exit_code, 0)
        self.assertEqual(turn.changed_paths, ["done.txt"])
        self.assertEqual(turn.model_call_id, "call_001")

    def test_network_is_disabled_when_internet_access_is_false(self):
        runner = FakeRunner()
        runner.add_completed()

        with tempfile.TemporaryDirectory() as temp_dir:
            sandbox = DockerTerminalSandbox(
                DockerTerminalSandboxConfig(
                    run_id="run_006",
                    image="python:3.12-slim",
                    workspace_root=Path(temp_dir),
                    internet_access=False,
                    timeout_seconds=30,
                ),
                runner=runner,
            )

            sandbox.execute("python --version")

        args = runner.calls[0]["args"]
        network_index = args.index("--network")
        self.assertEqual(args[network_index + 1], "none")

    def test_docker_volume_can_use_separate_host_workspace_root(self):
        runner = FakeRunner()
        runner.add_completed()

        with tempfile.TemporaryDirectory() as temp_dir:
            sandbox = DockerTerminalSandbox(
                DockerTerminalSandboxConfig(
                    run_id="run_007",
                    image="python:3.12-slim",
                    workspace_root=Path(temp_dir) / "container-workspaces",
                    host_workspace_root=Path("/srv/agentic-data-platform/dev/current/.runtime/sandbox-workspaces"),
                    timeout_seconds=30,
                ),
                runner=runner,
            )

            sandbox.execute("python --version")

        args = runner.calls[0]["args"]
        volume_index = args.index("-v")
        self.assertEqual(
            args[volume_index + 1],
            "/srv/agentic-data-platform/dev/current/.runtime/sandbox-workspaces/run_007:/workspace",
        )


def _workspace_from_args(args):
    volume_index = args.index("-v")
    volume_spec = args[volume_index + 1]
    host_path = volume_spec.split(":", maxsplit=1)[0]
    return Path(host_path)
