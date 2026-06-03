import subprocess
import tempfile
import unittest
from pathlib import Path

from agentic_data_platform.domain.run_records import TerminalTurn
from agentic_data_platform.sandbox.docker_terminal import (
    DockerOwnedContainerCleaner,
    DockerTerminalSandbox,
    DockerTerminalSandboxConfig,
)


class FakeRunner:
    def __init__(self):
        self.calls = []
        self.behaviors = []

    def add_completed(self, *, returncode=0, stdout="", stderr="", write_files=None, write_cidfile=None):
        self.behaviors.append(
            {
                "type": "completed",
                "returncode": returncode,
                "stdout": stdout,
                "stderr": stderr,
                "write_files": write_files or {},
                "write_cidfile": write_cidfile,
            }
        )

    def add_timeout(self):
        self.behaviors.append({"type": "timeout"})

    def run(self, args, *, timeout):
        self.calls.append({"args": args, "timeout": timeout})
        behavior = self.behaviors.pop(0)

        if behavior["type"] == "timeout":
            raise subprocess.TimeoutExpired(args, timeout, output="partial out", stderr="partial err")

        if "-v" in args:
            workspace = _workspace_from_args(args)
            for relative_path, content in behavior["write_files"].items():
                target = workspace / relative_path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(content)
        if behavior.get("write_cidfile"):
            cidfile = _cidfile_from_args(args)
            cidfile.parent.mkdir(parents=True, exist_ok=True)
            cidfile.write_text(str(behavior["write_cidfile"]))

        return subprocess.CompletedProcess(
            args=args,
            returncode=behavior["returncode"],
            stdout=behavior["stdout"],
            stderr=behavior["stderr"],
        )


class FakeLifecycleRecorder:
    def __init__(self):
        self.events = []

    def container_started(self, metadata):
        self.events.append(("started", metadata))

    def container_completed(self, metadata):
        self.events.append(("completed", metadata))

    def resource_sampled(self, metadata):
        self.events.append(("resource_sampled", metadata))


class FakeManagedRunner:
    def __init__(self, *, stdout="", stderr="", stats_stdout="", write_cidfile=None, write_files=None):
        self.stdout = stdout
        self.stderr = stderr
        self.stats_stdout = stats_stdout
        self.write_cidfile = write_cidfile
        self.write_files = write_files or {}
        self.calls = []

    def start(self, args, *, env=None):
        self.calls.append({"method": "start", "args": args, "env": env})
        workspace = _workspace_from_args(args)
        for relative_path, content in self.write_files.items():
            target = workspace / relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
        if self.write_cidfile:
            cidfile = _cidfile_from_args(args)
            cidfile.parent.mkdir(parents=True, exist_ok=True)
            cidfile.write_text(str(self.write_cidfile))
        return FakeManagedProcess(stdout=self.stdout, stderr=self.stderr)

    def run(self, args, *, timeout, env=None):
        self.calls.append({"method": "run", "args": args, "timeout": timeout, "env": env})
        return subprocess.CompletedProcess(args=args, returncode=0, stdout=self.stats_stdout, stderr="")


class FakeManagedProcess:
    def __init__(self, *, stdout="", stderr="", returncode=0):
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = None
        self._final_returncode = returncode
        self.killed = False

    def poll(self):
        return self.returncode

    def communicate(self, timeout=None):
        self.returncode = self._final_returncode
        return self.stdout, self.stderr

    def kill(self):
        self.killed = True
        self.returncode = -9


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

    def test_docker_run_labels_owned_container_for_cleanup(self):
        runner = FakeRunner()
        runner.add_completed()

        with tempfile.TemporaryDirectory() as temp_dir:
            sandbox = DockerTerminalSandbox(
                DockerTerminalSandboxConfig(
                    run_id="run_008",
                    attempt_id="run_008:attempt:1",
                    image="python:3.12-slim",
                    workspace_root=Path(temp_dir),
                    timeout_seconds=30,
                ),
                runner=runner,
            )

            sandbox.execute("python --version")

        args = runner.calls[0]["args"]
        labels = _labels_from_args(args)
        self.assertEqual(labels["com.agentic-data-platform.managed"], "true")
        self.assertEqual(labels["com.agentic-data-platform.run_id"], "run_008")
        self.assertEqual(labels["com.agentic-data-platform.attempt_id"], "run_008:attempt:1")
        self.assertEqual(labels["com.agentic-data-platform.resource"], "sandbox-container")

    def test_docker_command_records_resource_sample_from_container_stats(self):
        runner = FakeManagedRunner(
            stdout="ok\n",
            write_cidfile="container-abc123\n",
            stats_stdout=(
                '{"CPUPerc":"12.34%","MemUsage":"45.5MiB / 512MiB",'
                '"MemPerc":"8.89%","NetIO":"1.2kB / 3.4kB","BlockIO":"5.6MB / 7.8MB","PIDs":"4"}\n'
            ),
        )
        recorder = FakeLifecycleRecorder()

        with tempfile.TemporaryDirectory() as temp_dir:
            sandbox = DockerTerminalSandbox(
                DockerTerminalSandboxConfig(
                    run_id="run_011",
                    attempt_id="run_011:attempt:1",
                    image="python:3.12-slim",
                    workspace_root=Path(temp_dir),
                    cpu_limit=2,
                    memory_mb=512,
                    timeout_seconds=30,
                ),
                runner=runner,
                lifecycle_recorder=recorder,
            )

            result = sandbox.execute("python --version")

        self.assertEqual(result.metadata["resource_sample"]["sample_status"], "completed")
        self.assertEqual(result.metadata["resource_sample"]["cpu_percent"], 12.34)
        self.assertEqual(result.metadata["resource_sample"]["memory_used_bytes"], 47710208)
        self.assertEqual(result.metadata["resource_sample"]["memory_limit_bytes"], 536870912)
        stats_args = runner.calls[1]["args"]
        self.assertEqual(stats_args, ["docker", "stats", "--no-stream", "--format", "json", "container-abc123"])
        sampled_event = next(event for event in recorder.events if event[0] == "resource_sampled")
        self.assertEqual(sampled_event[1]["sandbox_command_index"], 0)
        self.assertEqual(sampled_event[1]["sandbox_status"], "running")
        self.assertEqual(sampled_event[1]["container_id"], "container-abc123")
        self.assertEqual(sampled_event[1]["cpu_percent"], 12.34)
        self.assertEqual(sampled_event[1]["memory_percent"], 8.89)
        self.assertEqual(sampled_event[1]["pids"], 4)

    def test_docker_owned_container_cleaner_lists_and_removes_only_matching_run(self):
        runner = FakeDockerCleanupRunner(
            ps_stdout="container-one\ncontainer-two\n",
            rm_stdout="container-one\ncontainer-two\n",
        )
        cleaner = DockerOwnedContainerCleaner(runner=runner)

        result = cleaner.cleanup_run(run_id="run_009")

        self.assertEqual(result.container_ids, ["container-one", "container-two"])
        self.assertEqual(result.removed_container_ids, ["container-one", "container-two"])
        self.assertEqual(result.removal_exit_code, 0)
        ps_args = runner.calls[0]["args"]
        self.assertEqual(ps_args[:3], ["docker", "ps", "-aq"])
        self.assertIn("label=com.agentic-data-platform.managed=true", ps_args)
        self.assertIn("label=com.agentic-data-platform.run_id=run_009", ps_args)
        rm_args = runner.calls[1]["args"]
        self.assertEqual(rm_args[:3], ["docker", "rm", "-f"])
        self.assertEqual(rm_args[3:], ["container-one", "container-two"])

    def test_docker_owned_container_cleaner_can_target_attempt(self):
        runner = FakeDockerCleanupRunner(ps_stdout="")
        cleaner = DockerOwnedContainerCleaner(runner=runner)

        result = cleaner.cleanup_run(run_id="run_010", attempt_id="run_010:attempt:2")

        self.assertEqual(result.container_ids, [])
        self.assertEqual(result.removed_container_ids, [])
        self.assertEqual(result.removal_exit_code, None)
        ps_args = runner.calls[0]["args"]
        self.assertIn("label=com.agentic-data-platform.run_id=run_010", ps_args)
        self.assertIn("label=com.agentic-data-platform.attempt_id=run_010:attempt:2", ps_args)


def _workspace_from_args(args):
    volume_index = args.index("-v")
    volume_spec = args[volume_index + 1]
    host_path = volume_spec.split(":", maxsplit=1)[0]
    return Path(host_path)


def _cidfile_from_args(args):
    cidfile_index = args.index("--cidfile")
    return Path(args[cidfile_index + 1])


def _labels_from_args(args):
    labels = {}
    for index, value in enumerate(args):
        if value != "--label":
            continue
        label = args[index + 1]
        key, label_value = label.split("=", maxsplit=1)
        labels[key] = label_value
    return labels


class FakeDockerCleanupRunner:
    def __init__(self, *, ps_stdout="", rm_stdout="", ps_returncode=0, rm_returncode=0):
        self.calls = []
        self.ps_stdout = ps_stdout
        self.rm_stdout = rm_stdout
        self.ps_returncode = ps_returncode
        self.rm_returncode = rm_returncode

    def run(self, args, *, timeout):
        self.calls.append({"args": args, "timeout": timeout})
        if args[:3] == ["docker", "ps", "-aq"]:
            return subprocess.CompletedProcess(
                args=args,
                returncode=self.ps_returncode,
                stdout=self.ps_stdout,
                stderr="",
            )
        if args[:3] == ["docker", "rm", "-f"]:
            return subprocess.CompletedProcess(
                args=args,
                returncode=self.rm_returncode,
                stdout=self.rm_stdout,
                stderr="",
            )
        raise AssertionError(f"unexpected docker cleanup command: {args}")
