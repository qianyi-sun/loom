from __future__ import annotations

import os
import signal
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path

import pytest
import scripts.ops.personal_dev_native_builder_conformance as conformance
from scripts.ops.personal_dev_native_builder_conformance import (
    CommandResult,
    ConformanceError,
    ConformanceInputs,
    SubprocessRunner,
    run_conformance,
)

BUILDER = "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "a" * 64
AGENT = "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:" + "b" * 64
PUBLIC_HTTPS = "https://objects.example"
NATIVE_ENDPOINT = "unix:///run/loom-personal-dev-builder/docker.sock"
PRIMARY_ENDPOINT = "unix:///var/run/docker.sock"
MANAGED_LABEL = "loom.personal-dev-native-builder.managed=true"
BUILDKIT_ID = "1" * 64
CLIENT_ID = "2" * 64
DENIAL_ID = "3" * 64
FOREIGN_ID = "4" * 64
PROVIDER_NETWORK_ID = "5" * 64
DENIED_NETWORK_ID = "6" * 64
HOST_DENIAL_PROGRAM = """import socket,sys
connection=socket.socket()
connection.settimeout(2)
try:
    connection.connect((sys.argv[1],1234))
except OSError:
    raise SystemExit(0)
finally:
    connection.close()
raise SystemExit(1)
"""
FOREIGN_DENIAL_PROGRAM = """import socket,sys
try:
    connection=socket.create_connection((sys.argv[1],1234),timeout=2)
except OSError:
    raise SystemExit(0)
connection.close()
raise SystemExit(1)
"""
CLIENT_PROGRAM = """import platform,socket,sys,urllib.error,urllib.request
if not __import__('os').path.exists('/proc/gvisor/kernel_is_gvisor'):
    raise SystemExit(1)
if platform.machine() != 'aarch64':
    raise SystemExit(1)
try:
    response=urllib.request.urlopen(sys.argv[1],timeout=10)
except urllib.error.HTTPError as error:
    if not 400 <= error.code < 500:
        raise
    error.close()
else:
    response.close()
for target in (('192.168.50.103',6443),('172.28.1.10',1234)):
    connection=socket.socket()
    connection.settimeout(2)
    try:
        connection.connect(target)
    except OSError:
        pass
    else:
        raise SystemExit(1)
    finally:
        connection.close()
__import__('os').execvp('buildctl',('buildctl','--addr','tcp://buildkit-0123456789ab:1234','debug','workers'))
"""


@dataclass
class RecordingDockerRunner:
    """A Docker boundary fake that returns only reviewed, public probe facts."""

    fail_at: int | None = None
    fail_cleanup: bool = False
    duplicate_client_id: bool = False
    platform: str = "linux/arm64"
    managed_containers_after: str = ""
    managed_networks_after: str = ""
    invalid_create_name: str | None = None

    def __post_init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.environments: list[dict[str, str]] = []
        self.created: list[str] = []
        self._operation_count = 0
        self._nanocpus_inspections = 0
        self._failed_call: tuple[str, ...] | None = None

    def run(
        self,
        argv: Sequence[str],
        *,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        call = tuple(argv)
        self.calls.append(call)
        self.environments.append({} if env is None else dict(env))
        is_cleanup = "rm" in call or ("network" in call and "rm" in call)
        if is_cleanup and self.fail_cleanup:
            return CommandResult(1, stderr="cleanup failed")
        is_preexisting_check = "inspect" in call and "--format" not in call
        is_emptiness_check = call[-2:] in (("ps", "-aq"), ("ls", "-q"))
        if self._failed_call == call:
            return CommandResult(1, stderr="primary failed")
        if not is_cleanup and not is_preexisting_check and not is_emptiness_check:
            self._operation_count += 1
            if self.fail_at == self._operation_count:
                self._failed_call = call
                return CommandResult(1, stderr="primary failed")
        if is_preexisting_check:
            return CommandResult(1)
        if call[3:5] == ("ps", "-aq"):
            return CommandResult(0, self.managed_containers_after)
        if call[3:6] == ("network", "ls", "-q"):
            return CommandResult(0, self.managed_networks_after)
        if call[-2:] == ("network", "create"):
            raise AssertionError("network create must have its fixed arguments")
        if "network" in call and "create" in call:
            if self.invalid_create_name == call[-1]:
                return CommandResult(0, "not-an-object-id\n")
            identifier = (
                PROVIDER_NETWORK_ID
                if call[-1] == "loom-native-conformance"
                else DENIED_NETWORK_ID
            )
            self.created.append(identifier)
            return CommandResult(
                0,
                identifier + "\n",
            )
        if "create" in call:
            names = {
                "loom-native-conformance-buildkit": BUILDKIT_ID,
                "loom-native-conformance-client": (
                    BUILDKIT_ID if self.duplicate_client_id else CLIENT_ID
                ),
                "loom-native-conformance-denial-target": DENIAL_ID,
                "loom-native-conformance-foreign-client": FOREIGN_ID,
            }
            name = call[call.index("--name") + 1]
            identifier = names[name]
            if self.invalid_create_name == name:
                return CommandResult(0, "not-an-object-id\n")
            self.created.append(identifier)
            return CommandResult(0, identifier + "\n")
        if "logs" in call:
            return CommandResult(0, "loom-buildkitd-native-child-preflight nnp=1\n")
        if "inspect" in call:
            template = call[call.index("--format") + 1]
            if template == "{{.HostConfig.NanoCpus}}" and self.duplicate_client_id:
                self._nanocpus_inspections += 1
                return CommandResult(0, "3000000000\n" if self._nanocpus_inspections == 1 else "1000000000\n")
            values = {
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}": "172.28.0.2\n",
                "{{.State.ExitCode}}": "0\n",
                "{{.HostConfig.Runtime}}": "runsc-personal-dev-native\n",
                "{{.Architecture}}": "arm64\n",
                "{{.Os}}/{{.Architecture}}": self.platform + "\n",
                "{{.HostConfig.CgroupParent}}": "loom-personal-dev-builder.slice\n",
                "{{.HostConfig.NanoCpus}}": (
                    "3000000000\n"
                    if call[-1] == BUILDKIT_ID
                    else "1000000000\n"
                ),
                "{{.HostConfig.Memory}}": "17179869184\n",
                "{{json .HostConfig.Devices}}": "[]\n",
                "{{json .HostConfig.Binds}}": "null\n",
            }
            return CommandResult(0, values[template])
        return CommandResult(0)

def _inputs() -> ConformanceInputs:
    return ConformanceInputs(BUILDER, AGENT, PUBLIC_HTTPS)


def test_runs_fixed_two_sandbox_conformance_without_a_shell() -> None:
    """Catches widening the root executor into a shell or non-dedicated Docker command surface."""
    runner = RecordingDockerRunner()

    receipt = run_conformance(_inputs(), runner)

    assert receipt == {
        "schema": "loom-personal-dev-native-builder-conformance-v1",
        "status": "passed",
        "runtime": "runsc-personal-dev-native",
        "platform": "linux/arm64",
        "architecture": "arm64",
        "buildkit_sandbox_id": BUILDKIT_ID,
        "client_sandbox_id": CLIENT_ID,
        "public_https": "allowed",
        "host_to_provider": "denied",
        "foreign_to_provider": "denied",
        "private_control_plane": "denied",
        "cross_provider_network": "denied",
        "managed_containers_after": 0,
        "managed_networks_after": 0,
    }
    assert all("sh" not in call[0] for call in runner.calls)
    assert all("qemu" not in " ".join(call).lower() for call in runner.calls)
    assert all("runc" not in " ".join(call).lower() for call in runner.calls)
    assert all(call[:3] == ("docker", "-H", NATIVE_ENDPOINT) for call in runner.calls if call[0] == "docker" and PRIMARY_ENDPOINT not in call)
    assert all(env == {"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin"} for env in runner.environments)


def test_uses_exact_names_networks_limits_and_python_client_program() -> None:
    """Catches drifting any reviewed sandbox ownership, isolation, resource, or native-gVisor check."""
    runner = RecordingDockerRunner()

    run_conformance(_inputs(), runner)

    assert (
        "docker", "-H", NATIVE_ENDPOINT, "network", "create", "--driver", "bridge", "--subnet",
        "172.28.0.0/24", "--label", MANAGED_LABEL, "loom-native-conformance",
    ) in runner.calls
    assert (
        "docker", "-H", NATIVE_ENDPOINT, "network", "create", "--driver", "bridge", "--subnet",
        "172.28.1.0/24", "--label", MANAGED_LABEL, "loom-native-conformance-denied",
    ) in runner.calls
    buildkit = next(
        call
        for call in runner.calls
        if "loom-native-conformance-buildkit" in call and "create" in call
    )
    assert buildkit == (
        "docker", "-H", NATIVE_ENDPOINT, "create", "--platform", "linux/arm64", "--name", "loom-native-conformance-buildkit",
        "--runtime", "runsc-personal-dev-native", "--network", "loom-native-conformance",
        "--label", MANAGED_LABEL,
        "--network-alias", "buildkit-0123456789ab", "--hostname", "buildkit-0123456789ab",
        "--read-only", "--user", "1000:1000", "--cgroup-parent", "loom-personal-dev-builder.slice",
        "--cap-drop", "ALL", "--cap-add", "SETUID", "--cap-add", "SETGID", "--security-opt",
        "seccomp=unconfined", "--cpus", "3", "--memory", "17179869184", "--memory-swap",
        "17179869184", "--pids-limit", "4096", "--tmpfs",
        "/tmp:rw,nosuid,nodev,noexec,size=2147483648,mode=1777", "--tmpfs",
        "/workspace/home:rw,nosuid,nodev,noexec,size=67108864,mode=0700,uid=1000,gid=1000",
        "--entrypoint", "/usr/local/bin/loom-personal-dev-buildkitd", BUILDER,
        "--native-tcp-buildkit-child",
    )
    client = next(
        call for call in runner.calls if "loom-native-conformance-client" in call and "create" in call
    )
    assert client[:30] == (
        "docker", "-H", NATIVE_ENDPOINT, "create", "--platform", "linux/arm64", "--name", "loom-native-conformance-client",
        "--runtime", "runsc-personal-dev-native", "--network", "loom-native-conformance", "--label", MANAGED_LABEL,
        "--read-only",
        "--user", "1000:1000", "--cap-drop", "ALL", "--cgroup-parent",
        "loom-personal-dev-builder.slice", "--security-opt", "no-new-privileges:true", "--security-opt",
        "seccomp=default", "--cpus", "1", "--memory", "17179869184", "--memory-swap",
    )
    assert client[30:40] == (
        "17179869184", "--pids-limit", "1024", "--tmpfs",
        "/tmp:rw,nosuid,nodev,noexec,size=1073741824,mode=1777", "--tmpfs",
        "/workspace:rw,nosuid,nodev,size=2147483648,mode=0700,uid=1000,gid=1000",
        "--entrypoint", "/usr/bin/python3", BUILDER,
    )
    assert client[40] == "-c"
    assert client[41] == CLIENT_PROGRAM
    assert client[42:] == (PUBLIC_HTTPS,)
    foreign = next(
        call
        for call in runner.calls
        if "loom-native-conformance-foreign-client" in call and "create" in call
    )
    assert foreign[:22] == (
        "docker", "-H", PRIMARY_ENDPOINT, "create", "--platform", "linux/arm64", "--name", "loom-native-conformance-foreign-client",
        "--network", "bridge", "--label", MANAGED_LABEL, "--read-only", "--cap-drop", "ALL", "--security-opt",
        "no-new-privileges:true", "--cpus", "1", "--memory", "268435456", "--memory-swap",
    )
    assert foreign[22:] == (
        "268435456", "--pids-limit", "64", "--tmpfs",
        "/tmp:rw,nosuid,nodev,noexec,size=16777216,mode=0700", "--entrypoint", "python", AGENT,
        "-c", FOREIGN_DENIAL_PROGRAM, "172.28.0.2",
    )
    assert (
        "/usr/bin/python3", "-c", HOST_DENIAL_PROGRAM, "172.28.0.2"
    ) in runner.calls


def test_verifies_both_platforms_labels_every_container_and_readies_denial_server() -> None:
    """Catches starting an unverified image, vacuous managed counts, or a dead denial target."""
    runner = RecordingDockerRunner()

    run_conformance(_inputs(), runner)

    first_start = next(index for index, call in enumerate(runner.calls) if "start" in call)
    platforms = [call for call in runner.calls if call[3:5] == ("image", "inspect")]
    assert platforms == [
        ("docker", "-H", NATIVE_ENDPOINT, "image", "inspect", "--format", "{{.Os}}/{{.Architecture}}", BUILDER),
        ("docker", "-H", PRIMARY_ENDPOINT, "image", "inspect", "--format", "{{.Os}}/{{.Architecture}}", AGENT),
    ]
    assert all(runner.calls.index(call) < first_start for call in platforms)
    creates = [call for call in runner.calls if "create" in call and "--name" in call]
    assert all(("--platform", "linux/arm64") in pairwise(call) for call in creates)
    assert all(("--label", MANAGED_LABEL) in pairwise(call) for call in creates)
    assert (
        "docker", "-H", NATIVE_ENDPOINT, "exec", DENIAL_ID, "/usr/bin/python3", "-c",
        "import socket\nconnection=socket.create_connection(('127.0.0.1',1234),timeout=2)\nconnection.close()\n",
    ) in runner.calls


@pytest.mark.parametrize("platform", ["linux/amd64", "windows/arm64"])
def test_rejects_any_non_arm64_image_before_container_creation(platform: str) -> None:
    """Catches QEMU/binfmt execution of an image that is not native linux/arm64."""
    runner = RecordingDockerRunner(platform=platform)

    with pytest.raises(ConformanceError, match="conformance failed"):
        run_conformance(_inputs(), runner)

    assert all("create" not in call for call in runner.calls)


@pytest.mark.parametrize("field", ["managed_containers_after", "managed_networks_after"])
def test_refuses_nonempty_post_cleanup_managed_counts(field: str) -> None:
    """Catches a passing receipt when labelled conformance objects survive cleanup."""
    runner = (
        RecordingDockerRunner(managed_containers_after="survivor\n")
        if field == "managed_containers_after"
        else RecordingDockerRunner(managed_networks_after="survivor\n")
    )

    with pytest.raises(ConformanceError, match="cleanup failed"):
        run_conformance(_inputs(), runner)


def test_subprocess_runner_caps_streamed_output_before_a_child_can_finish() -> None:
    """Catches unbounded communicate buffering of hostile command output in root memory."""
    program = "import sys,time;sys.stdout.write('x'*70000);sys.stdout.flush();time.sleep(10)"
    started = time.monotonic()

    with pytest.raises(ConformanceError, match="conformance failed"):
        SubprocessRunner().run((sys.executable, "-c", program))

    assert time.monotonic() - started < 2


def test_subprocess_runner_times_out_and_reaps_its_process_group(monkeypatch: pytest.MonkeyPatch) -> None:
    """Catches a deadline that leaves a root-started child process running after the call returns."""
    monkeypatch.setattr(conformance, "_COMMAND_TIMEOUT_SECONDS", 0.05)
    started = time.monotonic()

    with pytest.raises(ConformanceError, match="conformance failed"):
        SubprocessRunner().run((sys.executable, "-c", "import time;time.sleep(10)"))

    assert time.monotonic() - started < 2


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
def test_subprocess_runner_forwards_signals_and_reaps_its_process_group(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, signum: signal.Signals
) -> None:
    """Catches SIGINT/SIGTERM bypassing root cleanup while the command group keeps running."""
    ready = tmp_path / "handler-ready"
    original_forward = conformance._forward_signals

    def forward_after_handler(process: object) -> dict[int, object]:
        handlers = original_forward(process)  # type: ignore[arg-type]
        ready.write_text("ready", encoding="ascii")
        return handlers

    monkeypatch.setattr(conformance, "_forward_signals", forward_after_handler)
    program = (
        "import os,signal,time\n"
        f"ready={str(ready)!r}\n"
        "while not os.path.exists(ready): time.sleep(.01)\n"
        f"os.kill(os.getppid(),{signum.value})\n"
        "time.sleep(10)"
    )
    started = time.monotonic()

    with pytest.raises(ConformanceError, match="conformance failed"):
        SubprocessRunner().run((sys.executable, "-c", program))

    assert time.monotonic() - started < 2


def test_refuses_existing_exact_name_before_first_create() -> None:
    """Catches replacing exact object ownership checks with a destructive name takeover."""
    class ExistingNameRunner(RecordingDockerRunner):
        def run(
            self,
            argv: Sequence[str],
            *,
            check: bool = True,
            env: dict[str, str] | None = None,
        ) -> CommandResult:
            call = tuple(argv)
            if call[-1] == "loom-native-conformance-buildkit" and "inspect" in call:
                self.calls.append(call)
                return CommandResult(0, BUILDKIT_ID + "\n")
            return super().run(argv, check=check, env=env)

    existing = ExistingNameRunner()
    with pytest.raises(ConformanceError, match="conformance failed"):
        run_conformance(_inputs(), existing)
    assert all("create" not in call for call in existing.calls)


@pytest.mark.parametrize("failure_number", range(1, 32))
def test_each_primary_failure_cleans_only_recorded_ids_in_reverse_order(failure_number: int) -> None:
    """Catches leaked sandboxes or broad cleanup when a create, start, or probe fails."""
    runner = RecordingDockerRunner(fail_at=failure_number)

    with pytest.raises(ConformanceError, match="conformance failed"):
        run_conformance(_inputs(), runner)

    cleanup = [call for call in runner.calls if "rm" in call]
    assert [call[-1] for call in cleanup] == list(reversed(runner.created))
    assert all(call[-1] in set(runner.created) for call in cleanup)
    endpoint_for = {
        BUILDKIT_ID: NATIVE_ENDPOINT,
        CLIENT_ID: NATIVE_ENDPOINT,
        DENIAL_ID: NATIVE_ENDPOINT,
        FOREIGN_ID: PRIMARY_ENDPOINT,
        PROVIDER_NETWORK_ID: NATIVE_ENDPOINT,
        DENIED_NETWORK_ID: NATIVE_ENDPOINT,
    }
    assert cleanup == [
        (
            "docker", "-H", endpoint_for[identifier], "network", "rm", identifier
        )
        if identifier in {PROVIDER_NETWORK_ID, DENIED_NETWORK_ID}
        else ("docker", "-H", endpoint_for[identifier], "rm", "-f", identifier)
        for identifier in reversed(runner.created)
    ]


def test_cleanup_failure_is_not_hidden_by_primary_failure() -> None:
    """Catches cleanup errors being swallowed after a failed conformance operation."""
    runner = RecordingDockerRunner(fail_at=5, fail_cleanup=True)

    with pytest.raises(ConformanceError, match="cleanup failed") as raised:
        run_conformance(_inputs(), runner)

    assert isinstance(raised.value.__cause__, ConformanceError)


def test_rejects_a_client_id_that_is_not_a_separate_kvm_sandbox() -> None:
    """Catches accepting one Docker object as both required independent gVisor sandboxes."""
    runner = RecordingDockerRunner(duplicate_client_id=True)

    with pytest.raises(ConformanceError, match="conformance failed"):
        run_conformance(_inputs(), runner)


@pytest.mark.parametrize(
    "name",
    [
        "loom-native-conformance",
        "loom-native-conformance-denied",
        "loom-native-conformance-buildkit",
        "loom-native-conformance-client",
        "loom-native-conformance-denial-target",
        "loom-native-conformance-foreign-client",
    ],
)
def test_rejects_invalid_returned_ids_at_every_create_boundary(name: str) -> None:
    """Catches treating arbitrary Docker output as an owned object eligible for deletion."""
    runner = RecordingDockerRunner(invalid_create_name=name)

    with pytest.raises(ConformanceError, match="conformance failed"):
        run_conformance(_inputs(), runner)

    expected = {
        "loom-native-conformance": [],
        "loom-native-conformance-denied": [PROVIDER_NETWORK_ID],
        "loom-native-conformance-buildkit": [PROVIDER_NETWORK_ID, DENIED_NETWORK_ID],
        "loom-native-conformance-client": [PROVIDER_NETWORK_ID, DENIED_NETWORK_ID, BUILDKIT_ID, FOREIGN_ID, DENIAL_ID],
        "loom-native-conformance-denial-target": [PROVIDER_NETWORK_ID, DENIED_NETWORK_ID, BUILDKIT_ID, FOREIGN_ID],
        "loom-native-conformance-foreign-client": [PROVIDER_NETWORK_ID, DENIED_NETWORK_ID, BUILDKIT_ID],
    }
    assert runner.created == expected[name]


def test_subprocess_runner_kills_a_sigterm_resistant_descendant_after_its_leader_exits(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Catches returning after the leader exits while its SIGTERM-resistant process-group child survives."""
    marker = tmp_path / "grandchild-pid"
    monkeypatch.setattr(conformance, "_COMMAND_TIMEOUT_SECONDS", 0.2)
    program = (
        "import pathlib,signal,subprocess,sys,time\n"
        "child=subprocess.Popen([sys.executable,'-c',\"import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(10)\"])\n"
        f"pathlib.Path({str(marker)!r}).write_text(str(child.pid),encoding='ascii')\n"
        "time.sleep(10)"
    )

    with pytest.raises(ConformanceError, match="conformance failed"):
        SubprocessRunner().run((sys.executable, "-c", program))

    grandchild = int(marker.read_text(encoding="ascii"))
    try:
        for _ in range(100):
            if not _process_exists(grandchild):
                break
            time.sleep(0.02)
        assert not _process_exists(grandchild)
    finally:
        try:
            os.kill(grandchild, signal.SIGKILL)
        except ProcessLookupError:
            pass


def _process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


@pytest.mark.parametrize(
    "field,value",
    [
        ("builder_image", "ghcr.io/qianyi-sun/loom-personal-dev-builder:latest"),
        ("agent_image", "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "a" * 64),
        ("public_https", "https://127.0.0.1"),
        ("public_https", "https://objects.example/path"),
        ("public_https", "https://objects.example:444"),
    ],
)
def test_inputs_reject_unpinned_or_private_execution_targets(field: str, value: str) -> None:
    """Catches a caller directing root conformance at mutable images or private HTTPS targets."""
    values = {"builder_image": BUILDER, "agent_image": AGENT, "public_https": PUBLIC_HTTPS}
    values[field] = value

    with pytest.raises(ValueError, match="conformance input is invalid"):
        ConformanceInputs(**values)
