from __future__ import annotations

import ctypes
import json
import os
import signal
import sys
import threading
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
_SYS_PIDFD_SEND_SIGNAL = 424
_SYS_PIDFD_OPEN = 434
_LIBC = ctypes.CDLL(None, use_errno=True)
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


def _complete_inspect_fixture(
    create: tuple[str, ...] | None,
    identifier: str,
) -> dict[str, object]:
    assert create is not None
    labels: dict[str, str] = {}
    if create[3:5] == ("network", "create"):
        options: dict[str, list[str]] = {}
        values = create[5:-1]
        for index in range(0, len(values), 2):
            options.setdefault(values[index], []).append(values[index + 1])
        for label in options["--label"]:
            key, value = label.split("=", 1)
            labels[key] = value
        return {
            "Driver": options["--driver"][0],
            "IPAM": {"Config": [{"Subnet": subnet} for subnet in options["--subnet"]]},
            "Id": identifier,
            "Labels": labels,
            "Name": create[-1],
        }
    value_options = {
        "--cap-add",
        "--cap-drop",
        "--cgroup-parent",
        "--cpus",
        "--entrypoint",
        "--hostname",
        "--ip",
        "--label",
        "--memory",
        "--memory-swap",
        "--name",
        "--network",
        "--network-alias",
        "--pids-limit",
        "--platform",
        "--runtime",
        "--security-opt",
        "--tmpfs",
        "--user",
    }
    options = {}
    values = create[4:]
    index = 0
    while values[index].startswith("--"):
        option = values[index]
        if option == "--read-only":
            options.setdefault(option, []).append("true")
            index += 1
        else:
            assert option in value_options
            options.setdefault(option, []).append(values[index + 1])
            index += 2
    image = values[index]
    command = list(values[index + 1 :])
    for label in options["--label"]:
        key, value = label.split("=", 1)
        labels[key] = value
    network = options["--network"][0]
    attached: dict[str, object] = {
        "Aliases": list(options.get("--network-alias", [])),
        "IPAMConfig": {
            "IPv4Address": options.get("--ip", [""])[0],
        },
    }
    return {
        "Config": {
            "Cmd": command,
            "Entrypoint": list(options.get("--entrypoint", [])),
            "Hostname": options.get("--hostname", [identifier[:12]])[0],
            "Image": image,
            "Labels": labels,
            "User": options.get("--user", [""])[0],
        },
        "HostConfig": {
            "CapAdd": list(options.get("--cap-add", [])),
            "CapDrop": list(options.get("--cap-drop", [])),
            "CgroupParent": options.get("--cgroup-parent", [""])[0],
            "Memory": int(options["--memory"][0]),
            "MemorySwap": int(options["--memory-swap"][0]),
            "NanoCpus": int(float(options["--cpus"][0]) * 1_000_000_000),
            "NetworkMode": network,
            "PidsLimit": int(options["--pids-limit"][0]),
            "ReadonlyRootfs": "--read-only" in options,
            "Runtime": options.get("--runtime", ["runc"])[0],
            "SecurityOpt": list(options.get("--security-opt", [])),
            "Tmpfs": {
                item.split(":", 1)[0]: item.split(":", 1)[1] for item in options.get("--tmpfs", [])
            },
        },
        "Id": identifier,
        "Name": f"/{options['--name'][0]}",
        "NetworkSettings": {"Networks": {network: attached}},
    }


def _without_reconciliation_labels(call: tuple[str, ...]) -> tuple[str, ...]:
    values: list[str] = []
    index = 0
    while index < len(call):
        if (
            call[index] == "--label"
            and index + 1 < len(call)
            and call[index + 1].startswith("loom.personal-dev-native-builder.conformance-")
        ):
            index += 2
            continue
        values.append(call[index])
        index += 1
    return tuple(values)


def _not_found(kind: str, name: str) -> CommandResult:
    message = (
        f"Error response from daemon: network {name} not found\n"
        if kind == "network"
        else f"Error response from daemon: No such container: {name}\n"
    )
    return CommandResult(1, "", message)


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
    drifted_inspect_name: str | None = None

    def __post_init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.environments: list[dict[str, str]] = []
        self.created: list[str] = []
        self._operation_count = 0
        self._nanocpus_inspections = 0
        self._failed_call: tuple[str, ...] | None = None
        self.present: dict[str, tuple[tuple[str, ...], str]] = {}

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
        if (
            "inspect" in call
            and "--format" in call
            and call[call.index("--format") + 1] == "{{json .}}"
            and call[-1] in self.present
        ):
            create, identifier = self.present[call[-1]]
            identity = _complete_inspect_fixture(create, identifier)
            if call[-1] == self.drifted_inspect_name:
                if create[3:5] == ("network", "create"):
                    identity["Driver"] = "macvlan"
                else:
                    host = identity["HostConfig"]
                    assert isinstance(host, dict)
                    host["Memory"] = 1
            return CommandResult(
                0,
                json.dumps(
                    identity,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n",
            )
        is_cleanup = "rm" in call or ("network" in call and "rm" in call)
        is_reconciliation = (
            "inspect" in call
            and "--format" in call
            and call[call.index("--format") + 1] == "{{json .}}"
        )
        if is_cleanup and self.fail_cleanup:
            return CommandResult(1, stderr="cleanup failed")
        if is_cleanup:
            removed = next(
                (
                    present_name
                    for present_name, (_create, identifier) in self.present.items()
                    if identifier == call[-1]
                ),
                None,
            )
            if removed is not None:
                self.present.pop(removed)
        is_preexisting_check = "inspect" in call and "--format" not in call
        is_emptiness_check = call[-2:] in (("ps", "-aq"), ("ls", "-q"))
        if self._failed_call == call:
            return CommandResult(1, stderr="primary failed")
        if is_reconciliation and call[-1] not in self.present:
            kind = "network" if call[3] == "network" else "container"
            return _not_found(kind, call[-1])
        if (
            not is_cleanup
            and not is_preexisting_check
            and not is_emptiness_check
            and not is_reconciliation
        ):
            self._operation_count += 1
            if self.fail_at == self._operation_count:
                self._failed_call = call
                return CommandResult(1, stderr="primary failed")
        if is_preexisting_check:
            present = self.present.get(call[-1])
            kind = "network" if call[3] == "network" else "container"
            return _not_found(kind, call[-1]) if present is None else CommandResult(0, present[1])
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
                PROVIDER_NETWORK_ID if call[-1] == "loom-native-conformance" else DENIED_NETWORK_ID
            )
            self.created.append(identifier)
            self.present[call[-1]] = (call, identifier)
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
            self.present[name] = (call, identifier)
            return CommandResult(0, identifier + "\n")
        if "logs" in call:
            return CommandResult(0, "loom-buildkitd-native-child-preflight nnp=1\n")
        if "inspect" in call:
            template = call[call.index("--format") + 1]
            if template == "{{.HostConfig.NanoCpus}}" and self.duplicate_client_id:
                self._nanocpus_inspections += 1
                return CommandResult(
                    0, "3000000000\n" if self._nanocpus_inspections == 1 else "1000000000\n"
                )
            values = {
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}": "172.28.0.2\n",
                "{{.State.ExitCode}}": "0\n",
                "{{.HostConfig.Runtime}}": "runsc-personal-dev-native\n",
                "{{.Architecture}}": "arm64\n",
                "{{.Os}}/{{.Architecture}}": self.platform + "\n",
                "{{.HostConfig.CgroupParent}}": "loom-personal-dev-builder.slice\n",
                "{{.HostConfig.NanoCpus}}": (
                    "3000000000\n" if call[-1] == BUILDKIT_ID else "1000000000\n"
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
    assert all(
        call[:3] == ("docker", "-H", NATIVE_ENDPOINT)
        for call in runner.calls
        if call[0] == "docker" and PRIMARY_ENDPOINT not in call
    )
    assert all(
        env == {"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin"} for env in runner.environments
    )


def test_uses_exact_names_networks_limits_and_python_client_program() -> None:
    """Catches drifting any reviewed sandbox ownership, isolation, resource, or native-gVisor check."""
    runner = RecordingDockerRunner()

    run_conformance(_inputs(), runner)
    normalized_calls = [_without_reconciliation_labels(call) for call in runner.calls]

    assert (
        "docker",
        "-H",
        NATIVE_ENDPOINT,
        "network",
        "create",
        "--driver",
        "bridge",
        "--subnet",
        "172.28.0.0/24",
        "--label",
        MANAGED_LABEL,
        "loom-native-conformance",
    ) in normalized_calls
    assert (
        "docker",
        "-H",
        NATIVE_ENDPOINT,
        "network",
        "create",
        "--driver",
        "bridge",
        "--subnet",
        "172.28.1.0/24",
        "--label",
        MANAGED_LABEL,
        "loom-native-conformance-denied",
    ) in normalized_calls
    buildkit = next(
        call
        for call in normalized_calls
        if "loom-native-conformance-buildkit" in call and "create" in call
    )
    assert buildkit == (
        "docker",
        "-H",
        NATIVE_ENDPOINT,
        "create",
        "--platform",
        "linux/arm64",
        "--name",
        "loom-native-conformance-buildkit",
        "--runtime",
        "runsc-personal-dev-native",
        "--network",
        "loom-native-conformance",
        "--label",
        MANAGED_LABEL,
        "--network-alias",
        "buildkit-0123456789ab",
        "--hostname",
        "buildkit-0123456789ab",
        "--read-only",
        "--user",
        "1000:1000",
        "--cgroup-parent",
        "loom-personal-dev-builder.slice",
        "--cap-drop",
        "ALL",
        "--cap-add",
        "SETUID",
        "--cap-add",
        "SETGID",
        "--security-opt",
        "seccomp=unconfined",
        "--cpus",
        "3",
        "--memory",
        "17179869184",
        "--memory-swap",
        "17179869184",
        "--pids-limit",
        "4096",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,noexec,size=2147483648,mode=1777",
        "--tmpfs",
        "/workspace/home:rw,nosuid,nodev,noexec,size=67108864,mode=0700,uid=1000,gid=1000",
        "--entrypoint",
        "/usr/local/bin/loom-personal-dev-buildkitd",
        BUILDER,
        "--native-tcp-buildkit-child",
    )
    client = next(
        call
        for call in normalized_calls
        if "loom-native-conformance-client" in call and "create" in call
    )
    assert client[:30] == (
        "docker",
        "-H",
        NATIVE_ENDPOINT,
        "create",
        "--platform",
        "linux/arm64",
        "--name",
        "loom-native-conformance-client",
        "--runtime",
        "runsc-personal-dev-native",
        "--network",
        "loom-native-conformance",
        "--label",
        MANAGED_LABEL,
        "--read-only",
        "--user",
        "1000:1000",
        "--cap-drop",
        "ALL",
        "--cgroup-parent",
        "loom-personal-dev-builder.slice",
        "--security-opt",
        "no-new-privileges:true",
        "--security-opt",
        "seccomp=default",
        "--cpus",
        "1",
        "--memory",
        "17179869184",
        "--memory-swap",
    )
    assert client[30:40] == (
        "17179869184",
        "--pids-limit",
        "1024",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,noexec,size=1073741824,mode=1777",
        "--tmpfs",
        "/workspace:rw,nosuid,nodev,size=2147483648,mode=0700,uid=1000,gid=1000",
        "--entrypoint",
        "/usr/bin/python3",
        BUILDER,
    )
    assert client[40] == "-c"
    assert client[41] == CLIENT_PROGRAM
    assert client[42:] == (PUBLIC_HTTPS,)
    foreign = next(
        call
        for call in normalized_calls
        if "loom-native-conformance-foreign-client" in call and "create" in call
    )
    assert foreign[:22] == (
        "docker",
        "-H",
        PRIMARY_ENDPOINT,
        "create",
        "--platform",
        "linux/arm64",
        "--name",
        "loom-native-conformance-foreign-client",
        "--network",
        "bridge",
        "--label",
        MANAGED_LABEL,
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges:true",
        "--cpus",
        "1",
        "--memory",
        "268435456",
        "--memory-swap",
    )
    assert foreign[22:] == (
        "268435456",
        "--pids-limit",
        "64",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,noexec,size=16777216,mode=0700",
        "--entrypoint",
        "python",
        AGENT,
        "-c",
        FOREIGN_DENIAL_PROGRAM,
        "172.28.0.2",
    )
    assert ("/usr/bin/python3", "-c", HOST_DENIAL_PROGRAM, "172.28.0.2") in normalized_calls


def test_verifies_both_platforms_labels_every_container_and_readies_denial_server() -> None:
    """Catches starting an unverified image, vacuous managed counts, or a dead denial target."""
    runner = RecordingDockerRunner()

    run_conformance(_inputs(), runner)

    first_start = next(index for index, call in enumerate(runner.calls) if "start" in call)
    platforms = [call for call in runner.calls if call[3:5] == ("image", "inspect")]
    assert platforms == [
        (
            "docker",
            "-H",
            NATIVE_ENDPOINT,
            "image",
            "inspect",
            "--format",
            "{{.Os}}/{{.Architecture}}",
            BUILDER,
        ),
        (
            "docker",
            "-H",
            PRIMARY_ENDPOINT,
            "image",
            "inspect",
            "--format",
            "{{.Os}}/{{.Architecture}}",
            AGENT,
        ),
    ]
    assert all(runner.calls.index(call) < first_start for call in platforms)
    creates = [call for call in runner.calls if "create" in call and "--name" in call]
    assert all(("--platform", "linux/arm64") in pairwise(call) for call in creates)
    assert all(("--label", MANAGED_LABEL) in pairwise(call) for call in creates)
    assert (
        "docker",
        "-H",
        NATIVE_ENDPOINT,
        "exec",
        DENIAL_ID,
        "/usr/bin/python3",
        "-c",
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


def test_subprocess_runner_times_out_and_reaps_its_process_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catches a deadline that leaves a root-started child process running after the call returns."""
    monkeypatch.setattr(conformance, "_COMMAND_TIMEOUT_SECONDS", 0.05)
    started = time.monotonic()

    with pytest.raises(ConformanceError, match="conformance failed"):
        SubprocessRunner().run((sys.executable, "-c", "import time;time.sleep(10)"))

    assert time.monotonic() - started < 2


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM, signal.SIGHUP])
def test_subprocess_runner_forwards_signals_and_reaps_its_process_group(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, signum: signal.Signals
) -> None:
    """Catches transaction signals being consumed instead of forwarded, reaped, and replayed."""
    ready = tmp_path / "handler-ready"
    forwarded = tmp_path / "child-received-signal"
    original_forward = conformance._forward_signals

    def forward_after_handler(process: object) -> dict[int, object]:
        handlers = original_forward(process)  # type: ignore[arg-type]
        ready.write_text("ready", encoding="ascii")
        return handlers

    monkeypatch.setattr(conformance, "_forward_signals", forward_after_handler)
    monkeypatch.setattr(conformance, "_COMMAND_TIMEOUT_SECONDS", 0.2)
    program = (
        "import os,signal,sys,time\n"
        f"ready={str(ready)!r}\n"
        "while not os.path.exists(ready): time.sleep(.01)\n"
        f"marker=os.open({str(forwarded)!r},os.O_WRONLY|os.O_CREAT,0o600)\n"
        f"signal.signal({signum.value},lambda *_: (os.write(marker,b'forwarded'),sys.exit(0)))\n"
        f"os.kill(os.getppid(),{signum.value})\n"
        "time.sleep(10)"
    )
    started = time.monotonic()
    replayed: list[int] = []
    previous = signal.signal(signum, lambda observed, _frame: replayed.append(observed))

    try:
        with pytest.raises(ConformanceError, match="conformance failed"):
            SubprocessRunner().run((sys.executable, "-c", program))
    finally:
        signal.signal(signum, previous)

    assert time.monotonic() - started < 2
    assert forwarded.read_bytes().startswith(b"forwarded")
    assert replayed == [signum]


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
def test_each_primary_failure_cleans_only_recorded_ids_in_reverse_order(
    failure_number: int,
) -> None:
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
        ("docker", "-H", endpoint_for[identifier], "network", "rm", identifier)
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
        "loom-native-conformance-client": [
            PROVIDER_NETWORK_ID,
            DENIED_NETWORK_ID,
            BUILDKIT_ID,
            FOREIGN_ID,
            DENIAL_ID,
        ],
        "loom-native-conformance-denial-target": [
            PROVIDER_NETWORK_ID,
            DENIED_NETWORK_ID,
            BUILDKIT_ID,
            FOREIGN_ID,
        ],
        "loom-native-conformance-foreign-client": [
            PROVIDER_NETWORK_ID,
            DENIED_NETWORK_ID,
            BUILDKIT_ID,
        ],
    }
    assert runner.created == expected[name]


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
@pytest.mark.parametrize("ambiguity", ("malformed", "timeout"))
def test_ambiguous_create_reconciles_every_kind_and_endpoint_before_retry(
    name: str,
    ambiguity: str,
) -> None:
    """Catches a committed Docker object escaping cleanup when create output is ambiguous."""

    class AmbiguousCreateRunner(RecordingDockerRunner):
        def __post_init__(self) -> None:
            super().__post_init__()
            self.target_present = False
            self.target_identifier = ""
            self.target_create: tuple[str, ...] | None = None
            self.inject_ambiguity = True

        def run(
            self,
            argv: Sequence[str],
            *,
            check: bool = True,
            env: dict[str, str] | None = None,
        ) -> CommandResult:
            call = tuple(argv)
            call_name = call[call.index("--name") + 1] if "--name" in call else call[-1]
            is_target_create = "create" in call and call_name == name
            if (
                self.target_present
                and call_name == name
                and "inspect" in call
                and "--format" in call
            ):
                self.calls.append(call)
                self.environments.append({} if env is None else dict(env))
                return CommandResult(
                    0,
                    json.dumps(
                        _complete_inspect_fixture(
                            self.target_create,
                            self.target_identifier,
                        ),
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n",
                )
            if self.target_present and call_name == name and "inspect" in call:
                self.calls.append(call)
                self.environments.append({} if env is None else dict(env))
                return CommandResult(0, self.target_identifier + "\n")
            if self.target_present and "rm" in call and call[-1] == self.target_identifier:
                self.target_present = False
            if is_target_create and self.inject_ambiguity:
                result = super().run(argv, check=check, env=env)
                self.target_identifier = result.stdout.strip()
                self.target_create = call
                self.target_present = True
                self.inject_ambiguity = False
                if ambiguity == "timeout":
                    raise TimeoutError("Docker response was lost after commit")
                return CommandResult(0, "truncated-object-id\n")
            if self.target_present and call[3:5] == ("ps", "-aq"):
                return CommandResult(0, self.target_identifier + "\n")
            if self.target_present and call[3:6] == ("network", "ls", "-q"):
                return CommandResult(0, self.target_identifier + "\n")
            return super().run(argv, check=check, env=env)

    runner = AmbiguousCreateRunner()

    with pytest.raises(ConformanceError):
        run_conformance(_inputs(), runner)

    assert runner.target_present is False
    assert run_conformance(_inputs(), runner)["status"] == "passed"


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
@pytest.mark.parametrize(
    "inspect_behavior",
    ("daemon-error", "malformed", "raised", "unavailable", "exact-not-found"),
)
def test_ambiguous_create_requires_proven_ownership_or_exact_not_found(
    name: str,
    inspect_behavior: str,
) -> None:
    """Catches uncertain inspect results being treated as ownership or absence."""

    class IndeterminateInspectRunner(RecordingDockerRunner):
        def __post_init__(self) -> None:
            super().__post_init__()
            self.target_identifier = ""
            self.target_present = False
            self.inject_ambiguity = True

        def run(
            self,
            argv: Sequence[str],
            *,
            check: bool = True,
            env: dict[str, str] | None = None,
        ) -> CommandResult:
            call = tuple(argv)
            call_name = call[call.index("--name") + 1] if "--name" in call else call[-1]
            is_target_create = "create" in call and call_name == name
            if self.target_present and "inspect" in call and call[-1] == name:
                self.calls.append(call)
                self.environments.append({} if env is None else dict(env))
                if inspect_behavior == "raised":
                    raise TimeoutError("inspect transport unavailable")
                if inspect_behavior == "malformed" and "--format" in call:
                    return CommandResult(0, "{\n", "")
                if inspect_behavior == "daemon-error":
                    return CommandResult(
                        1,
                        "",
                        "Error response from daemon: daemon is unavailable\n",
                    )
                if inspect_behavior == "unavailable":
                    return CommandResult(
                        125,
                        "",
                        "Cannot connect to the Docker daemon\n",
                    )
                return CommandResult(1, "", "truncated inspect response\n")
            if is_target_create and self.inject_ambiguity:
                result = super().run(argv, check=check, env=env)
                self.target_identifier = result.stdout.strip()
                self.target_present = inspect_behavior != "exact-not-found"
                self.inject_ambiguity = False
                if not self.target_present:
                    self.present.pop(name)
                return CommandResult(0, "truncated-object-id\n", "")
            return super().run(argv, check=check, env=env)

    runner = IndeterminateInspectRunner()

    if inspect_behavior == "exact-not-found":
        with pytest.raises(ConformanceError, match=r"^conformance failed$"):
            run_conformance(_inputs(), runner)
    else:
        with pytest.raises(ConformanceError, match="cleanup failed"):
            run_conformance(_inputs(), runner)

    removed_ids = {call[-1] for call in runner.calls if "rm" in call}
    assert runner.target_identifier not in removed_ids
    assert runner.target_present is (inspect_behavior != "exact-not-found")


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
def test_create_reconciliation_requires_complete_spec_identity(name: str) -> None:
    """Catches trusting ownership labels without checking the committed object spec."""
    runner = RecordingDockerRunner(drifted_inspect_name=name)

    with pytest.raises(ConformanceError, match="conformance failed"):
        run_conformance(_inputs(), runner)

    assert runner.present == {}
    runner.drifted_inspect_name = None
    assert run_conformance(_inputs(), runner)["status"] == "passed"


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
    pidfds: list[int] = []

    def retain_descendant_identity() -> None:
        for _ in range(100):
            if marker.exists():
                pidfds.append(_pidfd_open(int(marker.read_text(encoding="ascii"))))
                return
            time.sleep(0.01)

    retainer = threading.Thread(target=retain_descendant_identity)
    retainer.start()

    with pytest.raises(ConformanceError, match="conformance failed"):
        SubprocessRunner().run((sys.executable, "-c", program))

    retainer.join(timeout=2)
    assert pidfds
    raw_kill = os.kill

    def reject_raw_signal(pid: int, signum: int) -> None:
        if signum != 0:
            raise AssertionError(f"raw PID signal attempted for {pid}")
        raw_kill(pid, signum)

    monkeypatch.setattr(os, "kill", reject_raw_signal)
    pidfd = pidfds[0]
    try:
        for _ in range(100):
            if not _pidfd_exists(pidfd):
                break
            time.sleep(0.02)
        assert not _pidfd_exists(pidfd)
    finally:
        try:
            _pidfd_send_signal(pidfd, signal.SIGKILL)
        except ProcessLookupError:
            pass
        os.close(pidfd)


def test_conformance_cleanup_never_signals_a_reused_group_after_leader_reap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catches conformance killpg interposition onto a reused numeric PGID."""
    reused_signals: list[int] = []

    class Stream:
        def close(self) -> None:
            pass

    class Process:
        pid = 434343
        stdout = Stream()
        stderr = Stream()
        args = ("fake",)
        returncode: int | None = None
        reaped = False

        def wait(self, timeout: float | None = None) -> int:
            del timeout
            self.reaped = True
            self.returncode = 0
            return 0

    process = Process()

    def interposed_killpg(process_group: int, signum: int) -> None:
        assert process_group == process.pid
        if process.reaped:
            if signum == signal.SIGKILL:
                reused_signals.append(signum)
                return
            if reused_signals:
                raise ProcessLookupError

    monkeypatch.setattr(conformance.os, "killpg", interposed_killpg)
    monkeypatch.setattr(
        conformance.os,
        "waitid",
        lambda *_args, **_kwargs: object(),
    )

    conformance._terminate_and_reap(process)  # type: ignore[arg-type]

    assert reused_signals == []


@pytest.mark.parametrize("signal_window", ("group-check", "leader-wait"))
def test_conformance_runner_defers_signal_until_retained_leader_is_reaped_once(
    monkeypatch: pytest.MonkeyPatch,
    signal_window: str,
) -> None:
    """Catches a post-WNOWAIT signal bypassing descendant cleanup or leader reap."""
    events: list[str] = []
    installed: dict[int, object] = {}

    class Process:
        pid = 454545
        stdout = object()
        stderr = object()
        args = ("fake",)
        returncode: int | None = None
        reaps = 0

        def wait(self, timeout: float | None = None) -> int:
            del timeout
            events.append("leader.wait")
            if signal_window == "leader-wait":
                handler = installed[signal.SIGTERM]
                assert callable(handler)
                handler(signal.SIGTERM, None)
                events.append("signal.deferred")
            self.reaps += 1
            self.returncode = 0
            return 0

    process = Process()

    def previous(signum: int, _frame: object) -> None:
        assert signum == signal.SIGTERM
        assert process.reaps == 1
        events.append("signal.replayed")

    def install(signum: int, handler: object) -> object:
        old = installed.get(signum, previous)
        installed[signum] = handler
        return old

    def group_has_other_members(process_group: int) -> bool:
        assert process_group == process.pid
        events.append("group.check")
        if signal_window == "group-check":
            handler = installed[signal.SIGTERM]
            assert callable(handler)
            handler(signal.SIGTERM, None)
            events.append("signal.deferred")
            return True
        return False

    def terminate(observed: object) -> None:
        assert observed is process
        events.append("group.cleanup")
        process.wait()

    monkeypatch.setattr(conformance.signal, "signal", install)
    monkeypatch.setattr(conformance.subprocess, "Popen", lambda *_args, **_kwargs: process)
    monkeypatch.setattr(conformance, "_drain_bounded", lambda _process: (b"", b""))
    monkeypatch.setattr(conformance, "_process_group_has_other_members", group_has_other_members)
    monkeypatch.setattr(conformance, "_terminate_and_reap", terminate)

    with pytest.raises(ConformanceError, match="conformance failed"):
        SubprocessRunner().run(("fake",))

    assert process.reaps == 1
    assert events.count("group.cleanup") == (1 if signal_window == "group-check" else 0)
    assert events[-1] == "signal.replayed"


def _pidfd_open(pid: int) -> int:
    return _linux_syscall(_SYS_PIDFD_OPEN, pid, 0)


def _pidfd_exists(pidfd: int) -> bool:
    try:
        _pidfd_send_signal(pidfd, 0)
    except ProcessLookupError:
        return False
    return True


def _pidfd_send_signal(pidfd: int, signum: signal.Signals | int) -> None:
    _linux_syscall(_SYS_PIDFD_SEND_SIGNAL, pidfd, int(signum), 0, 0)


def _linux_syscall(number: int, *arguments: int) -> int:
    result = _LIBC.syscall(number, *arguments)
    if result == -1:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return int(result)


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
