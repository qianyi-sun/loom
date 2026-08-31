"""Fixed, import-only conformance probe for the personal native builder."""

from __future__ import annotations

import ipaddress
import os
import re
import selectors
import signal
import subprocess
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, cast, runtime_checkable
from urllib.parse import urlsplit

_NATIVE_ENDPOINT = "unix:///run/loom-personal-dev-builder/docker.sock"
_PRIMARY_ENDPOINT = "unix:///var/run/docker.sock"
_MANAGED_LABEL = "loom.personal-dev-native-builder.managed=true"
_ROOT_ENV = {"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin"}
_COMMAND_TIMEOUT_SECONDS = 30
_MAX_COMMAND_OUTPUT = 64 * 1024
_HEX_64 = re.compile(r"[0-9a-f]{64}")
_HOST = re.compile(
    r"(?=.{1,253}\Z)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)(?:\.[a-z0-9]"
    r"(?:[a-z0-9-]{0,61}[a-z0-9])?)*"
)
_BUILDER_REPOSITORY = "ghcr.io/qianyi-sun/loom-personal-dev-builder"
_AGENT_REPOSITORY = "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent"
_NETWORK_NAME = "loom-native-conformance"
_DENIED_NETWORK_NAME = "loom-native-conformance-denied"
_BUILDKIT_NAME = "loom-native-conformance-buildkit"
_CLIENT_NAME = "loom-native-conformance-client"
_DENIAL_NAME = "loom-native-conformance-denial-target"
_FOREIGN_CLIENT_NAME = "loom-native-conformance-foreign-client"
_DENIAL_READY_PROGRAM = """import socket
connection=socket.create_connection(('127.0.0.1',1234),timeout=2)
connection.close()
"""

_HOST_DENIAL_PROGRAM = """import socket,sys
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
_FOREIGN_DENIAL_PROGRAM = """import socket,sys
try:
    connection=socket.create_connection((sys.argv[1],1234),timeout=2)
except OSError:
    raise SystemExit(0)
connection.close()
raise SystemExit(1)
"""
_CLIENT_PROGRAM = """import platform,socket,sys,urllib.error,urllib.request
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


class ConformanceError(RuntimeError):
    """The fixed probe failed without exposing command output or input values."""


@dataclass(frozen=True)
class ConformanceInputs:
    """The three public, immutable inputs permitted to influence the probe."""

    builder_image: str
    agent_image: str
    public_https: str

    def __post_init__(self) -> None:
        if not _image(self.builder_image, _BUILDER_REPOSITORY):
            raise ValueError("conformance input is invalid")
        if not _image(self.agent_image, _AGENT_REPOSITORY):
            raise ValueError("conformance input is invalid")
        if not _origin(self.public_https):
            raise ValueError("conformance input is invalid")


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


@runtime_checkable
class Runner(Protocol):
    def run(
        self,
        argv: Sequence[str],
        *,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> CommandResult: ...


class SubprocessRunner:
    """Run one exact argv with bounded output and a disposable process group."""

    def run(
        self,
        argv: Sequence[str],
        *,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        if not argv or any(not isinstance(value, str) or not value for value in argv):
            raise ConformanceError("conformance failed")
        process: subprocess.Popen[bytes] | None = None
        previous_handlers: dict[int, object] = {}
        try:
            process = subprocess.Popen(
                list(argv),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=_ROOT_ENV if env is None else env,
                start_new_session=True,
            )
            if threading.current_thread() is threading.main_thread():
                previous_handlers = _forward_signals(process)
            stdout, stderr = _drain_bounded(process)
        except BaseException as exc:
            if process is not None:
                _terminate_and_reap(process)
            if isinstance(exc, (subprocess.TimeoutExpired, _CommandOutputError, _CommandSignal)):
                raise ConformanceError("conformance failed") from exc
            raise
        finally:
            for signum, handler in previous_handlers.items():
                signal.signal(signum, cast(signal.Handlers, handler))
        if (
            len(stdout) > _MAX_COMMAND_OUTPUT
            or len(stderr) > _MAX_COMMAND_OUTPUT
        ):
            raise ConformanceError("conformance failed")
        result = CommandResult(
            process.returncode,
            stdout.decode("utf-8", errors="replace"),
            stderr.decode("utf-8", errors="replace"),
        )
        if check and result.returncode != 0:
            raise ConformanceError("conformance failed")
        return result


class _CommandOutputError(RuntimeError):
    pass


class _CommandSignal(BaseException):
    pass


def _forward_signals(process: subprocess.Popen[bytes]) -> dict[int, object]:
    previous: dict[int, object] = {}

    def forward(signum: int, _frame: object) -> None:
        try:
            os.killpg(process.pid, signum)
        except ProcessLookupError:
            pass
        raise _CommandSignal()

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.signal(signum, forward)
    return previous


def _drain_bounded(process: subprocess.Popen[bytes]) -> tuple[bytes, bytes]:
    if process.stdout is None or process.stderr is None:
        raise ConformanceError("conformance failed")
    chunks: dict[str, bytearray] = {"stdout": bytearray(), "stderr": bytearray()}
    deadline = time.monotonic() + _COMMAND_TIMEOUT_SECONDS
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ, "stdout")
        selector.register(process.stderr, selectors.EVENT_READ, "stderr")
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(process.args, _COMMAND_TIMEOUT_SECONDS)
            for key, _ in selector.select(remaining):
                stream = key.fileobj
                descriptor = stream if isinstance(stream, int) else stream.fileno()
                chunk = os.read(descriptor, 8192)
                if not chunk:
                    selector.unregister(stream)
                    continue
                output = chunks[key.data]
                if len(output) + len(chunk) > _MAX_COMMAND_OUTPUT:
                    raise _CommandOutputError()
                output.extend(chunk)
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise subprocess.TimeoutExpired(process.args, _COMMAND_TIMEOUT_SECONDS)
    process.wait(timeout=remaining)
    return bytes(chunks["stdout"]), bytes(chunks["stderr"])


def _terminate_and_reap(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    _close_process_pipes(process)
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired as exc:
            raise ConformanceError("conformance failed") from exc
    if _process_group_exists(process.pid):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        _settle_process_group(process.pid)


def _process_group_exists(process_group: int) -> bool:
    try:
        os.killpg(process_group, 0)
    except ProcessLookupError:
        return False
    return True


def _settle_process_group(process_group: int) -> None:
    for _ in range(100):
        if not _process_group_exists(process_group):
            return
        time.sleep(0.01)
    raise ConformanceError("conformance failed")


def _close_process_pipes(process: subprocess.Popen[bytes]) -> None:
    for stream in (process.stdout, process.stderr):
        if stream is not None:
            stream.close()


def _image(value: object, repository: str) -> bool:
    if not isinstance(value, str):
        return False
    prefix = f"{repository}@sha256:"
    return value.startswith(prefix) and _HEX_64.fullmatch(value[len(prefix) :]) is not None


def _origin(value: object) -> bool:
    if not isinstance(value, str) or any(character in value for character in "\r\n\0"):
        return False
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        return False
    if (
        parsed.scheme != "https"
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
        or port not in (None, 443)
    ):
        return False
    try:
        return ipaddress.ip_address(parsed.hostname).is_global
    except ValueError:
        return _HOST.fullmatch(parsed.hostname) is not None


def _docker(endpoint: str, *arguments: str) -> tuple[str, ...]:
    return ("docker", "-H", endpoint, *arguments)


def _run(runner: Runner, argv: tuple[str, ...], *, allow_failure: bool = False) -> CommandResult:
    try:
        result = runner.run(argv, check=False, env=dict(_ROOT_ENV))
    except BaseException as exc:
        if isinstance(exc, ConformanceError):
            raise
        raise ConformanceError("conformance failed") from exc
    if result.returncode != 0 and not allow_failure:
        raise ConformanceError("conformance failed")
    return result


def _object_id(result: CommandResult) -> str:
    identifier = result.stdout.strip()
    if _HEX_64.fullmatch(identifier) is None:
        raise ConformanceError("conformance failed")
    return identifier


def _expect(runner: Runner, argv: tuple[str, ...], expected: str) -> None:
    if _run(runner, argv).stdout.strip() != expected:
        raise ConformanceError("conformance failed")


def _expect_platform(runner: Runner, endpoint: str, image: str) -> None:
    _expect(
        runner,
        _docker(endpoint, "image", "inspect", "--format", "{{.Os}}/{{.Architecture}}", image),
        "linux/arm64",
    )


def _assert_absent(runner: Runner, endpoint: str, kind: str, name: str) -> None:
    result = _run(runner, _docker(endpoint, kind, "inspect", name), allow_failure=True)
    if result.returncode == 0:
        raise ConformanceError("conformance failed")


def _cleanup(runner: Runner, created: list[tuple[str, str, str]]) -> None:
    cleanup_failure: BaseException | None = None
    for endpoint, kind, identifier in reversed(created):
        command = _docker(endpoint, "rm", "-f", identifier)
        if kind == "network":
            command = _docker(endpoint, "network", "rm", identifier)
        try:
            if _run(runner, command, allow_failure=True).returncode != 0:
                raise ConformanceError("conformance cleanup failed")
        except BaseException as exc:
            if cleanup_failure is None:
                cleanup_failure = exc
    for command in (
        _docker(_NATIVE_ENDPOINT, "ps", "-aq", "--filter", f"label={_MANAGED_LABEL}"),
        _docker(_NATIVE_ENDPOINT, "network", "ls", "-q", "--filter", f"label={_MANAGED_LABEL}"),
    ):
        try:
            if _run(runner, command).stdout.strip():
                raise ConformanceError("conformance cleanup failed")
        except BaseException as exc:
            if cleanup_failure is None:
                cleanup_failure = exc
    if cleanup_failure is not None:
        raise ConformanceError("conformance cleanup failed") from cleanup_failure


def run_conformance(inputs: ConformanceInputs, runner: Runner) -> dict[str, object]:
    """Run the exact two-sandbox gVisor KVM probe and return a public receipt."""
    created: list[tuple[str, str, str]] = []
    try:
        for endpoint, kind, name in (
            (_NATIVE_ENDPOINT, "network", _NETWORK_NAME),
            (_NATIVE_ENDPOINT, "network", _DENIED_NETWORK_NAME),
            (_NATIVE_ENDPOINT, "container", _BUILDKIT_NAME),
            (_NATIVE_ENDPOINT, "container", _CLIENT_NAME),
            (_NATIVE_ENDPOINT, "container", _DENIAL_NAME),
            (_PRIMARY_ENDPOINT, "container", _FOREIGN_CLIENT_NAME),
        ):
            _assert_absent(runner, endpoint, kind, name)
        _expect_platform(runner, _NATIVE_ENDPOINT, inputs.builder_image)
        _expect_platform(runner, _PRIMARY_ENDPOINT, inputs.agent_image)

        provider_network_id = _object_id(
            _run(
                runner,
                _docker(
                    _NATIVE_ENDPOINT,
                    "network",
                    "create",
                    "--driver",
                    "bridge",
                    "--subnet",
                    "172.28.0.0/24",
                    "--label",
                    _MANAGED_LABEL,
                    _NETWORK_NAME,
                ),
            )
        )
        created.append((_NATIVE_ENDPOINT, "network", provider_network_id))
        denied_network_id = _object_id(
            _run(
                runner,
                _docker(
                    _NATIVE_ENDPOINT,
                    "network",
                    "create",
                    "--driver",
                    "bridge",
                    "--subnet",
                    "172.28.1.0/24",
                    "--label",
                    _MANAGED_LABEL,
                    _DENIED_NETWORK_NAME,
                ),
            )
        )
        created.append((_NATIVE_ENDPOINT, "network", denied_network_id))

        buildkit_id = _object_id(
            _run(
                runner,
                _docker(
                    _NATIVE_ENDPOINT,
                    "create",
                    "--platform",
                    "linux/arm64",
                    "--name",
                    _BUILDKIT_NAME,
                    "--runtime",
                    "runsc-personal-dev-native",
                    "--network",
                    _NETWORK_NAME,
                    "--label",
                    _MANAGED_LABEL,
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
                    inputs.builder_image,
                    "--native-tcp-buildkit-child",
                ),
            )
        )
        created.append((_NATIVE_ENDPOINT, "container", buildkit_id))
        _run(runner, _docker(_NATIVE_ENDPOINT, "start", buildkit_id))
        for attempt in range(60):
            logs = _run(runner, _docker(_NATIVE_ENDPOINT, "logs", buildkit_id), allow_failure=True)
            workers = _run(
                runner,
                _docker(
                    _NATIVE_ENDPOINT,
                    "exec",
                    buildkit_id,
                    "buildctl",
                    "--addr",
                    "tcp://127.0.0.1:1234",
                    "debug",
                    "workers",
                ),
                allow_failure=True,
            )
            if logs.returncode == 0 and "loom-buildkitd-native-child-preflight nnp=1" in logs.stdout and workers.returncode == 0:
                break
            if attempt == 59:
                raise ConformanceError("conformance failed")
            if isinstance(runner, SubprocessRunner):
                time.sleep(1)
        buildkit_ip = _run(
            runner,
            _docker(
                _NATIVE_ENDPOINT,
                "inspect",
                "--format",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                buildkit_id,
            ),
        ).stdout.strip()
        if ipaddress.ip_address(buildkit_ip) not in ipaddress.ip_network("172.28.0.0/24"):
            raise ConformanceError("conformance failed")
        _run(runner, ("/usr/bin/python3", "-c", _HOST_DENIAL_PROGRAM, buildkit_ip))

        foreign_id = _object_id(
            _run(
                runner,
                _docker(
                    _PRIMARY_ENDPOINT,
                    "create",
                    "--platform",
                    "linux/arm64",
                    "--name",
                    _FOREIGN_CLIENT_NAME,
                    "--network",
                    "bridge",
                    "--label",
                    _MANAGED_LABEL,
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
                    "268435456",
                    "--pids-limit",
                    "64",
                    "--tmpfs",
                    "/tmp:rw,nosuid,nodev,noexec,size=16777216,mode=0700",
                    "--entrypoint",
                    "python",
                    inputs.agent_image,
                    "-c",
                    _FOREIGN_DENIAL_PROGRAM,
                    buildkit_ip,
                ),
            )
        )
        created.append((_PRIMARY_ENDPOINT, "container", foreign_id))
        _run(runner, _docker(_PRIMARY_ENDPOINT, "start", "-a", foreign_id))
        _expect(
            runner,
            _docker(_PRIMARY_ENDPOINT, "inspect", "--format", "{{.State.ExitCode}}", foreign_id),
            "0",
        )

        denial_id = _object_id(
            _run(
                runner,
                _docker(
                    _NATIVE_ENDPOINT,
                    "create",
                    "--platform",
                    "linux/arm64",
                    "--name",
                    _DENIAL_NAME,
                    "--runtime",
                    "runsc-personal-dev-native",
                    "--network",
                    _DENIED_NETWORK_NAME,
                    "--ip",
                    "172.28.1.10",
                    "--label",
                    _MANAGED_LABEL,
                    "--read-only",
                    "--user",
                    "1000:1000",
                    "--cgroup-parent",
                    "loom-personal-dev-builder.slice",
                    "--cap-drop",
                    "ALL",
                    "--security-opt",
                    "no-new-privileges:true",
                    "--cpus",
                    "1",
                    "--memory",
                    "1073741824",
                    "--memory-swap",
                    "1073741824",
                    "--pids-limit",
                    "64",
                    "--tmpfs",
                    "/tmp:rw,nosuid,nodev,noexec,size=67108864,mode=0700,uid=1000,gid=1000",
                    "--entrypoint",
                    "/usr/bin/python3",
                    inputs.builder_image,
                    "-m",
                    "http.server",
                    "1234",
                    "--bind",
                    "0.0.0.0",
                ),
            )
        )
        created.append((_NATIVE_ENDPOINT, "container", denial_id))
        _run(runner, _docker(_NATIVE_ENDPOINT, "start", denial_id))
        for attempt in range(60):
            ready = _run(
                runner,
                _docker(
                    _NATIVE_ENDPOINT,
                    "exec",
                    denial_id,
                    "/usr/bin/python3",
                    "-c",
                    _DENIAL_READY_PROGRAM,
                ),
                allow_failure=True,
            )
            if ready.returncode == 0:
                break
            if attempt == 59:
                raise ConformanceError("conformance failed")
            if isinstance(runner, SubprocessRunner):
                time.sleep(1)

        client_id = _object_id(
            _run(
                runner,
                _docker(
                    _NATIVE_ENDPOINT,
                    "create",
                    "--platform",
                    "linux/arm64",
                    "--name",
                    _CLIENT_NAME,
                    "--runtime",
                    "runsc-personal-dev-native",
                    "--network",
                    _NETWORK_NAME,
                    "--label",
                    _MANAGED_LABEL,
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
                    "17179869184",
                    "--pids-limit",
                    "1024",
                    "--tmpfs",
                    "/tmp:rw,nosuid,nodev,noexec,size=1073741824,mode=1777",
                    "--tmpfs",
                    "/workspace:rw,nosuid,nodev,size=2147483648,mode=0700,uid=1000,gid=1000",
                    "--entrypoint",
                    "/usr/bin/python3",
                    inputs.builder_image,
                    "-c",
                    _CLIENT_PROGRAM,
                    inputs.public_https,
                ),
            )
        )
        created.append((_NATIVE_ENDPOINT, "container", client_id))
        if client_id == buildkit_id:
            raise ConformanceError("conformance failed")
        _run(runner, _docker(_NATIVE_ENDPOINT, "start", "-a", client_id))
        _expect(
            runner,
            _docker(_NATIVE_ENDPOINT, "inspect", "--format", "{{.State.ExitCode}}", client_id),
            "0",
        )
        _expect(
            runner,
            _docker(_NATIVE_ENDPOINT, "inspect", "--format", "{{.HostConfig.Runtime}}", buildkit_id),
            "runsc-personal-dev-native",
        )
        _expect(
            runner,
            _docker(_NATIVE_ENDPOINT, "inspect", "--format", "{{.HostConfig.Runtime}}", client_id),
            "runsc-personal-dev-native",
        )
        for identifier, expected in ((buildkit_id, "3000000000"), (client_id, "1000000000")):
            _expect(runner, _docker(_NATIVE_ENDPOINT, "inspect", "--format", "{{.HostConfig.CgroupParent}}", identifier), "loom-personal-dev-builder.slice")
            _expect(runner, _docker(_NATIVE_ENDPOINT, "inspect", "--format", "{{.HostConfig.NanoCpus}}", identifier), expected)
            _expect(runner, _docker(_NATIVE_ENDPOINT, "inspect", "--format", "{{.HostConfig.Memory}}", identifier), "17179869184")
            _expect(runner, _docker(_NATIVE_ENDPOINT, "inspect", "--format", "{{json .HostConfig.Devices}}", identifier), "[]")
            _expect(runner, _docker(_NATIVE_ENDPOINT, "inspect", "--format", "{{json .HostConfig.Binds}}", identifier), "null")
    except BaseException as primary_failure:
        try:
            _cleanup(runner, created)
        except BaseException as cleanup_failure:
            raise ConformanceError("conformance cleanup failed") from cleanup_failure
        if isinstance(primary_failure, ConformanceError):
            raise
        raise ConformanceError("conformance failed") from primary_failure

    _cleanup(runner, created)
    return {
        "schema": "loom-personal-dev-native-builder-conformance-v1",
        "status": "passed",
        "runtime": "runsc-personal-dev-native",
        "platform": "linux/arm64",
        "architecture": "arm64",
        "buildkit_sandbox_id": buildkit_id,
        "client_sandbox_id": client_id,
        "public_https": "allowed",
        "host_to_provider": "denied",
        "foreign_to_provider": "denied",
        "private_control_plane": "denied",
        "cross_provider_network": "denied",
        "managed_containers_after": 0,
        "managed_networks_after": 0,
    }
