"""Fixed, import-only conformance probe for the personal native builder."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
import selectors
import signal
import subprocess
import threading
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Protocol, cast, runtime_checkable
from urllib.parse import urlsplit

_NATIVE_ENDPOINT = "unix:///run/loom-personal-dev-builder/docker.sock"
_PRIMARY_ENDPOINT = "unix:///var/run/docker.sock"
_MANAGED_LABEL = "loom.personal-dev-native-builder.managed=true"
_INVOCATION_LABEL_KEY = "loom.personal-dev-native-builder.conformance-invocation"
_SPEC_LABEL_KEY = "loom.personal-dev-native-builder.conformance-spec"
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
        pending_signal: int | None = None
        caught: BaseException | None = None
        cleanup_failure: BaseException | None = None
        group_leaked = False
        stdout = b""
        stderr = b""

        def record_pending_signal(signum: int) -> None:
            nonlocal pending_signal
            if pending_signal is None:
                pending_signal = signum

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
                previous_handlers = _forward_signals(
                    process,
                    record_pending_signal,
                )
            stdout, stderr = _drain_bounded(process)
            group_leaked = _process_group_has_other_members(process.pid)
            if not group_leaked:
                process.wait()
        except BaseException as exc:
            caught = exc
        if process is not None and (caught is not None or group_leaked):
            try:
                _terminate_and_reap(process)
            except BaseException as exc:
                cleanup_failure = exc

        selected_cleanup_failure = (
            ConformanceError("conformance failed")
            if cleanup_failure is not None
            else None
        )
        if selected_cleanup_failure is not None:
            # Cleanup failure is terminal; keep the non-raising recorder installed
            # while the selected error propagates to the owning boundary.
            raise selected_cleanup_failure from cleanup_failure
        restoration_failure: BaseException | None = None
        for restored_signum, handler in previous_handlers.items():
            try:
                signal.signal(restored_signum, cast(signal.Handlers, handler))
            except BaseException as exc:
                if restoration_failure is None:
                    restoration_failure = exc

        if restoration_failure is not None:
            raise restoration_failure
        if pending_signal is not None:
            handler = previous_handlers[pending_signal]
            if handler == signal.SIG_DFL:
                signal.raise_signal(pending_signal)
            elif handler != signal.SIG_IGN and callable(handler):
                cast(Callable[[int, object], object], handler)(pending_signal, None)
        if group_leaked:
            raise ConformanceError("conformance failed")
        if pending_signal is not None:
            raise ConformanceError("conformance failed")
        if caught is not None:
            if isinstance(
                caught,
                (subprocess.TimeoutExpired, _CommandOutputError),
            ):
                raise ConformanceError("conformance failed") from caught
            raise caught
        if process is None:
            raise ConformanceError("conformance failed")
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


def _forward_signals(
    process: subprocess.Popen[bytes],
    record: Callable[[int], None],
) -> dict[int, object]:
    previous: dict[int, object] = {}

    def forward(signum: int, _frame: object) -> None:
        if process.returncode is None:
            try:
                os.killpg(process.pid, signum)
            except ProcessLookupError:
                pass
        record(signum)

    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
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
    _wait_without_reaping(process, remaining)
    return bytes(chunks["stdout"]), bytes(chunks["stderr"])


def _wait_without_reaping(process: subprocess.Popen[bytes], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        observed = os.waitid(
            os.P_PID,
            process.pid,
            os.WEXITED | os.WNOHANG | os.WNOWAIT,
        )
        if observed is not None:
            return
        if time.monotonic() >= deadline:
            raise subprocess.TimeoutExpired(process.args, timeout)
        time.sleep(0.01)


def _terminate_and_reap(process: subprocess.Popen[bytes]) -> None:
    try:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        _close_process_pipes(process)
        try:
            _wait_without_reaping(process, 2)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                _wait_without_reaping(process, 2)
            except subprocess.TimeoutExpired as exc:
                raise ConformanceError("conformance failed") from exc
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        _wait_for_other_group_members(process.pid, 2)
    finally:
        process.wait()


def _process_group_exists(process_group: int) -> bool:
    try:
        os.killpg(process_group, 0)
    except ProcessLookupError:
        return False
    return True


def _process_group_has_other_members(process_group: int) -> bool:
    try:
        entries = tuple(os.scandir("/proc"))
    except OSError:
        return True
    for entry in entries:
        if not entry.name.isdecimal() or int(entry.name) == process_group:
            continue
        try:
            with open(f"/proc/{entry.name}/stat", encoding="ascii") as stream:
                stat_payload = stream.read()
            fields = stat_payload[stat_payload.rindex(")") + 2 :].split()
            member_group = int(fields[2])
        except (OSError, ValueError, IndexError):
            continue
        if member_group == process_group:
            return True
    return False


def _wait_for_other_group_members(process_group: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while _process_group_has_other_members(process_group):
        if time.monotonic() >= deadline:
            raise ConformanceError("conformance failed")
        time.sleep(0.01)


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


def _create_parts(
    argv: tuple[str, ...],
    kind: str,
) -> tuple[dict[str, list[str]], str, tuple[str, ...]]:
    value_options = {
        "--cap-add",
        "--cap-drop",
        "--cgroup-parent",
        "--cpus",
        "--driver",
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
        "--subnet",
        "--tmpfs",
        "--user",
    }
    options: dict[str, list[str]] = {}
    if kind == "network":
        values = argv[5:-1]
        terminal = argv[-1]
    else:
        values = argv[4:]
        terminal = ""
    index = 0
    while index < len(values) and values[index].startswith("--"):
        option = values[index]
        if option == "--read-only":
            options.setdefault(option, []).append("true")
            index += 1
            continue
        if option not in value_options or index + 1 >= len(values):
            raise ConformanceError("conformance failed")
        options.setdefault(option, []).append(values[index + 1])
        index += 2
    if kind == "network":
        if index != len(values):
            raise ConformanceError("conformance failed")
        return options, terminal, ()
    if index >= len(values):
        raise ConformanceError("conformance failed")
    return options, values[index], tuple(values[index + 1 :])


def _owned_create_command(
    argv: tuple[str, ...],
    invocation: str,
) -> tuple[tuple[str, ...], str]:
    specification = hashlib.sha256(
        json.dumps(argv, ensure_ascii=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()
    label_index = argv.index("--label")
    owned = (
        *argv[: label_index + 2],
        "--label",
        f"{_INVOCATION_LABEL_KEY}={invocation}",
        "--label",
        f"{_SPEC_LABEL_KEY}={specification}",
        *argv[label_index + 2 :],
    )
    return owned, specification


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ConformanceError("conformance failed")
    return cast(dict[str, object], value)


def _strings(value: object) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ConformanceError("conformance failed")
    return cast(list[str], value)


def _inspect_matches_create(
    value: dict[str, object],
    argv: tuple[str, ...],
    kind: str,
) -> bool:
    try:
        options, image_or_name, command = _create_parts(argv, kind)
        if kind == "network":
            labels = _mapping(value.get("Labels"))
            ipam = _mapping(value.get("IPAM"))
            configurations = ipam.get("Config")
            if not isinstance(configurations, list):
                return False
            subnets = [_mapping(configuration).get("Subnet") for configuration in configurations]
            return (
                value.get("Name") == image_or_name
                and value.get("Driver") == options["--driver"][0]
                and subnets == options["--subnet"]
                and all(
                    labels.get(label.split("=", 1)[0]) == label.split("=", 1)[1]
                    for label in options["--label"]
                )
            )
        configuration = _mapping(value.get("Config"))
        host = _mapping(value.get("HostConfig"))
        network_settings = _mapping(value.get("NetworkSettings"))
        networks = _mapping(network_settings.get("Networks"))
        network_name = options["--network"][0]
        attached = _mapping(networks.get(network_name))
        labels = _mapping(configuration.get("Labels"))
        tmpfs = _mapping(host.get("Tmpfs"))
        expected_tmpfs = {
            item.split(":", 1)[0]: item.split(":", 1)[1] for item in options.get("--tmpfs", [])
        }
        expected_entrypoint = options.get("--entrypoint", [])
        observed_entrypoint = configuration.get("Entrypoint")
        if isinstance(observed_entrypoint, str):
            observed_entrypoint = [observed_entrypoint]
        if (
            value.get("Name") != f"/{options['--name'][0]}"
            or configuration.get("Image") != image_or_name
            or observed_entrypoint != expected_entrypoint
            or configuration.get("Cmd") != list(command)
            or configuration.get("User", "") != options.get("--user", [""])[0]
            or configuration.get("Hostname", "")
            != options.get("--hostname", [configuration.get("Hostname", "")])[0]
            or host.get("NetworkMode") != network_name
            or host.get("ReadonlyRootfs") is not True
            or host.get("CapDrop") != options.get("--cap-drop", [])
            or host.get("CapAdd", []) != options.get("--cap-add", [])
            or host.get("SecurityOpt") != options.get("--security-opt", [])
            or host.get("CgroupParent", "") != options.get("--cgroup-parent", [""])[0]
            or host.get("NanoCpus") != int(float(options["--cpus"][0]) * 1_000_000_000)
            or host.get("Memory") != int(options["--memory"][0])
            or host.get("MemorySwap") != int(options["--memory-swap"][0])
            or host.get("PidsLimit") != int(options["--pids-limit"][0])
            or tmpfs != expected_tmpfs
            or ("--runtime" in options and host.get("Runtime") != options["--runtime"][0])
            or any(
                labels.get(label.split("=", 1)[0]) != label.split("=", 1)[1]
                for label in options["--label"]
            )
        ):
            return False
        aliases = attached.get("Aliases")
        if "--network-alias" in options and not all(
            alias in _strings(aliases) for alias in options["--network-alias"]
        ):
            return False
        if "--ip" in options:
            ipam_configuration = _mapping(attached.get("IPAMConfig"))
            if ipam_configuration.get("IPv4Address") != options["--ip"][0]:
                return False
        return options.get("--platform") == ["linux/arm64"]
    except (ConformanceError, KeyError, TypeError, ValueError):
        return False


class _InspectDisposition(Enum):
    ABSENT = "absent"
    INDETERMINATE = "indeterminate"


def _is_exact_not_found(
    result: CommandResult,
    kind: str,
    name: str,
) -> bool:
    expected = (
        f"Error response from daemon: network {name} not found\n"
        if kind == "network"
        else f"Error response from daemon: No such container: {name}\n"
    )
    return result.returncode == 1 and result.stdout == "" and result.stderr == expected


def _inspect_owned_create(
    runner: Runner,
    endpoint: str,
    kind: str,
    name: str,
    argv: tuple[str, ...],
    invocation: str,
    specification: str,
) -> tuple[str, bool] | _InspectDisposition:
    exact_absences = 0
    for _attempt in range(2):
        try:
            result = _run(
                runner,
                _docker(endpoint, kind, "inspect", "--format", "{{json .}}", name),
                allow_failure=True,
            )
        except ConformanceError:
            continue
        if result.returncode != 0:
            if _is_exact_not_found(result, kind, name):
                exact_absences += 1
            continue
        try:
            value = json.loads(result.stdout)
            identity = _mapping(value)
            identifier = identity.get("Id")
            labels = _mapping(
                identity.get("Labels")
                if kind == "network"
                else _mapping(identity.get("Config")).get("Labels")
            )
        except (ConformanceError, json.JSONDecodeError, TypeError, ValueError):
            continue
        if (
            not isinstance(identifier, str)
            or _HEX_64.fullmatch(identifier) is None
            or labels.get("loom.personal-dev-native-builder.managed") != "true"
            or labels.get(_INVOCATION_LABEL_KEY) != invocation
            or labels.get(_SPEC_LABEL_KEY) != specification
        ):
            continue
        return identifier, _inspect_matches_create(identity, argv, kind)
    if exact_absences == 2:
        return _InspectDisposition.ABSENT
    return _InspectDisposition.INDETERMINATE


def _create_owned(
    runner: Runner,
    created: list[tuple[str, str, str]],
    *,
    endpoint: str,
    kind: str,
    name: str,
    argv: tuple[str, ...],
    invocation: str,
) -> str:
    owned_argv, specification = _owned_create_command(argv, invocation)
    result: CommandResult | None = None
    primary_failure: BaseException | None = None
    try:
        result = _run(runner, owned_argv, allow_failure=True)
    except BaseException as exc:
        primary_failure = exc
    returned_identifier = "" if result is None else result.stdout.strip()
    if result is None or result.returncode != 0 or _HEX_64.fullmatch(returned_identifier) is None:
        if primary_failure is None:
            primary_failure = ConformanceError("conformance failed")
    reconciled = _inspect_owned_create(
        runner,
        endpoint,
        kind,
        name,
        owned_argv,
        invocation,
        specification,
    )
    if isinstance(reconciled, _InspectDisposition):
        if primary_failure is not None:
            raise primary_failure
        raise ConformanceError("conformance failed")
    identifier, complete = reconciled
    created.append((endpoint, kind, identifier))
    if not complete or (primary_failure is None and returned_identifier != identifier):
        raise ConformanceError("conformance failed")
    if primary_failure is not None:
        raise primary_failure
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
    if not _is_exact_not_found(result, kind, name):
        raise ConformanceError("conformance failed")


def _cleanup(
    runner: Runner,
    created: list[tuple[str, str, str]],
    verified_absent: list[tuple[str, str, str]],
) -> None:
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
    for endpoint, kind, name in verified_absent:
        try:
            _assert_absent(runner, endpoint, kind, name)
        except BaseException as exc:
            if cleanup_failure is None:
                cleanup_failure = exc
    if cleanup_failure is not None:
        raise ConformanceError("conformance cleanup failed") from cleanup_failure


def run_conformance(inputs: ConformanceInputs, runner: Runner) -> dict[str, object]:
    """Run the exact two-sandbox gVisor KVM probe and return a public receipt."""
    created: list[tuple[str, str, str]] = []
    verified_absent: list[tuple[str, str, str]] = []
    invocation = uuid.uuid4().hex
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
            verified_absent.append((endpoint, kind, name))
        _expect_platform(runner, _NATIVE_ENDPOINT, inputs.builder_image)
        _expect_platform(runner, _PRIMARY_ENDPOINT, inputs.agent_image)

        _create_owned(
            runner,
            created,
            endpoint=_NATIVE_ENDPOINT,
            kind="network",
            name=_NETWORK_NAME,
            argv=_docker(
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
            invocation=invocation,
        )
        _create_owned(
            runner,
            created,
            endpoint=_NATIVE_ENDPOINT,
            kind="network",
            name=_DENIED_NETWORK_NAME,
            argv=_docker(
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
            invocation=invocation,
        )

        buildkit_id = _create_owned(
            runner,
            created,
            endpoint=_NATIVE_ENDPOINT,
            kind="container",
            name=_BUILDKIT_NAME,
            argv=_docker(
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
            invocation=invocation,
        )
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

        foreign_id = _create_owned(
            runner,
            created,
            endpoint=_PRIMARY_ENDPOINT,
            kind="container",
            name=_FOREIGN_CLIENT_NAME,
            argv=_docker(
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
            invocation=invocation,
        )
        _run(runner, _docker(_PRIMARY_ENDPOINT, "start", "-a", foreign_id))
        _expect(
            runner,
            _docker(_PRIMARY_ENDPOINT, "inspect", "--format", "{{.State.ExitCode}}", foreign_id),
            "0",
        )

        denial_id = _create_owned(
            runner,
            created,
            endpoint=_NATIVE_ENDPOINT,
            kind="container",
            name=_DENIAL_NAME,
            argv=_docker(
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
            invocation=invocation,
        )
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

        client_id = _create_owned(
            runner,
            created,
            endpoint=_NATIVE_ENDPOINT,
            kind="container",
            name=_CLIENT_NAME,
            argv=_docker(
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
            invocation=invocation,
        )
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
            _cleanup(runner, created, verified_absent)
        except BaseException as cleanup_failure:
            raise ConformanceError("conformance cleanup failed") from cleanup_failure
        if isinstance(primary_failure, ConformanceError):
            raise
        raise ConformanceError("conformance failed") from primary_failure

    _cleanup(runner, created, verified_absent)
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
