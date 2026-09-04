"""Bounded authenticated localhost transport for capacity configuration."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import selectors
import socket
import ssl
import stat
import subprocess
import tempfile
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO, Protocol, TextIO
from uuid import UUID

import httpcore
import httpx
from pydantic import ValidationError

from loom_capacity_manager.executable_contracts import (
    ExecutionContextV2,
    ExecutionPreparationAbortV2,
    ExecutionPreparationV2,
)
from loom_capacity_manager.preparation_readiness import (
    PreparedExecutionReadinessV2,
    canonical_prepared_readiness_digest,
)
from loom_cli.rollout.credential_authority import (
    TrustedFileRead,
    converge_new_private_file,
    read_trusted_file,
)

from .redaction import redact_rollout_text

_NAMESPACE = "loom-dev"
_REMOTE_PORT = 8443
_START_TIMEOUT_SECONDS = 15.0
_STOP_TIMEOUT_SECONDS = 5.0
_KUBECTL_TIMEOUT_SECONDS = 15.0
_HTTP_TIMEOUT_SECONDS = 10.0
_MAX_KUBECTL_OUTPUT_BYTES = 1024 * 1024
_MAX_REQUEST_BYTES = 8 * 1024 * 1024
_MAX_RESPONSE_BYTES = 8 * 1024 * 1024
_MAX_CREDENTIAL_BYTES = 1024 * 1024
_DIAGNOSTIC_LIMIT = 8 * 1024
_MAX_TUNNEL_LINE_CHARS = 4096
_ABSOLUTE_DEADLINE_EXTENSION = "loom_absolute_deadline"
_KUBECONFIG_SNAPSHOT_DIRECTORY = "capacity-manager-kubeconfig-snapshots"
_KUBECONFIG_SNAPSHOT_PREFIX = ".capacity-manager-kubeconfig-"
_POD_NAME = re.compile(r"^[a-z0-9](?:[-a-z0-9.]{0,251}[a-z0-9])?$")
_POD_LABELS = {
    "app.kubernetes.io/name": "loom-capacity-manager",
    "app.kubernetes.io/part-of": "loom",
    "loom.yylx.dev/capacity-component": "manager",
}
_CREDENTIAL_NAMES = frozenset(
    {
        "configuration-read",
        "configuration-fleet",
        "configuration-subject",
        "configuration-activate",
        "manager-abort",
        "manager-prepare",
        "manager-read",
    }
)
_CREDENTIAL_FILES = frozenset(
    {"bearer-token", "certificate.pem", "manager-ca.pem", "private-key.pem"}
)


class ManagerCommandRunner(Protocol):
    @property
    def environment(self) -> Mapping[str, str]: ...

    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes: ...


class TunnelProcess(Protocol):
    stdout: BinaryIO | TextIO | None
    stderr: BinaryIO | TextIO | None

    def poll(self) -> int | None: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...

    def wait(self, timeout: float | None = None) -> int: ...


class HTTPResponse(Protocol):
    status_code: int
    headers: Mapping[str, str]

    def iter_bytes(self, *, chunk_size: int) -> Iterator[bytes]: ...


class HTTPStream(Protocol):
    def __enter__(self) -> HTTPResponse: ...

    def __exit__(self, *args: object) -> object: ...


class HTTPClient(Protocol):
    def stream(self, method: str, url: str, **kwargs: object) -> HTTPStream: ...

    def close(self) -> None: ...


Spawn = Callable[[Sequence[str], Mapping[str, str]], TunnelProcess]
PortAllocator = Callable[[], int]
WaitReady = Callable[[TunnelProcess, int, "_BoundedChildOutputCapture"], None]
ClientFactory = Callable[[ssl.SSLContext], HTTPClient]


class _AbsoluteDeadlineNetworkStream(httpcore.NetworkStream):
    """Cap every blocking network operation by one monotonic deadline."""

    def __init__(
        self,
        stream: httpcore.NetworkStream,
        deadline: Callable[[float | None, type[httpcore.TimeoutException]], float],
    ) -> None:
        self._stream = stream
        self._deadline = deadline

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        return self._stream.read(max_bytes, self._deadline(timeout, httpcore.ReadTimeout))

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        raw_socket = self._stream.get_extra_info("socket")
        if not isinstance(raw_socket, socket.socket):
            self._stream.write(buffer, self._deadline(timeout, httpcore.WriteTimeout))
            return
        try:
            while buffer:
                raw_socket.settimeout(self._deadline(timeout, httpcore.WriteTimeout))
                sent = raw_socket.send(buffer)
                if sent <= 0:
                    raise OSError("capacity manager connection stopped accepting request data")
                buffer = buffer[sent:]
        except httpcore.TimeoutException:
            raise
        except TimeoutError as exc:
            raise httpcore.WriteTimeout("capacity manager request deadline expired") from exc
        except OSError as exc:
            raise httpcore.WriteError("capacity manager request write failed") from exc

    def close(self) -> None:
        self._stream.close()

    def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> httpcore.NetworkStream:
        stream = self._stream.start_tls(
            ssl_context,
            server_hostname,
            self._deadline(timeout, httpcore.ConnectTimeout),
        )
        return _AbsoluteDeadlineNetworkStream(stream, self._deadline)

    def get_extra_info(self, info: str) -> object:
        return self._stream.get_extra_info(info)


class _AbsoluteDeadlineNetworkBackend(httpcore.NetworkBackend):
    """Bind HTTPCore's per-operation timeouts to one request budget."""

    def __init__(self) -> None:
        self._backend = httpcore.SyncBackend()
        self._expires_at: float | None = None

    def arm(self, expires_at: float) -> None:
        self._expires_at = expires_at
        self.remaining(None, httpcore.TimeoutException)

    def remaining(
        self,
        requested: float | None,
        timeout_error: type[httpcore.TimeoutException],
    ) -> float:
        if self._expires_at is None:
            raise RuntimeError("capacity manager request deadline is unavailable")
        remaining = self._expires_at - time.monotonic()
        if remaining <= 0:
            raise timeout_error("capacity manager request deadline expired")
        return remaining if requested is None else min(requested, remaining)

    def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.NetworkStream:
        stream = self._backend.connect_tcp(
            host,
            port,
            self.remaining(timeout, httpcore.ConnectTimeout),
            local_address,
            socket_options,
        )
        return _AbsoluteDeadlineNetworkStream(stream, self.remaining)

    def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.NetworkStream:
        stream = self._backend.connect_unix_socket(
            path,
            self.remaining(timeout, httpcore.ConnectTimeout),
            socket_options,
        )
        return _AbsoluteDeadlineNetworkStream(stream, self.remaining)


class _AbsoluteDeadlineHTTPTransport(httpx.HTTPTransport):
    """Arm the network backend from the request's absolute-deadline extension."""

    def __init__(self, context: ssl.SSLContext) -> None:
        super().__init__(verify=context, trust_env=False)
        self._deadline_backend = _AbsoluteDeadlineNetworkBackend()
        self._pool = httpcore.ConnectionPool(
            ssl_context=context,
            max_connections=1,
            max_keepalive_connections=0,
            http1=True,
            http2=False,
            retries=0,
            network_backend=self._deadline_backend,
        )

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        expires_at = request.extensions.get(_ABSOLUTE_DEADLINE_EXTENSION)
        if isinstance(expires_at, bool) or not isinstance(expires_at, (float, int)):
            raise httpx.TimeoutException("capacity manager request deadline is unavailable")
        self._deadline_backend.arm(float(expires_at))
        return super().handle_request(request)


class ProtectedCapacityManagerClientError(RuntimeError):
    """A bounded failure whose rendered form never contains private input."""

    def __init__(self, reason: str, diagnostic: str | None = None) -> None:
        messages = {
            "credential": "protected capacity manager client credential failure",
            "transport": "protected capacity manager client transport failure",
            "timeout": "protected capacity manager client timeout failure",
            "redirected": "protected capacity manager response was redirected",
            "oversized": "protected capacity manager response exceeds its size bound",
            "encoding": "protected capacity manager response encoding is invalid",
            "content-type": "protected capacity manager response content type is invalid",
            "json": "protected capacity manager response contains invalid JSON",
            "unexpected": "protected capacity manager response is unexpected",
        }
        if reason not in messages:
            raise ValueError("protected capacity manager failure reason is invalid")
        sanitized = (
            None
            if diagnostic is None
            else redact_rollout_text(diagnostic, limit=_DIAGNOSTIC_LIMIT).strip() or None
        )
        super().__init__(messages[reason])
        self.reason = reason
        self.diagnostic = sanitized


@dataclass(frozen=True, slots=True)
class ProtectedExecutionPreparationStatus:
    readiness: PreparedExecutionReadinessV2
    readiness_sha256: str


@dataclass(frozen=True, slots=True)
class ProtectedExecutionPreparationAbortResult:
    execution_epoch: int
    execution_manifest_sha256: str
    retired_at: datetime
    replayed: bool


class _BoundedChildOutputCapture:
    """Poll child descriptors without any unbounded reader-thread cleanup."""

    def __init__(
        self,
        process: TunnelProcess,
        port: int,
    ) -> None:
        self._streams = tuple(
            stream
            for stream in (
                getattr(process, "stdout", None),
                getattr(process, "stderr", None),
            )
            if stream is not None
        )
        self._expected = f"Forwarding from 127.0.0.1:{port} -> {_REMOTE_PORT}"
        self._observed_size = 0
        self._confirmed = False
        self._invalid = False
        self._pending_streams = len(self._streams)
        self._finished = False
        self._started = False
        self._selector = selectors.DefaultSelector()
        self._buffers: dict[int, bytearray] = {}
        self._descriptor_streams: dict[int, BinaryIO | TextIO] = {}

    def start(self) -> None:
        if self._started:
            raise RuntimeError("capacity manager output capture already started")
        self._started = True
        for stream in self._streams:
            try:
                descriptor = stream.fileno()
            except (AttributeError, OSError, ValueError):
                self._drain_memory_stream(stream)
                continue
            try:
                if descriptor in self._buffers:
                    raise ValueError("capacity manager output descriptors are duplicated")
                os.set_blocking(descriptor, False)
                self._selector.register(descriptor, selectors.EVENT_READ)
                self._buffers[descriptor] = bytearray()
                self._descriptor_streams[descriptor] = stream
            except (OSError, ValueError):
                self._invalid = True
                self._pending_streams -= 1

    def _record_chunk(self, chunk: bytes) -> None:
        self._observed_size += len(chunk)
        if self._observed_size > _MAX_KUBECTL_OUTPUT_BYTES:
            self._invalid = True

    def _record_line(self, line: bytes) -> None:
        if line.endswith(b"\r"):
            line = line[:-1]
        if len(line) > _MAX_TUNNEL_LINE_CHARS:
            self._invalid = True
            return
        if line.decode("utf-8", errors="replace") == self._expected:
            self._confirmed = True

    def _consume(self, descriptor: int, chunk: bytes) -> None:
        self._record_chunk(chunk)
        buffer = self._buffers[descriptor]
        buffer.extend(chunk)
        while True:
            newline = buffer.find(b"\n")
            if newline < 0:
                if len(buffer) > _MAX_TUNNEL_LINE_CHARS + 1:
                    self._invalid = True
                return
            line = bytes(buffer[:newline])
            del buffer[: newline + 1]
            self._record_line(line)

    def _drain_memory_stream(self, stream: BinaryIO | TextIO) -> None:
        try:
            payload = stream.read(_MAX_KUBECTL_OUTPUT_BYTES + 1)
            if isinstance(payload, str):
                chunk = payload.encode("utf-8", errors="replace")
            elif isinstance(payload, bytes):
                chunk = payload
            else:
                raise ValueError("capacity manager output stream is invalid")
            descriptor = -self._pending_streams
            self._buffers[descriptor] = bytearray()
            if chunk:
                self._consume(descriptor, chunk)
            if self._buffers.pop(descriptor):
                self._invalid = True
        except (OSError, ValueError):
            self._invalid = True
        finally:
            self._pending_streams -= 1

    def _close_descriptor(self, descriptor: int) -> None:
        if descriptor not in self._buffers:
            return
        try:
            self._selector.unregister(descriptor)
        except (KeyError, ValueError):
            pass
        buffer = self._buffers.pop(descriptor)
        stream = self._descriptor_streams.pop(descriptor)
        if buffer:
            self._invalid = True
        try:
            stream.close()
        except (OSError, ValueError):
            self._invalid = True
        finally:
            self._pending_streams -= 1

    def _drain_available(self) -> None:
        for descriptor in tuple(self._buffers):
            if descriptor < 0:
                continue
            while not self._invalid:
                try:
                    chunk = os.read(descriptor, 64 * 1024)
                except BlockingIOError:
                    break
                except OSError:
                    self._invalid = True
                    self._close_descriptor(descriptor)
                    break
                if not chunk:
                    self._close_descriptor(descriptor)
                    break
                self._consume(descriptor, chunk)

    def state(self) -> tuple[bool, bool, bool]:
        self._drain_available()
        return self._confirmed, self._invalid, self._pending_streams == 0

    def wait_for_change(self, timeout: float) -> None:
        self._selector.select(timeout=timeout)
        self._drain_available()

    def diagnostic(self) -> str | None:
        return None

    def finish(self) -> None:
        if self._finished:
            return
        self._finished = True
        self._drain_available()
        for descriptor in tuple(self._buffers):
            if descriptor >= 0:
                self._close_descriptor(descriptor)
        self._selector.close()


def _spawn(argv: Sequence[str], environment: Mapping[str, str]) -> TunnelProcess:
    return subprocess.Popen(  # type: ignore[return-value]
        list(argv),
        env=dict(environment),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
        close_fds=True,
        start_new_session=False,
    )


def _allocate_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    if type(port) is not int or not 1024 <= port <= 65535:
        raise RuntimeError("protected capacity manager local port is invalid")
    return port


def _wait_ready(
    process: TunnelProcess,
    port: int,
    output: _BoundedChildOutputCapture,
) -> None:
    deadline = time.monotonic() + _START_TIMEOUT_SECONDS
    while True:
        if process.poll() is not None:
            raise RuntimeError("capacity manager port-forward exited early")
        confirmed, invalid, closed = output.state()
        if invalid:
            raise RuntimeError("capacity manager port-forward output is invalid")
        if confirmed:
            if process.poll() is not None:
                raise RuntimeError("capacity manager port-forward exited early")
            return
        if closed:
            raise RuntimeError("capacity manager port-forward output closed early")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("capacity manager port-forward timed out")
        output.wait_for_change(min(0.05, remaining))


def _client(context: ssl.SSLContext) -> HTTPClient:
    return httpx.Client(  # type: ignore[return-value]
        transport=_AbsoluteDeadlineHTTPTransport(context),
        timeout=httpx.Timeout(
            _HTTP_TIMEOUT_SECONDS,
            connect=3.0,
            read=_HTTP_TIMEOUT_SECONDS,
            write=_HTTP_TIMEOUT_SECONDS,
            pool=3.0,
        ),
        trust_env=False,
        follow_redirects=False,
    )


def _stop_exact(process: TunnelProcess) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=_STOP_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        if process.poll() is None:
            process.kill()
            try:
                process.wait(timeout=_STOP_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError("capacity manager port-forward did not stop") from exc


def _json_object(payload: bytes, *, label: str) -> dict[str, object]:
    if not payload or len(payload) > _MAX_RESPONSE_BYTES:
        raise ValueError(f"{label} is not bounded")
    try:
        value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"{label} is invalid") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def _execution_context(value: Mapping[str, object]) -> ExecutionContextV2:
    try:
        context = ExecutionContextV2.model_validate_json(
            json.dumps(
                dict(value),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
        )
    except (TypeError, ValueError, ValidationError) as exc:
        raise ValueError("capacity manager execution context is invalid") from exc
    if (
        context.execution_state != "prepared"
        or context.executable_new_capacity_ceiling != 0
        or context.executable_new_capacity_rate_per_minute != 0
    ):
        raise ValueError("capacity manager execution context is not prepared")
    return context


def _execution_preparation_status(
    value: Mapping[str, object],
) -> ProtectedExecutionPreparationStatus:
    expected_fields = set(PreparedExecutionReadinessV2.model_fields) | {"readiness_sha256"}
    copied = dict(value)
    if set(copied) != expected_fields:
        raise ValueError("capacity manager execution preparation status is invalid")
    readiness_sha256 = copied.pop("readiness_sha256")
    if (
        not isinstance(readiness_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", readiness_sha256) is None
        or readiness_sha256 == "0" * 64
    ):
        raise ValueError("capacity manager execution preparation digest is invalid")
    try:
        readiness = PreparedExecutionReadinessV2.model_validate_json(
            json.dumps(
                copied,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
        )
    except (TypeError, ValueError, ValidationError) as exc:
        raise ValueError("capacity manager execution preparation status is invalid") from exc
    if canonical_prepared_readiness_digest(readiness) != readiness_sha256:
        raise ValueError("capacity manager execution preparation digest differs from status")
    return ProtectedExecutionPreparationStatus(
        readiness=readiness,
        readiness_sha256=readiness_sha256,
    )


def _execution_preparation_abort_result(
    value: Mapping[str, object],
) -> ProtectedExecutionPreparationAbortResult:
    copied = dict(value)
    if set(copied) != {
        "execution_epoch",
        "execution_manifest_sha256",
        "replayed",
        "retired_at",
    }:
        raise ValueError("capacity manager execution preparation abort result is invalid")
    execution_epoch = copied["execution_epoch"]
    execution_manifest_sha256 = copied["execution_manifest_sha256"]
    replayed = copied["replayed"]
    retired_at_value = copied["retired_at"]
    if (
        type(execution_epoch) is not int
        or execution_epoch <= 0
        or not isinstance(execution_manifest_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", execution_manifest_sha256) is None
        or execution_manifest_sha256 == "0" * 64
        or type(replayed) is not bool
        or not isinstance(retired_at_value, str)
    ):
        raise ValueError("capacity manager execution preparation abort result is invalid")
    timestamp = (
        f"{retired_at_value[:-1]}+00:00" if retired_at_value.endswith("Z") else retired_at_value
    )
    try:
        retired_at = datetime.fromisoformat(timestamp)
    except ValueError as exc:
        raise ValueError("capacity manager execution preparation abort time is invalid") from exc
    if retired_at.tzinfo is None or retired_at.utcoffset() is None:
        raise ValueError("capacity manager execution preparation abort time is invalid")
    return ProtectedExecutionPreparationAbortResult(
        execution_epoch=execution_epoch,
        execution_manifest_sha256=execution_manifest_sha256,
        retired_at=retired_at.astimezone(UTC),
        replayed=replayed,
    )


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def _ready_manager_pod(payload: bytes) -> str:
    if len(payload) > _MAX_KUBECTL_OUTPUT_BYTES:
        raise ValueError("capacity manager pod response is oversized")
    value = _json_object(payload, label="capacity manager pod response")
    items = value.get("items")
    if (
        value.get("apiVersion") != "v1"
        or value.get("kind") != "List"
        or not isinstance(value.get("metadata"), dict)
        or not isinstance(items, list)
        or len(items) > 64
    ):
        raise ValueError("capacity manager pod response is invalid")
    ready: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("capacity manager pod response is invalid")
        metadata = item.get("metadata")
        spec = item.get("spec")
        status = item.get("status")
        if (
            item.get("apiVersion") != "v1"
            or item.get("kind") != "Pod"
            or not isinstance(metadata, dict)
            or not isinstance(spec, dict)
            or not isinstance(status, dict)
        ):
            raise ValueError("capacity manager pod response is invalid")
        name = metadata.get("name")
        labels = metadata.get("labels")
        if (
            not isinstance(name, str)
            or _POD_NAME.fullmatch(name) is None
            or metadata.get("namespace") != _NAMESPACE
            or not isinstance(labels, dict)
            or any(labels.get(key) != expected for key, expected in _POD_LABELS.items())
        ):
            raise ValueError("capacity manager pod response is invalid")
        if metadata.get("deletionTimestamp") is not None or status.get("phase") != "Running":
            continue
        containers = spec.get("containers")
        statuses = status.get("containerStatuses")
        conditions = status.get("conditions")
        if (
            not isinstance(containers, list)
            or not containers
            or not isinstance(statuses, list)
            or not isinstance(conditions, list)
        ):
            raise ValueError("capacity manager running pod status is invalid")
        declared = tuple(
            container.get("name") if isinstance(container, dict) else None
            for container in containers
        )
        observed = {
            entry.get("name"): entry
            for entry in statuses
            if isinstance(entry, dict) and isinstance(entry.get("name"), str)
        }
        ready_conditions = [
            condition
            for condition in conditions
            if isinstance(condition, dict) and condition.get("type") == "Ready"
        ]
        if (
            any(not isinstance(name, str) or not name for name in declared)
            or len(set(declared)) != len(declared)
            or len(observed) != len(statuses)
            or set(observed) != set(declared)
            or len(ready_conditions) != 1
            or ready_conditions[0].get("status") != "True"
            or any(
                observed[name].get("ready") is not True
                or observed[name].get("started") is not True
                or not isinstance(observed[name].get("state"), dict)
                or not isinstance(observed[name]["state"].get("running"), dict)
                for name in declared
            )
        ):
            continue
        ready.append(name)
    if len(ready) != 1:
        raise ValueError("capacity manager ready pod is ambiguous or unavailable")
    return ready[0]


def _directory_is_private(path: Path, *, service_uid: int, service_gid: int) -> None:
    metadata = path.stat(follow_symlinks=False)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or metadata.st_uid != service_uid
        or metadata.st_gid != service_gid
    ):
        raise ValueError("capacity manager credential directory is unsafe")


def _file_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _directory_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
    )


def _trusted_file_unchanged(
    path: Path,
    expected: TrustedFileRead,
    *,
    service_uid: int,
) -> None:
    observed = read_trusted_file(
        path,
        service_uid=service_uid,
        private=True,
        max_bytes=_MAX_KUBECTL_OUTPUT_BYTES,
        require_nonempty=True,
    )
    if (
        observed.payload != expected.payload
        or observed.metadata_fingerprint != expected.metadata_fingerprint
        or observed.acl_fingerprint != expected.acl_fingerprint
    ):
        raise ValueError("protected capacity manager kubeconfig changed")


@dataclass(slots=True)
class _KubeconfigAuthority:
    original_path: Path
    original: TrustedFileRead
    snapshot_path: Path
    snapshot: TrustedFileRead
    snapshot_descriptor: int
    service_uid: int
    removed: bool = False

    def revalidate(self) -> None:
        _trusted_file_unchanged(
            self.original_path,
            self.original,
            service_uid=self.service_uid,
        )
        _trusted_file_unchanged(
            self.snapshot_path,
            self.snapshot,
            service_uid=self.service_uid,
        )
        if _file_identity(os.fstat(self.snapshot_descriptor)) != _file_identity(
            self.snapshot.metadata
        ):
            raise ValueError("protected capacity manager kubeconfig snapshot changed")

    def remove(self) -> None:
        if self.removed:
            return
        self.removed = True
        try:
            self.snapshot_path.unlink(missing_ok=True)
        finally:
            os.close(self.snapshot_descriptor)


def _create_kubeconfig_authority(
    original_path: Path,
    original: TrustedFileRead,
    *,
    credentials_root: Path,
    service_uid: int,
    service_gid: int,
) -> _KubeconfigAuthority:
    _directory_is_private(
        credentials_root,
        service_uid=service_uid,
        service_gid=service_gid,
    )
    snapshot_directory, snapshot_directory_descriptor = _prepare_kubeconfig_snapshot_directory(
        credentials_root,
        service_uid=service_uid,
        service_gid=service_gid,
    )
    try:
        descriptor, raw_snapshot_path = tempfile.mkstemp(
            prefix=_KUBECONFIG_SNAPSHOT_PREFIX,
            dir=snapshot_directory,
        )
        snapshot_path = Path(raw_snapshot_path)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            metadata = os.fstat(descriptor)
            if metadata.st_uid != service_uid or metadata.st_gid != service_gid:
                os.fchown(descriptor, service_uid, service_gid)
            converge_new_private_file(descriptor, service_uid=service_uid)
            if os.fstat(descriptor).st_gid != service_gid:
                raise ValueError("protected capacity manager kubeconfig snapshot group is unsafe")
            offset = 0
            while offset < len(original.payload):
                written = os.write(descriptor, original.payload[offset:])
                if written <= 0:
                    raise OSError("protected capacity manager kubeconfig snapshot write stopped")
                offset += written
            os.fsync(descriptor)
            snapshot = read_trusted_file(
                snapshot_path,
                service_uid=service_uid,
                private=True,
                max_bytes=_MAX_KUBECTL_OUTPUT_BYTES,
                require_nonempty=True,
            )
            if snapshot.payload != original.payload or _file_identity(
                os.fstat(descriptor)
            ) != _file_identity(snapshot.metadata):
                raise ValueError("protected capacity manager kubeconfig snapshot is invalid")
            return _KubeconfigAuthority(
                original_path=original_path,
                original=original,
                snapshot_path=snapshot_path,
                snapshot=snapshot,
                snapshot_descriptor=descriptor,
                service_uid=service_uid,
            )
        except BaseException:
            try:
                snapshot_path.unlink(missing_ok=True)
            finally:
                os.close(descriptor)
            raise
    finally:
        os.close(snapshot_directory_descriptor)


def _reconcile_kubeconfig_snapshot(
    path: Path,
    *,
    service_uid: int,
) -> TrustedFileRead | None:
    try:
        trusted = read_trusted_file(
            path,
            service_uid=service_uid,
            private=True,
            max_bytes=_MAX_KUBECTL_OUTPUT_BYTES,
        )
    except ValueError:
        try:
            path.lstat()
        except FileNotFoundError:
            return None
        raise
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except FileNotFoundError:
        return None
    try:
        opened = os.fstat(descriptor)
        if _file_identity(opened) != _file_identity(trusted.metadata):
            raise ValueError("capacity manager kubeconfig snapshot residue changed")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            try:
                named = path.stat(follow_symlinks=False)
            except FileNotFoundError:
                return None
            if _file_identity(named) != _file_identity(opened):
                raise ValueError("capacity manager kubeconfig snapshot residue changed") from None
            return trusted
        try:
            named = path.stat(follow_symlinks=False)
        except FileNotFoundError:
            return None
        if _file_identity(named) != _file_identity(opened):
            raise ValueError("capacity manager kubeconfig snapshot residue changed")
        try:
            path.unlink()
        except FileNotFoundError:
            return None
        if os.fstat(descriptor).st_nlink != 0:
            raise ValueError("capacity manager kubeconfig snapshot residue did not converge")
        return None
    finally:
        os.close(descriptor)


def _prepare_kubeconfig_snapshot_directory(
    credentials_root: Path,
    *,
    service_uid: int,
    service_gid: int,
) -> tuple[Path, int]:
    protected_root = credentials_root.parent
    _directory_is_private(
        protected_root,
        service_uid=service_uid,
        service_gid=service_gid,
    )
    snapshot_directory = protected_root / _KUBECONFIG_SNAPSHOT_DIRECTORY
    try:
        snapshot_directory.mkdir(mode=0o700)
    except FileExistsError:
        pass
    _directory_is_private(
        snapshot_directory,
        service_uid=service_uid,
        service_gid=service_gid,
    )
    named = snapshot_directory.stat(follow_symlinks=False)
    directory_descriptor = os.open(
        snapshot_directory,
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        if _directory_identity(os.fstat(directory_descriptor)) != _directory_identity(named):
            raise ValueError("capacity manager kubeconfig snapshot directory changed")
        deadline = time.monotonic() + _START_TIMEOUT_SECONDS
        while True:
            try:
                fcntl.flock(directory_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "capacity manager kubeconfig snapshot lock timed out"
                    ) from None
                time.sleep(min(0.01, remaining))
        current = snapshot_directory.stat(follow_symlinks=False)
        if _directory_identity(os.fstat(directory_descriptor)) != _directory_identity(current):
            raise ValueError("capacity manager kubeconfig snapshot directory changed")
        live: dict[str, TrustedFileRead] = {}
        for stale in tuple(snapshot_directory.iterdir()):
            if not stale.name.startswith(_KUBECONFIG_SNAPSHOT_PREFIX):
                raise ValueError("capacity manager kubeconfig snapshot residue is unsafe")
            observed = _reconcile_kubeconfig_snapshot(
                stale,
                service_uid=service_uid,
            )
            if observed is not None:
                live[stale.name] = observed
        for residual in tuple(snapshot_directory.iterdir()):
            expected = live.get(residual.name)
            if expected is None:
                raise ValueError("capacity manager kubeconfig snapshot residue is unsafe")
            try:
                residual_metadata = residual.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            if _file_identity(residual_metadata) != _file_identity(expected.metadata):
                raise ValueError("capacity manager kubeconfig snapshot residue changed")
        return snapshot_directory, directory_descriptor
    except BaseException:
        os.close(directory_descriptor)
        raise


@contextmanager
def _open_private_file(
    path: Path,
    *,
    service_uid: int,
    service_gid: int,
    max_bytes: int = _MAX_CREDENTIAL_BYTES,
) -> Iterator[int]:
    metadata = path.lstat()
    if (
        path.is_symlink()
        or not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_uid != service_uid
        or metadata.st_gid != service_gid
        or metadata.st_nlink != 1
        or not 0 < metadata.st_size <= max_bytes
    ):
        raise ValueError("capacity manager credential file is unsafe")
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    opened = os.fstat(descriptor)
    try:
        if _file_identity(opened) != _file_identity(metadata):
            raise ValueError("capacity manager credential changed while opening")
        yield descriptor
        closed = os.fstat(descriptor)
        if _file_identity(closed) != _file_identity(opened):
            raise ValueError("capacity manager credential changed while in use")
    finally:
        os.close(descriptor)


def _read_private_file(
    path: Path,
    *,
    service_uid: int,
    service_gid: int,
    max_bytes: int = _MAX_CREDENTIAL_BYTES,
) -> bytes:
    with _open_private_file(
        path,
        service_uid=service_uid,
        service_gid=service_gid,
        max_bytes=max_bytes,
    ) as descriptor:
        parts: list[bytes] = []
        size = 0
        while size <= max_bytes:
            chunk = os.read(descriptor, min(16 * 1024, max_bytes + 1 - size))
            if not chunk:
                break
            parts.append(chunk)
            size += len(chunk)
    payload = b"".join(parts)
    if not payload or len(payload) > max_bytes:
        raise ValueError("capacity manager credential is not bounded")
    return payload


@dataclass(frozen=True, slots=True)
class _CredentialMaterial:
    context: ssl.SSLContext
    bearer_token: str
    fingerprints: Mapping[str, str]


def _load_credential_material(
    root: Path,
    name: str,
    *,
    service_uid: int,
    service_gid: int,
) -> _CredentialMaterial:
    if name not in _CREDENTIAL_NAMES:
        raise ValueError("capacity manager credential scope is invalid")
    _directory_is_private(root, service_uid=service_uid, service_gid=service_gid)
    directory = root / name
    _directory_is_private(directory, service_uid=service_uid, service_gid=service_gid)
    if {path.name for path in directory.iterdir()} != _CREDENTIAL_FILES:
        raise ValueError("capacity manager credential scope is incomplete")
    payloads = {
        filename: _read_private_file(
            directory / filename,
            service_uid=service_uid,
            service_gid=service_gid,
        )
        for filename in _CREDENTIAL_FILES
    }
    token_bytes = payloads["bearer-token"]
    try:
        token = token_bytes.decode("ascii")
        ca_data = payloads["manager-ca.pem"].decode("ascii")
    except UnicodeDecodeError as exc:
        raise ValueError("capacity manager credential is not ASCII") from exc
    if token.endswith("\n"):
        token = token[:-1]
    if (
        not 32 <= len(token) <= 4096
        or token != token.strip()
        or any(not 0x21 <= ord(character) <= 0x7E for character in token)
    ):
        raise ValueError("capacity manager bearer credential is invalid")
    context = ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_verify_locations(cadata=ca_data)
    with (
        _open_private_file(
            directory / "certificate.pem",
            service_uid=service_uid,
            service_gid=service_gid,
        ) as certificate_fd,
        _open_private_file(
            directory / "private-key.pem",
            service_uid=service_uid,
            service_gid=service_gid,
        ) as private_key_fd,
    ):
        certificate_path = Path(f"/proc/self/fd/{certificate_fd}")
        private_key_path = Path(f"/proc/self/fd/{private_key_fd}")
        if not certificate_path.exists() or not private_key_path.exists():
            raise ValueError("verified credential descriptors are unavailable")
        context.load_cert_chain(certfile=str(certificate_path), keyfile=str(private_key_path))
    return _CredentialMaterial(
        context=context,
        bearer_token=token,
        fingerprints={
            filename: hashlib.sha256(payload).hexdigest() for filename, payload in payloads.items()
        },
    )


def _revalidate_credential_material(
    root: Path,
    name: str,
    expected: _CredentialMaterial,
    *,
    service_uid: int,
    service_gid: int,
) -> None:
    _directory_is_private(root, service_uid=service_uid, service_gid=service_gid)
    directory = root / name
    _directory_is_private(directory, service_uid=service_uid, service_gid=service_gid)
    if {path.name for path in directory.iterdir()} != _CREDENTIAL_FILES:
        raise ValueError("capacity manager credential scope is incomplete")
    observed = {
        filename: hashlib.sha256(
            _read_private_file(
                directory / filename,
                service_uid=service_uid,
                service_gid=service_gid,
            )
        ).hexdigest()
        for filename in _CREDENTIAL_FILES
    }
    if observed != expected.fingerprints:
        raise ValueError("capacity manager credentials changed before request")


@dataclass(frozen=True, slots=True)
class ProtectedCapacityManagerClient:
    """Use fixed methods and credential scopes over one exact pod tunnel."""

    origin: str
    credentials_root: Path
    service_uid: int
    service_gid: int
    client_factory: ClientFactory = _client

    def get_configuration(self) -> dict[str, object]:
        return self._request("GET", "/v1/configuration", "configuration-read")

    def get_status(self) -> dict[str, object]:
        return self._request("GET", "/v1/status", "manager-read")

    def prepare_execution(
        self,
        preparation: ExecutionPreparationV2,
        idempotency_key: UUID,
    ) -> ExecutionContextV2:
        if not isinstance(preparation, ExecutionPreparationV2):
            raise TypeError("capacity manager execution preparation is invalid")
        response = self._request(
            "POST",
            "/v2/execution-preparations",
            "manager-prepare",
            payload=preparation.model_dump(mode="json", exclude_none=False),
            idempotency_key=idempotency_key,
        )
        try:
            return _execution_context(response)
        except ValueError as exc:
            raise ProtectedCapacityManagerClientError("unexpected") from exc

    def get_execution_preparation_status(self) -> ProtectedExecutionPreparationStatus:
        response = self._request(
            "GET",
            "/v2/status/execution-preparation",
            "manager-read",
        )
        try:
            return _execution_preparation_status(response)
        except ValueError as exc:
            raise ProtectedCapacityManagerClientError("unexpected") from exc

    def abort_execution_preparation(
        self,
        abort: ExecutionPreparationAbortV2,
        idempotency_key: UUID,
    ) -> ProtectedExecutionPreparationAbortResult:
        if not isinstance(abort, ExecutionPreparationAbortV2):
            raise TypeError("capacity manager execution preparation abort is invalid")
        response = self._request(
            "POST",
            f"/v2/execution-preparations/{abort.execution_epoch}/abort",
            "manager-abort",
            payload=abort.model_dump(mode="json", exclude_none=False),
            idempotency_key=idempotency_key,
        )
        try:
            result = _execution_preparation_abort_result(response)
        except ValueError as exc:
            raise ProtectedCapacityManagerClientError("unexpected") from exc
        if (
            result.execution_epoch != abort.execution_epoch
            or result.execution_manifest_sha256 != abort.execution_manifest_sha256
        ):
            raise ProtectedCapacityManagerClientError("unexpected")
        return result

    def propose_fleet(
        self,
        payload: Mapping[str, object],
        idempotency_key: UUID,
    ) -> dict[str, object]:
        return self._request(
            "PUT",
            "/v1/config-proposals/fleet",
            "configuration-fleet",
            payload=payload,
            idempotency_key=idempotency_key,
        )

    def propose_subject(
        self,
        subject_id: UUID,
        payload: Mapping[str, object],
        idempotency_key: UUID,
    ) -> dict[str, object]:
        if not isinstance(subject_id, UUID) or subject_id.int == 0:
            raise ValueError("capacity manager subject identity is invalid")
        return self._request(
            "PUT",
            f"/v1/config-proposals/subjects/{subject_id}",
            "configuration-subject",
            payload=payload,
            idempotency_key=idempotency_key,
        )

    def activate(
        self,
        payload: Mapping[str, object],
        idempotency_key: UUID,
    ) -> dict[str, object]:
        return self._request(
            "POST",
            "/v1/config-activations",
            "configuration-activate",
            payload=payload,
            idempotency_key=idempotency_key,
        )

    def rollback(
        self,
        payload: Mapping[str, object],
        idempotency_key: UUID,
    ) -> dict[str, object]:
        return self._request(
            "POST",
            "/v1/configuration-rollbacks",
            "configuration-activate",
            payload=payload,
            idempotency_key=idempotency_key,
        )

    def _request(
        self,
        method: str,
        path: str,
        credential_name: str,
        *,
        payload: Mapping[str, object] | None = None,
        idempotency_key: UUID | None = None,
    ) -> dict[str, object]:
        origin = (
            re.fullmatch(r"https://127\.0\.0\.1:([1-9][0-9]{0,4})", self.origin)
            if isinstance(self.origin, str)
            else None
        )
        if origin is None or not 1024 <= int(origin.group(1)) <= 65535:
            raise ValueError("capacity manager tunnel origin is invalid")
        if not isinstance(idempotency_key, UUID) and idempotency_key is not None:
            raise ValueError("capacity manager idempotency key is invalid")
        if (method == "GET") != (payload is None and idempotency_key is None):
            raise ValueError("capacity manager request contract is invalid")
        deadline = time.monotonic() + _HTTP_TIMEOUT_SECONDS
        content: bytes | None = None
        if payload is not None:
            try:
                content = json.dumps(
                    dict(payload),
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                    allow_nan=False,
                ).encode("ascii")
            except (TypeError, ValueError, UnicodeError) as exc:
                raise ValueError("capacity manager request payload is invalid") from exc
            if not content or len(content) > _MAX_REQUEST_BYTES:
                raise ValueError("capacity manager request payload is not bounded")
        try:
            material = _load_credential_material(
                self.credentials_root,
                credential_name,
                service_uid=self.service_uid,
                service_gid=self.service_gid,
            )
            _revalidate_credential_material(
                self.credentials_root,
                credential_name,
                material,
                service_uid=self.service_uid,
                service_gid=self.service_gid,
            )
        except (OSError, ValueError, ssl.SSLError) as exc:
            raise ProtectedCapacityManagerClientError("credential") from exc
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "Authorization": f"Bearer {material.bearer_token}",
        }
        if idempotency_key is not None:
            headers["Content-Type"] = "application/json"
            headers["Idempotency-Key"] = str(idempotency_key)
        client = self.client_factory(material.context)
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ProtectedCapacityManagerClientError("timeout")
            kwargs: dict[str, object] = {
                "headers": headers,
                "timeout": httpx.Timeout(
                    remaining,
                    connect=min(3.0, remaining),
                    read=remaining,
                    write=remaining,
                    pool=min(3.0, remaining),
                ),
                "extensions": {_ABSOLUTE_DEADLINE_EXTENSION: deadline},
            }
            if content is not None:
                kwargs["content"] = content
            try:
                try:
                    with client.stream(method, f"{self.origin}{path}", **kwargs) as response:
                        if time.monotonic() >= deadline:
                            raise ProtectedCapacityManagerClientError("timeout")
                        if 300 <= response.status_code < 400:
                            raise ProtectedCapacityManagerClientError("redirected")
                        if response.status_code != 200:
                            raise ProtectedCapacityManagerClientError("unexpected")
                        encoding = response.headers.get("content-encoding", "").strip().lower()
                        if encoding not in {"", "identity"}:
                            raise ProtectedCapacityManagerClientError("encoding")
                        content_type = response.headers.get("content-type", "")
                        if content_type.split(";", 1)[0].strip().lower() != "application/json":
                            raise ProtectedCapacityManagerClientError("content-type")
                        content_length = response.headers.get("content-length")
                        if content_length is not None and (
                            not content_length.isdigit()
                            or int(content_length) > _MAX_RESPONSE_BYTES
                        ):
                            raise ProtectedCapacityManagerClientError("oversized")
                        body = bytearray()
                        for chunk in response.iter_bytes(chunk_size=64 * 1024):
                            if time.monotonic() >= deadline:
                                raise ProtectedCapacityManagerClientError("timeout")
                            body.extend(chunk)
                            if len(body) > _MAX_RESPONSE_BYTES:
                                raise ProtectedCapacityManagerClientError("oversized")
                finally:
                    try:
                        _revalidate_credential_material(
                            self.credentials_root,
                            credential_name,
                            material,
                            service_uid=self.service_uid,
                            service_gid=self.service_gid,
                        )
                    except (OSError, ValueError) as exc:
                        raise ProtectedCapacityManagerClientError("credential") from exc
            except ProtectedCapacityManagerClientError:
                raise
            except (TimeoutError, httpx.TimeoutException) as exc:
                raise ProtectedCapacityManagerClientError("timeout") from exc
            except (OSError, ssl.SSLError, httpx.HTTPError) as exc:
                raise ProtectedCapacityManagerClientError("transport") from exc
        finally:
            client.close()
        try:
            return _json_object(bytes(body), label="capacity manager response")
        except ValueError as exc:
            raise ProtectedCapacityManagerClientError("json") from exc


@contextmanager
def open_protected_capacity_manager_client(
    *,
    runner: ManagerCommandRunner,
    credentials_root: Path,
    service_uid: int,
    service_gid: int,
    spawn: Spawn = _spawn,
    allocate_port: PortAllocator = _allocate_port,
    wait_ready: WaitReady = _wait_ready,
    client_factory: ClientFactory = _client,
) -> Iterator[ProtectedCapacityManagerClient]:
    """Yield one strict manager client through one exact ready pod."""

    environment = runner.environment
    raw_kubeconfig = environment.get("KUBECONFIG")
    if (
        not isinstance(raw_kubeconfig, str)
        or not raw_kubeconfig
        or not credentials_root.is_absolute()
        or ".." in credentials_root.parts
        or type(service_uid) is not int
        or type(service_gid) is not int
        or service_uid < 0
        or service_gid < 0
    ):
        raise ValueError("protected capacity manager client authority is invalid")
    kubeconfig = Path(raw_kubeconfig)
    if not kubeconfig.is_absolute() or ".." in kubeconfig.parts:
        raise ValueError("protected capacity manager kubeconfig authority is invalid")
    authority: _KubeconfigAuthority | None = None
    process: TunnelProcess | None = None
    diagnostics: _BoundedChildOutputCapture | None = None
    try:
        try:
            original_kubeconfig = read_trusted_file(
                kubeconfig,
                service_uid=service_uid,
                private=True,
                max_bytes=_MAX_KUBECTL_OUTPUT_BYTES,
                require_nonempty=True,
            )
            authority = _create_kubeconfig_authority(
                kubeconfig,
                original_kubeconfig,
                credentials_root=credentials_root,
                service_uid=service_uid,
                service_gid=service_gid,
            )
            authority.revalidate()
            pod_payload = runner.capture_stdout(
                (
                    "kubectl",
                    "--kubeconfig",
                    str(authority.snapshot_path),
                    "--namespace",
                    _NAMESPACE,
                    "get",
                    "pods",
                    "--selector=app.kubernetes.io/name=loom-capacity-manager,loom.yylx.dev/capacity-component=manager",
                    "--output=json",
                    "--request-timeout=15s",
                ),
                env=environment,
                timeout_seconds=_KUBECTL_TIMEOUT_SECONDS,
            )
            if not isinstance(pod_payload, bytes):
                raise ValueError("capacity manager pod response is invalid")
            pod_name = _ready_manager_pod(pod_payload)
            authority.revalidate()
        except (OSError, RuntimeError, ValueError) as exc:
            raise ProtectedCapacityManagerClientError("transport") from exc
        if authority is None:
            raise RuntimeError("protected capacity manager kubeconfig authority is unavailable")
        port = allocate_port()
        if type(port) is not int or not 1024 <= port <= 65535:
            raise ValueError("protected capacity manager local port is invalid")
        argv = (
            "kubectl",
            "--kubeconfig",
            str(authority.snapshot_path),
            "--namespace",
            _NAMESPACE,
            "port-forward",
            f"pod/{pod_name}",
            "--address=127.0.0.1",
            f"{port}:{_REMOTE_PORT}",
            "--pod-running-timeout=15s",
        )
        try:
            authority.revalidate()
            process = spawn(argv, environment)
            diagnostics = _BoundedChildOutputCapture(process, port)
            diagnostics.start()
            wait_ready(process, port, diagnostics)
            authority.revalidate()
        except Exception as exc:
            reason = (
                "timeout"
                if isinstance(exc, (TimeoutError, subprocess.TimeoutExpired))
                or "timed out" in str(exc).casefold()
                else "transport"
            )
            diagnostic = None if diagnostics is None else diagnostics.diagnostic()
            raise ProtectedCapacityManagerClientError(reason, diagnostic) from None
        yield ProtectedCapacityManagerClient(
            origin=f"https://127.0.0.1:{port}",
            credentials_root=credentials_root,
            service_uid=service_uid,
            service_gid=service_gid,
            client_factory=client_factory,
        )
    finally:
        teardown_error: Exception | None = None
        if authority is not None:
            try:
                authority.revalidate()
            except (OSError, RuntimeError, ValueError) as exc:
                teardown_error = exc
        if process is not None:
            try:
                _stop_exact(process)
            except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
                if teardown_error is None:
                    teardown_error = exc
        if diagnostics is not None:
            try:
                diagnostics.finish()
            except (OSError, RuntimeError, ValueError) as exc:
                if teardown_error is None:
                    teardown_error = exc
        if authority is not None:
            try:
                authority.revalidate()
            except (OSError, RuntimeError, ValueError) as exc:
                if teardown_error is None:
                    teardown_error = exc
            try:
                authority.remove()
            except OSError as exc:
                if teardown_error is None:
                    teardown_error = exc
        if teardown_error is not None:
            raise ProtectedCapacityManagerClientError("transport") from teardown_error


__all__ = [
    "ClientFactory",
    "ManagerCommandRunner",
    "ProtectedCapacityManagerClient",
    "ProtectedCapacityManagerClientError",
    "ProtectedExecutionPreparationAbortResult",
    "ProtectedExecutionPreparationStatus",
    "open_protected_capacity_manager_client",
]
