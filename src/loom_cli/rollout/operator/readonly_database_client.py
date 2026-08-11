"""Bounded localhost transport for the SELECT-only preflight database role."""

from __future__ import annotations

import socket
import subprocess
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from threading import Lock, Thread
from typing import Any, Literal, Protocol, TextIO

import psycopg
from psycopg.rows import dict_row

from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.preflight_credential_paths import (
    READONLY_DATABASE_CREDENTIAL_PATH,
    READONLY_KUBECONFIG_PATH,
)
from loom_cli.rollout.readonly_database_authority import (
    DatabaseQuery,
    ReadonlyDatabaseEvidence,
    ReadonlyMutationEpochEvidence,
    probe_readonly_database,
    probe_readonly_database_baseline,
    probe_readonly_mutation_epoch,
)
from loom_cli.rollout.readonly_database_bootstrap import ReadonlyDatabaseCredential

from .redaction import redact_rollout_text

_NAMESPACE = "loom-staging"
_SERVICE = "service/loom-postgres-rw"
_REMOTE_PORT = 5432
_START_TIMEOUT_SECONDS = 15.0
_STOP_TIMEOUT_SECONDS = 5.0
_DIAGNOSTIC_LIMIT = 8 * 1024
_CHILD_ENVIRONMENT = {
    "HOME": "/var/lib/loom-staging-rollout",
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "USER": "loom-rollout",
}


class TunnelProcess(Protocol):
    def poll(self) -> int | None: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...

    def wait(self, timeout: float | None = None) -> int: ...


class DatabaseConnection(Protocol):
    def execute(
        self,
        query: str,
        params: Sequence[object] | Mapping[str, object] | None = None,
    ) -> Any: ...

    def close(self) -> None: ...


Spawn = Callable[[Sequence[str], Mapping[str, str]], TunnelProcess]
Connect = Callable[[str, int, ReadonlyDatabaseCredential], DatabaseConnection]
PortAllocator = Callable[[], int]
WaitReady = Callable[[TunnelProcess, int], None]

ReadonlyDatabaseTunnelFailureKind = Literal["credential", "transport", "timeout"]


class ReadonlyDatabaseTunnelError(RuntimeError):
    """Typed tunnel failure whose string form never contains private diagnostics."""

    def __init__(
        self,
        kind: ReadonlyDatabaseTunnelFailureKind,
        diagnostic: str | None = None,
    ) -> None:
        if kind not in {"credential", "transport", "timeout"}:
            raise ValueError("readonly database tunnel failure kind is invalid")
        sanitized = (
            None
            if diagnostic is None
            else redact_rollout_text(diagnostic, limit=_DIAGNOSTIC_LIMIT).strip() or None
        )
        super().__init__(f"readonly database tunnel {kind} failure")
        self.kind = kind
        self.diagnostic = sanitized


class _BoundedStderrCapture:
    """Drain a child pipe concurrently while retaining only sanitized bounded text."""

    def __init__(
        self,
        stream: TextIO | None,
        *,
        known_secrets: Sequence[str],
    ) -> None:
        self._stream = stream
        self._known_secrets = tuple(known_secrets)
        self._lock = Lock()
        self._parts: list[str] = []
        self._size = 0
        self._thread = Thread(target=self._drain, daemon=True)

    def start(self) -> None:
        if self._stream is not None:
            self._thread.start()

    def _drain(self) -> None:
        assert self._stream is not None
        try:
            while chunk := self._stream.read(1024):
                sanitized = redact_rollout_text(
                    chunk,
                    known_secrets=self._known_secrets,
                )
                with self._lock:
                    remaining = _DIAGNOSTIC_LIMIT - self._size
                    if remaining > 0:
                        retained = sanitized[:remaining]
                        self._parts.append(retained)
                        self._size += len(retained)
        except (OSError, ValueError):
            return

    def diagnostic(self) -> str | None:
        if self._stream is not None:
            self._thread.join(timeout=0.05)
        with self._lock:
            joined = "".join(self._parts)
        # Redact the joined text again so a sensitive assignment split across
        # two fixed-size reads cannot escape the structural redactor.
        sanitized = redact_rollout_text(
            joined,
            known_secrets=self._known_secrets,
            limit=_DIAGNOSTIC_LIMIT,
        ).strip()
        return sanitized or None

    def finish(self) -> None:
        if self._stream is not None:
            self._thread.join(timeout=1.0)


def _classify_tunnel_failure(
    error: Exception,
    diagnostic: str | None,
) -> ReadonlyDatabaseTunnelFailureKind:
    rendered = f"{type(error).__name__}: {error}\n{diagnostic or ''}".casefold()
    if isinstance(error, (TimeoutError, subprocess.TimeoutExpired)) or "timed out" in rendered:
        return "timeout"
    if any(
        marker in rendered
        for marker in (
            "unauthorized",
            "forbidden",
            "authentication",
            "credentials",
            "token has expired",
            "you must be logged in",
        )
    ):
        return "credential"
    return "transport"


def _spawn(argv: Sequence[str], environment: Mapping[str, str]) -> TunnelProcess:
    return subprocess.Popen(
        list(argv),
        env=dict(environment),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        close_fds=True,
        start_new_session=False,
    )


def _allocate_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    if not isinstance(port, int) or port < 1024 or port > 65535:
        raise RuntimeError("readonly database local port is invalid")
    return port


def _wait_ready(process: TunnelProcess, port: int) -> None:
    deadline = time.monotonic() + _START_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("readonly database port-forward exited early")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("readonly database port-forward timed out")


def _connect(
    host: str,
    port: int,
    credential: ReadonlyDatabaseCredential,
) -> DatabaseConnection:
    return psycopg.connect(
        host=host,
        port=port,
        dbname=credential.database,
        user=credential.role,
        password=credential.password,
        connect_timeout=5,
        options="-c default_transaction_read_only=on -c statement_timeout=15000",
        row_factory=dict_row,
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
                raise RuntimeError("readonly database port-forward did not stop") from exc


@contextmanager
def open_readonly_database_query(
    *,
    service_uid: int,
    kubeconfig_path: Path = READONLY_KUBECONFIG_PATH,
    credential_path: Path = READONLY_DATABASE_CREDENTIAL_PATH,
    spawn: Spawn = _spawn,
    connect: Connect = _connect,
    allocate_port: PortAllocator = _allocate_port,
    wait_ready: WaitReady = _wait_ready,
) -> Iterator[DatabaseQuery]:
    """Yield one query callback backed by a single read-only transaction."""

    if (
        service_uid < 1
        or not kubeconfig_path.is_absolute()
        or not credential_path.is_absolute()
        or ".." in kubeconfig_path.parts
        or ".." in credential_path.parts
    ):
        raise ValueError("readonly database client authority is invalid")
    read_trusted_file(
        kubeconfig_path,
        service_uid=service_uid,
        private=True,
        max_bytes=1 << 20,
        require_nonempty=True,
    )
    credential = ReadonlyDatabaseCredential.from_bytes(
        read_trusted_file(
            credential_path,
            service_uid=service_uid,
            private=True,
            max_bytes=1024,
            require_nonempty=True,
        ).payload
    )
    port = allocate_port()
    argv = (
        "kubectl",
        "--kubeconfig",
        str(kubeconfig_path),
        "--namespace",
        _NAMESPACE,
        "port-forward",
        _SERVICE,
        "--address=127.0.0.1",
        f"{port}:{_REMOTE_PORT}",
        "--pod-running-timeout=15s",
    )
    process = spawn(argv, _CHILD_ENVIRONMENT)
    diagnostics = _BoundedStderrCapture(
        getattr(process, "stderr", None),
        known_secrets=(str(kubeconfig_path), credential.password),
    )
    diagnostics.start()
    connection: DatabaseConnection | None = None
    try:
        try:
            wait_ready(process, port)
        except ReadonlyDatabaseTunnelError:
            try:
                _stop_exact(process)
            finally:
                diagnostics.finish()
            raise
        except Exception as exc:
            try:
                _stop_exact(process)
            finally:
                diagnostics.finish()
            diagnostic = diagnostics.diagnostic()
            raise ReadonlyDatabaseTunnelError(
                _classify_tunnel_failure(exc, diagnostic),
                diagnostic,
            ) from None
        connection = connect("127.0.0.1", port, credential)
        connection.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")

        def query(sql: str) -> tuple[Mapping[str, object], ...]:
            if not sql or len(sql) > 16_384 or "\x00" in sql:
                raise ValueError("readonly database query is invalid")
            cursor = connection.execute(sql)
            rows = cursor.fetchall()
            if len(rows) > 1024 or not all(isinstance(row, Mapping) for row in rows):
                raise ValueError("readonly database query evidence is invalid")
            return tuple(dict(row) for row in rows)

        yield query
        connection.execute("ROLLBACK")
    except BaseException:
        if connection is not None:
            try:
                connection.execute("ROLLBACK")
            except Exception:
                pass
        raise
    finally:
        if connection is not None:
            connection.close()
        try:
            _stop_exact(process)
        finally:
            diagnostics.finish()


def probe_installed_readonly_database(
    *,
    service_uid: int,
    query_context: Callable[..., AbstractContextManager[DatabaseQuery]] = (
        open_readonly_database_query
    ),
) -> ReadonlyDatabaseEvidence:
    """Return secret-free Tier 2 evidence from installed exact credentials."""

    with query_context(service_uid=service_uid) as query:
        return probe_readonly_database(query)


def probe_installed_readonly_database_baseline(
    *,
    service_uid: int,
    query_context: Callable[..., AbstractContextManager[DatabaseQuery]] = (
        open_readonly_database_query
    ),
) -> ReadonlyDatabaseEvidence:
    """Return baseline evidence without letting checkpoint drift mask Tier 2."""

    with query_context(service_uid=service_uid) as query:
        return probe_readonly_database_baseline(query)


def probe_installed_readonly_mutation_epoch(
    *,
    service_uid: int,
    query_context: Callable[..., AbstractContextManager[DatabaseQuery]] = (
        open_readonly_database_query
    ),
) -> ReadonlyMutationEpochEvidence:
    """Return the exact epoch identity without requiring Tier 2 capacity state."""

    with query_context(service_uid=service_uid) as query:
        return probe_readonly_mutation_epoch(query)


class InstalledReadonlyDatabaseEvidenceSource:
    """Single-flight process-local snapshot shared by the concurrent DAG."""

    def __init__(
        self,
        *,
        service_uid: int,
        probe: Callable[..., ReadonlyDatabaseEvidence] = probe_installed_readonly_database,
    ) -> None:
        if service_uid < 1:
            raise ValueError("readonly database evidence source identity is invalid")
        self._service_uid = service_uid
        self._probe = probe
        self._lock = Lock()
        self._evidence: ReadonlyDatabaseEvidence | None = None
        self._failure: Exception | None = None

    def __call__(self) -> ReadonlyDatabaseEvidence:
        with self._lock:
            if self._failure is not None:
                raise self._failure
            if self._evidence is None:
                try:
                    self._evidence = self._probe(service_uid=self._service_uid)
                except ReadonlyDatabaseTunnelError as exc:
                    self._failure = exc
                    raise
                except Exception as exc:
                    failure = RuntimeError(f"{type(exc).__name__}: {exc}")
                    self._failure = failure
                    raise failure from exc
            return self._evidence


class InstalledReadonlyMutationEpochSource:
    """Single-flight epoch source used before the concurrent preflight DAG."""

    def __init__(
        self,
        *,
        service_uid: int,
        probe: Callable[..., ReadonlyMutationEpochEvidence] = (
            probe_installed_readonly_mutation_epoch
        ),
    ) -> None:
        if service_uid < 1:
            raise ValueError("readonly mutation epoch source identity is invalid")
        self._service_uid = service_uid
        self._probe = probe
        self._lock = Lock()
        self._evidence: ReadonlyMutationEpochEvidence | None = None

    def __call__(self) -> ReadonlyMutationEpochEvidence:
        with self._lock:
            if self._evidence is None:
                self._evidence = self._probe(service_uid=self._service_uid)
            return self._evidence

    def refresh(self) -> ReadonlyMutationEpochEvidence:
        """Replace the process-local snapshot after a protected mutation."""
        with self._lock:
            self._evidence = self._probe(service_uid=self._service_uid)
            return self._evidence


__all__ = [
    "Connect",
    "DatabaseConnection",
    "InstalledReadonlyDatabaseEvidenceSource",
    "InstalledReadonlyMutationEpochSource",
    "PortAllocator",
    "ReadonlyDatabaseTunnelError",
    "ReadonlyDatabaseTunnelFailureKind",
    "Spawn",
    "TunnelProcess",
    "WaitReady",
    "open_readonly_database_query",
    "probe_installed_readonly_database",
    "probe_installed_readonly_database_baseline",
    "probe_installed_readonly_mutation_epoch",
]
