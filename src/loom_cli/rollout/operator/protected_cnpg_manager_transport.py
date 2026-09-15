"""Fixed one-shot transport for an already admitted CNPG handoff.

This is not a rollout entrypoint or administrator exclusion. The enclosing
installed component must retain its original supervised authority window,
process/volume/configuration admission and owned-workload shutdown throughout.
After issuance it must reconcile exact exec AND harmful server work, regardless
of any HTTP result. This module never declares completion or releases a fence.
"""

from __future__ import annotations

import hashlib
import http.client
import os
import re
import selectors
import socket
import ssl
import subprocess
import tempfile
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, BinaryIO

from loom.application_database_admission import require_application_database_drained

from .protected_cnpg_manager_replacement import (
    CNPG_MANAGER_SHA256,
    CNPG_MANAGER_SIZE,
    CNPGManagerIdentity,
)
from .protected_cnpg_writer_configuration import _json, _mapping

if TYPE_CHECKING:
    from .protected_apply_executor import SubprocessProtectedApplyCommandRunner
    from .protected_apply_journal import ProtectedApplyJournal

_NAMESPACE = "loom-staging"
_COMMAND = b"/controller/manager\x00instance\x00run\x00--status-port-tls\x00--log-level=info\x00"
_PROCESS = (
    "cat /proc/1/cmdline; printf '\\n'; cat /proc/1/stat; "
    "stat -Lc '%d %i' /proc/1/exe /controller/manager; "
    "sha256sum /proc/1/exe /controller/manager"
)
_CHUNK = 64 * 1024
_SECONDS = 120


def _exec(identity: CNPGManagerIdentity, *argv: str) -> tuple[str, ...]:
    return ("kubectl", "--namespace", _NAMESPACE, "exec", f"pod/{identity.pod_name}",
            "--container=postgres", "--", *argv)


def _observe_identity(
    runner: SubprocessProtectedApplyCommandRunner, expected: CNPGManagerIdentity,
) -> CNPGManagerIdentity:
    """Observe fixed process identity and bytes, not complete startup/volume policy."""
    def metadata() -> tuple[str, str, str, int]:
        value = _json(runner.capture_stdout(
            ("kubectl", "--namespace", _NAMESPACE, "get", f"pod/{expected.pod_name}",
             "--output=json", "--request-timeout=30s"), env=runner.environment, timeout_seconds=30,
        ))
        meta, spec, status = (_mapping(value.get(name)) for name in ("metadata", "spec", "status"))
        statuses = status.get("containerStatuses")
        if (value.get("kind") != "Pod" or meta.get("name") != expected.pod_name
                or meta.get("namespace") != _NAMESPACE or meta.get("deletionTimestamp") is not None
                or not isinstance(statuses, list) or len(statuses) != 1):
            raise RuntimeError("CNPG manager Pod identity changed")
        container = _mapping(statuses[0])
        uid, node, container_id, restarts = (
            meta.get("uid"), spec.get("nodeName"), container.get("containerID"), container.get("restartCount"),
        )
        if (container.get("name") != "postgres" or not isinstance(uid, str)
                or not isinstance(node, str) or not isinstance(container_id, str)
                or type(restarts) is not int or "running" not in _mapping(container.get("state"))):
            raise RuntimeError("CNPG manager container identity changed")
        # Validate allowlisted node/Pod/container before any exec reaches it.
        observed = CNPGManagerIdentity(expected.pod_name, uid, container_id, node, restarts,
                                       expected.process_started_ticks, expected.executable_device,
                                       expected.executable_inode)
        if (observed.pod_uid, observed.container_id, observed.node_name, observed.restart_count) != (
            expected.pod_uid, expected.container_id, expected.node_name, expected.restart_count,
        ):
            raise RuntimeError("CNPG manager Pod identity changed")
        return uid, node, container_id, restarts

    before = metadata()
    payload = runner.capture_stdout(_exec(expected, "sh", "-ceu", _PROCESS),
                                    env=runner.environment, timeout_seconds=30)
    lines = payload.splitlines()
    if (len(lines) != 6 or lines[0] != _COMMAND or lines[2] != lines[3]
            or lines[4] != (CNPG_MANAGER_SHA256 + "  /proc/1/exe").encode()
            or lines[5] != (CNPG_MANAGER_SHA256 + "  /controller/manager").encode()
            or not lines[1].startswith(b"1 (manager) ")):
        raise RuntimeError("CNPG manager executable profile changed")
    try:
        started = int(lines[1].rsplit(b") ", 1)[1].split()[19])
        device, inode = (int(part) for part in lines[2].split())
    except (ValueError, IndexError):
        raise RuntimeError("CNPG manager process identity is invalid") from None
    if metadata() != before:
        raise RuntimeError("CNPG manager Pod changed during process observation")
    uid, node, container_id, restarts = before
    return CNPGManagerIdentity(expected.pod_name, uid, container_id, node, restarts, started, device, inode)


@contextmanager
def _child(argv: Sequence[str], environment: Mapping[str, str]) -> Iterator[subprocess.Popen[bytes]]:
    process = subprocess.Popen(tuple(argv), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, env=dict(environment))
    try:
        yield process
    finally:
        try:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        finally:
            for stream in (process.stdout, process.stderr):
                if stream is not None:
                    stream.close()


def _capture_binary(process: subprocess.Popen[bytes], destination: BinaryIO) -> None:
    """Bounded streaming only; do not widen the generic command runner's limits."""
    assert process.stdout is not None and process.stderr is not None
    deadline, size, diagnostics = time.monotonic() + _SECONDS, 0, 0
    digest = hashlib.sha256()
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ, True)
        selector.register(process.stderr, selectors.EVENT_READ, False)
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("CNPG manager binary capture timed out")
            for key, _event in selector.select(remaining):
                chunk = os.read(key.fd, _CHUNK)
                if not chunk:
                    selector.unregister(key.fileobj)
                elif key.data:
                    size += len(chunk)
                    if size > CNPG_MANAGER_SIZE:
                        raise RuntimeError("CNPG manager binary exceeds pinned size")
                    digest.update(chunk)
                    destination.write(chunk)
                else:
                    diagnostics += len(chunk)
                    if diagnostics > _CHUNK:
                        raise RuntimeError("CNPG manager binary diagnostics exceeded bound")
    remaining = deadline - time.monotonic()
    if (remaining <= 0 or process.wait(timeout=remaining) != 0 or size != CNPG_MANAGER_SIZE
            or digest.hexdigest() != CNPG_MANAGER_SHA256):
        raise RuntimeError("CNPG manager binary does not match independently pinned image")
    destination.seek(0)


def _forward_port(process: subprocess.Popen[bytes]) -> int:
    assert process.stdout is not None and process.stderr is not None
    deadline, stdout, stderr = time.monotonic() + 30, b"", b""
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ, True)
        selector.register(process.stderr, selectors.EVENT_READ, False)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or process.poll() is not None:
                raise RuntimeError("CNPG manager Pod tunnel did not become ready")
            for key, _event in selector.select(remaining):
                chunk = os.read(key.fd, 4096)
                if not chunk:
                    raise RuntimeError("CNPG manager Pod tunnel closed before readiness")
                if key.data:
                    stdout += chunk
                else:
                    stderr += chunk
                if len(stdout) > 4096 or len(stderr) > 4096:
                    raise RuntimeError("CNPG manager Pod tunnel response exceeded bound")
                match = re.fullmatch(rb"Forwarding from 127\.0\.0\.1:([0-9]{1,5}) -> 8000\n", stdout)
                if match is not None and 0 < int(match[1]) < 65536:
                    return int(match[1])


@dataclass(frozen=True, slots=True)
class _UpdateChannel:
    binary: BinaryIO
    port: int
    certificate_der: bytes

    def issue(self) -> None:
        # All subprocess/SQL checks AND durable dispatch precede the final TLS
        # connection. CNPG allows only 3s for headers and 20s for the request.
        # A failed dial/handshake still consumes issuance; never retry the PUT.
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        # Pin the exact public leaf read through authenticated Kubernetes.
        # No ambient CA/hostname trust and no credentials leave this tunnel.
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        with socket.create_connection(("127.0.0.1", self.port), timeout=3) as local:
            with context.wrap_socket(local, server_hostname="loom-postgres-rw") as secure:
                if secure.getpeercert(binary_form=True) != self.certificate_der:
                    raise RuntimeError("CNPG manager tunnel certificate changed")
                connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=20)
                connection.auto_open = 0
                connection.sock = secure
                try:
                    self._upload(connection)
                finally:
                    connection.close()

    def _upload(self, connection: http.client.HTTPConnection) -> None:
        # No redirects, proxy lookup or automatic request/socket retry.
        deadline = time.monotonic() + 20
        connection.putrequest("PUT", "/update", skip_host=True, skip_accept_encoding=True)
        connection.putheader("Host", "loom-postgres-rw")
        connection.putheader("Content-Length", str(CNPG_MANAGER_SIZE))
        connection.putheader("Content-Type", "application/octet-stream")
        connection.putheader("Connection", "close")
        connection.endheaders()
        while chunk := self.binary.read(_CHUNK):
            remaining = deadline - time.monotonic()
            if remaining <= 0 or connection.sock is None:
                raise TimeoutError("CNPG manager upload deadline expired")
            connection.sock.settimeout(remaining)
            connection.send(chunk)
        remaining = deadline - time.monotonic()
        if remaining <= 0 or connection.sock is None:
            raise TimeoutError("CNPG manager upload deadline expired")
        connection.sock.settimeout(remaining)
        # Keep the socket alive for remote receipt/EOF. Closing immediately after
        # send can truncate buffered TLS data. No reply or EOF proves execution.
        with connection.getresponse():
            pass


@contextmanager
def _prepared_update(
    runner: SubprocessProtectedApplyCommandRunner, journal: ProtectedApplyJournal,
    identity: CNPGManagerIdentity, runtime_password: str | None,
) -> Iterator[_UpdateChannel]:
    if _observe_identity(runner, identity) != identity:
        raise RuntimeError("CNPG manager process identity changed")
    with tempfile.TemporaryFile(mode="w+b") as binary:
        with _child(_exec(identity, "cat", "/proc/1/exe"), runner.environment) as capture:
            _capture_binary(capture, binary)
        certificate = runner.capture_stdout(
            _exec(identity, "cat", "/controller/certificates/server.crt"),
            env=runner.environment, timeout_seconds=30,
        )
        if len(certificate) > 16 * 1024:
            raise RuntimeError("CNPG manager TLS certificate exceeded bound")
        expected_der = ssl.PEM_cert_to_DER_cert(certificate.decode("ascii"))
        with _child(("kubectl", "--namespace", _NAMESPACE, "port-forward", f"pod/{identity.pod_name}",
                     ":8000", "--address=127.0.0.1"), runner.environment) as forward:
            port = _forward_port(forward)
            if _observe_identity(runner, identity) != identity:
                raise RuntimeError("CNPG manager process identity changed before dispatch")
            original = journal.read_application_admission_recovery()
            assert original is not None and original.coordination_guard is not None
            recoveries = journal.read_application_handoff_recoveries()
            if recoveries and recoveries[-1][1] is None:
                raise RuntimeError("CNPG manager replacement cannot overlap pending peer recovery")
            peer = recoveries[-1][1] if recoveries else None
            with runner.open_staging_peer_maintenance_database() as maintenance:
                require_application_database_drained(
                    maintenance, target=original.target, provisioner_role="postgres",
                    handoff_backend=peer.handoff_backend if peer else original.handoff_backend,
                    coordination_guard=original.coordination_guard, runtime_password=runtime_password,
                )
                yield _UpdateChannel(binary, port, expected_der)


def issue_staging_manager_replacement(
    runner: SubprocessProtectedApplyCommandRunner, *, journal: ProtectedApplyJournal,
    runtime_password: str | None = None,
) -> bool:
    """True means issued, NEVER executed; False means already issued, observe only."""
    record = journal.read_application_manager_replacement()
    if record is None:
        raise RuntimeError("CNPG manager transport requires durable original intent")
    intent, dispatched, _receipt = record
    if dispatched:
        return False
    with _prepared_update(runner, journal, intent.identity, runtime_password) as channel:
        if not journal.begin_application_manager_replacement():
            return False
        try:
            channel.issue()
        except (OSError, http.client.HTTPException):
            pass  # Includes timeout/EOF. Durable issuance remains; no retries.
    return True
