from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import threading
import time
from collections.abc import Mapping
from pathlib import Path

import pytest

from loom_cli.rollout.operator.readonly_database_client import (
    InstalledReadonlyDatabaseEvidenceSource,
    InstalledReadonlyMutationEpochSource,
    ReadonlyDatabaseTunnelError,
    open_readonly_database_query,
)
from loom_cli.rollout.readonly_database_authority import (
    ReadonlyDatabaseEvidence,
    ReadonlyMutationEpochEvidence,
)
from loom_cli.rollout.readonly_database_bootstrap import ReadonlyDatabaseCredential


class Process:
    def __init__(
        self,
        *,
        stderr: str | None = None,
        returncode: int | None = None,
    ) -> None:
        self.returncode = returncode
        self.stderr = None if stderr is None else io.StringIO(stderr)
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = 0

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        assert timeout is None or timeout <= 5
        if self.returncode is None:
            raise AssertionError("wait before stop")
        return self.returncode


class Cursor:
    def __init__(self, rows: tuple[Mapping[str, object], ...] = ()) -> None:
        self.rows = rows

    def fetchall(self) -> tuple[Mapping[str, object], ...]:
        return self.rows


class Connection:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.closed = False

    def execute(self, query: str, params=None) -> Cursor:
        assert params is None
        self.calls.append(query)
        return Cursor(({"value": 1},) if query == "SELECT 1 AS value" else ())

    def close(self) -> None:
        self.closed = True


def _private(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(0o600)


def _paths(tmp_path: Path) -> tuple[Path, Path, ReadonlyDatabaseCredential]:
    kubeconfig = tmp_path / "readonly-kubeconfig"
    credential_path = tmp_path / "readonly-database.json"
    credential = ReadonlyDatabaseCredential(
        role="loom_rollout_readonly",
        database="loom",
        password="a" * 64,
    )
    _private(kubeconfig, b"exact-kubeconfig")
    _private(credential_path, credential.to_bytes())
    return kubeconfig, credential_path, credential


def test_client_binds_exact_transport_and_keeps_password_out_of_process(tmp_path: Path) -> None:
    kubeconfig, credential_path, credential = _paths(tmp_path)
    process = Process()
    connection = Connection()
    spawns: list[tuple[tuple[str, ...], Mapping[str, str]]] = []
    connections: list[tuple[str, int, ReadonlyDatabaseCredential]] = []

    with open_readonly_database_query(
        service_uid=os.getuid(),
        kubeconfig_path=kubeconfig,
        credential_path=credential_path,
        spawn=lambda argv, env: spawns.append((tuple(argv), dict(env))) or process,
        connect=lambda host, port, exact: connections.append((host, port, exact)) or connection,
        allocate_port=lambda: 15432,
        wait_ready=lambda exact_process, port: (
            (exact_process is process and port == 15432)
            or (_ for _ in ()).throw(AssertionError("wrong tunnel"))
        ),
    ) as query:
        assert query("SELECT 1 AS value") == ({"value": 1},)

    assert spawns == [
        (
            (
                "kubectl",
                "--kubeconfig",
                str(kubeconfig),
                "--namespace",
                "loom-staging",
                "port-forward",
                "service/loom-postgres-rw",
                "--address=127.0.0.1",
                "15432:5432",
                "--pod-running-timeout=15s",
            ),
            {
                "HOME": "/var/lib/loom-staging-rollout",
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "USER": "loom-rollout",
            },
        )
    ]
    assert connections == [("127.0.0.1", 15432, credential)]
    assert credential.password not in json.dumps(spawns)
    assert connection.calls == [
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
        "SELECT 1 AS value",
        "ROLLBACK",
    ]
    assert connection.closed
    assert process.terminated and not process.killed


def test_client_rolls_back_and_stops_exact_tunnel_on_query_failure(tmp_path: Path) -> None:
    kubeconfig, credential_path, _credential = _paths(tmp_path)
    process = Process()
    connection = Connection()

    with pytest.raises(RuntimeError, match="probe failed"):
        with open_readonly_database_query(
            service_uid=os.getuid(),
            kubeconfig_path=kubeconfig,
            credential_path=credential_path,
            spawn=lambda _argv, _env: process,
            connect=lambda _host, _port, _exact: connection,
            allocate_port=lambda: 15433,
            wait_ready=lambda _process, _port: None,
        ):
            raise RuntimeError("probe failed")

    assert connection.calls == [
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
        "ROLLBACK",
    ]
    assert connection.closed and process.terminated


def test_client_rejects_public_or_symlinked_credential(tmp_path: Path) -> None:
    kubeconfig, credential_path, _credential = _paths(tmp_path)
    credential_path.chmod(0o644)
    with pytest.raises(ValueError, match="is unsafe"):
        with open_readonly_database_query(
            service_uid=os.getuid(),
            kubeconfig_path=kubeconfig,
            credential_path=credential_path,
        ):
            raise AssertionError("unreachable")

    credential_path.unlink()
    target = tmp_path / "target"
    _private(target, b"{}")
    credential_path.symlink_to(target)
    with pytest.raises(ValueError, match="is unsafe"):
        with open_readonly_database_query(
            service_uid=os.getuid(),
            kubeconfig_path=kubeconfig,
            credential_path=credential_path,
        ):
            raise AssertionError("unreachable")


@pytest.mark.parametrize(
    ("stderr", "wait_error", "expected_kind"),
    [
        (
            "error: You must be logged in to the server (Unauthorized) token=private-value\n",
            RuntimeError("readonly database port-forward exited early"),
            "credential",
        ),
        (
            "error: unable to forward port because the pod exited\n",
            RuntimeError("readonly database port-forward exited early"),
            "transport",
        ),
        (
            "waiting for pod readiness\n",
            RuntimeError("readonly database port-forward timed out"),
            "timeout",
        ),
    ],
)
def test_client_classifies_tunnel_failures_with_private_sanitized_diagnostic(
    tmp_path: Path,
    stderr: str,
    wait_error: Exception,
    expected_kind: str,
) -> None:
    kubeconfig, credential_path, _credential = _paths(tmp_path)
    process = Process(stderr=stderr, returncode=1)

    with pytest.raises(ReadonlyDatabaseTunnelError) as caught:
        with open_readonly_database_query(
            service_uid=os.getuid(),
            kubeconfig_path=kubeconfig,
            credential_path=credential_path,
            spawn=lambda _argv, _env: process,
            allocate_port=lambda: 15434,
            wait_ready=lambda _process, _port: (_ for _ in ()).throw(wait_error),
        ):
            raise AssertionError("unreachable")

    assert caught.value.kind == expected_kind
    assert str(caught.value) == f"readonly database tunnel {expected_kind} failure"
    assert caught.value.diagnostic is not None
    assert "private-value" not in caught.value.diagnostic
    assert "private-value" not in str(caught.value)


def test_client_caps_diagnostic_after_redaction(tmp_path: Path) -> None:
    kubeconfig, credential_path, _credential = _paths(tmp_path)
    process = Process(
        stderr=("x" * 9000) + " token=private-value\n",
        returncode=1,
    )

    with pytest.raises(ReadonlyDatabaseTunnelError) as caught:
        with open_readonly_database_query(
            service_uid=os.getuid(),
            kubeconfig_path=kubeconfig,
            credential_path=credential_path,
            spawn=lambda _argv, _env: process,
            allocate_port=lambda: 15435,
            wait_ready=lambda _process, _port: (_ for _ in ()).throw(
                RuntimeError("readonly database port-forward exited early")
            ),
        ):
            raise AssertionError("unreachable")

    assert caught.value.diagnostic is not None
    assert len(caught.value.diagnostic) <= 8 * 1024
    assert "private-value" not in caught.value.diagnostic


def test_client_stops_live_child_before_collecting_short_real_pipe_stderr(
    tmp_path: Path,
) -> None:
    kubeconfig, credential_path, _credential = _paths(tmp_path)
    marker = tmp_path / "stderr-ready"
    processes: list[subprocess.Popen[str]] = []

    def spawn(_argv, _environment):  # type: ignore[no-untyped-def]
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                (
                    "import pathlib,sys,time; "
                    "sys.stderr.write('Unauthorized token=private-value\\n'); "
                    "sys.stderr.flush(); pathlib.Path(sys.argv[1]).touch(); time.sleep(30)"
                ),
                str(marker),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        processes.append(process)
        return process

    def fail_after_short_write(_process, _port):  # type: ignore[no-untyped-def]
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and not marker.exists():
            time.sleep(0.01)
        assert marker.exists()
        raise RuntimeError("readonly database port-forward exited early")

    with pytest.raises(ReadonlyDatabaseTunnelError) as caught:
        with open_readonly_database_query(
            service_uid=os.getuid(),
            kubeconfig_path=kubeconfig,
            credential_path=credential_path,
            spawn=spawn,
            allocate_port=lambda: 15436,
            wait_ready=fail_after_short_write,
        ):
            raise AssertionError("unreachable")

    assert caught.value.kind == "credential"
    assert caught.value.diagnostic == "Unauthorized token=[REDACTED:token]"
    assert processes[0].poll() is not None


def test_installed_evidence_source_is_single_flight_under_concurrent_dag() -> None:
    evidence = ReadonlyDatabaseEvidence(
        schema_revision="0065",
        mutation_epoch=0,
        epoch_authority="legacy-pre-0069",
        baseline_counts={
            "agents": 0,
            "provider_models": 0,
            "tasks": 0,
            "teams": 0,
            "users": 0,
        },
        capacity=None,
        evidence_sha256="a" * 64,
    )
    calls: list[int] = []
    entered = threading.Barrier(4)

    def probe(*, service_uid: int) -> ReadonlyDatabaseEvidence:
        calls.append(service_uid)
        return evidence

    source = InstalledReadonlyDatabaseEvidenceSource(
        service_uid=os.getuid(),
        probe=probe,
    )
    results: list[ReadonlyDatabaseEvidence] = []

    def invoke() -> None:
        entered.wait()
        results.append(source())

    threads = [threading.Thread(target=invoke) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert results == [evidence] * 4
    assert calls == [os.getuid()]


def test_installed_epoch_source_is_single_flight_before_concurrent_dag() -> None:
    evidence = ReadonlyMutationEpochEvidence(
        schema_revision="0070",
        mutation_epoch=12,
        epoch_authority="staging-mutation-epoch-v1",
        evidence_sha256="b" * 64,
    )
    calls: list[int] = []
    entered = threading.Barrier(4)

    def probe(*, service_uid: int) -> ReadonlyMutationEpochEvidence:
        calls.append(service_uid)
        return evidence

    source = InstalledReadonlyMutationEpochSource(
        service_uid=os.getuid(),
        probe=probe,
    )
    results: list[ReadonlyMutationEpochEvidence] = []

    def invoke() -> None:
        entered.wait()
        results.append(source())

    threads = [threading.Thread(target=invoke) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert results == [evidence] * 4
    assert calls == [os.getuid()]


def test_installed_epoch_source_refreshes_after_protected_mutation() -> None:
    before = ReadonlyMutationEpochEvidence(
        schema_revision="0070",
        mutation_epoch=12,
        epoch_authority="staging-mutation-epoch-v1",
        evidence_sha256="b" * 64,
    )
    after = ReadonlyMutationEpochEvidence(
        schema_revision="0070",
        mutation_epoch=13,
        epoch_authority="staging-mutation-epoch-v1",
        evidence_sha256="c" * 64,
    )
    observations = iter((before, after))
    calls: list[int] = []

    def probe(*, service_uid: int) -> ReadonlyMutationEpochEvidence:
        calls.append(service_uid)
        return next(observations)

    source = InstalledReadonlyMutationEpochSource(
        service_uid=os.getuid(),
        probe=probe,
    )

    assert source().mutation_epoch == 12
    assert source().mutation_epoch == 12
    assert source.refresh().mutation_epoch == 13
    assert source().mutation_epoch == 13
    assert calls == [os.getuid(), os.getuid()]


def test_installed_epoch_source_failed_refresh_preserves_snapshot_and_fails_closed() -> None:
    before = ReadonlyMutationEpochEvidence(
        schema_revision="0070",
        mutation_epoch=12,
        epoch_authority="staging-mutation-epoch-v1",
        evidence_sha256="b" * 64,
    )
    calls = 0

    def probe(*, service_uid: int) -> ReadonlyMutationEpochEvidence:
        nonlocal calls
        assert service_uid == os.getuid()
        calls += 1
        if calls == 1:
            return before
        raise RuntimeError("current mutation epoch is unavailable")

    source = InstalledReadonlyMutationEpochSource(
        service_uid=os.getuid(),
        probe=probe,
    )

    assert source() == before
    with pytest.raises(RuntimeError, match="current mutation epoch is unavailable"):
        source.refresh()
    assert source() == before
    assert calls == 2


def test_installed_database_source_caches_one_fail_closed_probe_per_concurrent_dag() -> None:
    calls: list[int] = []
    entered = threading.Barrier(4)

    def probe(*, service_uid: int) -> ReadonlyDatabaseEvidence:
        calls.append(service_uid)
        raise ValueError("readonly database capacity evidence is incomplete")

    source = InstalledReadonlyDatabaseEvidenceSource(
        service_uid=os.getuid(),
        probe=probe,
    )
    failures: list[str] = []

    def invoke() -> None:
        entered.wait()
        with pytest.raises(RuntimeError, match="capacity evidence is incomplete") as caught:
            source()
        failures.append(str(caught.value))

    threads = [threading.Thread(target=invoke) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert calls == [os.getuid()]
    assert failures == ["ValueError: readonly database capacity evidence is incomplete"] * 4


def test_installed_database_source_preserves_typed_tunnel_failure() -> None:
    calls: list[int] = []
    failure = ReadonlyDatabaseTunnelError(
        "credential",
        "Unauthorized token=private-value",
    )

    def probe(*, service_uid: int) -> ReadonlyDatabaseEvidence:
        calls.append(service_uid)
        raise failure

    source = InstalledReadonlyDatabaseEvidenceSource(
        service_uid=os.getuid(),
        probe=probe,
    )

    for _ in range(2):
        with pytest.raises(ReadonlyDatabaseTunnelError) as caught:
            source()
        assert caught.value.kind == "credential"
        assert caught.value.diagnostic == "Unauthorized token=[REDACTED:token]"
    assert calls == [os.getuid()]
