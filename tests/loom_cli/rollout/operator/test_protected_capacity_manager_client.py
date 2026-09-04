from __future__ import annotations

import gc
import io
import json
import os
import signal
import socket
import ssl
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from loom_capacity_manager.executable_contracts import (
    ExecutionContextV2,
    ExecutionPreparationAbortV2,
    ExecutionPreparationV2,
)
from loom_capacity_manager.preparation_readiness import (
    PreparedExecutionReadinessV2,
    canonical_prepared_readiness_digest,
)
from loom_cli.rollout.operator import protected_capacity_manager_client as client_module
from loom_cli.rollout.operator.protected_capacity_manager_client import (
    ProtectedCapacityManagerClient,
    ProtectedCapacityManagerClientError,
    open_protected_capacity_manager_client,
)
from tests.loom_cli.rollout.operator.protected_execution_prerequisite_fixtures import (
    execution_prerequisite_artifact,
)
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_runtime import (
    _runtime,
    _write_bootstrap,
)

_KEYS = (
    UUID("00000000-0000-4000-8000-000000000101"),
    UUID("00000000-0000-4000-8000-000000000102"),
    UUID("00000000-0000-4000-8000-000000000103"),
    UUID("00000000-0000-4000-8000-000000000104"),
)


def _pod(
    name: str,
    *,
    phase: str = "Running",
    deleting: bool = False,
    duplicate_status: bool = False,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "name": name,
        "namespace": "loom-dev",
        "labels": {
            "app.kubernetes.io/name": "loom-capacity-manager",
            "app.kubernetes.io/part-of": "loom",
            "loom.yylx.dev/capacity-component": "manager",
        },
    }
    if deleting:
        metadata["deletionTimestamp"] = "2026-09-03T00:00:00Z"
    running = phase == "Running"
    container_statuses = [
        {
            "name": "manager",
            "ready": running,
            "started": running,
            "state": {"running": {}} if running else {"terminated": {}},
        },
        {
            "name": "global-execution-witness-publisher",
            "ready": running,
            "started": running,
            "state": {"running": {}} if running else {"terminated": {}},
        },
    ]
    if duplicate_status:
        container_statuses.append(dict(container_statuses[0]))
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": metadata,
        "spec": {
            "containers": [
                {"name": "manager"},
                {"name": "global-execution-witness-publisher"},
            ]
        },
        "status": {
            "phase": phase,
            "conditions": [{"type": "Ready", "status": "True" if running else "False"}],
            "containerStatuses": container_statuses,
        },
    }


class _Runner:
    def __init__(self, kubeconfig: Path, pods: list[dict[str, object]]) -> None:
        self.environment = {
            "HOME": "/var/lib/loom-staging-rollout",
            "KUBECONFIG": str(kubeconfig),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "PYTHONDONTWRITEBYTECODE": "1",
            "XDG_RUNTIME_DIR": f"/run/user/{os.geteuid()}",
        }
        self.pods = pods
        self.calls: list[tuple[str, ...]] = []

    def capture_stdout(self, argv, *, env, timeout_seconds):
        assert env == self.environment
        assert timeout_seconds == 15.0
        self.calls.append(tuple(argv))
        return json.dumps(
            {
                "apiVersion": "v1",
                "kind": "List",
                "metadata": {},
                "items": self.pods,
            },
            sort_keys=True,
        ).encode()


class _Process:
    def __init__(self, *, stdout: str = "", stderr: str = "") -> None:
        self.stdout = io.StringIO(stdout)
        self.stderr = io.StringIO(stderr)
        self.returncode: int | None = None
        self.terminated = False
        self.killed = False
        self.wait_timeouts: list[float | None] = []

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = 0

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        self.wait_timeouts.append(timeout)
        assert self.returncode is not None
        return self.returncode


class _Response:
    def __init__(
        self,
        body: bytes,
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.headers = {
            "content-type": "application/json",
            "content-encoding": "identity",
            **(headers or {}),
        }
        self._body = body

    def iter_bytes(self, *, chunk_size: int) -> list[bytes]:
        return [
            self._body[offset : offset + chunk_size]
            for offset in range(0, len(self._body), chunk_size)
        ]


class _HTTPClient:
    def __init__(self, responses: list[_Response]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []
        self.closed = False

    @contextmanager
    def stream(self, method: str, url: str, **kwargs: Any):
        self.calls.append({"method": method, "url": url, **kwargs})
        yield self.responses.pop(0)

    def close(self) -> None:
        self.closed = True


def _credentials(tmp_path: Path) -> Path:
    runtime = _runtime(tmp_path)
    _write_bootstrap(runtime)
    return runtime.credentials_root


def _kubeconfig(tmp_path: Path) -> Path:
    path = tmp_path / "protected-kubeconfig"
    path.write_text("apiVersion: v1\nkind: Config\n", encoding="utf-8")
    path.chmod(0o600)
    return path


def _prepared_execution() -> ExecutionContextV2:
    artifact = execution_prerequisite_artifact()
    return ExecutionContextV2(
        authority_incarnation=UUID(artifact.executor_profile_seed.authority_incarnation),
        writer_epoch=12,
        configuration_epoch=10,
        execution_epoch=1,
        execution_manifest_sha256="a" * 64,
        execution_state="prepared",
        executable_new_capacity_ceiling=0,
        executable_new_capacity_rate_per_minute=0,
        trusted_fleet_release_sha256=(artifact.execution_policy.trusted_fleet_release_sha256),
    )


def _execution_preparation() -> ExecutionPreparationV2:
    artifact = execution_prerequisite_artifact()
    policy = artifact.execution_policy
    return ExecutionPreparationV2(
        authority_incarnation=UUID(artifact.executor_profile_seed.authority_incarnation),
        expected_writer_epoch=11,
        configuration_epoch=10,
        fleet_generation=artifact.desired_fleet_generation,
        fleet_digest=artifact.desired_fleet_sha256,
        trusted_fleet_release_sha256=policy.trusted_fleet_release_sha256,
        requested_ceiling=policy.executable_new_capacity_ceiling,
        requested_rate_per_minute=policy.executable_new_capacity_rate_per_minute,
        executors=policy.executors,
        subject_acknowledgements=policy.subject_acknowledgements,
        legacy_writer_fences=policy.legacy_writer_fences,
        rollback_evidence_sha256=policy.rollback_evidence_sha256,
    )


def test_client_binds_one_ready_pod_and_uses_four_scoped_credentials(
    tmp_path: Path,
) -> None:
    credentials_root = _credentials(tmp_path)
    kubeconfig = _kubeconfig(tmp_path)
    runner = _Runner(
        kubeconfig,
        [
            _pod("loom-capacity-manager-old", phase="Succeeded"),
            _pod("loom-capacity-manager-terminating", deleting=True),
            _pod("loom-capacity-manager-exact"),
        ],
    )
    process = _Process()
    spawned: list[tuple[tuple[str, ...], dict[str, str]]] = []
    http = _HTTPClient(
        [
            _Response(b'{"configuration":"original","schema_version":1}'),
            _Response(b'{"digest":"fleet"}'),
            _Response(b'{"digest":"subject"}'),
            _Response(b'{"digest":"activation"}'),
        ]
    )
    contexts: list[ssl.SSLContext] = []

    def spawn(argv, environment):
        spawned.append((tuple(argv), dict(environment)))
        return process

    def client_factory(context: ssl.SSLContext):
        contexts.append(context)
        return http

    with open_protected_capacity_manager_client(
        runner=runner,
        credentials_root=credentials_root,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        spawn=spawn,
        allocate_port=lambda: 43210,
        wait_ready=lambda _process, port, _output: port == 43210,
        client_factory=client_factory,
    ) as client:
        assert client.get_configuration()["configuration"] == "original"
        assert client.propose_fleet({"schema_version": 1}, _KEYS[0]) == {"digest": "fleet"}
        assert client.propose_subject(
            UUID("00000000-0000-4000-8000-000000000201"),
            {"schema_version": 1},
            _KEYS[1],
        ) == {"digest": "subject"}
        assert client.activate({"schema_version": 1}, _KEYS[2]) == {"digest": "activation"}

    assert len(runner.calls) == 1
    query = runner.calls[0]
    snapshot = Path(query[2])
    assert query == (
        "kubectl",
        "--kubeconfig",
        str(snapshot),
        "--namespace",
        "loom-dev",
        "get",
        "pods",
        "--selector=app.kubernetes.io/name=loom-capacity-manager,loom.yylx.dev/capacity-component=manager",
        "--output=json",
        "--request-timeout=15s",
    )
    assert spawned == [
        (
            (
                "kubectl",
                "--kubeconfig",
                str(snapshot),
                "--namespace",
                "loom-dev",
                "port-forward",
                "pod/loom-capacity-manager-exact",
                "--address=127.0.0.1",
                "43210:8443",
                "--pod-running-timeout=15s",
            ),
            runner.environment,
        )
    ]
    assert snapshot.parent == credentials_root.parent / "capacity-manager-kubeconfig-snapshots"
    assert not snapshot.exists()
    assert process.terminated and process.wait_timeouts == [5.0]
    assert http.closed
    assert len(contexts) == 4
    assert [call["method"] for call in http.calls] == ["GET", "PUT", "PUT", "POST"]
    assert [call["url"] for call in http.calls] == [
        "https://127.0.0.1:43210/v1/configuration",
        "https://127.0.0.1:43210/v1/config-proposals/fleet",
        "https://127.0.0.1:43210/v1/config-proposals/subjects/00000000-0000-4000-8000-000000000201",
        "https://127.0.0.1:43210/v1/config-activations",
    ]
    expected_tokens = [
        f"Bearer token-{name}-{'x' * 48}"
        for name in (
            "configuration-read",
            "configuration-fleet",
            "configuration-subject",
            "configuration-activate",
        )
    ]
    assert [call["headers"]["Authorization"] for call in http.calls] == expected_tokens
    assert "Idempotency-Key" not in http.calls[0]["headers"]
    assert [call["headers"]["Idempotency-Key"] for call in http.calls[1:]] == [
        str(key) for key in _KEYS[:3]
    ]
    assert all(call["headers"]["Accept-Encoding"] == "identity" for call in http.calls)
    assert "content" not in http.calls[0]
    assert [call["content"] for call in http.calls[1:]] == [
        b'{"schema_version":1}',
        b'{"schema_version":1}',
        b'{"schema_version":1}',
    ]
    flattened = " ".join(item for argv, _environment in spawned for item in argv)
    assert "token-configuration" not in flattened
    assert "private-key" not in flattened


def test_client_uses_activation_credential_for_configuration_rollback(
    tmp_path: Path,
) -> None:
    runner = _Runner(_kubeconfig(tmp_path), [_pod("manager-exact")])
    process = _Process()
    http = _HTTPClient([_Response(b'{"digest":"rollback"}')])

    with open_protected_capacity_manager_client(
        runner=runner,
        credentials_root=_credentials(tmp_path),
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        spawn=lambda _argv, _environment: process,
        allocate_port=lambda: 43210,
        wait_ready=lambda _process, _port, _output: None,
        client_factory=lambda _context: http,
    ) as client:
        assert client.rollback({"schema_version": 1}, _KEYS[3]) == {"digest": "rollback"}

    assert process.terminated and http.closed
    assert len(http.calls) == 1
    assert http.calls[0]["method"] == "POST"
    assert http.calls[0]["url"] == "https://127.0.0.1:43210/v1/configuration-rollbacks"
    assert http.calls[0]["headers"]["Authorization"] == (
        f"Bearer token-configuration-activate-{'x' * 48}"
    )
    assert http.calls[0]["headers"]["Idempotency-Key"] == str(_KEYS[3])
    assert http.calls[0]["content"] == b'{"schema_version":1}'


def test_client_uses_execution_read_credential_for_manager_status(tmp_path: Path) -> None:
    credentials_root = _credentials(tmp_path)
    runner = _Runner(_kubeconfig(tmp_path), [_pod("manager-exact")])
    process = _Process()
    http = _HTTPClient(
        [
            _Response(
                b'{"execution_state":"shadow","execution_epoch":0,'
                b'"executable_new_capacity_ceiling":0,"increase_freeze":true}'
            )
        ]
    )

    with open_protected_capacity_manager_client(
        runner=runner,
        credentials_root=credentials_root,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        spawn=lambda _argv, _environment: process,
        allocate_port=lambda: 43210,
        wait_ready=lambda _process, _port, _output: None,
        client_factory=lambda _context: http,
    ) as client:
        assert client.get_status()["execution_state"] == "shadow"

    assert process.terminated and http.closed
    assert len(http.calls) == 1
    assert http.calls[0]["method"] == "GET"
    assert http.calls[0]["url"] == "https://127.0.0.1:43210/v1/status"
    assert http.calls[0]["headers"]["Authorization"] == (
        f"Bearer {credentials_root.joinpath('manager-read', 'bearer-token').read_text()}"
    )


def test_client_uses_separate_execution_transition_credentials_and_exact_parsers(
    tmp_path: Path,
) -> None:
    credentials_root = _credentials(tmp_path)
    runner = _Runner(_kubeconfig(tmp_path), [_pod("manager-exact")])
    process = _Process()
    preparation = _execution_preparation()
    prepared = _prepared_execution()
    readiness = PreparedExecutionReadinessV2(
        ready=False,
        policy_mode="pinned",
        policy_sha256="c" * 64,
        execution=prepared,
        expected_subject_count=1,
        acknowledged_subject_count=1,
        executors=(),
        blockers=("executor-registration-missing",),
    )
    readiness_payload = readiness.model_dump(mode="json") | {
        "readiness_sha256": canonical_prepared_readiness_digest(readiness)
    }
    abort_request = ExecutionPreparationAbortV2(
        authority_incarnation=prepared.authority_incarnation,
        expected_writer_epoch=prepared.writer_epoch,
        execution_epoch=prepared.execution_epoch,
        execution_manifest_sha256=prepared.execution_manifest_sha256,
    )
    retired_at = datetime(2026, 9, 4, 12, 30, tzinfo=UTC)
    http = _HTTPClient(
        [
            _Response(prepared.model_dump_json().encode("ascii")),
            _Response(json.dumps(readiness_payload, sort_keys=True).encode("ascii")),
            _Response(
                json.dumps(
                    {
                        "execution_epoch": prepared.execution_epoch,
                        "execution_manifest_sha256": prepared.execution_manifest_sha256,
                        "replayed": False,
                        "retired_at": retired_at.isoformat(),
                    },
                    sort_keys=True,
                ).encode("ascii")
            ),
        ]
    )

    with open_protected_capacity_manager_client(
        runner=runner,
        credentials_root=credentials_root,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        spawn=lambda _argv, _environment: process,
        allocate_port=lambda: 43210,
        wait_ready=lambda _process, _port, _output: None,
        client_factory=lambda _context: http,
    ) as client:
        assert client.prepare_execution(preparation, _KEYS[0]) == prepared
        status = client.get_execution_preparation_status()
        assert status.readiness == readiness
        assert status.readiness_sha256 == canonical_prepared_readiness_digest(readiness)
        retired = client.abort_execution_preparation(abort_request, _KEYS[1])
        assert retired.execution_epoch == prepared.execution_epoch
        assert retired.execution_manifest_sha256 == prepared.execution_manifest_sha256
        assert retired.retired_at == retired_at
        assert retired.replayed is False

    assert [call["method"] for call in http.calls] == ["POST", "GET", "POST"]
    assert [call["url"] for call in http.calls] == [
        "https://127.0.0.1:43210/v2/execution-preparations",
        "https://127.0.0.1:43210/v2/status/execution-preparation",
        "https://127.0.0.1:43210/v2/execution-preparations/1/abort",
    ]
    assert [call["headers"]["Authorization"] for call in http.calls] == [
        f"Bearer {credentials_root.joinpath(name, 'bearer-token').read_text()}"
        for name in ("manager-prepare", "manager-read", "manager-abort")
    ]
    assert "Idempotency-Key" not in http.calls[1]["headers"]
    assert [http.calls[index]["headers"]["Idempotency-Key"] for index in (0, 2)] == [
        str(_KEYS[0]),
        str(_KEYS[1]),
    ]
    assert json.loads(http.calls[0]["content"]) == preparation.model_dump(mode="json")
    assert json.loads(http.calls[2]["content"]) == abort_request.model_dump(mode="json")


def test_client_rejects_semantically_invalid_execution_responses(tmp_path: Path) -> None:
    runner = _Runner(_kubeconfig(tmp_path), [_pod("manager-exact")])
    process = _Process()
    prepared = _prepared_execution()
    readiness = PreparedExecutionReadinessV2(
        ready=False,
        policy_mode="pinned",
        policy_sha256="c" * 64,
        execution=prepared,
        expected_subject_count=1,
        acknowledged_subject_count=1,
        executors=(),
        blockers=("executor-registration-missing",),
    )
    malformed_prepared = prepared.model_dump(mode="json") | {"unexpected": True}
    mismatched_readiness = readiness.model_dump(mode="json") | {"readiness_sha256": "d" * 64}
    http = _HTTPClient(
        [
            _Response(json.dumps(malformed_prepared, sort_keys=True).encode("ascii")),
            _Response(json.dumps(mismatched_readiness, sort_keys=True).encode("ascii")),
        ]
    )

    with open_protected_capacity_manager_client(
        runner=runner,
        credentials_root=_credentials(tmp_path),
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        spawn=lambda _argv, _environment: process,
        allocate_port=lambda: 43210,
        wait_ready=lambda _process, _port, _output: None,
        client_factory=lambda _context: http,
    ) as client:
        with pytest.raises(ProtectedCapacityManagerClientError) as preparation_failure:
            client.prepare_execution(_execution_preparation(), _KEYS[0])
        with pytest.raises(ProtectedCapacityManagerClientError) as readiness_failure:
            client.get_execution_preparation_status()

    assert preparation_failure.value.reason == "unexpected"
    assert readiness_failure.value.reason == "unexpected"


def test_client_binds_kubectl_to_snapshot_and_rejects_original_replacement_at_spawn(
    tmp_path: Path,
) -> None:
    credentials_root = _credentials(tmp_path)
    kubeconfig = _kubeconfig(tmp_path)
    verified_payload = kubeconfig.read_bytes()
    replacement_payload = b"apiVersion: v1\nkind: Config\ncurrent-context: attacker\n"
    observed: dict[str, tuple[Path, bytes, int, int, int]] = {}

    class _SnapshotRunner(_Runner):
        def capture_stdout(self, argv, *, env, timeout_seconds):
            command = tuple(argv)
            if "--kubeconfig" in command:
                snapshot = Path(command[command.index("--kubeconfig") + 1])
                metadata = snapshot.stat(follow_symlinks=False)
                observed["query"] = (
                    snapshot,
                    snapshot.read_bytes(),
                    metadata.st_mode & 0o777,
                    metadata.st_uid,
                    metadata.st_gid,
                )
            return super().capture_stdout(argv, env=env, timeout_seconds=timeout_seconds)

    runner = _SnapshotRunner(kubeconfig, [_pod("manager-exact")])
    process = _Process()

    def spawn(argv, _environment):
        command = tuple(argv)
        if "--kubeconfig" in command:
            snapshot = Path(command[command.index("--kubeconfig") + 1])
            metadata = snapshot.stat(follow_symlinks=False)
            observed["spawn"] = (
                snapshot,
                snapshot.read_bytes(),
                metadata.st_mode & 0o777,
                metadata.st_uid,
                metadata.st_gid,
            )
        kubeconfig.write_bytes(replacement_payload)
        kubeconfig.chmod(0o600)
        return process

    with pytest.raises(ProtectedCapacityManagerClientError) as failure:
        with open_protected_capacity_manager_client(
            runner=runner,
            credentials_root=credentials_root,
            service_uid=os.geteuid(),
            service_gid=os.getegid(),
            spawn=spawn,
            allocate_port=lambda: 43210,
            wait_ready=lambda _process, _port, _output: None,
        ):
            pass

    assert failure.value.reason == "transport"
    query = observed["query"]
    spawned = observed["spawn"]
    assert query == spawned
    assert query[0] != kubeconfig
    assert query[1:] == (verified_payload, 0o600, os.geteuid(), os.getegid())
    assert not query[0].exists()
    assert kubeconfig.read_bytes() == replacement_payload
    assert process.terminated


def test_client_reconciles_private_crash_residue_outside_credential_root(
    tmp_path: Path,
) -> None:
    credentials_root = _credentials(tmp_path)
    scratch = credentials_root.parent / "capacity-manager-kubeconfig-snapshots"
    scratch.mkdir(mode=0o700)
    stale = scratch / ".capacity-manager-kubeconfig-stale"
    stale.write_text("apiVersion: v1\nkind: Config\n", encoding="utf-8")
    stale.chmod(0o600)
    runner = _Runner(_kubeconfig(tmp_path), [_pod("manager-exact")])
    process = _Process()

    with open_protected_capacity_manager_client(
        runner=runner,
        credentials_root=credentials_root,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        spawn=lambda _argv, _environment: process,
        allocate_port=lambda: 43210,
        wait_ready=lambda _process, _port, _output: None,
        client_factory=lambda _context: _HTTPClient([]),
    ):
        pass

    snapshot = Path(runner.calls[0][2])
    assert snapshot.parent == scratch
    assert not tuple(scratch.iterdir())
    assert {path.name for path in credentials_root.iterdir()} == {
        "client-ca.pem",
        "configuration-activate",
        "configuration-fleet",
        "configuration-read",
        "configuration-subject",
        "manager-abort",
        "manager-activate",
        "manager-drain",
        "manager-prepare",
        "manager-read",
        "manager-retire",
        "pool-executor-gb10",
        "pool-executor-oldlab",
        "pool-ownership-gb10",
        "pool-ownership-oldlab",
        "staging-reporter",
    }


def test_concurrent_clients_preserve_each_others_live_kubeconfig_snapshots(
    tmp_path: Path,
) -> None:
    credentials_root = _credentials(tmp_path)
    kubeconfig = _kubeconfig(tmp_path)
    expected_kubeconfig = kubeconfig.read_bytes()
    scratch = credentials_root.parent / "capacity-manager-kubeconfig-snapshots"
    first_snapshot_ready = threading.Event()
    release_first_query = threading.Event()
    first_client_ready = threading.Event()
    release_first_client = threading.Event()
    first_snapshots: list[Path] = []
    first_failures: list[Exception] = []

    class _BlockingRunner(_Runner):
        def capture_stdout(self, argv, *, env, timeout_seconds):
            command = tuple(argv)
            snapshot = Path(command[command.index("--kubeconfig") + 1])
            first_snapshots.append(snapshot)
            first_snapshot_ready.set()
            if not release_first_query.wait(timeout=5.0):
                raise AssertionError("first client query was not released")
            assert snapshot.read_bytes() == expected_kubeconfig
            return super().capture_stdout(argv, env=env, timeout_seconds=timeout_seconds)

    first_runner = _BlockingRunner(kubeconfig, [_pod("manager-exact")])

    def hold_first_client() -> None:
        try:
            with open_protected_capacity_manager_client(
                runner=first_runner,
                credentials_root=credentials_root,
                service_uid=os.geteuid(),
                service_gid=os.getegid(),
                spawn=lambda _argv, _environment: _Process(),
                allocate_port=lambda: 43210,
                wait_ready=lambda _process, _port, _output: None,
                client_factory=lambda _context: _HTTPClient([]),
            ):
                first_client_ready.set()
                if not release_first_client.wait(timeout=5.0):
                    raise AssertionError("first client teardown was not released")
        except Exception as exc:
            first_failures.append(exc)
            first_client_ready.set()

    first_thread = threading.Thread(target=hold_first_client)
    first_thread.start()
    assert first_snapshot_ready.wait(timeout=5.0)
    assert not first_failures
    assert len(first_snapshots) == 1
    first_snapshot = first_snapshots[0]
    assert first_snapshot.parent == scratch
    assert first_snapshot.read_bytes() == expected_kubeconfig

    second_runner = _Runner(kubeconfig, [_pod("manager-exact")])
    second_snapshot: Path | None = None
    try:
        with open_protected_capacity_manager_client(
            runner=second_runner,
            credentials_root=credentials_root,
            service_uid=os.geteuid(),
            service_gid=os.getegid(),
            spawn=lambda _argv, _environment: _Process(),
            allocate_port=lambda: 43211,
            wait_ready=lambda _process, _port, _output: None,
            client_factory=lambda _context: _HTTPClient([]),
        ):
            second_snapshot = Path(second_runner.calls[0][2])
            assert second_snapshot != first_snapshot
            assert first_snapshot.read_bytes() == expected_kubeconfig
            assert second_snapshot.read_bytes() == expected_kubeconfig
            release_first_query.set()
            assert first_client_ready.wait(timeout=5.0)
            assert not first_failures
            assert first_snapshot.exists()
            assert second_snapshot.exists()
        assert first_snapshot.exists()
    finally:
        release_first_query.set()
        release_first_client.set()
        first_thread.join(timeout=5.0)

    assert not first_thread.is_alive()
    assert not first_failures
    assert second_snapshot is not None
    assert not first_snapshot.exists()
    assert not second_snapshot.exists()
    assert not tuple(scratch.iterdir())


def test_live_teardown_cannot_break_concurrent_snapshot_prepare(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credentials_root = _credentials(tmp_path)
    kubeconfig = _kubeconfig(tmp_path)
    scratch = credentials_root.parent / "capacity-manager-kubeconfig-snapshots"
    first_runner = _Runner(kubeconfig, [_pod("manager-exact")])
    first_context = open_protected_capacity_manager_client(
        runner=first_runner,
        credentials_root=credentials_root,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        spawn=lambda _argv, _environment: _Process(),
        allocate_port=lambda: 43210,
        wait_ready=lambda _process, _port, _output: None,
        client_factory=lambda _context: _HTTPClient([]),
    )
    first_context.__enter__()

    original_open = os.open
    prepare_paused = threading.Event()
    release_prepare = threading.Event()
    second_client_ready = threading.Event()
    release_second_client = threading.Event()
    second_failures: list[Exception] = []
    paused = False

    def pause_snapshot_directory_open(path, flags, mode=0o777, *, dir_fd=None):
        nonlocal paused
        if (
            not paused
            and threading.current_thread().name == "snapshot-prepare-racer"
            and Path(path) == scratch
            and flags & getattr(os, "O_DIRECTORY", 0)
        ):
            paused = True
            prepare_paused.set()
            if not release_prepare.wait(timeout=5.0):
                raise AssertionError("concurrent snapshot prepare was not released")
        if dir_fd is None:
            return original_open(path, flags, mode)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(client_module.os, "open", pause_snapshot_directory_open)
    second_runner = _Runner(kubeconfig, [_pod("manager-exact")])

    def hold_second_client() -> None:
        try:
            with open_protected_capacity_manager_client(
                runner=second_runner,
                credentials_root=credentials_root,
                service_uid=os.geteuid(),
                service_gid=os.getegid(),
                spawn=lambda _argv, _environment: _Process(),
                allocate_port=lambda: 43211,
                wait_ready=lambda _process, _port, _output: None,
                client_factory=lambda _context: _HTTPClient([]),
            ):
                second_client_ready.set()
                if not release_second_client.wait(timeout=5.0):
                    raise AssertionError("second client teardown was not released")
        except Exception as exc:
            second_failures.append(exc)
            second_client_ready.set()

    second_thread = threading.Thread(
        target=hold_second_client,
        name="snapshot-prepare-racer",
    )
    first_closed = False
    try:
        second_thread.start()
        assert prepare_paused.wait(timeout=5.0)
        first_closed = True
        first_context.__exit__(None, None, None)
        release_prepare.set()
        assert second_client_ready.wait(timeout=5.0)
    finally:
        release_prepare.set()
        release_second_client.set()
        if not first_closed:
            first_context.__exit__(None, None, None)
        second_thread.join(timeout=5.0)

    assert not second_thread.is_alive()
    assert not second_failures
    assert len(second_runner.calls) == 1
    assert not tuple(scratch.iterdir())


@pytest.mark.parametrize(
    "pods",
    [
        [],
        [_pod("manager-a"), _pod("manager-b")],
        [_pod("manager-unready", phase="Pending")],
        [_pod("manager-duplicate-status", duplicate_status=True)],
    ],
)
def test_client_rejects_ambiguous_or_absent_ready_pod(
    tmp_path: Path,
    pods: list[dict[str, object]],
) -> None:
    runner = _Runner(_kubeconfig(tmp_path), pods)
    spawned = False

    def spawn(_argv, _environment):
        nonlocal spawned
        spawned = True
        raise AssertionError("unsafe pod selection reached port-forward")

    with pytest.raises(ProtectedCapacityManagerClientError, match="transport failure"):
        with open_protected_capacity_manager_client(
            runner=runner,
            credentials_root=_credentials(tmp_path),
            service_uid=os.geteuid(),
            service_gid=os.getegid(),
            spawn=spawn,
        ):
            pass
    assert not spawned


@pytest.mark.parametrize(
    ("response", "message"),
    [
        (
            _Response(b"{}", status_code=307, headers={"location": "https://example.test"}),
            "redirected",
        ),
        (_Response(b"x" * (8 * 1024 * 1024 + 1)), "size bound"),
        (_Response(b'{"schema_version":1,"schema_version":1}'), "invalid JSON"),
        (_Response(b"{}", headers={"content-encoding": "gzip"}), "encoding"),
    ],
)
def test_client_rejects_redirected_oversized_or_malformed_responses(
    tmp_path: Path,
    response: _Response,
    message: str,
) -> None:
    runner = _Runner(_kubeconfig(tmp_path), [_pod("manager-exact")])
    http = _HTTPClient([response])
    process = _Process()
    with pytest.raises(ProtectedCapacityManagerClientError, match=message):
        with open_protected_capacity_manager_client(
            runner=runner,
            credentials_root=_credentials(tmp_path),
            service_uid=os.geteuid(),
            service_gid=os.getegid(),
            spawn=lambda _argv, _environment: process,
            allocate_port=lambda: 43210,
            wait_ready=lambda _process, _port, _output: None,
            client_factory=lambda _context: http,
        ) as client:
            client.get_configuration()
    assert process.terminated


def test_client_enforces_one_deadline_across_slow_response_chunks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _Runner(_kubeconfig(tmp_path), [_pod("manager-exact")])
    process = _Process()

    class _SlowResponse(_Response):
        def iter_bytes(self, *, chunk_size: int):
            del chunk_size
            for chunk in (b'{"configuration":', b'"original",', b'"schema_version":1}'):
                time.sleep(0.012)
                yield chunk

    http = _HTTPClient([_SlowResponse(b"")])
    monkeypatch.setattr(client_module, "_HTTP_TIMEOUT_SECONDS", 0.02)

    with open_protected_capacity_manager_client(
        runner=runner,
        credentials_root=_credentials(tmp_path),
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        spawn=lambda _argv, _environment: process,
        allocate_port=lambda: 43210,
        wait_ready=lambda _process, _port, _output: None,
        client_factory=lambda _context: http,
    ) as client:
        with pytest.raises(ProtectedCapacityManagerClientError) as failure:
            client.get_configuration()

    assert failure.value.reason == "timeout"
    assert http.closed and process.terminated


def test_client_revalidates_private_credentials_before_each_request(tmp_path: Path) -> None:
    credentials_root = _credentials(tmp_path)
    runner = _Runner(_kubeconfig(tmp_path), [_pod("manager-exact")])
    process = _Process()
    http = _HTTPClient([_Response(b"{}")])
    with open_protected_capacity_manager_client(
        runner=runner,
        credentials_root=credentials_root,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        spawn=lambda _argv, _environment: process,
        allocate_port=lambda: 43210,
        wait_ready=lambda _process, _port, _output: None,
        client_factory=lambda _context: http,
    ) as client:
        token = credentials_root / "configuration-read" / "bearer-token"
        token.chmod(0o640)
        with pytest.raises(ProtectedCapacityManagerClientError, match="credential failure"):
            client.get_configuration()
    assert http.calls == []


@pytest.mark.parametrize(
    "origin",
    ["https://127.0.0.1:1023", "https://127.0.0.1:65536"],
)
def test_client_rejects_an_out_of_range_tunnel_origin(
    tmp_path: Path,
    origin: str,
) -> None:
    factory_called = False

    def client_factory(_context: ssl.SSLContext):
        nonlocal factory_called
        factory_called = True
        return _HTTPClient([])

    client = ProtectedCapacityManagerClient(
        origin=origin,
        credentials_root=_credentials(tmp_path),
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        client_factory=client_factory,
    )

    with pytest.raises(ValueError, match="tunnel origin"):
        client.get_configuration()

    assert not factory_called


@pytest.mark.parametrize("status_code", [200, 503])
def test_client_revalidates_private_credentials_after_every_response(
    tmp_path: Path,
    status_code: int,
) -> None:
    credentials_root = _credentials(tmp_path)
    token = credentials_root / "configuration-read" / "bearer-token"
    runner = _Runner(_kubeconfig(tmp_path), [_pod("manager-exact")])
    process = _Process()

    class _MutatingHTTPClient(_HTTPClient):
        @contextmanager
        def stream(self, method: str, url: str, **kwargs: Any):
            self.calls.append({"method": method, "url": url, **kwargs})
            token.write_text(f"replacement-{'y' * 48}", encoding="ascii")
            yield self.responses.pop(0)

    http = _MutatingHTTPClient([_Response(b"{}", status_code=status_code)])
    with open_protected_capacity_manager_client(
        runner=runner,
        credentials_root=credentials_root,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        spawn=lambda _argv, _environment: process,
        allocate_port=lambda: 43210,
        wait_ready=lambda _process, _port, _output: None,
        client_factory=lambda _context: http,
    ) as client:
        with pytest.raises(ProtectedCapacityManagerClientError, match="credential failure"):
            client.get_configuration()

    assert http.closed and process.terminated


def test_client_redacts_bounded_tunnel_diagnostics(tmp_path: Path) -> None:
    secret = "Bearer a-private-value-that-must-never-escape"
    runner = _Runner(_kubeconfig(tmp_path), [_pod("manager-exact")])
    process = _Process(stderr=f"Authorization: {secret}\n")

    def fail_ready(_process, _port, _output):
        raise RuntimeError("port-forward exited early")

    with pytest.raises(ProtectedCapacityManagerClientError) as failure:
        with open_protected_capacity_manager_client(
            runner=runner,
            credentials_root=_credentials(tmp_path),
            service_uid=os.geteuid(),
            service_gid=os.getegid(),
            spawn=lambda _argv, _environment: process,
            allocate_port=lambda: 43210,
            wait_ready=fail_ready,
        ):
            pass
    assert secret not in str(failure.value)
    assert secret not in (failure.value.diagnostic or "")


def test_client_suppresses_opaque_kubeconfig_value_echoed_by_child(tmp_path: Path) -> None:
    opaque_value = "saffron-lantern-4831"
    kubeconfig = _kubeconfig(tmp_path)
    kubeconfig.write_text(
        "apiVersion: v1\n"
        "kind: Config\n"
        "users:\n"
        "- name: manager\n"
        "  user:\n"
        f"    client-key-data: {opaque_value}\n",
        encoding="utf-8",
    )
    kubeconfig.chmod(0o600)
    runner = _Runner(kubeconfig, [_pod("manager-exact")])
    process = _Process(stderr=f"kubectl failed with {opaque_value}\n")

    def fail_ready(_process, _port, _output):
        raise RuntimeError("port-forward exited early")

    with pytest.raises(ProtectedCapacityManagerClientError) as failure:
        with open_protected_capacity_manager_client(
            runner=runner,
            credentials_root=_credentials(tmp_path),
            service_uid=os.geteuid(),
            service_gid=os.getegid(),
            spawn=lambda _argv, _environment: process,
            allocate_port=lambda: 43210,
            wait_ready=fail_ready,
        ):
            pass

    assert failure.value.reason == "transport"
    assert failure.value.diagnostic is None
    assert opaque_value not in str(failure.value)


def test_client_rejects_a_decoy_listener_without_exact_child_confirmation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _Runner(_kubeconfig(tmp_path), [_pod("manager-exact")])
    process = _Process()
    monkeypatch.setattr(client_module, "_START_TIMEOUT_SECONDS", 0.02)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as decoy:
        decoy.bind(("127.0.0.1", 0))
        decoy.listen()
        port = decoy.getsockname()[1]
        with pytest.raises(ProtectedCapacityManagerClientError):
            with open_protected_capacity_manager_client(
                runner=runner,
                credentials_root=_credentials(tmp_path),
                service_uid=os.geteuid(),
                service_gid=os.getegid(),
                spawn=lambda _argv, _environment: process,
                allocate_port=lambda: port,
            ):
                pytest.fail("an unrelated listener satisfied tunnel readiness")

    assert process.terminated


@pytest.mark.parametrize("stream_name", ["stdout", "stderr"])
def test_client_accepts_exact_child_forwarding_confirmation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stream_name: str,
) -> None:
    runner = _Runner(_kubeconfig(tmp_path), [_pod("manager-exact")])
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as allocator:
        allocator.bind(("127.0.0.1", 0))
        port = allocator.getsockname()[1]
    output = f"Forwarding from 127.0.0.1:{port} -> 8443\n"
    process = _Process(**{stream_name: output})
    monkeypatch.setattr(client_module, "_START_TIMEOUT_SECONDS", 0.02)

    with open_protected_capacity_manager_client(
        runner=runner,
        credentials_root=_credentials(tmp_path),
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        spawn=lambda _argv, _environment: process,
        allocate_port=lambda: port,
        client_factory=lambda _context: _HTTPClient([]),
    ) as client:
        assert client.origin == f"https://127.0.0.1:{port}"

    assert process.terminated


def test_client_observes_a_flushed_confirmation_while_child_remains_alive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _Runner(_kubeconfig(tmp_path), [_pod("manager-exact")])
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as allocator:
        allocator.bind(("127.0.0.1", 0))
        port = allocator.getsockname()[1]
    script = (
        "import sys,time;"
        f"sys.stderr.write('Forwarding from 127.0.0.1:{port} -> 8443\\n');"
        "sys.stderr.flush();time.sleep(10)"
    )
    processes: list[subprocess.Popen[str]] = []

    def spawn(_argv, environment):
        process = subprocess.Popen(
            [sys.executable, "-c", script],
            env=dict(environment),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            close_fds=True,
        )
        processes.append(process)
        return process

    monkeypatch.setattr(client_module, "_START_TIMEOUT_SECONDS", 0.2)
    with open_protected_capacity_manager_client(
        runner=runner,
        credentials_root=_credentials(tmp_path),
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        spawn=spawn,
        allocate_port=lambda: port,
        client_factory=lambda _context: _HTTPClient([]),
    ) as client:
        assert client.origin == f"https://127.0.0.1:{port}"

    assert len(processes) == 1
    assert processes[0].poll() is not None


def test_client_teardown_is_bounded_when_a_descendant_retains_pipe_writers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _Runner(_kubeconfig(tmp_path), [_pod("manager-exact")])
    port = 43210
    descendant_pid_path = tmp_path / "retained-writer.pid"
    script = (
        "import pathlib,subprocess,sys,time;"
        "descendant=subprocess.Popen([sys.executable,'-c','import time;time.sleep(2)'],"
        "stdin=subprocess.DEVNULL,stdout=sys.stdout,stderr=sys.stderr);"
        f"pathlib.Path({str(descendant_pid_path)!r}).write_text(str(descendant.pid));"
        f"sys.stderr.write('Forwarding from 127.0.0.1:{port} -> 8443\\n');"
        "sys.stderr.flush();time.sleep(10)"
    )
    processes: list[subprocess.Popen[str]] = []

    def spawn(_argv, environment):
        process = subprocess.Popen(
            [sys.executable, "-c", script],
            env=dict(environment),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            close_fds=True,
        )
        processes.append(process)
        return process

    monkeypatch.setattr(client_module, "_START_TIMEOUT_SECONDS", 0.5)
    teardown_started: float | None = None
    try:
        with open_protected_capacity_manager_client(
            runner=runner,
            credentials_root=_credentials(tmp_path),
            service_uid=os.geteuid(),
            service_gid=os.getegid(),
            spawn=spawn,
            allocate_port=lambda: port,
            client_factory=lambda _context: _HTTPClient([]),
        ):
            teardown_started = time.monotonic()
    finally:
        elapsed = None if teardown_started is None else time.monotonic() - teardown_started
        if descendant_pid_path.exists():
            descendant_pid = int(descendant_pid_path.read_text())
            try:
                os.kill(descendant_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=1.0)

    assert elapsed is not None and elapsed < 0.75
    assert len(processes) == 1 and processes[0].poll() is not None


def test_child_capture_cannot_close_a_reused_pipe_descriptor() -> None:
    read_descriptor, write_descriptor = os.pipe()
    reader = os.fdopen(read_descriptor, "rb", buffering=0)
    process = _Process()
    process.stdout = reader  # type: ignore[assignment]
    process.stderr = None  # type: ignore[assignment]
    capture = client_module._BoundedChildOutputCapture(process, 43210)
    replacement_read = -1
    replacement_write = -1
    try:
        capture.start()
        capture.finish()
        wrapper_closed = reader.closed
        os.close(write_descriptor)
        write_descriptor = -1
        replacement_read, replacement_write = os.pipe()
        assert replacement_read == read_descriptor
        del capture
        del process
        del reader
        gc.collect()

        os.fstat(replacement_read)
        assert wrapper_closed
    finally:
        for descriptor in (write_descriptor, replacement_read, replacement_write):
            if descriptor >= 0:
                try:
                    os.close(descriptor)
                except OSError:
                    pass


def test_client_kills_a_port_forward_that_does_not_terminate(tmp_path: Path) -> None:
    runner = _Runner(_kubeconfig(tmp_path), [_pod("manager-exact")])

    class _StuckProcess(_Process):
        def terminate(self) -> None:
            self.terminated = True

        def wait(self, timeout: float | None = None) -> int:
            self.wait_timeouts.append(timeout)
            if not self.killed:
                raise subprocess.TimeoutExpired("kubectl", timeout)
            self.returncode = -9
            return self.returncode

    process = _StuckProcess()
    with open_protected_capacity_manager_client(
        runner=runner,
        credentials_root=_credentials(tmp_path),
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        spawn=lambda _argv, _environment: process,
        allocate_port=lambda: 43210,
        wait_ready=lambda _process, _port, _output: None,
        client_factory=lambda _context: _HTTPClient([]),
    ):
        pass
    assert process.terminated and process.killed
    assert process.wait_timeouts == [5.0, 5.0]
