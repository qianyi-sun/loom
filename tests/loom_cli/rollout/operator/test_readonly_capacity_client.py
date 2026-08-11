from __future__ import annotations

import hashlib
import json
import os
import socket
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path

import pytest

from loom.data_lifecycle import StagingCapacity
from loom.data_lifecycle_capacity import DriveHeadroom
from loom_cli.rollout.operator import readonly_capacity_client as capacity_client_module
from loom_cli.rollout.operator.readonly_capacity_client import (
    InstalledReadonlyCapacitySource,
    open_readonly_minio_client,
    probe_installed_minio_admin_drives,
    probe_installed_minio_replica_count,
    probe_installed_readonly_object_store_health,
    probe_installed_staging_capacity,
    verify_installed_immutable_objects,
)
from loom_cli.rollout.operator.rollout_checkpoint import ImmutableObjectReference
from loom_cli.rollout.readonly_minio_bootstrap import (
    READONLY_MINIO_ACCESS_KEY,
    READONLY_MINIO_BUCKETS,
    ReadonlyMinioCredential,
)


class Process:
    def __init__(self) -> None:
        self.returncode: int | None = None
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


class Paginator:
    def __init__(self, pages: Sequence[Mapping[str, object]]) -> None:
        self.pages = pages

    def paginate(self, **_kwargs: object) -> Sequence[Mapping[str, object]]:
        return self.pages


class S3:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def get_bucket_versioning(self, **kwargs: str) -> dict[str, object]:
        assert kwargs == {"Bucket": kwargs["Bucket"]}
        assert kwargs["Bucket"] in READONLY_MINIO_BUCKETS
        return {}

    def get_paginator(self, operation: str) -> Paginator:
        assert operation == "list_objects_v2"
        return Paginator(
            (
                {
                    "Contents": (
                        {"Key": "run/a", "Size": 10},
                        {"Key": "run/b", "Size": 20},
                    )
                },
            )
        )


def _private(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(0o600)


def _paths(tmp_path: Path) -> tuple[Path, Path, ReadonlyMinioCredential]:
    kubeconfig = tmp_path / "readonly-kubeconfig"
    credential_path = tmp_path / "readonly-minio.json"
    credential = ReadonlyMinioCredential(
        access_key=READONLY_MINIO_ACCESS_KEY,
        secret_key="a" * 48,
    )
    _private(kubeconfig, b"exact-kubeconfig")
    _private(credential_path, credential.to_bytes())
    return kubeconfig, credential_path, credential


def test_client_binds_exact_transport_and_keeps_secret_out_of_process(tmp_path: Path) -> None:
    kubeconfig, credential_path, credential = _paths(tmp_path)
    process = Process()
    client = S3()
    spawns: list[tuple[tuple[str, ...], Mapping[str, str]]] = []
    factories: list[tuple[str, ReadonlyMinioCredential]] = []

    with open_readonly_minio_client(
        service_uid=os.getuid(),
        kubeconfig_path=kubeconfig,
        credential_path=credential_path,
        spawn=lambda argv, env: spawns.append((tuple(argv), dict(env))) or process,
        client_factory=lambda endpoint, exact: factories.append((endpoint, exact)) or client,
        allocate_port=lambda: 19001,
        wait_ready=lambda exact_process, port: (
            (exact_process is process and port == 19001)
            or (_ for _ in ()).throw(AssertionError("wrong tunnel"))
        ),
    ) as opened:
        assert opened is client

    assert spawns[0][0] == (
        "kubectl",
        "--kubeconfig",
        str(kubeconfig),
        "--namespace",
        "loom-staging",
        "port-forward",
        "pod/loom-minio-0",
        "--address=127.0.0.1",
        "19001:9000",
        "--pod-running-timeout=15s",
    )
    assert credential.secret_key not in json.dumps(spawns)
    assert factories == [("http://127.0.0.1:19001", credential)]
    assert client.closed and process.terminated and not process.killed


def test_wait_ready_rejects_unrelated_listener_without_child_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Process()
    read_fd, write_fd = os.pipe()
    process.stdout = os.fdopen(read_fd, "rb", buffering=0)  # type: ignore[attr-defined]
    monkeypatch.setattr(capacity_client_module, "_START_TIMEOUT_SECONDS", 0.02)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as decoy:
        decoy.bind(("127.0.0.1", 0))
        decoy.listen()
        port = decoy.getsockname()[1]
        try:
            with pytest.raises(RuntimeError, match="timed out"):
                capacity_client_module._wait_ready(process, port)
        finally:
            os.close(write_fd)
            process.stdout.close()  # type: ignore[attr-defined]


def test_wait_ready_accepts_exact_kubectl_child_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Process()
    read_fd, write_fd = os.pipe()
    process.stdout = os.fdopen(read_fd, "rb", buffering=0)  # type: ignore[attr-defined]
    monkeypatch.setattr(capacity_client_module, "_START_TIMEOUT_SECONDS", 0.1)
    port = 19002
    os.write(write_fd, f"Forwarding from 127.0.0.1:{port} -> 9000\n".encode())
    os.close(write_fd)
    try:
        capacity_client_module._wait_ready(process, port)
    finally:
        process.stdout.close()  # type: ignore[attr-defined]


def test_replica_count_probe_binds_ready_live_statefulset_to_configured_ceiling(
    tmp_path: Path,
) -> None:
    kubeconfig = tmp_path / "readonly-kubeconfig"
    _private(kubeconfig, b"exact-kubeconfig")
    calls: list[tuple[tuple[str, ...], Mapping[str, str]]] = []
    payload = {
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": "loom-minio",
            "namespace": "loom-staging",
            "generation": 7,
        },
        "spec": {"replicas": 4},
        "status": {"observedGeneration": 7, "readyReplicas": 4},
    }

    class Result:
        returncode = 0
        stdout = json.dumps(payload).encode()

    observed = probe_installed_minio_replica_count(
        service_uid=os.getuid(),
        configured_drive_count=4,
        kubeconfig_path=kubeconfig,
        run=lambda argv, environment: calls.append(
            (tuple(argv), dict(environment))
        )
        or Result(),
    )

    assert observed == 4
    assert calls[0][0] == (
        "kubectl",
        "--kubeconfig",
        str(kubeconfig),
        "--namespace",
        "loom-staging",
        "--request-timeout=10s",
        "get",
        "statefulset",
        "loom-minio",
        "--output=json",
    )


@pytest.mark.parametrize(
    ("spec_replicas", "ready_replicas", "observed_generation"),
    [
        (0, 0, 7),
        (5, 5, 7),
        (1, 1, 7),
        (1, 0, 7),
        (1, 1, 6),
        (True, True, 7),
    ],
)
def test_replica_count_probe_rejects_unready_or_drifted_statefulset(
    tmp_path: Path,
    spec_replicas: object,
    ready_replicas: object,
    observed_generation: object,
) -> None:
    kubeconfig = tmp_path / "readonly-kubeconfig"
    _private(kubeconfig, b"exact-kubeconfig")
    payload = {
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": "loom-minio",
            "namespace": "loom-staging",
            "generation": 7,
        },
        "spec": {"replicas": spec_replicas},
        "status": {
            "observedGeneration": observed_generation,
            "readyReplicas": ready_replicas,
        },
    }

    class Result:
        returncode = 0
        stdout = json.dumps(payload).encode()

    with pytest.raises(RuntimeError, match="replica authority"):
        probe_installed_minio_replica_count(
            service_uid=os.getuid(),
            configured_drive_count=4,
            kubeconfig_path=kubeconfig,
            run=lambda _argv, _environment: Result(),
        )


def test_admin_probe_requires_exact_drive_count_of_live_statefulset() -> None:
    credential = ReadonlyMinioCredential(
        access_key=READONLY_MINIO_ACCESS_KEY,
        secret_key="a" * 48,
    )
    calls: list[tuple[str, int]] = []

    @contextmanager
    def tunnel(*, service_uid: int):
        assert service_uid == os.getuid()
        yield "http://127.0.0.1:19003", credential

    def replica_count(*, service_uid: int, configured_drive_count: int) -> int:
        assert service_uid == os.getuid()
        assert configured_drive_count == 4
        return 4

    def drives(**kwargs: object) -> tuple[DriveHeadroom, ...]:
        calls.append((str(kwargs["endpoint_url"]), int(kwargs["expected_drive_count"])))
        return (DriveHeadroom(1000, 990, 1000, 980),) * 4

    assert len(
        probe_installed_minio_admin_drives(
            service_uid=os.getuid(),
            expected_drive_count=4,
            tunnel_context=tunnel,
            replica_count_probe=replica_count,
            drive_probe=drives,
        )
    ) == 4
    assert calls == [("http://127.0.0.1:19003", 4)]


def test_probe_counts_exact_execution_buckets_and_host_capacity(tmp_path: Path) -> None:
    client = S3()

    @contextmanager
    def context(*, service_uid: int) -> Iterator[S3]:
        assert service_uid == os.getuid()
        yield client

    capacity = probe_installed_staging_capacity(
        service_uid=os.getuid(),
        filesystem_paths=(tmp_path,),
        client_context=context,
    )

    assert capacity.object_count == 4
    assert capacity.bytes_used == 60
    assert 0 <= capacity.disk_free_percent <= 100
    assert 0 <= capacity.inode_free_percent <= 100


def test_probe_uses_minio_admin_drive_headroom_for_multinode() -> None:
    client = S3()

    @contextmanager
    def context(*, service_uid: int) -> Iterator[S3]:
        assert service_uid == os.getuid()
        yield client

    def admin_drives(
        *,
        service_uid: int,
        expected_drive_count: int,
    ) -> tuple[DriveHeadroom, ...]:
        assert service_uid == os.getuid()
        assert expected_drive_count == 2
        return (
            DriveHeadroom(
                total_bytes=1000,
                free_bytes=990,
                total_inodes=1000,
                free_inodes=980,
            ),
            DriveHeadroom(
                total_bytes=1000,
                free_bytes=970,
                total_inodes=1000,
                free_inodes=960,
            ),
        )

    capacity = probe_installed_staging_capacity(
        service_uid=os.getuid(),
        capacity_source="minio-admin",
        filesystem_paths=(),
        expected_drive_count=2,
        client_context=context,
        admin_drive_probe=admin_drives,
    )

    assert capacity.object_count == 4
    assert capacity.bytes_used == 60
    assert capacity.disk_free_percent == 97
    assert capacity.inode_free_percent == 96


def test_object_store_health_uses_only_fixed_list_authority() -> None:
    class HealthS3(S3):
        def get_bucket_versioning(self, **kwargs: str) -> dict[str, object]:
            assert kwargs["Bucket"] in READONLY_MINIO_BUCKETS
            return {"Status": "Enabled"}

    client = HealthS3()

    @contextmanager
    def context(*, service_uid: int) -> Iterator[S3]:
        assert service_uid == os.getuid()
        yield client

    evidence = probe_installed_readonly_object_store_health(
        service_uid=os.getuid(),
        client_context=context,
    )

    assert evidence.ready
    assert len(evidence.evidence_sha256) == 64


def _immutable_object(body: bytes, *, version_id: str = "v1") -> ImmutableObjectReference:
    return ImmutableObjectReference(
        authoritative_source="catalog:sha256:" + "d" * 64,
        bucket="loom-staging-artifacts",
        content_sha256=hashlib.sha256(body).hexdigest(),
        data_class="benchmark",
        object_key="benchmarks/example",
        size_bytes=len(body),
        version_id=version_id,
    )


def test_checkpoint_verifier_hashes_exact_version_and_normalizes_legacy_null() -> None:
    body = b"recoverable pinned object"
    legacy = _immutable_object(
        body,
        version_id="content-sha256:" + hashlib.sha256(body).hexdigest(),
    )
    calls: list[tuple[str, dict[str, str]]] = []

    class RecoveryS3(S3):
        def get_bucket_versioning(self, **kwargs: str) -> dict[str, object]:
            return {"Status": "Enabled"}

        def head_object(self, **kwargs: str) -> dict[str, object]:
            calls.append(("head", kwargs))
            return {"ContentLength": len(body), "VersionId": "null"}

        def get_object(self, **kwargs: str) -> dict[str, object]:
            calls.append(("get", kwargs))
            return {"Body": BytesIO(body)}

    @contextmanager
    def context(*, service_uid: int) -> Iterator[RecoveryS3]:
        assert service_uid == os.getuid()
        yield RecoveryS3()

    verified = verify_installed_immutable_objects(
        (legacy,),
        service_uid=os.getuid(),
        client_context=context,
        read_chunk_bytes=3,
    )

    assert verified[0].version_id == "null"
    assert calls == [
        (
            "head",
            {
                "Bucket": legacy.bucket,
                "Key": legacy.object_key,
                "VersionId": "null",
            },
        ),
        (
            "get",
            {
                "Bucket": legacy.bucket,
                "Key": legacy.object_key,
                "VersionId": "null",
            },
        ),
    ]


def test_checkpoint_verifier_rejects_unversioned_or_drifted_object() -> None:
    body = b"recoverable pinned object"
    item = _immutable_object(body)

    class RecoveryS3(S3):
        status = "Enabled"

        def get_bucket_versioning(self, **kwargs: str) -> dict[str, object]:
            return {"Status": self.status}

        def head_object(self, **kwargs: str) -> dict[str, object]:
            return {"ContentLength": len(body), "VersionId": "v1"}

        def get_object(self, **kwargs: str) -> dict[str, object]:
            return {"Body": BytesIO(b"drifted")}

    client = RecoveryS3()

    @contextmanager
    def context(*, service_uid: int) -> Iterator[RecoveryS3]:
        yield client

    client.status = "Suspended"
    with pytest.raises(ValueError, match="versioning must be enabled"):
        verify_installed_immutable_objects((item,), service_uid=os.getuid(), client_context=context)

    client.status = "Enabled"
    with pytest.raises(ValueError, match="digest drifted"):
        verify_installed_immutable_objects((item,), service_uid=os.getuid(), client_context=context)


def test_source_is_single_flight_under_concurrent_dag(tmp_path: Path) -> None:
    capacity = StagingCapacity(10, 20, 80, 90)
    calls: list[tuple[int, str, tuple[Path, ...], tuple[str, ...], int | None]] = []
    entered = threading.Barrier(4)

    def probe(
        *,
        service_uid: int,
        capacity_source: str,
        filesystem_paths: Sequence[Path],
        buckets: Sequence[str],
        expected_drive_count: int | None,
    ) -> StagingCapacity:
        calls.append(
            (
                service_uid,
                capacity_source,
                tuple(filesystem_paths),
                tuple(buckets),
                expected_drive_count,
            )
        )
        return capacity

    source = InstalledReadonlyCapacitySource(
        service_uid=os.getuid(),
        filesystem_paths=(tmp_path,),
        probe=probe,
    )
    results: list[StagingCapacity] = []

    def invoke() -> None:
        entered.wait()
        results.append(source())

    threads = [threading.Thread(target=invoke) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert results == [capacity] * 4
    assert calls == [
        (os.getuid(), "filesystem", (tmp_path,), READONLY_MINIO_BUCKETS, None)
    ]


def test_multinode_source_is_single_flight_without_retired_host_path() -> None:
    capacity = StagingCapacity(10, 20, 99, 98)
    calls: list[tuple[int, str, tuple[Path, ...], tuple[str, ...], int | None]] = []

    def probe(
        *,
        service_uid: int,
        capacity_source: str,
        filesystem_paths: Sequence[Path],
        buckets: Sequence[str],
        expected_drive_count: int | None,
    ) -> StagingCapacity:
        calls.append(
            (
                service_uid,
                capacity_source,
                tuple(filesystem_paths),
                tuple(buckets),
                expected_drive_count,
            )
        )
        return capacity

    source = InstalledReadonlyCapacitySource(
        service_uid=os.getuid(),
        capacity_source="minio-admin",
        filesystem_paths=(),
        expected_drive_count=4,
        probe=probe,
    )

    assert source() == capacity
    assert source() == capacity
    assert calls == [
        (os.getuid(), "minio-admin", (), READONLY_MINIO_BUCKETS, 4),
    ]


def test_multinode_source_requires_positive_expected_drive_count() -> None:
    with pytest.raises(ValueError, match="drive count"):
        InstalledReadonlyCapacitySource(
            service_uid=os.getuid(),
            capacity_source="minio-admin",
            filesystem_paths=(),
        )


def test_source_rejects_noncanonical_buckets(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="buckets drifted"):
        InstalledReadonlyCapacitySource(
            service_uid=os.getuid(),
            filesystem_paths=(tmp_path,),
            buckets=("loom-staging-artifacts",),
        )
