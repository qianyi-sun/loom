from __future__ import annotations

import hashlib
import json
import os
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path

import pytest

from loom.data_lifecycle import StagingCapacity
from loom_cli.rollout.operator.readonly_capacity_client import (
    InstalledReadonlyCapacitySource,
    open_readonly_minio_client,
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
    calls: list[tuple[int, tuple[Path, ...], tuple[str, ...]]] = []
    entered = threading.Barrier(4)

    def probe(
        *, service_uid: int, filesystem_paths: Sequence[Path], buckets: Sequence[str]
    ) -> StagingCapacity:
        calls.append((service_uid, tuple(filesystem_paths), tuple(buckets)))
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
    assert calls == [(os.getuid(), (tmp_path,), READONLY_MINIO_BUCKETS)]


def test_source_rejects_noncanonical_buckets(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="buckets drifted"):
        InstalledReadonlyCapacitySource(
            service_uid=os.getuid(),
            filesystem_paths=(tmp_path,),
            buckets=("loom-staging-artifacts",),
        )
