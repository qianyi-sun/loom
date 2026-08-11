"""Bounded read-only MinIO and filesystem sources for rollout preflight."""

from __future__ import annotations

import hashlib
import json
import socket
import subprocess
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any, Literal, Protocol, cast

import boto3
from botocore.config import Config

from loom.data_lifecycle import StagingCapacity
from loom.data_lifecycle_capacity import (
    DriveHeadroom,
    collect_staging_capacity,
    collect_staging_capacity_from_drives,
)
from loom.data_lifecycle_capacity_minio import probe_minio_admin_drives
from loom.data_lifecycle_inventory_s3 import S3ObservedObjectInventory
from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.preflight_credential_paths import (
    READONLY_KUBECONFIG_PATH,
    READONLY_MINIO_CREDENTIAL_PATH,
)
from loom_cli.rollout.readonly_minio_bootstrap import (
    READONLY_MINIO_BUCKETS,
    ReadonlyMinioCredential,
)
from loom_cli.rollout.staging_baseline_source import ObjectStoreBaselineEvidence

from .rollout_checkpoint import ImmutableObjectReference

_NAMESPACE = "loom-staging"
_POD = "loom-minio-0"
_REMOTE_PORT = 9000
_START_TIMEOUT_SECONDS = 15.0
_STOP_TIMEOUT_SECONDS = 5.0
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


class S3Client(Protocol):
    def get_bucket_versioning(self, **kwargs: str) -> Mapping[str, object]: ...

    def head_object(self, **kwargs: str) -> Mapping[str, object]: ...

    def get_object(self, **kwargs: str) -> Mapping[str, object]: ...

    def close(self) -> None: ...


Spawn = Callable[[Sequence[str], Mapping[str, str]], TunnelProcess]
ClientFactory = Callable[[str, ReadonlyMinioCredential], S3Client]
PortAllocator = Callable[[], int]
WaitReady = Callable[[TunnelProcess, int], None]
Now = Callable[[], datetime]
CapacitySource = Literal["filesystem", "minio-admin"]
AdminDriveProbe = Callable[..., Sequence[DriveHeadroom]]


def _spawn(argv: Sequence[str], environment: Mapping[str, str]) -> TunnelProcess:
    return subprocess.Popen(
        list(argv),
        env=dict(environment),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
        start_new_session=False,
    )


def _allocate_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    if not isinstance(port, int) or port < 1024 or port > 65535:
        raise RuntimeError("readonly MinIO local port is invalid")
    return port


def _wait_ready(process: TunnelProcess, port: int) -> None:
    deadline = time.monotonic() + _START_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("readonly MinIO port-forward exited early")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("readonly MinIO port-forward timed out")


def _client(endpoint: str, credential: ReadonlyMinioCredential) -> S3Client:
    return cast(
        S3Client,
        boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=credential.access_key,
            aws_secret_access_key=credential.secret_key,
            region_name=credential.region,
            config=Config(
                signature_version="s3v4",
                connect_timeout=3,
                read_timeout=5,
                retries={"max_attempts": 1, "mode": "standard"},
                s3={"addressing_style": "path"},
            ),
        ),
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
                raise RuntimeError("readonly MinIO port-forward did not stop") from exc


@contextmanager
def _open_readonly_minio_tunnel(
    *,
    service_uid: int,
    kubeconfig_path: Path = READONLY_KUBECONFIG_PATH,
    credential_path: Path = READONLY_MINIO_CREDENTIAL_PATH,
    spawn: Spawn = _spawn,
    allocate_port: PortAllocator = _allocate_port,
    wait_ready: WaitReady = _wait_ready,
) -> Iterator[tuple[str, ReadonlyMinioCredential]]:
    """Yield one exact localhost transport and its bounded read credential."""
    if (
        service_uid < 1
        or not kubeconfig_path.is_absolute()
        or not credential_path.is_absolute()
        or ".." in kubeconfig_path.parts
        or ".." in credential_path.parts
    ):
        raise ValueError("readonly MinIO client authority is invalid")
    read_trusted_file(
        kubeconfig_path,
        service_uid=service_uid,
        private=True,
        max_bytes=1 << 20,
        require_nonempty=True,
    )
    credential = ReadonlyMinioCredential.from_bytes(
        read_trusted_file(
            credential_path,
            service_uid=service_uid,
            private=True,
            max_bytes=1024,
            require_nonempty=True,
        ).payload
    )
    port = allocate_port()
    process = spawn(
        (
            "kubectl",
            "--kubeconfig",
            str(kubeconfig_path),
            "--namespace",
            _NAMESPACE,
            "port-forward",
            f"pod/{_POD}",
            "--address=127.0.0.1",
            f"{port}:{_REMOTE_PORT}",
            "--pod-running-timeout=15s",
        ),
        _CHILD_ENVIRONMENT,
    )
    try:
        wait_ready(process, port)
        yield f"http://127.0.0.1:{port}", credential
    finally:
        _stop_exact(process)


@contextmanager
def open_readonly_minio_client(
    *,
    service_uid: int,
    kubeconfig_path: Path = READONLY_KUBECONFIG_PATH,
    credential_path: Path = READONLY_MINIO_CREDENTIAL_PATH,
    spawn: Spawn = _spawn,
    client_factory: ClientFactory = _client,
    allocate_port: PortAllocator = _allocate_port,
    wait_ready: WaitReady = _wait_ready,
) -> Iterator[S3Client]:
    """Yield one non-mutating client over one exact localhost transport."""
    with _open_readonly_minio_tunnel(
        service_uid=service_uid,
        kubeconfig_path=kubeconfig_path,
        credential_path=credential_path,
        spawn=spawn,
        allocate_port=allocate_port,
        wait_ready=wait_ready,
    ) as (endpoint, credential):
        client = client_factory(endpoint, credential)
        try:
            yield client
        finally:
            client.close()


def probe_installed_minio_admin_drives(
    *,
    service_uid: int,
    tunnel_context: Callable[..., Any] = _open_readonly_minio_tunnel,
    drive_probe: Callable[..., Sequence[DriveHeadroom]] = probe_minio_admin_drives,
) -> tuple[DriveHeadroom, ...]:
    """Read distributed MinIO drive headroom through the bounded admin action."""
    with tunnel_context(service_uid=service_uid) as (endpoint, credential):
        return tuple(
            drive_probe(
                endpoint_url=endpoint,
                access_key=credential.access_key,
                secret_key=credential.secret_key,
                region=credential.region,
            )
        )


def probe_installed_staging_capacity(
    *,
    service_uid: int,
    filesystem_paths: Sequence[Path],
    capacity_source: CapacitySource = "filesystem",
    buckets: Sequence[str] = READONLY_MINIO_BUCKETS,
    client_context: Callable[..., Any] = open_readonly_minio_client,
    admin_drive_probe: AdminDriveProbe = probe_installed_minio_admin_drives,
    now: Now = lambda: datetime.now(UTC),
) -> StagingCapacity:
    """Return a fresh exact capacity snapshot without database mutation."""
    exact_buckets = tuple(buckets)
    exact_paths = tuple(filesystem_paths)
    if exact_buckets != READONLY_MINIO_BUCKETS:
        raise ValueError("readonly capacity bucket authority drifted")
    if capacity_source == "filesystem":
        if not exact_paths:
            raise ValueError("readonly filesystem capacity authority is unavailable")
    elif capacity_source == "minio-admin":
        if exact_paths:
            raise ValueError("readonly MinIO admin capacity cannot use filesystem paths")
    else:
        raise ValueError("readonly capacity source is invalid")
    with client_context(service_uid=service_uid) as client:
        objects = S3ObservedObjectInventory(client).load(buckets=exact_buckets)
        if capacity_source == "minio-admin":
            evidence = collect_staging_capacity_from_drives(
                namespace=_NAMESPACE,
                objects=objects,
                drives=admin_drive_probe(service_uid=service_uid),
                observed_at=now(),
            )
        else:
            evidence = collect_staging_capacity(
                namespace=_NAMESPACE,
                objects=objects,
                filesystem_paths=exact_paths,
                observed_at=now(),
            )
    return evidence.capacity


def probe_installed_readonly_object_store_health(
    *,
    service_uid: int,
    buckets: Sequence[str] = READONLY_MINIO_BUCKETS,
    client_context: Callable[..., Any] = open_readonly_minio_client,
) -> ObjectStoreBaselineEvidence:
    """Prove exact bucket reachability and durable versioning."""
    exact_buckets = tuple(buckets)
    if exact_buckets != READONLY_MINIO_BUCKETS:
        raise ValueError("readonly object-store bucket authority drifted")
    statuses: dict[str, str] = {}
    with client_context(service_uid=service_uid) as client:
        for bucket in exact_buckets:
            response = client.get_bucket_versioning(Bucket=bucket)
            if not isinstance(response, Mapping):
                raise ValueError("readonly object-store response is invalid")
            status = response.get("Status", "unversioned")
            if status != "Enabled":
                raise ValueError("readonly object-store versioning must be enabled")
            statuses[bucket] = str(status)
    digest = hashlib.sha256(
        json.dumps(
            {
                "buckets": statuses,
                "namespace": _NAMESPACE,
                "version": "v1",
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return ObjectStoreBaselineEvidence(True, digest)


def verify_installed_immutable_objects(
    objects: Sequence[ImmutableObjectReference],
    *,
    service_uid: int,
    client_context: Callable[..., Any] = open_readonly_minio_client,
    read_chunk_bytes: int = 1024 * 1024,
) -> tuple[ImmutableObjectReference, ...]:
    """Prove every DB-referenced pinned object version and exact bytes."""
    if service_uid < 1 or read_chunk_bytes <= 0:
        raise ValueError("readonly checkpoint verifier authority is invalid")
    exact_objects = tuple(objects)
    if any(item.bucket not in READONLY_MINIO_BUCKETS for item in exact_objects):
        raise ValueError("readonly checkpoint bucket authority drifted")
    verified: list[ImmutableObjectReference] = []
    with client_context(service_uid=service_uid) as client:
        for bucket in sorted({item.bucket for item in exact_objects}):
            response = client.get_bucket_versioning(Bucket=bucket)
            if not isinstance(response, Mapping) or response.get("Status") != "Enabled":
                raise ValueError("readonly checkpoint bucket versioning must be enabled")
        for item in exact_objects:
            legacy_version = f"content-sha256:{item.content_sha256}"
            version_id = "null" if item.version_id == legacy_version else item.version_id
            params = {
                "Bucket": item.bucket,
                "Key": item.object_key,
                "VersionId": version_id,
            }
            head = client.head_object(**params)
            if not isinstance(head, Mapping) or type(head.get("ContentLength")) is not int:
                raise ValueError("readonly checkpoint object metadata is invalid")
            if head["ContentLength"] != item.size_bytes:
                raise ValueError("readonly checkpoint object size drifted")
            observed_version = head.get("VersionId")
            if version_id == "null":
                if observed_version not in {None, "null"}:
                    raise ValueError("readonly checkpoint object version drifted")
            elif observed_version != version_id:
                raise ValueError("readonly checkpoint object version drifted")
            response = client.get_object(**params)
            if not isinstance(response, Mapping):
                raise ValueError("readonly checkpoint object response is invalid")
            body = response.get("Body")
            if body is None or not callable(getattr(body, "read", None)):
                raise ValueError("readonly checkpoint object body is invalid")
            digest = hashlib.sha256()
            observed_size = 0
            try:
                while chunk := body.read(read_chunk_bytes):
                    if not isinstance(chunk, bytes):
                        raise ValueError("readonly checkpoint object body is invalid")
                    digest.update(chunk)
                    observed_size += len(chunk)
            finally:
                close = getattr(body, "close", None)
                if callable(close):
                    close()
            if observed_size != item.size_bytes or digest.hexdigest() != item.content_sha256:
                raise ValueError("readonly checkpoint object digest drifted")
            verified.append(replace(item, version_id=version_id))
    return tuple(sorted(verified, key=lambda item: item.identity))


class InstalledReadonlyCapacitySource:
    """Single-flight fresh capacity snapshot shared by the concurrent DAG."""

    def __init__(
        self,
        *,
        service_uid: int,
        filesystem_paths: Sequence[Path],
        capacity_source: CapacitySource = "filesystem",
        buckets: Sequence[str] = READONLY_MINIO_BUCKETS,
        probe: Callable[..., StagingCapacity] = probe_installed_staging_capacity,
    ) -> None:
        exact_paths = tuple(filesystem_paths)
        if (
            service_uid < 1
            or any(not path.is_absolute() or ".." in path.parts for path in exact_paths)
            or (capacity_source == "filesystem" and not exact_paths)
            or (capacity_source == "minio-admin" and bool(exact_paths))
            or capacity_source not in {"filesystem", "minio-admin"}
        ):
            raise ValueError("readonly capacity source authority is invalid")
        if tuple(buckets) != READONLY_MINIO_BUCKETS:
            raise ValueError("readonly capacity source buckets drifted")
        self._service_uid = service_uid
        self._filesystem_paths = exact_paths
        self._capacity_source = capacity_source
        self._buckets = tuple(buckets)
        self._probe = probe
        self._lock = Lock()
        self._capacity: StagingCapacity | None = None

    def __call__(self) -> StagingCapacity:
        with self._lock:
            if self._capacity is None:
                self._capacity = self._probe(
                    service_uid=self._service_uid,
                    capacity_source=self._capacity_source,
                    filesystem_paths=self._filesystem_paths,
                    buckets=self._buckets,
                )
            return self._capacity


__all__ = [
    "ClientFactory",
    "InstalledReadonlyCapacitySource",
    "PortAllocator",
    "S3Client",
    "Spawn",
    "TunnelProcess",
    "WaitReady",
    "open_readonly_minio_client",
    "probe_installed_minio_admin_drives",
    "probe_installed_readonly_object_store_health",
    "probe_installed_staging_capacity",
    "verify_installed_immutable_objects",
]
