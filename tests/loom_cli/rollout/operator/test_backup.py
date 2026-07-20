from __future__ import annotations

import base64
import hashlib
import io
import json
import logging
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from collections.abc import Callable, Mapping, Sequence
from dataclasses import fields
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import BinaryIO

import pytest

from loom_cli.cluster_backup_guard import BackupTraversalLimits, validate_backup_manifest
from loom_cli.rollout.operator import backup as backup_module
from loom_cli.rollout.operator.backup import (
    BackupCreator,
    BackupError,
    Boto3MinioMirror,
    SubprocessBackupCommandRunner,
    VerifiedBackup,
)
from loom_cli.rollout.operator.config import OperatorConfig
from loom_cli.rollout.operator.model import (
    CallerIdentity,
    CandidateBinding,
    RolloutRequest,
)
from loom_cli.rollout.operator.rollout_checkpoint import build_immutable_inventory

FIXED_NOW = datetime(2026, 7, 13, 20, 0, tzinfo=UTC)
POSTGRES_BYTES = b"pg\x00dump\xffbytes"
MINIO_ACCESS_KEY = "minio-access-sensitive"
MINIO_SECRET_KEY = "minio-secret-sensitive"
TEST_MINIO_PORT = 39123


def make_config(tmp_path: Path) -> OperatorConfig:
    rollout_root = tmp_path / "data"
    rollout_root.mkdir(mode=0o700)
    runner_repo = tmp_path / "runner" / "repo"
    runner_repo.mkdir(parents=True)
    cluster_config_path = runner_repo / "deploy" / "environments" / "staging.cluster.toml"
    cluster_config_path.parent.mkdir(parents=True)
    cluster_config_path.write_text(
        "\n".join(
            [
                'namespace = "loom-staging"',
                'trajectories_bucket = "loom-staging-trajectories"',
                'artifacts_bucket = "loom-staging-artifacts"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return OperatorConfig(
        schema_version=1,
        service_user="loom-rollout",
        operator_group="loom-staging-operators",
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="refs/heads/dev",
        runner_repo=runner_repo,
        state_root=tmp_path / "state",
        runtime_root=tmp_path / "runtime",
        rollout_root=rollout_root,
        kubeconfig_path=tmp_path / "state" / "kubeconfig",
        cluster_config_path=cluster_config_path,
        admin_token_source=f"file:{tmp_path / 'state' / 'credentials' / 'admin-token'}",
        worker_token_source=f"file:{tmp_path / 'state' / 'credentials' / 'worker-token'}",
        service_token_source=f"file:{tmp_path / 'state' / 'credentials' / 'service-token'}",
        expect_admin_token_fingerprint="sha256:abc123def456 len=64",
        cluster_name="loom-staging",
        namespace="loom-staging",
        environment="staging",
        cp_url="http://127.0.0.1:18081",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="11111111-1111-4111-8111-111111111111",
        scope="current-gb10",
        gb10_prep_concurrency=8,
        config_path=tmp_path / "staging-rollout.toml",
        config_sha256="2" * 64,
    )


def make_request() -> RolloutRequest:
    candidate = CandidateBinding(
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="origin/dev",
        resolved_sha="abcdef1234567890abcdef1234567890abcdef12",
        image_tag="staging-abcdef1",
        fetched_at="2026-07-13T19:59:59Z",
    )
    return RolloutRequest(
        request_id="stg-20260713-abcdef12",
        rollout_id="staging-abcdef1",
        caller=CallerIdentity(username="hongjian", uid=2002),
        candidate=candidate,
        requested_at="2026-07-13T20:00:00Z",
        runner_config_sha256="2" * 64,
        preflight_attestation_sha256="3" * 64,
        preflight_registry_sha256="4" * 64,
        preflight_coverage_sha256="5" * 64,
    )


class RecordingPortForward:
    def __init__(self, *, selected_port: int = TEST_MINIO_PORT) -> None:
        self.events: list[str] = []
        self.selected_port = selected_port

    def wait_ready(self, host: str, timeout_seconds: float) -> int:
        selected_port = getattr(self, "selected_port", TEST_MINIO_PORT)
        self.events.append(f"ready:{host}:{timeout_seconds}:{selected_port}")
        return selected_port

    def terminate(self) -> None:
        self.events.append("terminate")

    def wait(self, timeout_seconds: float) -> bool:
        self.events.append(f"wait:{timeout_seconds}")
        return True

    def kill(self) -> None:
        self.events.append("kill")

    def close(self) -> None:
        pass


class RecordingRunner:
    def __init__(self, *, port_forward: RecordingPortForward | None = None) -> None:
        self.argvs: list[list[str]] = []
        self.environments: list[dict[str, str]] = []
        self.timeouts: list[float | None] = []
        self.port_forward = port_forward or RecordingPortForward()

    def _record(self, argv: Sequence[str], env: Mapping[str, str]) -> list[str]:
        rendered = list(argv)
        self.argvs.append(rendered)
        self.environments.append(dict(env))
        return rendered

    def stream_stdout(
        self,
        argv: Sequence[str],
        sink: BinaryIO,
        *,
        env: Mapping[str, str],
        timeout_seconds: float | None = None,
    ) -> None:
        self._record(argv, env)
        self.timeouts.append(timeout_seconds)
        sink.write(POSTGRES_BYTES)

    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float | None = None,
    ) -> bytes:
        rendered = self._record(argv, env)
        self.timeouts.append(timeout_seconds)
        if rendered[-2:] == ["-o", "json"]:
            return json.dumps(
                {
                    "data": {
                        "minio-access-key": base64.b64encode(
                            MINIO_ACCESS_KEY.encode("utf-8")
                        ).decode("ascii"),
                        "minio-secret-key": base64.b64encode(
                            MINIO_SECRET_KEY.encode("utf-8")
                        ).decode("ascii"),
                    }
                }
            ).encode("utf-8")
        secret_name = rendered[rendered.index("secret") + 1]
        return (
            "apiVersion: v1\n"
            "kind: Secret\n"
            "metadata:\n"
            f"  name: {secret_name}\n"
            "data:\n"
            "  value: c2VjcmV0\n"
        ).encode()

    def start(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
    ) -> RecordingPortForward:
        self._record(argv, env)
        return self.port_forward


class FailingMinioMirror:
    def mirror(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        buckets: tuple[str, ...],
        destination: Path,
        cancel_on_timeout: Callable[[], None],
        resources: backup_module._BackupResourceBudget,
    ) -> None:
        raise RuntimeError(f"mirror leaked {access_key} {secret_key}")


class SuccessfulMinioMirror:
    def mirror(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        buckets: tuple[str, ...],
        destination: Path,
        cancel_on_timeout: Callable[[], None],
        resources: backup_module._BackupResourceBudget,
    ) -> None:
        assert endpoint_url == f"http://127.0.0.1:{TEST_MINIO_PORT}"
        assert buckets == (
            "loom-staging-trajectories",
            "loom-staging-artifacts",
        )
        for bucket in buckets:
            bucket_dir = destination / bucket
            bucket_dir.mkdir(mode=0o700)
            object_path = bucket_dir / "object.bin"
            backup_module._write_private_bytes(
                object_path,
                f"object:{bucket}".encode(),
                resources=resources,
                component="minio",
            )


def test_partial_backup_never_publishes_latest_or_returns_manifest(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    backups_root = config.rollout_root / "backups"
    backups_root.mkdir(mode=0o700)
    old = backups_root / "20260712T120000Z-old-request"
    old.mkdir(mode=0o700)
    (backups_root / "latest").symlink_to(old.name)
    runner = RecordingRunner()
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=runner,
        minio=FailingMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="minio_snapshot_failed"):
        creator.create(make_request())

    assert (backups_root / "latest").readlink() == Path(old.name)
    assert list(backups_root.glob("*/backup-manifest.json")) == []
    failed_root = backups_root / "20260713T200000Z-stg-20260713-abcdef12"
    assert failed_root.is_dir()
    assert failed_root.stat().st_mode & 0o777 == 0o700
    assert runner.port_forward.events == [
        f"ready:127.0.0.1:15.0:{TEST_MINIO_PORT}",
        "terminate",
        "wait:5.0",
    ]


def test_binary_dump_and_exact_secret_allowlist_never_expose_credentials(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    runner = RecordingRunner()
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=runner,
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    backup = creator.create(make_request())

    port_forward_index = next(
        index for index, argv in enumerate(runner.argvs) if "port-forward" in argv
    )
    postgres_index = next(
        index for index, argv in enumerate(runner.argvs) if any("pg_dump" in arg for arg in argv)
    )
    assert port_forward_index < postgres_index

    bundle_root = config.rollout_root / "backups" / "20260713T200000Z-stg-20260713-abcdef12"
    assert (bundle_root / "postgres" / "loom.dump").read_bytes() == POSTGRES_BYTES
    secret_files = sorted((bundle_root / "secrets").glob("*.yaml"))
    assert [path.name for path in secret_files] == [
        "loom-admin-secret.yaml",
        "loom-secrets.yaml",
        "loom-staging-tls.yaml",
    ]
    for path in secret_files:
        assert path.stat().st_mode & 0o777 == 0o600
        assert f"name: {path.stem}" in path.read_text(encoding="utf-8")

    secret_export_argvs = [argv for argv in runner.argvs if argv[-2:] == ["-o", "yaml"]]
    assert [argv[argv.index("secret") + 1] for argv in secret_export_argvs] == [
        "loom-secrets",
        "loom-admin-secret",
        "loom-staging-tls",
    ]
    rendered_boundary = json.dumps(
        {
            "argvs": runner.argvs,
            "environments": runner.environments,
            "backup": str(backup),
        }
    )
    for value in (MINIO_ACCESS_KEY, MINIO_SECRET_KEY):
        assert value not in rendered_boundary
    assert runner.timeouts == [600.0, 30.0, 30.0, 30.0, 30.0]


def test_critical_checkpoint_records_inventory_without_minio_payload_copy(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    runner = RecordingRunner()

    def inventory(created_at: datetime):
        return build_immutable_inventory(
            environment="staging",
            namespace="loom-staging",
            mutation_epoch=6,
            schema_revision="0066",
            created_at=created_at,
            objects=[],
        )

    backup = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=runner,
        minio=FailingMinioMirror(),
        now=lambda: FIXED_NOW,
        object_inventory_provider=inventory,
    ).create(make_request())

    manifest = json.loads(backup.manifest_path.read_text(encoding="utf-8"))
    bundle = backup.manifest_path.parent
    assert manifest["schema_version"] == 2
    assert set(manifest["components"]) == {
        "postgres",
        "object_inventory",
        "k8s_secrets",
    }
    assert not (bundle / "minio").exists()
    inventory_document = json.loads((bundle / "object-inventory.json").read_text(encoding="utf-8"))
    assert inventory_document["mutation_epoch"] == 6
    assert len(inventory_document["inventory_root"]) == 64
    assert all("port-forward" not in argv for argv in runner.argvs)
    assert not any(argv[-2:] == ["-o", "json"] for argv in runner.argvs)
    assert runner.timeouts == [600.0, 30.0, 30.0, 30.0]


def test_critical_checkpoint_defers_latest_until_explicit_activation(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    runner = RecordingRunner()

    def inventory(created_at: datetime):
        return build_immutable_inventory(
            environment="staging",
            namespace="loom-staging",
            mutation_epoch=6,
            schema_revision="0066",
            created_at=created_at,
            objects=[],
        )

    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=runner,
        minio=FailingMinioMirror(),
        now=lambda: FIXED_NOW,
        object_inventory_provider=inventory,
        publish_latest=False,
    )
    backup = creator.create(make_request())
    latest = config.rollout_root / "backups" / "latest"

    assert not latest.exists()
    creator.activate(backup)
    assert latest.readlink() == Path(backup.manifest_path.parent.name)


def test_oversized_postgres_dump_stops_before_crossing_component_cap(
    tmp_path: Path,
) -> None:
    class OversizedPostgresRunner(RecordingRunner):
        def stream_stdout(
            self,
            argv: Sequence[str],
            sink: BinaryIO,
            *,
            env: Mapping[str, str],
            timeout_seconds: float | None = None,
        ) -> None:
            self._record(argv, env)
            self.timeouts.append(timeout_seconds)
            sink.write(b"12345")

    capacity = lambda _path: backup_module._CapacitySnapshot(  # noqa: E731
        # Linux reports 4 KiB directory blocks while APFS commonly reports a
        # much smaller value. Leave room for private directory accounting so
        # this test reaches the Postgres component cap on every platform.
        free_bytes=100_000,
        free_inodes=10_000,
        block_size=1,
    )
    config = make_config(tmp_path)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=OversizedPostgresRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
        max_postgres_bytes=4,
        max_total_bytes=100_000,
        disk_reserve_bytes=0,
        inode_reserve=0,
        capacity_provider=capacity,
    )

    with pytest.raises(BackupError, match="postgres_dump_failed"):
        creator.create(make_request())

    bundle_root = config.rollout_root / "backups" / "20260713T200000Z-stg-20260713-abcdef12"
    assert (bundle_root / "postgres" / "loom.dump").read_bytes() == b""
    assert not (bundle_root / "backup-manifest.json").exists()
    assert not (bundle_root.parent / "latest").exists()


def test_postgres_stream_rechecks_declining_host_free_space_between_chunks(
    tmp_path: Path,
) -> None:
    # Directory allocation consumes several 4 KiB blocks on Linux. Start with
    # enough room to reach the stream, then make the second write observe the
    # intended one-byte free-space boundary.
    state = {"free_bytes": 100_000}

    class DecliningPostgresRunner(RecordingRunner):
        def stream_stdout(
            self,
            argv: Sequence[str],
            sink: BinaryIO,
            *,
            env: Mapping[str, str],
            timeout_seconds: float | None = None,
        ) -> None:
            self._record(argv, env)
            self.timeouts.append(timeout_seconds)
            sink.write(b"a")
            state["free_bytes"] = 1
            sink.write(b"b")

    def capacity(_path: Path) -> object:
        return backup_module._CapacitySnapshot(
            free_bytes=state["free_bytes"],
            free_inodes=100,
            block_size=1,
        )

    config = make_config(tmp_path)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=DecliningPostgresRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
        max_postgres_bytes=100,
        max_total_bytes=1_000_000,
        disk_reserve_bytes=1,
        inode_reserve=1,
        capacity_provider=capacity,
    )

    with pytest.raises(BackupError, match="postgres_dump_failed"):
        creator.create(make_request())

    bundle_root = config.rollout_root / "backups" / "20260713T200000Z-stg-20260713-abcdef12"
    assert (bundle_root / "postgres" / "loom.dump").read_bytes() == b"a"
    assert not (bundle_root / "backup-manifest.json").exists()
    assert not (bundle_root.parent / "latest").exists()


def test_postgres_dump_succeeds_at_exact_component_byte_cap(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
        max_postgres_bytes=len(POSTGRES_BYTES),
        max_total_bytes=1_000_000,
        disk_reserve_bytes=0,
        inode_reserve=0,
        capacity_provider=lambda _path: backup_module._CapacitySnapshot(
            free_bytes=100_000,
            free_inodes=100_000,
            block_size=1,
        ),
    )

    backup = creator.create(make_request())

    assert backup.manifest_path.is_file()
    assert (backup.manifest_path.parent / "postgres" / "loom.dump").read_bytes() == POSTGRES_BYTES


def test_capacity_snapshot_uses_service_available_blocks_not_root_free_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = type(
        "Statvfs",
        (),
        {
            "f_bavail": 2,
            "f_bfree": 999,
            "f_frsize": 4096,
            "f_bsize": 4096,
            "f_favail": 7,
        },
    )()
    monkeypatch.setattr(backup_module.os, "statvfs", lambda _path: values)

    snapshot = backup_module._capacity_snapshot(tmp_path)

    assert snapshot.free_bytes == 2 * 4096
    assert snapshot.free_inodes == 7
    assert backup_module._available_bytes(tmp_path) == 2 * 4096


def test_success_returns_only_timestamped_verified_manifest_and_digest(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    runner = RecordingRunner()
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=runner,
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    backup = creator.create(make_request())

    assert isinstance(backup, VerifiedBackup)
    assert [field.name for field in fields(backup)] == [
        "manifest_path",
        "manifest_sha256",
    ]
    expected_root = config.rollout_root / "backups" / "20260713T200000Z-stg-20260713-abcdef12"
    assert backup.manifest_path == expected_root / "backup-manifest.json"
    assert (
        backup.manifest_path != config.rollout_root / "backups" / "latest" / "backup-manifest.json"
    )
    assert backup.manifest_sha256 == hashlib.sha256(backup.manifest_path.read_bytes()).hexdigest()
    assert (
        validate_backup_manifest(
            backup.manifest_path,
            environment="staging",
            namespace="loom-staging",
            expected_owner_uid=os.getuid(),
            require_private_files=True,
            min_remaining_hours=2,
            now=FIXED_NOW,
        )
        == []
    )
    latest = config.rollout_root / "backups" / "latest"
    assert latest.is_symlink()
    assert latest.readlink() == Path(expected_root.name)
    assert not latest.readlink().is_absolute()
    assert (latest / "backup-manifest.json").samefile(backup.manifest_path)
    manifest_text = backup.manifest_path.read_text(encoding="utf-8")
    assert MINIO_ACCESS_KEY not in manifest_text
    assert MINIO_SECRET_KEY not in manifest_text


class FakeStreamingBody:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._offset = 0
        self.closed = False

    def read(self, amount: int) -> bytes:
        chunk = self._payload[self._offset : self._offset + amount]
        self._offset += len(chunk)
        return chunk

    def close(self) -> None:
        self.closed = True


class PaginatedS3:
    def __init__(self) -> None:
        self.list_calls: list[dict[str, str]] = []
        self.bodies: list[FakeStreamingBody] = []
        self.closed = False

    def list_objects_v2(self, **kwargs: str) -> dict[str, object]:
        self.list_calls.append(dict(kwargs))
        bucket = kwargs["Bucket"]
        token = kwargs.get("ContinuationToken")
        if token is None:
            return {
                "Contents": [{"Key": "first/object.bin"}],
                "IsTruncated": True,
                "NextContinuationToken": f"next-{bucket}",
            }
        assert token == f"next-{bucket}"
        return {
            "Contents": [{"Key": "second.bin"}],
            "IsTruncated": False,
        }

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:  # noqa: N803
        payload = f"{Bucket}:{Key}".encode()
        body = FakeStreamingBody(payload)
        self.bodies.append(body)
        return {"Body": body, "ContentLength": len(payload)}

    def close(self) -> None:
        self.closed = True


class OneObjectS3:
    """Small client used by whole-bundle resource tests."""

    def __init__(
        self,
        body: FakeStreamingBody | None = None,
        *,
        declared_size: int = 1,
    ) -> None:
        self.body = body or FakeStreamingBody(b"x")
        self.declared_size = declared_size
        self.closed = False

    def list_objects_v2(self, **kwargs: str) -> dict[str, object]:
        if kwargs["Bucket"] == "loom-staging-trajectories":
            return {"Contents": [{"Key": "object.bin"}], "IsTruncated": False}
        return {"Contents": [], "IsTruncated": False}

    def get_object(self, **_kwargs: str) -> dict[str, object]:
        return {"Body": self.body, "ContentLength": self.declared_size}

    def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize(
    "endpoint_url",
    [
        "http://localhost:39123",
        "http://127.0.0.1:0",
        "http://127.0.0.1:65536",
        "https://127.0.0.1:39123",
        "http://127.0.0.1:39123/path",
    ],
)
def test_boto3_minio_mirror_rejects_non_child_local_endpoints(
    tmp_path: Path,
    endpoint_url: str,
) -> None:
    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    mirror = Boto3MinioMirror(
        client_factory=lambda *_args, **_kwargs: pytest.fail(
            "client must not be created for an unapproved endpoint"
        )
    )

    with pytest.raises(ValueError, match="endpoint is not approved"):
        mirror.mirror(
            endpoint_url=endpoint_url,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )


def test_boto3_minio_mirror_paginates_both_buckets_with_bounded_client_config(
    tmp_path: Path,
) -> None:
    client = PaginatedS3()
    client_calls: list[dict[str, object]] = []

    def client_factory(service_name: str, **kwargs: object) -> PaginatedS3:
        assert service_name == "s3"
        client_calls.append(dict(kwargs))
        return client

    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    mirror = Boto3MinioMirror(client_factory=client_factory)

    mirror.mirror(
        endpoint_url="http://127.0.0.1:19000",
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
        destination=destination,
    )

    assert len(client_calls) == 1
    client_config = client_calls[0]["config"]
    assert client_config.connect_timeout == 5
    assert client_config.read_timeout == 30
    assert client_config.retries["total_max_attempts"] == 3
    assert client_config.proxies == {}
    assert client.list_calls == [
        {"Bucket": "loom-staging-trajectories"},
        {
            "Bucket": "loom-staging-trajectories",
            "ContinuationToken": "next-loom-staging-trajectories",
        },
        {"Bucket": "loom-staging-artifacts"},
        {
            "Bucket": "loom-staging-artifacts",
            "ContinuationToken": "next-loom-staging-artifacts",
        },
    ]
    for bucket in ("loom-staging-trajectories", "loom-staging-artifacts"):
        first = destination / bucket / "first" / "object.bin"
        second = destination / bucket / "second.bin"
        assert first.read_bytes() == f"{bucket}:first/object.bin".encode()
        assert second.read_bytes() == f"{bucket}:second.bin".encode()
        assert first.stat().st_mode & 0o777 == 0o600
        assert second.stat().st_mode & 0o777 == 0o600
    assert all(body.closed for body in client.bodies)
    assert client.closed


def test_shared_budget_counts_block_rounded_tiny_minio_objects(tmp_path: Path) -> None:
    class TwoTinyObjectsS3(OneObjectS3):
        def list_objects_v2(self, **kwargs: str) -> dict[str, object]:
            if kwargs["Bucket"] == "loom-staging-trajectories":
                return {
                    "Contents": [{"Key": "first.bin"}, {"Key": "second.bin"}],
                    "IsTruncated": False,
                }
            return {"Contents": [], "IsTruncated": False}

        def get_object(self, **_kwargs: str) -> dict[str, object]:
            return {"Body": FakeStreamingBody(b"x"), "ContentLength": 1}

    client = TwoTinyObjectsS3()
    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    block_size = 4096
    resources = backup_module._BackupResourceBudget(
        tmp_path,
        max_postgres_bytes=100_000,
        max_total_bytes=(5 * block_size) - 1,
        disk_reserve_bytes=0,
        inode_reserve=0,
        capacity_provider=lambda _path: backup_module._CapacitySnapshot(
            free_bytes=1_000_000,
            free_inodes=1_000_000,
            block_size=block_size,
        ),
    )
    mirror = Boto3MinioMirror(
        client_factory=lambda *_args, **_kwargs: client,
        disk_reserve_bytes=0,
        inode_reserve=0,
    )

    with pytest.raises(ValueError, match="allocated-byte limit"):
        mirror.mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
            resources=resources,
        )

    assert (destination / "loom-staging-trajectories" / "first.bin").read_bytes() == b"x"
    assert not (destination / "loom-staging-trajectories" / "second.bin").exists()
    assert not list(destination.rglob("*.part"))
    assert client.closed


def test_standalone_mirror_counts_block_rounded_tiny_object_metadata(
    tmp_path: Path,
) -> None:
    client = OneObjectS3()
    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    values = os.statvfs(destination)
    block_size = values.f_frsize or values.f_bsize
    mirror = Boto3MinioMirror(
        client_factory=lambda *_args, **_kwargs: client,
        max_total_bytes=(3 * block_size) - 1,
        disk_reserve_bytes=0,
        inode_reserve=0,
        available_bytes=lambda _path: 1_000_000,
        available_inodes=lambda _path: 1_000_000,
    )

    with pytest.raises(ValueError, match="allocated-byte limit"):
        mirror.mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )

    assert client.body.closed
    assert client.closed
    assert not (destination / "loom-staging-trajectories" / "object.bin").exists()
    assert not list(destination.rglob("*.part"))


def test_standalone_mirror_rechecks_declining_free_space_between_body_chunks(
    tmp_path: Path,
) -> None:
    state = {"free_bytes": 1_000_000}

    class DecliningBody(FakeStreamingBody):
        def read(self, amount: int) -> bytes:
            if self._offset == 1:
                state["free_bytes"] = 0
            return super().read(1 if self._offset < 2 else amount)

    client = OneObjectS3(DecliningBody(b"ab"), declared_size=2)
    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    mirror = Boto3MinioMirror(
        client_factory=lambda *_args, **_kwargs: client,
        max_total_bytes=100_000,
        disk_reserve_bytes=1,
        inode_reserve=0,
        available_bytes=lambda _path: state["free_bytes"],
        available_inodes=lambda _path: 100_000,
    )

    with pytest.raises(ValueError, match="free-space reserve"):
        mirror.mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )

    assert client.body.closed
    assert client.closed
    assert not (destination / "loom-staging-trajectories" / "object.bin").exists()
    assert not list(destination.rglob("*.part"))


def test_shared_budget_reconciles_filesystem_blocks_above_logical_size(
    tmp_path: Path,
) -> None:
    class AllocatedPath:
        def stat(self, *, follow_symlinks: bool) -> object:
            assert not follow_symlinks
            return type("Metadata", (), {"st_size": 1, "st_blocks": 16})()

    resources = backup_module._BackupResourceBudget(
        tmp_path,
        max_postgres_bytes=10_000,
        max_total_bytes=8191,
        disk_reserve_bytes=0,
        inode_reserve=0,
        capacity_provider=lambda _path: backup_module._CapacitySnapshot(
            free_bytes=100_000,
            free_inodes=100_000,
            block_size=1,
        ),
    )
    account = resources.reserve_entry(
        AllocatedPath(),  # type: ignore[arg-type]
        component="minio",
    )
    resources.prepare_write(account, 1)
    resources.commit_write(account, 1)

    with pytest.raises(ValueError, match="allocated-byte limit"):
        resources.reconcile_writer(account)


def test_minio_stream_rechecks_declining_shared_free_space(tmp_path: Path) -> None:
    state = {"free_bytes": 100_000}

    class DecliningBody(FakeStreamingBody):
        def read(self, amount: int) -> bytes:
            if self._offset == 1:
                state["free_bytes"] = 1
            return super().read(1 if self._offset < 2 else amount)

    client = OneObjectS3(DecliningBody(b"ab"), declared_size=2)
    config = make_config(tmp_path)
    mirror = Boto3MinioMirror(
        client_factory=lambda *_args, **_kwargs: client,
        disk_reserve_bytes=0,
        inode_reserve=0,
    )
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=mirror,
        now=lambda: FIXED_NOW,
        max_postgres_bytes=100_000,
        max_total_bytes=1_000_000,
        disk_reserve_bytes=1,
        inode_reserve=1,
        capacity_provider=lambda _path: backup_module._CapacitySnapshot(
            free_bytes=state["free_bytes"],
            free_inodes=100_000,
            block_size=1,
        ),
    )

    with pytest.raises(BackupError, match="minio_snapshot_failed"):
        creator.create(make_request())

    bundle_root = config.rollout_root / "backups" / "20260713T200000Z-stg-20260713-abcdef12"
    assert client.body.closed
    assert client.closed
    assert not list(bundle_root.rglob("*.part"))
    assert not (bundle_root / "backup-manifest.json").exists()
    assert not (bundle_root.parent / "latest").exists()


def test_minio_rechecks_declining_shared_inode_capacity_between_objects(
    tmp_path: Path,
) -> None:
    state = {"free_inodes": 100_000}

    class FirstBody(FakeStreamingBody):
        def close(self) -> None:
            super().close()
            state["free_inodes"] = 1

    class TwoObjectsS3(OneObjectS3):
        def list_objects_v2(self, **kwargs: str) -> dict[str, object]:
            if kwargs["Bucket"] == "loom-staging-trajectories":
                return {
                    "Contents": [{"Key": "first.bin"}, {"Key": "second.bin"}],
                    "IsTruncated": False,
                }
            return {"Contents": [], "IsTruncated": False}

        def get_object(self, *, Key: str, **_kwargs: str) -> dict[str, object]:  # noqa: N803
            body: FakeStreamingBody
            body = FirstBody(b"x") if Key == "first.bin" else FakeStreamingBody(b"y")
            return {"Body": body, "ContentLength": 1}

    client = TwoObjectsS3()
    config = make_config(tmp_path)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=Boto3MinioMirror(
            client_factory=lambda *_args, **_kwargs: client,
            disk_reserve_bytes=0,
            inode_reserve=0,
        ),
        now=lambda: FIXED_NOW,
        max_postgres_bytes=100_000,
        max_total_bytes=1_000_000,
        disk_reserve_bytes=0,
        inode_reserve=1,
        capacity_provider=lambda _path: backup_module._CapacitySnapshot(
            free_bytes=100_000,
            free_inodes=state["free_inodes"],
            block_size=1,
        ),
    )

    with pytest.raises(BackupError, match="minio_snapshot_failed"):
        creator.create(make_request())

    bundle_root = config.rollout_root / "backups" / "20260713T200000Z-stg-20260713-abcdef12"
    assert (bundle_root / "minio" / "loom-staging-trajectories" / "first.bin").is_file()
    assert not (bundle_root / "minio" / "loom-staging-trajectories" / "second.bin").exists()
    assert not (bundle_root / "backup-manifest.json").exists()
    assert not (bundle_root.parent / "latest").exists()


def test_whole_backup_succeeds_at_exact_block_accounted_total_cap(tmp_path: Path) -> None:
    block_size = 4096
    client = OneObjectS3()
    config = make_config(tmp_path)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=Boto3MinioMirror(
            client_factory=lambda *_args, **_kwargs: client,
            disk_reserve_bytes=0,
            inode_reserve=0,
        ),
        now=lambda: FIXED_NOW,
        max_postgres_bytes=len(POSTGRES_BYTES),
        max_total_bytes=16 * block_size,
        disk_reserve_bytes=0,
        inode_reserve=0,
        capacity_provider=lambda _path: backup_module._CapacitySnapshot(
            free_bytes=1_000_000,
            free_inodes=1_000_000,
            block_size=block_size,
        ),
    )

    backup = creator.create(make_request())

    assert backup.manifest_path.is_file()
    assert (backup.manifest_path.parent.parent / "latest").is_symlink()


def test_whole_backup_rejects_one_byte_below_block_accounted_total_cap(
    tmp_path: Path,
) -> None:
    block_size = 4096
    config = make_config(tmp_path)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=Boto3MinioMirror(
            client_factory=lambda *_args, **_kwargs: OneObjectS3(),
            disk_reserve_bytes=0,
            inode_reserve=0,
        ),
        now=lambda: FIXED_NOW,
        max_postgres_bytes=len(POSTGRES_BYTES),
        max_total_bytes=(16 * block_size) - 1,
        disk_reserve_bytes=0,
        inode_reserve=0,
        capacity_provider=lambda _path: backup_module._CapacitySnapshot(
            free_bytes=1_000_000,
            free_inodes=1_000_000,
            block_size=block_size,
        ),
    )

    with pytest.raises(BackupError):
        creator.create(make_request())

    backups_root = config.rollout_root / "backups"
    assert not (backups_root / "latest").exists()
    assert not list(backups_root.glob("*/backup-manifest.json"))


def test_manifest_is_pending_until_validation_and_failure_cleans_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    runner = RecordingRunner()
    seen_paths: list[Path] = []

    def reject_validation(manifest_path: Path, **_kwargs: object) -> list[str]:
        seen_paths.append(manifest_path)
        assert manifest_path.name.startswith(".backup-manifest.")
        assert not (manifest_path.parent / "backup-manifest.json").exists()
        return ["untrusted-value-that-must-not-escape"]

    monkeypatch.setattr(backup_module, "validate_backup_manifest", reject_validation)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=runner,
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="manifest_validation_failed") as exc_info:
        creator.create(make_request())

    assert len(seen_paths) == 1
    assert seen_paths[0].name.startswith(".backup-manifest.")
    bundle_root = seen_paths[0].parent
    assert bundle_root.is_dir()
    assert not (bundle_root / "backup-manifest.json").exists()
    assert list(bundle_root.glob(".backup-manifest.*")) == []
    assert not (bundle_root.parent / "latest").exists()
    assert "untrusted-value-that-must-not-escape" not in str(exc_info.value)


def test_create_fails_closed_when_shared_manifest_traversal_limit_is_exceeded(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
        traversal_limits=BackupTraversalLimits(max_files=5),
    )

    with pytest.raises(BackupError, match="manifest_write_failed"):
        creator.create(make_request())

    bundle_root = config.rollout_root / "backups" / "20260713T200000Z-stg-20260713-abcdef12"
    assert not (bundle_root / "backup-manifest.json").exists()
    assert not list(bundle_root.glob(".backup-manifest.*"))
    assert not (bundle_root.parent / "latest").exists()


def test_shared_entry_cap_stops_before_starting_postgres_command(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    runner = RecordingRunner()
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=runner,
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
        traversal_limits=BackupTraversalLimits(max_entries=5),
    )

    with pytest.raises(BackupError, match="postgres_dump_failed"):
        creator.create(make_request())

    assert runner.argvs == [
        [
            "kubectl",
            "-n",
            "loom-staging",
            "port-forward",
            "--address",
            "127.0.0.1",
            "service/loom-minio",
            ":9000",
        ]
    ]
    assert runner.port_forward.events == [
        f"ready:127.0.0.1:15.0:{TEST_MINIO_PORT}",
        "terminate",
        "wait:5.0",
    ]
    backups_root = config.rollout_root / "backups"
    assert not (backups_root / "latest").exists()
    assert not list(backups_root.glob("*/backup-manifest.json"))


class UnstoppablePortForward(RecordingPortForward):
    def wait(self, timeout_seconds: float) -> bool:
        self.events.append(f"wait:{timeout_seconds}")
        return False


def test_port_forward_cleanup_must_confirm_exit_before_manifest_publication(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    port_forward = UnstoppablePortForward()
    runner = RecordingRunner(port_forward=port_forward)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=runner,
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="minio_transport_cleanup_failed"):
        creator.create(make_request())

    assert port_forward.events == [
        f"ready:127.0.0.1:15.0:{TEST_MINIO_PORT}",
        "terminate",
        "wait:5.0",
        "kill",
        "wait:5.0",
    ]
    bundle_root = config.rollout_root / "backups" / "20260713T200000Z-stg-20260713-abcdef12"
    assert bundle_root.is_dir()
    assert not (bundle_root / "backup-manifest.json").exists()
    assert not (bundle_root.parent / "latest").exists()


def test_creator_has_real_command_and_minio_defaults(tmp_path: Path) -> None:
    creator = BackupCreator(
        make_config(tmp_path),
        service_uid=os.getuid(),
        now=lambda: FIXED_NOW,
    )

    assert isinstance(creator._runner, SubprocessBackupCommandRunner)
    assert isinstance(creator._minio, Boto3MinioMirror)
    assert creator._minio._max_objects == 1_000_000
    assert creator._minio._max_entries == 15_999_994
    assert creator._traversal_limits.max_files == 1_000_004
    assert creator._traversal_limits.max_entries == 16_000_000


def test_resume_revalidates_exact_old_backup_without_creating_another(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    first = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    ).create(make_request())
    backups_root = config.rollout_root / "backups"
    roots_before = sorted(path.name for path in backups_root.iterdir())
    resume_runner = RecordingRunner()
    resume_creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=resume_runner,
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW + timedelta(days=2),
    )

    revalidated = resume_creator.revalidate(first, enforce_freshness=False)

    assert revalidated == first
    assert resume_runner.argvs == []
    assert sorted(path.name for path in backups_root.iterdir()) == roots_before


def test_resume_fails_closed_when_shared_traversal_limit_is_exceeded(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    backup = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    ).create(make_request())
    runner = RecordingRunner()
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=runner,
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
        traversal_limits=BackupTraversalLimits(max_files=1),
    )

    with pytest.raises(BackupError, match="backup_revalidation_failed"):
        creator.revalidate(backup, enforce_freshness=False)

    assert runner.argvs == []
    assert backup.manifest_path.is_file()


def test_revalidate_detects_component_mutation_without_running_commands(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    backup = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    ).create(make_request())
    object_path = backup.manifest_path.parent / "minio" / "loom-staging-trajectories" / "object.bin"
    original = object_path.read_bytes()
    object_path.write_bytes(b"x" * len(original))
    object_path.chmod(0o600)
    runner = RecordingRunner()

    with pytest.raises(BackupError, match="backup_revalidation_failed"):
        BackupCreator(
            config,
            service_uid=os.getuid(),
            runner=runner,
            minio=SuccessfulMinioMirror(),
            now=lambda: FIXED_NOW,
        ).revalidate(backup, enforce_freshness=False)

    assert runner.argvs == []


def test_revalidate_rejects_supplied_manifest_digest_mismatch_without_commands(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    backup = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    ).create(make_request())
    mismatched = VerifiedBackup(
        manifest_path=backup.manifest_path,
        manifest_sha256="0" * 64,
    )
    runner = RecordingRunner()

    with pytest.raises(BackupError, match="backup_manifest_digest_mismatch"):
        BackupCreator(
            config,
            service_uid=os.getuid(),
            runner=runner,
            minio=SuccessfulMinioMirror(),
            now=lambda: FIXED_NOW,
        ).revalidate(mismatched, enforce_freshness=False)

    assert runner.argvs == []


def test_resume_rejects_symlinked_snapshot_ancestor_without_commands(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    backup = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    ).create(make_request())
    snapshot_root = backup.manifest_path.parent
    relocated_root = snapshot_root.parent / f"relocated-{snapshot_root.name}"
    snapshot_root.rename(relocated_root)
    snapshot_root.symlink_to(relocated_root.name)
    resume_runner = RecordingRunner()
    resume_creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=resume_runner,
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="backup_revalidation_failed"):
        resume_creator.revalidate(backup, enforce_freshness=False)

    assert resume_runner.argvs == []


def test_backup_root_parent_symlink_is_rejected_without_writing_outside(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir(mode=0o700)
    (config.rollout_root / "backups").symlink_to(outside)
    runner = RecordingRunner()
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=runner,
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="backup_root_create_failed"):
        creator.create(make_request())

    assert list(outside.iterdir()) == []
    assert runner.argvs == []


def test_backup_root_parent_non_directory_is_rejected_before_commands(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    backups_path = config.rollout_root / "backups"
    backups_path.write_bytes(b"not-a-directory")
    runner = RecordingRunner()
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=runner,
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="backup_root_create_failed"):
        creator.create(make_request())

    assert backups_path.read_bytes() == b"not-a-directory"
    assert runner.argvs == []


def test_new_backups_fd_is_closed_when_parent_fsync_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    real_open = backup_module.os.open
    real_close = backup_module.os.close
    real_fsync = backup_module.os.fsync
    rollout_fd: int | None = None
    backups_fd: int | None = None
    closed: list[int] = []

    def recording_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        nonlocal rollout_fd, backups_fd
        fd = real_open(path, flags, *args, **kwargs)  # type: ignore[arg-type]
        if path == config.rollout_root.name:
            rollout_fd = fd
        elif path == "backups":
            backups_fd = fd
        return fd

    def failing_fsync(fd: int) -> None:
        if fd == rollout_fd and backups_fd is not None:
            raise OSError("injected parent fsync failure")
        real_fsync(fd)

    def recording_close(fd: int) -> None:
        closed.append(fd)
        real_close(fd)

    monkeypatch.setattr(backup_module.os, "open", recording_open)
    monkeypatch.setattr(backup_module.os, "fsync", failing_fsync)
    monkeypatch.setattr(backup_module.os, "close", recording_close)
    runner = RecordingRunner()

    with pytest.raises(BackupError, match="backup_root_create_failed"):
        BackupCreator(
            config,
            service_uid=os.getuid(),
            runner=runner,
            minio=SuccessfulMinioMirror(),
            now=lambda: FIXED_NOW,
            disk_reserve_bytes=0,
            inode_reserve=0,
            capacity_provider=lambda _path: backup_module._CapacitySnapshot(
                free_bytes=1_000_000,
                free_inodes=1_000_000,
                block_size=1,
            ),
        ).create(make_request())

    assert backups_fd is not None
    assert backups_fd in closed
    assert runner.argvs == []


class FailingReadinessPortForward(RecordingPortForward):
    def wait_ready(self, host: str, timeout_seconds: float) -> int:
        self.events.append(f"ready:{host}:{timeout_seconds}")
        raise RuntimeError("untrusted-stage-detail")


class StageFailingRunner(RecordingRunner):
    def __init__(self, stage: str) -> None:
        port_forward: RecordingPortForward | None = None
        if stage == "port_forward_readiness":
            port_forward = FailingReadinessPortForward()
        super().__init__(port_forward=port_forward)
        self.stage = stage

    def stream_stdout(
        self,
        argv: Sequence[str],
        sink: BinaryIO,
        *,
        env: Mapping[str, str],
        timeout_seconds: float | None = None,
    ) -> None:
        if self.stage == "postgres":
            self._record(argv, env)
            self.timeouts.append(timeout_seconds)
            sink.write(b"partial\x00dump")
            raise RuntimeError("untrusted-stage-detail")
        super().stream_stdout(
            argv,
            sink,
            env=env,
            timeout_seconds=timeout_seconds,
        )

    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float | None = None,
    ) -> bytes:
        rendered = list(argv)
        if self.stage == "credentials" and rendered[-2:] == ["-o", "json"]:
            self._record(argv, env)
            self.timeouts.append(timeout_seconds)
            raise RuntimeError("untrusted-stage-detail")
        if self.stage.startswith("secret:") and rendered[-2:] == ["-o", "yaml"]:
            secret_name = rendered[rendered.index("secret") + 1]
            if secret_name == self.stage.removeprefix("secret:"):
                self._record(argv, env)
                self.timeouts.append(timeout_seconds)
                raise RuntimeError("untrusted-stage-detail")
        return super().capture_stdout(
            argv,
            env=env,
            timeout_seconds=timeout_seconds,
        )


def test_port_forward_is_localhost_only_and_readiness_failure_is_explicit(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    runner = StageFailingRunner("port_forward_readiness")
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=runner,
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="minio_transport_failed") as exc_info:
        creator.create(make_request())
    assert exc_info.value.public_reason == "backup_transport_failed"

    port_forward_argv = next(argv for argv in runner.argvs if "port-forward" in argv)
    assert port_forward_argv == [
        "kubectl",
        "-n",
        "loom-staging",
        "port-forward",
        "--address",
        "127.0.0.1",
        "service/loom-minio",
        ":9000",
    ]
    assert runner.port_forward.events == [
        "ready:127.0.0.1:15.0",
        "terminate",
        "wait:5.0",
    ]
    backups_root = config.rollout_root / "backups"
    assert not (backups_root / "latest").exists()
    assert list(backups_root.glob("*/backup-manifest.json")) == []
    assert not any(argv[:2] == ["kubectl", "exec"] for argv in runner.argvs)


def test_unrelated_legacy_tunnel_handle_is_untouched_during_backup(tmp_path: Path) -> None:
    legacy_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        try:
            legacy_listener.bind(("127.0.0.1", 19000))
            legacy_listener.listen(1)
        except OSError:
            # A real operator-owned tunnel may already occupy the historical port.
            legacy_listener.close()

        unrelated = RecordingPortForward(selected_port=19000)
        runner = RecordingRunner()
        config = make_config(tmp_path)
        creator = BackupCreator(
            config,
            service_uid=os.getuid(),
            runner=runner,
            minio=SuccessfulMinioMirror(),
            now=lambda: FIXED_NOW,
        )

        creator.create(make_request())

        port_forward_argv = next(argv for argv in runner.argvs if "port-forward" in argv)
        assert port_forward_argv[-1] == ":9000"
        assert "19000" not in " ".join(port_forward_argv)
        assert unrelated.events == []
    finally:
        legacy_listener.close()


def test_concurrent_transports_keep_distinct_child_ports_and_cleanup(
    tmp_path: Path,
) -> None:
    first_root = tmp_path / "first-rollout"
    second_root = tmp_path / "second-rollout"
    first_root.mkdir()
    second_root.mkdir()
    first_handle = RecordingPortForward(selected_port=39123)
    second_handle = RecordingPortForward(selected_port=39124)
    first = BackupCreator(
        make_config(first_root),
        service_uid=os.getuid(),
        runner=RecordingRunner(port_forward=first_handle),
        minio=SuccessfulMinioMirror(),
    )
    second = BackupCreator(
        make_config(second_root),
        service_uid=os.getuid(),
        runner=RecordingRunner(port_forward=second_handle),
        minio=SuccessfulMinioMirror(),
    )

    first_port, stop_first = first._start_minio_transport()
    second_port, stop_second = second._start_minio_transport()

    assert (first_port, second_port) == (39123, 39124)
    stop_first()
    assert first_handle.events[-2:] == ["terminate", "wait:5.0"]
    assert second_handle.events == ["ready:127.0.0.1:15.0:39124"]
    stop_second()
    assert second_handle.events[-2:] == ["terminate", "wait:5.0"]


@pytest.mark.parametrize(
    ("stage", "expected_code"),
    [
        ("postgres", "postgres_dump_failed"),
        ("credentials", "minio_credentials_failed"),
        ("port_forward_readiness", "minio_transport_failed"),
        ("minio", "minio_snapshot_failed"),
        ("secret:loom-secrets", "secret_export_failed"),
        ("secret:loom-admin-secret", "secret_export_failed"),
        ("secret:loom-staging-tls", "secret_export_failed"),
        ("manifest_write", "manifest_write_failed"),
        ("manifest_validation", "manifest_validation_failed"),
        ("manifest_hash", "manifest_hash_failed"),
        ("manifest_publish", "manifest_publish_failed"),
        ("latest_replace", "latest_publish_failed"),
    ],
)
def test_each_failure_stage_preserves_old_latest_and_private_components(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
    expected_code: str,
) -> None:
    config = make_config(tmp_path)
    backups_root = config.rollout_root / "backups"
    backups_root.mkdir(mode=0o700)
    old = backups_root / "20260712T120000Z-old-request"
    old.mkdir(mode=0o700)
    (backups_root / "latest").symlink_to(old.name)
    runner = StageFailingRunner(stage)
    minio: object = FailingMinioMirror() if stage == "minio" else SuccessfulMinioMirror()

    def fail(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("untrusted-stage-detail")

    if stage == "manifest_write":
        monkeypatch.setattr(backup_module, "write_backup_manifest", fail)
    elif stage == "manifest_validation":
        monkeypatch.setattr(
            backup_module,
            "validate_backup_manifest",
            lambda *_args, **_kwargs: ["untrusted-stage-detail"],
        )
    elif stage == "manifest_hash":
        monkeypatch.setattr(backup_module, "backup_manifest_sha256", fail)
    elif stage == "manifest_publish":
        monkeypatch.setattr(backup_module.os, "link", fail)
    elif stage == "latest_replace":
        monkeypatch.setattr(backup_module.os, "replace", fail)

    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=runner,
        minio=minio,  # type: ignore[arg-type]
        now=lambda: FIXED_NOW,
    )
    with pytest.raises(BackupError, match=expected_code) as exc_info:
        creator.create(make_request())

    failed_root = backups_root / "20260713T200000Z-stg-20260713-abcdef12"
    assert failed_root.is_dir()
    assert failed_root.stat().st_mode & 0o777 == 0o700
    assert not (failed_root / "backup-manifest.json").exists()
    assert list(failed_root.glob(".backup-manifest.*")) == []
    assert list(backups_root.glob(".latest.*.tmp")) == []
    assert (backups_root / "latest").readlink() == Path(old.name)
    rendered_error = "".join(
        traceback.format_exception(
            type(exc_info.value),
            exc_info.value,
            exc_info.value.__traceback__,
        )
    )
    for value in (MINIO_ACCESS_KEY, MINIO_SECRET_KEY, "untrusted-stage-detail"):
        assert value not in rendered_error
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


def test_latest_directory_fsync_failure_restores_previous_relative_pointer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    backups_root = config.rollout_root / "backups"
    backups_root.mkdir(mode=0o700)
    old = backups_root / "20260712T120000Z-old-request"
    old.mkdir(mode=0o700)
    latest = backups_root / "latest"
    latest.symlink_to(old.name)
    original_replace = backup_module.os.replace
    original_fsync = backup_module.os.fsync
    original_symlink = backup_module.os.symlink
    original_reserve = backup_module._BackupResourceBudget.reserve_entry
    state = {
        "rollback_reserved": False,
        "rollback_created": False,
        "rollback_durable": False,
        "latest_replaced": False,
        "failed": False,
    }

    def recording_reserve(
        resources: object,
        path: Path,
        *,
        component: str,
        inode: bool = True,
    ) -> object:
        account = original_reserve(
            resources,  # type: ignore[arg-type]
            path,
            component=component,
            inode=inode,
        )
        if component == "publication-rollback":
            state["rollback_reserved"] = True
        return account

    def tracking_symlink(
        target: object,
        link_name: object,
        *args: object,
        **kwargs: object,
    ) -> None:
        original_symlink(target, link_name, *args, **kwargs)  # type: ignore[arg-type]
        if isinstance(link_name, str) and link_name.endswith(".rollback"):
            assert state["rollback_reserved"]
            assert os.readlink(backups_root / link_name) == old.name
            state["rollback_created"] = True

    def tracking_replace(*args: object, **kwargs: object) -> None:
        assert state["rollback_reserved"]
        assert state["rollback_created"]
        assert state["rollback_durable"]
        original_replace(*args, **kwargs)  # type: ignore[arg-type]
        state["latest_replaced"] = True

    def fail_first_post_replace_fsync(fd: int) -> None:
        if state["latest_replaced"] and not state["failed"]:
            state["failed"] = True
            raise OSError("untrusted-stage-detail")
        original_fsync(fd)
        if state["rollback_created"] and not state["latest_replaced"]:
            state["rollback_durable"] = True

    monkeypatch.setattr(backup_module.os, "symlink", tracking_symlink)
    monkeypatch.setattr(backup_module.os, "replace", tracking_replace)
    monkeypatch.setattr(backup_module.os, "fsync", fail_first_post_replace_fsync)
    monkeypatch.setattr(
        backup_module._BackupResourceBudget,
        "reserve_entry",
        recording_reserve,
    )
    block_size = 4096
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=Boto3MinioMirror(
            client_factory=lambda *_args, **_kwargs: OneObjectS3(),
            disk_reserve_bytes=0,
            inode_reserve=0,
        ),
        now=lambda: FIXED_NOW,
        max_postgres_bytes=len(POSTGRES_BYTES),
        max_total_bytes=17 * block_size,
        disk_reserve_bytes=0,
        inode_reserve=0,
        capacity_provider=lambda _path: backup_module._CapacitySnapshot(
            free_bytes=1_000_000,
            free_inodes=1_000_000,
            block_size=block_size,
        ),
    )

    with pytest.raises(BackupError, match="latest_publish_failed"):
        creator.create(make_request())

    failed_root = backups_root / "20260713T200000Z-stg-20260713-abcdef12"
    assert state == {
        "rollback_reserved": True,
        "rollback_created": True,
        "rollback_durable": True,
        "latest_replaced": True,
        "failed": True,
    }
    assert latest.is_symlink()
    assert latest.readlink() == Path(old.name)
    assert not (failed_root / "backup-manifest.json").exists()
    assert list(backups_root.glob(".latest.*")) == []


def test_replace_exception_after_atomic_success_restores_previous_latest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    backups_root = config.rollout_root / "backups"
    backups_root.mkdir(mode=0o700)
    old = backups_root / "20260712T120000Z-old-request"
    old.mkdir(mode=0o700)
    latest = backups_root / "latest"
    latest.symlink_to(old.name)
    original_replace = backup_module.os.replace
    replace_raised = False

    def replace_then_raise(*args: object, **kwargs: object) -> None:
        nonlocal replace_raised
        should_raise = (
            not replace_raised
            and len(args) >= 2
            and str(args[0]).endswith(".tmp")
            and args[1] == "latest"
        )
        original_replace(*args, **kwargs)  # type: ignore[arg-type]
        if should_raise:
            replace_raised = True
            raise OSError("replace completion was ambiguous")

    monkeypatch.setattr(backup_module.os, "replace", replace_then_raise)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="latest_publish_failed"):
        creator.create(make_request())

    failed_root = backups_root / "20260713T200000Z-stg-20260713-abcdef12"
    assert replace_raised
    assert latest.readlink() == Path(old.name)
    assert not (failed_root / "backup-manifest.json").exists()
    assert list(backups_root.glob(".latest.*")) == []


def test_replace_exception_after_first_latest_removes_ambiguous_pointer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    original_replace = backup_module.os.replace
    replace_raised = False

    def replace_then_raise(*args: object, **kwargs: object) -> None:
        nonlocal replace_raised
        should_raise = (
            not replace_raised
            and len(args) >= 2
            and str(args[0]).endswith(".tmp")
            and args[1] == "latest"
        )
        original_replace(*args, **kwargs)  # type: ignore[arg-type]
        if should_raise:
            replace_raised = True
            raise OSError("replace completion was ambiguous")

    monkeypatch.setattr(backup_module.os, "replace", replace_then_raise)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="latest_publish_failed"):
        creator.create(make_request())

    backups_root = config.rollout_root / "backups"
    failed_root = backups_root / "20260713T200000Z-stg-20260713-abcdef12"
    assert replace_raised
    assert not (backups_root / "latest").exists()
    assert not (failed_root / "backup-manifest.json").exists()
    assert list(backups_root.glob(".latest.*")) == []


@pytest.mark.parametrize("recovery_failure", ["readback", "restore"])
def test_ambiguous_replace_recovery_failure_preserves_canonical_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recovery_failure: str,
) -> None:
    config = make_config(tmp_path)
    backups_root = config.rollout_root / "backups"
    backups_root.mkdir(mode=0o700)
    old = backups_root / "20260712T120000Z-old-request"
    old.mkdir(mode=0o700)
    latest = backups_root / "latest"
    latest.symlink_to(old.name)
    original_replace = backup_module.os.replace
    replace_raised = False

    def replace_then_raise(*args: object, **kwargs: object) -> None:
        nonlocal replace_raised
        should_raise = (
            not replace_raised
            and len(args) >= 2
            and str(args[0]).endswith(".tmp")
            and args[1] == "latest"
        )
        original_replace(*args, **kwargs)  # type: ignore[arg-type]
        if should_raise:
            replace_raised = True
            raise OSError("replace completion was ambiguous")

    def fail_recovery(*_args: object, **_kwargs: object) -> object:
        raise OSError("recovery state is unavailable")

    monkeypatch.setattr(backup_module.os, "replace", replace_then_raise)
    if recovery_failure == "readback":
        monkeypatch.setattr(backup_module, "_latest_matches", fail_recovery)
    else:
        monkeypatch.setattr(backup_module, "_restore_latest", fail_recovery)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="latest_publish_failed"):
        creator.create(make_request())

    failed_root = backups_root / "20260713T200000Z-stg-20260713-abcdef12"
    manifest_path = failed_root / "backup-manifest.json"
    assert replace_raised
    assert latest.readlink() == Path(failed_root.name)
    assert manifest_path.is_file()
    assert list(backups_root.glob(".latest.*")) == []


def test_successful_latest_replacement_removes_precreated_rollback(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    backups_root = config.rollout_root / "backups"
    backups_root.mkdir(mode=0o700)
    old = backups_root / "20260712T120000Z-old-request"
    old.mkdir(mode=0o700)
    latest = backups_root / "latest"
    latest.symlink_to(old.name)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    backup = creator.create(make_request())

    assert latest.readlink() == Path(backup.manifest_path.parent.name)
    assert list(backups_root.glob(".latest.*")) == []


def test_uncertain_latest_rollback_preserves_verified_canonical_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    backups_root = config.rollout_root / "backups"
    backups_root.mkdir(mode=0o700)
    old = backups_root / "20260712T120000Z-old-request"
    old.mkdir(mode=0o700)
    latest = backups_root / "latest"
    latest.symlink_to(old.name)
    original_replace = backup_module.os.replace
    original_fsync = backup_module.os.fsync
    latest_replaced = False
    fsync_failed = False

    def tracking_replace(*args: object, **kwargs: object) -> None:
        nonlocal latest_replaced
        original_replace(*args, **kwargs)  # type: ignore[arg-type]
        latest_replaced = True

    def fail_first_post_replace_fsync(fd: int) -> None:
        nonlocal fsync_failed
        if latest_replaced and not fsync_failed:
            fsync_failed = True
            raise OSError("untrusted-stage-detail")
        original_fsync(fd)

    def fail_restore(*_args: object, **_kwargs: object) -> None:
        raise OSError("untrusted-rollback-detail")

    monkeypatch.setattr(backup_module.os, "replace", tracking_replace)
    monkeypatch.setattr(backup_module.os, "fsync", fail_first_post_replace_fsync)
    monkeypatch.setattr(backup_module, "_restore_latest", fail_restore)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="latest_publish_failed") as exc_info:
        creator.create(make_request())

    failed_root = backups_root / "20260713T200000Z-stg-20260713-abcdef12"
    manifest_path = failed_root / "backup-manifest.json"
    assert latest_replaced and fsync_failed
    assert latest.readlink() == Path(failed_root.name)
    assert manifest_path.is_file()
    assert (
        validate_backup_manifest(
            manifest_path,
            environment="staging",
            namespace="loom-staging",
            expected_owner_uid=os.getuid(),
            require_private_files=True,
            min_remaining_hours=2,
            now=FIXED_NOW,
        )
        == []
    )
    assert list(failed_root.glob(".backup-manifest.*")) == []
    assert list(backups_root.glob(".latest.*")) == []
    rendered_error = "".join(
        traceback.format_exception(
            type(exc_info.value),
            exc_info.value,
            exc_info.value.__traceback__,
        )
    )
    for sentinel in ("untrusted-stage-detail", "untrusted-rollback-detail"):
        assert sentinel not in rendered_error


class StartupOutputProcess:
    def __init__(self, output: BinaryIO) -> None:
        self.stdout = output

    def poll(self) -> int | None:
        return None


def test_child_selected_port_is_independent_of_legacy_port() -> None:
    with tempfile.TemporaryFile(mode="w+b") as output:
        output.write(f"Forwarding from 127.0.0.1:{TEST_MINIO_PORT} -> 9000\n".encode())
        output.flush()
        output.seek(0)
        process = StartupOutputProcess(output)
        handle = backup_module._SubprocessPortForward(process)  # type: ignore[arg-type]

        assert handle.wait_ready("127.0.0.1", 0.1) == TEST_MINIO_PORT


def test_subprocess_port_forward_rejects_untrusted_startup_output() -> None:
    process = StartupOutputProcess(io.BytesIO(b"Forwarding from 0.0.0.0:39123 -> 9000\n"))
    handle = backup_module._SubprocessPortForward(process)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="readiness"):
        handle.wait_ready("127.0.0.1", 0.1)


def test_subprocess_port_forward_accepts_exact_child_ready_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    popen_calls: list[tuple[list[str], dict[str, object]]] = []

    def fake_popen(argv: list[str], **kwargs: object) -> StartupOutputProcess:
        popen_calls.append((argv, dict(kwargs)))
        return StartupOutputProcess(
            io.BytesIO(f"Forwarding from 127.0.0.1:{TEST_MINIO_PORT} -> 9000\n".encode())
        )

    monkeypatch.setattr(backup_module.subprocess, "Popen", fake_popen)
    runner = SubprocessBackupCommandRunner()
    argv = [
        "kubectl",
        "-n",
        "loom-staging",
        "port-forward",
        "--address",
        "127.0.0.1",
        "service/loom-minio",
        ":9000",
    ]

    handle = runner.start(argv, env={"PATH": "/usr/bin"})
    assert handle.wait_ready("127.0.0.1", 0.1) == TEST_MINIO_PORT
    handle.close()

    assert len(popen_calls) == 1
    assert popen_calls[0][0] == argv
    assert popen_calls[0][1]["stderr"] == subprocess.STDOUT
    assert popen_calls[0][1]["stdout"] == subprocess.PIPE


def test_subprocess_port_forward_wrapper_failure_reaps_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class UnwrappedProcess:
        def __init__(self) -> None:
            self.stdout = (tmp_path / "startup.log").open("w+b")
            self.running = True

        def terminate(self) -> None:
            events.append("terminate")

        def wait(self, *, timeout: float) -> int:
            events.append(f"wait:{timeout}")
            if self.running:
                raise subprocess.TimeoutExpired("kubectl", timeout)
            return -9

        def kill(self) -> None:
            events.append("kill")
            self.running = False

        def poll(self) -> int | None:
            return None if self.running else -9

    process = UnwrappedProcess()

    def fake_popen(_argv: list[str], **_kwargs: object) -> UnwrappedProcess:
        return process

    def fail_thread_start(_thread: object) -> None:
        raise RuntimeError("thread resources exhausted")

    monkeypatch.setattr(backup_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(backup_module.threading.Thread, "start", fail_thread_start)

    with pytest.raises(RuntimeError, match="thread resources exhausted"):
        SubprocessBackupCommandRunner().start(
            ["kubectl", "port-forward"],
            env={"PATH": "/usr/bin"},
        )

    assert events == ["terminate", "wait:5.0", "kill", "wait:5.0"]
    assert process.poll() == -9
    assert process.stdout.closed


@pytest.mark.parametrize("failure", ["constructor", "start"])
def test_subprocess_stream_watchdog_setup_failure_reaps_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    events: list[str] = []

    class UnwatchedProcess:
        def __init__(self) -> None:
            self.stdout = (tmp_path / "postgres.dump").open("w+b")
            self.running = True

        def terminate(self) -> None:
            events.append("terminate")

        def wait(self, *, timeout: float) -> int:
            events.append(f"wait:{timeout}")
            if self.running:
                raise subprocess.TimeoutExpired("pg_dump", timeout)
            return -9

        def kill(self) -> None:
            events.append("kill")
            self.running = False

        def poll(self) -> int | None:
            return None if self.running else -9

    process = UnwatchedProcess()

    monkeypatch.setattr(
        backup_module.subprocess,
        "Popen",
        lambda *_args, **_kwargs: process,
    )

    def fail_thread_constructor(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("thread resources exhausted")

    def fail_thread_start(_thread: object) -> None:
        raise RuntimeError("thread resources exhausted")

    if failure == "constructor":
        monkeypatch.setattr(
            backup_module.threading,
            "Thread",
            fail_thread_constructor,
        )
    else:
        monkeypatch.setattr(backup_module.threading.Thread, "start", fail_thread_start)

    with pytest.raises(RuntimeError, match="thread resources exhausted"):
        SubprocessBackupCommandRunner().stream_stdout(
            ["pg_dump"],
            io.BytesIO(),
            env={"PATH": "/usr/bin"},
            timeout_seconds=600.0,
        )

    assert events == ["terminate", "wait:5.0", "kill", "wait:5.0"]
    assert process.poll() == -9
    assert process.stdout.closed


def test_subprocess_runner_applies_explicit_command_timeouts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_calls: list[tuple[list[str], dict[str, object]]] = []
    popen_calls: list[tuple[list[str], dict[str, object]]] = []

    class SuccessfulProcess:
        def __init__(self) -> None:
            self.stdout = io.BytesIO(b"streamed")

        def wait(self, timeout: float) -> int:
            assert timeout == 5.0
            return 0

        def kill(self) -> None:
            raise AssertionError("successful process was killed")

        def terminate(self) -> None:
            raise AssertionError("successful process was terminated")

    def fake_popen(argv: list[str], **kwargs: object) -> SuccessfulProcess:
        popen_calls.append((argv, dict(kwargs)))
        return SuccessfulProcess()

    def fake_run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        run_calls.append((argv, dict(kwargs)))
        return subprocess.CompletedProcess(argv, 0, stdout=b"captured")

    monkeypatch.setattr(backup_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(backup_module.subprocess, "run", fake_run)
    runner = SubprocessBackupCommandRunner()
    sink = io.BytesIO()

    runner.stream_stdout(
        ["kubectl", "exec"],
        sink,
        env={"PATH": "/usr/bin"},
        timeout_seconds=600.0,
    )
    captured = runner.capture_stdout(
        ["kubectl", "get"],
        env={"PATH": "/usr/bin"},
        timeout_seconds=30.0,
    )

    assert sink.getvalue() == b"streamed"
    assert captured == b"captured"
    assert popen_calls[0][1]["stdout"] == subprocess.PIPE
    assert popen_calls[0][1]["stderr"] == subprocess.DEVNULL
    assert run_calls[0][1]["timeout"] == 30.0
    assert run_calls[0][1]["stderr"] == subprocess.DEVNULL


def test_subprocess_stream_timeout_kills_and_reaps_child() -> None:
    started = time.monotonic()

    with pytest.raises(RuntimeError, match="backup command failed"):
        SubprocessBackupCommandRunner().stream_stdout(
            [sys.executable, "-c", "import time; time.sleep(5)"],
            io.BytesIO(),
            env={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
            timeout_seconds=0.01,
        )

    assert time.monotonic() - started < 1.0


def test_subprocess_runner_pumps_stdout_through_budgetable_sink() -> None:
    class NoDirectFileDescriptor(io.BytesIO):
        def fileno(self) -> int:
            raise AssertionError("subprocess stdout bypassed the guarded writer")

    sink = NoDirectFileDescriptor()

    SubprocessBackupCommandRunner().stream_stdout(
        [sys.executable, "-c", "import sys; sys.stdout.buffer.write(b'chunked')"],
        sink,
        env={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
        timeout_seconds=2.0,
    )

    assert sink.getvalue() == b"chunked"


def test_subprocess_runner_rejects_nonzero_exit_without_stderr_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailedProcess:
        def __init__(self) -> None:
            self.stdout = io.BytesIO(b"")

        def wait(self, timeout: float) -> int:
            return 1

        def terminate(self) -> None:
            return None

        def kill(self) -> None:
            return None

    def fail(argv: list[str], **kwargs: object) -> FailedProcess:
        assert kwargs["stderr"] == subprocess.DEVNULL
        return FailedProcess()

    monkeypatch.setattr(backup_module.subprocess, "Popen", fail)

    with pytest.raises(RuntimeError, match="backup command failed"):
        SubprocessBackupCommandRunner().stream_stdout(
            ["kubectl", "exec"],
            io.BytesIO(),
            env={"PATH": "/usr/bin"},
            timeout_seconds=600.0,
        )


def test_create_rejects_naive_clock_before_creating_backup_root(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    runner = RecordingRunner()
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=runner,
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW.replace(tzinfo=None),
    )

    with pytest.raises(BackupError, match="backup_clock_invalid"):
        creator.create(make_request())

    assert runner.argvs == []
    assert not (config.rollout_root / "backups").exists()


def test_revalidate_rejects_naive_clock_without_commands(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    backup = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    ).create(make_request())
    runner = RecordingRunner()
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=runner,
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW.replace(tzinfo=None),
    )

    with pytest.raises(BackupError, match="backup_clock_invalid"):
        creator.revalidate(backup, enforce_freshness=False)

    assert runner.argvs == []


def test_long_running_backup_uses_completion_time_for_freshness_gate(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    clock_values = iter([FIXED_NOW, FIXED_NOW + timedelta(hours=23)])
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: next(clock_values),
    )

    with pytest.raises(BackupError, match="manifest_validation_failed"):
        creator.create(make_request())

    bundle_root = config.rollout_root / "backups" / "20260713T200000Z-stg-20260713-abcdef12"
    assert bundle_root.is_dir()
    assert not (bundle_root / "backup-manifest.json").exists()
    assert list(bundle_root.glob(".backup-manifest.*")) == []
    assert not (bundle_root.parent / "latest").exists()


def test_create_rechecks_freshness_after_integrity_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    validation_completed = False
    original_validate = backup_module.validate_backup_manifest

    def delayed_validate(*args: object, **kwargs: object) -> list[str]:
        nonlocal validation_completed
        problems = original_validate(*args, **kwargs)  # type: ignore[arg-type]
        validation_completed = True
        return problems

    def clock() -> datetime:
        if validation_completed:
            return FIXED_NOW + timedelta(hours=23)
        return FIXED_NOW

    monkeypatch.setattr(backup_module, "validate_backup_manifest", delayed_validate)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=clock,
    )

    with pytest.raises(BackupError, match="manifest_validation_failed"):
        creator.create(make_request())

    bundle_root = config.rollout_root / "backups" / "20260713T200000Z-stg-20260713-abcdef12"
    assert validation_completed
    assert not (bundle_root / "backup-manifest.json").exists()
    assert list(bundle_root.glob(".backup-manifest.*")) == []
    assert not (bundle_root.parent / "latest").exists()


def test_revalidate_rechecks_freshness_after_integrity_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    backup = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    ).create(make_request())
    validation_completed = False
    original_validate = backup_module.validate_backup_manifest

    def delayed_validate(*args: object, **kwargs: object) -> list[str]:
        nonlocal validation_completed
        problems = original_validate(*args, **kwargs)  # type: ignore[arg-type]
        validation_completed = True
        return problems

    def clock() -> datetime:
        if validation_completed:
            return FIXED_NOW + timedelta(hours=23)
        return FIXED_NOW

    monkeypatch.setattr(backup_module, "validate_backup_manifest", delayed_validate)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=clock,
    )

    with pytest.raises(BackupError, match="backup_revalidation_failed"):
        creator.revalidate(backup, enforce_freshness=True)

    assert validation_completed
    assert backup.manifest_path.is_file()


class LifecycleS3:
    def __init__(self, events: list[str], *, fail_close: bool = False) -> None:
        self.events = events
        self.fail_close = fail_close

    def list_objects_v2(self, **_kwargs: str) -> dict[str, object]:
        return {"Contents": [], "IsTruncated": False}

    def close(self) -> None:
        self.events.append("client_close")
        if self.fail_close:
            raise RuntimeError("client close failed")


class LifecyclePortForward(RecordingPortForward):
    def __init__(self, events: list[str]) -> None:
        self.events = events


class DeadlineBody:
    def __init__(self, events: list[str], entered: threading.Event) -> None:
        self.events = events
        self.entered = entered
        self.closed = False
        self.read_calls = 0
        self.released = threading.Event()

    def read(self, _amount: int) -> bytes:
        self.read_calls += 1
        if self.read_calls == 1:
            return b"x"
        self.entered.set()
        if not self.released.wait(1.0):
            raise AssertionError("deadline did not asynchronously close the body")
        raise OSError("body closed at deadline")

    def close(self) -> None:
        if not self.closed:
            self.events.append("body_close")
            self.closed = True
            self.released.set()


class DeadlineS3(LifecycleS3):
    def __init__(
        self,
        events: list[str],
        *,
        stage: str,
        entered: threading.Event,
    ) -> None:
        super().__init__(events)
        self.stage = stage
        self.entered = entered
        self.released = threading.Event()
        self.closed = False
        self.body = DeadlineBody(events, entered)

    def _block(self) -> None:
        self.entered.set()
        if not self.released.wait(1.0):
            raise AssertionError("deadline did not asynchronously close the client")
        raise OSError("client closed at deadline")

    def list_objects_v2(self, **_kwargs: str) -> dict[str, object]:
        if self.stage == "list":
            self._block()
        return {"Contents": [{"Key": "object.bin"}], "IsTruncated": False}

    def get_object(self, **_kwargs: str) -> dict[str, object]:
        if self.stage == "get":
            self._block()
        return {"Body": self.body, "ContentLength": 4}

    def close(self) -> None:
        if not self.closed:
            self.events.append("client_close")
            self.closed = True
            self.released.set()


class DeadlinePortForward(RecordingPortForward):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self.events = events

    def close(self) -> None:
        self.events.append("port_forward_close")


@pytest.mark.parametrize("stage", ["list", "get", "read"])
def test_minio_wall_clock_deadline_interrupts_blocking_io_and_cleans_everything(
    tmp_path: Path,
    stage: str,
) -> None:
    events: list[str] = []
    entered = threading.Event()
    client = DeadlineS3(events, stage=stage, entered=entered)
    port_forward = DeadlinePortForward(events)

    def expire_after_block(stop: threading.Event, _timeout_seconds: float) -> bool:
        assert entered.wait(1.0)
        return stop.is_set()

    mirror = Boto3MinioMirror(
        client_factory=lambda *_args, **_kwargs: client,
        timeout_seconds=0.01,
        deadline_waiter=expire_after_block,
        cancellation_grace_seconds=0.5,
    )
    config = make_config(tmp_path)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(port_forward=port_forward),
        minio=mirror,
        now=lambda: FIXED_NOW,
    )

    started = time.monotonic()
    with pytest.raises(BackupError, match="minio_snapshot_failed"):
        creator.create(make_request())
    elapsed = time.monotonic() - started

    assert elapsed < 1.0
    assert client.closed
    if stage == "read":
        assert client.body.closed
        assert events.index("body_close") < events.index("client_close")
    assert events.index("client_close") < events.index("terminate")
    assert events[-3:] == ["terminate", "wait:5.0", "port_forward_close"]
    backups_root = config.rollout_root / "backups"
    assert not (backups_root / "latest").exists()
    assert list(backups_root.glob("*/backup-manifest.json")) == []
    assert list(backups_root.rglob("*.part")) == []


def test_minio_watchdog_uses_remaining_absolute_deadline_after_client_setup(
    tmp_path: Path,
) -> None:
    entered = threading.Event()
    now = [100.0]
    waiter_timeouts: list[float] = []

    class ReturnsAfterCloseS3:
        def __init__(self) -> None:
            self.released = threading.Event()
            self.closed = False

        def list_objects_v2(self, **_kwargs: str) -> dict[str, object]:
            entered.set()
            assert self.released.wait(1.0)
            return {"Contents": [], "IsTruncated": False}

        def close(self) -> None:
            self.closed = True
            self.released.set()

    client = ReturnsAfterCloseS3()

    def create_client(*_args: object, **_kwargs: object) -> ReturnsAfterCloseS3:
        now[0] += 55.0
        return client

    def expire_after_call_starts(stop: threading.Event, timeout: float) -> bool:
        waiter_timeouts.append(timeout)
        assert entered.wait(1.0)
        return stop.is_set()

    config = make_config(tmp_path)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=Boto3MinioMirror(
            client_factory=create_client,
            timeout_seconds=60.0,
            monotonic=lambda: now[0],
            deadline_waiter=expire_after_call_starts,
            cancellation_grace_seconds=0.5,
        ),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="minio_snapshot_failed"):
        creator.create(make_request())

    assert waiter_timeouts == [pytest.approx(5.0)]
    assert client.closed
    backups_root = config.rollout_root / "backups"
    assert not (backups_root / "latest").exists()
    assert not list(backups_root.glob("*/backup-manifest.json"))


def test_minio_rejects_deadline_exhausted_during_client_setup(tmp_path: Path) -> None:
    now = [100.0]
    waiter_timeouts: list[float] = []

    class NeverListedS3(LifecycleS3):
        def __init__(self) -> None:
            super().__init__([])
            self.list_calls = 0

        def list_objects_v2(self, **_kwargs: str) -> dict[str, object]:
            self.list_calls += 1
            return {"Contents": [], "IsTruncated": False}

    client = NeverListedS3()

    def create_client(*_args: object, **_kwargs: object) -> NeverListedS3:
        now[0] += 60.0
        return client

    def wait_for_stop(stop: threading.Event, timeout: float) -> bool:
        waiter_timeouts.append(timeout)
        return stop.wait(1.0)

    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    mirror = Boto3MinioMirror(
        client_factory=create_client,
        timeout_seconds=60.0,
        monotonic=lambda: now[0],
        deadline_waiter=wait_for_stop,
    )

    with pytest.raises(ValueError, match="total deadline"):
        mirror.mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )

    assert client.list_calls == 0
    assert waiter_timeouts == []
    assert client.events == ["client_close"]


def test_minio_does_not_create_client_after_capacity_work_exhausts_deadline(
    tmp_path: Path,
) -> None:
    now = [100.0]
    client_factory_calls: list[str] = []
    client = LifecycleS3([])

    def expire_while_reading_inodes(_path: Path) -> int:
        now[0] = 160.0
        return 1_000_000

    def create_client(*_args: object, **_kwargs: object) -> LifecycleS3:
        client_factory_calls.append("create_client")
        return client

    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    mirror = Boto3MinioMirror(
        client_factory=create_client,
        timeout_seconds=60.0,
        disk_reserve_bytes=0,
        inode_reserve=0,
        available_bytes=lambda _path: 1_000_000,
        available_inodes=expire_while_reading_inodes,
        monotonic=lambda: now[0],
    )

    with pytest.raises(ValueError, match="total deadline"):
        mirror.mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )

    assert client_factory_calls == []
    assert client.events == []


@pytest.mark.parametrize("stage", ["list", "get", "read"])
def test_minio_rechecks_absolute_deadline_after_final_blocking_call(
    tmp_path: Path,
    stage: str,
) -> None:
    now = [100.0]

    class DeadlineAdvancingBody:
        def read(self, _amount: int) -> bytes:
            if stage == "read":
                now[0] += 61.0
            return b""

        def close(self) -> None:
            return None

    class DeadlineAdvancingS3:
        def __init__(self) -> None:
            self.list_calls = 0
            self.closed = False

        def list_objects_v2(self, **_kwargs: str) -> dict[str, object]:
            self.list_calls += 1
            if self.list_calls == 1:
                return {"Contents": [], "IsTruncated": False}
            if stage == "list":
                now[0] += 61.0
                return {"Contents": [], "IsTruncated": False}
            return {"Contents": [{"Key": "object.bin"}], "IsTruncated": False}

        def get_object(self, **_kwargs: str) -> dict[str, object]:
            if stage == "get":
                now[0] += 61.0
            return {"Body": DeadlineAdvancingBody(), "ContentLength": 0}

        def close(self) -> None:
            self.closed = True

    client = DeadlineAdvancingS3()
    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    mirror = Boto3MinioMirror(
        client_factory=lambda *_args, **_kwargs: client,
        timeout_seconds=60.0,
        monotonic=lambda: now[0],
        deadline_waiter=lambda stop, _timeout: stop.wait(1.0),
    )

    with pytest.raises(ValueError, match="total deadline"):
        mirror.mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )

    assert client.closed


def test_minio_closes_body_returned_by_get_after_absolute_deadline(
    tmp_path: Path,
) -> None:
    now = [100.0]
    events: list[str] = []

    class LateBody(FakeStreamingBody):
        def close(self) -> None:
            if not self.closed:
                events.append("body_close")
            super().close()

    class LateGetS3:
        def __init__(self) -> None:
            self.list_calls = 0
            self.body = LateBody(b"")
            self.closed = False

        def list_objects_v2(self, **_kwargs: str) -> dict[str, object]:
            self.list_calls += 1
            if self.list_calls == 1:
                return {"Contents": [], "IsTruncated": False}
            return {"Contents": [{"Key": "object.bin"}], "IsTruncated": False}

        def get_object(self, **_kwargs: str) -> dict[str, object]:
            now[0] += 61.0
            return {"Body": self.body, "ContentLength": 0}

        def close(self) -> None:
            events.append("client_close")
            self.closed = True

    client = LateGetS3()
    port_forward = DeadlinePortForward(events)
    mirror = Boto3MinioMirror(
        client_factory=lambda *_args, **_kwargs: client,
        timeout_seconds=60.0,
        monotonic=lambda: now[0],
        deadline_waiter=lambda stop, _timeout: stop.wait(1.0),
    )
    config = make_config(tmp_path)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(port_forward=port_forward),
        minio=mirror,
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="minio_snapshot_failed"):
        creator.create(make_request())

    assert client.body.closed
    assert client.closed
    assert events.index("body_close") < events.index("client_close")
    assert events.index("client_close") < events.index("terminate")
    assert events[-3:] == ["terminate", "wait:5.0", "port_forward_close"]
    backups_root = config.rollout_root / "backups"
    assert not (backups_root / "latest").exists()
    assert not list(backups_root.glob("*/backup-manifest.json"))


def test_minio_rechecks_absolute_deadline_after_client_cleanup(
    tmp_path: Path,
) -> None:
    now = [0.0]

    class CloseAdvancesDeadlineS3(OneObjectS3):
        def close(self) -> None:
            now[0] = 61.0
            events.append("client_close")
            super().close()

    events: list[str] = []
    client = CloseAdvancesDeadlineS3()
    config = make_config(tmp_path)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=Boto3MinioMirror(
            client_factory=lambda *_args, **_kwargs: client,
            timeout_seconds=60.0,
            monotonic=lambda: now[0],
            deadline_waiter=lambda stop, _timeout: stop.wait(1.0),
        ),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="minio_snapshot_failed"):
        creator.create(make_request())

    assert events == ["client_close"]
    backups_root = config.rollout_root / "backups"
    assert not (backups_root / "latest").exists()
    assert not list(backups_root.glob("*/backup-manifest.json"))


def test_boto_client_closes_before_port_forward_cleanup(tmp_path: Path) -> None:
    events: list[str] = []
    client = LifecycleS3(events)
    runner = RecordingRunner(port_forward=LifecyclePortForward(events))
    minio_root = tmp_path / "minio"
    minio_root.mkdir(mode=0o700)
    creator = BackupCreator(
        make_config(tmp_path),
        service_uid=os.getuid(),
        runner=runner,
        minio=Boto3MinioMirror(client_factory=lambda *_args, **_kwargs: client),
        now=lambda: FIXED_NOW,
    )

    local_port, stop_port_forward = creator._start_minio_transport()
    try:
        creator._mirror_minio(
            minio_root,
            local_port=local_port,
            stop_port_forward=stop_port_forward,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
        )
    finally:
        stop_port_forward()

    assert events.index("client_close") < events.index("terminate")


def test_boto_client_close_failure_is_fail_closed(tmp_path: Path) -> None:
    events: list[str] = []
    client = LifecycleS3(events, fail_close=True)
    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    mirror = Boto3MinioMirror(client_factory=lambda *_args, **_kwargs: client)

    with pytest.raises(RuntimeError, match="client close failed"):
        mirror.mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )

    assert events == ["client_close"]


def test_boto_watchdog_start_failure_still_closes_client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    client = LifecycleS3(events)
    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)

    def fail_start(_thread: object) -> None:
        raise RuntimeError("thread resources exhausted")

    monkeypatch.setattr(backup_module.threading.Thread, "start", fail_start)

    with pytest.raises(RuntimeError, match="thread resources exhausted"):
        Boto3MinioMirror(client_factory=lambda *_args, **_kwargs: client).mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )

    assert events == ["client_close"]


def test_boto_watchdog_constructor_failure_still_closes_client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    client = LifecycleS3(events)
    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)

    def fail_thread_constructor(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("thread resources exhausted")

    monkeypatch.setattr(
        backup_module.threading,
        "Thread",
        fail_thread_constructor,
    )

    with pytest.raises(RuntimeError, match="thread resources exhausted"):
        Boto3MinioMirror(client_factory=lambda *_args, **_kwargs: client).mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )

    assert events == ["client_close"]


def test_boto_closes_body_when_content_length_is_malformed(tmp_path: Path) -> None:
    events: list[str] = []

    class MalformedLengthS3(LifecycleS3):
        def __init__(self) -> None:
            super().__init__(events)
            self.body = FakeStreamingBody(b"never-read")

        def list_objects_v2(self, **_kwargs: str) -> dict[str, object]:
            return {"Contents": [{"Key": "object.bin"}], "IsTruncated": False}

        def get_object(self, **_kwargs: str) -> dict[str, object]:
            return {"Body": self.body, "ContentLength": "secret-invalid"}

    client = MalformedLengthS3()
    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)

    with pytest.raises(ValueError, match="content length is malformed"):
        Boto3MinioMirror(client_factory=lambda *_args, **_kwargs: client).mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )

    assert client.body.closed
    assert events == ["client_close"]


def test_boto_closes_body_when_read_accessor_raises(
    tmp_path: Path,
) -> None:
    events: list[str] = []

    class RaisingReadBody:
        def __init__(self) -> None:
            self.closed = False

        @property
        def read(self) -> object:
            raise RuntimeError("malformed read accessor")

        def close(self) -> None:
            if not self.closed:
                events.append("body_close")
                self.closed = True

    class RaisingReadS3:
        def __init__(self) -> None:
            self.body = RaisingReadBody()

        def list_objects_v2(self, **_kwargs: str) -> dict[str, object]:
            return {"Contents": [{"Key": "object.bin"}], "IsTruncated": False}

        def get_object(self, **_kwargs: str) -> dict[str, object]:
            return {"Body": self.body, "ContentLength": 0}

        def close(self) -> None:
            events.append("client_close")

    client = RaisingReadS3()
    port_forward = DeadlinePortForward(events)
    config = make_config(tmp_path)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(port_forward=port_forward),
        minio=Boto3MinioMirror(
            client_factory=lambda *_args, **_kwargs: client,
        ),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="minio_snapshot_failed"):
        creator.create(make_request())

    assert client.body.closed
    assert events.index("body_close") < events.index("client_close")
    assert events.index("client_close") < events.index("terminate")
    assert events[-3:] == ["terminate", "wait:5.0", "port_forward_close"]
    backups_root = config.rollout_root / "backups"
    assert not (backups_root / "latest").exists()
    assert not list(backups_root.glob("*/backup-manifest.json"))


def test_deadline_rejects_nominal_success_returned_after_expiry(
    tmp_path: Path,
) -> None:
    entered = threading.Event()

    class ReturnsAfterCloseS3:
        def __init__(self) -> None:
            self.released = threading.Event()
            self.closed = False

        def list_objects_v2(self, **_kwargs: str) -> dict[str, object]:
            entered.set()
            assert self.released.wait(1.0)
            return {"Contents": [], "IsTruncated": False}

        def close(self) -> None:
            self.closed = True
            self.released.set()

    client = ReturnsAfterCloseS3()

    def expire_after_call_starts(stop: threading.Event, _timeout: float) -> bool:
        assert entered.wait(1.0)
        return stop.is_set()

    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    mirror = Boto3MinioMirror(
        client_factory=lambda *_args, **_kwargs: client,
        deadline_waiter=expire_after_call_starts,
        cancellation_grace_seconds=0.5,
    )

    with pytest.raises(ValueError, match="total deadline"):
        mirror.mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )

    assert client.closed


def test_deadline_cleanup_failure_is_fail_closed_without_publication(
    tmp_path: Path,
) -> None:
    entered = threading.Event()

    class ReturnsAfterCloseS3:
        def __init__(self) -> None:
            self.released = threading.Event()

        def list_objects_v2(self, **_kwargs: str) -> dict[str, object]:
            entered.set()
            assert self.released.wait(1.0)
            return {"Contents": [], "IsTruncated": False}

        def close(self) -> None:
            self.released.set()

    class FailingCleanupPortForward(RecordingPortForward):
        def wait(self, timeout_seconds: float) -> bool:
            self.events.append(f"wait:{timeout_seconds}")
            return False

        def close(self) -> None:
            self.events.append("port_forward_close")
            raise RuntimeError("sensitive cleanup detail")

    def expire_after_call_starts(stop: threading.Event, _timeout: float) -> bool:
        assert entered.wait(1.0)
        return stop.is_set()

    client = ReturnsAfterCloseS3()
    port_forward = FailingCleanupPortForward()
    mirror = Boto3MinioMirror(
        client_factory=lambda *_args, **_kwargs: client,
        deadline_waiter=expire_after_call_starts,
        cancellation_grace_seconds=0.5,
    )
    config = make_config(tmp_path)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(port_forward=port_forward),
        minio=mirror,
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="minio_transport_cleanup_failed") as exc_info:
        creator.create(make_request())

    assert "sensitive" not in str(exc_info.value)
    assert port_forward.events[-5:] == [
        "terminate",
        "wait:5.0",
        "kill",
        "wait:5.0",
        "port_forward_close",
    ]
    backups_root = config.rollout_root / "backups"
    assert not (backups_root / "latest").exists()
    assert not list(backups_root.glob("*/backup-manifest.json"))


def test_deadline_waits_for_in_progress_exact_port_forward_cleanup(
    tmp_path: Path,
) -> None:
    entered = threading.Event()

    class ReturnsAfterCloseS3:
        def __init__(self) -> None:
            self.released = threading.Event()

        def list_objects_v2(self, **_kwargs: str) -> dict[str, object]:
            entered.set()
            assert self.released.wait(1.0)
            return {"Contents": [], "IsTruncated": False}

        def close(self) -> None:
            self.released.set()

    class SlowCleanupPortForward(RecordingPortForward):
        def __init__(self) -> None:
            super().__init__()
            self.cleanup_complete = threading.Event()

        def wait(self, timeout_seconds: float) -> bool:
            self.events.append(f"wait:{timeout_seconds}")
            time.sleep(0.2)
            return True

        def close(self) -> None:
            self.cleanup_complete.set()

    def expire_after_call_starts(stop: threading.Event, _timeout: float) -> bool:
        assert entered.wait(1.0)
        return stop.is_set()

    port_forward = SlowCleanupPortForward()
    config = make_config(tmp_path)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(port_forward=port_forward),
        minio=Boto3MinioMirror(
            client_factory=lambda *_args, **_kwargs: ReturnsAfterCloseS3(),
            deadline_waiter=expire_after_call_starts,
            cancellation_grace_seconds=0.01,
        ),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="minio_snapshot_failed"):
        creator.create(make_request())

    assert port_forward.cleanup_complete.is_set()
    assert not (config.rollout_root / "backups" / "latest").exists()


class EndlessListingS3(LifecycleS3):
    def __init__(self, events: list[str]) -> None:
        super().__init__(events)
        self.list_calls = 0

    def list_objects_v2(self, **_kwargs: str) -> dict[str, object]:
        self.list_calls += 1
        return {
            "Contents": [],
            "IsTruncated": True,
            "NextContinuationToken": f"page-{self.list_calls}",
        }


def test_boto_mirror_has_total_page_bound(tmp_path: Path) -> None:
    events: list[str] = []
    client = EndlessListingS3(events)
    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    mirror = Boto3MinioMirror(
        client_factory=lambda *_args, **_kwargs: client,
        max_pages=2,
        max_objects=10,
        max_total_bytes=100_000,
        timeout_seconds=60.0,
        disk_reserve_bytes=0,
        available_bytes=lambda _path: 1_000_000,
        monotonic=lambda: 0.0,
    )

    with pytest.raises(ValueError, match="page limit"):
        mirror.mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )

    assert client.list_calls == 2
    assert events == ["client_close"]


def test_boto_mirror_has_total_deadline(tmp_path: Path) -> None:
    events: list[str] = []
    client = LifecycleS3(events)
    ticks = iter([0.0, 61.0])
    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    mirror = Boto3MinioMirror(
        client_factory=lambda *_args, **_kwargs: client,
        max_pages=2,
        max_objects=10,
        max_total_bytes=100,
        timeout_seconds=60.0,
        disk_reserve_bytes=0,
        available_bytes=lambda _path: 1000,
        monotonic=lambda: next(ticks),
    )

    with pytest.raises(ValueError, match="deadline"):
        mirror.mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )

    assert events == []


def test_boto_mirror_rejects_declared_bytes_over_disk_aware_budget(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    values = os.statvfs(destination)
    block_size = values.f_frsize or values.f_bsize

    class OversizedDeclaredObjectS3(LifecycleS3):
        def __init__(self) -> None:
            super().__init__(events)
            self.body = FakeStreamingBody(b"x")

        def list_objects_v2(self, **_kwargs: str) -> dict[str, object]:
            return {"Contents": [{"Key": "object.bin"}], "IsTruncated": False}

        def get_object(self, **_kwargs: str) -> dict[str, object]:
            return {"Body": self.body, "ContentLength": block_size + 1}

    client = OversizedDeclaredObjectS3()
    mirror = Boto3MinioMirror(
        client_factory=lambda *_args, **_kwargs: client,
        max_total_bytes=100_000,
        disk_reserve_bytes=block_size,
        available_bytes=lambda _path: 2 * block_size,
    )

    with pytest.raises(ValueError, match="byte limit"):
        mirror.mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )

    assert client.body.closed
    assert not any(path.is_file() for path in destination.rglob("*"))
    assert events == ["client_close"]


def test_boto_mirror_object_count_warns_and_continues(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    events: list[str] = []

    class TooManyObjectsS3(LifecycleS3):
        def list_objects_v2(self, **_kwargs: str) -> dict[str, object]:
            return {
                "Contents": [{"Key": "first.bin"}, {"Key": "second.bin"}],
                "IsTruncated": False,
            }

        def get_object(self, **_kwargs: str) -> dict[str, object]:
            body = FakeStreamingBody(b"x")
            return {"Body": body, "ContentLength": 1}

    client = TooManyObjectsS3(events)
    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    mirror = Boto3MinioMirror(
        client_factory=lambda *_args, **_kwargs: client,
        max_objects=1,
        disk_reserve_bytes=0,
        available_bytes=lambda _path: 100_000,
    )

    with caplog.at_level(logging.WARNING, logger="loom_cli.rollout.operator.backup"):
        mirror.mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )

    # Object count is a caution signal, not a hard bound: crossing the threshold
    # does not abort the backup, so every object is still mirrored in full.
    assert (destination / "loom-staging-trajectories" / "first.bin").read_bytes() == b"x"
    assert (destination / "loom-staging-trajectories" / "second.bin").read_bytes() == b"x"
    # The caution surfaces exactly once as a warning.
    cautions = [record for record in caplog.records if "caution threshold" in record.getMessage()]
    assert len(cautions) == 1
    assert cautions[0].levelno == logging.WARNING
    assert events == ["client_close"]


def test_reviewed_policy_crosses_old_99996_object_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class LargeListingS3(LifecycleS3):
        def list_objects_v2(self, **kwargs: str) -> dict[str, object]:
            if kwargs["Bucket"] == "loom-staging-artifacts":
                return {"Contents": [], "IsTruncated": False}
            page = int(kwargs.get("ContinuationToken", "0"))
            first = page * 1000
            remaining = 100_001 - first
            count = min(1000, remaining)
            truncated = remaining > count
            result: dict[str, object] = {
                "Contents": [{"Key": f"object-{first + offset:06d}"} for offset in range(count)],
                "IsTruncated": truncated,
            }
            if truncated:
                result["NextContinuationToken"] = str(page + 1)
            return result

    client = LargeListingS3(events)
    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    config = make_config(tmp_path)
    mirror = Boto3MinioMirror(
        client_factory=lambda *_args, **_kwargs: client,
        max_objects=config.backup_max_objects,
        disk_reserve_bytes=0,
        inode_reserve=0,
        available_bytes=lambda _path: 1024**4,
        available_inodes=lambda _path: 2_000_000,
    )
    monkeypatch.setattr(backup_module, "_stream_s3_object", lambda *_args, **_kwargs: None)

    mirror.mirror(
        endpoint_url="http://127.0.0.1:19000",
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
        destination=destination,
    )

    assert config.backup_max_objects == 1_000_000
    assert events == ["client_close"]


def test_stage_maps_code_to_durable_public_reason() -> None:
    def boom() -> None:
        raise ValueError("secret-bearing detail must not leak")

    for code, expected in (
        ("postgres_dump_failed", "backup_postgres_failed"),
        ("minio_snapshot_failed", "backup_minio_failed"),
        ("minio_credentials_failed", "backup_minio_failed"),
        ("secret_export_failed", "backup_secrets_failed"),
        ("manifest_write_failed", "backup_manifest_failed"),
        ("manifest_publish_failed", "backup_manifest_failed"),
        ("backup_capacity_unavailable", "backup_capacity_exhausted"),
        ("minio_bucket_config_invalid", "backup_config_invalid"),
        ("backup_clock_invalid", "backup_precondition_failed"),
    ):
        with pytest.raises(backup_module.BackupError) as exc_info:
            backup_module._stage(code, boom)
        assert exc_info.value.code == code
        assert exc_info.value.public_reason == expected
        # The stage discards the underlying secret-bearing cause.
        assert "secret-bearing detail" not in str(exc_info.value)


def test_stage_unknown_code_defaults_to_backup_failed() -> None:
    def boom() -> None:
        raise RuntimeError("x")

    with pytest.raises(backup_module.BackupError) as exc_info:
        backup_module._stage("some_unmapped_code", boom)
    assert exc_info.value.public_reason == "backup_failed"


def test_stage_reraises_specific_backup_error_unchanged() -> None:
    def limit() -> None:
        raise backup_module.BackupPolicyLimitError(
            "minio_object_limit_exceeded",
            public_reason="backup_object_limit_exceeded",
            message="MinIO mirror exceeded object limit",
        )

    with pytest.raises(backup_module.BackupError) as exc_info:
        backup_module._stage("postgres_dump_failed", limit)
    # An already-coded BackupError propagates with its own reason, not the
    # wrapping stage's reason.
    assert exc_info.value.public_reason == "backup_object_limit_exceeded"


def test_all_stage_public_reasons_are_approved() -> None:
    for reason in backup_module._STAGE_PUBLIC_REASONS.values():
        assert reason in backup_module._BACKUP_PUBLIC_REASONS


def test_backup_public_reason_literal_matches_approved_event_tokens() -> None:
    # The Literal type, the backup module's runtime set, and the event-model
    # allowlist must all agree, or a raised reason gets rejected at append time.
    from typing import get_args

    from loom_cli.rollout.operator.model import APPROVED_BACKUP_EVENT_REASONS

    assert set(get_args(backup_module.BackupPublicReason)) == APPROVED_BACKUP_EVENT_REASONS
    assert backup_module._BACKUP_PUBLIC_REASONS == APPROVED_BACKUP_EVENT_REASONS


def _incomplete_bundle(tmp_path: Path) -> tuple[BackupCreator, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    config = make_config(tmp_path)
    backups = config.rollout_root / "backups"
    backups.mkdir(mode=0o700)
    bundle = backups / "20260713T200000Z-stg-20260713-abcdef12"
    bundle.mkdir(mode=0o700)
    component = bundle / "minio"
    component.mkdir(mode=0o700)
    payload = component / "object.bin"
    payload.write_bytes(b"payload")
    payload.chmod(0o600)
    return BackupCreator(config, service_uid=os.geteuid()), bundle


def test_cleanup_incomplete_is_request_scoped_and_idempotent(tmp_path: Path) -> None:
    creator, bundle = _incomplete_bundle(tmp_path)

    assert creator.cleanup_incomplete("stg-20260713-abcdef12") is True
    assert not bundle.exists()
    assert creator.cleanup_incomplete("stg-20260713-abcdef12") is False


def test_cleanup_incomplete_refuses_manifest_backed_or_latest_bundle(tmp_path: Path) -> None:
    manifest_creator, manifest_bundle = _incomplete_bundle(tmp_path / "manifest")
    manifest = manifest_bundle / "backup-manifest.json"
    manifest.write_bytes(b"{}")
    manifest.chmod(0o600)

    with pytest.raises(BackupError, match="backup_cleanup_failed"):
        manifest_creator.cleanup_incomplete("stg-20260713-abcdef12")
    assert manifest_bundle.exists()

    latest_creator, latest_bundle = _incomplete_bundle(tmp_path / "latest")
    (latest_bundle.parent / "latest").symlink_to(latest_bundle.name)

    with pytest.raises(BackupError, match="backup_cleanup_failed"):
        latest_creator.cleanup_incomplete("stg-20260713-abcdef12")
    assert latest_bundle.exists()


def test_retire_payload_requires_exact_manifest_and_refuses_latest(tmp_path: Path) -> None:
    creator, bundle = _incomplete_bundle(tmp_path)
    manifest = bundle / "backup-manifest.json"
    manifest.write_bytes(b"{}\n")
    manifest.chmod(0o600)
    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()

    with pytest.raises(BackupError, match="backup_retirement_failed"):
        creator.retire_payload(
            "stg-20260713-abcdef12",
            bundle_name=bundle.name,
            expected_manifest_sha256="f" * 64,
        )
    assert bundle.exists()

    (bundle.parent / "latest").symlink_to(bundle.name)
    with pytest.raises(BackupError, match="backup_retirement_failed"):
        creator.retire_payload(
            "stg-20260713-abcdef12",
            bundle_name=bundle.name,
            expected_manifest_sha256=digest,
        )
    (bundle.parent / "latest").unlink()

    assert creator.retire_payload(
        "stg-20260713-abcdef12",
        bundle_name=bundle.name,
        expected_manifest_sha256=digest,
    )
    assert not bundle.exists()
    assert not creator.retire_payload(
        "stg-20260713-abcdef12",
        bundle_name=bundle.name,
        expected_manifest_sha256=digest,
    )


@pytest.mark.parametrize("unsafe_kind", ["symlink", "hardlink", "fifo"])
def test_cleanup_incomplete_refuses_unsafe_entries(
    tmp_path: Path,
    unsafe_kind: str,
) -> None:
    creator, bundle = _incomplete_bundle(tmp_path)
    unsafe = bundle / "unsafe"
    if unsafe_kind == "symlink":
        unsafe.symlink_to("/tmp")
    elif unsafe_kind == "hardlink":
        source = bundle / "minio" / "object.bin"
        os.link(source, unsafe)
    else:
        os.mkfifo(unsafe, mode=0o600)

    with pytest.raises(BackupError, match="backup_cleanup_failed"):
        creator.cleanup_incomplete("stg-20260713-abcdef12")
    assert bundle.exists()


@pytest.mark.parametrize("late_guard", ["manifest", "latest"])
def test_cleanup_rechecks_publication_guards_before_first_removal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    late_guard: str,
) -> None:
    creator, bundle = _incomplete_bundle(tmp_path)
    original_validate = backup_module._validate_cleanup_directory

    def inject_guard(*args: object, **kwargs: object) -> None:
        original_validate(*args, **kwargs)  # type: ignore[arg-type]
        if late_guard == "manifest":
            manifest = bundle / "backup-manifest.json"
            manifest.write_bytes(b"{}")
            manifest.chmod(0o600)
        else:
            (bundle.parent / "latest").symlink_to(bundle.name)

    monkeypatch.setattr(backup_module, "_validate_cleanup_directory", inject_guard)

    with pytest.raises(BackupError, match="backup_cleanup_failed"):
        creator.cleanup_incomplete("stg-20260713-abcdef12")

    assert (bundle / "minio" / "object.bin").read_bytes() == b"payload"
    assert bundle.exists()


def test_interrupted_cleanup_is_safely_retryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    creator, bundle = _incomplete_bundle(tmp_path)
    extra = bundle / "extra.bin"
    extra.write_bytes(b"extra")
    extra.chmod(0o600)
    original_unlink = backup_module.os.unlink
    interrupted = False

    def unlink_then_interrupt(path: str, *, dir_fd: int | None = None) -> None:
        nonlocal interrupted
        original_unlink(path, dir_fd=dir_fd)
        if not interrupted:
            interrupted = True
            raise OSError("simulated cleanup interruption")

    monkeypatch.setattr(backup_module.os, "unlink", unlink_then_interrupt)
    with pytest.raises(BackupError, match="backup_cleanup_failed"):
        creator.cleanup_incomplete("stg-20260713-abcdef12")
    assert bundle.exists()

    monkeypatch.setattr(backup_module.os, "unlink", original_unlink)
    assert creator.cleanup_incomplete("stg-20260713-abcdef12") is True
    assert not bundle.exists()


def test_cleanup_refuses_bundle_root_on_another_filesystem(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    creator, bundle = _incomplete_bundle(tmp_path)
    original_stat = backup_module.os.stat
    actual = bundle.lstat()
    altered_values = list(actual)
    altered_values[2] = actual.st_dev + 1
    altered = os.stat_result(altered_values)

    def cross_device_stat(
        path: object,
        *,
        dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> os.stat_result:
        if path == bundle.name and dir_fd is not None and not follow_symlinks:
            return altered
        return original_stat(path, dir_fd=dir_fd, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(backup_module.os, "stat", cross_device_stat)

    with pytest.raises(BackupError, match="backup_cleanup_failed"):
        creator.cleanup_incomplete("stg-20260713-abcdef12")
    assert (bundle / "minio" / "object.bin").read_bytes() == b"payload"


@pytest.mark.parametrize("limit_kind", ["files", "entries", "bytes", "depth", "directory"])
def test_cleanup_enforces_traversal_bounds_before_removal(
    tmp_path: Path,
    limit_kind: str,
) -> None:
    original, bundle = _incomplete_bundle(tmp_path)
    extra = bundle / "extra.bin"
    extra.write_bytes(b"extra")
    extra.chmod(0o600)
    kwargs: dict[str, int] = {}
    if limit_kind == "files":
        kwargs["max_files"] = 1
    elif limit_kind == "entries":
        kwargs["max_entries"] = 1
    elif limit_kind == "bytes":
        kwargs["max_total_bytes"] = 1
    elif limit_kind == "depth":
        kwargs["max_depth"] = 0
    else:
        kwargs["max_directory_entries"] = 1
    creator = BackupCreator(
        original.config,
        service_uid=os.geteuid(),
        traversal_limits=BackupTraversalLimits(**kwargs),
    )

    with pytest.raises(BackupError, match="backup_cleanup_failed"):
        creator.cleanup_incomplete("stg-20260713-abcdef12")

    assert (bundle / "minio" / "object.bin").read_bytes() == b"payload"
    assert extra.read_bytes() == b"extra"


def test_cleanup_exact_bundle_binding_preserves_same_request_decoy(tmp_path: Path) -> None:
    creator, bundle = _incomplete_bundle(tmp_path)
    decoy = bundle.parent / "20260713T210000Z-stg-20260713-abcdef12"
    decoy.mkdir(mode=0o700)
    decoy_file = decoy / "keep.bin"
    decoy_file.write_bytes(b"keep")
    decoy_file.chmod(0o600)

    assert (
        creator.cleanup_incomplete(
            "stg-20260713-abcdef12",
            bundle_name=bundle.name,
        )
        is True
    )
    assert not bundle.exists()
    assert decoy_file.read_bytes() == b"keep"


def test_cleanup_rejects_mismatched_or_ambiguous_root_binding(tmp_path: Path) -> None:
    creator, bundle = _incomplete_bundle(tmp_path)
    decoy = bundle.parent / "20260713T210000Z-stg-20260713-abcdef12"
    decoy.mkdir(mode=0o700)

    with pytest.raises(BackupError, match="backup_cleanup_failed"):
        creator.cleanup_incomplete(
            "stg-20260713-abcdef12",
            bundle_name="20260713T200000Z-req-another",
        )
    with pytest.raises(BackupError, match="backup_cleanup_failed"):
        creator.cleanup_incomplete("stg-20260713-abcdef12")

    assert (bundle / "minio" / "object.bin").read_bytes() == b"payload"
    assert decoy.exists()


def test_cleanup_uses_one_elapsed_deadline_across_discovery_and_validation(
    tmp_path: Path,
) -> None:
    original, bundle = _incomplete_bundle(tmp_path)
    ticks = iter((0.0, 0.25, 1.0))
    creator = BackupCreator(
        original.config,
        service_uid=os.geteuid(),
        traversal_limits=BackupTraversalLimits(
            max_elapsed_seconds=0.5,
            monotonic=lambda: next(ticks, 1.0),
        ),
    )

    with pytest.raises(BackupError, match="backup_cleanup_failed"):
        creator.cleanup_incomplete("stg-20260713-abcdef12")
    assert (bundle / "minio" / "object.bin").read_bytes() == b"payload"


def test_boto_mirror_has_disk_aware_inode_bound(tmp_path: Path) -> None:
    events: list[str] = []

    class NestedObjectS3(LifecycleS3):
        def list_objects_v2(self, **_kwargs: str) -> dict[str, object]:
            return {"Contents": [{"Key": "one/two/object.bin"}], "IsTruncated": False}

        def get_object(self, **_kwargs: str) -> dict[str, object]:
            raise AssertionError("inode-bounded key reached object retrieval")

    client = NestedObjectS3(events)
    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    mirror = Boto3MinioMirror(
        client_factory=lambda *_args, **_kwargs: client,
        inode_reserve=0,
        available_inodes=lambda _path: 3,
    )

    with pytest.raises(ValueError, match="inode limit"):
        mirror.mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )

    assert not any(path.is_file() for path in destination.rglob("*"))
    assert events == ["client_close"]


def test_boto_mirror_stops_on_body_larger_than_declared_length(
    tmp_path: Path,
) -> None:
    events: list[str] = []

    class LargerBody(FakeStreamingBody):
        def read(self, amount: int) -> bytes:
            if self._offset:
                raise AssertionError("mirror continued after declared length")
            return super().read(amount)

    class LargerBodyS3(LifecycleS3):
        def __init__(self) -> None:
            super().__init__(events)
            self.body = LargerBody(b"xx")

        def list_objects_v2(self, **_kwargs: str) -> dict[str, object]:
            return {"Contents": [{"Key": "object.bin"}], "IsTruncated": False}

        def get_object(self, **_kwargs: str) -> dict[str, object]:
            return {"Body": self.body, "ContentLength": 1}

    client = LargerBodyS3()
    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    mirror = Boto3MinioMirror(client_factory=lambda *_args, **_kwargs: client)

    with pytest.raises(ValueError, match="body length"):
        mirror.mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )

    assert client.body.closed
    assert not any(path.is_file() for path in destination.rglob("*"))
    assert events == ["client_close"]


@pytest.mark.parametrize(
    "key",
    [
        "../escape",
        "/absolute",
        "nested//object",
        "nested/./object",
        "nested/../object",
        "nested\\object",
        "nul\x00object",
        "control\x1fobject",
    ],
)
def test_boto_mirror_rejects_hostile_object_keys_without_partial_files(
    tmp_path: Path,
    key: str,
) -> None:
    events: list[str] = []

    class HostileKeyS3(LifecycleS3):
        def list_objects_v2(self, **_kwargs: str) -> dict[str, object]:
            return {"Contents": [{"Key": key}], "IsTruncated": False}

        def get_object(self, **_kwargs: str) -> dict[str, object]:
            raise AssertionError("unsafe key reached object retrieval")

    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    client = HostileKeyS3(events)
    mirror = Boto3MinioMirror(client_factory=lambda *_args, **_kwargs: client)

    with pytest.raises(ValueError, match="safe relative path"):
        mirror.mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )

    assert not any(path.is_file() for path in destination.rglob("*"))
    assert events == ["client_close"]


@pytest.mark.parametrize("failure", ["short_body", "read_error"])
def test_boto_mirror_removes_partial_object_on_stream_failure(
    tmp_path: Path,
    failure: str,
) -> None:
    events: list[str] = []

    class BrokenBody(FakeStreamingBody):
        def read(self, amount: int) -> bytes:
            if failure == "read_error":
                raise OSError("stream failed")
            return super().read(amount)

    class BrokenObjectS3(LifecycleS3):
        def __init__(self) -> None:
            super().__init__(events)
            self.body = BrokenBody(b"x")

        def list_objects_v2(self, **_kwargs: str) -> dict[str, object]:
            return {"Contents": [{"Key": "object.bin"}], "IsTruncated": False}

        def get_object(self, **_kwargs: str) -> dict[str, object]:
            return {"Body": self.body, "ContentLength": 2}

    destination = tmp_path / "minio"
    destination.mkdir(mode=0o700)
    client = BrokenObjectS3()
    mirror = Boto3MinioMirror(client_factory=lambda *_args, **_kwargs: client)

    with pytest.raises((OSError, ValueError)):
        mirror.mirror(
            endpoint_url="http://127.0.0.1:19000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            buckets=("loom-staging-trajectories", "loom-staging-artifacts"),
            destination=destination,
        )

    assert client.body.closed
    assert not any(path.is_file() for path in destination.rglob("*"))
    assert not list(destination.rglob("*.part"))
    assert events == ["client_close"]


@pytest.mark.parametrize(
    ("payload", "expected_code"),
    [
        ("kind: ConfigMap\nmetadata:\n  name: loom-secrets\n", "secret_export_failed"),
        ("kind: Secret\nmetadata:\n  name: wrong-name\n", "secret_export_failed"),
    ],
)
def test_secret_export_rejects_wrong_kind_or_name_without_publication(
    tmp_path: Path,
    payload: str,
    expected_code: str,
) -> None:
    class InvalidSecretRunner(RecordingRunner):
        def capture_stdout(
            self,
            argv: Sequence[str],
            *,
            env: Mapping[str, str],
            timeout_seconds: float | None = None,
        ) -> bytes:
            rendered = list(argv)
            if rendered[-2:] == ["-o", "yaml"]:
                self._record(argv, env)
                self.timeouts.append(timeout_seconds)
                return payload.encode("utf-8")
            return super().capture_stdout(
                argv,
                env=env,
                timeout_seconds=timeout_seconds,
            )

    config = make_config(tmp_path)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=InvalidSecretRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match=expected_code):
        creator.create(make_request())

    backup_root = config.rollout_root / "backups"
    assert not (backup_root / "latest").exists()
    assert list(backup_root.glob("*/backup-manifest.json")) == []


def test_creator_fsyncs_complete_component_tree_before_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[Path] = []
    original = backup_module._fsync_private_tree

    def recording_fsync_tree(root: Path) -> None:
        calls.append(root)
        original(root)

    monkeypatch.setattr(backup_module, "_fsync_private_tree", recording_fsync_tree)
    config = make_config(tmp_path)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    creator.create(make_request())

    assert calls == [config.rollout_root / "backups" / "20260713T200000Z-stg-20260713-abcdef12"]


@pytest.mark.parametrize("latest_kind", ["absolute", "traversal", "missing", "regular"])
def test_invalid_existing_latest_is_not_overwritten(
    tmp_path: Path,
    latest_kind: str,
) -> None:
    config = make_config(tmp_path)
    backups_root = config.rollout_root / "backups"
    backups_root.mkdir(mode=0o700)
    latest = backups_root / "latest"
    if latest_kind == "regular":
        latest.write_bytes(b"do-not-overwrite")
        original: str | bytes = latest.read_bytes()
    else:
        target = {
            "absolute": str(tmp_path / "outside"),
            "traversal": "../outside",
            "missing": "missing-snapshot",
        }[latest_kind]
        latest.symlink_to(target)
        original = os.readlink(latest)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="latest_publish_failed"):
        creator.create(make_request())

    if latest_kind == "regular":
        assert latest.read_bytes() == original
    else:
        assert latest.is_symlink()
        assert os.readlink(latest) == original
    failed_root = backups_root / "20260713T200000Z-stg-20260713-abcdef12"
    assert not (failed_root / "backup-manifest.json").exists()


def test_existing_acl_managed_backups_parent_preserves_owner_and_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    backups_root = config.rollout_root / "backups"
    backups_root.mkdir(mode=0o770)
    backups_root.chmod(0o770)
    original_fstat = backup_module.os.fstat
    backups_identity = backups_root.stat()
    trusted_owner_uid = os.getuid() + 1000

    def trusted_parent_fstat(fd: int) -> os.stat_result:
        metadata = original_fstat(fd)
        if metadata.st_dev != backups_identity.st_dev or metadata.st_ino != backups_identity.st_ino:
            return metadata
        values = list(metadata)
        values[0] = (metadata.st_mode & ~0o777) | 0o770
        values[4] = trusted_owner_uid
        return os.stat_result(values)

    monkeypatch.setattr(backup_module.os, "fstat", trusted_parent_fstat)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    backup = creator.create(make_request())

    assert backups_root.stat().st_mode & 0o777 == 0o770
    assert backup.manifest_path.parent.stat().st_mode & 0o777 == 0o700
    assert backup.manifest_path.parent.stat().st_uid == os.getuid()


def test_existing_world_writable_backups_parent_is_rejected_before_commands(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    backups_root = config.rollout_root / "backups"
    backups_root.mkdir(mode=0o777)
    backups_root.chmod(0o777)
    runner = RecordingRunner()
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=runner,
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="backup_root_create_failed"):
        creator.create(make_request())

    assert backups_root.stat().st_mode & 0o777 == 0o777
    assert list(backups_root.iterdir()) == []
    assert runner.argvs == []


@pytest.mark.parametrize("mode", [0o770, 0o755, 0o775])
def test_existing_service_owned_unapproved_backups_parent_is_rejected(
    tmp_path: Path,
    mode: int,
) -> None:
    config = make_config(tmp_path)
    backups_root = config.rollout_root / "backups"
    backups_root.mkdir(mode=mode)
    backups_root.chmod(mode)
    runner = RecordingRunner()
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=runner,
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    with pytest.raises(BackupError, match="backup_root_create_failed"):
        creator.create(make_request())

    assert backups_root.stat().st_mode & 0o777 == mode
    assert list(backups_root.iterdir()) == []
    assert runner.argvs == []


def test_new_backups_parent_is_forced_to_private_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_mkdir = backup_module.os.mkdir

    def restrictive_mkdir(
        path: object,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        if path in {"backups", "20260713T200000Z-stg-20260713-abcdef12"}:
            mode = 0
        original_mkdir(path, mode, dir_fd=dir_fd)  # type: ignore[arg-type]

    monkeypatch.setattr(backup_module.os, "mkdir", restrictive_mkdir)
    config = make_config(tmp_path)
    creator = BackupCreator(
        config,
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        minio=SuccessfulMinioMirror(),
        now=lambda: FIXED_NOW,
    )

    backup = creator.create(make_request())

    assert backup.manifest_path.is_file()
    assert (config.rollout_root / "backups").stat().st_mode & 0o777 == 0o700
    assert backup.manifest_path.parent.stat().st_mode & 0o777 == 0o700
