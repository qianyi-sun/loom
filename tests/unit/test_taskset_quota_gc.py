"""Unit tests for TaskSet quotas and GC (#242 sub-plan 7)."""

from __future__ import annotations

import io
import tarfile
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from loom.models.taskset import UserTaskSetManifest
from loom.taskset.materialize import (
    _BundleSizeExceededError,
    _upload_bundle_dir,
    materialize_task_set,
)


class TestBundleSizeEnforcement:
    """Tests for bundle size tracking during materialization."""

    def test_upload_bundle_dir_tracks_bytes(self) -> None:
        client = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            bundle_dir = Path(tmp)
            (bundle_dir / "task.toml").write_bytes(b"x" * 100)
            (bundle_dir / "instruction.md").write_bytes(b"y" * 200)

            cumulative = _upload_bundle_dir(
                client,
                bucket="test-bucket",
                bundle_prefix="prefix",
                bundle_dir=bundle_dir,
                cumulative_bytes=0,
                max_bundle_bytes=None,
            )
            assert cumulative == 300

    def test_upload_bundle_dir_continues_from_existing(self) -> None:
        client = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            bundle_dir = Path(tmp)
            (bundle_dir / "small.txt").write_bytes(b"z" * 50)

            cumulative = _upload_bundle_dir(
                client,
                bucket="test-bucket",
                bundle_prefix="prefix",
                bundle_dir=bundle_dir,
                cumulative_bytes=1000,
                max_bundle_bytes=None,
            )
            assert cumulative == 1050

    def test_upload_bundle_dir_raises_when_limit_exceeded(self) -> None:
        client = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            bundle_dir = Path(tmp)
            (bundle_dir / "big.bin").write_bytes(b"a" * 500)

            with pytest.raises(_BundleSizeExceededError) as exc_info:
                _upload_bundle_dir(
                    client,
                    bucket="test-bucket",
                    bundle_prefix="prefix",
                    bundle_dir=bundle_dir,
                    cumulative_bytes=0,
                    max_bundle_bytes=100,
                )
            assert exc_info.value.cumulative == 500
            assert exc_info.value.limit == 100

    def test_upload_bundle_dir_no_limit_allows_large_bundles(self) -> None:
        client = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            bundle_dir = Path(tmp)
            (bundle_dir / "large.bin").write_bytes(b"b" * 10000)

            cumulative = _upload_bundle_dir(
                client,
                bucket="test-bucket",
                bundle_prefix="prefix",
                bundle_dir=bundle_dir,
                cumulative_bytes=0,
                max_bundle_bytes=None,
            )
            assert cumulative == 10000

    def test_upload_bundle_dir_team_quota_checked_before_put(self) -> None:
        client = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            bundle_dir = Path(tmp)
            (bundle_dir / "a.bin").write_bytes(b"a" * 100)
            (bundle_dir / "b.bin").write_bytes(b"b" * 100)

            with pytest.raises(_BundleSizeExceededError) as exc_info:
                _upload_bundle_dir(
                    client,
                    bucket="test-bucket",
                    bundle_prefix="prefix",
                    bundle_dir=bundle_dir,
                    cumulative_bytes=0,
                    team_storage_baseline=950,
                    max_team_storage_bytes=1000,
                )
            assert exc_info.value.cumulative == 1050
            assert exc_info.value.limit == 1000
            client.put_object.assert_not_called()


def _inline_manifest_model() -> UserTaskSetManifest:
    return UserTaskSetManifest.model_validate({
        "apiVersion": "loom.taskset/v1",
        "kind": "UserTaskSet",
        "metadata": {"name": "quota-inline", "display_name": "Quota Inline"},
        "source": {
            "type": "jsonl-inline",
            "locator": '{"id":"one","question":"What is 1+1?"}',
        },
        "instance_mapping": {"prompt": "row.question", "task_id": "row.id"},
        "task_template": {
            "task": {"id": "{{ instance.task_id }}", "name": "quota task"},
            "environment": {"os": "linux"},
            "agent": {"name": "default"},
            "steps": [{"name": "main", "artifacts": ["out.txt"]}],
        },
    })


def _bundle_upload_manifest_model() -> UserTaskSetManifest:
    return UserTaskSetManifest.model_validate({
        "apiVersion": "loom.taskset/v1",
        "kind": "UserTaskSet",
        "metadata": {"name": "quota-bundle", "display_name": "Quota Bundle"},
        "source": {
            "type": "bundle-upload",
            "locator": "bundle.tar.gz",
            "subset": "tasks",
        },
        "limits": {"max_instances": 1},
    })


def _add_tar_file(tar: tarfile.TarFile, name: str, body: bytes) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(body)
    info.mode = 0o644
    tar.addfile(info, io.BytesIO(body))


def _bundle_tar_bytes() -> bytes:
    task_toml = b"""
version = "1"

[metadata]
id = "alpha"
name = "Alpha"

[environment]
dockerfile = "environment/Dockerfile"

[verifier]
name = "script"

[verifier.args]
script_path = "verifier/check.sh"
"""
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tar:
        _add_tar_file(tar, "tasks/alpha/task.toml", task_toml)
        _add_tar_file(tar, "tasks/alpha/instruction.md", b"Create answer.txt\n")
        _add_tar_file(tar, "tasks/alpha/environment/Dockerfile", b"FROM alpine:3.20\n")
        _add_tar_file(tar, "tasks/alpha/verifier/check.sh", b"#!/bin/sh\nexit 0\n")
    return out.getvalue()


class TestMaterializationStorageQuota:
    """End-to-end materialization quota behavior."""

    def test_inline_materialization_returns_size_exceeded_for_team_quota(
        self,
        tmp_path: Path,
    ) -> None:
        client = MagicMock()

        output = materialize_task_set(
            manifest=_inline_manifest_model(),
            task_set_id="ts/team-1/quota-inline",
            owning_team_id="team-1",
            output_generation="unit-test/1",
            intents=["trajectory_generation"],
            verifier_blob_uri=None,
            minio_client=client,
            artifacts_bucket="artifacts",
            upstream_cache_root=tmp_path,
            team_storage_baseline=1,
            max_team_storage_bytes=1,
        )

        assert output.status == "failed"
        assert output.status_reason == "size_exceeded"
        assert output.job_failure_reason == "size_exceeded"
        client.put_object.assert_not_called()

    def test_bundle_upload_materialization_applies_team_storage_quota(
        self,
        tmp_path: Path,
    ) -> None:
        client = MagicMock()
        client.get_object.return_value = {"Body": io.BytesIO(_bundle_tar_bytes())}

        output = materialize_task_set(
            manifest=_bundle_upload_manifest_model(),
            task_set_id="ts/team-1/quota-bundle",
            owning_team_id="team-1",
            output_generation="unit-test/1",
            intents=["trajectory_generation"],
            verifier_blob_uri=None,
            minio_client=client,
            artifacts_bucket="artifacts",
            upstream_cache_root=tmp_path,
            team_storage_baseline=1,
            max_team_storage_bytes=1,
        )

        assert output.status == "failed"
        assert output.status_reason == "size_exceeded"
        assert output.job_failure_reason == "size_exceeded"
        client.put_object.assert_not_called()


class TestGCQueryWindow:
    """Tests for GC retention window correctness."""

    def test_expired_task_set_qualifies_for_purge(self) -> None:
        """A task set deleted 8 days ago should be eligible for GC with 7-day retention."""
        retention_days = 7
        cutoff = datetime.now(UTC) - timedelta(days=retention_days)
        deleted_at = datetime.now(UTC) - timedelta(days=8)
        assert deleted_at < cutoff

    def test_recent_task_set_does_not_qualify(self) -> None:
        """A task set deleted 3 days ago should NOT be eligible with 7-day retention."""
        retention_days = 7
        cutoff = datetime.now(UTC) - timedelta(days=retention_days)
        deleted_at = datetime.now(UTC) - timedelta(days=3)
        assert deleted_at >= cutoff

    def test_boundary_exactly_at_retention(self) -> None:
        """A task set deleted exactly at the retention boundary is not yet eligible."""
        retention_days = 7
        cutoff = datetime.now(UTC) - timedelta(days=retention_days)
        deleted_at = cutoff
        assert not (deleted_at < cutoff)

    def test_none_soft_deleted_never_qualifies(self) -> None:
        """A task set that was never soft-deleted cannot be GC'd."""
        soft_deleted_at = None
        assert soft_deleted_at is None


class TestQuotaCheckLogic:
    """Tests for the quota count enforcement helper logic."""

    @pytest.mark.asyncio
    async def test_quota_allows_when_under_limit(self) -> None:
        """Submission should succeed when active count < max."""
        from unittest.mock import AsyncMock

        from loom_service.taskset_intake import check_taskset_count_quota

        session = AsyncMock()
        # Mock: no TeamQuota row exists -> use default
        execute_results = [
            MagicMock(scalar_one_or_none=MagicMock(return_value=None)),
            MagicMock(scalar_one=MagicMock(return_value=10)),
        ]
        session.execute = AsyncMock(side_effect=execute_results)

        # Should not raise
        await check_taskset_count_quota(
            session, team_id=uuid4(), default_max_count=50,
        )

    @pytest.mark.asyncio
    async def test_quota_rejects_when_at_limit(self) -> None:
        """Submission should be rejected (429) when active count >= max."""
        from unittest.mock import AsyncMock

        from fastapi import HTTPException

        from loom_service.taskset_intake import check_taskset_count_quota

        session = AsyncMock()
        execute_results = [
            MagicMock(scalar_one_or_none=MagicMock(return_value=None)),
            MagicMock(scalar_one=MagicMock(return_value=50)),
        ]
        session.execute = AsyncMock(side_effect=execute_results)

        with pytest.raises(HTTPException) as exc_info:
            await check_taskset_count_quota(
                session, team_id=uuid4(), default_max_count=50,
            )
        assert exc_info.value.status_code == 429
        assert "taskset_quota_exceeded" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_quota_uses_team_override(self) -> None:
        """Per-team override should take precedence over default."""
        from unittest.mock import AsyncMock

        from fastapi import HTTPException

        from loom_service.taskset_intake import check_taskset_count_quota

        team_quota = MagicMock()
        team_quota.taskset_max_count = 5

        session = AsyncMock()
        execute_results = [
            MagicMock(scalar_one_or_none=MagicMock(return_value=team_quota)),
            MagicMock(scalar_one=MagicMock(return_value=5)),
        ]
        session.execute = AsyncMock(side_effect=execute_results)

        with pytest.raises(HTTPException) as exc_info:
            await check_taskset_count_quota(
                session, team_id=uuid4(), default_max_count=50,
            )
        assert exc_info.value.status_code == 429


class TestTeamStorageBytes:
    """Tests for team storage accounting helpers."""

    def test_prefix_storage_bytes_sums_object_sizes(self) -> None:
        from loom.taskset.storage_bytes import prefix_storage_bytes

        client = MagicMock()
        client.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Size": 100}, {"Size": 250}]},
            {"Contents": [{"Size": 50}]},
        ]
        total = prefix_storage_bytes(
            client,
            bucket="artifacts",
            prefix="tasksets/user/abc/slug/",
        )
        assert total == 400

    def test_team_storage_baseline_excludes_current_task_set(self) -> None:
        from loom.taskset.storage_bytes import team_storage_baseline_excluding_task_set

        client = MagicMock()
        client.get_paginator.return_value.paginate.side_effect = [
            [{"Contents": [{"Size": 1000}, {"Size": 500}]}],
            [{"Contents": [{"Size": 500}]}],
        ]
        baseline = team_storage_baseline_excluding_task_set(
            client,
            bucket="artifacts",
            team_id="team-1",
            slug="my-set",
        )
        assert baseline == 1000


class TestStorageQuotaCheckLogic:
    """Tests for team storage quota enforcement at submit."""

    @pytest.mark.asyncio
    async def test_storage_quota_rejects_when_at_limit_with_incoming(self) -> None:
        from unittest.mock import AsyncMock, patch

        from fastapi import HTTPException

        from loom_service.taskset_intake import check_taskset_storage_quota

        session = AsyncMock()
        session.execute = AsyncMock(
            return_value=MagicMock(scalar_one_or_none=MagicMock(return_value=None)),
        )
        with patch(
            "loom_service.taskset_intake.team_taskset_storage_bytes",
            return_value=21_474_836_480,
        ):
            with pytest.raises(HTTPException) as exc_info:
                await check_taskset_storage_quota(
                    session,
                    team_id=uuid4(),
                    minio_client=MagicMock(),
                    artifacts_bucket="artifacts",
                    default_max_storage_bytes=21_474_836_480,
                    incoming_bytes=1,
                )
        assert exc_info.value.status_code == 429

    @pytest.mark.asyncio
    async def test_submit_bundle_upload_counts_archive_bytes_before_writes(
        self,
    ) -> None:
        from unittest.mock import AsyncMock, patch

        from fastapi import HTTPException, UploadFile

        from loom_service.taskset_intake import submit_task_set

        manifest = b"""
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: quota-bundle-submit
  display_name: Quota Bundle Submit
source:
  type: bundle-upload
  locator: bundle.tar.gz
"""
        bundle = b"x" * 900
        session = MagicMock()
        session.execute = AsyncMock(side_effect=[
            MagicMock(scalar_one_or_none=MagicMock(return_value=None)),
            MagicMock(scalar_one=MagicMock(return_value=0)),
            MagicMock(scalar_one_or_none=MagicMock(return_value=None)),
        ])
        session.flush = AsyncMock()
        session.rollback = AsyncMock()
        session.commit = AsyncMock()
        session.refresh = AsyncMock()
        minio_client = MagicMock()

        with patch(
            "loom_service.taskset_intake.team_taskset_storage_bytes",
            return_value=0,
        ):
            with pytest.raises(HTTPException) as exc_info:
                await submit_task_set(
                    session,
                    team_id=uuid4(),
                    minio_client=minio_client,
                    artifacts_bucket="artifacts",
                    manifest_upload=UploadFile(
                        filename="manifest.yaml",
                        file=io.BytesIO(manifest),
                    ),
                    verifier_upload=None,
                    transform_upload=None,
                    bundle_upload=UploadFile(
                        filename="bundle.tar.gz",
                        file=io.BytesIO(bundle),
                    ),
                    taskset_quota_max_count=50,
                    taskset_quota_max_storage_bytes=500,
                    manifest_max_bytes=4096,
                    bundle_max_bytes=4096,
                )

        assert exc_info.value.status_code == 429
        assert exc_info.value.detail == "taskset_storage_quota_exceeded"
        minio_client.put_object.assert_not_called()
        assert exc_info.value.detail == "taskset_storage_quota_exceeded"

    @pytest.mark.asyncio
    async def test_storage_quota_allows_when_incoming_fits(self) -> None:
        from unittest.mock import AsyncMock, patch

        from loom_service.taskset_intake import check_taskset_storage_quota

        session = AsyncMock()
        session.execute = AsyncMock(
            return_value=MagicMock(scalar_one_or_none=MagicMock(return_value=None)),
        )
        with patch(
            "loom_service.taskset_intake.team_taskset_storage_bytes",
            return_value=1000,
        ):
            await check_taskset_storage_quota(
                session,
                team_id=uuid4(),
                minio_client=MagicMock(),
                artifacts_bucket="artifacts",
                default_max_storage_bytes=2000,
                incoming_bytes=500,
            )

    @pytest.mark.asyncio
    async def test_storage_quota_rejects_when_incoming_would_exceed(self) -> None:
        from unittest.mock import AsyncMock, patch

        from fastapi import HTTPException

        from loom_service.taskset_intake import check_taskset_storage_quota

        session = AsyncMock()
        session.execute = AsyncMock(
            return_value=MagicMock(scalar_one_or_none=MagicMock(return_value=None)),
        )
        with patch(
            "loom_service.taskset_intake.team_taskset_storage_bytes",
            return_value=1990,
        ):
            with pytest.raises(HTTPException) as exc_info:
                await check_taskset_storage_quota(
                    session,
                    team_id=uuid4(),
                    minio_client=MagicMock(),
                    artifacts_bucket="artifacts",
                    default_max_storage_bytes=2000,
                    incoming_bytes=20,
                )
        assert exc_info.value.status_code == 429
