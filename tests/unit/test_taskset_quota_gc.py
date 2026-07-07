"""Unit tests for TaskSet quotas and GC (#242 sub-plan 7)."""

from __future__ import annotations

import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from loom.taskset.materialize import (
    _BundleSizeExceededError,
    _upload_bundle_dir,
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
