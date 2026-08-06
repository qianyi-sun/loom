from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID

from loom.dev_instance_provisioner import DevInstanceRecord, OwnerAccessSnapshot
from loom_service.dev_instance_lifecycle import DevInstanceLifecycleRunner

_NOW = datetime(2026, 8, 6, tzinfo=UTC)
_USER = UUID("00000000-0000-0000-0000-000000000001")
_TEAM = UUID("00000000-0000-0000-0000-000000000002")
_OPERATION = UUID("00000000-0000-0000-0000-000000000003")


def _record() -> DevInstanceRecord:
    return DevInstanceRecord(
        name="alice",
        owner_user_id=_USER,
        owner_team_id=_TEAM,
        min_slots=0,
        max_slots=2,
        status="provisioning",
        deployment_generation=1,
        candidate_sha="a" * 40,
        operation_epoch=1,
        operation_id=_OPERATION,
        created_at=_NOW,
        updated_at=_NOW,
    )


def _access() -> OwnerAccessSnapshot:
    return OwnerAccessSnapshot(
        user_id=_USER,
        email=None,
        username="alice",
        username_normalized="alice",
        display_name=None,
        password_hash=None,
        password_set_at=None,
        user_status="active",
        user_disabled_at=None,
        user_created_at=_NOW,
        user_last_login_at=None,
        team_id=_TEAM,
        team_name="alice-team",
        team_created_at=_NOW,
        membership_role="owner",
        membership_created_at=_NOW,
        fair_share_weight=1.0,
        max_attempts_ceiling=3,
        license_allowlist=("MIT",),
        taskset_max_count=None,
        taskset_max_storage_bytes=None,
        allow_private_endpoints=False,
    )


class _SessionContext:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *_args):
        return None


class _SessionFactory:
    def __call__(self):
        return _SessionContext()


class _Provisioner:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def converge_create(self, record, *, access):
        assert record.operation_id == _OPERATION
        assert access.user_id == _USER
        self.calls += 1
        self.started.set()
        await self.release.wait()


async def test_lifecycle_runner_deduplicates_one_fenced_operation() -> None:
    provisioner = _Provisioner()
    runner = DevInstanceLifecycleRunner(
        session_factory=_SessionFactory(),  # type: ignore[arg-type]
        provisioner_factory=lambda _store: provisioner,  # type: ignore[arg-type,return-value]
    )

    assert runner.submit_create(_record(), _access()) is True
    await provisioner.started.wait()
    assert runner.submit_create(_record(), _access()) is False
    provisioner.release.set()
    await runner.close()

    assert provisioner.calls == 1
