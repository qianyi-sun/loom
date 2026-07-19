from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from loom.data_lifecycle import DataClass, OwnerKind
from loom.data_lifecycle_registry import (
    RuntimeLifecycleScope,
    ensure_batch_lifecycle_authority,
    register_lifecycle_object,
)

NOW = datetime(2026, 7, 20, tzinfo=UTC)


def test_protected_runtime_requires_explicit_namespace(monkeypatch) -> None:
    monkeypatch.setenv("LOOM_ENV", "staging")
    monkeypatch.delenv("LOOM_NAMESPACE", raising=False)

    with pytest.raises(RuntimeError, match="LOOM_NAMESPACE"):
        RuntimeLifecycleScope.from_environ()


def test_development_runtime_has_non_destructive_defaults(monkeypatch) -> None:
    monkeypatch.delenv("LOOM_ENV", raising=False)
    monkeypatch.delenv("LOOM_NAMESPACE", raising=False)

    scope = RuntimeLifecycleScope.from_environ()
    spec = scope.authority_spec(
        team_id=uuid4(),
        data_class=DataClass.RUN,
        owner_kind=OwnerKind.BATCH,
        owner_id="batch-1",
        created_at=NOW,
    )

    assert (scope.environment, scope.namespace) == ("development", "loom")
    assert spec.pinned
    assert spec.expires_at is None


def test_staging_execution_authority_expires_in_seven_days(monkeypatch) -> None:
    team_id = uuid4()
    monkeypatch.setenv("LOOM_ENV", "staging")
    monkeypatch.setenv("LOOM_NAMESPACE", "loom-staging")

    spec = RuntimeLifecycleScope.from_environ().authority_spec(
        team_id=team_id,
        data_class=DataClass.TRIAL,
        owner_kind=OwnerKind.TRIAL,
        owner_id="trial-1",
        created_at=NOW,
    )

    assert spec.environment == "staging"
    assert spec.namespace == "loom-staging"
    assert spec.team_id == team_id
    assert not spec.pinned
    assert spec.expires_at == NOW + timedelta(days=7)


def test_staging_authority_requires_team() -> None:
    with pytest.raises(ValueError, match="requires a team"):
        RuntimeLifecycleScope(environment="staging", namespace="loom-staging").authority_spec(
            team_id=None,
            data_class=DataClass.TRIAL,
            owner_kind=OwnerKind.TRIAL,
            owner_id="trial-1",
            created_at=NOW,
        )


@pytest.mark.asyncio
async def test_unversioned_object_requires_exact_hash() -> None:
    session = AsyncMock()

    with pytest.raises(ValueError, match="requires a SHA-256"):
        await register_lifecycle_object(
            session,
            authority_id=uuid4(),
            bucket="artifacts",
            object_key="team/trial/result.json",
            version_id=None,
            content_sha256=None,
            size_bytes=1,
            created_at=NOW,
        )

    session.get.assert_not_awaited()


@pytest.mark.asyncio
async def test_object_hash_must_be_canonical_lowercase_hex() -> None:
    session = AsyncMock()

    with pytest.raises(ValueError, match="lowercase hexadecimal"):
        await register_lifecycle_object(
            session,
            authority_id=uuid4(),
            bucket="artifacts",
            object_key="team/trial/result.json",
            version_id=None,
            content_sha256="A" * 64,
            size_bytes=1,
            created_at=NOW,
        )

    session.get.assert_not_awaited()


@pytest.mark.asyncio
async def test_staging_batch_authority_checks_capacity_before_registration(
    monkeypatch,
) -> None:
    monkeypatch.setenv("LOOM_ENV", "staging")
    monkeypatch.setenv("LOOM_NAMESPACE", "loom-staging")
    calls: list[str] = []
    authority_id = uuid4()
    batch_id = uuid4()

    async def capacity(_session, *, namespace, now) -> None:
        assert namespace == "loom-staging"
        assert now == NOW
        calls.append("capacity")

    async def authority(_session, *, spec):
        assert spec.owner_id == str(batch_id)
        calls.append("authority")
        return authority_id

    monkeypatch.setattr(
        "loom.data_lifecycle_registry.require_staging_capacity_admission",
        capacity,
    )
    monkeypatch.setattr(
        "loom.data_lifecycle_registry.ensure_lifecycle_authority",
        authority,
    )

    result = await ensure_batch_lifecycle_authority(
        AsyncMock(),
        batch_id=batch_id,
        team_id=uuid4(),
        created_at=NOW,
    )

    assert result == authority_id
    assert calls == ["capacity", "authority"]
