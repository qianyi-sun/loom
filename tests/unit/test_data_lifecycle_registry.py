from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from loom.data_lifecycle import DataClass, OwnerKind
from loom.data_lifecycle_registry import RuntimeLifecycleScope

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
