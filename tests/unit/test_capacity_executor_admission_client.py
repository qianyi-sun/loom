from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest

from loom_capacity_agent.admission import ProtectedIntentObservationV2
from loom_capacity_agent.claim_guard import ExecutableClaimProposalV2
from loom_capacity_executor.admission_client import (
    DatabaseExecutableAdmissionClient,
    ExecutableAdmissionClientError,
)
from tests.unit.test_capacity_executor_launch_renderer import launch_context_fixture


def _owner_file(path: Path, value: str) -> Path:
    path.write_text(value)
    path.chmod(0o600)
    return path


def test_database_client_loads_only_bounded_owner_only_tls_urls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, object] = {}

    class Engine:
        def dispose(self) -> None:  # pragma: no cover - constructor test only
            return None

    def engine_factory(url: str, **kwargs: object) -> Engine:
        captured["url"] = url
        captured["kwargs"] = kwargs
        return Engine()

    monkeypatch.setattr(
        "loom_capacity_executor.admission_client.create_async_engine", engine_factory
    )
    path = _owner_file(
        tmp_path / "database-url",
        "postgresql+psycopg://admission:secret@db.internal/loom_dev_alice?sslmode=verify-full",
    )
    client = DatabaseExecutableAdmissionClient.from_database_url_file(
        path,
        subject_id=UUID(int=1),
        subject_incarnation=UUID(int=2),
        timeout_seconds=7,
    )
    assert client.subject_id == UUID(int=1)
    assert captured["url"] == path.read_text()
    assert captured["kwargs"] == {
        "connect_args": {"connect_timeout": 7},
        "isolation_level": "SERIALIZABLE",
        "pool_pre_ping": True,
    }

    path.chmod(0o644)
    with pytest.raises(ExecutableAdmissionClientError, match="0600"):
        DatabaseExecutableAdmissionClient.from_database_url_file(
            path,
            subject_id=UUID(int=1),
            subject_incarnation=UUID(int=2),
        )


@pytest.mark.parametrize(
    "url",
    (
        "postgresql+psycopg://admission:secret@db.internal/loom_dev_alice",
        "postgresql+psycopg://admission:secret@db.internal/loom_dev_alice?sslmode=require",
        "postgresql+psycopg://db.internal/loom_dev_alice?sslmode=verify-full",
        "sqlite:///loom.db?sslmode=verify-full",
        "postgresql+psycopg://admission:secret@db.internal/loom_dev_alice"
        "?sslmode=verify-full&connect_timeout=99",
    ),
)
def test_database_client_rejects_unscoped_or_weakened_database_urls(
    tmp_path: Path, url: str
) -> None:
    path = _owner_file(tmp_path / "database-url", url)
    with pytest.raises(ExecutableAdmissionClientError):
        DatabaseExecutableAdmissionClient.from_database_url_file(
            path,
            subject_id=UUID(int=1),
            subject_incarnation=UUID(int=2),
        )


def test_database_client_rejects_symlink_oversize_and_invalid_timeout(tmp_path: Path) -> None:
    target = _owner_file(
        tmp_path / "target",
        "postgresql+psycopg://admission:secret@db.internal/loom_dev_alice?sslmode=verify-full",
    )
    link = tmp_path / "database-url"
    link.symlink_to(target)
    with pytest.raises(ExecutableAdmissionClientError, match="nonsymlink"):
        DatabaseExecutableAdmissionClient.from_database_url_file(
            link,
            subject_id=UUID(int=1),
            subject_incarnation=UUID(int=2),
        )
    oversized = _owner_file(tmp_path / "oversized", "x" * 16_385)
    with pytest.raises(ExecutableAdmissionClientError, match="maximum"):
        DatabaseExecutableAdmissionClient.from_database_url_file(
            oversized,
            subject_id=UUID(int=1),
            subject_incarnation=UUID(int=2),
        )
    with pytest.raises(ExecutableAdmissionClientError, match="timeout"):
        DatabaseExecutableAdmissionClient.from_database_url_file(
            target,
            subject_id=UUID(int=1),
            subject_incarnation=UUID(int=2),
            timeout_seconds=0,
        )


@pytest.mark.asyncio
async def test_database_client_sends_complete_claim_to_protected_transaction() -> None:
    proposal = ExecutableClaimProposalV2(
        operation_id=UUID(int=10),
        protected_attempt_id=UUID(int=11),
        execution_generation=7,
        requirements_digest="b" * 64,
        worker_id=UUID(int=12),
        worker_incarnation=UUID(int=13),
        expected_claim_high_water=3,
    )
    client = object.__new__(DatabaseExecutableAdmissionClient)

    async def store_call(method: str, value: object) -> None:
        assert method == "admit_claim"
        assert value is proposal

    client._store_call = store_call  # type: ignore[assignment]

    assert await client.admit_claim(proposal) is None


# Production break caught: the production database client could not obtain the
# exact protected worker/drain high-water needed before conditional cancellation.
@pytest.mark.asyncio
async def test_database_client_observes_exact_protected_intent() -> None:
    binding = launch_context_fixture().binding
    expected = ProtectedIntentObservationV2(binding=binding)
    client = object.__new__(DatabaseExecutableAdmissionClient)

    async def store_call(method: str, value: object) -> object:
        assert method == "observe_intent"
        assert value is binding
        return expected

    client._store_call = store_call  # type: ignore[assignment]

    assert await client.observe_intent(binding) == expected
