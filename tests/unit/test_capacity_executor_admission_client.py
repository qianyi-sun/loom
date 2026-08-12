from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest
from loom_capacity_executor.admission_client import (
    DatabaseExecutableAdmissionClient,
    ExecutableAdmissionClientError,
)


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
