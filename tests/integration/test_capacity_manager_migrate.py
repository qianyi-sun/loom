"""Fresh-database-only bootstrap for the global capacity authority."""

from __future__ import annotations

import sys
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import create_engine, delete, select, update

import loom_capacity_manager.migrate as capacity_migrate
from loom_capacity_manager.migrate import (
    CapacityAuthorityBootstrapError,
    bind_fresh_authority,
    migrate_capacity_database,
)
from loom_capacity_manager.models import Base, CapacityAuditEvent, CapacityAuthorityState

_MIGRATION_AUTHORITY = UUID("00000000-0000-4000-8000-000000000900")
_REVIEWED_AUTHORITY = UUID("00000000-0000-4000-8000-000000000901")


def _reset_empty_shadow(database_url: str) -> None:
    engine = create_engine(database_url)
    try:
        with engine.begin() as connection:
            for table in reversed(Base.metadata.sorted_tables):
                if table.name != CapacityAuthorityState.__tablename__:
                    connection.execute(delete(table))
            connection.execute(
                update(CapacityAuthorityState)
                .where(CapacityAuthorityState.singleton_id == 1)
                .values(
                    authority_incarnation=_MIGRATION_AUTHORITY,
                    writer_epoch=0,
                    recovery_state="shadow",
                    increase_freeze=True,
                    increase_freeze_reason="initial_shadow_freeze",
                    executable_new_capacity_ceiling=0,
                    global_pending_slot_ceiling=0,
                    global_pending_job_ceiling=0,
                    global_submission_rate_ceiling=0,
                )
            )
    finally:
        engine.dispose()


def _authority(database_url: str) -> dict[str, object]:
    engine = create_engine(database_url)
    try:
        with engine.connect() as connection:
            return dict(
                connection.execute(
                    select(CapacityAuthorityState.__table__).where(
                        CapacityAuthorityState.singleton_id == 1
                    )
                )
                .mappings()
                .one()
            )
    finally:
        engine.dispose()


def test_empty_shadow_database_binds_the_reviewed_authority(
    capacity_postgres_url: str,
) -> None:
    _reset_empty_shadow(capacity_postgres_url)
    engine = create_engine(capacity_postgres_url)
    try:
        bind_fresh_authority(engine, _REVIEWED_AUTHORITY)
    finally:
        engine.dispose()

    assert _authority(capacity_postgres_url)["authority_incarnation"] == _REVIEWED_AUTHORITY


def test_matching_authority_is_idempotent_after_writer_registration(
    capacity_postgres_url: str,
) -> None:
    _reset_empty_shadow(capacity_postgres_url)
    engine = create_engine(capacity_postgres_url)
    try:
        bind_fresh_authority(engine, _REVIEWED_AUTHORITY)
        with engine.begin() as connection:
            connection.execute(
                update(CapacityAuthorityState)
                .where(CapacityAuthorityState.singleton_id == 1)
                .values(writer_epoch=4)
            )
        bind_fresh_authority(engine, _REVIEWED_AUTHORITY)
    finally:
        engine.dispose()

    authority = _authority(capacity_postgres_url)
    assert authority["authority_incarnation"] == _REVIEWED_AUTHORITY
    assert authority["writer_epoch"] == 4


def test_different_authority_cannot_replace_reviewed_bootstrap(
    capacity_postgres_url: str,
) -> None:
    _reset_empty_shadow(capacity_postgres_url)
    replacement = UUID("00000000-0000-4000-8000-000000000902")
    engine = create_engine(capacity_postgres_url)
    try:
        bind_fresh_authority(engine, _REVIEWED_AUTHORITY)
        with pytest.raises(CapacityAuthorityBootstrapError):
            bind_fresh_authority(engine, replacement)
    finally:
        engine.dispose()

    assert _authority(capacity_postgres_url)["authority_incarnation"] == _REVIEWED_AUTHORITY


def test_binding_marker_is_exact_and_idempotent(capacity_postgres_url: str) -> None:
    _reset_empty_shadow(capacity_postgres_url)
    engine = create_engine(capacity_postgres_url)
    try:
        bind_fresh_authority(engine, _REVIEWED_AUTHORITY)
        bind_fresh_authority(engine, _REVIEWED_AUTHORITY)
        with engine.connect() as connection:
            markers = (
                connection.execute(
                    select(CapacityAuditEvent).where(
                        CapacityAuditEvent.event_kind == "authority_incarnation_bound"
                    )
                )
                .mappings()
                .all()
            )
    finally:
        engine.dispose()

    assert len(markers) == 1
    assert markers[0]["actor_kind"] == "migration"
    assert markers[0]["actor_id"] == "capacity-authority-bootstrap"
    assert markers[0]["object_binding"] == {
        "authority_incarnation": str(_REVIEWED_AUTHORITY)
    }
    assert markers[0]["detail"] == {"state": "reviewed-bootstrap-bound"}


def test_matching_authority_backfills_a_missing_binding_fence(
    capacity_postgres_url: str,
) -> None:
    _reset_empty_shadow(capacity_postgres_url)
    replacement = UUID("00000000-0000-4000-8000-000000000902")
    engine = create_engine(capacity_postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                update(CapacityAuthorityState)
                .where(CapacityAuthorityState.singleton_id == 1)
                .values(authority_incarnation=_REVIEWED_AUTHORITY)
            )
        bind_fresh_authority(engine, _REVIEWED_AUTHORITY)
        with pytest.raises(CapacityAuthorityBootstrapError):
            bind_fresh_authority(engine, replacement)
    finally:
        engine.dispose()

    assert _authority(capacity_postgres_url)["authority_incarnation"] == _REVIEWED_AUTHORITY


def test_matching_authority_rejects_a_conflicting_binding_marker(
    capacity_postgres_url: str,
) -> None:
    _reset_empty_shadow(capacity_postgres_url)
    conflicting = UUID("00000000-0000-4000-8000-000000000902")
    engine = create_engine(capacity_postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                update(CapacityAuthorityState)
                .where(CapacityAuthorityState.singleton_id == 1)
                .values(authority_incarnation=_REVIEWED_AUTHORITY)
            )
            connection.execute(
                CapacityAuditEvent.__table__.insert().values(
                    actor_kind="migration",
                    actor_id="capacity-authority-bootstrap",
                    event_kind="authority_incarnation_bound",
                    object_binding={"authority_incarnation": str(conflicting)},
                    detail={"state": "reviewed-bootstrap-bound"},
                )
            )
        with pytest.raises(CapacityAuthorityBootstrapError):
            bind_fresh_authority(engine, _REVIEWED_AUTHORITY)
    finally:
        engine.dispose()


def test_nil_authority_is_rejected_without_mutating_the_database(
    capacity_postgres_url: str,
) -> None:
    _reset_empty_shadow(capacity_postgres_url)
    engine = create_engine(capacity_postgres_url)
    try:
        with pytest.raises(ValueError, match="non-nil"):
            bind_fresh_authority(engine, UUID(int=0))
    finally:
        engine.dispose()

    assert _authority(capacity_postgres_url)["authority_incarnation"] == _MIGRATION_AUTHORITY


def test_migration_rejects_nil_authority_before_reading_database_url(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="non-nil"):
        migrate_capacity_database(tmp_path / "missing-database-url", UUID(int=0))


def test_migration_cli_rejects_nil_authority_as_an_argument_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "loom-capacity-migrate",
            "--db-url-file",
            "/does/not/matter",
            "--expected-authority-incarnation",
            str(UUID(int=0)),
        ],
    )

    with pytest.raises(SystemExit) as stopped:
        capacity_migrate.main()

    assert stopped.value.code == 2
    assert "non-nil" in capsys.readouterr().err


def test_migration_cli_redacts_runtime_failure_details(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_detail = "postgresql://operator:do-not-log@example.invalid/capacity"

    def fail(_db_url_file: Path, _expected: UUID) -> None:
        raise CapacityAuthorityBootstrapError(secret_detail)

    monkeypatch.setattr(capacity_migrate, "migrate_capacity_database", fail)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "loom-capacity-migrate",
            "--db-url-file",
            "/run/credentials/database-url",
            "--expected-authority-incarnation",
            str(_REVIEWED_AUTHORITY),
        ],
    )

    with pytest.raises(SystemExit) as stopped:
        capacity_migrate.main()

    captured = capsys.readouterr()
    assert stopped.value.code == 1
    assert captured.out == ""
    assert captured.err == "error: capacity migration failed\n"
    assert secret_detail not in captured.err


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("writer_epoch", 1),
        ("increase_freeze", False),
        ("global_pending_slot_ceiling", 1),
        ("global_pending_job_ceiling", 1),
        ("global_submission_rate_ceiling", 1),
    ],
)
def test_different_authority_cannot_bind_after_shadow_state_was_used(
    capacity_postgres_url: str,
    field: str,
    value: object,
) -> None:
    _reset_empty_shadow(capacity_postgres_url)
    engine = create_engine(capacity_postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                update(CapacityAuthorityState)
                .where(CapacityAuthorityState.singleton_id == 1)
                .values(**{field: value})
            )
        with pytest.raises(CapacityAuthorityBootstrapError, match="unused frozen shadow"):
            bind_fresh_authority(engine, _REVIEWED_AUTHORITY)
    finally:
        engine.dispose()

    assert _authority(capacity_postgres_url)["authority_incarnation"] == _MIGRATION_AUTHORITY


def test_different_authority_cannot_bind_after_any_capacity_row_exists(
    capacity_postgres_url: str,
) -> None:
    _reset_empty_shadow(capacity_postgres_url)
    engine = create_engine(capacity_postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                CapacityAuditEvent.__table__.insert().values(
                    actor_kind="operator",
                    actor_id="migration-test",
                    event_kind="authority_observed",
                    object_binding={},
                    detail={},
                )
            )
        with pytest.raises(CapacityAuthorityBootstrapError, match="not empty"):
            bind_fresh_authority(engine, _REVIEWED_AUTHORITY)
    finally:
        engine.dispose()

    assert _authority(capacity_postgres_url)["authority_incarnation"] == _MIGRATION_AUTHORITY


def test_migration_entrypoint_reads_owner_only_url_and_binds_authority(
    capacity_postgres_url: str,
    tmp_path: Path,
) -> None:
    _reset_empty_shadow(capacity_postgres_url)
    database_url_file = tmp_path / "database-url"
    database_url_file.write_text(capacity_postgres_url, encoding="utf-8")
    database_url_file.chmod(0o600)

    migrate_capacity_database(database_url_file, _REVIEWED_AUTHORITY)

    assert _authority(capacity_postgres_url)["authority_incarnation"] == _REVIEWED_AUTHORITY


def test_migration_entrypoint_accepts_percent_encoded_database_url(
    capacity_postgres_url: str,
    tmp_path: Path,
) -> None:
    _reset_empty_shadow(capacity_postgres_url)
    separator = "&" if "?" in capacity_postgres_url else "?"
    encoded_url = (
        f"{capacity_postgres_url}{separator}"
        "application_name=capacity%40bootstrap"
    )
    database_url_file = tmp_path / "database-url"
    database_url_file.write_text(encoded_url, encoding="utf-8")
    database_url_file.chmod(0o600)

    migrate_capacity_database(database_url_file, _REVIEWED_AUTHORITY)

    assert _authority(capacity_postgres_url)["authority_incarnation"] == _REVIEWED_AUTHORITY
