"""Fresh-database-only bootstrap for the global capacity authority."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import create_engine, delete, select, update

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
