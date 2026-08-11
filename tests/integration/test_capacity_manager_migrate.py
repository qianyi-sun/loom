"""Fresh-database-only bootstrap for the global capacity authority."""

from __future__ import annotations

import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import Connection, Engine, create_engine, delete, select, text, update

import loom_capacity_manager.migrate as capacity_migrate
from loom_capacity_manager.migrate import (
    CapacityAuthorityBootstrapError,
    bind_fresh_authority,
    migrate_capacity_database,
)
from loom_capacity_manager.models import Base, CapacityAuditEvent, CapacityAuthorityState

_MIGRATION_AUTHORITY = UUID("00000000-0000-4000-8000-000000000900")
_REVIEWED_AUTHORITY = UUID("00000000-0000-4000-8000-000000000901")
_OTHER_AUTHORITY = UUID("00000000-0000-4000-8000-000000000902")


def _seed_marker(authority: UUID) -> dict[str, object]:
    return {
        "actor_kind": "migration",
        "actor_id": "capacity-authority-bootstrap",
        "event_kind": "authority_incarnation_seeded",
        "object_binding": {"authority_incarnation": str(authority)},
        "detail": {"state": "migration-generated-seed"},
    }


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
            connection.execute(
                CapacityAuditEvent.__table__.insert().values(
                    **_seed_marker(_MIGRATION_AUTHORITY)
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
    engine = create_engine(capacity_postgres_url)
    try:
        bind_fresh_authority(engine, _REVIEWED_AUTHORITY)
        with pytest.raises(CapacityAuthorityBootstrapError):
            bind_fresh_authority(engine, _OTHER_AUTHORITY)
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
    engine = create_engine(capacity_postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(delete(CapacityAuditEvent))
            connection.execute(
                update(CapacityAuthorityState)
                .where(CapacityAuthorityState.singleton_id == 1)
                .values(authority_incarnation=_REVIEWED_AUTHORITY)
            )
        bind_fresh_authority(engine, _REVIEWED_AUTHORITY)
        with pytest.raises(CapacityAuthorityBootstrapError):
            bind_fresh_authority(engine, _OTHER_AUTHORITY)
    finally:
        engine.dispose()

    assert _authority(capacity_postgres_url)["authority_incarnation"] == _REVIEWED_AUTHORITY


def test_matching_authority_rejects_a_conflicting_binding_marker(
    capacity_postgres_url: str,
) -> None:
    _reset_empty_shadow(capacity_postgres_url)
    engine = create_engine(capacity_postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(delete(CapacityAuditEvent))
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
                    object_binding={"authority_incarnation": str(_OTHER_AUTHORITY)},
                    detail={"state": "reviewed-bootstrap-bound"},
                )
            )
        with pytest.raises(CapacityAuthorityBootstrapError):
            bind_fresh_authority(engine, _REVIEWED_AUTHORITY)
    finally:
        engine.dispose()


def test_wrong_uuid_cannot_claim_markerless_reviewed_authority_before_backfill(
    capacity_postgres_url: str,
) -> None:
    _reset_empty_shadow(capacity_postgres_url)
    engine = create_engine(capacity_postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(delete(CapacityAuditEvent))
            connection.execute(
                update(CapacityAuthorityState)
                .where(CapacityAuthorityState.singleton_id == 1)
                .values(authority_incarnation=_REVIEWED_AUTHORITY)
            )

        with pytest.raises(CapacityAuthorityBootstrapError):
            bind_fresh_authority(engine, _OTHER_AUTHORITY)
        bind_fresh_authority(engine, _REVIEWED_AUTHORITY)
    finally:
        engine.dispose()

    assert _authority(capacity_postgres_url)["authority_incarnation"] == _REVIEWED_AUTHORITY


def test_concurrent_expected_and_wrong_uuid_fail_closed_on_markerless_state(
    capacity_postgres_url: str,
) -> None:
    _reset_empty_shadow(capacity_postgres_url)
    engine = create_engine(capacity_postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(delete(CapacityAuditEvent))
            connection.execute(
                update(CapacityAuthorityState)
                .where(CapacityAuthorityState.singleton_id == 1)
                .values(authority_incarnation=_REVIEWED_AUTHORITY)
            )
    finally:
        engine.dispose()

    wrong_application = "capacity-bootstrap-wrong"
    expected_application = "capacity-bootstrap-expected"

    wrong_engine = create_engine(
        capacity_postgres_url,
        connect_args={"application_name": wrong_application},
    )
    expected_engine = create_engine(
        capacity_postgres_url,
        connect_args={"application_name": expected_application},
    )
    for thread_engine, application_name in (
        (wrong_engine, wrong_application),
        (expected_engine, expected_application),
    ):
        with thread_engine.connect() as connection:
            assert connection.execute(text("SHOW application_name")).scalar_one() == (
                application_name
            )

    def bind(authority: UUID, thread_engine: Engine) -> str:
        try:
            bind_fresh_authority(thread_engine, authority)
            return "bound"
        except CapacityAuthorityBootstrapError:
            return "rejected"

    def wait_until_blocked(connection: Connection, application_name: str) -> None:
        deadline = time.monotonic() + 5
        observed: list[dict[str, object]] = []
        while time.monotonic() < deadline:
            observed = [
                dict(row)
                for row in connection.execute(
                text(
                    "SELECT state, wait_event_type, wait_event, query "
                    "FROM pg_stat_activity WHERE application_name = :application_name"
                ),
                {"application_name": application_name},
            ).mappings()
            ]
            if len(observed) == 1 and observed[0]["wait_event_type"] == "Lock":
                return
            time.sleep(0.01)
        raise AssertionError(
            f"{application_name} did not wait for the authority row lock: {observed!r}"
        )

    blocker_engine = create_engine(capacity_postgres_url)
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            with blocker_engine.begin() as blocker:
                blocker.execute(
                    select(CapacityAuthorityState)
                    .where(CapacityAuthorityState.singleton_id == 1)
                    .with_for_update()
                ).one()
                wrong = executor.submit(bind, _OTHER_AUTHORITY, wrong_engine)
                wait_until_blocked(blocker, wrong_application)
                expected = executor.submit(
                    bind,
                    _REVIEWED_AUTHORITY,
                    expected_engine,
                )
                wait_until_blocked(blocker, expected_application)
    finally:
        blocker_engine.dispose()
        wrong_engine.dispose()
        expected_engine.dispose()

    assert expected.result(timeout=5) == "bound"
    assert wrong.result(timeout=5) == "rejected"
    assert _authority(capacity_postgres_url)["authority_incarnation"] == _REVIEWED_AUTHORITY


@pytest.mark.parametrize(
    "reserved_marker",
    [
        {
            "actor_kind": "operator",
            "actor_id": "capacity-authority-bootstrap",
            "event_kind": "authority_incarnation_bound",
            "object_binding": {"authority_incarnation": str(_REVIEWED_AUTHORITY)},
            "detail": {"state": "reviewed-bootstrap-bound"},
        },
        {
            "actor_kind": "migration",
            "actor_id": "other-bootstrap",
            "event_kind": "authority_incarnation_bound",
            "object_binding": {"authority_incarnation": str(_REVIEWED_AUTHORITY)},
            "detail": {"state": "reviewed-bootstrap-bound"},
        },
        {
            **_seed_marker(_REVIEWED_AUTHORITY),
            "object_binding": {"authority_incarnation": str(_OTHER_AUTHORITY)},
        },
        {
            **_seed_marker(_REVIEWED_AUTHORITY),
            "detail": {"state": "reviewed-bootstrap-bound"},
        },
    ],
    ids=("actor-kind-drift", "actor-id-drift", "seed-payload-drift", "seed-detail-drift"),
)
def test_any_malformed_reserved_authority_evidence_fails_closed(
    capacity_postgres_url: str,
    reserved_marker: dict[str, object],
) -> None:
    _reset_empty_shadow(capacity_postgres_url)
    engine = create_engine(capacity_postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(delete(CapacityAuditEvent))
            connection.execute(
                update(CapacityAuthorityState)
                .where(CapacityAuthorityState.singleton_id == 1)
                .values(authority_incarnation=_REVIEWED_AUTHORITY)
            )
            connection.execute(
                CapacityAuditEvent.__table__.insert().values(**reserved_marker)
            )
        with pytest.raises(CapacityAuthorityBootstrapError):
            bind_fresh_authority(engine, _REVIEWED_AUTHORITY)
    finally:
        engine.dispose()


def test_duplicate_seed_evidence_fails_closed(capacity_postgres_url: str) -> None:
    _reset_empty_shadow(capacity_postgres_url)
    engine = create_engine(capacity_postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                CapacityAuditEvent.__table__.insert().values(
                    **_seed_marker(_MIGRATION_AUTHORITY)
                )
            )
        with pytest.raises(CapacityAuthorityBootstrapError):
            bind_fresh_authority(engine, _REVIEWED_AUTHORITY)
    finally:
        engine.dispose()

    assert _authority(capacity_postgres_url)["authority_incarnation"] == _MIGRATION_AUTHORITY


def test_contradictory_seed_and_bound_evidence_fails_closed(
    capacity_postgres_url: str,
) -> None:
    _reset_empty_shadow(capacity_postgres_url)
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
                    object_binding={"authority_incarnation": str(_OTHER_AUTHORITY)},
                    detail={"state": "reviewed-bootstrap-bound"},
                )
            )
        with pytest.raises(CapacityAuthorityBootstrapError):
            bind_fresh_authority(engine, _REVIEWED_AUTHORITY)
    finally:
        engine.dispose()

    assert _authority(capacity_postgres_url)["authority_incarnation"] == _REVIEWED_AUTHORITY


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


def test_authority_binding_connection_enforces_fixed_postgres_timeouts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_url = (
        "postgresql+psycopg://operator:secret@example.invalid/capacity"
        "?application_name=capacity%40bootstrap&connect_timeout=99"
    )
    database_url_file = tmp_path / "database-url"
    database_url_file.write_text(database_url, encoding="utf-8")
    database_url_file.chmod(0o600)
    captured: dict[str, object] = {}

    class FakeEngine:
        def dispose(self) -> None:
            captured["disposed"] = True

    def create(url: str, **kwargs: object) -> FakeEngine:
        captured["url"] = url
        captured["kwargs"] = kwargs
        return FakeEngine()

    monkeypatch.setattr(capacity_migrate.command, "upgrade", lambda *_args: None)
    monkeypatch.setattr(capacity_migrate, "create_engine", create)
    monkeypatch.setattr(capacity_migrate, "bind_fresh_authority", lambda *_args: None)

    migrate_capacity_database(database_url_file, _REVIEWED_AUTHORITY)

    assert captured == {
        "url": database_url,
        "kwargs": {
            "isolation_level": "SERIALIZABLE",
            "connect_args": {
                "connect_timeout": 10,
                "options": "-c lock_timeout=30000 -c statement_timeout=300000",
            },
        },
        "disposed": True,
    }
