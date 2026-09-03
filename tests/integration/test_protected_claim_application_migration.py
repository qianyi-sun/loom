"""Application migration coverage for protected worker admission ownership."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory
from sqlalchemy import Connection, create_engine, text
from sqlalchemy.exc import IntegrityError


def _config(database_url: str) -> AlembicConfig:
    root = Path(__file__).resolve().parents[2]
    config = AlembicConfig(str(root / "migrations" / "alembic.ini"))
    config.set_main_option("script_location", str(root / "migrations"))
    config.set_main_option("sqlalchemy.url", database_url)
    return config


def _head_revision(config: AlembicConfig) -> str:
    head = ScriptDirectory.from_config(config).get_current_head()
    assert head is not None
    return head


def _seed_trial(connection: Connection, *, team_id: UUID, state: str) -> UUID:
    task_id = f"protected-admission-migration-{uuid4()}"
    trial_id = uuid4()
    connection.execute(
        text("INSERT INTO tasks (id, checksum, config) VALUES (:id, :checksum, '{}'::jsonb)"),
        {"id": task_id, "checksum": uuid4().hex},
    )
    connection.execute(
        text(
            "INSERT INTO trials "
            "(id, team_id, task_id, config, requires_caps, state, attempt_count) "
            "VALUES (:id, :team_id, :task_id, '{}'::jsonb, '{}'::jsonb, :state, 1)"
        ),
        {"id": trial_id, "team_id": team_id, "task_id": task_id, "state": state},
    )
    return trial_id


def _insert_reservation(
    connection: Connection,
    *,
    trial_id: UUID,
    team_id: UUID,
    owner_kind: str,
    attempt: int = 1,
) -> UUID:
    reservation_id = uuid4()
    connection.execute(
        text(
            "INSERT INTO execution_admission_reservations "
            "(id, trial_id, attempt, execution_role, team_id, environment, pool_id, "
            "owner_kind, owner_id) VALUES "
            "(:id, :trial_id, :attempt, 'attempt', :team_id, 'staging', 'oldlab', "
            ":owner_kind, :owner_id)"
        ),
        {
            "id": reservation_id,
            "trial_id": trial_id,
            "team_id": team_id,
            "owner_kind": owner_kind,
            "owner_id": uuid4(),
            "attempt": attempt,
        },
    )
    return reservation_id


def test_0128_requeue_trampoline_is_private_and_reversible(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    engine = create_engine(isolated_migration_postgres_url)
    signature = "public.loom_transform_protected_runtime_trial_requeue()"
    trigger_name = "capacity_guard_transform_protected_runtime_trial_requeue"
    try:
        with engine.connect() as connection:
            routine = (
                connection.execute(
                    text(
                        "SELECT pg_get_userbyid(routine.proowner) AS owner, "
                        "routine.prosecdef, routine.proconfig "
                        "FROM pg_proc AS routine "
                        "WHERE routine.oid = to_regprocedure(:signature)"
                    ),
                    {"signature": signature},
                )
                .mappings()
                .one()
            )
            trigger = connection.execute(
                text(
                    "SELECT pg_get_triggerdef(oid) FROM pg_trigger "
                    "WHERE tgrelid = 'public.trials'::regclass "
                    "AND tgname = :trigger_name AND NOT tgisinternal"
                ),
                {"trigger_name": trigger_name},
            ).scalar_one()
            assert routine["owner"]
            assert routine["prosecdef"] is True
            assert routine["proconfig"] == ["search_path=pg_catalog"]
            assert "BEFORE UPDATE OF state" in trigger
            assert (
                connection.execute(
                    text("SELECT has_function_privilege('public', :signature, 'EXECUTE')"),
                    {"signature": signature},
                ).scalar_one()
                is False
            )

        command.downgrade(config, "0127")
        with engine.connect() as connection:
            assert (
                connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
                == "0127"
            )
            assert (
                connection.execute(
                    text("SELECT to_regprocedure(:signature)"),
                    {"signature": signature},
                ).scalar_one()
                is None
            )
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM pg_trigger "
                        "WHERE tgrelid = 'public.trials'::regclass "
                        "AND tgname = :trigger_name AND NOT tgisinternal"
                    ),
                    {"trigger_name": trigger_name},
                ).scalar_one()
                == 0
            )
            assert (
                connection.execute(
                    text(
                        "SELECT to_regprocedure"
                        "('public.loom_close_protected_runtime_trial_claim()')"
                    )
                ).scalar_one()
                is not None
            )

        command.upgrade(config, "head")
        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == _head_revision(config)
            assert (
                connection.execute(
                    text("SELECT to_regprocedure(:signature)"),
                    {"signature": signature},
                ).scalar_one()
                is not None
            )
    finally:
        command.upgrade(config, "head")
        engine.dispose()


def test_0128_refuses_downgrade_while_protected_claim_can_be_requeued(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    engine = create_engine(isolated_migration_postgres_url)
    trigger_name = "capacity_guard_transform_protected_runtime_trial_requeue"
    team_id = uuid4()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
                {"id": team_id, "name": f"protected-requeue-downgrade-{team_id}"},
            )
            trial_id = _seed_trial(connection, team_id=team_id, state="claimed")
            _insert_reservation(
                connection,
                trial_id=trial_id,
                team_id=team_id,
                owner_kind="protected_worker_claim",
            )

        with pytest.raises(
            RuntimeError,
            match="cannot downgrade 0128 while protected claims can be requeued",
        ):
            command.downgrade(config, "0127")

        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == _head_revision(config)
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM pg_trigger "
                        "WHERE tgrelid = 'public.trials'::regclass "
                        "AND tgname = :trigger_name AND NOT tgisinternal"
                    ),
                    {"trigger_name": trigger_name},
                ).scalar_one()
                == 1
            )
    finally:
        command.upgrade(config, "head")
        engine.dispose()


def test_0127_downgrade_and_reupgrade_preserve_private_terminal_trampoline(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    engine = create_engine(isolated_migration_postgres_url)
    signature = "public.loom_close_protected_runtime_trial_claim()"
    trigger_name = "capacity_guard_close_protected_runtime_trial_claim"
    team_id = uuid4()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
                {"id": team_id, "name": f"protected-migration-{team_id}"},
            )
            retained_trial_id = _seed_trial(connection, team_id=team_id, state="claimed")
            rejected_trial_id = _seed_trial(connection, team_id=team_id, state="claimed")
            reservation_id = _insert_reservation(
                connection,
                trial_id=retained_trial_id,
                team_id=team_id,
                owner_kind="protected_worker_claim",
            )
            connection.execute(
                text("UPDATE trials SET state = 'failed' WHERE id = :trial_id"),
                {"trial_id": retained_trial_id},
            )

        with engine.connect() as connection:
            application_routine = (
                connection.execute(
                    text(
                        "SELECT pg_get_userbyid(routine.proowner) AS owner, "
                        "routine.prosecdef, routine.proconfig "
                        "FROM pg_proc AS routine "
                        "JOIN pg_namespace AS namespace "
                        "ON namespace.oid = routine.pronamespace "
                        "WHERE namespace.nspname = 'public' "
                        "AND routine.proname = 'loom_close_protected_runtime_trial_claim'"
                    )
                )
                .mappings()
                .one()
            )
            assert application_routine["prosecdef"] is True
            assert application_routine["proconfig"] == ["search_path=pg_catalog"]
            assert (
                connection.execute(
                    text("SELECT has_function_privilege('public', :function, 'EXECUTE')"),
                    {"function": signature},
                ).scalar_one()
                is False
            )
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM pg_trigger "
                        "WHERE tgrelid = 'public.trials'::regclass "
                        "AND tgname = :trigger_name AND NOT tgisinternal"
                    ),
                    {"trigger_name": trigger_name},
                ).scalar_one()
                == 1
            )
            protected_release = connection.execute(
                text(
                    "SELECT pg_get_functiondef("
                    "'public.loom_release_legacy_execution_admission()'::regprocedure)"
                )
            ).scalar_one()
            assert "'protected_worker_claim'" in protected_release

        command.downgrade(config, "0126")
        with engine.connect() as connection:
            assert (
                connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
                == "0126"
            )
            assert (
                connection.execute(
                    text("SELECT to_regprocedure(:function)"),
                    {"function": signature},
                ).scalar_one()
                is None
            )
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM pg_trigger "
                        "WHERE tgrelid = 'public.trials'::regclass "
                        "AND tgname = :trigger_name AND NOT tgisinternal"
                    ),
                    {"trigger_name": trigger_name},
                ).scalar_one()
                == 0
            )
            constraint = (
                connection.execute(
                    text(
                        "SELECT convalidated, pg_get_constraintdef(oid) AS definition "
                        "FROM pg_constraint "
                        "WHERE conrelid = "
                        "'public.execution_admission_reservations'::regclass "
                        "AND conname = "
                        "'execution_admission_reservations_owner_kind_check'"
                    )
                )
                .mappings()
                .one()
            )
            assert constraint["convalidated"] is False
            assert "protected_worker_claim" not in constraint["definition"]
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM execution_admission_reservations "
                        "WHERE id = :reservation_id AND owner_kind = 'protected_worker_claim'"
                    ),
                    {"reservation_id": reservation_id},
                ).scalar_one()
                == 1
            )
            legacy_release = connection.execute(
                text(
                    "SELECT pg_get_functiondef("
                    "'public.loom_release_legacy_execution_admission()'::regprocedure)"
                )
            ).scalar_one()
            assert "owner_kind = 'legacy_worker_claim'" in legacy_release
            assert "'protected_worker_claim'" not in legacy_release

        with engine.connect() as connection:
            transaction = connection.begin()
            with pytest.raises(IntegrityError):
                _insert_reservation(
                    connection,
                    trial_id=rejected_trial_id,
                    team_id=team_id,
                    owner_kind="protected_worker_claim",
                )
            transaction.rollback()

        command.upgrade(config, "head")
        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == _head_revision(config)
            assert (
                connection.execute(
                    text("SELECT to_regprocedure(:function)"),
                    {"function": signature},
                ).scalar_one()
                is not None
            )
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM pg_trigger "
                        "WHERE tgrelid = 'public.trials'::regclass "
                        "AND tgname = :trigger_name AND NOT tgisinternal"
                    ),
                    {"trigger_name": trigger_name},
                ).scalar_one()
                == 1
            )
            assert (
                connection.execute(
                    text("SELECT has_function_privilege('public', :function, 'EXECUTE')"),
                    {"function": signature},
                ).scalar_one()
                is False
            )
            protected_release = connection.execute(
                text(
                    "SELECT pg_get_functiondef("
                    "'public.loom_release_legacy_execution_admission()'::regprocedure)"
                )
            ).scalar_one()
            assert "'protected_worker_claim'" in protected_release
    finally:
        command.upgrade(config, "head")
        engine.dispose()


def test_0127_keeps_legacy_admission_release_behavior(
    isolated_migration_postgres_url: str,
) -> None:
    engine = create_engine(isolated_migration_postgres_url)
    team_id = uuid4()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
                {"id": team_id, "name": f"legacy-release-{team_id}"},
            )
            trial_id = _seed_trial(connection, team_id=team_id, state="claimed")
            policy_id = uuid4()
            connection.execute(
                text(
                    "INSERT INTO execution_admission_policies "
                    "(id, scope_kind, scope_key, max_concurrent, active_count, enabled) "
                    "VALUES (:id, 'global', '*', 2, 1, true)"
                ),
                {"id": policy_id},
            )
            reservation_id = _insert_reservation(
                connection,
                trial_id=trial_id,
                team_id=team_id,
                owner_kind="legacy_worker_claim",
            )
            connection.execute(
                text("UPDATE trials SET state = 'failed' WHERE id = :trial_id"),
                {"trial_id": trial_id},
            )

        with engine.connect() as connection:
            reservation = (
                connection.execute(
                    text(
                        "SELECT state, release_reason, released_at IS NOT NULL AS released "
                        "FROM execution_admission_reservations WHERE id = :id"
                    ),
                    {"id": reservation_id},
                )
                .mappings()
                .one()
            )
            assert dict(reservation) == {
                "state": "released",
                "release_reason": "trial_left_active_state",
                "released": True,
            }
            assert (
                connection.execute(
                    text("SELECT active_count FROM execution_admission_policies WHERE id = :id"),
                    {"id": policy_id},
                ).scalar_one()
                == 0
            )
    finally:
        engine.dispose()


def test_0127_releases_protected_reservation_by_monotonic_attempt_identity(
    isolated_migration_postgres_url: str,
) -> None:
    engine = create_engine(isolated_migration_postgres_url)
    team_id = uuid4()
    try:
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
                {"id": team_id, "name": f"protected-refund-{team_id}"},
            )
            trial_id = _seed_trial(connection, team_id=team_id, state="claimed")
            policy_id = uuid4()
            connection.execute(
                text(
                    "INSERT INTO execution_admission_policies "
                    "(id, scope_kind, scope_key, max_concurrent, active_count, enabled) "
                    "VALUES (:id, 'global', '*', 2, 1, true)"
                ),
                {"id": policy_id},
            )
            reservation_id = _insert_reservation(
                connection,
                trial_id=trial_id,
                team_id=team_id,
                owner_kind="protected_worker_claim",
                attempt=2,
            )
            connection.execute(
                text("UPDATE trials SET state = 'protected-pending' WHERE id = :trial_id"),
                {"trial_id": trial_id},
            )

        with engine.connect() as connection:
            reservation = (
                connection.execute(
                    text(
                        "SELECT state, release_reason, released_at IS NOT NULL AS released "
                        "FROM execution_admission_reservations WHERE id = :id"
                    ),
                    {"id": reservation_id},
                )
                .mappings()
                .one()
            )
            assert dict(reservation) == {
                "state": "released",
                "release_reason": "trial_left_active_state",
                "released": True,
            }
            assert (
                connection.execute(
                    text("SELECT active_count FROM execution_admission_policies WHERE id = :id"),
                    {"id": policy_id},
                ).scalar_one()
                == 0
            )
    finally:
        engine.dispose()
