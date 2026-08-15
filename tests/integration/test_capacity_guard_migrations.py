"""Independent protected-admission migration and ownership constraints."""

from __future__ import annotations

import os
from collections.abc import Iterator
from configparser import ConfigParser
from contextlib import contextmanager
from logging import INFO, Formatter, LogRecord
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from psycopg.errors import InsufficientPrivilege
from sqlalchemy import Engine, create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, IntegrityError, ProgrammingError
from sqlalchemy.ext.asyncio import create_async_engine

from loom.db.schema import Task, Team, TeamQuota, Trial
from loom_capacity_guard.schema_startup import assert_capacity_guard_schema_at_head

EXPECTED_GUARD_TABLES = {
    "capacity_guard_alembic_version",
    "authority_state",
    "atomic_trial_submissions",
    "trial_requirements",
    "trial_attempts",
    "audit_events",
    "agent_runtime_authority",
    "agent_registrations",
    "agent_reporter_state",
    "demand_observations",
    "prepared_admission_plans",
    "prepared_worker_shapes",
    "prepared_placement_allowances",
    "prepared_bootstrap_bindings",
    "prepared_worker_bindings",
    "protected_release_acknowledgements",
    "protected_executable_bootstrap_registrations",
    "claim_guard_activation",
    "attempt_lifecycle_events",
    "attempt_lifecycle_heads",
    "attempt_lifecycle_projection_blockers",
    "attempt_lifecycle_projection_resolutions",
    "protected_claim_leases",
    "legacy_compatibility_preparations",
    "legacy_writer_cursors",
    "legacy_compatibility_freezes",
    "executable_admission_authority",
    "executable_admission_events",
    "executable_claim_state",
    "executable_claim_leases",
    "executable_claim_terminal_events",
}


def _value(database: dict[str, object], key: str) -> str:
    value = database[key]
    assert isinstance(value, str)
    return value


@pytest.mark.asyncio
async def test_guard_schema_startup_returns_numeric_head(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_async_engine(_value(capacity_guard_database, "migrator_url"))
    try:
        assert await assert_capacity_guard_schema_at_head(engine) == 13
    finally:
        await engine.dispose()


@contextmanager
def _owner_connection(database: dict[str, object]) -> Iterator[Any]:
    engine = create_engine(_value(database, "migrator_url"))
    owner = _value(database, "owner_role")
    quoted_owner = engine.dialect.identifier_preparer.quote(owner)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f"SET LOCAL ROLE {quoted_owner}")
            yield connection
    finally:
        engine.dispose()


def _seed_trial(engine: Engine) -> UUID:
    team_id = uuid4()
    trial_id = uuid4()
    task_id = f"guard-task-{uuid4().hex}"
    with engine.begin() as connection:
        connection.execute(Team.__table__.insert().values(id=team_id, name=f"guard-{team_id}"))
        connection.execute(TeamQuota.__table__.insert().values(team_id=team_id))
        connection.execute(
            Task.__table__.insert().values(
                id=task_id,
                checksum="0" * 64,
                config={"schema_version": "1"},
            )
        )
        connection.execute(
            Trial.__table__.insert().values(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={},
                requires_caps={
                    "os": "linux",
                    "cpu_arch": "x86_64",
                    "gpu_vendor": "none",
                    "network_policies": ["public"],
                },
                state="queued",
            )
        )
    return trial_id


def _insert_foundation_rows(connection: Any, trial_id: UUID) -> tuple[UUID, UUID]:
    protected_attempt_id = uuid4()
    subject_id = uuid4()
    requirement_digest = "a" * 64
    connection.execute(
        text(
            "INSERT INTO loom_capacity_guard.authority_state "
            "(singleton_id, schema_version, environment_id, subject_id, "
            "subject_incarnation, authority_mode, authority_incarnation, "
            "reporter_incarnation, reporter_high_water, allocation_epoch, "
            "deployment_generation, configuration_generation, candidate_digest) "
            "VALUES (1, 1, 'dev-alice', :subject_id, :subject_incarnation, "
            "'disabled', :authority_incarnation, :reporter_incarnation, 0, 0, 1, 1, :digest)"
        ),
        {
            "subject_id": subject_id,
            "subject_incarnation": uuid4(),
            "authority_incarnation": uuid4(),
            "reporter_incarnation": uuid4(),
            "digest": "b" * 64,
        },
    )
    connection.execute(
        text(
            "INSERT INTO loom_capacity_guard.trial_requirements "
            "(trial_id, schema_version, requirements_digest, requirements) "
            "VALUES (:trial_id, 1, :digest, :requirements)"
        ),
        {
            "trial_id": trial_id,
            "digest": requirement_digest,
            "requirements": '{"schema_version":1}',
        },
    )
    connection.execute(
        text(
            "INSERT INTO loom_capacity_guard.trial_attempts "
            "(protected_attempt_id, trial_id, execution_generation, "
            "requirements_digest, claim_state) "
            "VALUES (:attempt_id, :trial_id, 1, :digest, 'queued')"
        ),
        {
            "attempt_id": protected_attempt_id,
            "trial_id": trial_id,
            "digest": requirement_digest,
        },
    )
    connection.execute(
        text(
            "INSERT INTO loom_capacity_guard.audit_events "
            "(event_type, trial_id, protected_attempt_id, payload, payload_digest) "
            "VALUES ('trial_registered.v1', :trial_id, :attempt_id, "
            ":payload, :digest)"
        ),
        {
            "trial_id": trial_id,
            "attempt_id": protected_attempt_id,
            "payload": '{"schema_version":1}',
            "digest": "c" * 64,
        },
    )
    return protected_attempt_id, subject_id


def test_guard_schema_has_exact_owner_and_preserves_public_application_tables(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    owner = _value(capacity_guard_database, "owner_role")
    try:
        with engine.connect() as connection:
            assert set(inspect(connection).get_table_names(schema="loom_capacity_guard")) == (
                EXPECTED_GUARD_TABLES
            )
            revision = connection.execute(
                text("SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version")
            ).scalar_one()
            assert revision == "guard_0013"
            public_before = capacity_guard_database["public_tables_before"]
            assert isinstance(public_before, frozenset)
            assert frozenset(inspect(connection).get_table_names(schema="public")) == public_before

            schema_owner = connection.execute(
                text(
                    "SELECT pg_get_userbyid(nspowner) FROM pg_namespace "
                    "WHERE nspname = 'loom_capacity_guard'"
                )
            ).scalar_one()
            assert schema_owner == owner
            assert (
                connection.execute(
                    text("SELECT rolcanlogin FROM pg_roles WHERE rolname = :owner"),
                    {"owner": owner},
                ).scalar_one()
                is False
            )

            object_owners = (
                connection.execute(
                    text(
                        "SELECT DISTINCT pg_get_userbyid(c.relowner) "
                        "FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace "
                        "WHERE n.nspname = 'loom_capacity_guard' "
                        "AND c.relkind IN ('r','S')"
                    )
                )
                .scalars()
                .all()
            )
            assert object_owners == [owner]
            function_owners = (
                connection.execute(
                    text(
                        "SELECT DISTINCT pg_get_userbyid(p.proowner) "
                        "FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid = p.pronamespace "
                        "WHERE n.nspname = 'loom_capacity_guard'"
                    )
                )
                .scalars()
                .all()
            )
            assert function_owners == [owner]

            activation = (
                connection.execute(
                    text(
                        "SELECT activation_state, authority_mode, activation_epoch, "
                        "executable_new_capacity_ceiling, live_claim_entry_enabled "
                        "FROM loom_capacity_guard.claim_guard_activation"
                    )
                )
                .mappings()
                .one()
            )
            assert dict(activation) == {
                "activation_state": "disabled",
                "authority_mode": "disabled",
                "activation_epoch": 0,
                "executable_new_capacity_ceiling": 0,
                "live_claim_entry_enabled": False,
            }
    finally:
        engine.dispose()


def test_guard_constraints_fail_closed_and_bind_exact_requirement(
    capacity_guard_database: dict[str, object],
) -> None:
    admin_engine = create_engine(_value(capacity_guard_database, "admin_url"))
    trial_id = _seed_trial(admin_engine)
    other_trial_id = _seed_trial(admin_engine)
    try:
        with pytest.raises(IntegrityError):
            with _owner_connection(capacity_guard_database) as connection:
                connection.execute(
                    text(
                        "INSERT INTO loom_capacity_guard.authority_state "
                        "(singleton_id, schema_version, environment_id, subject_id, "
                        "subject_incarnation, authority_mode, authority_incarnation, "
                        "reporter_incarnation, reporter_high_water, allocation_epoch, "
                        "deployment_generation, configuration_generation, candidate_digest) "
                        "VALUES (1, 1, 'dev-alice', :subject_id, :subject_incarnation, "
                        "'global', :authority_incarnation, :reporter_incarnation, "
                        "0, 0, 1, 1, :digest)"
                    ),
                    {
                        "subject_id": uuid4(),
                        "subject_incarnation": uuid4(),
                        "authority_incarnation": uuid4(),
                        "reporter_incarnation": uuid4(),
                        "digest": "b" * 64,
                    },
                )
        with _owner_connection(capacity_guard_database) as connection:
            attempt_id, _ = _insert_foundation_rows(connection, trial_id)

        invalid_statements = (
            (
                "INSERT INTO loom_capacity_guard.trial_attempts "
                "(protected_attempt_id, trial_id, execution_generation, "
                "requirements_digest, claim_state) VALUES "
                "(:id, :trial, 0, :digest, 'queued')",
                {"id": uuid4(), "trial": trial_id, "digest": "a" * 64},
            ),
            (
                "INSERT INTO loom_capacity_guard.trial_attempts "
                "(protected_attempt_id, trial_id, execution_generation, "
                "requirements_digest, claim_state) VALUES "
                "(:id, :trial, 2, :digest, 'claimed')",
                {"id": uuid4(), "trial": trial_id, "digest": "a" * 64},
            ),
            (
                "INSERT INTO loom_capacity_guard.trial_attempts "
                "(protected_attempt_id, trial_id, execution_generation, "
                "requirements_digest, claim_state) VALUES "
                "(:id, :trial, 2, :digest, 'queued')",
                {"id": uuid4(), "trial": trial_id, "digest": "d" * 64},
            ),
            (
                "INSERT INTO loom_capacity_guard.trial_attempts "
                "(protected_attempt_id, trial_id, execution_generation, "
                "requirements_digest, claim_state) VALUES "
                "(:id, :trial, 1, :digest, 'queued')",
                {"id": uuid4(), "trial": trial_id, "digest": "a" * 64},
            ),
            (
                "INSERT INTO loom_capacity_guard.audit_events "
                "(event_type, trial_id, protected_attempt_id, payload, payload_digest) "
                "VALUES ('trial_registered.v1', :trial, :attempt, '{}'::jsonb, :digest)",
                {
                    "trial": other_trial_id,
                    "attempt": attempt_id,
                    "digest": "e" * 64,
                },
            ),
        )
        for statement, parameters in invalid_statements:
            with pytest.raises(IntegrityError):
                with _owner_connection(capacity_guard_database) as connection:
                    connection.execute(text(statement), parameters)

        with pytest.raises(IntegrityError):
            with admin_engine.begin() as connection:
                connection.execute(Trial.__table__.delete().where(Trial.id == trial_id))
    finally:
        admin_engine.dispose()


def test_guard_json_and_digests_are_bounded(
    capacity_guard_database: dict[str, object],
) -> None:
    admin_engine = create_engine(_value(capacity_guard_database, "admin_url"))
    trial_id = _seed_trial(admin_engine)
    try:
        cases = (
            (
                "INSERT INTO loom_capacity_guard.trial_requirements "
                "(trial_id, schema_version, requirements_digest, requirements) "
                "VALUES (:trial, 1, 'not-a-digest', '{}'::jsonb)",
                {"trial": trial_id},
            ),
            (
                "INSERT INTO loom_capacity_guard.trial_requirements "
                "(trial_id, schema_version, requirements_digest, requirements) "
                "VALUES (:trial, 1, :digest, "
                "jsonb_build_object('value', repeat('x', 8388609)))",
                {"trial": trial_id, "digest": "a" * 64},
            ),
            (
                "INSERT INTO loom_capacity_guard.audit_events "
                "(event_type, payload, payload_digest) VALUES "
                "('bounded.v1', jsonb_build_object('value', repeat('x', 16385)), :digest)",
                {"digest": "b" * 64},
            ),
        )
        for statement, parameters in cases:
            with pytest.raises(IntegrityError):
                with _owner_connection(capacity_guard_database) as connection:
                    connection.execute(text(statement), parameters)
    finally:
        admin_engine.dispose()


def test_guard_rows_are_append_only(
    capacity_guard_database: dict[str, object],
) -> None:
    admin_engine = create_engine(_value(capacity_guard_database, "admin_url"))
    trial_id = _seed_trial(admin_engine)
    try:
        with _owner_connection(capacity_guard_database) as connection:
            _insert_foundation_rows(connection, trial_id)

        tables_and_keys = (
            ("authority_state", "singleton_id = singleton_id", "singleton_id = 1"),
            ("trial_requirements", "trial_id = trial_id", f"trial_id = '{trial_id}'"),
            ("trial_attempts", "trial_id = trial_id", f"trial_id = '{trial_id}'"),
            ("audit_events", "event_type = event_type", f"trial_id = '{trial_id}'"),
        )
        for table, assignment, predicate in tables_and_keys:
            for verb in ("UPDATE", "DELETE", "TRUNCATE"):
                statement = (
                    f"UPDATE loom_capacity_guard.{table} SET {assignment} WHERE {predicate}"
                    if verb == "UPDATE"
                    else (
                        f"DELETE FROM loom_capacity_guard.{table} WHERE {predicate}"
                        if verb == "DELETE"
                        else f"TRUNCATE loom_capacity_guard.{table} CASCADE"
                    )
                )
                with pytest.raises(DBAPIError, match=r"append-only|generation"):
                    with _owner_connection(capacity_guard_database) as connection:
                        connection.execute(text(statement))
    finally:
        admin_engine.dispose()


def test_candidate_role_has_no_protected_privileges(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    candidate = f"candidate_runtime_test_{uuid4().hex[:12]}"
    quoted_candidate = engine.dialect.identifier_preparer.quote(candidate)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f"CREATE ROLE {quoted_candidate} NOLOGIN NOSUPERUSER "
                "NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
            )
            connection.exec_driver_sql(f"GRANT USAGE ON SCHEMA public TO {quoted_candidate}")

        with _owner_connection(capacity_guard_database) as connection:
            connection.exec_driver_sql(
                "CREATE TABLE loom_capacity_guard.future_default_test (id bigint)"
            )
            connection.exec_driver_sql(
                "CREATE SEQUENCE loom_capacity_guard.future_default_sequence"
            )
            connection.exec_driver_sql(
                "CREATE FUNCTION loom_capacity_guard.future_default_function() "
                "RETURNS integer LANGUAGE sql SET search_path = pg_catalog "
                "AS 'SELECT 1'"
            )

        with engine.connect() as connection:
            future_privileges = (
                connection.execute(
                    text(
                        "SELECT "
                        "has_table_privilege(:candidate, "
                        "'loom_capacity_guard.future_default_test', 'SELECT') AS table_select, "
                        "has_sequence_privilege(:candidate, "
                        "'loom_capacity_guard.future_default_sequence', 'USAGE') AS sequence_usage, "
                        "has_function_privilege(:candidate, "
                        "'loom_capacity_guard.future_default_function()', 'EXECUTE') "
                        "AS function_execute"
                    ),
                    {"candidate": candidate},
                )
                .mappings()
                .one()
            )
        assert dict(future_privileges) == {
            "table_select": False,
            "sequence_usage": False,
            "function_execute": False,
        }

        statements = [
            f"SELECT * FROM loom_capacity_guard.{table} LIMIT 1" for table in EXPECTED_GUARD_TABLES
        ]
        statements.extend(
            [
                "INSERT INTO loom_capacity_guard.audit_events "
                "(event_type, payload, payload_digest) "
                "VALUES ('candidate.v1', '{}'::jsonb, '" + "a" * 64 + "')",
                "UPDATE loom_capacity_guard.authority_state "
                "SET reporter_high_water = reporter_high_water",
                "DELETE FROM loom_capacity_guard.audit_events WHERE false",
                "CREATE TABLE loom_capacity_guard.candidate_escape (id bigint)",
                "CREATE SCHEMA candidate_escape",
                "SELECT loom_capacity_guard.reject_append_only_mutation()",
                "SELECT loom_capacity_guard.capture_demand_observation("
                "'00000000-0000-0000-0000-000000000001'::uuid, 0, 100)",
                "SELECT loom_capacity_guard.capture_lifecycle_demand_observation("
                "'00000000-0000-0000-0000-000000000001'::uuid, 0, 100)",
                "SELECT loom_capacity_guard.capture_demand_observation_v1_legacy("
                "'00000000-0000-0000-0000-000000000001'::uuid, 0, 100)",
                "SELECT loom_capacity_guard.prepare_inert_admission_plan("
                "'00000000-0000-0000-0000-000000000001'::uuid, '{}'::jsonb, "
                "'{}'::bytea, '" + "a" * 64 + "')",
                "SELECT loom_capacity_guard.apply_inert_attempt_transition("
                "'00000000-0000-0000-0000-000000000001'::uuid, '{}'::jsonb, "
                "'{}'::bytea, '" + "a" * 64 + "')",
                "SELECT loom_capacity_guard.inspect_inert_claim_proposal("
                "'00000000-0000-0000-0000-000000000001'::uuid, '{}'::jsonb, "
                "'{}'::bytea, '" + "a" * 64 + "')",
                "SELECT loom_capacity_guard.register_inert_trial_submission("
                "'00000000-0000-0000-0000-000000000001'::uuid, '{}'::jsonb, "
                "'{}'::bytea, '" + "a" * 64 + "', '{}'::bytea, '" + "b" * 64 + "')",
                "SELECT loom_capacity_guard.submit_inert_trial_projection("
                "'00000000-0000-0000-0000-000000000001'::uuid, '{}'::jsonb, "
                "'{}'::bytea, '"
                + "a" * 64
                + "', '{}'::jsonb, '{}'::bytea, '"
                + "b" * 64
                + "', '{}'::bytea, '"
                + "c" * 64
                + "')",
                "SELECT loom_capacity_guard.protect_executable_bootstrap("
                "'00000000-0000-0000-0000-000000000001'::uuid, '{}'::jsonb, "
                "'{}'::bytea, '" + "a" * 64 + "', '" + "b" * 64 + "')",
            ]
        )
        for statement in statements:
            with engine.connect() as connection:
                transaction = connection.begin()
                connection.exec_driver_sql(f"SET LOCAL ROLE {quoted_candidate}")
                with pytest.raises(DBAPIError) as caught:
                    connection.execute(text(statement))
                assert isinstance(caught.value.orig, InsufficientPrivilege)
                transaction.rollback()
    finally:
        with engine.begin() as connection:
            connection.exec_driver_sql("DROP SCHEMA IF EXISTS candidate_escape CASCADE")
            connection.exec_driver_sql(f"REVOKE USAGE ON SCHEMA public FROM {quoted_candidate}")
            connection.exec_driver_sql(f"DROP ROLE IF EXISTS {quoted_candidate}")
        engine.dispose()


def test_guard_owner_has_only_bounded_public_submission_privileges(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    owner = _value(capacity_guard_database, "owner_role")
    try:
        with engine.connect() as connection:
            privileges = (
                connection.execute(
                    text(
                        "SELECT has_table_privilege(:owner, 'public.trials', 'INSERT') "
                        "AS can_insert, "
                        "has_table_privilege(:owner, 'public.trials', 'UPDATE') "
                        "AS can_update, "
                        "has_table_privilege(:owner, 'public.trials', 'DELETE') "
                        "AS can_delete, "
                        "has_column_privilege(:owner, 'public.trials', 'config', 'INSERT') "
                        "AS can_insert_config, "
                        "has_column_privilege(:owner, 'public.trials', 'config', 'SELECT') "
                        "AS can_select_config, "
                        "has_column_privilege(:owner, 'public.trials', "
                        "'lifecycle_authority_id', 'UPDATE') AS can_bind_lifecycle, "
                        "has_column_privilege(:owner, 'public.trials', 'state', 'UPDATE') "
                        "AS can_fence_state, "
                        "has_table_privilege(:owner, 'public.trials', 'REFERENCES') "
                        "AS can_reference_trial_table, "
                        "has_column_privilege(:owner, 'public.trials', 'id', 'REFERENCES') "
                        "AS can_reference_trial_id, "
                        "has_column_privilege(:owner, 'public.trials', 'worker_id', 'UPDATE') "
                        "AS can_update_worker, "
                        "has_table_privilege(:owner, 'public.data_lifecycle_authorities', "
                        "'INSERT') AS can_insert_lifecycle_table, "
                        "has_column_privilege(:owner, 'public.data_lifecycle_authorities', "
                        "'environment', 'INSERT') AS can_insert_lifecycle_environment, "
                        "has_column_privilege(:owner, 'public.data_lifecycle_authorities', "
                        "'id', 'SELECT') AS can_select_lifecycle_id, "
                        "has_column_privilege(:owner, 'public.data_lifecycle_authorities', "
                        "'environment', 'SELECT') AS can_select_lifecycle_environment, "
                        "has_column_privilege(:owner, 'public.data_lifecycle_authorities', "
                        "'deletion_token', 'INSERT') AS can_insert_lifecycle_deletion_token, "
                        "has_table_privilege(:owner, 'public.data_lifecycle_authorities', "
                        "'REFERENCES') AS can_reference_lifecycle_table, "
                        "has_column_privilege(:owner, 'public.data_lifecycle_authorities', "
                        "'id', 'REFERENCES') AS can_reference_lifecycle_id"
                    ),
                    {"owner": owner},
                )
                .mappings()
                .one()
            )
        assert dict(privileges) == {
            "can_insert": False,
            "can_update": False,
            "can_delete": False,
            "can_insert_config": True,
            "can_select_config": False,
            "can_bind_lifecycle": True,
            "can_fence_state": True,
            "can_reference_trial_table": False,
            "can_reference_trial_id": True,
            "can_update_worker": False,
            "can_insert_lifecycle_table": False,
            "can_insert_lifecycle_environment": True,
            "can_select_lifecycle_id": True,
            "can_select_lifecycle_environment": False,
            "can_insert_lifecycle_deletion_token": False,
            "can_reference_lifecycle_table": False,
            "can_reference_lifecycle_id": True,
        }
    finally:
        engine.dispose()


def _guard_config(database: dict[str, object]) -> AlembicConfig:
    root = Path(__file__).resolve().parents[2]
    cfg = AlembicConfig(str(root / "capacity_guard_migrations" / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "capacity_guard_migrations"))
    os.environ["LOOM_CAPACITY_GUARD_DB_URL"] = _value(database, "migrator_url")
    os.environ["LOOM_CAPACITY_GUARD_OWNER_ROLE"] = _value(database, "owner_role")
    os.environ["LOOM_CAPACITY_GUARD_AGENT_ROLE"] = _value(database, "agent_role")
    os.environ["LOOM_CAPACITY_GUARD_EXECUTOR_ROLE"] = _value(database, "executor_role")
    return cfg


def test_guard_migration_downgrades_and_reupgrades_without_public_changes(
    capacity_guard_database: dict[str, object],
) -> None:
    cfg = _guard_config(capacity_guard_database)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    public_before = capacity_guard_database["public_tables_before"]
    assert isinstance(public_before, frozenset)
    try:
        command.downgrade(cfg, "base")
        with engine.connect() as connection:
            assert set(inspect(connection).get_table_names(schema="loom_capacity_guard")) == {
                "capacity_guard_alembic_version"
            }
            assert frozenset(inspect(connection).get_table_names(schema="public")) == public_before
        command.upgrade(cfg, "head")
        with engine.connect() as connection:
            assert set(inspect(connection).get_table_names(schema="loom_capacity_guard")) == (
                EXPECTED_GUARD_TABLES
            )
    finally:
        engine.dispose()


def test_lifecycle_projection_backfills_existing_terminal_public_blocker(
    capacity_guard_database: dict[str, object],
) -> None:
    cfg = _guard_config(capacity_guard_database)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    trial_id = _seed_trial(engine)
    transition_id = uuid4()
    try:
        command.downgrade(cfg, "guard_0005")
        with _owner_connection(capacity_guard_database) as connection:
            protected_attempt_id, _subject_id = _insert_foundation_rows(connection, trial_id)
            connection.execute(
                text(
                    "INSERT INTO loom_capacity_guard.attempt_lifecycle_events "
                    "(transition_id, protected_attempt_id, execution_generation, "
                    "requirements_digest, transition_sequence, operation, previous_state, "
                    "lifecycle_state, executable, payload, payload_digest) "
                    "SELECT :transition_id, protected_attempt_id, execution_generation, "
                    "requirements_digest, 1, 'cancel', 'pending-unassigned', "
                    "'cancelled-terminal', false, "
                    "jsonb_build_object('schema_version', 1, 'operation', 'cancel'), "
                    ":payload_digest "
                    "FROM loom_capacity_guard.trial_attempts "
                    "WHERE protected_attempt_id = :protected_attempt_id"
                ),
                {
                    "transition_id": transition_id,
                    "protected_attempt_id": protected_attempt_id,
                    "payload_digest": "d" * 64,
                },
            )

        command.upgrade(cfg, "head")
        with engine.connect() as connection:
            blocker = (
                connection.execute(
                    text(
                        "SELECT transition_id, protected_attempt_id, blocker_reason, executable "
                        "FROM loom_capacity_guard.attempt_lifecycle_projection_blockers"
                    )
                )
                .mappings()
                .one()
            )
        assert dict(blocker) == {
            "transition_id": transition_id,
            "protected_attempt_id": protected_attempt_id,
            "blocker_reason": "terminal-public-queued",
            "executable": False,
        }
    finally:
        engine.dispose()


def test_lifecycle_projection_has_bounded_unresolved_blocker_access_path(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            transaction = connection.begin()
            connection.exec_driver_sql("SET LOCAL enable_seqscan = off")
            connection.exec_driver_sql("SET LOCAL enable_bitmapscan = off")
            plan = "\n".join(
                connection.execute(
                    text(
                        "EXPLAIN (COSTS OFF) SELECT transition_id "
                        "FROM loom_capacity_guard.attempt_lifecycle_projection_blockers "
                        "WHERE resolved_at IS NULL LIMIT 1"
                    )
                ).scalars()
            )
            demand_plan = "\n".join(
                connection.execute(
                    text(
                        "EXPLAIN (COSTS OFF) SELECT protected_attempt_id "
                        "FROM loom_capacity_guard.attempt_lifecycle_heads "
                        "WHERE lifecycle_state IN ('pending-unassigned', 'assigned') "
                        "ORDER BY protected_attempt_id LIMIT 101"
                    )
                ).scalars()
            )
            transaction.rollback()
        assert "guard_lifecycle_projection_unresolved_blocker_key" in plan
        assert "Seq Scan" not in plan
        assert "guard_lifecycle_current_demand_key" in demand_plan
        assert "Sort" not in demand_plan
    finally:
        engine.dispose()


def test_guard_alembic_environment_has_no_database_fallback() -> None:
    source = Path("capacity_guard_migrations/env.py").read_text(encoding="utf-8")
    assert "LOOM_CAPACITY_GUARD_DB_URL" in source
    assert "LOOM_CAPACITY_GUARD_OWNER_ROLE" in source
    assert "LOOM_CAPACITY_GUARD_AGENT_ROLE" in source
    assert "LOOM_CAPACITY_GUARD_EXECUTOR_ROLE" in source
    assert "LOOM_DB_URL" not in source
    assert "LOOM_CP_DB_URL" not in source
    assert "LOOM_CAPACITY_DB_URL" not in source


def test_guard_alembic_logging_formatter_is_valid() -> None:
    config = ConfigParser(interpolation=None)
    assert config.read("capacity_guard_migrations/alembic.ini")
    formatter = Formatter(
        config["formatter_generic"]["format"],
        datefmt=config["formatter_generic"]["datefmt"],
    )
    record = LogRecord("alembic", INFO, __file__, 1, "migration ready", (), None)
    assert "migration ready" in formatter.format(record)


def test_guard_migration_requires_explicit_canonical_settings(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    cfg = AlembicConfig(str(root / "capacity_guard_migrations" / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "capacity_guard_migrations"))

    monkeypatch.delenv("LOOM_CAPACITY_GUARD_DB_URL", raising=False)
    monkeypatch.delenv("LOOM_CAPACITY_GUARD_OWNER_ROLE", raising=False)
    monkeypatch.delenv("LOOM_CAPACITY_GUARD_AGENT_ROLE", raising=False)
    monkeypatch.delenv("LOOM_CAPACITY_GUARD_EXECUTOR_ROLE", raising=False)
    with pytest.raises(RuntimeError, match="LOOM_CAPACITY_GUARD_DB_URL"):
        command.current(cfg)

    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_DB_URL", _value(capacity_guard_database, "migrator_url")
    )
    monkeypatch.setenv("LOOM_CAPACITY_GUARD_OWNER_ROLE", "invalid-owner")
    with pytest.raises(RuntimeError, match="LOOM_CAPACITY_GUARD_OWNER_ROLE"):
        command.current(cfg)

    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_OWNER_ROLE", _value(capacity_guard_database, "owner_role")
    )
    with pytest.raises(RuntimeError, match="LOOM_CAPACITY_GUARD_AGENT_ROLE"):
        command.current(cfg)


def test_guard_migration_login_must_be_owner_member(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    outsider = f"guard_outsider_test_{uuid4().hex[:12]}"
    password = f"outsider-test-{uuid4().hex}"
    quoted_outsider = engine.dialect.identifier_preparer.quote(outsider)
    outsider_url = make_url(_value(capacity_guard_database, "admin_url")).set(
        username=outsider,
        password=password,
    )
    root = Path(__file__).resolve().parents[2]
    cfg = AlembicConfig(str(root / "capacity_guard_migrations" / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "capacity_guard_migrations"))
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f"CREATE ROLE {quoted_outsider} LOGIN NOSUPERUSER NOCREATEDB "
                "NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS "
                f"PASSWORD '{password}'"
            )
        monkeypatch.setenv(
            "LOOM_CAPACITY_GUARD_DB_URL",
            outsider_url.render_as_string(hide_password=False),
        )
        monkeypatch.setenv(
            "LOOM_CAPACITY_GUARD_OWNER_ROLE", _value(capacity_guard_database, "owner_role")
        )
        monkeypatch.setenv(
            "LOOM_CAPACITY_GUARD_AGENT_ROLE", _value(capacity_guard_database, "agent_role")
        )
        monkeypatch.setenv(
            "LOOM_CAPACITY_GUARD_EXECUTOR_ROLE",
            _value(capacity_guard_database, "executor_role"),
        )
        with pytest.raises(ProgrammingError) as caught:
            command.current(cfg)
        assert isinstance(caught.value.orig, InsufficientPrivilege)
    finally:
        with engine.begin() as connection:
            connection.exec_driver_sql(f"DROP ROLE IF EXISTS {quoted_outsider}")
        engine.dispose()


def test_guard_migration_rejects_superuser_login(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    cfg = AlembicConfig(str(root / "capacity_guard_migrations" / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "capacity_guard_migrations"))
    monkeypatch.setenv("LOOM_CAPACITY_GUARD_DB_URL", _value(capacity_guard_database, "admin_url"))
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_OWNER_ROLE", _value(capacity_guard_database, "owner_role")
    )
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_AGENT_ROLE", _value(capacity_guard_database, "agent_role")
    )
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_EXECUTOR_ROLE",
        _value(capacity_guard_database, "executor_role"),
    )
    with pytest.raises(RuntimeError, match="least-privileged"):
        command.current(cfg)


def test_guard_migration_rejects_broad_nonlogin_owner(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    bad_owner = f"guard_broad_owner_test_{uuid4().hex[:12]}"
    quoted_bad_owner = engine.dialect.identifier_preparer.quote(bad_owner)
    quoted_migrator = engine.dialect.identifier_preparer.quote(
        _value(capacity_guard_database, "migrator_role")
    )
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f"CREATE ROLE {quoted_bad_owner} NOLOGIN NOSUPERUSER CREATEDB "
                "NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
            )
            connection.exec_driver_sql(f"GRANT {quoted_bad_owner} TO {quoted_migrator}")
        cfg = _guard_config(capacity_guard_database)
        monkeypatch.setenv("LOOM_CAPACITY_GUARD_OWNER_ROLE", bad_owner)
        with pytest.raises(RuntimeError, match="least-privileged"):
            command.current(cfg)
    finally:
        with engine.begin() as connection:
            connection.exec_driver_sql(f"REVOKE {quoted_bad_owner} FROM {quoted_migrator}")
            connection.exec_driver_sql(f"DROP ROLE IF EXISTS {quoted_bad_owner}")
        engine.dispose()
