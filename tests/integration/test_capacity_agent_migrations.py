"""Database authority tests for the trusted read-only demand agent."""

from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from psycopg.errors import InsufficientPrivilege, ObjectNotInPrerequisiteState
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError


def _value(database: dict[str, object], key: str) -> str:
    value = database[key]
    assert isinstance(value, str)
    return value


def _guard_config(database: dict[str, object]) -> AlembicConfig:
    root = Path(__file__).resolve().parents[2]
    cfg = AlembicConfig(str(root / "capacity_guard_migrations" / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "capacity_guard_migrations"))
    os.environ["LOOM_CAPACITY_GUARD_DB_URL"] = _value(database, "migrator_url")
    os.environ["LOOM_CAPACITY_GUARD_OWNER_ROLE"] = _value(database, "owner_role")
    os.environ["LOOM_CAPACITY_GUARD_AGENT_ROLE"] = _value(database, "agent_role")
    os.environ["LOOM_CAPACITY_GUARD_EXECUTOR_ROLE"] = _value(database, "executor_role")
    os.environ["LOOM_CAPACITY_GUARD_OBSERVER_ROLE"] = _value(database, "observer_role")
    return cfg


def test_executor_role_is_distinct_and_has_only_bounded_executable_procedures(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    executor = _value(capacity_guard_database, "executor_role")
    try:
        with engine.connect() as connection:
            role = (
                connection.execute(
                    text(
                        "SELECT rolcanlogin, rolinherit, rolsuper, rolcreatedb, "
                        "rolcreaterole, rolreplication, rolbypassrls, "
                        "pg_has_role(:executor, :owner, 'MEMBER') AS owner_member, "
                        "pg_has_role(:executor, :agent, 'MEMBER') AS agent_member, "
                        "(SELECT count(*) FROM pg_auth_members AS m "
                        "JOIN pg_roles AS r ON r.oid = m.member "
                        "WHERE r.rolname = :executor) AS role_memberships, "
                        "has_schema_privilege(:executor, 'loom_capacity_guard', 'USAGE') "
                        "AS schema_usage, "
                        "has_table_privilege(:executor, "
                        "'loom_capacity_guard.agent_registrations', 'SELECT') AS table_select, "
                        "has_sequence_privilege(:executor, "
                        "'loom_capacity_guard.demand_observations_observation_id_seq', "
                        "'USAGE') AS sequence_usage "
                        "FROM pg_roles WHERE rolname = :executor"
                    ),
                    {
                        "executor": executor,
                        "owner": _value(capacity_guard_database, "owner_role"),
                        "agent": _value(capacity_guard_database, "agent_role"),
                    },
                )
                .mappings()
                .one()
            )
            routines = {
                row[0]
                for row in connection.execute(
                    text(
                        "SELECT routine_name FROM information_schema.role_routine_grants "
                        "WHERE grantee = :executor AND routine_schema = 'loom_capacity_guard'"
                    ),
                    {"executor": executor},
                ).all()
            }
            agent_executable_routines = {
                row[0]
                for row in connection.execute(
                    text(
                        "SELECT routine_name FROM information_schema.role_routine_grants "
                        "WHERE grantee = :agent AND routine_schema = 'loom_capacity_guard' "
                        "AND routine_name = ANY(:routines)"
                    ),
                    {
                        "agent": _value(capacity_guard_database, "agent_role"),
                        "routines": list(routines),
                    },
                ).all()
            }
        assert dict(role) == {
            "rolcanlogin": True,
            "rolinherit": False,
            "rolsuper": False,
            "rolcreatedb": False,
            "rolcreaterole": False,
            "rolreplication": False,
            "rolbypassrls": False,
            "owner_member": False,
            "agent_member": False,
            "role_memberships": 0,
            "schema_usage": True,
            "table_select": False,
            "sequence_usage": False,
        }
        assert routines == {
            "acknowledge_executable_release",
            "admit_executable_claim",
            "begin_executable_worker_drain",
            "bind_executable_slurm_job",
            "observe_executable_intent",
            "prepare_executable_worker",
            "revoke_prepared_executable_bootstrap",
            "register_executable_worker",
            "withdraw_unregistered_executable_worker",
        }
        assert agent_executable_routines == set()
    finally:
        engine.dispose()


def test_guard_migration_requires_explicit_distinct_executor_role(
    capacity_guard_database: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _guard_config(capacity_guard_database)
    monkeypatch.delenv("LOOM_CAPACITY_GUARD_EXECUTOR_ROLE")
    with pytest.raises(RuntimeError, match="LOOM_CAPACITY_GUARD_EXECUTOR_ROLE"):
        command.current(cfg)
    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_EXECUTOR_ROLE", _value(capacity_guard_database, "agent_role")
    )
    with pytest.raises(RuntimeError, match="distinct canonical SQL role"):
        command.current(cfg)


def test_agent_role_is_least_privileged_and_not_an_owner_member(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            role = (
                connection.execute(
                    text(
                        "SELECT rolcanlogin, rolinherit, rolsuper, rolcreatedb, "
                        "rolcreaterole, rolreplication, rolbypassrls, "
                        "pg_has_role(:agent, :owner, 'MEMBER') AS owner_member, "
                        "(SELECT count(*) FROM pg_auth_members AS m "
                        "JOIN pg_roles AS r ON r.oid = m.member "
                        "WHERE r.rolname = :agent) AS role_memberships "
                        "FROM pg_roles WHERE rolname = :agent"
                    ),
                    {
                        "agent": _value(capacity_guard_database, "agent_role"),
                        "owner": _value(capacity_guard_database, "owner_role"),
                    },
                )
                .mappings()
                .one()
            )
        assert dict(role) == {
            "rolcanlogin": True,
            "rolinherit": False,
            "rolsuper": False,
            "rolcreatedb": False,
            "rolcreaterole": False,
            "rolreplication": False,
            "rolbypassrls": False,
            "owner_member": False,
            "role_memberships": 0,
        }
    finally:
        engine.dispose()


def test_agent_can_execute_only_the_bounded_agent_functions(
    capacity_guard_database: dict[str, object],
) -> None:
    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    agent_role = _value(capacity_guard_database, "agent_role")
    try:
        with admin.connect() as connection:
            privileges = (
                connection.execute(
                    text(
                        "SELECT "
                        "has_schema_privilege(:agent, 'loom_capacity_guard', 'USAGE') "
                        "AS schema_usage, "
                        "has_table_privilege(:agent, "
                        "'loom_capacity_guard.agent_registrations', 'SELECT') AS table_select, "
                        "has_table_privilege(:agent, "
                        "'loom_capacity_guard.agent_reporter_state', 'UPDATE') AS table_update, "
                        "has_sequence_privilege(:agent, "
                        "'loom_capacity_guard.demand_observations_observation_id_seq', 'USAGE') "
                        "AS sequence_usage, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.capture_demand_observation(uuid,bigint,integer)', "
                        "'EXECUTE') AS capture_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.capture_lifecycle_demand_observation"
                        "(uuid,bigint,integer)', 'EXECUTE') AS lifecycle_capture_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.capture_lifecycle_demand_observation_v2_queued"
                        "(uuid,bigint,integer)', 'EXECUTE') "
                        "AS queued_lifecycle_capture_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.capture_demand_observation_v1_legacy"
                        "(uuid,bigint,integer)', 'EXECUTE') AS legacy_capture_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.enforce_attempt_lifecycle_head_transition()', "
                        "'EXECUTE') AS lifecycle_head_guard_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.project_attempt_lifecycle_head()', 'EXECUTE') "
                        "AS lifecycle_head_project_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard."
                        "enforce_attempt_lifecycle_projection_blocker()', 'EXECUTE') "
                        "AS lifecycle_blocker_guard_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard."
                        "project_attempt_lifecycle_projection_resolution()', 'EXECUTE') "
                        "AS lifecycle_resolution_project_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.reject_append_only_mutation()', 'EXECUTE') "
                        "AS trigger_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.prepare_inert_admission_plan"
                        "(uuid,jsonb,bytea,text)', 'EXECUTE') AS plan_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.register_inert_bootstrap"
                        "(uuid,jsonb,bytea,text)', 'EXECUTE') AS bootstrap_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.record_inert_worker"
                        "(uuid,jsonb,bytea,text)', 'EXECUTE') AS worker_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.acknowledge_inert_protected_release"
                        "(uuid,jsonb,bytea,text)', 'EXECUTE') AS protected_release_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.assert_inert_agent_binding"
                        "(uuid,jsonb,bytea,text)', 'EXECUTE') AS binding_guard_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.apply_inert_attempt_transition"
                        "(uuid,jsonb,bytea,text)', 'EXECUTE') AS lifecycle_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.initialize_attempt_lifecycle()', 'EXECUTE') "
                        "AS lifecycle_initializer_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.inspect_inert_claim_proposal"
                        "(uuid,jsonb,bytea,text)', 'EXECUTE') AS claim_inspect_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.prepare_inert_legacy_compatibility"
                        "(uuid,jsonb,bytea,text)', 'EXECUTE') AS legacy_prepare_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.freeze_inert_legacy_compatibility"
                        "(uuid,jsonb,bytea,text)', 'EXECUTE') AS legacy_freeze_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.reject_global_preparation_with_legacy()', "
                        "'EXECUTE') AS cross_mode_guard_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.register_inert_trial_submission"
                        "(uuid,jsonb,bytea,text,bytea,text)', 'EXECUTE') "
                        "AS submission_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.submit_inert_trial_projection"
                        "(uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text)', 'EXECUTE') "
                        "AS atomic_submission_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.read_next_executable_protected_release"
                        "(uuid)', 'EXECUTE') AS release_outbox_read_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard."
                        "acknowledge_executable_protected_release_publication"
                        "(uuid,bigint,jsonb,bytea,text,text)', 'EXECUTE') "
                        "AS release_outbox_ack_execute"
                    ),
                    {"agent": agent_role},
                )
                .mappings()
                .one()
            )
        assert dict(privileges) == {
            "schema_usage": True,
            "table_select": False,
            "table_update": False,
            "sequence_usage": False,
            "capture_execute": True,
            "lifecycle_capture_execute": True,
            "queued_lifecycle_capture_execute": False,
            "legacy_capture_execute": False,
            "lifecycle_head_guard_execute": False,
            "lifecycle_head_project_execute": False,
            "lifecycle_blocker_guard_execute": False,
            "lifecycle_resolution_project_execute": False,
            "trigger_execute": False,
            "plan_execute": True,
            "bootstrap_execute": True,
            "worker_execute": True,
            "protected_release_execute": True,
            "binding_guard_execute": False,
            "lifecycle_execute": True,
            "lifecycle_initializer_execute": False,
            "claim_inspect_execute": True,
            "legacy_prepare_execute": True,
            "legacy_freeze_execute": True,
            "cross_mode_guard_execute": False,
            "submission_execute": True,
            "atomic_submission_execute": True,
            "release_outbox_read_execute": True,
            "release_outbox_ack_execute": True,
        }
    finally:
        admin.dispose()

    agent = create_engine(_value(capacity_guard_database, "agent_url"))
    try:
        with agent.connect() as connection:
            with pytest.raises(DBAPIError) as denied:
                connection.execute(text("SELECT * FROM loom_capacity_guard.agent_registrations"))
            assert isinstance(denied.value.orig, InsufficientPrivilege)

        with agent.connect() as connection:
            with pytest.raises(DBAPIError, match="not registered") as unregistered:
                connection.execute(
                    text(
                        "SELECT loom_capacity_guard.capture_demand_observation("
                        ":agent_incarnation, 0, 100)"
                    ),
                    {"agent_incarnation": uuid4()},
                )
            assert isinstance(unregistered.value.orig, ObjectNotInPrerequisiteState)
    finally:
        agent.dispose()


def test_agent_functions_have_fixed_search_paths_and_exact_definer_status(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            rows = (
                connection.execute(
                    text(
                        "SELECT p.proname, p.prosecdef, p.proconfig, "
                        "pg_get_userbyid(p.proowner) AS owner "
                        "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                        "WHERE n.nspname = 'loom_capacity_guard' "
                        "AND p.proname IN ('capture_demand_observation', "
                        "'capture_lifecycle_demand_observation', "
                        "'capture_lifecycle_demand_observation_v2_queued', "
                        "'capture_demand_observation_v1_legacy', "
                        "'enforce_attempt_lifecycle_head_transition', "
                        "'project_attempt_lifecycle_head', "
                        "'enforce_attempt_lifecycle_projection_blocker', "
                        "'project_attempt_lifecycle_projection_resolution', "
                        "'assert_inert_agent_binding', 'prepare_inert_admission_plan', "
                        "'register_inert_bootstrap', 'record_inert_worker', "
                        "'reject_released_shape_registration', "
                        "'acknowledge_inert_protected_release', "
                        "'apply_inert_attempt_transition', "
                        "'initialize_attempt_lifecycle', "
                        "'inspect_inert_claim_proposal', "
                        "'prepare_inert_legacy_compatibility', "
                        "'freeze_inert_legacy_compatibility', "
                        "'reject_global_preparation_with_legacy', "
                        "'register_inert_trial_submission', "
                        "'submit_inert_trial_projection', "
                        "'read_next_executable_protected_release', "
                        "'acknowledge_executable_protected_release_publication')"
                    )
                )
                .mappings()
                .all()
            )
        functions = {row["proname"]: dict(row) for row in rows}
        assert set(functions) == {
            "capture_demand_observation",
            "capture_lifecycle_demand_observation",
            "capture_lifecycle_demand_observation_v2_queued",
            "capture_demand_observation_v1_legacy",
            "enforce_attempt_lifecycle_head_transition",
            "project_attempt_lifecycle_head",
            "enforce_attempt_lifecycle_projection_blocker",
            "project_attempt_lifecycle_projection_resolution",
            "assert_inert_agent_binding",
            "prepare_inert_admission_plan",
            "register_inert_bootstrap",
            "record_inert_worker",
            "reject_released_shape_registration",
            "acknowledge_inert_protected_release",
            "apply_inert_attempt_transition",
            "initialize_attempt_lifecycle",
            "inspect_inert_claim_proposal",
            "prepare_inert_legacy_compatibility",
            "freeze_inert_legacy_compatibility",
            "reject_global_preparation_with_legacy",
            "register_inert_trial_submission",
            "submit_inert_trial_projection",
            "read_next_executable_protected_release",
            "acknowledge_executable_protected_release_publication",
        }
        for name, row in functions.items():
            assert row["proconfig"] == ["search_path=pg_catalog"]
            assert row["owner"] == _value(capacity_guard_database, "owner_role")
            assert row["prosecdef"] is (
                name
                not in {
                    "assert_inert_agent_binding",
                    "enforce_attempt_lifecycle_head_transition",
                    "enforce_attempt_lifecycle_projection_blocker",
                    "initialize_attempt_lifecycle",
                    "project_attempt_lifecycle_head",
                    "project_attempt_lifecycle_projection_resolution",
                    "reject_global_preparation_with_legacy",
                    "reject_released_shape_registration",
                }
            )
    finally:
        engine.dispose()


def test_guard_migration_rejects_missing_direct_demand_source_privilege(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    owner = _value(capacity_guard_database, "owner_role")
    quoted_owner = engine.dialect.identifier_preparer.quote(owner)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f"REVOKE SELECT (submit_priority) ON TABLE public.trials FROM {quoted_owner}"
            )
        with pytest.raises(RuntimeError, match="submit_priority"):
            command.current(_guard_config(capacity_guard_database))
    finally:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f"GRANT SELECT (submit_priority) ON TABLE public.trials TO {quoted_owner}"
            )
        engine.dispose()


def test_guard_migration_rejects_missing_atomic_submission_privileges(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    owner = _value(capacity_guard_database, "owner_role")
    quoted_owner = engine.dialect.identifier_preparer.quote(owner)
    cases = (
        (
            f"REVOKE INSERT (config) ON TABLE public.trials FROM {quoted_owner}",
            "config",
            f"GRANT INSERT (config) ON TABLE public.trials TO {quoted_owner}",
        ),
        (
            f"REVOKE UPDATE (lifecycle_authority_id) ON TABLE public.trials FROM {quoted_owner}",
            "lifecycle_authority_id",
            f"GRANT UPDATE (lifecycle_authority_id) ON TABLE public.trials TO {quoted_owner}",
        ),
        (
            f"REVOKE UPDATE (state) ON TABLE public.trials FROM {quoted_owner}",
            "state",
            f"GRANT UPDATE (state) ON TABLE public.trials TO {quoted_owner}",
        ),
        (
            "REVOKE INSERT (environment) ON TABLE public.data_lifecycle_authorities "
            f"FROM {quoted_owner}",
            "environment",
            "GRANT INSERT (environment) ON TABLE public.data_lifecycle_authorities "
            f"TO {quoted_owner}",
        ),
        (
            f"REVOKE REFERENCES (id) ON TABLE public.trials FROM {quoted_owner}",
            "public.trials.id",
            f"GRANT REFERENCES (id) ON TABLE public.trials TO {quoted_owner}",
        ),
        (
            "REVOKE REFERENCES (id) ON TABLE public.data_lifecycle_authorities "
            f"FROM {quoted_owner}",
            "public.data_lifecycle_authorities.id",
            f"GRANT REFERENCES (id) ON TABLE public.data_lifecycle_authorities TO {quoted_owner}",
        ),
    )
    try:
        for revoke, expected, restore in cases:
            with engine.begin() as connection:
                connection.exec_driver_sql(revoke)
            try:
                with pytest.raises(RuntimeError, match=expected):
                    command.current(_guard_config(capacity_guard_database))
            finally:
                with engine.begin() as connection:
                    connection.exec_driver_sql(restore)
    finally:
        engine.dispose()


def test_guard_migration_rejects_agent_role_memberships(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    extra_role = f"guard_agent_extra_test_{uuid4().hex[:12]}"
    quoted_extra = engine.dialect.identifier_preparer.quote(extra_role)
    quoted_agent = engine.dialect.identifier_preparer.quote(
        _value(capacity_guard_database, "agent_role")
    )
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f"CREATE ROLE {quoted_extra} NOLOGIN NOSUPERUSER NOCREATEDB "
                "NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
            )
            connection.exec_driver_sql(f"GRANT {quoted_extra} TO {quoted_agent}")
        with pytest.raises(RuntimeError, match="least-privileged"):
            command.current(_guard_config(capacity_guard_database))
    finally:
        with engine.begin() as connection:
            connection.exec_driver_sql(f"REVOKE {quoted_extra} FROM {quoted_agent}")
            connection.exec_driver_sql(f"DROP ROLE IF EXISTS {quoted_extra}")
        engine.dispose()
