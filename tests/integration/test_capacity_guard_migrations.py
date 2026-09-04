"""Independent protected-admission migration and ownership constraints."""

from __future__ import annotations

import hashlib
import json
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
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Task, Team, TeamQuota, Trial
from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_agent.store import CapacityAgentStore
from loom_capacity_guard.contracts import GuardFenceV1, canonical_digest
from loom_capacity_guard.schema_startup import assert_capacity_guard_schema_at_head
from loom_capacity_manager.contracts import ResourceVectorV1
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableIntentBindingV2,
    ExecutionFenceV2,
)

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
    "abandoned_admission_plans",
    "never_converged_admission_plans",
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
    "executable_observer_authority",
    "executable_admission_events",
    "executable_claim_state",
    "executable_claim_leases",
    "executable_claim_terminal_events",
    "executable_release_publication_state",
    "executable_release_publication_events",
    "staging_worker_runtime_authority",
    "protected_runtime_trial_submissions",
    "protected_runtime_trial_readiness",
}


def _value(database: dict[str, object], key: str) -> str:
    value = database[key]
    assert isinstance(value, str)
    return value


def _digest_payload(payload: dict[str, object]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@pytest.mark.asyncio
async def test_guard_schema_startup_returns_numeric_head(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_async_engine(_value(capacity_guard_database, "migrator_url"))
    try:
        assert await assert_capacity_guard_schema_at_head(engine) == 26
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


def _executable_binding(subject_id: UUID, subject_incarnation: UUID) -> ExecutableIntentBindingV2:
    return ExecutableIntentBindingV2(
        execution=ExecutionFenceV2(
            authority_incarnation=UUID(int=101),
            writer_epoch=3,
            configuration_epoch=5,
            execution_epoch=7,
            execution_manifest_sha256="1" * 64,
            execution_state="active",
            executable_new_capacity_ceiling=1,
            executable_new_capacity_rate_per_minute=1,
            trusted_fleet_release_sha256="2" * 64,
            allocation_epoch=11,
        ),
        tranche_id=UUID(int=102),
        intent_id=UUID(int=103),
        shape_instance_id="oldlab-shape-0001",
        subject_id=subject_id,
        subject_incarnation=subject_incarnation,
        account_id="owner-alice",
        tier_id="development",
        candidate=CandidateBindingV2(
            algorithm="git-sha1",
            identity="a" * 40,
            publication_sha256="a" * 64,
        ),
        candidate_generation=7,
        deployment_generation=7,
        pool_id="oldlab",
        pool_generation=13,
        executor_id="oldlab-executor",
        executor_incarnation=UUID(int=104),
        shape_id="oldlab-cpu-small",
        profile_id="oldlab-default",
        profile_generation=17,
        profile_digest="3" * 64,
        concurrency_slots=1,
        resources=ResourceVectorV1(slots=1, cpu_millicores=1000, memory_bytes=1024),
        node_ids=("oldlab-node-01",),
    )


def _seed_executable_observation_rows(
    connection: Any,
    *,
    include_prepared: bool = True,
) -> tuple[UUID, UUID, UUID, UUID, UUID]:
    subject_id = uuid4()
    subject_incarnation = uuid4()
    agent_incarnation = uuid4()
    worker_id = uuid4()
    worker_incarnation = uuid4()
    binding = _executable_binding(subject_id, subject_incarnation)
    binding_json = json.dumps(binding.model_dump(mode="json"), sort_keys=True)
    connection.execute(
        text(
            "INSERT INTO loom_capacity_guard.authority_state "
            "(singleton_id, schema_version, environment_id, subject_id, subject_incarnation, "
            "authority_mode, authority_incarnation, reporter_incarnation, "
            "reporter_high_water, allocation_epoch, deployment_generation, "
            "configuration_generation, candidate_digest) "
            "VALUES (1, 1, 'dev-observer', :subject_id, :subject_incarnation, "
            "'disabled', :authority_incarnation, :reporter_incarnation, 0, 0, 7, 5, :digest)"
        ),
        {
            "subject_id": subject_id,
            "subject_incarnation": subject_incarnation,
            "authority_incarnation": binding.execution.authority_incarnation,
            "reporter_incarnation": uuid4(),
            "digest": "b" * 64,
        },
    )
    connection.execute(
        text(
            "INSERT INTO loom_capacity_guard.agent_registrations "
            "(agent_incarnation, singleton_id, schema_version, environment_id, subject_id, "
            "subject_incarnation, authority_incarnation, reporter_incarnation, authority_mode, "
            "allocation_epoch, candidate_digest, candidate_identity_algorithm, "
            "candidate_identity, candidate_publication_sha256, deployment_generation, "
            "configuration_generation, registration_state) "
            "VALUES (:agent_incarnation, 1, 1, 'dev-observer', :subject_id, "
            ":subject_incarnation, :authority_incarnation, :reporter_incarnation, "
            "'disabled', 0, :digest, 'source-sha256', :digest, :digest, 7, 5, 'registered')"
        ),
        {
            "agent_incarnation": agent_incarnation,
            "subject_id": subject_id,
            "subject_incarnation": subject_incarnation,
            "authority_incarnation": binding.execution.authority_incarnation,
            "reporter_incarnation": uuid4(),
            "digest": "c" * 64,
        },
    )
    connection.execute(
        text(
            "INSERT INTO loom_capacity_guard.executable_claim_state "
            "(intent_id, subject_id, subject_incarnation, binding, claim_high_water, "
            "terminal_high_water, draining) "
            "VALUES (:intent_id, :subject_id, :subject_incarnation, CAST(:binding AS jsonb), "
            "0, 0, false)"
        ),
        {
            "intent_id": binding.intent_id,
            "subject_id": subject_id,
            "subject_incarnation": subject_incarnation,
            "binding": binding_json,
        },
    )
    connection.execute(
        text(
            "INSERT INTO loom_capacity_guard.executable_admission_events "
            "(operation_id, event_kind, agent_incarnation, subject_id, subject_incarnation, "
            "intent_id, bootstrap_registration_epoch, protected_registration_epoch, "
            "physical_job_id, worker_id, worker_incarnation, worker_credential_sha256, "
            "bootstrap_revoked, predecessor_credential_revoked, worker_credential_revoked, "
            "binding, request_payload, request_digest, receipt) "
            "VALUES (:operation_id, 'worker-registered', :agent_incarnation, :subject_id, "
            ":subject_incarnation, :intent_id, 19, 23, 'oldlab-12345', :worker_id, "
            ":worker_incarnation, :worker_credential_sha256, true, false, false, "
            "CAST(:binding AS jsonb), CAST(:request_payload AS jsonb), :request_digest, "
            "CAST(:receipt AS jsonb))"
        ),
        {
            "operation_id": uuid4(),
            "agent_incarnation": agent_incarnation,
            "subject_id": subject_id,
            "subject_incarnation": subject_incarnation,
            "intent_id": binding.intent_id,
            "worker_id": worker_id,
            "worker_incarnation": worker_incarnation,
            "worker_credential_sha256": "d" * 64,
            "binding": binding_json,
            "request_payload": '{"schema_version":2}',
            "request_digest": "e" * 64,
            "receipt": '{"schema_version":2}',
        },
    )
    if include_prepared:
        connection.execute(
            text(
                "INSERT INTO loom_capacity_guard.executable_admission_events "
                "(operation_id, event_kind, agent_incarnation, subject_id, subject_incarnation, "
                "intent_id, bootstrap_registration_epoch, bootstrap_sha256, binding, "
                "request_payload, request_digest, receipt) "
                "VALUES (:operation_id, 'prepared', :agent_incarnation, :subject_id, "
                ":subject_incarnation, :intent_id, 19, :bootstrap_sha256, "
                "CAST(:binding AS jsonb), CAST(:request_payload AS jsonb), :request_digest, "
                "CAST(:receipt AS jsonb))"
            ),
            {
                "operation_id": uuid4(),
                "agent_incarnation": agent_incarnation,
                "subject_id": subject_id,
                "subject_incarnation": subject_incarnation,
                "intent_id": binding.intent_id,
                "bootstrap_sha256": "f" * 64,
                "binding": binding_json,
                "request_payload": '{"schema_version":2}',
                "request_digest": "0" * 64,
                "receipt": '{"schema_version":2}',
            },
        )
    return (
        subject_id,
        subject_incarnation,
        binding.intent_id,
        worker_id,
        worker_incarnation,
    )


def _insert_guard_0021_abandonment_evidence(connection: Any) -> UUID:
    subject_id, _subject_incarnation, _intent_id, _worker_id, _worker_incarnation = (
        _seed_executable_observation_rows(connection)
    )
    agent_incarnation = connection.execute(
        text(
            "SELECT agent_incarnation FROM loom_capacity_guard.agent_registrations "
            "WHERE subject_id = :subject_id"
        ),
        {"subject_id": subject_id},
    ).scalar_one()
    plan_id = uuid4()
    admission_incarnation = uuid4()
    manager_authority_incarnation = uuid4()
    connection.execute(
        text(
            "INSERT INTO loom_capacity_guard.prepared_admission_plans "
            "(plan_id, agent_incarnation, admission_incarnation, "
            "manager_authority_incarnation, manager_writer_epoch, "
            "manager_allocation_epoch, manager_input_digest, "
            "manager_allocation_digest, pool_id, pool_generation, profile_id, "
            "profile_generation, profile_digest, protocol_generation, "
            "protocol_digest, lease_not_after, plan_state, executable, payload, "
            "payload_digest) VALUES "
            "(:plan_id, :agent_incarnation, :admission_incarnation, "
            ":manager_authority_incarnation, 3, 11, repeat('1', 64), "
            "repeat('2', 64), 'oldlab', 13, 'oldlab-default', 17, "
            "repeat('3', 64), 1, repeat('4', 64), now() + interval '1 hour', "
            "'prepared', false, '{}'::jsonb, repeat('5', 64))"
        ),
        {
            "plan_id": plan_id,
            "agent_incarnation": agent_incarnation,
            "admission_incarnation": admission_incarnation,
            "manager_authority_incarnation": manager_authority_incarnation,
        },
    )
    closure_id = uuid4()
    connection.execute(
        text(
            "INSERT INTO loom_capacity_guard.abandoned_admission_plans "
            "(closure_id, proposal_id, proposal_digest, plan_id, "
            "admission_incarnation, agent_incarnation, "
            "manager_authority_incarnation, manager_writer_epoch, "
            "manager_allocation_epoch, manager_input_digest, "
            "manager_allocation_digest, pool_id, close_reason, "
            "abandonment_state, executable, payload, payload_digest) VALUES "
            "(:closure_id, :proposal_id, repeat('6', 64), :plan_id, "
            ":admission_incarnation, :agent_incarnation, "
            ":manager_authority_incarnation, 3, 11, repeat('1', 64), "
            "repeat('2', 64), 'oldlab', 'manager-closed', 'abandoned', false, "
            "'{}'::jsonb, repeat('7', 64))"
        ),
        {
            "closure_id": closure_id,
            "proposal_id": uuid4(),
            "plan_id": plan_id,
            "admission_incarnation": admission_incarnation,
            "agent_incarnation": agent_incarnation,
            "manager_authority_incarnation": manager_authority_incarnation,
        },
    )
    return closure_id


def _insert_guard_0021_never_converged_evidence(connection: Any) -> tuple[UUID, UUID]:
    subject_id, _subject_incarnation, _intent_id, _worker_id, _worker_incarnation = (
        _seed_executable_observation_rows(connection)
    )
    agent_incarnation = connection.execute(
        text(
            "SELECT agent_incarnation FROM loom_capacity_guard.agent_registrations "
            "WHERE subject_id = :subject_id"
        ),
        {"subject_id": subject_id},
    ).scalar_one()
    closure_id = uuid4()
    plan_id = uuid4()
    connection.execute(
        text(
            "INSERT INTO loom_capacity_guard.never_converged_admission_plans "
            "(closure_id, proposal_id, plan_id, admission_incarnation, "
            "agent_incarnation, registration_digest, closure_digest, "
            "proposal_digest, close_reason, disposition_state, executable, "
            "payload, payload_digest) VALUES "
            "(:closure_id, :proposal_id, :plan_id, :admission_incarnation, "
            ":agent_incarnation, repeat('1', 64), repeat('2', 64), "
            "repeat('3', 64), 'manager-closed', 'never-converged', false, "
            "'{}'::jsonb, repeat('4', 64))"
        ),
        {
            "closure_id": closure_id,
            "proposal_id": uuid4(),
            "plan_id": plan_id,
            "admission_incarnation": uuid4(),
            "agent_incarnation": agent_incarnation,
        },
    )
    return closure_id, plan_id


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
            assert revision == "guard_0026"
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
                        "has_column_privilege(:owner, 'public.trials', 'requires_caps', 'UPDATE') "
                        "AS can_update_requires_caps, "
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
            "can_select_config": True,
            "can_bind_lifecycle": True,
            "can_fence_state": True,
            "can_update_requires_caps": True,
            "can_reference_trial_table": False,
            "can_reference_trial_id": True,
            "can_update_worker": True,
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
    os.environ["LOOM_CAPACITY_GUARD_OBSERVER_ROLE"] = _value(database, "observer_role")
    os.environ["LOOM_CAPACITY_GUARD_RUNTIME_ROLE"] = _value(database, "runtime_role")
    return cfg


def _atomic_submission_function_definition(engine: Engine) -> str:
    with engine.connect() as connection:
        definition = connection.execute(
            text(
                "SELECT pg_get_functiondef("
                "'loom_capacity_guard.submit_inert_trial_projection"
                "(uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text)'::regprocedure)"
            )
        ).scalar_one()
    assert isinstance(definition, str)
    return definition


def test_guard_0022_staging_submission_admission_is_reversible(
    capacity_guard_database: dict[str, object],
) -> None:
    cfg = _guard_config(capacity_guard_database)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    rejection = "atomic trial submission protected retention is unavailable for staging"
    try:
        assert rejection not in _atomic_submission_function_definition(engine)

        command.downgrade(cfg, "guard_0021")
        assert rejection in _atomic_submission_function_definition(engine)
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0021"
            )

        command.upgrade(cfg, "head")
        assert rejection not in _atomic_submission_function_definition(engine)
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0026"
            )
    finally:
        engine.dispose()


def test_guard_0023_empty_downgrade_and_reupgrade_restore_runtime_surface(
    capacity_guard_database: dict[str, object],
) -> None:
    cfg = _guard_config(capacity_guard_database)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    runtime = _value(capacity_guard_database, "runtime_role")
    agent = _value(capacity_guard_database, "agent_role")
    signatures = (
        "loom_capacity_guard.current_protected_runtime_registration()",
        "loom_capacity_guard.submit_protected_runtime_trial_projection"
        "(uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text,jsonb,bytea,text)",
        "loom_capacity_guard.publish_protected_runtime_trial_readiness(uuid,uuid,uuid)",
        "loom_capacity_guard.register_staging_public_worker(text,jsonb)",
        "loom_capacity_guard.assert_staging_worker_session(uuid,text)",
    )
    runtime_tables = (
        "staging_worker_runtime_authority",
        "protected_runtime_trial_submissions",
        "protected_runtime_trial_readiness",
    )
    inspect_signature = (
        "loom_capacity_guard.inspect_protected_runtime_trial_prerequisites(uuid,uuid,uuid,boolean)"
    )
    try:
        with engine.connect() as connection:
            protected_capture = connection.execute(
                text(
                    "SELECT pg_get_functiondef("
                    "'loom_capacity_guard.capture_lifecycle_demand_observation"
                    "(uuid,bigint,integer)'::regprocedure)"
                )
            ).scalar_one()
            assert "protected_runtime_trial_readiness" in protected_capture
        command.downgrade(cfg, "guard_0022")
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0022"
            )
            for table in runtime_tables:
                assert (
                    connection.execute(
                        text("SELECT to_regclass(:table)"),
                        {"table": f"loom_capacity_guard.{table}"},
                    ).scalar_one()
                    is None
                )
            for signature in signatures:
                assert (
                    connection.execute(
                        text("SELECT to_regprocedure(:signature)"),
                        {"signature": signature},
                    ).scalar_one()
                    is None
                )
            assert (
                connection.execute(
                    text("SELECT to_regprocedure(:signature)"),
                    {"signature": inspect_signature},
                ).scalar_one()
                is None
            )
            unprotected_capture = connection.execute(
                text(
                    "SELECT pg_get_functiondef("
                    "'loom_capacity_guard.capture_lifecycle_demand_observation"
                    "(uuid,bigint,integer)'::regprocedure)"
                )
            ).scalar_one()
            assert "protected_runtime_trial_readiness" not in unprotected_capture

        command.upgrade(cfg, "head")
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0026"
            )
            for table in runtime_tables:
                assert (
                    connection.execute(
                        text("SELECT to_regclass(:table)"),
                        {"table": f"loom_capacity_guard.{table}"},
                    ).scalar_one()
                    == f"loom_capacity_guard.{table}"
                )
            for signature in signatures:
                assert (
                    connection.execute(
                        text("SELECT has_function_privilege(:runtime, :signature, 'EXECUTE')"),
                        {"runtime": runtime, "signature": signature},
                    ).scalar_one()
                    is True
                )
                assert (
                    connection.execute(
                        text("SELECT has_function_privilege(:agent, :signature, 'EXECUTE')"),
                        {"agent": agent, "signature": signature},
                    ).scalar_one()
                    is False
                )
            assert (
                connection.execute(
                    text("SELECT has_function_privilege(:runtime, :signature, 'EXECUTE')"),
                    {"runtime": runtime, "signature": inspect_signature},
                ).scalar_one()
                is False
            )
            assert (
                connection.execute(
                    text(
                        "SELECT has_table_privilege("
                        ":runtime, 'loom_capacity_guard.executable_admission_events', 'SELECT')"
                    ),
                    {"runtime": runtime},
                ).scalar_one()
                is False
            )
    finally:
        engine.dispose()


def test_guard_0024_terminal_closure_is_private_and_reversible(
    capacity_guard_database: dict[str, object],
) -> None:
    cfg = _guard_config(capacity_guard_database)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    guard_owner = _value(capacity_guard_database, "owner_role")
    signature = (
        "loom_capacity_guard.close_protected_runtime_trial_claim(uuid,text,text,uuid,integer)"
    )
    application_signature = "public.loom_close_protected_runtime_trial_claim()"
    denied_roles = (
        _value(capacity_guard_database, "agent_role"),
        _value(capacity_guard_database, "executor_role"),
        _value(capacity_guard_database, "observer_role"),
        _value(capacity_guard_database, "runtime_role"),
        "public",
    )

    def assert_installed() -> None:
        with engine.connect() as connection:
            guard_routine = (
                connection.execute(
                    text(
                        "SELECT pg_get_userbyid(routine.proowner) AS owner, "
                        "routine.prosecdef, routine.proconfig "
                        "FROM pg_proc AS routine "
                        "JOIN pg_namespace AS namespace "
                        "ON namespace.oid = routine.pronamespace "
                        "WHERE namespace.nspname = 'loom_capacity_guard' "
                        "AND routine.proname = 'close_protected_runtime_trial_claim'"
                    )
                )
                .mappings()
                .one()
            )
            application_routine = (
                connection.execute(
                    text(
                        "SELECT pg_get_userbyid(routine.proowner) AS owner, "
                        "routine.prosecdef, routine.proconfig, "
                        "pg_get_functiondef(routine.oid) AS definition "
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
            assert dict(guard_routine) == {
                "owner": guard_owner,
                "prosecdef": True,
                "proconfig": ["search_path=pg_catalog"],
            }
            assert application_routine["prosecdef"] is True
            assert application_routine["proconfig"] == ["search_path=pg_catalog"]
            assert (
                "loom_capacity_guard.close_protected_runtime_trial_claim"
                in application_routine["definition"]
            )
            assert (
                connection.execute(
                    text("SELECT has_function_privilege(:owner, :function, 'EXECUTE')"),
                    {"owner": application_routine["owner"], "function": signature},
                ).scalar_one()
                is True
            )
            assert {
                role: connection.execute(
                    text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                    {"role": role, "function": signature},
                ).scalar_one()
                for role in denied_roles
            } == {role: False for role in denied_roles}
            assert (
                connection.execute(
                    text("SELECT has_function_privilege('public', :function, 'EXECUTE')"),
                    {"function": application_signature},
                ).scalar_one()
                is False
            )

    try:
        assert_installed()

        command.downgrade(cfg, "guard_0023")
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT to_regprocedure(:function)"),
                    {"function": signature},
                ).scalar_one()
                is None
            )
            assert (
                connection.execute(
                    text("SELECT to_regprocedure(:function)"),
                    {"function": application_signature},
                ).scalar_one()
                is not None
            )
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0023"
            )

        command.upgrade(cfg, "head")
        assert_installed()
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0026"
            )
    finally:
        engine.dispose()


def test_guard_0025_retry_is_runtime_only_schema_bound_and_reversible(
    capacity_guard_database: dict[str, object],
) -> None:
    cfg = _guard_config(capacity_guard_database)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    owner = _value(capacity_guard_database, "owner_role")
    runtime = _value(capacity_guard_database, "runtime_role")
    denied_roles = (
        _value(capacity_guard_database, "agent_role"),
        _value(capacity_guard_database, "executor_role"),
        _value(capacity_guard_database, "observer_role"),
        "public",
    )
    signature = "loom_capacity_guard.retry_staging_claimed_trial(uuid,text,jsonb)"

    def assert_installed() -> None:
        with engine.connect() as connection:
            routine = (
                connection.execute(
                    text(
                        "SELECT pg_get_userbyid(p.proowner) AS owner, p.prosecdef, "
                        "p.proconfig FROM pg_proc AS p "
                        "JOIN pg_namespace AS n ON n.oid = p.pronamespace "
                        "WHERE n.nspname = 'loom_capacity_guard' "
                        "AND p.proname = 'retry_staging_claimed_trial'"
                    )
                )
                .mappings()
                .one()
            )
            assert dict(routine) == {
                "owner": owner,
                "prosecdef": True,
                "proconfig": ["search_path=pg_catalog"],
            }
            assert (
                connection.execute(
                    text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                    {"role": runtime, "function": signature},
                ).scalar_one()
                is True
            )
            assert {
                role: connection.execute(
                    text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                    {"role": role, "function": signature},
                ).scalar_one()
                for role in denied_roles
            } == {role: False for role in denied_roles}

            columns = {
                row["column_name"]: (row["data_type"], row["is_nullable"])
                for row in connection.execute(
                    text(
                        "SELECT column_name, data_type, is_nullable "
                        "FROM information_schema.columns "
                        "WHERE table_schema = 'loom_capacity_guard' "
                        "AND table_name = 'protected_runtime_trial_submissions' "
                        "AND column_name IN "
                        "('attempt_sequence', 'public_attempt_count', 'not_before')"
                    )
                ).mappings()
            }
            assert columns == {
                "attempt_sequence": ("bigint", "NO"),
                "public_attempt_count": ("integer", "NO"),
                "not_before": ("timestamp with time zone", "YES"),
            }
            primary_keys = {
                row["table_name"]: list(row["columns"])
                for row in connection.execute(
                    text(
                        "SELECT relation.relname AS table_name, "
                        "array_agg(attribute.attname ORDER BY key.ordinality) AS columns "
                        "FROM pg_constraint AS constraint_row "
                        "JOIN pg_class AS relation "
                        "ON relation.oid = constraint_row.conrelid "
                        "JOIN pg_namespace AS namespace "
                        "ON namespace.oid = relation.relnamespace "
                        "CROSS JOIN LATERAL unnest(constraint_row.conkey) "
                        "WITH ORDINALITY AS key(attnum, ordinality) "
                        "JOIN pg_attribute AS attribute "
                        "ON attribute.attrelid = relation.oid "
                        "AND attribute.attnum = key.attnum "
                        "WHERE namespace.nspname = 'loom_capacity_guard' "
                        "AND relation.relname IN "
                        "('protected_runtime_trial_submissions', "
                        "'protected_runtime_trial_readiness') "
                        "AND constraint_row.contype = 'p' "
                        "GROUP BY relation.relname"
                    )
                ).mappings()
            }
            assert primary_keys == {
                "protected_runtime_trial_submissions": [
                    "trial_id",
                    "protected_attempt_id",
                ],
                "protected_runtime_trial_readiness": [
                    "trial_id",
                    "protected_attempt_id",
                ],
            }
            exact_guard = connection.execute(
                text(
                    "SELECT pg_get_functiondef("
                    "'loom_capacity_guard.enforce_executable_claim_assignment()'"
                    "::regprocedure)"
                )
            ).scalar_one()
            assert "runtime.attempt_sequence > 0" in exact_guard
            assert "retry_trial.state = 'protected-pending'" in exact_guard
            claim_definition = connection.execute(
                text(
                    "SELECT pg_get_functiondef("
                    "'loom_capacity_guard.claim_staging_assigned_trial"
                    "(uuid,text,jsonb)'::regprocedure)"
                )
            ).scalar_one()
            assert "v_candidate.attempt_sequence + 1" in claim_definition

    try:
        assert_installed()

        command.downgrade(cfg, "guard_0024")
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT to_regprocedure(:function)"),
                    {"function": signature},
                ).scalar_one()
                is None
            )
            columns = set(
                connection.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = 'loom_capacity_guard' "
                        "AND table_name = 'protected_runtime_trial_submissions'"
                    )
                ).scalars()
            )
            assert {
                "attempt_sequence",
                "public_attempt_count",
                "not_before",
            }.isdisjoint(columns)
            exact_guard = connection.execute(
                text(
                    "SELECT pg_get_functiondef("
                    "'loom_capacity_guard.enforce_executable_claim_assignment()'"
                    "::regprocedure)"
                )
            ).scalar_one()
            assert "runtime.attempt_sequence > 0" not in exact_guard
            assert "lifecycle.manager_allocation_epoch =" in exact_guard
            assert "worker.binding" in exact_guard
            claim_definition = connection.execute(
                text(
                    "SELECT pg_get_functiondef("
                    "'loom_capacity_guard.claim_staging_assigned_trial"
                    "(uuid,text,jsonb)'::regprocedure)"
                )
            ).scalar_one()
            assert "v_candidate.attempt_sequence + 1" not in claim_definition
            assert "v_candidate.attempt_count + 1" in claim_definition
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0024"
            )

        command.upgrade(cfg, "head")
        assert_installed()
    finally:
        engine.dispose()


def test_guard_0026_requeue_is_trigger_only_and_reversible(
    capacity_guard_database: dict[str, object],
) -> None:
    cfg = _guard_config(capacity_guard_database)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    owner = _value(capacity_guard_database, "owner_role")
    signature = (
        "loom_capacity_guard.transform_protected_runtime_trial_requeue"
        "(uuid,text,uuid,integer,uuid,integer,text,text,timestamp with time zone)"
    )
    trigger_signature = "public.loom_transform_protected_runtime_trial_requeue()"

    def assert_installed() -> None:
        with engine.connect() as connection:
            trigger_owner = connection.execute(
                text(
                    "SELECT pg_get_userbyid(proowner) FROM pg_proc "
                    "WHERE oid = to_regprocedure(:signature)"
                ),
                {"signature": trigger_signature},
            ).scalar_one()
            routine = (
                connection.execute(
                    text(
                        "SELECT pg_get_userbyid(proowner) AS owner, prosecdef, proconfig "
                        "FROM pg_proc WHERE oid = to_regprocedure(:signature)"
                    ),
                    {"signature": signature},
                )
                .mappings()
                .one()
            )
            assert dict(routine) == {
                "owner": owner,
                "prosecdef": True,
                "proconfig": ["search_path=pg_catalog"],
            }
            assert (
                connection.execute(
                    text(
                        "SELECT has_function_privilege"
                        "(:role, :signature, 'EXECUTE')"
                    ),
                    {"role": trigger_owner, "signature": signature},
                ).scalar_one()
                is True
            )
            denied_roles = (
                _value(capacity_guard_database, "agent_role"),
                _value(capacity_guard_database, "executor_role"),
                _value(capacity_guard_database, "observer_role"),
                _value(capacity_guard_database, "runtime_role"),
                "public",
            )
            assert {
                role: connection.execute(
                    text(
                        "SELECT has_function_privilege"
                        "(:role, :signature, 'EXECUTE')"
                    ),
                    {"role": role, "signature": signature},
                ).scalar_one()
                for role in denied_roles
            } == {role: False for role in denied_roles}
            definition = connection.execute(
                text("SELECT pg_get_functiondef(to_regprocedure(:signature))"),
                {"signature": signature},
            ).scalar_one()
            assert "claimed-attempt-reclaimed" in definition
            assert "protected-pending" in definition
            assert "max_attempts_ceiling" in definition

    try:
        assert_installed()
        command.downgrade(cfg, "guard_0025")
        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT to_regprocedure(:signature)"),
                {"signature": signature},
            ).scalar_one() is None
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM "
                        "loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0025"
            )
            assert connection.execute(
                text("SELECT to_regprocedure(:signature)"),
                {"signature": trigger_signature},
            ).scalar_one() is not None

        command.upgrade(cfg, "head")
        assert_installed()
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM "
                        "loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0026"
            )
    finally:
        engine.dispose()


def test_guard_0025_refuses_downgrade_with_retry_attempt_evidence(
    capacity_guard_database: dict[str, object],
) -> None:
    cfg = _guard_config(capacity_guard_database)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    trial_id = _seed_trial(engine)
    try:
        with _owner_connection(capacity_guard_database) as connection:
            _, _ = _insert_foundation_rows(connection, trial_id)
            connection.execute(
                text(
                    "INSERT INTO loom_capacity_guard.trial_attempts "
                    "(protected_attempt_id, trial_id, execution_generation, "
                    "attempt_sequence, requirements_digest, claim_state) "
                    "VALUES (:attempt_id, :trial_id, 2, 1, :digest, 'queued')"
                ),
                {
                    "attempt_id": uuid4(),
                    "trial_id": trial_id,
                    "digest": "a" * 64,
                },
            )

        with pytest.raises(
            RuntimeError,
            match="cannot downgrade guard_0025 while retry attempts exist",
        ):
            command.downgrade(cfg, "guard_0024")

        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0026"
            )
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM loom_capacity_guard.trial_attempts "
                        "WHERE trial_id = :trial_id AND attempt_sequence = 1"
                    ),
                    {"trial_id": trial_id},
                ).scalar_one()
                == 1
            )
    finally:
        engine.dispose()


def test_guard_0021_current_assignment_routine_is_agent_only_and_reversible(
    capacity_guard_database: dict[str, object],
) -> None:
    cfg = _guard_config(capacity_guard_database)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    owner = _value(capacity_guard_database, "owner_role")
    agent = _value(capacity_guard_database, "agent_role")
    executor = _value(capacity_guard_database, "executor_role")
    observer = _value(capacity_guard_database, "observer_role")
    signature = "loom_capacity_guard.assert_current_inert_assignment(uuid,jsonb,bytea,text)"
    try:
        with engine.connect() as connection:
            routine = (
                connection.execute(
                    text(
                        "SELECT pg_get_userbyid(p.proowner) AS owner, p.prosecdef, "
                        "p.proconfig FROM pg_proc AS p "
                        "JOIN pg_namespace AS n ON n.oid = p.pronamespace "
                        "WHERE n.nspname = 'loom_capacity_guard' "
                        "AND p.proname = 'assert_current_inert_assignment'"
                    )
                )
                .mappings()
                .one()
            )
            assert dict(routine) == {
                "owner": owner,
                "prosecdef": True,
                "proconfig": ["search_path=pg_catalog"],
            }
            privileges = {
                role: connection.execute(
                    text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                    {"role": role, "function": signature},
                ).scalar_one()
                for role in (agent, executor, observer, "public")
            }
            assert privileges == {
                agent: True,
                executor: False,
                observer: False,
                "public": False,
            }

        command.downgrade(cfg, "guard_0020")
        with engine.connect() as connection:
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
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0020"
            )

        command.upgrade(cfg, "head")
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT to_regprocedure(:function)"),
                    {"function": signature},
                ).scalar_one()
                == signature
            )
            assert (
                connection.execute(
                    text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                    {"role": agent, "function": signature},
                ).scalar_one()
                is True
            )
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0026"
            )
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "statement",
    (
        "UPDATE loom_capacity_guard.abandoned_admission_plans SET payload_digest = payload_digest",
        "DELETE FROM loom_capacity_guard.abandoned_admission_plans",
        "TRUNCATE loom_capacity_guard.abandoned_admission_plans CASCADE",
    ),
)
def test_guard_0021_abandonment_evidence_is_append_only(
    capacity_guard_database: dict[str, object],
    statement: str,
) -> None:
    with _owner_connection(capacity_guard_database) as connection:
        _insert_guard_0021_abandonment_evidence(connection)

    with pytest.raises(DBAPIError, match="append-only"):
        with _owner_connection(capacity_guard_database) as connection:
            connection.execute(text(statement))


def test_guard_0021_refuses_downgrade_with_abandonment_evidence(
    capacity_guard_database: dict[str, object],
) -> None:
    cfg = _guard_config(capacity_guard_database)
    with _owner_connection(capacity_guard_database) as connection:
        closure_id = _insert_guard_0021_abandonment_evidence(connection)

    with pytest.raises(
        RuntimeError,
        match="cannot downgrade guard_0021 with abandonment evidence",
    ):
        command.downgrade(cfg, "guard_0020")

    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0026"
            )
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM "
                        "loom_capacity_guard.abandoned_admission_plans "
                        "WHERE closure_id = :closure_id"
                    ),
                    {"closure_id": closure_id},
                ).scalar_one()
                == 1
            )
    finally:
        engine.dispose()


def test_guard_0021_refuses_downgrade_with_never_converged_evidence(
    capacity_guard_database: dict[str, object],
) -> None:
    cfg = _guard_config(capacity_guard_database)
    with _owner_connection(capacity_guard_database) as connection:
        closure_id, _plan_id = _insert_guard_0021_never_converged_evidence(connection)

    with pytest.raises(
        RuntimeError,
        match="cannot downgrade guard_0021 with closure disposition evidence",
    ):
        command.downgrade(cfg, "guard_0020")

    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0026"
            )
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM "
                        "loom_capacity_guard.never_converged_admission_plans "
                        "WHERE closure_id = :closure_id"
                    ),
                    {"closure_id": closure_id},
                ).scalar_one()
                == 1
            )
    finally:
        engine.dispose()


def test_guard_0021_empty_downgrade_and_reupgrade_restore_closure_guards(
    capacity_guard_database: dict[str, object],
) -> None:
    cfg = _guard_config(capacity_guard_database)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        command.downgrade(cfg, "guard_0020")
        with engine.connect() as connection:
            tables = set(inspect(connection).get_table_names(schema="loom_capacity_guard"))
            assert {
                "abandoned_admission_plans",
                "never_converged_admission_plans",
            }.isdisjoint(tables)

        command.upgrade(cfg, "guard_0021")
        with engine.connect() as connection:
            triggers = set(
                connection.execute(
                    text(
                        "SELECT trigger.tgname FROM pg_trigger AS trigger "
                        "JOIN pg_class AS relation ON relation.oid = trigger.tgrelid "
                        "JOIN pg_namespace AS namespace "
                        "ON namespace.oid = relation.relnamespace "
                        "WHERE namespace.nspname = 'loom_capacity_guard' "
                        "AND relation.relname = 'abandoned_admission_plans' "
                        "AND NOT trigger.tgisinternal"
                    )
                ).scalars()
            )
            assert triggers == {
                "abandoned_admission_plans_append_only_row",
                "abandoned_admission_plans_append_only_truncate",
            }
            never_converged_triggers = set(
                connection.execute(
                    text(
                        "SELECT trigger.tgname FROM pg_trigger AS trigger "
                        "JOIN pg_class AS relation ON relation.oid = trigger.tgrelid "
                        "JOIN pg_namespace AS namespace "
                        "ON namespace.oid = relation.relnamespace "
                        "WHERE namespace.nspname = 'loom_capacity_guard' "
                        "AND relation.relname = 'never_converged_admission_plans' "
                        "AND NOT trigger.tgisinternal"
                    )
                ).scalars()
            )
            assert never_converged_triggers == {
                "never_converged_admission_plans_append_only_row",
                "never_converged_admission_plans_append_only_truncate",
            }
            prepared_triggers = set(
                connection.execute(
                    text(
                        "SELECT trigger.tgname FROM pg_trigger AS trigger "
                        "JOIN pg_class AS relation ON relation.oid = trigger.tgrelid "
                        "JOIN pg_namespace AS namespace "
                        "ON namespace.oid = relation.relnamespace "
                        "WHERE namespace.nspname = 'loom_capacity_guard' "
                        "AND relation.relname = 'prepared_admission_plans' "
                        "AND NOT trigger.tgisinternal"
                    )
                ).scalars()
            )
            assert "prepared_admission_plans_never_converged_exclusion" in prepared_triggers
    finally:
        engine.dispose()


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


def test_guard_0015_downgrade_restores_executor_only_observation(
    capacity_guard_database: dict[str, object],
) -> None:
    cfg = _guard_config(capacity_guard_database)
    observer_role = _value(capacity_guard_database, "observer_role")
    observation_signature = "loom_capacity_guard.observe_executable_intent(uuid,uuid,uuid)"
    with _owner_connection(capacity_guard_database) as connection:
        connection.exec_driver_sql(
            f'GRANT USAGE ON SCHEMA loom_capacity_guard TO "{observer_role}"'
        )
    executor = create_engine(
        _value(capacity_guard_database, "executor_url"), isolation_level="SERIALIZABLE"
    )
    observer = create_engine(
        _value(capacity_guard_database, "observer_url"), isolation_level="SERIALIZABLE"
    )
    try:
        command.downgrade(cfg, "guard_0014")
        with create_engine(_value(capacity_guard_database, "admin_url")).connect() as connection:
            assert (
                connection.execute(
                    text("SELECT has_schema_privilege(:role, 'loom_capacity_guard', 'USAGE')"),
                    {"role": observer_role},
                ).scalar_one()
                is True
            )
            assert (
                connection.execute(
                    text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                    {"role": observer_role, "function": observation_signature},
                ).scalar_one()
                is False
            )
        statement = text(
            "SELECT loom_capacity_guard.observe_executable_intent("
            "'00000000-0000-0000-0000-000000000001'::uuid, "
            "'00000000-0000-0000-0000-000000000002'::uuid, "
            "'00000000-0000-0000-0000-000000000003'::uuid)"
        )
        # The executor reaches the restored function and fails only because
        # the exact protected binding is absent; observer has no EXECUTE grant.
        with executor.connect() as connection, pytest.raises(DBAPIError) as caught:
            connection.execute(statement)
        assert getattr(caught.value.orig, "sqlstate", None) == "55000"
        with observer.connect() as connection, pytest.raises(DBAPIError) as caught:
            connection.execute(statement)
        assert getattr(caught.value.orig, "sqlstate", None) == "42501"
        command.upgrade(cfg, "head")
        with executor.connect() as connection, pytest.raises(DBAPIError) as caught:
            connection.execute(statement)
        assert getattr(caught.value.orig, "sqlstate", None) == "55000"
        with create_engine(_value(capacity_guard_database, "admin_url")).connect() as connection:
            assert (
                connection.execute(
                    text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                    {"role": observer_role, "function": observation_signature},
                ).scalar_one()
                is True
            )
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0026"
            )
        with observer.connect() as connection, pytest.raises(DBAPIError) as caught:
            connection.execute(statement)
        assert getattr(caught.value.orig, "sqlstate", None) == "55000"
    finally:
        executor.dispose()
        observer.dispose()


def test_guard_0018_routine_security_and_grant_inventory(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    owner = _value(capacity_guard_database, "owner_role")
    executor_role = _value(capacity_guard_database, "executor_role")
    observer_role = _value(capacity_guard_database, "observer_role")
    observe_signature = "loom_capacity_guard.observe_executable_intent(uuid,uuid,uuid)"
    revoke_signature = (
        "loom_capacity_guard.revoke_prepared_executable_bootstrap(uuid,uuid,jsonb,bytea,text)"
    )
    try:
        with engine.connect() as connection:
            rows = (
                connection.execute(
                    text(
                        "SELECT p.proname, pg_get_userbyid(p.proowner) AS owner, "
                        "p.prosecdef, p.proconfig, pg_get_functiondef(p.oid) AS definition "
                        "FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid = p.pronamespace "
                        "WHERE n.nspname = 'loom_capacity_guard' "
                        "AND p.proname IN ("
                        "'observe_executable_intent', "
                        "'revoke_prepared_executable_bootstrap') "
                        "ORDER BY p.proname"
                    )
                )
                .mappings()
                .all()
            )
            assert [row["proname"] for row in rows] == [
                "observe_executable_intent",
                "revoke_prepared_executable_bootstrap",
            ]
            by_name = {row["proname"]: row for row in rows}
            assert by_name["observe_executable_intent"]["owner"] == owner
            assert by_name["observe_executable_intent"]["prosecdef"] is True
            assert by_name["observe_executable_intent"]["proconfig"] == ["search_path=pg_catalog"]
            assert (
                "'prepared_revocation', v_prepared_revocation.receipt"
                in by_name["observe_executable_intent"]["definition"]
            )
            assert by_name["revoke_prepared_executable_bootstrap"]["owner"] == owner
            assert by_name["revoke_prepared_executable_bootstrap"]["prosecdef"] is True
            assert by_name["revoke_prepared_executable_bootstrap"]["proconfig"] == [
                "search_path=pg_catalog"
            ]
            assert (
                connection.execute(
                    text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                    {"role": executor_role, "function": observe_signature},
                ).scalar_one()
                is True
            )
            assert (
                connection.execute(
                    text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                    {"role": observer_role, "function": observe_signature},
                ).scalar_one()
                is True
            )
            assert (
                connection.execute(
                    text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                    {"role": executor_role, "function": revoke_signature},
                ).scalar_one()
                is True
            )
            assert (
                connection.execute(
                    text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                    {"role": observer_role, "function": revoke_signature},
                ).scalar_one()
                is False
            )
    finally:
        engine.dispose()


# Production break caught: guard_0018 downgrade must restore guard_0017's
# observer-capable status routine, remove only the prepared-revocation primitive,
# and then re-upgrade without losing fixed search_path/security/grants.
def test_guard_0018_downgrades_to_0016_and_reupgrades_observation_faithfully(
    capacity_guard_database: dict[str, object],
) -> None:
    cfg = _guard_config(capacity_guard_database)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    executor_role = _value(capacity_guard_database, "executor_role")
    observer_role = _value(capacity_guard_database, "observer_role")
    observe_signature = "loom_capacity_guard.observe_executable_intent(uuid,uuid,uuid)"
    revoke_signature = (
        "loom_capacity_guard.revoke_prepared_executable_bootstrap(uuid,uuid,jsonb,bytea,text)"
    )
    try:
        command.downgrade(cfg, "guard_0017")
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0017"
            )
            row = (
                connection.execute(
                    text(
                        "SELECT p.prosecdef, p.proconfig, pg_get_functiondef(p.oid) AS definition "
                        "FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid = p.pronamespace "
                        "WHERE n.nspname = 'loom_capacity_guard' "
                        "AND p.proname = 'observe_executable_intent'"
                    )
                )
                .mappings()
                .one()
            )
            assert row["prosecdef"] is True
            assert row["proconfig"] == ["search_path=pg_catalog"]
            assert "prepared_revocation" not in row["definition"]
            assert (
                connection.execute(
                    text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                    {"role": executor_role, "function": observe_signature},
                ).scalar_one()
                is True
            )
            assert (
                connection.execute(
                    text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                    {"role": observer_role, "function": observe_signature},
                ).scalar_one()
                is True
            )
            assert (
                connection.execute(
                    text("SELECT to_regprocedure(:function)"),
                    {"function": revoke_signature},
                ).scalar_one()
                is None
            )

        command.upgrade(cfg, "head")
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0026"
            )
            row = (
                connection.execute(
                    text(
                        "SELECT p.prosecdef, p.proconfig, pg_get_functiondef(p.oid) AS definition "
                        "FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid = p.pronamespace "
                        "WHERE n.nspname = 'loom_capacity_guard' "
                        "AND p.proname = 'observe_executable_intent'"
                    )
                )
                .mappings()
                .one()
            )
            assert row["prosecdef"] is True
            assert row["proconfig"] == ["search_path=pg_catalog"]
            assert "'prepared_revocation', v_prepared_revocation.receipt" in row["definition"]
            assert (
                connection.execute(
                    text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                    {"role": observer_role, "function": observe_signature},
                ).scalar_one()
                is True
            )
            assert (
                connection.execute(
                    text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                    {"role": executor_role, "function": revoke_signature},
                ).scalar_one()
                is True
            )
    finally:
        engine.dispose()


def test_guard_0018_refuses_downgrade_with_prepared_revocation_evidence(
    capacity_guard_database: dict[str, object],
) -> None:
    cfg = _guard_config(capacity_guard_database)
    command.downgrade(cfg, "guard_0018")
    with _owner_connection(capacity_guard_database) as connection:
        subject_id, subject_incarnation, intent_id, _worker_id, _worker_incarnation = (
            _seed_executable_observation_rows(connection)
        )
        row = (
            connection.execute(
                text(
                    "SELECT agent_incarnation, binding "
                    "FROM loom_capacity_guard.executable_admission_events "
                    "WHERE intent_id = :intent_id AND event_kind = 'prepared'"
                ),
                {"intent_id": intent_id},
            )
            .mappings()
            .one()
        )
        receipt = json.dumps(
            {
                "schema_version": 2,
                "binding": row["binding"],
                "reporter_incarnation": str(uuid4()),
                "bootstrap_registration_epoch": 19,
                "protected_registration_epoch": 20,
                "claim_high_water": 0,
                "live_claim_count": 0,
                "bootstrap_revoked": True,
                "request_digest": "1" * 64,
                "protected_release_sha256": "1" * 64,
                "protected_high_water": 3,
                "revocation_state": "revoked",
                "executable": True,
            },
            sort_keys=True,
        )
        connection.execute(
            text(
                "INSERT INTO loom_capacity_guard.executable_admission_events "
                "(operation_id, event_kind, agent_incarnation, subject_id, "
                "subject_incarnation, intent_id, bootstrap_registration_epoch, "
                "protected_registration_epoch, bootstrap_revoked, claim_high_water, "
                "binding, request_payload, request_digest, receipt) "
                "VALUES (:operation_id, 'prepared-revoked', :agent_incarnation, "
                ":subject_id, :subject_incarnation, :intent_id, 19, 20, true, 0, "
                ":binding, CAST(:request_payload AS jsonb), :request_digest, "
                "CAST(:receipt AS jsonb))"
            ),
            {
                "operation_id": uuid4(),
                "agent_incarnation": row["agent_incarnation"],
                "subject_id": subject_id,
                "subject_incarnation": subject_incarnation,
                "intent_id": intent_id,
                "binding": json.dumps(row["binding"], sort_keys=True),
                "request_payload": '{"schema_version":2}',
                "request_digest": "1" * 64,
                "receipt": receipt,
            },
        )

    with pytest.raises(DBAPIError, match="cannot downgrade guard_0018"):
        command.downgrade(cfg, "guard_0017")


# Production break caught: guard_0019 downgrade must never erase manager
# publication evidence, and an evidence-free downgrade must restore the exact
# guard_0018 routine/grant surface for later re-upgrade.
def test_guard_0019_downgrades_to_0017_and_reupgrades_outbox_faithfully(
    capacity_guard_database: dict[str, object],
) -> None:
    cfg = _guard_config(capacity_guard_database)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    agent_role = _value(capacity_guard_database, "agent_role")
    executor_role = _value(capacity_guard_database, "executor_role")
    observer_role = _value(capacity_guard_database, "observer_role")
    read_signature = "loom_capacity_guard.read_next_executable_protected_release(uuid)"
    ack_signature = (
        "loom_capacity_guard.acknowledge_executable_protected_release_publication"
        "(uuid,bigint,jsonb,bytea,text,text)"
    )
    revoke_signature = (
        "loom_capacity_guard.revoke_prepared_executable_bootstrap(uuid,uuid,jsonb,bytea,text)"
    )
    try:
        command.downgrade(cfg, "guard_0018")
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0018"
            )
            assert (
                connection.execute(
                    text("SELECT to_regprocedure(:function)"),
                    {"function": read_signature},
                ).scalar_one()
                is None
            )
            assert (
                connection.execute(
                    text("SELECT to_regprocedure(:function)"),
                    {"function": ack_signature},
                ).scalar_one()
                is None
            )
            tables = set(inspect(connection).get_table_names(schema="loom_capacity_guard"))
            assert "executable_release_publication_state" not in tables
            assert "executable_release_publication_events" not in tables
            assert (
                connection.execute(
                    text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                    {"role": executor_role, "function": revoke_signature},
                ).scalar_one()
                is True
            )
            assert (
                connection.execute(
                    text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                    {"role": observer_role, "function": revoke_signature},
                ).scalar_one()
                is False
            )

        command.upgrade(cfg, "head")
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0026"
            )
            for signature in (read_signature, ack_signature):
                assert (
                    connection.execute(
                        text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                        {"role": agent_role, "function": signature},
                    ).scalar_one()
                    is True
                )
            for signature in (read_signature, ack_signature):
                for role in (executor_role, observer_role):
                    assert (
                        connection.execute(
                            text("SELECT has_function_privilege(:role, :function, 'EXECUTE')"),
                            {"role": role, "function": signature},
                        ).scalar_one()
                        is False
                    )
    finally:
        engine.dispose()


def test_guard_0019_refuses_downgrade_with_publication_evidence(
    capacity_guard_database: dict[str, object],
) -> None:
    cfg = _guard_config(capacity_guard_database)
    command.downgrade(cfg, "guard_0019")
    with _owner_connection(capacity_guard_database) as connection:
        _subject_id, _subject_incarnation, intent_id, _worker_id, _worker_incarnation = (
            _seed_executable_observation_rows(connection)
        )
        event = (
            connection.execute(
                text(
                    "SELECT event_id, agent_incarnation "
                    "FROM loom_capacity_guard.executable_admission_events "
                    "WHERE intent_id = :intent_id "
                    "ORDER BY event_id LIMIT 1"
                ),
                {"intent_id": intent_id},
            )
            .mappings()
            .one()
        )
        connection.execute(
            text(
                "INSERT INTO loom_capacity_guard.executable_release_publication_events "
                "(agent_incarnation, admission_event_id, publication_payload, "
                "publication_canonical_payload, publication_digest, "
                "manager_acknowledgement_digest) "
                "VALUES (:agent_incarnation, :event_id, '{}'::jsonb, CAST(:canonical AS bytea), "
                ":publication_digest, :manager_acknowledgement_digest)"
            ),
            {
                "agent_incarnation": event["agent_incarnation"],
                "event_id": event["event_id"],
                "canonical": b"{}",
                "publication_digest": "a" * 64,
                "manager_acknowledgement_digest": "b" * 64,
            },
        )

    with pytest.raises(DBAPIError, match="cannot downgrade guard_0019"):
        command.downgrade(cfg, "guard_0018")


def test_executable_release_publication_evidence_is_append_only(
    capacity_guard_database: dict[str, object],
) -> None:
    with _owner_connection(capacity_guard_database) as connection:
        _subject_id, _subject_incarnation, intent_id, _worker_id, _worker_incarnation = (
            _seed_executable_observation_rows(connection)
        )
        event = (
            connection.execute(
                text(
                    "SELECT event_id, agent_incarnation "
                    "FROM loom_capacity_guard.executable_admission_events "
                    "WHERE intent_id = :intent_id "
                    "ORDER BY event_id LIMIT 1"
                ),
                {"intent_id": intent_id},
            )
            .mappings()
            .one()
        )
        connection.execute(
            text(
                "INSERT INTO loom_capacity_guard.executable_release_publication_events "
                "(agent_incarnation, admission_event_id, publication_payload, "
                "publication_canonical_payload, publication_digest, "
                "manager_acknowledgement_digest) "
                "VALUES (:agent_incarnation, :event_id, '{}'::jsonb, CAST(:canonical AS bytea), "
                ":publication_digest, :manager_acknowledgement_digest)"
            ),
            {
                "agent_incarnation": event["agent_incarnation"],
                "event_id": event["event_id"],
                "canonical": b"{}",
                "publication_digest": hashlib.sha256(b"{}").hexdigest(),
                "manager_acknowledgement_digest": "b" * 64,
            },
        )

    statements = (
        "UPDATE loom_capacity_guard.executable_release_publication_events "
        "SET publication_digest = publication_digest",
        "DELETE FROM loom_capacity_guard.executable_release_publication_events",
        "TRUNCATE loom_capacity_guard.executable_release_publication_events CASCADE",
    )
    for statement in statements:
        with pytest.raises(DBAPIError, match="append-only"):
            with _owner_connection(capacity_guard_database) as connection:
                connection.execute(text(statement))


@pytest.mark.asyncio
async def test_guard_0016_backfills_legacy_agent_registration_audit_for_replay(
    capacity_guard_database: dict[str, object],
) -> None:
    """Upgraded guard_0015 agent registrations must replay against current audits."""

    cfg = _guard_config(capacity_guard_database)
    owner_role = _value(capacity_guard_database, "owner_role")
    agent_role = _value(capacity_guard_database, "agent_role")
    fence = GuardFenceV1(
        environment_id="dev-legacy-audit",
        subject_id=uuid4(),
        subject_incarnation=uuid4(),
        authority_incarnation=uuid4(),
        reporter_incarnation=uuid4(),
        deployment_generation=7,
        configuration_generation=11,
        candidate_digest="c" * 64,
    )
    registration = AgentRegistrationV1(
        environment_id=fence.environment_id,
        subject_id=fence.subject_id,
        subject_incarnation=fence.subject_incarnation,
        authority_incarnation=fence.authority_incarnation,
        agent_incarnation=uuid4(),
        reporter_incarnation=fence.reporter_incarnation,
        candidate_digest=fence.candidate_digest,
        deployment_generation=fence.deployment_generation,
        configuration_generation=fence.configuration_generation,
    )
    legacy_registration_payload = registration.model_dump(mode="json", exclude_none=False)
    for field in (
        "candidate_identity_algorithm",
        "candidate_identity",
        "candidate_publication_sha256",
    ):
        legacy_registration_payload.pop(field)

    command.downgrade(cfg, "guard_0015")
    with _owner_connection(capacity_guard_database) as connection:
        connection.execute(
            text(
                "INSERT INTO loom_capacity_guard.authority_state "
                "(singleton_id, schema_version, environment_id, subject_id, "
                "subject_incarnation, authority_mode, authority_incarnation, "
                "reporter_incarnation, reporter_high_water, allocation_epoch, "
                "deployment_generation, configuration_generation, candidate_digest) "
                "VALUES (1, 1, :environment_id, :subject_id, :subject_incarnation, "
                "'disabled', :authority_incarnation, :reporter_incarnation, 0, 0, "
                ":deployment_generation, :configuration_generation, :candidate_digest)"
            ),
            fence.model_dump(mode="python", exclude_none=False),
        )
        connection.execute(
            text(
                "INSERT INTO loom_capacity_guard.audit_events "
                "(event_type, payload, payload_digest) "
                "VALUES ('authority_initialized.v1', CAST(:payload AS jsonb), :digest)"
            ),
            {
                "payload": json.dumps(fence.model_dump(mode="json", exclude_none=False)),
                "digest": canonical_digest(fence),
            },
        )
        connection.execute(
            text(
                "INSERT INTO loom_capacity_guard.agent_registrations "
                "(agent_incarnation, singleton_id, schema_version, environment_id, "
                "subject_id, subject_incarnation, authority_incarnation, "
                "reporter_incarnation, authority_mode, allocation_epoch, "
                "candidate_digest, deployment_generation, configuration_generation, "
                "registration_state) "
                "VALUES (:agent_incarnation, 1, 1, :environment_id, :subject_id, "
                ":subject_incarnation, :authority_incarnation, :reporter_incarnation, "
                "'disabled', 0, :candidate_digest, :deployment_generation, "
                ":configuration_generation, 'registered')"
            ),
            registration.model_dump(mode="python", exclude_none=False),
        )
        connection.execute(
            text(
                "INSERT INTO loom_capacity_guard.agent_reporter_state "
                "(agent_incarnation, high_water) VALUES (:agent_incarnation, 0)"
            ),
            {"agent_incarnation": registration.agent_incarnation},
        )
        connection.execute(
            text(
                "INSERT INTO loom_capacity_guard.audit_events "
                "(event_type, payload, payload_digest) "
                "VALUES ('agent_registered.v1', CAST(:payload AS jsonb), :digest)"
            ),
            {
                "payload": json.dumps(legacy_registration_payload),
                "digest": _digest_payload(legacy_registration_payload),
            },
        )

    command.upgrade(cfg, "head")
    engine = create_async_engine(
        make_url(_value(capacity_guard_database, "migrator_url")),
        isolation_level="SERIALIZABLE",
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    quoted_owner = engine.sync_engine.dialect.identifier_preparer.quote(owner_role)
    try:
        async with factory() as session, session.begin():
            await session.execute(text(f"SET LOCAL ROLE {quoted_owner}"))
            store = CapacityAgentStore(
                session,
                expected_owner_role=owner_role,
                expected_agent_role=agent_role,
            )
            assert await store.register_agent(registration) == registration
    finally:
        await engine.dispose()


def test_guard_0016_backfills_legacy_agent_audits_from_event_time_candidate_digest(
    capacity_guard_database: dict[str, object],
) -> None:
    """Legacy register/reconfigure audits keep their event-time candidate identity."""

    cfg = _guard_config(capacity_guard_database)
    fence_a = GuardFenceV1(
        environment_id="dev-legacy-audit-history",
        subject_id=uuid4(),
        subject_incarnation=uuid4(),
        authority_incarnation=uuid4(),
        reporter_incarnation=uuid4(),
        deployment_generation=7,
        configuration_generation=11,
        candidate_digest="c" * 64,
    )
    registration_a = AgentRegistrationV1(
        environment_id=fence_a.environment_id,
        subject_id=fence_a.subject_id,
        subject_incarnation=fence_a.subject_incarnation,
        authority_incarnation=fence_a.authority_incarnation,
        agent_incarnation=uuid4(),
        reporter_incarnation=fence_a.reporter_incarnation,
        candidate_digest=fence_a.candidate_digest,
        deployment_generation=fence_a.deployment_generation,
        configuration_generation=fence_a.configuration_generation,
    )
    registration_b = AgentRegistrationV1(
        environment_id=fence_a.environment_id,
        subject_id=fence_a.subject_id,
        subject_incarnation=fence_a.subject_incarnation,
        authority_incarnation=fence_a.authority_incarnation,
        agent_incarnation=registration_a.agent_incarnation,
        reporter_incarnation=uuid4(),
        candidate_digest="d" * 64,
        deployment_generation=8,
        configuration_generation=12,
    )

    def legacy_payload(registration: AgentRegistrationV1) -> dict[str, object]:
        payload = registration.model_dump(mode="json", exclude_none=False)
        for field in (
            "candidate_identity_algorithm",
            "candidate_identity",
            "candidate_publication_sha256",
        ):
            payload.pop(field)
        return payload

    legacy_payload_a = legacy_payload(registration_a)
    legacy_payload_b = legacy_payload(registration_b)

    command.downgrade(cfg, "guard_0015")
    with _owner_connection(capacity_guard_database) as connection:
        connection.execute(
            text(
                "INSERT INTO loom_capacity_guard.authority_state "
                "(singleton_id, schema_version, environment_id, subject_id, "
                "subject_incarnation, authority_mode, authority_incarnation, "
                "reporter_incarnation, reporter_high_water, allocation_epoch, "
                "deployment_generation, configuration_generation, candidate_digest) "
                "VALUES (1, 1, :environment_id, :subject_id, :subject_incarnation, "
                "'disabled', :authority_incarnation, :reporter_incarnation, 0, 0, "
                ":deployment_generation, :configuration_generation, :candidate_digest)"
            ),
            fence_a.model_dump(mode="python", exclude_none=False),
        )
        connection.execute(
            text(
                "INSERT INTO loom_capacity_guard.agent_registrations "
                "(agent_incarnation, singleton_id, schema_version, environment_id, "
                "subject_id, subject_incarnation, authority_incarnation, "
                "reporter_incarnation, authority_mode, allocation_epoch, "
                "candidate_digest, deployment_generation, configuration_generation, "
                "registration_state) "
                "VALUES (:agent_incarnation, 1, 1, :environment_id, :subject_id, "
                ":subject_incarnation, :authority_incarnation, :reporter_incarnation, "
                "'disabled', 0, :candidate_digest, :deployment_generation, "
                ":configuration_generation, 'registered')"
            ),
            registration_a.model_dump(mode="python", exclude_none=False),
        )
        connection.execute(
            text(
                "INSERT INTO loom_capacity_guard.agent_reporter_state "
                "(agent_incarnation, high_water) VALUES (:agent_incarnation, 0)"
            ),
            {"agent_incarnation": registration_a.agent_incarnation},
        )
        connection.execute(
            text(
                "INSERT INTO loom_capacity_guard.audit_events "
                "(event_type, payload, payload_digest) "
                "VALUES ('agent_registered.v1', CAST(:payload AS jsonb), :digest)"
            ),
            {
                "payload": json.dumps(legacy_payload_a),
                "digest": _digest_payload(legacy_payload_a),
            },
        )
        connection.execute(
            text(
                "UPDATE loom_capacity_guard.authority_state "
                "SET reporter_incarnation = :reporter_incarnation, "
                "candidate_digest = :candidate_digest, "
                "deployment_generation = :deployment_generation, "
                "configuration_generation = :configuration_generation, "
                "updated_at = updated_at + interval '1 second' "
                "WHERE singleton_id = 1"
            ),
            registration_b.model_dump(mode="python", exclude_none=False),
        )
        connection.execute(
            text(
                "UPDATE loom_capacity_guard.agent_registrations "
                "SET reporter_incarnation = :reporter_incarnation, "
                "candidate_digest = :candidate_digest, "
                "deployment_generation = :deployment_generation, "
                "configuration_generation = :configuration_generation "
                "WHERE agent_incarnation = :agent_incarnation"
            ),
            registration_b.model_dump(mode="python", exclude_none=False),
        )
        connection.execute(
            text(
                "INSERT INTO loom_capacity_guard.audit_events "
                "(event_type, payload, payload_digest) "
                "VALUES ('agent_reconfigured.v1', CAST(:payload AS jsonb), :digest)"
            ),
            {
                "payload": json.dumps(legacy_payload_b),
                "digest": _digest_payload(legacy_payload_b),
            },
        )

    command.upgrade(cfg, "head")
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            rows = (
                connection.execute(
                    text(
                        "SELECT event_type, payload, payload_digest "
                        "FROM loom_capacity_guard.audit_events "
                        "WHERE event_type IN "
                        "('agent_registered.v1', 'agent_reconfigured.v1') "
                        "ORDER BY event_id"
                    )
                )
                .mappings()
                .all()
            )
    finally:
        engine.dispose()

    expected = [
        (
            "agent_registered.v1",
            registration_a.model_dump(mode="json", exclude_none=False),
            canonical_digest(registration_a),
        ),
        (
            "agent_reconfigured.v1",
            registration_b.model_dump(mode="json", exclude_none=False),
            canonical_digest(registration_b),
        ),
    ]
    assert [(row["event_type"], row["payload"], row["payload_digest"]) for row in rows] == expected


def test_guard_0014_observation_requires_the_exact_prepared_event(
    capacity_guard_database: dict[str, object],
) -> None:
    with _owner_connection(capacity_guard_database) as connection:
        (
            subject_id,
            subject_incarnation,
            intent_id,
            _worker_id,
            _worker_incarnation,
        ) = _seed_executable_observation_rows(connection, include_prepared=False)
        connection.exec_driver_sql(
            "GRANT USAGE ON SCHEMA loom_capacity_guard TO "
            f'"{_value(capacity_guard_database, "observer_role")}"'
        )

    observer = create_engine(
        _value(capacity_guard_database, "observer_url"), isolation_level="SERIALIZABLE"
    )
    try:
        with observer.connect() as connection:
            connection.exec_driver_sql("SET LOCAL enable_bitmapscan = off")
            connection.exec_driver_sql("SET LOCAL enable_indexscan = off")
            with pytest.raises(DBAPIError) as caught:
                connection.execute(
                    text(
                        "SELECT loom_capacity_guard.observe_executable_intent("
                        ":subject_id, :subject_incarnation, :intent_id)"
                    ),
                    {
                        "subject_id": subject_id,
                        "subject_incarnation": subject_incarnation,
                        "intent_id": intent_id,
                    },
                ).scalar_one()
        assert getattr(caught.value.orig, "sqlstate", None) == "55000"
    finally:
        observer.dispose()


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
    assert "LOOM_CAPACITY_GUARD_RUNTIME_ROLE" in source
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
    monkeypatch.delenv("LOOM_CAPACITY_GUARD_OBSERVER_ROLE", raising=False)
    monkeypatch.delenv("LOOM_CAPACITY_GUARD_RUNTIME_ROLE", raising=False)
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

    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_AGENT_ROLE", _value(capacity_guard_database, "agent_role")
    )
    with pytest.raises(RuntimeError, match="LOOM_CAPACITY_GUARD_EXECUTOR_ROLE"):
        command.current(cfg)

    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_EXECUTOR_ROLE",
        _value(capacity_guard_database, "executor_role"),
    )
    with pytest.raises(RuntimeError, match="LOOM_CAPACITY_GUARD_OBSERVER_ROLE"):
        command.current(cfg)

    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_OBSERVER_ROLE",
        _value(capacity_guard_database, "observer_role"),
    )
    with pytest.raises(RuntimeError, match="LOOM_CAPACITY_GUARD_RUNTIME_ROLE"):
        command.current(cfg)

    monkeypatch.setenv(
        "LOOM_CAPACITY_GUARD_RUNTIME_ROLE",
        _value(capacity_guard_database, "observer_role"),
    )
    with pytest.raises(RuntimeError, match="LOOM_CAPACITY_GUARD_RUNTIME_ROLE"):
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
