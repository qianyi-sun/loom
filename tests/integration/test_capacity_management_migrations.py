"""Independent management-schema migration and safety constraints."""

from __future__ import annotations

import importlib
import json
import os
import time
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from uuid import UUID, uuid4

import pytest
import sqlalchemy
from alembic import command
from alembic.config import Config as AlembicConfig
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from loom_capacity_manager.executable_contracts import (
    MAX_EXECUTABLE_ADMISSION_PROPOSAL_BYTES,
)
from loom_capacity_manager.models import CapacityAuditEvent, CapacityAuthorityState
from loom_capacity_manager.schema_startup import (
    CapacitySchemaNotAtHeadError,
    assert_capacity_schema_at_head,
)

EXPECTED_TABLES = {
    "capacity_account_policies",
    "capacity_allocation_epochs",
    "capacity_allocations",
    "capacity_audit_events",
    "capacity_authority_state",
    "capacity_candidates",
    "capacity_config_generations",
    "capacity_configuration_epochs",
    "capacity_demand_reporters",
    "capacity_demand_snapshots",
    "capacity_development_projections",
    "capacity_deployment_generations",
    "capacity_executors",
    "capacity_executor_observations",
    "capacity_execution_epochs",
    "capacity_execution_executors",
    "capacity_executable_bootstrap_acknowledgements",
    "capacity_executable_bootstrap_proposals",
    "capacity_executable_admission_acknowledgements",
    "capacity_executable_admission_closure_acknowledgements",
    "capacity_executable_admission_proposals",
    "capacity_executable_command_receipts",
    "capacity_executable_executor_states",
    "capacity_executable_intents",
    "capacity_executable_launch_rate_buckets",
    "capacity_executable_protected_release_receipts",
    "capacity_executable_tranches",
    "capacity_fairness_state",
    "capacity_launch_permits",
    "capacity_launch_rate_buckets",
    "capacity_observed_commitments",
    "capacity_pool_observations",
    "capacity_pool_reporters",
    "capacity_pools",
    "capacity_protected_release_acknowledgements",
    "capacity_reservation_release_evidence",
    "capacity_reservation_shapes",
    "capacity_reservation_tranches",
    "capacity_submission_intents",
    "capacity_subjects",
    "capacity_tiers",
    "capacity_worker_profiles",
}

CAPACITY_0014_FUNCTION_SIGNATURES = (
    "public.capacity_executable_canonical_jsonb_text(jsonb)",
    "public.capacity_executable_admission_proposal_payload_is_exact("
    "jsonb,uuid,bigint,bigint,uuid,uuid,text,text,text,text,timestamptz)",
    "public.capacity_executable_admission_ack_payload_is_exact(jsonb,jsonb)",
    "public.capacity_executable_intent_protected_bootstrap_guard()",
    "public.capacity_executable_admission_proposal_insert_guard()",
    "public.capacity_executable_admission_ack_insert_guard()",
    "public.capacity_executable_admission_closure_ack_insert_guard()",
)

CAPACITY_0012_INTENT_GUARD_SIGNATURE = (
    "public.capacity_executable_intent_protected_bootstrap_guard()"
)


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.split())


def _capacity_config(url: str) -> AlembicConfig:
    root = Path(__file__).resolve().parents[2]
    cfg = AlembicConfig(str(root / "capacity_migrations" / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "capacity_migrations"))
    os.environ["LOOM_CAPACITY_DB_URL"] = url
    return cfg


def _assert_function_execute_acls_are_owner_only(
    connection: Connection,
    *,
    signatures: tuple[str, ...],
) -> None:
    owner = connection.execute(text("SELECT current_user")).scalar_one()
    rows = connection.execute(
        text(
            "WITH requested(signature) AS ("
            "SELECT unnest(CAST(:signatures AS text[]))"
            ") "
            "SELECT requested.signature, "
            "pg_get_userbyid(procedure.proowner) AS procedure_owner, "
            "CASE WHEN privilege.grantee = 0 THEN 'PUBLIC' "
            "ELSE pg_get_userbyid(privilege.grantee) END AS grantee, "
            "pg_get_userbyid(privilege.grantor) AS grantor, "
            "privilege.privilege_type, privilege.is_grantable "
            "FROM requested "
            "JOIN pg_proc AS procedure "
            "ON procedure.oid = to_regprocedure(requested.signature) "
            "CROSS JOIN LATERAL aclexplode(COALESCE("
            "procedure.proacl, acldefault('f', procedure.proowner)"
            ")) AS privilege "
            "ORDER BY requested.signature, grantee, privilege.privilege_type"
        ),
        {"signatures": list(signatures)},
    ).all()
    assert rows == [
        (signature, owner, owner, owner, "EXECUTE", False)
        for signature in sorted(signatures)
    ]


def _seed_execution_pools(connection: Connection, *, configuration_epoch: int) -> None:
    for pool_id in ("gb10", "oldlab"):
        connection.execute(
            text(
                "INSERT INTO capacity_pools "
                "(id, configuration_epoch, pool_id, pool_generation, pool_digest, "
                "controller, partition, association, protocol_generation, "
                "protocol_digest, topology, envelope, health, max_slots, "
                "max_pending_slots, max_pending_jobs, submission_rate_per_minute) "
                "VALUES (:id, :configuration_epoch, :pool_id, 1, repeat('a', 64), "
                ":controller, 'migration-test', :association, 1, repeat('b', 64), "
                "'{}'::jsonb, '{}'::jsonb, 'eligible', 1, 1, 1, 1)"
            ),
            {
                "id": uuid4(),
                "configuration_epoch": configuration_epoch,
                "pool_id": pool_id,
                "controller": f"{pool_id}-controller",
                "association": f"{pool_id}-association",
            },
        )


def _seed_executable_allocation_history(
    connection: sqlalchemy.Connection,
    *,
    activate_execution: bool = True,
    include_child: bool = False,
    allocation_count: int | None = None,
    complete_payload: dict[str, object] | None = None,
    seal: bool = True,
) -> int:
    has_input_valid_until = (
        connection.execute(
            text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = 'public' "
                "AND table_name = 'capacity_allocation_epochs' "
                "AND column_name = 'input_valid_until'"
            )
        ).scalar_one_or_none()
        is not None
    )
    authority = connection.execute(
        text("SELECT authority_incarnation FROM capacity_authority_state WHERE singleton_id = 1")
    ).scalar_one()
    connection.execute(
        text(
            "INSERT INTO capacity_configuration_epochs "
            "(configuration_epoch, fleet_generation, fleet_digest, "
            "subject_generation_manifest, canonical_digest, "
            "activation_idempotency_key, activation_actor, "
            "activation_request_digest) VALUES "
            "(1, 1, repeat('1', 64), '[]'::jsonb, repeat('2', 64), "
            ":configuration_key, 'migration-test', repeat('3', 64))"
        ),
        {"configuration_key": uuid4()},
    )
    for pool_id in ("gb10", "oldlab"):
        connection.execute(
            text(
                "INSERT INTO capacity_pools "
                "(id, configuration_epoch, pool_id, pool_generation, pool_digest, "
                "controller, partition, association, protocol_generation, "
                "protocol_digest, topology, envelope, health, max_slots, "
                "max_pending_slots, max_pending_jobs, submission_rate_per_minute) "
                "VALUES (:id, 1, :pool_id, 1, repeat('4', 64), "
                "'slurm', :pool_id, :pool_id, 1, repeat('5', 64), "
                "'{}'::jsonb, '{}'::jsonb, 'healthy', 1, 1, 1, 1)"
            ),
            {"id": uuid4(), "pool_id": pool_id},
        )
    execution_manifest = "6" * 64
    oldlab_incarnation = uuid4()
    gb10_incarnation = uuid4()
    connection.execute(
        text(
            "INSERT INTO capacity_execution_epochs "
            "(execution_epoch, authority_incarnation, prepared_writer_epoch, "
            "current_writer_epoch, configuration_epoch, fleet_generation, "
            "fleet_digest, execution_manifest_sha256, manifest_payload, "
            "trusted_fleet_release_sha256, oldlab_executor_id, "
            "oldlab_executor_incarnation, oldlab_pool_id, oldlab_pool_generation, "
            "oldlab_signing_key_sha256, oldlab_local_authority_sha256, "
            "oldlab_controller_authority_sha256, gb10_executor_id, "
            "gb10_executor_incarnation, gb10_pool_id, gb10_pool_generation, "
            "gb10_signing_key_sha256, gb10_local_authority_sha256, "
            "gb10_controller_authority_sha256, environment_acknowledgements_sha256, "
            "legacy_writer_manifest_sha256, rollback_evidence_sha256, "
            "requested_ceiling, effective_ceiling, requested_rate_per_minute, "
            "effective_rate_per_minute, state, actor, idempotency_key, "
            "request_digest) VALUES "
            "(1, :authority, 1, 1, 1, 1, repeat('1', 64), "
            ":execution_manifest, '{}'::jsonb, repeat('7', 64), "
            "'oldlab-executor', :oldlab_incarnation, 'oldlab', 1, "
            "repeat('8', 64), repeat('9', 64), repeat('a', 64), "
            "'gb10-executor', :gb10_incarnation, 'gb10', 1, "
            "repeat('b', 64), repeat('c', 64), repeat('d', 64), "
            "repeat('e', 64), repeat('f', 64), repeat('0', 64), "
            "1, 0, 1, 0, 'prepared', 'migration-test', :execution_key, "
            "repeat('1', 64))"
        ),
        {
            "authority": authority,
            "execution_manifest": execution_manifest,
            "oldlab_incarnation": oldlab_incarnation,
            "gb10_incarnation": gb10_incarnation,
            "execution_key": uuid4(),
        },
    )
    if activate_execution:
        executor_values = {
            "oldlab": (oldlab_incarnation, "8", "9", "a"),
            "gb10": (gb10_incarnation, "b", "c", "d"),
        }
        for pool_id, (
            executor_incarnation,
            signing_digest,
            local_digest,
            controller_digest,
        ) in executor_values.items():
            connection.execute(
                text(
                    "INSERT INTO capacity_execution_executors "
                    "(id, execution_epoch, execution_manifest_sha256, executor_id, "
                    "executor_incarnation, pool_id, pool_generation, signing_key_id, "
                    "signing_key_sha256, local_authority_sha256, "
                    "controller_authority_sha256, actor, idempotency_key, "
                    "registration_digest, registration_payload) VALUES "
                    "(:id, 1, :execution_manifest, :executor_id, "
                    ":executor_incarnation, :pool_id, 1, :signing_key_id, "
                    ":signing_key_sha256, :local_authority_sha256, "
                    ":controller_authority_sha256, 'migration-test', "
                    ":idempotency_key, repeat('2', 64), '{}'::jsonb)"
                ),
                {
                    "id": uuid4(),
                    "execution_manifest": execution_manifest,
                    "executor_id": f"{pool_id}-executor",
                    "executor_incarnation": executor_incarnation,
                    "pool_id": pool_id,
                    "signing_key_id": f"{pool_id}-key",
                    "signing_key_sha256": signing_digest * 64,
                    "local_authority_sha256": local_digest * 64,
                    "controller_authority_sha256": controller_digest * 64,
                    "idempotency_key": uuid4(),
                },
            )
        connection.execute(
            text("UPDATE capacity_authority_state SET writer_epoch = 1 WHERE singleton_id = 1")
        )
        connection.execute(
            text(
                "UPDATE capacity_authority_state SET "
                "execution_epoch = 1, execution_state = 'prepared', "
                "execution_manifest_sha256 = :execution_manifest, "
                "executable_new_capacity_ceiling = 0, increase_freeze = true, "
                "increase_freeze_reason = 'execution_epoch_prepared' "
                "WHERE singleton_id = 1"
            ),
            {"execution_manifest": execution_manifest},
        )
        connection.execute(
            text(
                "UPDATE capacity_execution_epochs SET "
                "state = 'active', effective_ceiling = 1, "
                "effective_rate_per_minute = 1, activation_actor = 'migration-test', "
                "activation_idempotency_key = :activation_key, "
                "activation_request_digest = repeat('3', 64), "
                "activated_at = now() WHERE execution_epoch = 1"
            ),
            {"activation_key": uuid4()},
        )
        connection.execute(
            text(
                "UPDATE capacity_authority_state SET "
                "execution_state = 'active', executable_new_capacity_ceiling = 1, "
                "increase_freeze = false, increase_freeze_reason = NULL "
                "WHERE singleton_id = 1"
            )
        )
    resolved_allocation_count = int(include_child) if allocation_count is None else allocation_count
    allocation_epoch = connection.execute(
        text(
            "INSERT INTO capacity_allocation_epochs "
            "(writer_epoch, configuration_epoch, input_digest, status, "
            "failure_reason, complete_payload, executable, execution_epoch, "
            + ("input_valid_until, " if has_input_valid_until else "")
            + "execution_manifest_sha256, sealed, allocation_count, committed_at) VALUES "
            "(1, 1, repeat('a', 64), 'executable', NULL, "
            "CAST(:complete_payload AS jsonb), true, 1, "
            + ("now(), " if has_input_valid_until else "")
            + ":execution_manifest, false, "
            ":allocation_count, now()) RETURNING allocation_epoch"
        ),
        {
            "allocation_count": resolved_allocation_count,
            "complete_payload": json.dumps(
                {"allocations": [{} for _ in range(resolved_allocation_count)]}
                if complete_payload is None
                else complete_payload
            ),
            "execution_manifest": execution_manifest,
        },
    ).scalar_one()
    if include_child:
        connection.execute(
            text(
                "INSERT INTO capacity_allocations "
                "(id, allocation_epoch, subject_id, subject_incarnation, "
                "deployment_generation, pool_id, desired_shapes, desired_resources, "
                "commitments, drains, allowances, witness, mode, executable, "
                "execution_epoch, execution_manifest_sha256) VALUES "
                "(:id, :allocation_epoch, :subject_id, :subject_incarnation, "
                "1, 'gb10', '[]'::jsonb, '{}'::jsonb, '[]'::jsonb, "
                "'[]'::jsonb, '[]'::jsonb, '{}'::jsonb, 'executable', true, "
                "1, repeat('6', 64))"
            ),
            {
                "id": uuid4(),
                "allocation_epoch": allocation_epoch,
                "subject_id": uuid4(),
                "subject_incarnation": uuid4(),
            },
        )
    if seal:
        connection.execute(
            text(
                "UPDATE capacity_allocation_epochs SET sealed = true "
                "WHERE allocation_epoch = :allocation_epoch"
            ),
            {"allocation_epoch": allocation_epoch},
        )
    return allocation_epoch


@pytest.fixture
def isolated_capacity_migration_url(postgres_url: str) -> Iterator[str]:
    source_url = make_url(postgres_url)
    database_name = f"loom_capacity_migration_{uuid4().hex}"
    admin_engine = create_engine(source_url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    quoted = admin_engine.dialect.identifier_preparer.quote(database_name)
    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f"CREATE DATABASE {quoted} TEMPLATE template0")
        yield source_url.set(database=database_name).render_as_string(hide_password=False)
    finally:
        with admin_engine.connect() as connection:
            connection.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :database_name AND pid <> pg_backend_pid()"
                ),
                {"database_name": database_name},
            )
            connection.exec_driver_sql(f"DROP DATABASE IF EXISTS {quoted}")
        admin_engine.dispose()


def test_forward_migration_replaces_existing_0008_retirement_constraint(
    isolated_capacity_migration_url: str,
) -> None:
    """An installation already upgraded through 0008 must receive the new binding."""

    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0008")
    engine = create_engine(isolated_capacity_migration_url)
    try:
        with engine.connect() as connection:
            old_columns = {
                item["name"]
                for item in inspect(connection).get_columns("capacity_executable_executor_states")
            }
        assert "inventory_confirmation_journal_digest" not in old_columns

        execution_epoch = 1_300_001
        configuration_epoch = 1_300_001
        executor_incarnation = UUID(int=13001)
        with engine.begin() as connection:
            authority = connection.execute(
                text("SELECT authority_incarnation FROM capacity_authority_state")
            ).scalar_one()
            connection.execute(
                text(
                    "INSERT INTO capacity_configuration_epochs "
                    "(configuration_epoch, fleet_generation, fleet_digest, "
                    "subject_generation_manifest, canonical_digest, "
                    "activation_idempotency_key, activation_actor, "
                    "activation_request_digest) VALUES "
                    "(:configuration_epoch, 1, repeat('1', 64), '[]'::jsonb, "
                    "repeat('2', 64), :configuration_key, 'migration-test', "
                    "repeat('3', 64))"
                ),
                {
                    "configuration_epoch": configuration_epoch,
                    "configuration_key": uuid4(),
                },
            )
            _seed_execution_pools(connection, configuration_epoch=configuration_epoch)
            connection.execute(
                text(
                    "INSERT INTO capacity_execution_epochs "
                    "(execution_epoch, authority_incarnation, prepared_writer_epoch, "
                    "current_writer_epoch, configuration_epoch, fleet_generation, "
                    "fleet_digest, execution_manifest_sha256, manifest_payload, "
                    "trusted_fleet_release_sha256, oldlab_executor_id, "
                    "oldlab_executor_incarnation, oldlab_pool_id, oldlab_pool_generation, "
                    "oldlab_signing_key_sha256, oldlab_local_authority_sha256, "
                    "oldlab_controller_authority_sha256, "
                    "gb10_executor_id, gb10_executor_incarnation, gb10_pool_id, "
                    "gb10_pool_generation, gb10_signing_key_sha256, "
                    "gb10_local_authority_sha256, gb10_controller_authority_sha256, "
                    "environment_acknowledgements_sha256, "
                    "legacy_writer_manifest_sha256, rollback_evidence_sha256, "
                    "requested_ceiling, effective_ceiling, requested_rate_per_minute, "
                    "effective_rate_per_minute, state, actor, idempotency_key, "
                    "request_digest) VALUES "
                    "(:execution_epoch, :authority, 1, 1, :configuration_epoch, 1, "
                    "repeat('1', 64), repeat('4', 64), '{}'::jsonb, repeat('5', 64), "
                    "'oldlab-executor', :oldlab_incarnation, 'oldlab', 1, "
                    "repeat('a', 64), repeat('b', 64), repeat('c', 64), "
                    "'gb10-executor', :gb10_incarnation, 'gb10', 1, "
                    "repeat('a', 64), repeat('b', 64), repeat('c', 64), repeat('6', 64), "
                    "repeat('7', 64), repeat('8', 64), 1, 0, 2, 0, 'prepared', "
                    "'migration-test', :execution_key, repeat('9', 64))"
                ),
                {
                    "execution_epoch": execution_epoch,
                    "authority": authority,
                    "configuration_epoch": configuration_epoch,
                    "oldlab_incarnation": UUID(int=13002),
                    "gb10_incarnation": executor_incarnation,
                    "execution_key": uuid4(),
                },
            )
            connection.execute(
                text(
                    "INSERT INTO capacity_execution_executors "
                    "(id, execution_epoch, execution_manifest_sha256, executor_id, "
                    "executor_incarnation, pool_id, pool_generation, signing_key_id, "
                    "signing_key_sha256, local_authority_sha256, "
                    "controller_authority_sha256, actor, idempotency_key, "
                    "registration_digest, registration_payload) VALUES "
                    "(:id, :execution_epoch, repeat('4', 64), 'gb10-executor', "
                    ":executor_incarnation, 'gb10', 1, 'gb10-key', repeat('a', 64), "
                    "repeat('b', 64), repeat('c', 64), 'migration-test', "
                    ":idempotency_key, repeat('d', 64), '{}'::jsonb)"
                ),
                {
                    "id": uuid4(),
                    "execution_epoch": execution_epoch,
                    "executor_incarnation": executor_incarnation,
                    "idempotency_key": uuid4(),
                },
            )
            connection.execute(
                text(
                    "INSERT INTO capacity_executable_executor_states "
                    "(id, execution_epoch, execution_manifest_sha256, executor_id, "
                    "executor_incarnation, pool_id, pool_generation, state, "
                    "journal_high_water, journal_digest, inventory_high_water, "
                    "last_inventory_digest, inventory_payload, last_inventory_at, "
                    "retirement_safe, retirement_inventory_digest, lease_expires_at, "
                    "last_heartbeat_at) VALUES "
                    "(:id, :execution_epoch, repeat('4', 64), 'gb10-executor', "
                    ":executor_incarnation, 'gb10', 1, 'current', 1, repeat('a', 64), "
                    "1, repeat('f', 64), jsonb_build_object("
                    "'schema_version', 2, 'execution', jsonb_build_object("
                    "'execution_epoch', :execution_epoch, "
                    "'execution_manifest_sha256', repeat('4', 64)), "
                    "'executor_id', 'gb10-executor', "
                    "'executor_incarnation', CAST(:executor_incarnation AS text), "
                    "'pool_id', 'gb10', 'pool_generation', 1, "
                    "'inventory_sequence', 1, 'journal_sequence', 1, "
                    "'journal_digest', repeat('a', 64)), now(), true, "
                    "repeat('f', 64), now() + interval '1 minute', now())"
                ),
                {
                    "id": uuid4(),
                    "execution_epoch": execution_epoch,
                    "executor_incarnation": executor_incarnation,
                },
            )
            assert (
                connection.execute(
                    text(
                        "SELECT retirement_safe FROM capacity_executable_executor_states "
                        "WHERE execution_epoch = :execution_epoch"
                    ),
                    {"execution_epoch": execution_epoch},
                ).scalar_one()
                is True
            )

        command.upgrade(cfg, "head")
        with engine.connect() as connection:
            columns = {
                item["name"]
                for item in inspect(connection).get_columns("capacity_executable_executor_states")
            }
            checks = {
                item["name"]: item["sqltext"]
                for item in inspect(connection).get_check_constraints(
                    "capacity_executable_executor_states"
                )
            }
            invalidated = connection.execute(
                text(
                    "SELECT retirement_safe, retirement_inventory_digest, "
                    "inventory_confirmation_journal_digest "
                    "FROM capacity_executable_executor_states "
                    "WHERE execution_epoch = :execution_epoch"
                ),
                {"execution_epoch": execution_epoch},
            ).one()
        assert "inventory_confirmation_journal_digest" in columns
        assert invalidated == (False, None, None)
        assert (
            "inventory_confirmation_journal_digest"
            in checks["capacity_executable_executor_retirement_check"]
        )
        assert (
            "last_heartbeat_at > last_inventory_at"
            in checks["capacity_executable_executor_retirement_check"]
        )
    finally:
        engine.dispose()


def test_shadow_schema_has_execution_epoch_table_and_zero_execution_guard(
    capacity_postgres_url: str,
) -> None:
    engine = create_engine(capacity_postgres_url)
    try:
        with engine.connect() as connection:
            inspector = inspect(connection)
            tables = set(inspector.get_table_names())
            assert tables == EXPECTED_TABLES | {"alembic_version"}
            assert "teams" not in tables
            execution_pool_foreign_keys = {
                tuple(item["constrained_columns"])
                for item in inspector.get_foreign_keys("capacity_execution_epochs")
            }
            assert {
                ("configuration_epoch", "oldlab_pool_id", "oldlab_pool_generation"),
                ("configuration_epoch", "gb10_pool_id", "gb10_pool_generation"),
            } <= execution_pool_foreign_keys
            authority_execution_foreign_keys = {
                tuple(item["constrained_columns"])
                for item in inspector.get_foreign_keys("capacity_authority_state")
            }
            assert (
                "authority_incarnation",
                "writer_epoch",
                "execution_epoch",
                "execution_manifest_sha256",
                "execution_state",
                "executable_new_capacity_ceiling",
            ) in authority_execution_foreign_keys
            authority = (
                connection.execute(
                    text(
                        "SELECT execution_epoch, execution_state, "
                        "execution_manifest_sha256, executable_new_capacity_ceiling "
                        "FROM capacity_authority_state WHERE singleton_id = 1"
                    )
                )
                .mappings()
                .one()
            )
            assert authority == {
                "execution_epoch": 0,
                "execution_state": "shadow",
                "execution_manifest_sha256": None,
                "executable_new_capacity_ceiling": 0,
            }
        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "UPDATE capacity_authority_state "
                        "SET execution_epoch = 1, execution_state = 'active', "
                        "execution_manifest_sha256 = NULL, "
                        "executable_new_capacity_ceiling = 1 "
                        "WHERE singleton_id = 1"
                    )
                )
    finally:
        engine.dispose()


def test_capacity_0004_accepts_candidate_insert_from_running_0003_writer(
    capacity_postgres_url: str,
) -> None:
    """The expand migration must not break an old manager during rollout."""

    candidate_id = uuid4()
    engine = create_engine(capacity_postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO capacity_candidates "
                    "(id, subject_id, subject_incarnation, candidate_generation, "
                    "candidate_digest, source_payload, artifact_payload, "
                    "architecture_payload, launcher_payload, attestation_payload, "
                    "protocol_payload) VALUES "
                    "(:id, :subject_id, :subject_incarnation, 1, repeat('1', 64), "
                    "jsonb_build_object('publication_sha256', repeat('2', 64)), "
                    "'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb)"
                ),
                {
                    "id": candidate_id,
                    "subject_id": uuid4(),
                    "subject_incarnation": uuid4(),
                },
            )
        with engine.connect() as connection:
            candidate = (
                connection.execute(
                    text(
                        "SELECT candidate_identity_algorithm, candidate_identity "
                        "FROM capacity_candidates WHERE id = :id"
                    ),
                    {"id": candidate_id},
                )
                .mappings()
                .one()
            )
            assert candidate == {
                "candidate_identity_algorithm": "source-sha256",
                "candidate_identity": "1" * 64,
            }
    finally:
        engine.dispose()


def test_capacity_0004_refuses_lossy_git_candidate_downgrade(
    isolated_capacity_migration_url: str,
) -> None:
    """A Git identity must never be silently relabeled by re-upgrade."""

    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0004")
    candidate_id = uuid4()
    engine = create_engine(isolated_capacity_migration_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO capacity_candidates "
                    "(id, subject_id, subject_incarnation, candidate_generation, "
                    "candidate_digest, candidate_identity_algorithm, candidate_identity, "
                    "source_payload, artifact_payload, architecture_payload, launcher_payload, "
                    "attestation_payload, protocol_payload) VALUES "
                    "(:id, :subject_id, :subject_incarnation, 1, repeat('2', 64), "
                    "'git-sha1', repeat('1', 40), "
                    "jsonb_build_object('publication_sha256', repeat('2', 64)), "
                    "'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb)"
                ),
                {
                    "id": candidate_id,
                    "subject_id": uuid4(),
                    "subject_incarnation": uuid4(),
                },
            )

        with pytest.raises(RuntimeError, match=r"cannot downgrade.*candidate identity"):
            command.downgrade(cfg, "capacity_0003")

        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == ("capacity_0004")
            identity = connection.execute(
                text(
                    "SELECT candidate_identity_algorithm, candidate_identity "
                    "FROM capacity_candidates WHERE id = :id"
                ),
                {"id": candidate_id},
            ).one()
            assert identity == ("git-sha1", "1" * 40)
    finally:
        engine.dispose()


def test_capacity_0004_downgrade_serializes_candidate_preflight_with_writers(
    isolated_capacity_migration_url: str,
) -> None:
    """A concurrent Git candidate cannot arrive after downgrade's safety check."""

    application_name = f"capacity-downgrade-race-{uuid4().hex}"
    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0004")
    observer_engine = create_engine(
        isolated_capacity_migration_url,
        isolation_level="AUTOCOMMIT",
    )
    writer_engine = create_engine(isolated_capacity_migration_url)
    downgrade_engine = create_engine(
        isolated_capacity_migration_url,
        connect_args={"application_name": application_name},
    )
    migration = importlib.import_module(
        "capacity_migrations.versions.capacity_0004_executable_bridge"
    )

    def run_downgrade() -> None:
        with downgrade_engine.begin() as connection:
            migration_context = MigrationContext.configure(connection)
            with Operations.context(migration_context):
                migration.downgrade()

    def wait_until_downgrade_is_blocked(
        connection: sqlalchemy.Connection,
        downgrade: Future[None],
    ) -> None:
        deadline = time.monotonic() + 5
        observed: list[dict[str, object]] = []
        while time.monotonic() < deadline:
            if downgrade.done():
                downgrade.result()
                raise AssertionError(
                    "capacity downgrade exited before waiting for the candidate table lock"
                )
            connection.execute(text("SELECT pg_stat_clear_snapshot()"))
            observed = [
                dict(row)
                for row in connection.execute(
                    text(
                        "SELECT application_name, state, wait_event_type, wait_event, query "
                        "FROM pg_stat_activity WHERE datname = current_database() "
                        "AND pid <> pg_backend_pid()"
                    )
                ).mappings()
            ]
            if any(row["wait_event_type"] == "Lock" for row in observed):
                return
            time.sleep(0.01)
        raise AssertionError(
            f"capacity downgrade did not wait for the candidate table lock: {observed!r}"
        )

    try:
        with (
            ThreadPoolExecutor(max_workers=1) as executor,
            writer_engine.connect() as writer,
            observer_engine.connect() as observer,
        ):
            writer_transaction = writer.begin()
            try:
                candidate_id = uuid4()
                writer.execute(
                    text(
                        "INSERT INTO capacity_candidates "
                        "(id, subject_id, subject_incarnation, candidate_generation, "
                        "candidate_digest, candidate_identity_algorithm, candidate_identity, "
                        "source_payload, artifact_payload, architecture_payload, "
                        "launcher_payload, attestation_payload, protocol_payload) VALUES "
                        "(:id, :subject_id, :subject_incarnation, 1, repeat('2', 64), "
                        "'git-sha1', repeat('1', 40), "
                        "jsonb_build_object('publication_sha256', repeat('2', 64)), "
                        "'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb)"
                    ),
                    {
                        "id": candidate_id,
                        "subject_id": uuid4(),
                        "subject_incarnation": uuid4(),
                    },
                )
                downgrade = executor.submit(run_downgrade)
                wait_until_downgrade_is_blocked(observer, downgrade)
                writer_transaction.commit()
            finally:
                if writer_transaction.is_active:
                    writer_transaction.rollback()

            with pytest.raises(RuntimeError, match=r"cannot downgrade.*candidate identity"):
                downgrade.result(timeout=5)

        with writer_engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == ("capacity_0004")
            assert connection.execute(
                text(
                    "SELECT candidate_identity_algorithm, candidate_identity "
                    "FROM capacity_candidates WHERE id = :id"
                ),
                {"id": candidate_id},
            ).one() == ("git-sha1", "1" * 40)
    finally:
        observer_engine.dispose()
        writer_engine.dispose()
        downgrade_engine.dispose()


def test_execution_epochs_reject_truncate(capacity_postgres_url: str) -> None:
    """A bulk delete must not bypass immutable execution history."""

    engine = create_engine(capacity_postgres_url)
    try:
        with pytest.raises(IntegrityError, match="append-only"):
            with engine.begin() as connection:
                connection.execute(text("TRUNCATE TABLE capacity_execution_epochs CASCADE"))
    finally:
        engine.dispose()


def test_executable_runtime_rows_bind_exact_registered_executor(
    capacity_postgres_url: str,
) -> None:
    engine = create_engine(capacity_postgres_url)
    expected_executor_columns = [
        "execution_epoch",
        "execution_manifest_sha256",
        "executor_id",
        "executor_incarnation",
        "pool_id",
        "pool_generation",
    ]
    try:
        with engine.connect() as connection:
            schema = inspect(connection)
            executor_uniques = {
                item["name"]: item["column_names"]
                for item in schema.get_unique_constraints("capacity_execution_executors")
            }
            assert executor_uniques["capacity_execution_executor_exact_binding_key"] == (
                expected_executor_columns
            )

            for table_name, constraint_name in (
                (
                    "capacity_executable_executor_states",
                    "capacity_executable_executor_registration_fkey",
                ),
                (
                    "capacity_executable_intents",
                    "capacity_executable_intent_executor_fkey",
                ),
            ):
                foreign_keys = {item["name"]: item for item in schema.get_foreign_keys(table_name)}
                assert foreign_keys[constraint_name]["constrained_columns"] == (
                    expected_executor_columns
                )

            receipt_foreign_keys = {
                item["name"]: item
                for item in schema.get_foreign_keys("capacity_executable_command_receipts")
            }
            assert receipt_foreign_keys["capacity_executable_command_receipt_executor_fkey"][
                "constrained_columns"
            ] == ["execution_epoch", "executor_incarnation"]
    finally:
        engine.dispose()


def test_capacity_0004_preserves_populated_writer_and_journal_across_downgrade(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    engine = create_engine(isolated_capacity_migration_url)
    try:
        command.upgrade(cfg, "capacity_0003")
        with engine.begin() as connection:
            candidate_id = uuid4()
            authority = connection.execute(
                text(
                    "UPDATE capacity_authority_state SET writer_epoch = 7 "
                    "WHERE singleton_id = 1 RETURNING authority_incarnation"
                )
            ).scalar_one()
            connection.execute(
                text(
                    "INSERT INTO capacity_executors "
                    "(id, executor_id, executor_incarnation, pool_id, pool_generation, "
                    "authority_incarnation, registered_writer_epoch, signing_key_id, "
                    "signing_key_sha256, local_authority_sha256, registration_actor, "
                    "registration_idempotency_key, registration_digest, state, "
                    "command_high_water, last_command_digest, heartbeat_high_water, "
                    "last_heartbeat_digest, journal_high_water, journal_digest, "
                    "inventory_high_water, last_inventory_digest, lease_expires_at, "
                    "last_heartbeat_at) VALUES "
                    "(:id, 'oldlab-executor', :incarnation, 'oldlab', 1, :authority, 7, "
                    "'oldlab-key', repeat('a', 64), repeat('b', 64), 'installer', "
                    ":idempotency_key, repeat('c', 64), 'dry-run', 4, repeat('d', 64), "
                    "3, repeat('e', 64), 5, repeat('f', 64), 2, repeat('1', 64), "
                    "now() + interval '1 hour', now())"
                ),
                {
                    "id": uuid4(),
                    "incarnation": uuid4(),
                    "authority": authority,
                    "idempotency_key": uuid4(),
                },
            )
            connection.execute(
                text(
                    "INSERT INTO capacity_candidates "
                    "(id, subject_id, subject_incarnation, candidate_generation, "
                    "candidate_digest, source_payload, artifact_payload, "
                    "architecture_payload, launcher_payload, attestation_payload, "
                    "protocol_payload) VALUES "
                    "(:id, :subject_id, :subject_incarnation, 1, repeat('1', 64), "
                    "jsonb_build_object('publication_sha256', repeat('2', 64)), "
                    "'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb)"
                ),
                {
                    "id": candidate_id,
                    "subject_id": uuid4(),
                    "subject_incarnation": uuid4(),
                },
            )

        command.upgrade(cfg, "capacity_0004")
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT writer_epoch FROM capacity_authority_state")
                ).scalar_one()
                == 7
            )
            assert (
                connection.execute(
                    text("SELECT journal_high_water FROM capacity_executors")
                ).scalar_one()
                == 5
            )
            assert (
                connection.execute(
                    text("SELECT execution_epoch FROM capacity_authority_state")
                ).scalar_one()
                == 0
            )
            candidate = (
                connection.execute(
                    text(
                        "SELECT candidate_digest, candidate_identity_algorithm, "
                        "candidate_identity FROM capacity_candidates WHERE id = :id"
                    ),
                    {"id": candidate_id},
                )
                .mappings()
                .one()
            )
            assert candidate == {
                "candidate_digest": "1" * 64,
                "candidate_identity_algorithm": "source-sha256",
                "candidate_identity": "1" * 64,
            }
            candidate_columns = {
                column["name"]: column
                for column in inspect(connection).get_columns("capacity_candidates")
            }
            for column_name in (
                "candidate_identity_algorithm",
                "candidate_identity",
            ):
                assert candidate_columns[column_name]["nullable"] is False
                assert candidate_columns[column_name]["default"] is None

        for values in (
            {
                "candidate_identity_algorithm": "source-sha256",
                "candidate_identity": "3" * 40,
            },
            {
                "candidate_identity_algorithm": "git-sha1",
                "candidate_identity": "3" * 64,
            },
            {
                "candidate_identity_algorithm": "sha256",
                "candidate_identity": "3" * 64,
            },
        ):
            with pytest.raises(IntegrityError):
                with engine.begin() as connection:
                    connection.execute(
                        text(
                            "UPDATE capacity_candidates SET "
                            "candidate_identity_algorithm = :candidate_identity_algorithm, "
                            "candidate_identity = :candidate_identity WHERE id = :id"
                        ),
                        values | {"id": candidate_id},
                    )

        command.downgrade(cfg, "capacity_0003")
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT writer_epoch FROM capacity_authority_state")
                ).scalar_one()
                == 7
            )
            assert (
                connection.execute(
                    text("SELECT journal_high_water FROM capacity_executors")
                ).scalar_one()
                == 5
            )
            assert "capacity_execution_epochs" not in inspect(connection).get_table_names()
            assert {
                "candidate_identity_algorithm",
                "candidate_identity",
            }.isdisjoint(
                column["name"] for column in inspect(connection).get_columns("capacity_candidates")
            )
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM pg_proc "
                        "WHERE proname = 'capacity_execution_epoch_transition_guard'"
                    )
                ).scalar_one()
                == 0
            )

        command.upgrade(cfg, "capacity_0004")
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT writer_epoch FROM capacity_authority_state")
                ).scalar_one()
                == 7
            )
            assert (
                connection.execute(
                    text("SELECT journal_high_water FROM capacity_executors")
                ).scalar_one()
                == 5
            )
            candidate = (
                connection.execute(
                    text(
                        "SELECT candidate_identity_algorithm, candidate_identity "
                        "FROM capacity_candidates WHERE id = :id"
                    ),
                    {"id": candidate_id},
                )
                .mappings()
                .one()
            )
            assert candidate == {
                "candidate_identity_algorithm": "source-sha256",
                "candidate_identity": "1" * 64,
            }
    finally:
        engine.dispose()


def test_existing_executable_intents_upgrade_to_observed_state_database_check(
    isolated_capacity_migration_url: str,
) -> None:
    """An already-0010 database must reject raw impossible observed states after 0011."""

    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0010")
    engine = create_engine(isolated_capacity_migration_url)
    try:
        with engine.begin() as connection:
            allocation_epoch = _seed_executable_allocation_history(
                connection,
                include_child=False,
                allocation_count=0,
                complete_payload={"allocations": []},
            )
            execution = (
                connection.execute(
                    text(
                        "SELECT execution_epoch, execution_manifest_sha256, "
                        "configuration_epoch, gb10_executor_id, gb10_executor_incarnation, "
                        "gb10_pool_id, gb10_pool_generation "
                        "FROM capacity_execution_epochs WHERE execution_epoch = 1"
                    )
                )
                .mappings()
                .one()
            )
            subject_id = UUID(int=17002)
            subject_incarnation = UUID(int=17003)
            for launch_rank in range(1, 3):
                intent_id = uuid4()
                tranche_id = uuid4()
                shape_instance_id = f"shape-{launch_rank}"
                connection.execute(
                    text(
                        "INSERT INTO capacity_executable_intents "
                        "(id, intent_id, tranche_id, shape_instance_id, execution_epoch, "
                        "execution_manifest_sha256, configuration_epoch, allocation_epoch, "
                        "executor_id, executor_incarnation, pool_id, pool_generation, "
                        "subject_id, subject_incarnation, launch_rank, proposal_digest, "
                        "proposal_payload, binding_digest, binding_payload) "
                        "VALUES "
                        "(:id, :intent_id, :tranche_id, :shape_instance_id, :execution_epoch, "
                        ":execution_manifest, :configuration_epoch, :allocation_epoch, "
                        ":executor_id, :executor_incarnation, :pool_id, :pool_generation, "
                        ":subject_id, :subject_incarnation, :launch_rank, repeat('e', 64), "
                        "'{}'::jsonb, repeat('f', 64), "
                        "jsonb_build_object("
                        "'schema_version', 2, "
                        "'intent_id', CAST(:intent_id AS text), "
                        "'tranche_id', CAST(:tranche_id AS text), "
                        "'shape_instance_id', CAST(:shape_instance_id AS text), "
                        "'subject_id', CAST(:subject_id AS text), "
                        "'subject_incarnation', CAST(:subject_incarnation AS text), "
                        "'pool_id', CAST(:pool_id AS text), "
                        "'pool_generation', CAST(:pool_generation AS bigint), "
                        "'executor_id', CAST(:executor_id AS text), "
                        "'executor_incarnation', CAST(:executor_incarnation AS text), "
                        "'execution', jsonb_build_object("
                        "'configuration_epoch', CAST(:configuration_epoch AS bigint), "
                        "'allocation_epoch', CAST(:allocation_epoch AS bigint), "
                        "'execution_epoch', CAST(:execution_epoch AS bigint), "
                        "'execution_manifest_sha256', CAST(:execution_manifest AS text))))"
                    ),
                    {
                        "id": uuid4(),
                        "intent_id": intent_id,
                        "tranche_id": tranche_id,
                        "shape_instance_id": shape_instance_id,
                        "execution_epoch": execution["execution_epoch"],
                        "execution_manifest": execution["execution_manifest_sha256"],
                        "configuration_epoch": execution["configuration_epoch"],
                        "allocation_epoch": allocation_epoch,
                        "executor_id": execution["gb10_executor_id"],
                        "executor_incarnation": execution["gb10_executor_incarnation"],
                        "pool_id": execution["gb10_pool_id"],
                        "pool_generation": execution["gb10_pool_generation"],
                        "subject_id": subject_id,
                        "subject_incarnation": subject_incarnation,
                        "launch_rank": launch_rank,
                    },
                )

        command.upgrade(cfg, "head")

        with engine.begin() as connection:
            connection.execute(
                text(
                    "ALTER TABLE capacity_executable_intents "
                    "DISABLE TRIGGER capacity_executable_intent_mutation_guard"
                )
            )
            connection.execute(
                text(
                    "UPDATE capacity_executable_intents "
                    "SET observed_state = 'active' WHERE shape_instance_id = 'shape-1'"
                )
            )
            connection.execute(
                text(
                    "UPDATE capacity_executable_intents "
                    "SET observed_state = NULL WHERE shape_instance_id = 'shape-2'"
                )
            )
            with pytest.raises(IntegrityError):
                connection.execute(
                    text(
                        "UPDATE capacity_executable_intents "
                        "SET observed_state = 'impossible' WHERE shape_instance_id = 'shape-1'"
                    )
                )
    finally:
        engine.dispose()


def test_fleet_configuration_generation_is_unique_despite_null_subject_binding(
    capacity_postgres_url: str,
) -> None:
    engine = create_engine(capacity_postgres_url)
    statement = text(
        "INSERT INTO capacity_config_generations "
        "(id, scope, subject_id, subject_incarnation, scope_generation, digest, "
        "payload, state, actor, idempotency_key) VALUES "
        "(:id, 'fleet', NULL, NULL, 99, :digest, '{}'::jsonb, 'proposed', "
        "'test', :idempotency_key)"
    )
    try:
        with engine.begin() as connection:
            connection.execute(
                statement,
                {
                    "id": uuid4(),
                    "digest": "a" * 64,
                    "idempotency_key": uuid4(),
                },
            )
        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    statement,
                    {
                        "id": uuid4(),
                        "digest": "b" * 64,
                        "idempotency_key": uuid4(),
                    },
                )
    finally:
        engine.dispose()


def test_capacity_0005_refuses_downgrade_with_executable_allocation_history(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    engine = create_engine(isolated_capacity_migration_url)
    try:
        command.upgrade(cfg, "capacity_0005")
        with engine.begin() as connection:
            _seed_executable_allocation_history(connection)

        with pytest.raises(
            RuntimeError,
            match="cannot downgrade capacity_0005 with executable allocation history",
        ):
            command.downgrade(cfg, "capacity_0004")

        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == ("capacity_0005")
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM capacity_allocation_epochs "
                        "WHERE status = 'executable'"
                    )
                ).scalar_one()
                == 1
            )
    finally:
        engine.dispose()


def test_capacity_0005_rejects_executable_allocation_without_active_authority(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    engine = create_engine(isolated_capacity_migration_url)
    try:
        command.upgrade(cfg, "capacity_0005")
        with pytest.raises(
            sqlalchemy.exc.DBAPIError,
            match="executable allocation requires the exact active authority",
        ):
            with engine.begin() as connection:
                _seed_executable_allocation_history(
                    connection,
                    activate_execution=False,
                )
    finally:
        engine.dispose()


def test_capacity_0005_requires_executable_allocation_seal_before_commit(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    engine = create_engine(isolated_capacity_migration_url)
    try:
        command.upgrade(cfg, "capacity_0005")
        with pytest.raises(
            sqlalchemy.exc.DBAPIError,
            match="executable allocation epoch must be sealed before commit",
        ):
            with engine.begin() as connection:
                _seed_executable_allocation_history(connection, seal=False)
                connection.execute(
                    text("SET CONSTRAINTS capacity_executable_allocation_seal_guard IMMEDIATE")
                )
    finally:
        engine.dispose()


def test_capacity_0005_requires_executable_child_manifest(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    engine = create_engine(isolated_capacity_migration_url)
    try:
        command.upgrade(cfg, "capacity_0005")
        with pytest.raises(sqlalchemy.exc.DBAPIError):
            with engine.begin() as connection:
                _seed_executable_allocation_history(
                    connection,
                    complete_payload={},
                )
    finally:
        engine.dispose()


def test_capacity_0005_requires_sealed_executable_child_count(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    engine = create_engine(isolated_capacity_migration_url)
    try:
        command.upgrade(cfg, "capacity_0005")
        with pytest.raises(
            sqlalchemy.exc.DBAPIError,
            match="executable allocation child count does not match sealed parent",
        ):
            with engine.begin() as connection:
                _seed_executable_allocation_history(
                    connection,
                    allocation_count=1,
                )
                connection.execute(
                    text("SET CONSTRAINTS capacity_executable_allocation_seal_guard IMMEDIATE")
                )
    finally:
        engine.dispose()


def test_capacity_0005_accepts_shadow_rows_from_running_0004_writer(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    engine = create_engine(isolated_capacity_migration_url)
    try:
        command.upgrade(cfg, "capacity_0005")
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO capacity_configuration_epochs "
                    "(configuration_epoch, fleet_generation, fleet_digest, "
                    "subject_generation_manifest, canonical_digest, "
                    "activation_idempotency_key, activation_actor, "
                    "activation_request_digest) VALUES "
                    "(1, 1, repeat('1', 64), '[]'::jsonb, repeat('2', 64), "
                    ":configuration_key, 'old-manager', repeat('3', 64))"
                ),
                {"configuration_key": uuid4()},
            )
            allocation_epoch = connection.execute(
                text(
                    "INSERT INTO capacity_allocation_epochs "
                    "(writer_epoch, configuration_epoch, input_digest, status, "
                    "failure_reason, complete_payload, executable, committed_at) VALUES "
                    "(1, 1, repeat('4', 64), 'shadow', NULL, '{}'::jsonb, false, now()) "
                    "RETURNING allocation_epoch"
                )
            ).scalar_one()
            connection.execute(
                text(
                    "INSERT INTO capacity_allocations "
                    "(id, allocation_epoch, subject_id, subject_incarnation, "
                    "deployment_generation, pool_id, desired_shapes, desired_resources, "
                    "commitments, drains, allowances, witness, mode, executable) VALUES "
                    "(:id, :allocation_epoch, :subject_id, :subject_incarnation, "
                    "1, 'gb10', '[]'::jsonb, '{}'::jsonb, '[]'::jsonb, "
                    "'[]'::jsonb, '[]'::jsonb, '{}'::jsonb, 'shadow', false)"
                ),
                {
                    "id": uuid4(),
                    "allocation_epoch": allocation_epoch,
                    "subject_id": uuid4(),
                    "subject_incarnation": uuid4(),
                },
            )

        with engine.connect() as connection:
            parent = connection.execute(
                text(
                    "SELECT execution_epoch, execution_manifest_sha256 "
                    "FROM capacity_allocation_epochs"
                )
            ).one()
            child = connection.execute(
                text("SELECT execution_epoch, execution_manifest_sha256 FROM capacity_allocations")
            ).one()
            assert parent == (None, None)
            assert child == (None, None)
        with engine.begin() as connection:
            connection.execute(text("TRUNCATE TABLE capacity_allocations"))
            assert (
                connection.execute(text("SELECT count(*) FROM capacity_allocations")).scalar_one()
                == 0
            )
            assert (
                connection.execute(
                    text("SELECT count(*) FROM capacity_allocation_epochs")
                ).scalar_one()
                == 1
            )
    finally:
        engine.dispose()


def test_capacity_0005_rejects_truncating_executable_allocation_history(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    engine = create_engine(isolated_capacity_migration_url)
    try:
        command.upgrade(cfg, "capacity_0005")
        with engine.begin() as connection:
            _seed_executable_allocation_history(connection, include_child=True)

        with engine.begin() as connection:
            with pytest.raises(
                sqlalchemy.exc.DBAPIError,
                match="executable allocation epochs are append-only",
            ):
                with connection.begin_nested():
                    connection.execute(text("TRUNCATE TABLE capacity_allocation_epochs CASCADE"))
            with pytest.raises(
                sqlalchemy.exc.DBAPIError,
                match="executable allocations are append-only",
            ):
                with connection.begin_nested():
                    connection.execute(text("TRUNCATE TABLE capacity_allocations"))

            assert (
                connection.execute(
                    text("SELECT count(*) FROM capacity_allocation_epochs")
                ).scalar_one()
                == 1
            )
            assert (
                connection.execute(text("SELECT count(*) FROM capacity_allocations")).scalar_one()
                == 1
            )
    finally:
        engine.dispose()


def test_capacity_0006_requires_input_deadlines_and_small_refill_remainders(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    engine = create_engine(isolated_capacity_migration_url)
    try:
        command.upgrade(cfg, "capacity_0005")
        with engine.connect() as connection:
            inspector = inspect(connection)
            expected_allocation_epoch_columns = {
                column["name"]
                for column in inspector.get_columns(
                    "capacity_allocation_epochs",
                    schema="public",
                )
            }
            expected_allocation_epoch_mode_check = {
                item["name"]: _normalize_sql(str(item["sqltext"]))
                for item in inspector.get_check_constraints(
                    "capacity_allocation_epochs",
                    schema="public",
                )
            }["capacity_allocation_epoch_mode_check"]
        with engine.begin() as connection:
            allocation_epoch = _seed_executable_allocation_history(connection, include_child=True)
        command.upgrade(cfg, "capacity_0006")

        with engine.connect() as connection:
            assert connection.execute(
                text(
                    "SELECT input_valid_until IS NOT NULL "
                    "FROM capacity_allocation_epochs WHERE allocation_epoch = :allocation_epoch"
                ),
                {"allocation_epoch": allocation_epoch},
            ).scalar_one()

        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "INSERT INTO capacity_allocation_epochs "
                        "(writer_epoch, configuration_epoch, input_digest, status, "
                        "failure_reason, complete_payload, executable, execution_epoch, "
                        "execution_manifest_sha256, input_valid_until, sealed, "
                        "allocation_count, committed_at) VALUES "
                        "(1, 1, repeat('a', 64), 'executable', NULL, "
                        "'{\"allocations\":[]}'::jsonb, true, 1, repeat('6', 64), "
                        "NULL, true, 0, now())"
                    )
                )

        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "INSERT INTO capacity_allocation_epochs "
                        "(writer_epoch, configuration_epoch, input_digest, status, "
                        "failure_reason, complete_payload, executable, input_valid_until, "
                        "committed_at) VALUES "
                        "(1, 1, repeat('b', 64), 'shadow', NULL, '{}'::jsonb, false, now(), now())"
                    )
                )

        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "INSERT INTO capacity_executable_launch_rate_buckets "
                        "(id, execution_epoch, configuration_epoch, scope, scope_identity, "
                        "rate_per_minute, capacity_microtokens, available_microtokens, "
                        "refill_remainder, last_refill_at) VALUES "
                        "(:id, 1, 1, 'global', 'fleet', 1, 1000000, 1000000, 60, now())"
                    ),
                    {"id": uuid4()},
                )

        command.downgrade(cfg, "capacity_0005")

        with engine.connect() as connection:
            inspector = inspect(connection)
            allocation_epoch_columns = {
                column["name"]
                for column in inspector.get_columns(
                    "capacity_allocation_epochs",
                    schema="public",
                )
            }
            assert allocation_epoch_columns == expected_allocation_epoch_columns
            assert "input_valid_until" not in allocation_epoch_columns
            checks = {
                item["name"]: _normalize_sql(str(item["sqltext"]))
                for item in inspector.get_check_constraints(
                    "capacity_allocation_epochs",
                    schema="public",
                )
            }
            assert (
                checks["capacity_allocation_epoch_mode_check"]
                == expected_allocation_epoch_mode_check
            )
            assert (
                connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
                == "capacity_0005"
            )

        command.upgrade(cfg, "capacity_0006")

        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT 1 FROM information_schema.columns "
                        "WHERE table_schema = 'public' "
                        "AND table_name = 'capacity_allocation_epochs' "
                        "AND column_name = 'input_valid_until'"
                    )
                ).scalar_one()
                == 1
            )
            assert (
                connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
                == "capacity_0006"
            )
    finally:
        engine.dispose()


def test_capacity_0005_downgrade_serializes_executable_history_preflight(
    isolated_capacity_migration_url: str,
) -> None:
    application_name = f"capacity-allocation-downgrade-{uuid4().hex}"
    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0005")
    observer_engine = create_engine(
        isolated_capacity_migration_url,
        isolation_level="AUTOCOMMIT",
    )
    writer_engine = create_engine(isolated_capacity_migration_url)
    downgrade_engine = create_engine(
        isolated_capacity_migration_url,
        connect_args={"application_name": application_name},
    )
    migration = importlib.import_module(
        "capacity_migrations.versions.capacity_0005_executable_allocation"
    )

    def run_downgrade() -> None:
        with downgrade_engine.begin() as connection:
            migration_context = MigrationContext.configure(connection)
            with Operations.context(migration_context):
                migration.downgrade()

    def wait_until_downgrade_is_blocked(
        connection: sqlalchemy.Connection,
        downgrade: Future[None],
    ) -> None:
        deadline = time.monotonic() + 5
        observed: list[dict[str, object]] = []
        while time.monotonic() < deadline:
            if downgrade.done():
                downgrade.result()
                raise AssertionError(
                    "capacity downgrade exited before waiting for allocation history"
                )
            connection.execute(text("SELECT pg_stat_clear_snapshot()"))
            observed = [
                dict(row)
                for row in connection.execute(
                    text(
                        "SELECT application_name, state, wait_event_type, wait_event, query "
                        "FROM pg_stat_activity WHERE datname = current_database() "
                        "AND application_name = :application_name"
                    ),
                    {"application_name": application_name},
                ).mappings()
            ]
            if any(row["wait_event_type"] == "Lock" for row in observed):
                return
            time.sleep(0.01)
        raise AssertionError(
            f"capacity allocation downgrade did not wait for its table lock: {observed!r}"
        )

    try:
        with (
            ThreadPoolExecutor(max_workers=1) as executor,
            writer_engine.connect() as writer,
            observer_engine.connect() as observer,
        ):
            writer_transaction = writer.begin()
            try:
                _seed_executable_allocation_history(writer)
                downgrade = executor.submit(run_downgrade)
                wait_until_downgrade_is_blocked(observer, downgrade)
                writer_transaction.commit()
            finally:
                if writer_transaction.is_active:
                    writer_transaction.rollback()

            with pytest.raises(
                RuntimeError,
                match="cannot downgrade capacity_0005 with executable allocation history",
            ):
                downgrade.result(timeout=5)

        with writer_engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == ("capacity_0005")
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM capacity_allocation_epochs "
                        "WHERE status = 'executable'"
                    )
                ).scalar_one()
                == 1
            )
    finally:
        observer_engine.dispose()
        writer_engine.dispose()
        downgrade_engine.dispose()


def test_capacity_0005_downgrade_refuses_snapshot_isolation(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0005")
    engine = create_engine(
        isolated_capacity_migration_url,
        isolation_level="SERIALIZABLE",
    )
    migration = importlib.import_module(
        "capacity_migrations.versions.capacity_0005_executable_allocation"
    )
    try:
        with pytest.raises(
            RuntimeError,
            match="capacity_0005 downgrade requires READ COMMITTED",
        ):
            with engine.begin() as connection:
                migration_context = MigrationContext.configure(connection)
                with Operations.context(migration_context):
                    migration.downgrade()

        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == ("capacity_0005")
            assert {
                "execution_epoch",
                "execution_manifest_sha256",
            } <= {
                column["name"]
                for column in inspect(connection).get_columns("capacity_allocation_epochs")
            }
    finally:
        engine.dispose()


def test_capacity_0005_upgrade_and_downgrade_ignore_search_path(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0004")
    engine = create_engine(isolated_capacity_migration_url)
    migration = importlib.import_module(
        "capacity_migrations.versions.capacity_0005_executable_allocation"
    )
    try:
        with engine.begin() as connection:
            connection.execute(text("CREATE SCHEMA capacity_migration_decoy"))
            connection.execute(
                text("SET LOCAL search_path TO capacity_migration_decoy, pg_catalog")
            )
            migration_context = MigrationContext.configure(connection)
            with Operations.context(migration_context):
                migration.upgrade()
            assert {
                "execution_epoch",
                "execution_manifest_sha256",
            } <= {
                column["name"]
                for column in inspect(connection).get_columns(
                    "capacity_allocation_epochs",
                    schema="public",
                )
            }

            with Operations.context(migration_context):
                migration.downgrade()
            assert {
                "execution_epoch",
                "execution_manifest_sha256",
            }.isdisjoint(
                {
                    column["name"]
                    for column in inspect(connection).get_columns(
                        "capacity_allocation_epochs",
                        schema="public",
                    )
                }
            )
    finally:
        engine.dispose()


def test_capacity_0005_allocation_guards_fix_their_search_path(
    capacity_postgres_url: str,
) -> None:
    engine = create_engine(capacity_postgres_url)
    try:
        with engine.connect() as connection:
            functions = dict(
                connection.execute(
                    text(
                        "SELECT proname, array_to_string(proconfig, ',') "
                        "FROM pg_proc JOIN pg_namespace ON pg_namespace.oid = pronamespace "
                        "WHERE nspname = 'public' AND proname IN "
                        "('capacity_executable_allocation_admission_guard', "
                        "'capacity_executable_allocation_seal_guard', "
                        "'capacity_allocation_epoch_binding_guard', "
                        "'capacity_allocation_binding_guard')"
                    )
                ).all()
            )
        assert functions == {
            "capacity_allocation_binding_guard": "search_path=pg_catalog",
            "capacity_allocation_epoch_binding_guard": "search_path=pg_catalog",
            "capacity_executable_allocation_admission_guard": "search_path=pg_catalog",
            "capacity_executable_allocation_seal_guard": "search_path=pg_catalog",
        }
    finally:
        engine.dispose()


def test_capacity_0006_refuses_downgrade_with_executable_queue_history(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    engine = create_engine(isolated_capacity_migration_url)
    try:
        command.upgrade(cfg, "capacity_0006")
        with engine.begin() as connection:
            _seed_executable_allocation_history(connection)
            connection.execute(
                text(
                    "INSERT INTO public.capacity_executable_executor_states "
                    "(id, execution_epoch, execution_manifest_sha256, executor_id, "
                    "executor_incarnation, pool_id, pool_generation, state, "
                    "lease_expires_at, last_heartbeat_at) "
                    "SELECT :id, execution_epoch, execution_manifest_sha256, executor_id, "
                    "executor_incarnation, pool_id, pool_generation, 'current', now(), now() "
                    "FROM public.capacity_execution_executors WHERE pool_id = 'gb10'"
                ),
                {"id": uuid4()},
            )

        with pytest.raises(
            RuntimeError,
            match="cannot downgrade capacity_0006 with executable queue history",
        ):
            command.downgrade(cfg, "capacity_0005")

        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == ("capacity_0006")
            assert (
                connection.execute(
                    text("SELECT count(*) FROM public.capacity_executable_executor_states")
                ).scalar_one()
                == 1
            )
    finally:
        engine.dispose()


def test_capacity_0006_upgrade_and_downgrade_ignore_search_path(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0005")
    engine = create_engine(isolated_capacity_migration_url)
    migration = importlib.import_module(
        "capacity_migrations.versions.capacity_0006_executable_work_queue"
    )
    try:
        with engine.begin() as connection:
            connection.execute(text("CREATE SCHEMA capacity_queue_decoy"))
            connection.execute(text("SET LOCAL search_path TO capacity_queue_decoy, pg_catalog"))
            migration_context = MigrationContext.configure(connection)
            with Operations.context(migration_context):
                migration.upgrade()
            assert {
                "capacity_executable_command_receipts",
                "capacity_executable_executor_states",
                "capacity_executable_intents",
                "capacity_executable_launch_rate_buckets",
            } <= set(inspect(connection).get_table_names(schema="public"))
            assert not inspect(connection).get_table_names(schema="capacity_queue_decoy")

            with Operations.context(migration_context):
                migration.downgrade()
            assert "capacity_executable_executor_states" not in inspect(connection).get_table_names(
                schema="public"
            )
    finally:
        engine.dispose()


def test_capacity_0007_refuses_downgrade_with_protected_bootstrap_evidence(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    engine = create_engine(isolated_capacity_migration_url)
    try:
        command.upgrade(cfg, "capacity_0007")
        with engine.begin() as connection:
            connection.execute(
                text(
                    "ALTER TABLE public.capacity_executable_bootstrap_proposals DISABLE TRIGGER ALL"
                )
            )
            connection.execute(
                text(
                    "INSERT INTO public.capacity_executable_bootstrap_proposals "
                    "(id, intent_id, execution_epoch, execution_manifest_sha256, "
                    "proposal_epoch, command_sequence, bootstrap_sha256, expires_at, "
                    "proposal_digest, proposal_payload) VALUES "
                    "(:id, :intent_id, 1, repeat('1', 64), 1, 1, repeat('2', 64), "
                    "now() + interval '1 minute', repeat('3', 64), '{}'::jsonb)"
                ),
                {"id": uuid4(), "intent_id": uuid4()},
            )
            connection.execute(
                text(
                    "ALTER TABLE public.capacity_executable_bootstrap_proposals ENABLE TRIGGER ALL"
                )
            )

        with pytest.raises(
            RuntimeError,
            match="cannot downgrade capacity_0007 while protected bootstrap evidence exists",
        ):
            command.downgrade(cfg, "capacity_0006")

        with engine.connect() as connection:
            assert (
                connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
                == "capacity_0007"
            )
            assert (
                connection.execute(
                    text("SELECT count(*) FROM public.capacity_executable_bootstrap_proposals")
                ).scalar_one()
                == 1
            )
    finally:
        engine.dispose()


def test_capacity_0007_upgrade_and_downgrade_ignore_search_path(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0006")
    engine = create_engine(isolated_capacity_migration_url)
    migration = importlib.import_module(
        "capacity_migrations.versions.capacity_0007_protected_bootstrap_handshake"
    )
    try:
        with engine.begin() as connection:
            connection.execute(text("CREATE SCHEMA capacity_bootstrap_decoy"))
            connection.execute(
                text("SET LOCAL search_path TO capacity_bootstrap_decoy, pg_catalog")
            )
            migration_context = MigrationContext.configure(connection)
            with Operations.context(migration_context):
                migration.upgrade()
            assert {
                "capacity_executable_bootstrap_acknowledgements",
                "capacity_executable_bootstrap_proposals",
            } <= set(inspect(connection).get_table_names(schema="public"))
            assert not inspect(connection).get_table_names(schema="capacity_bootstrap_decoy")
            functions = dict(
                connection.execute(
                    text(
                        "SELECT proname, array_to_string(proconfig, ',') "
                        "FROM pg_proc JOIN pg_namespace "
                        "ON pg_namespace.oid = pronamespace "
                        "WHERE nspname = 'public' AND proname IN "
                        "('capacity_executable_bootstrap_proposal_insert_guard', "
                        "'capacity_executable_bootstrap_ack_insert_guard', "
                        "'capacity_executable_intent_protected_bootstrap_guard')"
                    )
                ).all()
            )
            assert functions == {
                "capacity_executable_bootstrap_ack_insert_guard": ("search_path=pg_catalog"),
                "capacity_executable_bootstrap_proposal_insert_guard": ("search_path=pg_catalog"),
                "capacity_executable_intent_protected_bootstrap_guard": ("search_path=pg_catalog"),
            }

            with Operations.context(migration_context):
                migration.downgrade()
            assert "capacity_executable_bootstrap_proposals" not in inspect(
                connection
            ).get_table_names(schema="public")
    finally:
        engine.dispose()


def _insert_unchecked_capacity_0014_admission_proposal(
    connection: sqlalchemy.Connection,
    *,
    proposal_payload: dict[str, object] | None = None,
) -> None:
    connection.execute(
        text(
            "ALTER TABLE public.capacity_executable_admission_proposals "
            "DISABLE TRIGGER ALL"
        )
    )
    connection.execute(
        text(
            "INSERT INTO public.capacity_executable_admission_proposals "
            "(id, proposal_id, plan_id, admission_incarnation, tranche_id, "
            "execution_epoch, execution_manifest_sha256, allocation_epoch, "
            "subject_id, subject_incarnation, pool_id, reporter_incarnation, "
            "protected_admission_sha256, manager_input_digest, "
            "manager_allocation_digest, proposal_digest, proposal_payload, expires_at) "
            "VALUES (:id, :proposal_id, :plan_id, :admission_incarnation, :tranche_id, "
            "1, repeat('1', 64), 1, :subject_id, :subject_incarnation, 'oldlab', "
            ":reporter_incarnation, repeat('2', 64), repeat('3', 64), "
            "repeat('4', 64), repeat('5', 64), CAST(:proposal_payload AS jsonb), "
            "now() + interval '1 minute')"
        ),
        {
            "id": uuid4(),
            "proposal_id": uuid4(),
            "plan_id": uuid4(),
            "admission_incarnation": uuid4(),
            "tranche_id": uuid4(),
            "subject_id": uuid4(),
            "subject_incarnation": uuid4(),
            "reporter_incarnation": uuid4(),
            "proposal_payload": json.dumps(proposal_payload or {}),
        },
    )
    connection.execute(
        text(
            "ALTER TABLE public.capacity_executable_admission_proposals "
            "ENABLE TRIGGER ALL"
        )
    )


def _insert_unchecked_capacity_0014_admission_closure_acknowledgement(
    connection: sqlalchemy.Connection,
) -> None:
    proposal = connection.execute(
        text(
            "SELECT proposal_id, proposal_digest, plan_id, admission_incarnation, "
            "subject_id, subject_incarnation, reporter_incarnation, "
            "protected_admission_sha256 "
            "FROM public.capacity_executable_admission_proposals"
        )
    ).mappings().one()
    connection.execute(
        text(
            "ALTER TABLE public.capacity_executable_admission_closure_acknowledgements "
            "DISABLE TRIGGER ALL"
        )
    )
    connection.execute(
        text(
            "INSERT INTO public.capacity_executable_admission_closure_acknowledgements "
            "(id, idempotency_key, closure_id, proposal_id, proposal_digest, "
            "plan_id, admission_incarnation, subject_id, subject_incarnation, "
            "reporter_incarnation, protected_admission_sha256, close_reason, "
            "disposition_kind, disposition_digest, acknowledgement_digest, "
            "actor_id, acknowledgement_payload) VALUES "
            "(:id, :idempotency_key, :closure_id, :proposal_id, :proposal_digest, "
            ":plan_id, :admission_incarnation, :subject_id, :subject_incarnation, "
            ":reporter_incarnation, :protected_admission_sha256, 'manager-closed', "
            "'never-converged', repeat('6', 64), repeat('7', 64), "
            "'migration-test', '{}'::jsonb)"
        ),
        {
            "id": uuid4(),
            "idempotency_key": uuid4(),
            "closure_id": uuid4(),
            **proposal,
        },
    )
    connection.execute(
        text(
            "ALTER TABLE public.capacity_executable_admission_closure_acknowledgements "
            "ENABLE TRIGGER ALL"
        )
    )


def _insert_unchecked_capacity_0014_bootstrap_without_launch_intent(
    connection: sqlalchemy.Connection,
    *,
    state: str,
) -> None:
    allocation_epoch = _seed_executable_allocation_history(connection)
    executor_incarnation = connection.execute(
        text(
            "SELECT executor_incarnation FROM public.capacity_execution_executors "
            "WHERE pool_id = 'gb10'"
        )
    ).scalar_one()
    intent_id = uuid4()
    tranche_id = uuid4()
    subject_id = uuid4()
    subject_incarnation = uuid4()
    shape_instance_id = "shape-capacity-0014-only"
    binding_payload = {
        "schema_version": 2,
        "intent_id": str(intent_id),
        "tranche_id": str(tranche_id),
        "shape_instance_id": shape_instance_id,
        "subject_id": str(subject_id),
        "subject_incarnation": str(subject_incarnation),
        "pool_id": "gb10",
        "pool_generation": 1,
        "executor_id": "gb10-executor",
        "executor_incarnation": str(executor_incarnation),
        "resources": {"cpu": 1},
        "node_ids": [],
        "execution": {
            "configuration_epoch": 1,
            "allocation_epoch": allocation_epoch,
            "execution_epoch": 1,
            "execution_manifest_sha256": "6" * 64,
        },
    }
    has_terminal_evidence = state in {"closing", "released"}
    connection.execute(
        text(
            "INSERT INTO public.capacity_executable_tranches "
            "(tranche_id, execution_epoch, execution_manifest_sha256, "
            "configuration_epoch, allocation_epoch, executor_id, "
            "executor_incarnation, pool_id, pool_generation, subject_id, "
            "subject_incarnation, proposal_digest, proposal_payload) VALUES "
            "(:tranche_id, 1, repeat('6', 64), 1, :allocation_epoch, "
            "'gb10-executor', :executor_incarnation, 'gb10', 1, "
            ":subject_id, :subject_incarnation, repeat('1', 64), '{}'::jsonb)"
        ),
        {
            "tranche_id": tranche_id,
            "allocation_epoch": allocation_epoch,
            "executor_incarnation": executor_incarnation,
            "subject_id": subject_id,
            "subject_incarnation": subject_incarnation,
        },
    )
    connection.execute(text("ALTER TABLE public.capacity_executable_intents DISABLE TRIGGER ALL"))
    try:
        connection.execute(
            text(
                "INSERT INTO public.capacity_executable_intents "
                "(id, intent_id, tranche_id, shape_instance_id, execution_epoch, "
                "execution_manifest_sha256, configuration_epoch, allocation_epoch, "
                "executor_id, executor_incarnation, pool_id, pool_generation, "
                "subject_id, subject_incarnation, launch_rank, proposal_digest, "
                "proposal_payload, binding_digest, binding_payload, state, accepted_at, "
                "bootstrap_registration_epoch, bootstrap_evidence_sha256, "
                "launch_ready_at, inventory_sequence, observed_state, terminal_kind, "
                "terminal_identity, terminal_evidence_sha256, released_at) VALUES "
                "(:id, :intent_id, :tranche_id, :shape_instance_id, 1, "
                "repeat('6', 64), 1, :allocation_epoch, 'gb10-executor', "
                ":executor_incarnation, 'gb10', 1, :subject_id, :subject_incarnation, "
                "1, repeat('1', 64), '{}'::jsonb, repeat('2', 64), "
                "CAST(:binding_payload AS jsonb), :state, now(), 1, repeat('8', 64), "
                "NULL, :inventory_sequence, NULL, :terminal_kind, :terminal_identity, "
                ":terminal_evidence_sha256, "
                "CASE WHEN :state = 'released' THEN now() ELSE NULL END)"
            ),
            {
                "id": uuid4(),
                "intent_id": intent_id,
                "tranche_id": tranche_id,
                "shape_instance_id": shape_instance_id,
                "allocation_epoch": allocation_epoch,
                "executor_incarnation": executor_incarnation,
                "subject_id": subject_id,
                "subject_incarnation": subject_incarnation,
                "binding_payload": json.dumps(binding_payload),
                "state": state,
                "inventory_sequence": 1 if has_terminal_evidence else None,
                "terminal_kind": "unused" if has_terminal_evidence else None,
                "terminal_identity": shape_instance_id if has_terminal_evidence else None,
                "terminal_evidence_sha256": "9" * 64 if has_terminal_evidence else None,
            },
        )
    finally:
        connection.execute(text("ALTER TABLE public.capacity_executable_intents ENABLE TRIGGER ALL"))


def _seed_capacity_0007_intent(
    connection: sqlalchemy.Connection,
    *,
    state: str,
    repeated_scope: bool = False,
) -> None:
    subject_id = uuid4()
    subject_incarnation = uuid4()
    ranks = [
        {
            "rank": 1,
            "subject_id": str(subject_id),
            "pool_id": "gb10",
            "shape_instance_id": "shape-legacy-00000001",
        }
    ]
    if repeated_scope:
        ranks.append(
            {
                "rank": 2,
                "subject_id": str(subject_id),
                "pool_id": "gb10",
                "shape_instance_id": "shape-legacy-00000002",
            }
        )
    allocation_epoch = _seed_executable_allocation_history(
        connection,
        complete_payload={
            "allocations": [],
            "hypothetical_launch_rank": ranks,
        },
    )
    executor_incarnation = connection.execute(
        text(
            "SELECT executor_incarnation FROM public.capacity_execution_executors "
            "WHERE pool_id = 'gb10'"
        )
    ).scalar_one()
    permit_id = uuid4() if state == "permitted" else None
    connection.execute(
        text("ALTER TABLE public.capacity_executable_intents DISABLE TRIGGER ALL")
    )
    try:
        connection.execute(
            text(
                "INSERT INTO public.capacity_executable_intents "
                "(id, intent_id, tranche_id, shape_instance_id, execution_epoch, "
                "execution_manifest_sha256, configuration_epoch, allocation_epoch, "
                "executor_id, executor_incarnation, pool_id, pool_generation, "
                "subject_id, subject_incarnation, launch_rank, proposal_digest, "
                "proposal_payload, binding_digest, binding_payload, state, accepted_at, "
                "bootstrap_registration_epoch, bootstrap_evidence_sha256, "
                "launch_ready_at, permit_id, permit_epoch, permit_digest, permit_payload, "
                "permit_expires_at, inventory_sequence, observed_state) VALUES "
                "(:id, :intent_id, :tranche_id, 'shape-legacy-00000001', 1, "
                "repeat('6', 64), 1, :allocation_epoch, 'gb10-executor', "
                ":executor_incarnation, 'gb10', 1, :subject_id, :subject_incarnation, "
                "1, repeat('1', 64), '{}'::jsonb, repeat('2', 64), '{}'::jsonb, "
                ":state, now(), 1, repeat('3', 64), now(), CAST(:permit_id AS uuid), "
                "CASE WHEN CAST(:permit_id AS uuid) IS NULL THEN NULL ELSE 1 END, "
                "CASE WHEN CAST(:permit_id AS uuid) IS NULL "
                "THEN NULL ELSE repeat('4', 64) END, "
                "CASE WHEN CAST(:permit_id AS uuid) IS NULL "
                "THEN NULL ELSE '{}'::jsonb END, "
                "CASE WHEN CAST(:permit_id AS uuid) IS NULL "
                "THEN NULL ELSE now() + interval '1 minute' END, "
                "CASE WHEN :state = 'observed' THEN 1 ELSE NULL END, "
                "CASE WHEN :state = 'observed' THEN 'active' ELSE NULL END)"
            ),
            {
                "id": uuid4(),
                "intent_id": uuid4(),
                "tranche_id": uuid4(),
                "allocation_epoch": allocation_epoch,
                "executor_incarnation": executor_incarnation,
                "subject_id": subject_id,
                "subject_incarnation": subject_incarnation,
                "state": state,
                "permit_id": permit_id,
            },
        )
    finally:
        connection.execute(
            text("ALTER TABLE public.capacity_executable_intents ENABLE TRIGGER ALL")
        )


@pytest.mark.parametrize("state", ("launch-ready", "permitted"))
def test_capacity_0014_refuses_bootstrap_only_launchable_legacy_intents(
    isolated_capacity_migration_url: str,
    state: str,
) -> None:
    """Pre-admission launch authority must not survive the admission-gate upgrade."""

    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0007")
    engine = create_engine(isolated_capacity_migration_url)
    try:
        with engine.begin() as connection:
            _seed_capacity_0007_intent(connection, state=state)

        with pytest.raises(
            RuntimeError,
            match="cannot upgrade capacity_0014 with bootstrap-only launch authority",
        ):
            command.upgrade(cfg, "capacity_0014")

        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == "capacity_0007"
            assert connection.execute(
                text("SELECT state FROM public.capacity_executable_intents")
            ).scalar_one() == state
    finally:
        engine.dispose()


def test_capacity_0014_refuses_partial_legacy_multi_shape_scope(
    isolated_capacity_migration_url: str,
) -> None:
    """A legacy one-intent tranche cannot represent a partially-created batched scope."""

    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0007")
    engine = create_engine(isolated_capacity_migration_url)
    try:
        with engine.begin() as connection:
            _seed_capacity_0007_intent(
                connection,
                state="observed",
                repeated_scope=True,
            )

        with pytest.raises(
            RuntimeError,
            match="cannot upgrade capacity_0014 with unbatchable legacy intent scope",
        ):
            command.upgrade(cfg, "capacity_0014")

        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == "capacity_0007"
    finally:
        engine.dispose()


def test_capacity_0014_adds_batched_tranche_and_protected_admission_schema(
    isolated_capacity_migration_url: str,
) -> None:
    """Keeping one-intent tranches or mutable admission evidence must fail migration review."""

    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0013")
    engine = create_engine(isolated_capacity_migration_url)
    migration = importlib.import_module(
        "capacity_migrations.versions.capacity_0014_protected_admission_plan"
    )
    try:
        with engine.begin() as connection:
            connection.execute(text("CREATE SCHEMA capacity_admission_decoy"))
            connection.execute(
                text("SET LOCAL search_path TO capacity_admission_decoy, pg_catalog")
            )
            migration_context = MigrationContext.configure(connection)
            with Operations.context(migration_context):
                migration.upgrade()

            public_tables = set(inspect(connection).get_table_names(schema="public"))
            assert {
                "capacity_executable_admission_acknowledgements",
                "capacity_executable_admission_closure_acknowledgements",
                "capacity_executable_admission_proposals",
                "capacity_executable_tranches",
            } <= public_tables
            assert not inspect(connection).get_table_names(schema="capacity_admission_decoy")
            closure_ack_columns = {
                item["name"]
                for item in inspect(connection).get_columns(
                    "capacity_executable_admission_closure_acknowledgements",
                    schema="public",
                )
            }
            assert {"disposition_kind", "disposition_digest"} <= closure_ack_columns
            assert "abandonment_digest" not in closure_ack_columns

            intent_uniques = {
                item["name"]
                for item in inspect(connection).get_unique_constraints(
                    "capacity_executable_intents",
                    schema="public",
                )
            }
            assert "capacity_executable_tranche_identity_key" not in intent_uniques
            assert "capacity_executable_tranche_intent_key" in intent_uniques
            state_check = next(
                item["sqltext"]
                for item in inspect(connection).get_check_constraints(
                    "capacity_executable_intents",
                    schema="public",
                )
                if item["name"] == "capacity_executable_intent_state_check"
            )
            assert "bootstrap-acknowledged" in state_check
            execution_quantity_check = next(
                item["sqltext"]
                for item in inspect(connection).get_check_constraints(
                    "capacity_execution_epochs",
                    schema="public",
                )
                if item["name"] == "capacity_execution_epoch_quantity_check"
            )
            assert "requested_ceiling > 0" in execution_quantity_check
            assert "requested_ceiling = 1" not in execution_quantity_check

            functions = dict(
                connection.execute(
                    text(
                        "SELECT proname, array_to_string(proconfig, ',') "
                        "FROM pg_proc JOIN pg_namespace "
                        "ON pg_namespace.oid = pronamespace "
                        "WHERE nspname = 'public' AND proname IN "
                        "('capacity_executable_admission_proposal_insert_guard', "
                        "'capacity_executable_admission_ack_insert_guard', "
                        "'capacity_executable_admission_closure_ack_insert_guard', "
                        "'capacity_executable_intent_protected_bootstrap_guard')"
                    )
                ).all()
            )
            assert functions == {
                "capacity_executable_admission_ack_insert_guard": "search_path=pg_catalog",
                "capacity_executable_admission_closure_ack_insert_guard": (
                    "search_path=pg_catalog"
                ),
                "capacity_executable_admission_proposal_insert_guard": (
                    "search_path=pg_catalog"
                ),
                "capacity_executable_intent_protected_bootstrap_guard": (
                    "search_path=pg_catalog"
                ),
            }

            with Operations.context(migration_context):
                migration.downgrade()
            assert {
                "capacity_executable_admission_acknowledgements",
                "capacity_executable_admission_closure_acknowledgements",
                "capacity_executable_admission_proposals",
                "capacity_executable_tranches",
            }.isdisjoint(inspect(connection).get_table_names(schema="public"))
            restored_uniques = {
                item["name"]
                for item in inspect(connection).get_unique_constraints(
                    "capacity_executable_intents",
                    schema="public",
                )
            }
            assert "capacity_executable_tranche_identity_key" in restored_uniques
    finally:
        engine.dispose()


def test_capacity_0014_fresh_upgrade_makes_all_new_functions_owner_only(
    isolated_capacity_migration_url: str,
) -> None:
    """Every internal capacity_0014 helper must reject implicit PUBLIC execution."""

    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0014")
    engine = create_engine(isolated_capacity_migration_url)
    try:
        with engine.connect() as connection:
            _assert_function_execute_acls_are_owner_only(
                connection,
                signatures=CAPACITY_0014_FUNCTION_SIGNATURES,
            )
    finally:
        engine.dispose()


def test_capacity_0014_downgrade_and_reupgrade_preserve_owner_only_function_acls(
    isolated_capacity_migration_url: str,
) -> None:
    """Recreated legacy and head helpers must not reacquire PUBLIC execution."""

    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0014")
    command.downgrade(cfg, "capacity_0013")
    engine = create_engine(isolated_capacity_migration_url)
    try:
        with engine.connect() as connection:
            _assert_function_execute_acls_are_owner_only(
                connection,
                signatures=(CAPACITY_0012_INTENT_GUARD_SIGNATURE,),
            )
        command.upgrade(cfg, "capacity_0014")
        with engine.connect() as connection:
            _assert_function_execute_acls_are_owner_only(
                connection,
                signatures=CAPACITY_0014_FUNCTION_SIGNATURES,
            )
    finally:
        engine.dispose()


def test_capacity_0014_admission_evidence_is_append_only(
    isolated_capacity_migration_url: str,
) -> None:
    """Allowing a committed protected plan proposal to change must fail."""

    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0014")
    engine = create_engine(isolated_capacity_migration_url)
    try:
        with engine.begin() as connection:
            _insert_unchecked_capacity_0014_admission_proposal(connection)
        for statement in (
            "UPDATE public.capacity_executable_admission_proposals "
            "SET manager_input_digest = repeat('9', 64)",
            "DELETE FROM public.capacity_executable_admission_proposals",
            "TRUNCATE public.capacity_executable_admission_closure_acknowledgements, "
            "public.capacity_executable_admission_acknowledgements, "
            "public.capacity_executable_admission_proposals",
        ):
            with pytest.raises(DBAPIError, match="append-only"):
                with engine.begin() as connection:
                    connection.execute(text(statement))
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "statement",
    (
        "UPDATE public.capacity_executable_admission_closure_acknowledgements "
        "SET disposition_digest = disposition_digest",
        "DELETE FROM public.capacity_executable_admission_closure_acknowledgements",
        "TRUNCATE public.capacity_executable_admission_closure_acknowledgements",
    ),
)
def test_capacity_0014_admission_closure_acknowledgements_are_append_only(
    isolated_capacity_migration_url: str,
    statement: str,
) -> None:
    """Every direct mutation path must preserve a committed closure receipt."""

    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0014")
    engine = create_engine(isolated_capacity_migration_url)
    try:
        with engine.begin() as connection:
            _insert_unchecked_capacity_0014_admission_proposal(connection)
            _insert_unchecked_capacity_0014_admission_closure_acknowledgement(
                connection
            )
        with pytest.raises(DBAPIError, match="append-only"):
            with engine.begin() as connection:
                connection.execute(text(statement))
        with engine.connect() as connection:
            assert connection.execute(
                text(
                    "SELECT count(*) FROM "
                    "public.capacity_executable_admission_closure_acknowledgements"
                )
            ).scalar_one() == 1
    finally:
        engine.dispose()


def test_capacity_0014_persists_the_exact_shared_admission_response_boundary(
    isolated_capacity_migration_url: str,
) -> None:
    """Persistence must accept the derived exact limit and reject one byte more."""

    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0014")
    engine = create_engine(isolated_capacity_migration_url)
    empty_payload_bytes = len(b'{"padding":""}')
    exact = {
        "padding": "x"
        * (MAX_EXECUTABLE_ADMISSION_PROPOSAL_BYTES - empty_payload_bytes)
    }
    oversized = {"padding": exact["padding"] + "x"}
    assert len(
        json.dumps(exact, sort_keys=True, separators=(",", ":")).encode("ascii")
    ) == MAX_EXECUTABLE_ADMISSION_PROPOSAL_BYTES
    try:
        with engine.begin() as connection:
            _insert_unchecked_capacity_0014_admission_proposal(
                connection,
                proposal_payload=exact,
            )
        with pytest.raises(
            DBAPIError,
            match="capacity_executable_admission_proposal_payload_check",
        ):
            with engine.begin() as connection:
                _insert_unchecked_capacity_0014_admission_proposal(
                    connection,
                    proposal_payload=oversized,
                )
    finally:
        engine.dispose()


def test_capacity_0014_refuses_downgrade_with_protected_admission_evidence(
    isolated_capacity_migration_url: str,
) -> None:
    """Dropping durable local-plan acknowledgement history must fail closed."""

    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0014")
    engine = create_engine(isolated_capacity_migration_url)
    try:
        with engine.begin() as connection:
            _insert_unchecked_capacity_0014_admission_proposal(connection)

        with pytest.raises(
            RuntimeError,
            match="cannot downgrade capacity_0014 while protected admission evidence exists",
        ):
            command.downgrade(cfg, "capacity_0013")

        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == "capacity_0014"
            assert connection.execute(
                text("SELECT count(*) FROM capacity_executable_admission_proposals")
            ).scalar_one() == 1
    finally:
        engine.dispose()


@pytest.mark.parametrize("state", ("closing", "released", "quarantined"))
def test_capacity_0014_refuses_downgrade_with_bootstrap_without_launch_tuple(
    isolated_capacity_migration_url: str,
    state: str,
) -> None:
    """capacity_0013 cannot safely continue retained bootstrap evidence without launch."""

    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0014")
    engine = create_engine(isolated_capacity_migration_url)
    try:
        with engine.begin() as connection:
            _insert_unchecked_capacity_0014_bootstrap_without_launch_intent(
                connection,
                state=state,
            )

        with pytest.raises(
            RuntimeError,
            match="cannot downgrade capacity_0014 with capacity_0014-only executable history",
        ):
            command.downgrade(cfg, "capacity_0013")

        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == "capacity_0014"
            assert connection.execute(
                text(
                    "SELECT count(*) FROM public.capacity_executable_intents "
                    "WHERE state = :state "
                    "AND bootstrap_registration_epoch IS NOT NULL "
                    "AND bootstrap_evidence_sha256 IS NOT NULL "
                    "AND launch_ready_at IS NULL"
                ),
                {"state": state},
            ).scalar_one() == 1
    finally:
        engine.dispose()


def test_capacity_schema_has_independent_revision_table(
    capacity_postgres_url: str,
    postgres_url: str,
) -> None:
    capacity_engine = create_engine(capacity_postgres_url)
    environment_engine = create_engine(postgres_url)
    try:
        with capacity_engine.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == ("capacity_0014")
        with environment_engine.connect() as connection:
            environment_revision = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            assert environment_revision != "capacity_0014"
            assert not (EXPECTED_TABLES & set(inspect(connection).get_table_names()))
    finally:
        capacity_engine.dispose()
        environment_engine.dispose()


async def test_capacity_schema_error_uses_installed_capacity_migration_command(
    empty_capacity_engine: AsyncEngine,
) -> None:
    with pytest.raises(CapacitySchemaNotAtHeadError) as caught:
        await assert_capacity_schema_at_head(empty_capacity_engine)
    message = str(caught.value)
    assert "python -m loom_capacity_manager.migrate" in message
    assert "--db-url-file <owner-only-database-url-file>" in message
    assert "--expected-authority-incarnation <reviewed-non-nil-uuid>" in message
    assert "capacity_migrations/alembic.ini" not in message


async def test_capacity_schema_startup_returns_numeric_head(
    capacity_engine: AsyncEngine,
) -> None:
    assert await assert_capacity_schema_at_head(capacity_engine) == 14


def test_package3_tables_are_database_constrained_to_dry_run(
    capacity_postgres_url: str,
) -> None:
    engine = create_engine(capacity_postgres_url)
    try:
        with engine.connect() as connection:
            inspector = inspect(connection)
            for table in (
                "capacity_reservation_tranches",
                "capacity_submission_intents",
                "capacity_launch_permits",
                "capacity_protected_release_acknowledgements",
            ):
                checks = " ".join(
                    str(item["sqltext"]) for item in inspector.get_check_constraints(table)
                ).lower()
                assert "executable = false" in checks
            executor_checks = " ".join(
                str(item["sqltext"])
                for item in inspector.get_check_constraints("capacity_executors")
            ).lower()
            assert "dry-run" in executor_checks
            executor_unique_columns = {
                tuple(item["column_names"])
                for item in inspector.get_unique_constraints("capacity_executors")
            }
            assert ("executor_incarnation",) in executor_unique_columns
            bucket_checks = " ".join(
                str(item["sqltext"])
                for item in inspector.get_check_constraints("capacity_launch_rate_buckets")
            ).lower()
            assert "dry-run" in bucket_checks
            assert "9223372036854" in bucket_checks
            foreign_keys = {
                table: {
                    tuple(item["constrained_columns"]) for item in inspector.get_foreign_keys(table)
                }
                for table in (
                    "capacity_allocation_epochs",
                    "capacity_executor_observations",
                    "capacity_launch_permits",
                    "capacity_launch_rate_buckets",
                    "capacity_protected_release_acknowledgements",
                    "capacity_reservation_release_evidence",
                    "capacity_reservation_tranches",
                    "capacity_submission_intents",
                )
            }
            assert ("configuration_epoch",) in foreign_keys["capacity_allocation_epochs"]
            assert (
                "executor_row_id",
                "executor_incarnation",
                "pool_id",
                "pool_generation",
            ) in foreign_keys["capacity_executor_observations"]
            assert {("allocation_epoch",), ("configuration_epoch",)} <= foreign_keys[
                "capacity_launch_permits"
            ]
            assert (
                "intent_id",
                "executor_id",
                "executor_incarnation",
            ) in foreign_keys["capacity_launch_permits"]
            assert ("configuration_epoch",) in foreign_keys["capacity_launch_rate_buckets"]
            assert (
                "tranche_id",
                "shape_instance_id",
                "intent_id",
            ) in foreign_keys["capacity_protected_release_acknowledgements"]
            assert (
                "subject_id",
                "subject_incarnation",
                "reporter_incarnation",
            ) in foreign_keys["capacity_protected_release_acknowledgements"]
            assert (
                "tranche_id",
                "shape_instance_id",
                "intent_id",
            ) in foreign_keys["capacity_reservation_release_evidence"]
            assert (
                "intent_id",
                "executor_id",
                "executor_incarnation",
            ) in foreign_keys["capacity_reservation_release_evidence"]
            assert {("allocation_epoch",), ("configuration_epoch",)} <= foreign_keys[
                "capacity_reservation_tranches"
            ]
            assert (
                "executor_id",
                "executor_incarnation",
                "pool_id",
                "pool_generation",
            ) in foreign_keys["capacity_reservation_tranches"]
            assert (
                "tranche_id",
                "shape_instance_id",
                "id",
            ) in foreign_keys["capacity_submission_intents"]
            append_only_triggers = set(
                connection.execute(
                    text(
                        "SELECT tgname FROM pg_trigger WHERE tgname IN "
                        "('capacity_release_evidence_append_only', "
                        "'capacity_release_evidence_append_only_truncate', "
                        "'capacity_protected_release_append_only', "
                        "'capacity_protected_release_append_only_truncate') "
                        "AND tgenabled <> 'D'"
                    )
                ).scalars()
            )
            assert append_only_triggers == {
                "capacity_release_evidence_append_only",
                "capacity_release_evidence_append_only_truncate",
                "capacity_protected_release_append_only",
                "capacity_protected_release_append_only_truncate",
            }
    finally:
        engine.dispose()


def test_capacity_migration_downgrades_and_reupgrades(
    capacity_postgres_url: str,
) -> None:
    cfg = _capacity_config(capacity_postgres_url)
    engine = create_engine(capacity_postgres_url)
    try:
        command.downgrade(cfg, "base")
        with engine.connect() as connection:
            assert not (EXPECTED_TABLES & set(inspect(connection).get_table_names()))
        command.upgrade(cfg, "head")
        with engine.connect() as connection:
            assert EXPECTED_TABLES <= set(inspect(connection).get_table_names())
            authority = connection.execute(
                select(CapacityAuthorityState.authority_incarnation).where(
                    CapacityAuthorityState.singleton_id == 1
                )
            ).scalar_one()
            seeds = (
                connection.execute(
                    select(CapacityAuditEvent).where(
                        CapacityAuditEvent.event_kind == "authority_incarnation_seeded"
                    )
                )
                .mappings()
                .all()
            )
            assert len(seeds) == 1
            assert seeds[0]["actor_kind"] == "migration"
            assert seeds[0]["actor_id"] == "capacity-authority-bootstrap"
            assert seeds[0]["object_binding"] == {"authority_incarnation": str(authority)}
            assert seeds[0]["detail"] == {"state": "migration-generated-seed"}
    finally:
        engine.dispose()


def test_capacity_models_match_migration_head(capacity_postgres_url: str) -> None:
    command.check(_capacity_config(capacity_postgres_url))


def test_capacity_alembic_environment_has_no_environment_db_fallback() -> None:
    source = Path("capacity_migrations/env.py").read_text(encoding="utf-8")
    assert "LOOM_CAPACITY_DB_URL" in source
    assert "LOOM_DB_URL" not in source
    assert "LOOM_CP_DB_URL" not in source


def test_capacity_alembic_connection_enforces_fixed_postgres_timeouts(
    capacity_postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).resolve().parents[2]
    encoded_url = (
        f"{capacity_postgres_url}?application_name=capacity%40migration&connect_timeout=99"
    )
    cfg = AlembicConfig(str(root / "capacity_migrations" / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "capacity_migrations"))
    monkeypatch.setenv("LOOM_CAPACITY_DB_URL", encoded_url)
    real_engine_from_config = sqlalchemy.engine_from_config
    captured: dict[str, object] = {}

    def capture(configuration: dict[str, object], **kwargs: object) -> sqlalchemy.Engine:
        captured["url"] = configuration["sqlalchemy.url"]
        captured["connect_args"] = kwargs.get("connect_args")
        return real_engine_from_config(configuration, **kwargs)

    monkeypatch.setattr(sqlalchemy, "engine_from_config", capture)

    command.upgrade(cfg, "head")

    assert captured == {
        "url": encoded_url,
        "connect_args": {
            "connect_timeout": 10,
            "options": "-c lock_timeout=30000 -c statement_timeout=300000",
        },
    }
