"""Independent management-schema migration and safety constraints."""

from __future__ import annotations

import importlib
import json
import os
import time
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy
from alembic import command
from alembic.config import Config as AlembicConfig
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

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
    "capacity_executable_command_receipts",
    "capacity_executable_executor_states",
    "capacity_executable_intents",
    "capacity_executable_launch_rate_buckets",
    "capacity_executable_protected_release_receipts",
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


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.split())


def _capacity_config(url: str) -> AlembicConfig:
    root = Path(__file__).resolve().parents[2]
    cfg = AlembicConfig(str(root / "capacity_migrations" / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "capacity_migrations"))
    os.environ["LOOM_CAPACITY_DB_URL"] = url
    return cfg


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
        text(
            "SELECT authority_incarnation FROM capacity_authority_state "
            "WHERE singleton_id = 1"
        )
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
            text(
                "UPDATE capacity_authority_state SET writer_epoch = 1 "
                "WHERE singleton_id = 1"
            )
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
    resolved_allocation_count = (
        int(include_child) if allocation_count is None else allocation_count
    )
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
            assert connection.execute(
                text(
                    "SELECT count(*) FROM capacity_allocation_epochs "
                    "WHERE status = 'executable'"
                )
            ).scalar_one() == 1
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
                    text(
                        "SET CONSTRAINTS "
                        "capacity_executable_allocation_seal_guard IMMEDIATE"
                    )
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
                    text(
                        "SET CONSTRAINTS "
                        "capacity_executable_allocation_seal_guard IMMEDIATE"
                    )
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
                text(
                    "SELECT execution_epoch, execution_manifest_sha256 "
                    "FROM capacity_allocations"
                )
            ).one()
            assert parent == (None, None)
            assert child == (None, None)
        with engine.begin() as connection:
            connection.execute(text("TRUNCATE TABLE capacity_allocations"))
            assert connection.execute(
                text("SELECT count(*) FROM capacity_allocations")
            ).scalar_one() == 0
            assert connection.execute(
                text("SELECT count(*) FROM capacity_allocation_epochs")
            ).scalar_one() == 1
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
                    connection.execute(
                        text("TRUNCATE TABLE capacity_allocation_epochs CASCADE")
                    )
            with pytest.raises(
                sqlalchemy.exc.DBAPIError,
                match="executable allocations are append-only",
            ):
                with connection.begin_nested():
                    connection.execute(text("TRUNCATE TABLE capacity_allocations"))

            assert connection.execute(
                text("SELECT count(*) FROM capacity_allocation_epochs")
            ).scalar_one() == 1
            assert connection.execute(
                text("SELECT count(*) FROM capacity_allocations")
            ).scalar_one() == 1
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
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == "capacity_0005"

        command.upgrade(cfg, "capacity_0006")

        with engine.connect() as connection:
            assert connection.execute(
                text(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema = 'public' "
                    "AND table_name = 'capacity_allocation_epochs' "
                    "AND column_name = 'input_valid_until'"
                )
            ).scalar_one() == 1
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == "capacity_0006"
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
            assert connection.execute(
                text(
                    "SELECT count(*) FROM capacity_allocation_epochs "
                    "WHERE status = 'executable'"
                )
            ).scalar_one() == 1
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
                for column in inspect(connection).get_columns(
                    "capacity_allocation_epochs"
                )
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
            assert connection.execute(
                text("SELECT count(*) FROM public.capacity_executable_executor_states")
            ).scalar_one() == 1
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
            assert "capacity_executable_executor_states" not in inspect(
                connection
            ).get_table_names(schema="public")
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
            ).scalar_one() == ("capacity_0006")
        with environment_engine.connect() as connection:
            environment_revision = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            assert environment_revision != "capacity_0005"
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
    assert await assert_capacity_schema_at_head(capacity_engine) == 6


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
