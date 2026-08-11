"""Independent management-schema migration and safety constraints."""

from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy
from alembic import command
from alembic.config import Config as AlembicConfig
from sqlalchemy import create_engine, inspect, select, text
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


def _capacity_config(url: str) -> AlembicConfig:
    root = Path(__file__).resolve().parents[2]
    cfg = AlembicConfig(str(root / "capacity_migrations" / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "capacity_migrations"))
    os.environ["LOOM_CAPACITY_DB_URL"] = url
    return cfg


def test_shadow_schema_has_exact_tables_and_zero_execution_guard(
    capacity_postgres_url: str,
) -> None:
    engine = create_engine(capacity_postgres_url)
    try:
        with engine.begin() as connection:
            tables = set(inspect(connection).get_table_names())
            assert tables == EXPECTED_TABLES | {"alembic_version"}
            assert "teams" not in tables
            with pytest.raises(IntegrityError):
                connection.execute(
                    text(
                        "UPDATE capacity_authority_state "
                        "SET executable_new_capacity_ceiling = 1 "
                        "WHERE singleton_id = 1"
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
            ).scalar_one() == ("capacity_0003")
        with environment_engine.connect() as connection:
            environment_revision = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            assert environment_revision != "capacity_0003"
            assert not (EXPECTED_TABLES & set(inspect(connection).get_table_names()))
    finally:
        capacity_engine.dispose()
        environment_engine.dispose()


async def test_capacity_schema_error_never_names_environment_migrations(
    empty_capacity_engine: AsyncEngine,
) -> None:
    with pytest.raises(CapacitySchemaNotAtHeadError) as caught:
        await assert_capacity_schema_at_head(empty_capacity_engine)
    message = str(caught.value)
    assert "capacity_migrations/alembic.ini" in message
    assert "migrations/alembic.ini" not in message.replace("capacity_migrations/alembic.ini", "")


async def test_capacity_schema_startup_returns_numeric_head(
    capacity_engine: AsyncEngine,
) -> None:
    assert await assert_capacity_schema_at_head(capacity_engine) == 3


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
            seeds = connection.execute(
                select(CapacityAuditEvent).where(
                    CapacityAuditEvent.event_kind == "authority_incarnation_seeded"
                )
            ).mappings().all()
            assert len(seeds) == 1
            assert seeds[0]["actor_kind"] == "migration"
            assert seeds[0]["actor_id"] == "capacity-authority-bootstrap"
            assert seeds[0]["object_binding"] == {
                "authority_incarnation": str(authority)
            }
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
        f"{capacity_postgres_url}?application_name=capacity%40migration"
        "&connect_timeout=99"
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
