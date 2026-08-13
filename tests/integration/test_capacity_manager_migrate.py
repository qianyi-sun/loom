"""Fresh-database-only bootstrap for the global capacity authority."""

from __future__ import annotations

import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import (
    CheckConstraint,
    Connection,
    Engine,
    UniqueConstraint,
    create_engine,
    delete,
    inspect,
    select,
    text,
    update,
)
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError

import loom_capacity_manager.migrate as capacity_migrate
from loom_capacity_manager.migrate import (
    CapacityAuthorityBootstrapError,
    bind_fresh_authority,
    migrate_capacity_database,
)
from loom_capacity_manager.models import (
    Base,
    CapacityAuditEvent,
    CapacityAuthorityState,
    CapacityExecutableExecutorState,
    CapacityExecutionEpoch,
)

_MIGRATION_AUTHORITY = UUID("00000000-0000-4000-8000-000000000900")
_REVIEWED_AUTHORITY = UUID("00000000-0000-4000-8000-000000000901")
_OTHER_AUTHORITY = UUID("00000000-0000-4000-8000-000000000902")
_MIGRATION_ADVISORY_LOCK = (1280266061, 1128353857)
_TEST_BINDING_GATE_LOCK = (1280266061, 1413829460)


def _seed_active_execution(connection: Connection) -> int:
    """Seed one minimal active epoch inside a caller-owned rollback transaction."""

    execution_epoch = 1_200_001
    configuration_epoch = 1_200_001
    authority = connection.execute(
        select(CapacityAuthorityState.authority_incarnation).where(
            CapacityAuthorityState.singleton_id == 1
        )
    ).scalar_one()
    connection.execute(
        text(
            "INSERT INTO capacity_configuration_epochs "
            "(configuration_epoch, fleet_generation, fleet_digest, "
            "subject_generation_manifest, canonical_digest, "
            "activation_idempotency_key, activation_actor, "
            "activation_request_digest) VALUES "
            "(:configuration_epoch, 1, repeat('1', 64), '[]'::jsonb, "
            "repeat('2', 64), :configuration_key, 'migration-test', repeat('3', 64))"
        ),
        {
            "configuration_epoch": configuration_epoch,
            "configuration_key": uuid4(),
        },
    )
    connection.execute(
        text(
            "INSERT INTO capacity_execution_epochs "
            "(execution_epoch, authority_incarnation, prepared_writer_epoch, "
            "current_writer_epoch, configuration_epoch, fleet_generation, "
            "fleet_digest, execution_manifest_sha256, manifest_payload, "
            "trusted_fleet_release_sha256, oldlab_executor_id, "
            "oldlab_executor_incarnation, oldlab_pool_id, oldlab_pool_generation, "
            "gb10_executor_id, gb10_executor_incarnation, gb10_pool_id, "
            "gb10_pool_generation, environment_acknowledgements_sha256, "
            "legacy_writer_manifest_sha256, rollback_evidence_sha256, "
            "requested_ceiling, effective_ceiling, requested_rate_per_minute, "
            "effective_rate_per_minute, state, actor, idempotency_key, request_digest) "
            "VALUES (:execution_epoch, :authority, 1, 1, :configuration_epoch, 1, "
            "repeat('1', 64), repeat('4', 64), '{}'::jsonb, repeat('5', 64), "
            "'oldlab-executor', :oldlab_incarnation, 'oldlab', 1, "
            "'gb10-executor', :gb10_incarnation, 'gb10', 1, repeat('6', 64), "
            "repeat('7', 64), repeat('8', 64), 2, 0, 2, 0, 'prepared', "
            "'migration-test', :execution_key, repeat('9', 64))"
        ),
        {
            "execution_epoch": execution_epoch,
            "authority": authority,
            "configuration_epoch": configuration_epoch,
            "oldlab_incarnation": UUID(int=12012),
            "gb10_incarnation": UUID(int=12011),
            "execution_key": uuid4(),
        },
    )
    for index, pool_id in enumerate(("gb10", "oldlab"), start=1):
        connection.execute(
            text(
                "INSERT INTO capacity_execution_executors "
                "(id, execution_epoch, execution_manifest_sha256, executor_id, "
                "executor_incarnation, pool_id, pool_generation, signing_key_id, "
                "signing_key_sha256, local_authority_sha256, "
                "controller_authority_sha256, actor, idempotency_key, "
                "registration_digest, registration_payload) VALUES "
                "(:id, :execution_epoch, repeat('4', 64), :executor_id, "
                ":executor_incarnation, :pool_id, 1, :signing_key_id, "
                "repeat('a', 64), repeat('b', 64), repeat('c', 64), "
                "'migration-test', :idempotency_key, repeat(:digit, 64), '{}'::jsonb)"
            ),
            {
                "id": uuid4(),
                "execution_epoch": execution_epoch,
                "executor_id": f"{pool_id}-executor",
                "executor_incarnation": UUID(int=12010 + index),
                "pool_id": pool_id,
                "signing_key_id": f"{pool_id}-key",
                "idempotency_key": uuid4(),
                "digit": str(index),
            },
        )
    connection.execute(
        text(
            "UPDATE capacity_execution_epochs SET state = 'active', "
            "effective_ceiling = 1, effective_rate_per_minute = 1, "
            "activation_actor = 'migration-test', "
            "activation_idempotency_key = :activation_key, "
            "activation_request_digest = repeat('d', 64), activated_at = now() "
            "WHERE execution_epoch = :execution_epoch"
        ),
        {"activation_key": uuid4(), "execution_epoch": execution_epoch},
    )
    return execution_epoch


def _drain_for_sql_guard(connection: Connection, execution_epoch: int) -> None:
    connection.execute(
        text(
            "UPDATE capacity_execution_epochs SET state = 'drain-only', "
            "effective_ceiling = 0, effective_rate_per_minute = 0, "
            "current_writer_epoch = current_writer_epoch + 1, "
            "drain_actor = 'capacity-writer-replacement', "
            "drain_idempotency_key = :drain_key, "
            "drain_request_digest = repeat('e', 64), "
            "drain_request_payload = '{}'::jsonb, drain_only_at = now() "
            "WHERE execution_epoch = :execution_epoch"
        ),
        {"drain_key": uuid4(), "execution_epoch": execution_epoch},
    )


def _database_url_for_application(database_url: str, application_name: str) -> str:
    return (
        make_url(database_url)
        .update_query_dict({"application_name": application_name})
        .render_as_string(hide_password=False)
    )


def _migration_process(
    database_url_file: Path,
    authority: UUID,
) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "loom_capacity_manager.migrate",
            "--db-url-file",
            str(database_url_file),
            "--expected-authority-incarnation",
            str(authority),
        ],
        cwd=Path(__file__).resolve().parents[2],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _wait_for_advisory_lock(
    observer: Connection,
    application_name: str,
    query_fragment: str,
    process: subprocess.Popen[str],
) -> None:
    deadline = time.monotonic() + 10
    observed: list[dict[str, object]] = []
    while time.monotonic() < deadline:
        observed = [
            dict(row)
            for row in observer.execute(
                text(
                    "SELECT state, wait_event_type, wait_event, query "
                    "FROM pg_stat_activity WHERE application_name = :application_name"
                ),
                {"application_name": application_name},
            ).mappings()
        ]
        if (
            len(observed) == 1
            and observed[0]["wait_event_type"] == "Lock"
            and observed[0]["wait_event"] == "advisory"
            and query_fragment in str(observed[0]["query"])
        ):
            return
        if process.poll() is not None:
            output, error = process.communicate()
            raise AssertionError(
                f"{application_name} exited before waiting on {query_fragment!r}: "
                f"returncode={process.returncode}, stdout={output!r}, stderr={error!r}"
            )
        time.sleep(0.01)
    raise AssertionError(f"{application_name} did not wait on {query_fragment!r}: {observed!r}")


def _terminate_process(process: subprocess.Popen[str] | None) -> None:
    if process is None:
        return
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    if process.stdout is not None:
        process.stdout.close()
    if process.stderr is not None:
        process.stderr.close()


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
            connection.execute(
                text(
                    "ALTER TABLE capacity_authority_state DISABLE TRIGGER "
                    "capacity_authority_execution_transition_guard"
                )
            )
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
                CapacityAuditEvent.__table__.insert().values(**_seed_marker(_MIGRATION_AUTHORITY))
            )
            connection.execute(
                text(
                    "ALTER TABLE capacity_authority_state ENABLE TRIGGER "
                    "capacity_authority_execution_transition_guard"
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


def test_retirement_lifecycle_schema_matches_model_and_migration(
    capacity_postgres_url: str,
) -> None:
    """Dropping lifecycle evidence or its database guards must create model drift."""

    lifecycle_columns = {
        "drain_actor": ("TEXT", True),
        "drain_idempotency_key": ("UUID", True),
        "drain_request_digest": ("TEXT", True),
        "drain_request_payload": ("JSONB", True),
        "retirement_actor": ("TEXT", True),
        "retirement_idempotency_key": ("UUID", True),
        "retirement_request_digest": ("TEXT", True),
        "retirement_request_payload": ("JSONB", True),
        "drain_only_at": ("TIMESTAMP", True),
        "retired_at": ("TIMESTAMP", True),
    }
    executor_columns = {
        "retirement_safe": ("BOOLEAN", False),
        "retirement_inventory_digest": ("TEXT", True),
    }
    expected_unique_constraints = {
        "capacity_execution_epoch_drain_idempotency_key": ("drain_idempotency_key",),
        "capacity_execution_epoch_retirement_idempotency_key": ("retirement_idempotency_key",),
    }
    expected_model_checks = {
        "capacity_execution_epoch_lifecycle_actor_check": (
            "(drain_actor IS NULL OR "
            "octet_length(drain_actor) BETWEEN 1 AND 256) "
            "AND (retirement_actor IS NULL OR "
            "octet_length(retirement_actor) BETWEEN 1 AND 256)"
        ),
        "capacity_execution_epoch_lifecycle_payload_check": (
            "(drain_request_payload IS NULL OR "
            "(jsonb_typeof(drain_request_payload) = 'object' "
            "AND octet_length(drain_request_payload::text) <= 8388608)) "
            "AND (retirement_request_payload IS NULL OR "
            "(jsonb_typeof(retirement_request_payload) = 'object' "
            "AND octet_length(retirement_request_payload::text) <= 8388608))"
        ),
        "capacity_executable_executor_retirement_check": (
            "(retirement_safe AND retirement_inventory_digest IS NOT NULL "
            "AND retirement_inventory_digest ~ '^[0-9a-f]{64}$' "
            "AND retirement_inventory_digest = last_inventory_digest "
            "AND inventory_high_water > 0 AND inventory_payload IS NOT NULL "
            "AND jsonb_typeof(inventory_payload) = 'object' "
            "AND last_inventory_at IS NOT NULL "
            "AND inventory_payload -> 'schema_version' = '2'::jsonb "
            "AND inventory_payload -> 'inventory_sequence' "
            "= to_jsonb(inventory_high_water) "
            "AND inventory_payload ->> 'executor_id' = executor_id "
            "AND inventory_payload ->> 'executor_incarnation' "
            "= executor_incarnation::text "
            "AND inventory_payload ->> 'pool_id' = pool_id "
            "AND inventory_payload -> 'pool_generation' = to_jsonb(pool_generation) "
            "AND inventory_payload -> 'journal_sequence' = to_jsonb(journal_high_water) "
            "AND inventory_payload ->> 'journal_digest' = journal_digest "
            "AND inventory_payload -> 'execution' -> 'execution_epoch' "
            "= to_jsonb(execution_epoch) "
            "AND inventory_payload -> 'execution' ->> 'execution_manifest_sha256' "
            "= execution_manifest_sha256) OR "
            "(NOT retirement_safe AND retirement_inventory_digest IS NULL)"
        ),
    }
    expected_database_checks = {
        "capacity_execution_epoch_lifecycle_actor_check": (
            "drain_actor IS NULL OR octet_length(drain_actor) >= 1 "
            "AND octet_length(drain_actor) <= 256) AND (retirement_actor IS NULL "
            "OR octet_length(retirement_actor) >= 1 "
            "AND octet_length(retirement_actor) <= 256"
        ),
        "capacity_execution_epoch_lifecycle_payload_check": (
            "drain_request_payload IS NULL OR "
            "jsonb_typeof(drain_request_payload) = 'object'::text "
            "AND octet_length(drain_request_payload::text) <= 8388608) AND "
            "(retirement_request_payload IS NULL OR "
            "jsonb_typeof(retirement_request_payload) = 'object'::text "
            "AND octet_length(retirement_request_payload::text) <= 8388608"
        ),
        "capacity_executable_executor_retirement_check": (
            "retirement_safe AND retirement_inventory_digest IS NOT NULL AND "
            "retirement_inventory_digest ~ '^[0-9a-f]{64}$'::text AND "
            "retirement_inventory_digest = last_inventory_digest AND "
            "inventory_high_water > 0 AND inventory_payload IS NOT NULL AND "
            "jsonb_typeof(inventory_payload) = 'object'::text AND "
            "last_inventory_at IS NOT NULL AND "
            "(inventory_payload -> 'schema_version'::text) = '2'::jsonb AND "
            "(inventory_payload -> 'inventory_sequence'::text) "
            "= to_jsonb(inventory_high_water) AND "
            "(inventory_payload ->> 'executor_id'::text) = executor_id AND "
            "(inventory_payload ->> 'executor_incarnation'::text) "
            "= executor_incarnation::text AND "
            "(inventory_payload ->> 'pool_id'::text) = pool_id AND "
            "(inventory_payload -> 'pool_generation'::text) "
            "= to_jsonb(pool_generation) AND "
            "(inventory_payload -> 'journal_sequence'::text) "
            "= to_jsonb(journal_high_water) AND "
            "(inventory_payload ->> 'journal_digest'::text) = journal_digest AND "
            "((inventory_payload -> 'execution'::text) -> "
            "'execution_epoch'::text) = to_jsonb(execution_epoch) AND "
            "((inventory_payload -> 'execution'::text) ->> "
            "'execution_manifest_sha256'::text) = execution_manifest_sha256 OR "
            "NOT retirement_safe AND retirement_inventory_digest IS NULL"
        ),
    }
    model_epoch_columns = CapacityExecutionEpoch.__table__.columns
    model_executor_columns = CapacityExecutableExecutorState.__table__.columns
    assert {
        name: (str(model_epoch_columns[name].type), model_epoch_columns[name].nullable)
        for name in lifecycle_columns
    } == lifecycle_columns
    assert {
        name: (str(model_executor_columns[name].type), model_executor_columns[name].nullable)
        for name in executor_columns
    } == executor_columns
    model_uniques = {
        constraint.name: tuple(column.name for column in constraint.columns)
        for constraint in CapacityExecutionEpoch.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert expected_unique_constraints.items() <= model_uniques.items()
    model_checks = {
        constraint.name: str(constraint.sqltext)
        for table in (
            CapacityExecutionEpoch.__table__,
            CapacityExecutableExecutorState.__table__,
        )
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert expected_model_checks.items() <= model_checks.items()
    state_time_sql = model_checks["capacity_execution_epoch_state_time_check"]
    for exact_state_clause in (
        "state = 'prepared' AND effective_ceiling = 0 ",
        "state = 'active' AND effective_ceiling > 0 ",
        "state = 'drain-only' AND effective_ceiling = 0 ",
        "state = 'retired' AND effective_ceiling = 0 ",
        "drain_request_payload IS NOT NULL ",
        "retirement_request_payload IS NOT NULL ",
    ):
        assert exact_state_clause in state_time_sql

    engine = create_engine(capacity_postgres_url)
    try:
        with engine.connect() as connection:
            schema = inspect(connection)
            database_epoch_columns = {
                column["name"]: column for column in schema.get_columns("capacity_execution_epochs")
            }
            database_executor_columns = {
                column["name"]: column
                for column in schema.get_columns("capacity_executable_executor_states")
            }
            assert {
                name: (
                    str(database_epoch_columns[name]["type"]),
                    database_epoch_columns[name]["nullable"],
                )
                for name in lifecycle_columns
            } == lifecycle_columns
            assert {
                name: (
                    str(database_executor_columns[name]["type"]),
                    database_executor_columns[name]["nullable"],
                )
                for name in executor_columns
            } == executor_columns
            database_uniques = {
                constraint["name"]: tuple(constraint["column_names"])
                for constraint in schema.get_unique_constraints("capacity_execution_epochs")
            }
            assert expected_unique_constraints.items() <= database_uniques.items()
            database_indexes = {
                index["name"]: (tuple(index["column_names"]), index["unique"])
                for index in schema.get_indexes("capacity_execution_epochs")
                if index["name"] in expected_unique_constraints
            }
            assert database_indexes == {
                name: (columns, True) for name, columns in expected_unique_constraints.items()
            }
            database_checks = {
                constraint["name"]: constraint["sqltext"]
                for table_name in (
                    "capacity_execution_epochs",
                    "capacity_executable_executor_states",
                )
                for constraint in schema.get_check_constraints(table_name)
            }
            assert {
                name: database_checks[name] for name in expected_database_checks
            } == expected_database_checks
            trigger_body = connection.execute(
                text(
                    "SELECT pg_get_functiondef("
                    "'capacity_execution_epoch_transition_guard()'::regprocedure)"
                )
            ).scalar_one()
    finally:
        engine.dispose()

    normalized_trigger = (
        " ".join(trigger_body.lower().split()).replace("( ", "(").replace(" )", ")")
    )
    for exact_trigger_clause in (
        "(select count(*) from jsonb_object_keys(new.retirement_request_payload)) <> 7",
        "jsonb_array_length(new.retirement_request_payload -> 'executor_checkpoints') <> 2",
        "new.retirement_request_payload ->> 'authority_incarnation' is distinct from new.authority_incarnation::text",
        "new.retirement_request_payload -> 'expected_writer_epoch' is distinct from to_jsonb(new.current_writer_epoch)",
        "new.retirement_request_payload -> 'execution_epoch' is distinct from to_jsonb(new.execution_epoch)",
        "new.retirement_request_payload ->> 'execution_manifest_sha256' is distinct from new.execution_manifest_sha256",
        "checkpoint.value -> 'pool_generation' = to_jsonb(executor.pool_generation)",
        "checkpoint.value -> 'heartbeat_sequence' = to_jsonb(executor.heartbeat_high_water)",
        "checkpoint.value -> 'command_sequence' = to_jsonb(executor.command_high_water)",
        "checkpoint.value -> 'journal_sequence' = to_jsonb(executor.journal_high_water)",
        "checkpoint.value -> 'inventory_sequence' = to_jsonb(executor.inventory_high_water)",
        "checkpoint.value ->> 'inventory_digest'",
        "executor.retirement_safe",
        "executor.last_heartbeat_at > executor.last_inventory_at",
        "order by executor.pool_id for update",
        "order by intent.launch_rank for update",
        "intent.state <> 'released'",
    ):
        assert exact_trigger_clause in normalized_trigger


@pytest.mark.parametrize(
    "mutation",
    (
        "state = 'retired', effective_ceiling = 0, effective_rate_per_minute = 0, "
        "retirement_actor = 'migration-test', "
        "retirement_idempotency_key = '00000000-0000-4000-8000-000000001221', "
        "retirement_request_digest = repeat('f', 64), "
        "retirement_request_payload = '{}'::jsonb, retired_at = now()",
        "effective_ceiling = 2",
        "effective_rate_per_minute = 2",
    ),
    ids=("skip-drain", "change-ceiling", "change-rate"),
)
def test_direct_sql_cannot_skip_drain_or_mutate_active_envelope(
    capacity_postgres_url: str,
    mutation: str,
) -> None:
    """The transition trigger, not only the store, must reject active-state bypasses."""

    engine = create_engine(capacity_postgres_url)
    connection = engine.connect()
    transaction = connection.begin()
    try:
        execution_epoch = _seed_active_execution(connection)
        with pytest.raises(IntegrityError):
            with connection.begin_nested():
                connection.execute(
                    text(
                        "UPDATE capacity_execution_epochs SET "
                        f"{mutation} WHERE execution_epoch = :execution_epoch"
                    ),
                    {"execution_epoch": execution_epoch},
                )
        assert connection.execute(
            text(
                "SELECT state, effective_ceiling, effective_rate_per_minute "
                "FROM capacity_execution_epochs WHERE execution_epoch = :execution_epoch"
            ),
            {"execution_epoch": execution_epoch},
        ).one() == ("active", 1, 1)
    finally:
        transaction.rollback()
        connection.close()
        engine.dispose()


@pytest.mark.parametrize(
    "mutation",
    (
        "state = 'active', effective_ceiling = 1, effective_rate_per_minute = 1",
        "state = 'retired', retired_at = now()",
    ),
    ids=("reactivate", "retire-without-evidence"),
)
def test_direct_sql_cannot_reactivate_or_retire_without_evidence(
    capacity_postgres_url: str,
    mutation: str,
) -> None:
    """Drain-only is monotonic and retirement requires its complete durable request."""

    engine = create_engine(capacity_postgres_url)
    connection = engine.connect()
    transaction = connection.begin()
    try:
        execution_epoch = _seed_active_execution(connection)
        _drain_for_sql_guard(connection, execution_epoch)
        with pytest.raises(IntegrityError):
            with connection.begin_nested():
                connection.execute(
                    text(
                        "UPDATE capacity_execution_epochs SET "
                        f"{mutation} WHERE execution_epoch = :execution_epoch"
                    ),
                    {"execution_epoch": execution_epoch},
                )
        assert connection.execute(
            text(
                "SELECT state, effective_ceiling, effective_rate_per_minute "
                "FROM capacity_execution_epochs WHERE execution_epoch = :execution_epoch"
            ),
            {"execution_epoch": execution_epoch},
        ).one() == ("drain-only", 0, 0)
    finally:
        transaction.rollback()
        connection.close()
        engine.dispose()


def test_direct_sql_cannot_retire_with_fabricated_bounded_evidence(
    capacity_postgres_url: str,
) -> None:
    """A coordinated authority reset still needs exact safe final checkpoints."""

    engine = create_engine(capacity_postgres_url)
    connection = engine.connect()
    transaction = connection.begin()
    try:
        execution_epoch = _seed_active_execution(connection)
        _drain_for_sql_guard(connection, execution_epoch)
        connection.execute(
            text(
                "UPDATE capacity_authority_state SET writer_epoch = 2, "
                "execution_epoch = :execution_epoch, execution_state = 'drain-only', "
                "execution_manifest_sha256 = repeat('4', 64), "
                "executable_new_capacity_ceiling = 0, increase_freeze = true "
                "WHERE singleton_id = 1"
            ),
            {"execution_epoch": execution_epoch},
        )

        with pytest.raises(IntegrityError):
            with connection.begin_nested():
                connection.execute(
                    text(
                        "UPDATE capacity_execution_epochs SET state = 'retired', "
                        "retirement_actor = 'fabricated-sql-retirement', "
                        "retirement_idempotency_key = :retirement_key, "
                        "retirement_request_digest = repeat('f', 64), "
                        "retirement_request_payload = '{}'::jsonb, retired_at = now() "
                        "WHERE execution_epoch = :execution_epoch"
                    ),
                    {
                        "execution_epoch": execution_epoch,
                        "retirement_key": uuid4(),
                    },
                )
                connection.execute(
                    text(
                        "UPDATE capacity_authority_state SET execution_epoch = 0, "
                        "execution_state = 'shadow', execution_manifest_sha256 = NULL, "
                        "executable_new_capacity_ceiling = 0 "
                        "WHERE singleton_id = 1"
                    )
                )
                connection.execute(
                    text("SET CONSTRAINTS capacity_authority_execution_epoch_fkey IMMEDIATE")
                )

        assert connection.execute(
            text(
                "SELECT state, effective_ceiling, effective_rate_per_minute "
                "FROM capacity_execution_epochs WHERE execution_epoch = :execution_epoch"
            ),
            {"execution_epoch": execution_epoch},
        ).one() == ("drain-only", 0, 0)
    finally:
        transaction.rollback()
        connection.close()
        engine.dispose()


@pytest.mark.parametrize(
    ("retirement_safe", "retirement_inventory_digest"),
    (
        (True, None),
        (False, "f" * 64),
        (True, "not-a-digest"),
        (True, "f" * 64),
    ),
)
def test_executor_retirement_safety_requires_exact_canonical_digest(
    capacity_postgres_url: str,
    retirement_safe: bool,
    retirement_inventory_digest: str | None,
) -> None:
    """A boolean alone or an unbound digest must never establish retirement safety."""

    engine = create_engine(capacity_postgres_url)
    connection = engine.connect()
    transaction = connection.begin()
    try:
        execution_epoch = _seed_active_execution(connection)
        with pytest.raises(IntegrityError):
            with connection.begin_nested():
                connection.execute(
                    text(
                        "INSERT INTO capacity_executable_executor_states "
                        "(id, execution_epoch, execution_manifest_sha256, executor_id, "
                        "executor_incarnation, pool_id, pool_generation, state, "
                        "retirement_safe, retirement_inventory_digest, "
                        "lease_expires_at, last_heartbeat_at) VALUES "
                        "(:id, :execution_epoch, repeat('4', 64), 'gb10-executor', "
                        ":executor_incarnation, 'gb10', 1, 'current', "
                        ":retirement_safe, :retirement_inventory_digest, "
                        "now() + interval '1 minute', now())"
                    ),
                    {
                        "id": uuid4(),
                        "execution_epoch": execution_epoch,
                        "executor_incarnation": UUID(int=12011),
                        "retirement_safe": retirement_safe,
                        "retirement_inventory_digest": retirement_inventory_digest,
                    },
                )
    finally:
        transaction.rollback()
        connection.close()
        engine.dispose()


def test_executor_retirement_safety_rejects_noncanonical_inventory_payload(
    capacity_postgres_url: str,
) -> None:
    """Stringified numeric fields cannot impersonate an authenticated inventory."""

    engine = create_engine(capacity_postgres_url)
    connection = engine.connect()
    transaction = connection.begin()
    try:
        execution_epoch = _seed_active_execution(connection)
        with pytest.raises(IntegrityError):
            with connection.begin_nested():
                connection.execute(
                    text(
                        "INSERT INTO capacity_executable_executor_states "
                        "(id, execution_epoch, execution_manifest_sha256, executor_id, "
                        "executor_incarnation, pool_id, pool_generation, state, "
                        "inventory_high_water, last_inventory_digest, inventory_payload, "
                        "last_inventory_at, retirement_safe, retirement_inventory_digest, "
                        "lease_expires_at, last_heartbeat_at) VALUES "
                        "(:id, :execution_epoch, repeat('4', 64), 'gb10-executor', "
                        ":executor_incarnation, 'gb10', 1, 'current', 1, repeat('f', 64), "
                        "jsonb_build_object("
                        "'schema_version', 2, "
                        "'execution', jsonb_build_object("
                        "'execution_epoch', :execution_epoch, "
                        "'execution_manifest_sha256', repeat('4', 64)), "
                        "'executor_id', 'gb10-executor', "
                        "'executor_incarnation', CAST(:executor_incarnation AS text), "
                        "'pool_id', 'gb10', 'pool_generation', 1, "
                        "'inventory_sequence', '1', "
                        "'journal_sequence', 0, 'journal_digest', repeat('0', 64), "
                        "'journal_checkpoint_sequence', 0, "
                        "'journal_checkpoint_digest', repeat('0', 64), "
                        "'complete', true, 'records', '[]'::jsonb, 'executable', true), "
                        "now(), true, repeat('f', 64), now() + interval '1 minute', now())"
                    ),
                    {
                        "id": uuid4(),
                        "execution_epoch": execution_epoch,
                        "executor_incarnation": UUID(int=12011),
                    },
                )
    finally:
        transaction.rollback()
        connection.close()
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
    assert markers[0]["object_binding"] == {"authority_incarnation": str(_REVIEWED_AUTHORITY)}
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
            connection.execute(CapacityAuditEvent.__table__.insert().values(**reserved_marker))
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
                CapacityAuditEvent.__table__.insert().values(**_seed_marker(_MIGRATION_AUTHORITY))
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


def test_seed_event_after_binding_event_fails_closed(
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
                    object_binding={"authority_incarnation": str(_REVIEWED_AUTHORITY)},
                    detail={"state": "reviewed-bootstrap-bound"},
                )
            )
            connection.execute(
                CapacityAuditEvent.__table__.insert().values(**_seed_marker(_MIGRATION_AUTHORITY))
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
    encoded_url = f"{capacity_postgres_url}{separator}application_name=capacity%40bootstrap"
    database_url_file = tmp_path / "database-url"
    database_url_file.write_text(encoded_url, encoding="utf-8")
    database_url_file.chmod(0o600)

    migrate_capacity_database(database_url_file, _REVIEWED_AUTHORITY)

    assert _authority(capacity_postgres_url)["authority_incarnation"] == _REVIEWED_AUTHORITY


def test_whole_migration_commands_share_one_lock_through_authority_binding(
    capacity_postgres_url: str,
    tmp_path: Path,
) -> None:
    _reset_empty_shadow(capacity_postgres_url)
    expected_application = "capacity-migration-expected"
    wrong_application = "capacity-migration-wrong"
    expected_url_file = tmp_path / "expected-database-url"
    wrong_url_file = tmp_path / "wrong-database-url"
    expected_url_file.write_text(
        _database_url_for_application(capacity_postgres_url, expected_application),
        encoding="utf-8",
    )
    wrong_url_file.write_text(
        _database_url_for_application(capacity_postgres_url, wrong_application),
        encoding="utf-8",
    )
    expected_url_file.chmod(0o600)
    wrong_url_file.chmod(0o600)

    control_engine = create_engine(capacity_postgres_url)
    expected_process: subprocess.Popen[str] | None = None
    wrong_process: subprocess.Popen[str] | None = None
    gate_connection: Connection | None = None
    gate_transaction = None
    try:
        with control_engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE FUNCTION loom_test_gate_capacity_binding() "
                    "RETURNS trigger LANGUAGE plpgsql AS $$ "
                    "BEGIN "
                    "IF NEW.event_kind = 'authority_incarnation_bound' THEN "
                    "PERFORM pg_advisory_xact_lock("
                    f"{_TEST_BINDING_GATE_LOCK[0]}, {_TEST_BINDING_GATE_LOCK[1]}); "
                    "END IF; "
                    "RETURN NEW; "
                    "END; $$"
                )
            )
            connection.execute(
                text(
                    "CREATE TRIGGER loom_test_gate_capacity_binding "
                    "BEFORE INSERT ON capacity_audit_events "
                    "FOR EACH ROW EXECUTE FUNCTION loom_test_gate_capacity_binding()"
                )
            )

        gate_connection = control_engine.connect()
        gate_transaction = gate_connection.begin()
        gate_connection.execute(
            text("SELECT pg_advisory_xact_lock(:namespace, :resource)"),
            {
                "namespace": _TEST_BINDING_GATE_LOCK[0],
                "resource": _TEST_BINDING_GATE_LOCK[1],
            },
        )
        with control_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as observer:
            expected_process = _migration_process(
                expected_url_file,
                _REVIEWED_AUTHORITY,
            )
            _wait_for_advisory_lock(
                observer,
                expected_application,
                "INSERT INTO capacity_audit_events",
                expected_process,
            )

            wrong_process = _migration_process(wrong_url_file, _OTHER_AUTHORITY)
            _wait_for_advisory_lock(
                observer,
                wrong_application,
                "pg_advisory_lock",
                wrong_process,
            )

        gate_transaction.commit()
        expected_output, expected_error = expected_process.communicate(timeout=15)
        wrong_output, wrong_error = wrong_process.communicate(timeout=15)

        assert expected_process.returncode == 0, (expected_output, expected_error)
        assert wrong_process.returncode == 1, (wrong_output, wrong_error)
        assert wrong_output == ""
        assert wrong_error.endswith("error: capacity migration failed\n")
        assert capacity_postgres_url not in wrong_error
        assert _authority(capacity_postgres_url)["authority_incarnation"] == (_REVIEWED_AUTHORITY)
        with control_engine.connect() as connection:
            binding_markers = connection.execute(
                select(CapacityAuditEvent.id).where(
                    CapacityAuditEvent.event_kind == "authority_incarnation_bound"
                )
            ).all()
        assert len(binding_markers) == 1
    finally:
        if gate_transaction is not None and gate_transaction.is_active:
            gate_transaction.rollback()
        _terminate_process(expected_process)
        _terminate_process(wrong_process)
        if gate_connection is not None:
            gate_connection.close()
        with control_engine.begin() as connection:
            connection.execute(
                text(
                    "DROP TRIGGER IF EXISTS loom_test_gate_capacity_binding "
                    "ON capacity_audit_events"
                )
            )
            connection.execute(text("DROP FUNCTION IF EXISTS loom_test_gate_capacity_binding()"))
        control_engine.dispose()


def test_migration_releases_supplied_connection_lock_after_alembic_failure(
    capacity_postgres_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_url_file = tmp_path / "database-url"
    database_url_file.write_text(capacity_postgres_url, encoding="utf-8")
    database_url_file.chmod(0o600)
    explicit_alembic_ini = Path(__file__).resolve().parents[2] / "capacity_migrations/alembic.ini"
    captured: dict[str, object] = {}
    observer_engine = create_engine(capacity_postgres_url)

    def fail_upgrade(config: object, revision: str) -> None:
        assert isinstance(config, capacity_migrate.AlembicConfig)
        connection = config.attributes["connection"]
        assert isinstance(connection, Connection)
        backend_pid = connection.execute(text("SELECT pg_backend_pid()"))
        captured["backend_pid"] = backend_pid.scalar_one()
        captured["config_file_name"] = config.config_file_name
        captured["revision"] = revision
        with observer_engine.connect() as observer:
            captured["lock_granted"] = observer.execute(
                text(
                    "SELECT granted FROM pg_locks "
                    "WHERE locktype = 'advisory' AND pid = :backend_pid "
                    "AND classid = :namespace AND objid = :resource "
                    "AND objsubid = 2"
                ),
                {
                    "backend_pid": captured["backend_pid"],
                    "namespace": _MIGRATION_ADVISORY_LOCK[0],
                    "resource": _MIGRATION_ADVISORY_LOCK[1],
                },
            ).scalar_one_or_none()
        raise RuntimeError("deliberate Alembic failure")

    monkeypatch.setattr(capacity_migrate.command, "upgrade", fail_upgrade)
    try:
        with pytest.raises(
            CapacityAuthorityBootstrapError,
            match="capacity schema migration failed",
        ):
            migrate_capacity_database(
                database_url_file,
                _REVIEWED_AUTHORITY,
                alembic_ini=explicit_alembic_ini,
            )

        assert captured == {
            "backend_pid": captured["backend_pid"],
            "config_file_name": str(explicit_alembic_ini),
            "revision": "head",
            "lock_granted": True,
        }
        with observer_engine.connect() as observer:
            acquired = observer.execute(
                text("SELECT pg_try_advisory_lock(:namespace, :resource)"),
                {
                    "namespace": _MIGRATION_ADVISORY_LOCK[0],
                    "resource": _MIGRATION_ADVISORY_LOCK[1],
                },
            ).scalar_one()
            assert acquired is True
            assert (
                observer.execute(
                    text("SELECT pg_advisory_unlock(:namespace, :resource)"),
                    {
                        "namespace": _MIGRATION_ADVISORY_LOCK[0],
                        "resource": _MIGRATION_ADVISORY_LOCK[1],
                    },
                ).scalar_one()
                is True
            )
    finally:
        observer_engine.dispose()


def test_authority_binding_connection_enforces_fixed_postgres_timeouts(
    capacity_postgres_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_url = (
        make_url(capacity_postgres_url)
        .update_query_dict(
            {
                "application_name": "capacity@bootstrap",
                "connect_timeout": "99",
            }
        )
        .render_as_string(hide_password=False)
    )
    database_url_file = tmp_path / "database-url"
    database_url_file.write_text(database_url, encoding="utf-8")
    database_url_file.chmod(0o600)
    captured: dict[str, object] = {}
    real_create_engine = create_engine

    def create(url: str, **kwargs: object) -> Engine:
        captured["url"] = url
        captured["kwargs"] = kwargs
        return real_create_engine(url, **kwargs)

    monkeypatch.setattr(capacity_migrate, "create_engine", create)

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
    }
