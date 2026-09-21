"""Published shared-cluster retirement evidence constraints on retained databases."""

from __future__ import annotations

import hashlib
import json
from uuid import UUID, uuid4

import pytest
from sqlalchemy import (
    Connection,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.exc import IntegrityError


def _seed_prepared_execution(connection: Connection) -> int:
    """Seed one minimal prepared epoch inside a caller-owned rollback transaction."""

    execution_epoch = 1_200_001
    configuration_epoch = 1_200_001
    authority = connection.execute(
        text("SELECT authority_incarnation FROM capacity_authority_state WHERE singleton_id=1")
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
            "effective_rate_per_minute, state, actor, idempotency_key, request_digest) "
            "VALUES (:execution_epoch, :authority, 1, 1, :configuration_epoch, 1, "
            "repeat('1', 64), repeat('4', 64), '{}'::jsonb, repeat('5', 64), "
            "'oldlab-executor', :oldlab_incarnation, 'oldlab', 1, "
            "repeat('a', 64), repeat('b', 64), repeat('c', 64), "
            "'gb10-executor', :gb10_incarnation, 'gb10', 1, "
            "repeat('a', 64), repeat('b', 64), repeat('c', 64), repeat('6', 64), "
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
        text("UPDATE capacity_authority_state SET writer_epoch = 1 WHERE singleton_id = 1")
    )
    connection.execute(
        text(
            "UPDATE capacity_authority_state SET execution_epoch = :execution_epoch, "
            "execution_state = 'prepared', execution_manifest_sha256 = repeat('4', 64), "
            "executable_new_capacity_ceiling = 0 WHERE singleton_id = 1"
        ),
        {"execution_epoch": execution_epoch},
    )
    return execution_epoch


def _seed_active_execution(connection: Connection) -> int:
    """Seed one minimal active epoch inside a caller-owned rollback transaction."""

    execution_epoch = _seed_prepared_execution(connection)
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
    connection.execute(
        text(
            "UPDATE capacity_authority_state SET execution_state = 'active', "
            "executable_new_capacity_ceiling = 1 WHERE singleton_id = 1"
        )
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


def test_retirement_lifecycle_schema_preserves_evidence_constraints(
    capacity_postgres_url: str,
) -> None:
    """Published lifecycle columns, constraints and trigger guards remain intact."""

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
        "inventory_confirmation_journal_digest": ("TEXT", True),
        "chunked_inventory_seen": ("BOOLEAN", False),
    }
    expected_unique_constraints = {
        "capacity_execution_epoch_drain_idempotency_key": ("drain_idempotency_key",),
        "capacity_execution_epoch_retirement_idempotency_key": ("retirement_idempotency_key",),
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
            "(retirement_safe AND retirement_inventory_digest IS NOT NULL AND "
            "retirement_inventory_digest ~ '^[0-9a-f]{64}$'::text AND "
            "retirement_inventory_digest = last_inventory_digest AND "
            "inventory_high_water > 0 AND inventory_payload IS NOT NULL AND "
            "jsonb_typeof(inventory_payload) = 'object'::text AND "
            "last_inventory_at IS NOT NULL AND "
            "last_heartbeat_at > last_inventory_at AND "
            "((inventory_payload -> 'schema_version'::text) = ANY (ARRAY['2'::jsonb, '3'::jsonb])) AND "
            "(inventory_payload -> 'inventory_sequence'::text) "
            "= to_jsonb(inventory_high_water) AND "
            "(inventory_payload ->> 'executor_id'::text) = executor_id AND "
            "(inventory_payload ->> 'executor_incarnation'::text) "
            "= executor_incarnation::text AND "
            "(inventory_payload ->> 'pool_id'::text) = pool_id AND "
            "(inventory_payload -> 'pool_generation'::text) "
            "= to_jsonb(pool_generation) AND "
            "inventory_confirmation_journal_digest ~ '^[0-9a-f]{64}$'::text AND "
            "((inventory_payload -> 'journal_sequence'::text) "
            "= to_jsonb(journal_high_water) AND "
            "(inventory_payload ->> 'journal_digest'::text) = journal_digest OR "
            "(inventory_payload -> 'journal_sequence'::text) "
            "= to_jsonb(journal_high_water - (2 +\nCASE\n"
            "    WHEN (inventory_payload -> 'schema_version'::text) = '3'::jsonb AND "
            "octet_length(capacity_executable_canonical_jsonb_text(inventory_payload)) > 32768 "
            "THEN (octet_length(capacity_executable_canonical_jsonb_text(inventory_payload)) + 32767) / 32768\n"
            "    ELSE 0\nEND)) AND "
            "inventory_confirmation_journal_digest = journal_digest) AND "
            "((inventory_payload -> 'execution'::text) -> "
            "'execution_epoch'::text) = to_jsonb(execution_epoch) AND "
            "((inventory_payload -> 'execution'::text) ->> "
            "'execution_manifest_sha256'::text) = execution_manifest_sha256) IS TRUE OR "
            "NOT retirement_safe AND retirement_inventory_digest IS NULL"
        ),
    }
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


def test_prepared_abort_guard_accepts_only_exact_operator_request_evidence(
    capacity_postgres_url: str,
) -> None:
    """Canonical abort evidence is independent of the caller's search path."""

    engine = create_engine(capacity_postgres_url)
    connection = engine.connect()
    transaction = connection.begin()
    try:
        connection.execute(text("CREATE SCHEMA capacity_prepared_abort_decoy"))
        connection.execute(
            text(
                "CREATE FUNCTION capacity_prepared_abort_decoy.sha256(bytea) "
                "RETURNS bytea LANGUAGE sql IMMUTABLE AS "
                "'SELECT decode(repeat(''00'', 32), ''hex'')'"
            )
        )
        connection.execute(
            text("SET LOCAL search_path TO capacity_prepared_abort_decoy, public, pg_catalog")
        )
        execution_epoch = _seed_prepared_execution(connection)
        authority = connection.execute(
            text("SELECT authority_incarnation FROM capacity_authority_state WHERE singleton_id=1")
        ).scalar_one()
        payload = {
            "schema_version": 2,
            "authority_incarnation": str(authority),
            "expected_writer_epoch": 1,
            "execution_epoch": execution_epoch,
            "execution_manifest_sha256": "4" * 64,
            "executable": True,
        }
        digest = hashlib.sha256(
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            ).encode("ascii")
        ).hexdigest()
        statement = text(
            "UPDATE capacity_execution_epochs SET state = 'retired', "
            "current_writer_epoch = 2, retirement_actor = 'preparation-operator', "
            "retirement_idempotency_key = :idempotency_key, "
            "retirement_request_digest = :digest, "
            "retirement_request_payload = CAST(:payload AS jsonb), retired_at = now() "
            "WHERE execution_epoch = :execution_epoch"
        )
        values = {
            "idempotency_key": UUID(int=12210),
            "digest": digest,
            "payload": json.dumps(payload),
            "execution_epoch": execution_epoch,
        }

        with pytest.raises(IntegrityError, match="retirement evidence is not exact"):
            with connection.begin_nested():
                connection.execute(statement, values | {"digest": "f" * 64})

        connection.execute(statement, values)
        connection.execute(
            text(
                "UPDATE capacity_authority_state SET writer_epoch = 2, "
                "execution_epoch = 0, execution_state = 'shadow', "
                "execution_manifest_sha256 = NULL, executable_new_capacity_ceiling = 0, "
                "increase_freeze = true, "
                "increase_freeze_reason = 'execution_preparation_aborted' "
                "WHERE singleton_id = 1"
            )
        )
        connection.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))

        assert connection.execute(
            text(
                "SELECT state, current_writer_epoch, retirement_actor, "
                "retirement_idempotency_key, retirement_request_digest, "
                "retirement_request_payload FROM capacity_execution_epochs "
                "WHERE execution_epoch = :execution_epoch"
            ),
            {"execution_epoch": execution_epoch},
        ).one() == (
            "retired",
            2,
            "preparation-operator",
            UUID(int=12210),
            digest,
            payload,
        )
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


@pytest.mark.parametrize(
    "missing_field",
    (
        "schema_version",
        "inventory_sequence",
        "executor_id",
        "executor_incarnation",
        "pool_id",
        "pool_generation",
        "journal_sequence",
        "journal_digest",
        "execution.execution_epoch",
        "execution.execution_manifest_sha256",
    ),
)
def test_executor_retirement_safety_rejects_missing_inventory_binding_fields(
    capacity_postgres_url: str,
    missing_field: str,
) -> None:
    """Missing JSON fields must not satisfy retirement safety by SQL UNKNOWN."""

    engine = create_engine(capacity_postgres_url)
    connection = engine.connect()
    transaction = connection.begin()
    try:
        execution_epoch = _seed_active_execution(connection)
        executor_incarnation = UUID(int=12011)
        payload: dict[str, object] = {
            "schema_version": 2,
            "execution": {
                "execution_epoch": execution_epoch,
                "execution_manifest_sha256": "4" * 64,
            },
            "executor_id": "gb10-executor",
            "executor_incarnation": str(executor_incarnation),
            "pool_id": "gb10",
            "pool_generation": 1,
            "inventory_sequence": 1,
            "journal_sequence": 0,
            "journal_digest": "0" * 64,
            "journal_checkpoint_sequence": 0,
            "journal_checkpoint_digest": "0" * 64,
            "complete": True,
            "records": [],
            "executable": True,
        }
        if missing_field.startswith("execution."):
            execution = payload["execution"]
            assert isinstance(execution, dict)
            execution.pop(missing_field.removeprefix("execution."))
        else:
            payload.pop(missing_field)

        with pytest.raises(IntegrityError):
            with connection.begin_nested():
                connection.execute(
                    text(
                        "INSERT INTO capacity_executable_executor_states "
                        "(id, execution_epoch, execution_manifest_sha256, executor_id, "
                        "executor_incarnation, pool_id, pool_generation, state, "
                        "journal_high_water, journal_digest, inventory_high_water, "
                        "last_inventory_digest, inventory_payload, "
                        "inventory_confirmation_journal_digest, last_inventory_at, "
                        "retirement_safe, retirement_inventory_digest, lease_expires_at, "
                        "last_heartbeat_at) VALUES "
                        "(:id, :execution_epoch, repeat('4', 64), 'gb10-executor', "
                        ":executor_incarnation, 'gb10', 1, 'current', 0, repeat('0', 64), "
                        "1, repeat('f', 64), CAST(:payload AS jsonb), "
                        "repeat('0', 64), CAST(:observed_at AS timestamptz), true, "
                        "repeat('f', 64), CAST(:observed_at AS timestamptz) + "
                        "interval '1 minute', CAST(:observed_at AS timestamptz) + "
                        "interval '1 second')"
                    ),
                    {
                        "id": uuid4(),
                        "execution_epoch": execution_epoch,
                        "executor_incarnation": executor_incarnation,
                        "payload": json.dumps(payload),
                        "observed_at": "2026-08-14T12:00:00+00:00",
                    },
                )
    finally:
        transaction.rollback()
        connection.close()
        engine.dispose()


def test_executor_retirement_safety_requires_post_inventory_heartbeat(
    capacity_postgres_url: str,
) -> None:
    """Direct SQL cannot mark safe until a heartbeat confirms after inventory time."""

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
                        "journal_high_water, journal_digest, inventory_high_water, "
                        "last_inventory_digest, inventory_payload, "
                        "inventory_confirmation_journal_digest, last_inventory_at, "
                        "retirement_safe, retirement_inventory_digest, lease_expires_at, "
                        "last_heartbeat_at) VALUES "
                        "(:id, :execution_epoch, repeat('4', 64), 'gb10-executor', "
                        ":executor_incarnation, 'gb10', 1, 'current', 0, repeat('0', 64), "
                        "1, repeat('f', 64), jsonb_build_object("
                        "'schema_version', 2, "
                        "'execution', jsonb_build_object("
                        "'execution_epoch', :execution_epoch, "
                        "'execution_manifest_sha256', repeat('4', 64)), "
                        "'executor_id', 'gb10-executor', "
                        "'executor_incarnation', CAST(:executor_incarnation AS text), "
                        "'pool_id', 'gb10', 'pool_generation', 1, "
                        "'inventory_sequence', 1, "
                        "'journal_sequence', 0, 'journal_digest', repeat('0', 64), "
                        "'journal_checkpoint_sequence', 0, "
                        "'journal_checkpoint_digest', repeat('0', 64), "
                        "'complete', true, 'records', '[]'::jsonb, 'executable', true), "
                        "repeat('0', 64), CAST(:observed_at AS timestamptz), true, "
                        "repeat('f', 64), CAST(:observed_at AS timestamptz) + "
                        "interval '1 minute', CAST(:observed_at AS timestamptz))"
                    ),
                    {
                        "id": uuid4(),
                        "execution_epoch": execution_epoch,
                        "executor_incarnation": UUID(int=12011),
                        "observed_at": "2026-08-14T12:00:00+00:00",
                    },
                )
    finally:
        transaction.rollback()
        connection.close()
        engine.dispose()


