from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
import rfc8785
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError, IntegrityError

from loom.db import schema

NOW = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
GRANT_ID = UUID("11111111-1111-1111-1111-111111111111")
PROJECTION_ID = UUID("22222222-2222-2222-2222-222222222222")
SESSION_ID = UUID("33333333-3333-3333-3333-333333333333")
ATTESTATION_ID = UUID("44444444-4444-4444-4444-444444444444")
TOKEN = "loom_tibs_" + "A" * 64
TOKEN_HASH = hashlib.sha256(TOKEN.encode("ascii")).digest()
ATTESTATION_SHA256 = "7" * 64


def _config(postgres_url: str) -> Config:
    config = Config("migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url)
    return config


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _session_public_binding() -> dict[str, object]:
    return {
        "schema_version": 1,
        "grant_id": str(GRANT_ID),
        "session_id": str(SESSION_ID),
        "purpose": "production",
        "shadow_campaign_id": None,
        "pool_id": "staging-gb10-task-image",
        "cpu_arch": "arm64",
        "attestation_generation": 1,
        "attestation_sha256": ATTESTATION_SHA256,
        "issued_at": _timestamp(NOW + timedelta(seconds=6)),
        "expires_at": _timestamp(NOW + timedelta(seconds=36)),
        "session_token_sha256": TOKEN_HASH.hex(),
    }


def _insert_exchanged_projection(
    connection: Connection,
    *,
    authority_version: int = 1,
) -> dict[str, object]:
    session_json = _session_public_binding()
    session_sha256 = hashlib.sha256(rfc8785.dumps(session_json)).hexdigest()
    authority = {
        "schema_version": authority_version,
        "purpose": "production",
        "shadow_campaign_id": None,
        "environment": "staging",
        "pool_id": "staging-gb10-task-image",
        "slurm_cluster_id": "gb10",
        "cpu_arch": "arm64",
        "slurm_request_sha256": "1" * 64,
        "builder_release_sha256": ("6" * 64 if authority_version == 1 else "a" * 64),
        "build_policy_sha256": "3" * 64,
        "containment_policy_sha256": "4" * 64,
        "resource_profile_sha256": "5" * 64,
        "issued_at": _timestamp(NOW - timedelta(minutes=1)),
        "expires_at": _timestamp(NOW + timedelta(hours=2)),
    }
    if authority_version == 2:
        authority["supervisor_executable_sha256"] = "6" * 64
    connection.execute(
        text(
            """
            INSERT INTO task_image_build_grants (
                id, environment, provider, slurm_cluster_id, cpu_arch, state,
                submitting_identity, slurm_account, slurm_partition, slurm_qos,
                request_spec, request_sha256, authority_spec, authority_sha256,
                grant_expires_at, slurm_comment, ambiguity_settle_seconds,
                ambiguity_settle_until, invocation_started_at, slurm_job_id,
                journal_sequence, bound_at, released_at
            ) VALUES (
                :grant_id, 'staging', 'slurm-rootless-v1', 'gb10', 'arm64',
                'released', 'loom-builder', 'loom-task-builder',
                'loom-task-builder', 'loom-task-image-builder-rootless-gb10',
                '{}'::jsonb, :request_sha256, CAST(:authority AS jsonb),
                :authority_sha256, :grant_expires_at,
                'loom-task-builder-v1:grant=' || CAST(:grant_id AS text),
                30, :settle_until, :invocation_started_at, '12345', 0,
                :bound_at, :released_at
            )
            """
        ),
        {
            "grant_id": GRANT_ID,
            "request_sha256": "1" * 64,
            "authority": json.dumps(authority),
            "authority_sha256": hashlib.sha256(rfc8785.dumps(authority)).hexdigest(),
            "grant_expires_at": NOW + timedelta(hours=2),
            "settle_until": NOW + timedelta(seconds=30),
            "invocation_started_at": NOW,
            "bound_at": NOW + timedelta(seconds=1),
            "released_at": NOW + timedelta(seconds=2),
        },
    )
    connection.execute(
        text(
            """
            INSERT INTO task_image_build_projections (
                id, grant_id, state, principal_id, principal_sha256,
                request_id, request_json, request_sha256, node_name,
                node_boot_id, slurm_cluster_id, slurm_job_id, supervisor_pid,
                supervisor_uid, supervisor_gid, supervisor_executable_sha256,
                cgroup_path, cgroup_inode, challenge_nonce, challenge_json,
                challenge_sha256, challenge_issued_at, challenge_expires_at,
                proof_id, proof_json, proof_sha256, bootstrap_token_hash,
                bootstrap_secret_ref, bootstrap_issued_at, bootstrap_expires_at,
                exchange_id, exchange_json, exchange_sha256, session_id,
                session_token_hash, session_secret_ref, session_json,
                session_sha256, session_issued_at, session_expires_at,
                attestation_generation, attestation_sha256,
                attestation_expires_at, event_sequence
            ) VALUES (
                :projection_id, :grant_id, 'exchanged', 'gb10-trt-gb10-1',
                :principal_sha256, :request_id, '{}'::jsonb, :request_sha256,
                'trt-gb10-1', :node_boot_id, 'gb10', '12345', 42100, 993, 980,
                :supervisor_sha256,
                '/sys/fs/cgroup/system.slice/slurmstepd.scope/job_12345/step_batch',
                987654, :challenge_nonce, '{}'::jsonb, :challenge_sha256,
                :challenge_issued_at, :challenge_expires_at, :proof_id,
                '{}'::jsonb, :proof_sha256, :bootstrap_token_hash,
                'loom://task-image-bootstrap/bootstrap-one',
                :bootstrap_issued_at, :bootstrap_expires_at, :exchange_id,
                '{}'::jsonb, :exchange_sha256, :session_id,
                :session_token_hash, 'loom://task-image-session/session-one',
                CAST(:session_json AS jsonb), :session_sha256,
                :session_issued_at, :session_expires_at, 1,
                :attestation_sha256, :attestation_expires_at, 3
            )
            """
        ),
        {
            "projection_id": PROJECTION_ID,
            "grant_id": GRANT_ID,
            "principal_sha256": "a" * 64,
            "request_id": UUID("55555555-5555-5555-5555-555555555555"),
            "request_sha256": "b" * 64,
            "node_boot_id": UUID("66666666-6666-6666-6666-666666666666"),
            "supervisor_sha256": "6" * 64,
            "challenge_nonce": UUID("77777777-7777-7777-7777-777777777777"),
            "challenge_sha256": "c" * 64,
            "challenge_issued_at": NOW + timedelta(seconds=3),
            "challenge_expires_at": NOW + timedelta(seconds=63),
            "proof_id": ATTESTATION_ID,
            "proof_sha256": "d" * 64,
            "bootstrap_token_hash": hashlib.sha256(b"bootstrap").digest(),
            "bootstrap_issued_at": NOW + timedelta(seconds=5),
            "bootstrap_expires_at": NOW + timedelta(seconds=40),
            "exchange_id": UUID("88888888-8888-8888-8888-888888888888"),
            "exchange_sha256": "e" * 64,
            "session_id": SESSION_ID,
            "session_token_hash": TOKEN_HASH,
            "session_json": json.dumps(session_json),
            "session_sha256": session_sha256,
            "session_issued_at": NOW + timedelta(seconds=6),
            "session_expires_at": NOW + timedelta(seconds=36),
            "attestation_sha256": ATTESTATION_SHA256,
            "attestation_expires_at": NOW + timedelta(seconds=40),
        },
    )
    connection.execute(
        text(
            """
            INSERT INTO task_image_build_containment_attestations (
                id, grant_id, generation, attestation_json,
                attestation_sha256, issued_at, expires_at, recorded_at
            ) VALUES (
                :id, :grant_id, 1, '{}'::jsonb, :sha256,
                :issued_at, :expires_at, :recorded_at
            )
            """
        ),
        {
            "id": ATTESTATION_ID,
            "grant_id": GRANT_ID,
            "sha256": ATTESTATION_SHA256,
            "issued_at": NOW + timedelta(seconds=5),
            "expires_at": NOW + timedelta(seconds=40),
            "recorded_at": NOW + timedelta(seconds=5),
        },
    )
    return {"session_json": session_json, "session_sha256": session_sha256}


@pytest.mark.parametrize("authority_version", [1, 2])
def test_0129_backfills_one_exact_immutable_session_generation(
    isolated_migration_postgres_url: str,
    authority_version: int,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0128")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            expected = _insert_exchanged_projection(
                connection,
                authority_version=authority_version,
            )

        command.upgrade(config, "0129")
        inspector = inspect(engine)
        assert schema.TaskImageBuildSessionGeneration.__tablename__ in set(
            inspector.get_table_names()
        )
        assert "session_generation" in {
            item["name"] for item in inspector.get_columns("task_image_build_projections")
        }
        assert set(schema.TaskImageBuildSessionGeneration.__table__.columns.keys()) == {
            item["name"] for item in inspector.get_columns("task_image_build_session_generations")
        }
        uniques = {
            item["name"]: tuple(item["column_names"])
            for item in inspector.get_unique_constraints("task_image_build_session_generations")
        }
        assert uniques == {
            "task_image_build_session_generations_current_uidx": (
                "grant_id",
                "generation",
                "session_id",
            ),
            "task_image_build_session_generations_generation_uidx": (
                "grant_id",
                "generation",
            ),
            "task_image_build_session_generations_renewal_uidx": ("renewal_id",),
            "task_image_build_session_generations_session_uidx": ("session_id",),
        }
        foreign_keys = {
            item["name"]: item
            for item in inspector.get_foreign_keys("task_image_build_session_generations")
        }
        assert set(foreign_keys) == {
            "task_image_build_session_generations_attestation_fkey",
            "task_image_build_session_generations_predecessor_fkey",
            "task_image_build_session_generations_projection_fkey",
        }
        assert all(item["options"]["ondelete"] == "RESTRICT" for item in foreign_keys.values())

        with engine.connect() as connection:
            projection = (
                connection.execute(
                    text(
                        "SELECT session_generation FROM task_image_build_projections "
                        "WHERE grant_id = :grant_id"
                    ),
                    {"grant_id": GRANT_ID},
                )
                .mappings()
                .one()
            )
            generation = (
                connection.execute(
                    text(
                        "SELECT * FROM task_image_build_session_generations "
                        "WHERE grant_id = :grant_id"
                    ),
                    {"grant_id": GRANT_ID},
                )
                .mappings()
                .one()
            )
        assert projection["session_generation"] == 1
        assert generation["generation"] == 1
        assert generation["session_id"] == SESSION_ID
        assert bytes(generation["session_token_hash"]) == TOKEN_HASH
        assert generation["session_secret_ref"] == "loom://task-image-session/session-one"
        assert generation["session_json"] == expected["session_json"]
        assert generation["session_sha256"] == expected["session_sha256"]
        assert generation["attestation_generation"] == 1
        assert generation["attestation_sha256"] == ATTESTATION_SHA256
        assert generation["renewal_id"] is None
        assert generation["renewal_sha256"] is None
        assert generation["predecessor_session_id"] is None

        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "UPDATE task_image_build_session_generations "
                        "SET renewal_id = :renewal_id WHERE grant_id = :grant_id"
                    ),
                    {"renewal_id": UUID(int=9), "grant_id": GRANT_ID},
                )

        command.downgrade(config, "0128")
        downgraded = inspect(engine)
        assert "task_image_build_session_generations" not in set(downgraded.get_table_names())
        assert "session_generation" not in {
            item["name"] for item in downgraded.get_columns("task_image_build_projections")
        }
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "missing_column",
    [
        "session_id",
        "session_token_hash",
        "session_secret_ref",
        "session_json",
        "session_sha256",
        "session_issued_at",
        "session_expires_at",
        "attestation_generation",
        "attestation_sha256",
    ],
)
def test_0129_aborts_incomplete_legacy_session_before_backfill(
    isolated_migration_postgres_url: str,
    missing_column: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0128")
    engine = create_engine(isolated_migration_postgres_url)
    allowed_columns = {
        "session_id",
        "session_token_hash",
        "session_secret_ref",
        "session_json",
        "session_sha256",
        "session_issued_at",
        "session_expires_at",
        "attestation_generation",
        "attestation_sha256",
    }
    assert missing_column in allowed_columns
    try:
        with engine.begin() as connection:
            _insert_exchanged_projection(connection)
            connection.execute(
                text(
                    "ALTER TABLE task_image_build_projections DROP CONSTRAINT "
                    "task_image_build_projections_state_fields_check"
                )
            )
            connection.execute(
                text(
                    f"UPDATE task_image_build_projections SET {missing_column} = NULL "
                    "WHERE grant_id = :grant_id"
                ),
                {"grant_id": GRANT_ID},
            )

        with pytest.raises(DBAPIError, match="contradictory legacy task-image session"):
            command.upgrade(config, "0129")
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("json_path", "changed_value"),
    [
        ("session_id", "99999999-9999-9999-9999-999999999999"),
        ("attestation_sha256", "9" * 64),
        ("session_token_sha256", "8" * 64),
        ("expires_at", _timestamp(NOW + timedelta(seconds=37))),
    ],
)
def test_0129_aborts_instead_of_inventing_a_contradictory_legacy_session(
    isolated_migration_postgres_url: str,
    json_path: str,
    changed_value: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0128")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            _insert_exchanged_projection(connection)
            connection.execute(
                text(
                    "UPDATE task_image_build_projections "
                    "SET session_json = jsonb_set(session_json, CAST(:path AS text[]), "
                    "to_jsonb(CAST(:value AS text)), false) "
                    "WHERE grant_id = :grant_id"
                ),
                {
                    "path": [json_path],
                    "value": changed_value,
                    "grant_id": GRANT_ID,
                },
            )

        with pytest.raises(DBAPIError, match="contradictory legacy task-image session"):
            command.upgrade(config, "0129")
    finally:
        engine.dispose()


def test_0129_adds_legacy_compatible_session_attempt_and_operation_authority(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0128")
    engine = create_engine(isolated_migration_postgres_url)
    materialization_id = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    legacy_attempt_id = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    bound_attempt_id = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
    operation_id = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
    try:
        with engine.begin() as connection:
            _insert_exchanged_projection(connection, authority_version=2)
            connection.execute(
                text(
                    """
                    INSERT INTO task_image_materializations (
                        id, materialization_key, task_id, task_checksum,
                        cpu_arch, task_config
                    ) VALUES (
                        :id, :key, 'phase2c-migration', :checksum,
                        'arm64', '{}'::jsonb
                    )
                    """
                ),
                {"id": materialization_id, "key": "8" * 64, "checksum": "9" * 64},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO task_image_materialization_attempts (
                        id, materialization_id, attempt_number, lease_epoch,
                        builder_id, claimed_at
                    ) VALUES (
                        :id, :materialization_id, 1, 1, 'legacy-builder', :now
                    )
                    """
                ),
                {
                    "id": legacy_attempt_id,
                    "materialization_id": materialization_id,
                    "now": NOW,
                },
            )

        command.upgrade(config, "0129")

        with engine.begin() as connection:
            connection.execute(
                    text(
                        """
                        INSERT INTO task_image_materialization_attempts (
                            id, materialization_id, attempt_number, lease_epoch,
                            builder_id, claimed_at, grant_id, session_id,
                            session_generation, claim_id,
                            claim_deterministic_failure_count,
                            claim_lease_expires_at, claim_plan_json,
                            claim_plan_sha256
                        ) VALUES (
                            :attempt_id, :materialization_id, 2, 2,
                            'rootless:' || replace(CAST(:session_id AS text), '-', ''),
                            :now, :grant_id, :session_id, 1, :claim_id,
                            0, :lease_expires_at, CAST(:plan_json AS jsonb), :plan_sha256
                        )
                        """
                    ),
                    {
                        "attempt_id": bound_attempt_id,
                        "materialization_id": materialization_id,
                        "now": NOW,
                        "grant_id": GRANT_ID,
                        "session_id": SESSION_ID,
                        "claim_id": UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"),
                        "lease_expires_at": NOW + timedelta(minutes=1),
                        "plan_json": json.dumps({"plan": True}),
                        "plan_sha256": "f" * 64,
                    },
            )

        with pytest.raises(IntegrityError, match="task_image_materialization_operation_events_secret_check"):
            with engine.begin() as connection:
                connection.execute(
                    text(
                        """
                        INSERT INTO task_image_materialization_operation_events (
                            operation_id, operation_type,
                            materialization_attempt_id, materialization_id,
                            attempt_number, lease_epoch, builder_id, grant_id,
                            session_id, session_generation, result_state,
                            result_attempt_count, result_lease_expires_at,
                            secret_response_ref, secret_response_sha256,
                            recorded_at
                        ) VALUES (
                            :operation_id, 'bundle', :attempt_id, :materialization_id,
                            2, 2,
                            'rootless:' || replace(CAST(:session_id AS text), '-', ''),
                            :grant_id, :session_id, 1, 'running', 2,
                            :lease_expires_at,
                            'loom://task-image-bundle-capability/capability-one',
                            :secret_sha256, :recorded_at
                        )
                        """
                    ),
                    {
                        "operation_id": operation_id,
                        "attempt_id": bound_attempt_id,
                        "materialization_id": materialization_id,
                        "grant_id": GRANT_ID,
                        "session_id": SESSION_ID,
                        "lease_expires_at": NOW + timedelta(minutes=1),
                        "secret_sha256": "a" * 64,
                        "recorded_at": NOW,
                    },
                )

        inspector = inspect(engine)
        attempt_columns = {
            item["name"] for item in inspector.get_columns("task_image_materialization_attempts")
        }
        assert {
            "grant_id",
            "session_id",
            "session_generation",
            "claim_id",
            "claim_deterministic_failure_count",
            "claim_lease_expires_at",
            "claim_plan_json",
            "claim_plan_sha256",
        } <= attempt_columns
        attempt_checks = {
            item["name"]
            for item in inspector.get_check_constraints("task_image_materialization_attempts")
        }
        assert {
            "task_image_materialization_attempts_session_fields_check",
            "task_image_materialization_attempts_claim_id_check",
        } <= attempt_checks
        attempt_foreign_keys = {
            item["name"]: item
            for item in inspector.get_foreign_keys("task_image_materialization_attempts")
        }
        session_binding = attempt_foreign_keys["task_image_materialization_attempts_session_fkey"]
        assert session_binding["constrained_columns"] == [
            "grant_id",
            "session_generation",
            "session_id",
        ]
        assert session_binding["referred_columns"] == [
            "grant_id",
            "generation",
            "session_id",
        ]
        assert session_binding["options"]["ondelete"] == "RESTRICT"

        assert "task_image_materialization_operation_events" in set(inspector.get_table_names())
        operation_uniques = {
            item["name"]: tuple(item["column_names"])
            for item in inspector.get_unique_constraints(
                "task_image_materialization_operation_events"
            )
        }
        assert operation_uniques["task_image_materialization_operation_events_operation_uidx"] == (
            "operation_id",
        )

        with engine.connect() as connection:
            legacy = (
                connection.execute(
                    text(
                        """
                        SELECT grant_id, session_id, session_generation, claim_id,
                               claim_deterministic_failure_count,
                               claim_lease_expires_at, claim_plan_json,
                               claim_plan_sha256
                          FROM task_image_materialization_attempts
                         WHERE id = :id
                        """
                    ),
                    {"id": legacy_attempt_id},
                )
                .mappings()
                .one()
            )
        assert dict(legacy) == {
            "grant_id": None,
            "session_id": None,
            "session_generation": None,
            "claim_id": None,
            "claim_deterministic_failure_count": None,
            "claim_lease_expires_at": None,
            "claim_plan_json": None,
            "claim_plan_sha256": None,
        }

        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    text(
                        """
                        UPDATE task_image_materialization_attempts
                           SET grant_id = :grant_id
                         WHERE id = :id
                        """
                    ),
                    {"grant_id": GRANT_ID, "id": legacy_attempt_id},
                )

        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    text(
                        """
                        UPDATE task_image_materialization_attempts
                           SET grant_id = :grant_id,
                               session_id = :session_id,
                               session_generation = 1,
                               claim_id = '00000000-0000-0000-0000-000000000000'
                         WHERE id = :id
                        """
                    ),
                    {
                        "grant_id": GRANT_ID,
                        "session_id": SESSION_ID,
                        "id": legacy_attempt_id,
                    },
                )

        command.downgrade(config, "0128")
        downgraded = inspect(engine)
        assert "task_image_materialization_operation_events" not in set(
            downgraded.get_table_names()
        )
        assert not {
            "grant_id",
            "session_id",
            "session_generation",
            "claim_id",
            "claim_deterministic_failure_count",
            "claim_lease_expires_at",
            "claim_plan_json",
            "claim_plan_sha256",
        } & {item["name"] for item in downgraded.get_columns("task_image_materialization_attempts")}
    finally:
        engine.dispose()
