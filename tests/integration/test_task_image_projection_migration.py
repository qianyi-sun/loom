from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError, IntegrityError

from loom.db import schema

NOW = datetime(2026, 9, 2, 14, 0, tzinfo=UTC)
GRANT_ID = UUID("11111111-1111-1111-1111-111111111111")


def _config(postgres_url: str) -> Config:
    config = Config("migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url)
    return config


def _insert_grant(connection: Connection, *, with_authority: bool) -> None:
    columns = ""
    values = ""
    parameters: dict[str, object] = {
        "id": GRANT_ID,
        "request_spec": json.dumps(
            {
                "schema": "loom.task-image-build-grant/v1",
                "request_sha256": "1" * 64,
            }
        ),
    }
    if with_authority:
        columns = ", authority_spec, authority_sha256, grant_expires_at"
        values = ", CAST(:authority_spec AS jsonb), :authority_sha256, :grant_expires_at"
        parameters.update(
            {
                "authority_spec": json.dumps(
                    {
                        "schema_version": 1,
                        "purpose": "production",
                        "shadow_campaign_id": None,
                        "environment": "staging",
                        "pool_id": "staging-gb10-task-image",
                        "slurm_cluster_id": "gb10",
                        "cpu_arch": "arm64",
                        "slurm_request_sha256": "1" * 64,
                        "builder_release_sha256": "2" * 64,
                        "build_policy_sha256": "3" * 64,
                        "containment_policy_sha256": "4" * 64,
                        "resource_profile_sha256": "5" * 64,
                        "issued_at": NOW.isoformat(),
                        "expires_at": (NOW + timedelta(hours=2)).isoformat(),
                    }
                ),
                "authority_sha256": "a" * 64,
                "grant_expires_at": NOW + timedelta(hours=2),
            }
        )
    connection.execute(
        text(f"""
            INSERT INTO task_image_build_grants (
                id, environment, provider, slurm_cluster_id, cpu_arch,
                state, submitting_identity, slurm_account, slurm_partition,
                slurm_qos, request_spec, request_sha256, slurm_comment,
                ambiguity_settle_seconds, journal_sequence{columns}
            ) VALUES (
                :id, 'staging', 'slurm-rootless-v1', 'gb10', 'arm64',
                'issued', 'loom-builder', 'loom-task-builder',
                'loom-task-builder', 'loom-task-image-builder-rootless-gb10',
                CAST(:request_spec AS jsonb), '{"1" * 64}',
                'loom-task-builder-v1:grant=' || CAST(:id AS text), 30, 0{values}
            )
        """),
        parameters,
    )


def _insert_challenged_projection(connection: Connection, *, projection_id: UUID) -> None:
    connection.execute(
        text("""
            INSERT INTO task_image_build_projections (
                id, grant_id, state, principal_id, principal_sha256,
                request_id, request_json, request_sha256,
                node_name, node_boot_id, slurm_cluster_id, slurm_job_id,
                supervisor_pid, supervisor_uid, supervisor_gid,
                supervisor_executable_sha256, cgroup_path, cgroup_inode,
                challenge_nonce, challenge_json, challenge_sha256,
                challenge_issued_at, challenge_expires_at, event_sequence
            ) VALUES (
                :id, :grant_id, 'challenged', 'gb10-trt-gb10-1', :digest,
                :request_id, '{}'::jsonb, :digest,
                'trt-gb10-1', :node_boot_id, 'gb10', '12345',
                42100, 993, 980, :digest,
                '/sys/fs/cgroup/system.slice/slurmstepd.scope/job_12345/step_batch',
                987654, :challenge_nonce, '{}'::jsonb, :digest,
                :issued_at, :expires_at, 0
            )
        """),
        {
            "id": projection_id,
            "grant_id": GRANT_ID,
            "digest": "b" * 64,
            "request_id": uuid4(),
            "node_boot_id": uuid4(),
            "challenge_nonce": uuid4(),
            "issued_at": NOW,
            "expires_at": NOW + timedelta(seconds=60),
        },
    )


def test_0124_adds_fail_closed_projection_authority_and_round_trips(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0123")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            _insert_grant(connection, with_authority=False)

        with pytest.raises(
            DBAPIError,
            match="unexpected pre-authority task-image build grants",
        ):
            command.upgrade(config, "0124")

        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM task_image_build_grants WHERE id = :id"),
                {"id": GRANT_ID},
            )

        command.upgrade(config, "0124")
        inspector = inspect(engine)
        tables = set(inspector.get_table_names())
        assert {
            "task_image_build_projections",
            "task_image_build_projection_events",
            "task_image_build_containment_attestations",
        } <= tables
        assert {
            schema.TaskImageBuildProjection.__tablename__,
            schema.TaskImageBuildProjectionEvent.__tablename__,
            schema.TaskImageBuildContainmentAttestation.__tablename__,
        } <= tables
        for model in (
            schema.TaskImageBuildProjection,
            schema.TaskImageBuildProjectionEvent,
            schema.TaskImageBuildContainmentAttestation,
        ):
            assert set(model.__table__.columns.keys()) == {
                item["name"] for item in inspector.get_columns(model.__tablename__)
            }

        grant_columns = {
            item["name"] for item in inspector.get_columns("task_image_build_grants")
        }
        assert {"authority_spec", "authority_sha256", "grant_expires_at"} <= grant_columns
        grant_checks = {
            item["name"]
            for item in inspector.get_check_constraints("task_image_build_grants")
        }
        assert "task_image_build_grants_authority_check" in grant_checks

        projection_checks = {
            item["name"]
            for item in inspector.get_check_constraints("task_image_build_projections")
        }
        assert {
            "task_image_build_projections_identity_check",
            "task_image_build_projections_state_check",
            "task_image_build_projections_state_fields_check",
            "task_image_build_projections_terminal_check",
            "task_image_build_projections_time_check",
        } <= projection_checks
        projection_uniques = {
            item["name"]: tuple(item["column_names"])
            for item in inspector.get_unique_constraints("task_image_build_projections")
        }
        assert projection_uniques == {
            "task_image_build_projections_exchange_uidx": ("grant_id", "exchange_id"),
            "task_image_build_projections_grant_uidx": ("grant_id",),
            "task_image_build_projections_proof_uidx": ("grant_id", "proof_id"),
            "task_image_build_projections_request_uidx": ("grant_id", "request_id"),
            "task_image_build_projections_session_uidx": ("session_id",),
        }
        projection_fks = {
            item["name"]: item
            for item in inspector.get_foreign_keys("task_image_build_projections")
        }
        assert projection_fks["task_image_build_projections_grant_fkey"][
            "options"
        ]["ondelete"] == "RESTRICT"

        event_checks = {
            item["name"]
            for item in inspector.get_check_constraints("task_image_build_projection_events")
        }
        assert {
            "task_image_build_projection_events_key_check",
            "task_image_build_projection_events_sequence_check",
            "task_image_build_projection_events_type_check",
        } <= event_checks
        event_indexes = {
            item["name"]
            for item in inspector.get_indexes("task_image_build_projection_events")
        }
        assert "task_image_build_projection_events_created_idx" in event_indexes
        event_uniques = {
            item["name"]: tuple(item["column_names"])
            for item in inspector.get_unique_constraints(
                "task_image_build_projection_events"
            )
        }
        assert event_uniques == {
            "task_image_build_projection_events_sequence_uidx": (
                "grant_id",
                "event_sequence",
            ),
            "task_image_build_projection_events_type_key_uidx": (
                "grant_id",
                "event_type",
                "event_key",
            ),
        }
        event_fks = {
            item["name"]: item
            for item in inspector.get_foreign_keys(
                "task_image_build_projection_events"
            )
        }
        assert event_fks["task_image_build_projection_events_projection_fkey"][
            "options"
        ]["ondelete"] == "RESTRICT"

        attestation_checks = {
            item["name"]
            for item in inspector.get_check_constraints(
                "task_image_build_containment_attestations"
            )
        }
        assert {
            "task_image_build_containment_attestations_digest_check",
            "task_image_build_containment_attestations_generation_check",
            "task_image_build_containment_attestations_time_check",
        } <= attestation_checks
        attestation_uniques = {
            item["name"]: tuple(item["column_names"])
            for item in inspector.get_unique_constraints(
                "task_image_build_containment_attestations"
            )
        }
        assert attestation_uniques == {
            "task_image_build_containment_attestations_generation_uidx": (
                "grant_id",
                "generation",
            )
        }
        attestation_fks = {
            item["name"]: item
            for item in inspector.get_foreign_keys(
                "task_image_build_containment_attestations"
            )
        }
        assert attestation_fks[
            "task_image_build_containment_attestations_projection_fkey"
        ]["options"]["ondelete"] == "RESTRICT"

        projection_indexes = {
            item["name"] for item in inspector.get_indexes("task_image_build_projections")
        }
        assert {
            "task_image_build_projections_active_session_idx",
            "task_image_build_projections_attestation_expiry_idx",
        } <= projection_indexes
        attestation_indexes = {
            item["name"]
            for item in inspector.get_indexes(
                "task_image_build_containment_attestations"
            )
        }
        assert "task_image_build_containment_attestations_expiry_idx" in (
            attestation_indexes
        )

        with engine.begin() as connection:
            _insert_grant(connection, with_authority=True)
            _insert_challenged_projection(connection, projection_id=uuid4())

        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                _insert_challenged_projection(connection, projection_id=uuid4())

        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    text("""
                        UPDATE task_image_build_projections
                        SET session_json = '{}'::jsonb,
                            session_sha256 = :digest
                        WHERE grant_id = :grant_id
                    """),
                    {"digest": "d" * 64, "grant_id": GRANT_ID},
                )

        with engine.begin() as connection:
            for generation in (1, 2):
                connection.execute(
                    text("""
                        INSERT INTO task_image_build_containment_attestations (
                            id, grant_id, generation, attestation_json,
                            attestation_sha256, issued_at, expires_at
                        ) VALUES (
                            :id, :grant_id, :generation, '{}'::jsonb,
                            :digest, :issued_at, :expires_at
                        )
                    """),
                    {
                        "id": uuid4(),
                        "grant_id": GRANT_ID,
                        "generation": generation,
                        "digest": f"{generation}" * 64,
                        "issued_at": NOW + timedelta(seconds=generation),
                        "expires_at": NOW + timedelta(seconds=30 + generation),
                    },
                )

        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    text("""
                        INSERT INTO task_image_build_containment_attestations (
                            id, grant_id, generation, attestation_json,
                            attestation_sha256, issued_at, expires_at
                        ) VALUES (
                            :id, :grant_id, 2, '{}'::jsonb, :digest,
                            :issued_at, :expires_at
                        )
                    """),
                    {
                        "id": uuid4(),
                        "grant_id": GRANT_ID,
                        "digest": "c" * 64,
                        "issued_at": NOW + timedelta(seconds=2),
                        "expires_at": NOW + timedelta(seconds=32),
                    },
                )

        command.downgrade(config, "0123")
        downgraded = inspect(engine)
        assert not {
            "task_image_build_projections",
            "task_image_build_projection_events",
            "task_image_build_containment_attestations",
        } & set(downgraded.get_table_names())
        grant_columns = {
            item["name"] for item in downgraded.get_columns("task_image_build_grants")
        }
        assert not {"authority_spec", "authority_sha256", "grant_expires_at"} & (
            grant_columns
        )
    finally:
        engine.dispose()
