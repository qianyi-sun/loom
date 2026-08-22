from __future__ import annotations

import json
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import IntegrityError

from loom.db.schema import (
    TaskImageBuildGrant,
    TaskImageBuildGrantEvent,
    TaskImageMaterializationAttempt,
    TaskImagePublicationEvidence,
)


def _config(postgres_url: str) -> Config:
    config = Config("migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url)
    return config


def test_0108_adds_durable_grant_attempt_and_publication_ledgers(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0107")
    command.upgrade(config, "0108")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        tables = set(inspect(engine).get_table_names())
        assert {
            "task_image_build_grants",
            "task_image_build_grant_events",
            "task_image_materialization_attempts",
            "task_image_publication_evidence",
        } <= tables
        assert {
            TaskImageBuildGrant.__tablename__,
            TaskImageBuildGrantEvent.__tablename__,
            TaskImageMaterializationAttempt.__tablename__,
            TaskImagePublicationEvidence.__tablename__,
        } <= tables
        inspector = inspect(engine)
        grant_checks = {
            item["name"] for item in inspector.get_check_constraints("task_image_build_grants")
        }
        assert {
            "task_image_build_grants_native_check",
            "task_image_build_grants_settle_check",
            "task_image_build_grants_state_check",
            "task_image_build_grants_state_fields_check",
        } <= grant_checks
        grant_indexes = {
            item["name"]: item for item in inspector.get_indexes("task_image_build_grants")
        }
        assert grant_indexes["task_image_build_grants_job_uidx"]["unique"] is True
        evidence_foreign_keys = {
            item["name"]: item
            for item in inspector.get_foreign_keys("task_image_publication_evidence")
        }
        attempt_binding = evidence_foreign_keys[
            "task_image_publication_evidence_attempt_fkey"
        ]
        assert attempt_binding["constrained_columns"] == [
            "materialization_attempt_id",
            "materialization_id",
            "attempt_number",
            "lease_epoch",
            "builder_id",
        ]
        assert attempt_binding["referred_columns"] == [
            "id",
            "materialization_id",
            "attempt_number",
            "lease_epoch",
            "builder_id",
        ]

        materialization_id = uuid4()
        attempt_ids = (uuid4(), uuid4())
        digest = "registry.example/loom/fixture@sha256:" + "a" * 64
        with engine.begin() as connection:
            connection.execute(
                text("""
                    INSERT INTO task_image_materializations (
                        id, materialization_key, task_id, task_checksum,
                        cpu_arch, task_config
                    ) VALUES (
                        :id, :key, 'phase2-fixture', :checksum, 'arm64', '{}'::jsonb
                    )
                """),
                {"id": materialization_id, "key": "b" * 64, "checksum": "c" * 64},
            )
            for attempt_id, attempt_number, lease_epoch in (
                (attempt_ids[0], 1, 1),
                (attempt_ids[1], 1, 3),
            ):
                connection.execute(
                    text("""
                        INSERT INTO task_image_materialization_attempts (
                            id, materialization_id, attempt_number, lease_epoch,
                            builder_id, claimed_at
                        ) VALUES (
                            :id, :materialization_id, :attempt_number, :lease_epoch,
                            :builder_id, now()
                        )
                    """),
                    {
                        "id": attempt_id,
                        "materialization_id": materialization_id,
                        "attempt_number": attempt_number,
                        "lease_epoch": lease_epoch,
                        "builder_id": f"builder:{lease_epoch}",
                    },
                )
                connection.execute(
                    text("""
                        INSERT INTO task_image_publication_evidence (
                            materialization_attempt_id, materialization_id,
                            attempt_number, lease_epoch, builder_id, component,
                            registry_image, recorded_at
                        ) VALUES (
                            :attempt_id, :materialization_id, 1, :lease_epoch,
                            :builder_id, 'task', :digest, now()
                        )
                    """),
                    {
                        "attempt_id": attempt_id,
                        "materialization_id": materialization_id,
                        "lease_epoch": lease_epoch,
                        "builder_id": f"builder:{lease_epoch}",
                        "digest": digest,
                    },
                )

        with engine.connect() as connection:
            rows = connection.execute(
                text("""
                    SELECT attempt_number, lease_epoch, registry_image
                      FROM task_image_publication_evidence
                     WHERE materialization_id = :materialization_id
                     ORDER BY lease_epoch
                """),
                {"materialization_id": materialization_id},
            ).all()
        assert rows == [(1, 1, digest), (1, 3, digest)]

        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    text("""
                        INSERT INTO task_image_publication_evidence (
                            materialization_attempt_id, materialization_id,
                            attempt_number, lease_epoch, builder_id, component,
                            registry_image, recorded_at
                        ) VALUES (
                            :attempt_id, :materialization_id, 1, 3,
                            'builder:3', 'task', :digest, now()
                        )
                    """),
                    {
                        "attempt_id": attempt_ids[1],
                        "materialization_id": materialization_id,
                        "digest": digest,
                    },
                )

        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    text("""
                        INSERT INTO task_image_publication_evidence (
                            materialization_attempt_id, materialization_id,
                            attempt_number, lease_epoch, builder_id, component,
                            registry_image, recorded_at
                        ) VALUES (
                            :attempt_id, :materialization_id, 1, 3,
                            'builder:forged', 'task', :digest, now()
                        )
                    """),
                    {
                        "attempt_id": attempt_ids[1],
                        "materialization_id": materialization_id,
                        "digest": "registry.example/loom/fixture@sha256:" + "b" * 64,
                    },
                )

        grant_ids = (uuid4(), uuid4())
        shared_job_id = "12345"
        request_spec = {
            "schema": "loom.task-image-build-grant/v1",
            "request_sha256": "d" * 64,
        }
        with engine.begin() as connection:
            for grant_id, comment in (
                (grant_ids[0], f"loom-task-builder-v1:grant={grant_ids[0]}"),
                (grant_ids[1], f"loom-task-builder-v1:grant={grant_ids[1]}"),
            ):
                connection.execute(
                    text("""
                        INSERT INTO task_image_build_grants (
                            id, environment, provider, slurm_cluster_id, cpu_arch,
                            state, submitting_identity, slurm_account,
                            slurm_partition, slurm_qos, request_spec,
                            request_sha256, slurm_comment, ambiguity_settle_seconds,
                            journal_sequence
                        ) VALUES (
                            :id, 'staging', 'slurm-rootless-v1', 'gb10', 'arm64',
                            'issued', 'loom-builder', 'loom-task-builder',
                            'loom-task-builder', 'loom-task-image-builder-rootless-gb10',
                            CAST(:request_spec AS jsonb), :request_sha256, :comment,
                            30, 0
                        )
                    """),
                    {
                        "id": grant_id,
                        "request_spec": json.dumps(request_spec),
                        "request_sha256": "d" * 64,
                        "comment": comment,
                    },
                )
            connection.execute(
                text("""
                    UPDATE task_image_build_grants
                       SET state='bound', invocation_started_at=now(),
                           ambiguity_settle_until=now(),
                           slurm_job_id=:job_id, bound_at=now()
                     WHERE id=:id
                """),
                {"id": grant_ids[0], "job_id": shared_job_id},
            )
        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    text("""
                        UPDATE task_image_build_grants
                           SET state='bound', invocation_started_at=now(),
                               ambiguity_settle_until=now(),
                               slurm_job_id=:job_id, bound_at=now()
                         WHERE id=:id
                    """),
                    {"id": grant_ids[1], "job_id": shared_job_id},
                )

        command.downgrade(config, "0107")
        remaining = set(inspect(engine).get_table_names())
        assert "task_image_publication_evidence" not in remaining
        assert "task_image_build_grants" not in remaining
        command.upgrade(config, "0108")
    finally:
        engine.dispose()
