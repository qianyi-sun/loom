"""Verify Alembic migrations apply cleanly and the in_flight_count trigger fires."""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from loom.db.schema import Team, User
from loom.personal_dev_candidate import (
    PERSONAL_DEV_COMPONENTS,
    PERSONAL_DEV_PLATFORMS,
    PersonalDevCandidateRecord,
)
from loom.personal_dev_candidate_store import (
    PersonalDevBuildLeaseFencedError,
    SqlAlchemyPersonalDevCandidateStore,
)


@pytest.fixture(scope="module")
def postgres_url():
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace(
            "postgresql+psycopg2://",
            "postgresql+psycopg://",
        )
        os.environ["LOOM_DB_URL"] = url
        repo_root = Path(__file__).resolve().parents[2]
        # Use the venv's alembic via `python -m alembic` so PATH doesn't matter.
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini", "upgrade", "head"],
            cwd=repo_root,
            check=True,
        )
        yield url


def test_all_tables_exist(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT table_name
              FROM information_schema.tables
             WHERE table_schema = 'public'
        """)
        )
        names = {row[0] for row in result}
    expected = {
        "teams",
        "team_quotas",
        "tasks",
        "agents",
        "workers",
        "trials",
        "tokens",
        "rate_cards",
        "llm_calls",
        "benchmarks",
        "pending_team_registrations",
        "slurm_worker_jobs",
        "gb10_worker_pool_desired_states",
        "gb10_worker_node_statuses",
        "worker_pool_autoscaler_policies",
        "dev_instances",
        "personal_dev_candidates",
        "personal_dev_candidate_build_attempts",
        "artifacts",
        "artifact_lineage_edges",
        "pipeline_runs",
        "pipeline_stage_runs",
        "pipeline_stage_dependencies",
        "pipeline_fanout_expansions",
        "execution_attempts",
        "pipeline_events",
        "pipeline_terminal_snapshots",
        "pipeline_acceptance_preflight_prerequisites",
        "pipeline_budget_ledgers",
        "pipeline_budget_reservations",
        "execution_attempt_provider_budgets",
        "pipeline_cancellation_outbox",
        "alembic_version",
    }
    assert expected.issubset(names)


def _publication(
    candidate: PersonalDevCandidateRecord,
    now: datetime,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "attestation_scope": "personal-dev-only",
        "candidate_sha": candidate.candidate_sha,
        "source_sha256": candidate.source_sha256,
        "archive_sha256": candidate.archive_sha256,
        "build_contract_sha256": candidate.build_contract_sha256,
        "image_set_manifest_digest": "sha256:" + "6" * 64,
        "images": {
            component: {
                "index": f"registry.example/loom-{component}@sha256:" + "7" * 64,
                "platforms": {
                    platform: "sha256:" + ("8" if platform.endswith("amd64") else "9") * 64
                    for platform in PERSONAL_DEV_PLATFORMS
                },
            }
            for component in PERSONAL_DEV_COMPONENTS
        },
        "supported_pools": ["gb10", "oldlab"],
        "supported_architectures": list(PERSONAL_DEV_PLATFORMS),
        "protocol_versions": {
            "capacity-agent": "v1",
            "claim-guard": "v1",
            "control-plane-worker": "v1",
        },
        "trusted_launcher_profile_sha256": "4" * 64,
        "safety_evidence_sha256": "5" * 64,
        "publisher_identity": "system:serviceaccount:loom-dev:candidate-builder",
        "published_at": now.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    }


async def test_personal_dev_candidate_registration_and_build_lease(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id = uuid4()
    team_id = uuid4()
    now = datetime.now(UTC)
    requested = PersonalDevCandidateRecord(
        id=uuid4(),
        owner_user_id=owner_id,
        owner_team_id=team_id,
        candidate_sha="a" * 64,
        source_sha256="b" * 64,
        archive_sha256="c" * 64,
        build_contract_sha256="d" * 64,
        source_commit="e" * 40,
        dirty=True,
        manifest_json={"schema_version": 1, "attestation_scope": "personal-dev-only"},
        object_bucket="artifacts",
        object_key=f"personal-dev/sources/{team_id}/{owner_id}/source.tar",
        archive_size_bytes=10240,
        status="uploaded",
        created_at=now,
        updated_at=now,
    )
    try:
        async with sessions() as session:
            session.add(Team(id=team_id, name=f"candidate-{team_id}"))
            session.add(
                User(
                    id=owner_id,
                    email=f"{owner_id}@example.test",
                    username=f"user-{owner_id}",
                    username_normalized=f"user-{owner_id}",
                    status="active",
                ),
            )
            await session.commit()

        async with sessions() as session:
            store = SqlAlchemyPersonalDevCandidateStore(session)
            created = await store.register(requested)
            assert created.created is True
            retried = await store.register(
                replace(requested, id=uuid4()),
            )
            assert retried.created is False
            assert retried.candidate.id == created.candidate.id
            assert retried.build_attempt is None

            subject_id = uuid4()
            subject_incarnation = uuid4()
            operation_id = uuid4()
            queued = await store.enqueue_build(
                candidate_id=created.candidate.id,
                subject_id=subject_id,
                subject_incarnation=subject_incarnation,
                operation_id=operation_id,
                operation_epoch=1,
                now=now,
            )
            assert queued.created is True
            assert queued.build_attempt is not None
            assert queued.build_attempt.subject_id == subject_id
            retried_queue = await store.enqueue_build(
                candidate_id=created.candidate.id,
                subject_id=subject_id,
                subject_incarnation=subject_incarnation,
                operation_id=operation_id,
                operation_epoch=1,
                now=now,
            )
            assert retried_queue.created is False
            assert retried_queue.build_attempt == queued.build_attempt

        async with sessions() as session:
            store = SqlAlchemyPersonalDevCandidateStore(session)
            claimed = await store.claim_next_build(
                builder_id="builder-a",
                now=now,
                lease_seconds=60,
            )
            assert claimed is not None
            assert claimed.build_attempt is not None
            assert claimed.build_attempt.lease_epoch == 1
            running = await store.start_build(
                attempt_id=claimed.build_attempt.id,
                builder_id="builder-a",
                lease_epoch=1,
                now=now,
            )
            assert running.state == "running"
            finished = await store.finish_build(
                attempt_id=running.id,
                builder_id="builder-a",
                lease_epoch=1,
                now=now,
                publication=_publication(claimed.candidate, now),
            )
            assert finished.candidate.status == "ready"
            assert finished.build_attempt is not None
            assert finished.build_attempt.state == "succeeded"
            with pytest.raises(PersonalDevBuildLeaseFencedError):
                await store.heartbeat_build(
                    attempt_id=running.id,
                    builder_id="builder-a",
                    lease_epoch=1,
                    now=now,
                    lease_seconds=60,
                )

        async with sessions() as session:
            store = SqlAlchemyPersonalDevCandidateStore(session)
            retry_candidate = await store.register(
                replace(
                    requested,
                    id=uuid4(),
                    candidate_sha="1" * 64,
                    source_sha256="2" * 64,
                    archive_sha256="3" * 64,
                    object_key=f"personal-dev/sources/{team_id}/{owner_id}/retry.tar",
                ),
            )
            retry_subject = uuid4()
            retry_incarnation = uuid4()
            retry_operation = uuid4()
            queued = await store.enqueue_build(
                candidate_id=retry_candidate.candidate.id,
                subject_id=retry_subject,
                subject_incarnation=retry_incarnation,
                operation_id=retry_operation,
                operation_epoch=1,
                now=now,
            )
            assert queued.build_attempt is not None
            claimed = await store.claim_next_build(
                builder_id="builder-b",
                now=now,
                lease_seconds=60,
            )
            assert claimed is not None and claimed.build_attempt is not None
            await store.start_build(
                attempt_id=claimed.build_attempt.id,
                builder_id="builder-b",
                lease_epoch=1,
                now=now,
            )
            failed = await store.finish_build(
                attempt_id=claimed.build_attempt.id,
                builder_id="builder-b",
                lease_epoch=1,
                now=now,
                failure_reason="build_failed",
            )
            assert failed.candidate.status == "failed"
            retried = await store.enqueue_build(
                candidate_id=retry_candidate.candidate.id,
                subject_id=retry_subject,
                subject_incarnation=retry_incarnation,
                operation_id=retry_operation,
                operation_epoch=1,
                now=now,
            )
            assert retried.created is True
            assert retried.build_attempt is not None
            assert retried.build_attempt.attempt_sequence == 1
    finally:
        await engine.dispose()


def test_in_flight_count_trigger(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    team_id = uuid4()
    task_id = "demo"
    trial_id = uuid4()
    worker_id = uuid4()

    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
            {"id": team_id, "name": "test"},
        )
        conn.execute(text("INSERT INTO team_quotas (team_id) VALUES (:tid)"), {"tid": team_id})
        conn.execute(
            text("INSERT INTO tasks (id, checksum, config) VALUES (:i, :c, '{}'::jsonb)"),
            {"i": task_id, "c": "0" * 64},
        )
        conn.execute(
            text(
                "INSERT INTO workers (id, hostname, version, capabilities, "
                "registered_at, last_seen_at, status) VALUES "
                "(:id, 'h', 'v', '[]'::jsonb, :now, :now, 'active')"
            ),
            {"id": worker_id, "now": datetime.now(UTC)},
        )
        conn.execute(
            text(
                "INSERT INTO trials (id, team_id, task_id, config, requires_caps, state) "
                "VALUES (:id, :t, :ti, '{}'::jsonb, '{}'::jsonb, 'queued')"
            ),
            {"id": trial_id, "t": team_id, "ti": task_id},
        )

    def in_flight() -> int:
        with engine.connect() as conn:
            return conn.execute(
                text("SELECT in_flight_count FROM team_quotas WHERE team_id = :t"), {"t": team_id}
            ).scalar_one()

    assert in_flight() == 0

    # queued → claimed: +1
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE trials SET state='claimed', worker_id=:w WHERE id=:id"),
            {"w": worker_id, "id": trial_id},
        )
    assert in_flight() == 1

    # claimed → running: 0 (both active)
    with engine.begin() as conn:
        conn.execute(text("UPDATE trials SET state='running' WHERE id=:id"), {"id": trial_id})
    assert in_flight() == 1

    # running → succeeded: -1
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE trials SET state='succeeded', result='{}'::jsonb WHERE id=:id"),
            {"id": trial_id},
        )
    assert in_flight() == 0

    # Re-queue → +1 next time we go claimed
    with engine.begin() as conn:
        conn.execute(text("UPDATE trials SET state='queued' WHERE id=:id"), {"id": trial_id})
        conn.execute(text("UPDATE trials SET state='claimed' WHERE id=:id"), {"id": trial_id})
    assert in_flight() == 1
