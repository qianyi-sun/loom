"""Verify Alembic migrations apply cleanly and the in_flight_count trigger fires."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import CheckConstraint, create_engine, inspect, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from loom.db.schema import (
    DevInstance,
    DevLifecycleOperation,
    PersonalDevCandidate,
    PersonalDevCandidateArtifactCollection,
    Team,
    User,
)
from loom.personal_dev_activation import (
    PersonalDevActivationAcknowledgement,
    PersonalDevActivationSigner,
    PersonalDevActivationVerifier,
)
from loom.personal_dev_candidate import (
    PERSONAL_DEV_COMPONENTS,
    PERSONAL_DEV_PLATFORMS,
    CandidateRegistration,
    PersonalDevCandidateLimits,
    PersonalDevCandidateQuotaError,
    PersonalDevCandidateRecord,
    personal_dev_image_set_manifest_digest,
)
from loom.personal_dev_candidate_store import (
    PersonalDevArtifactGcLeaseFencedError,
    PersonalDevBuildLeaseFencedError,
    SqlAlchemyPersonalDevCandidateStore,
)
from loom.personal_dev_capacity import PersonalDevCapacityProjectionResult
from loom.personal_dev_environment import (
    PersonalDevAccessBinding,
    PersonalDevEnvironmentApplyRequest,
    PersonalDevEnvironmentDestroyRequest,
    PersonalDevLifecycleLimits,
)
from loom.personal_dev_environment_store import (
    PersonalDevEnvironmentConflictError,
    PersonalDevEnvironmentEpochFencedError,
    PersonalDevEnvironmentNotFoundError,
    PersonalDevEnvironmentOperationFencedError,
    SqlAlchemyPersonalDevActivationIntentReader,
    SqlAlchemyPersonalDevEnvironmentAuthority,
)

_PERSONAL_DEV_ACCESS = PersonalDevAccessBinding(
    auth_kind="bearer",
    credential_hash=b"a" * 32,
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
            [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini", "upgrade", "0120"],
            cwd=repo_root,
            check=True,
        )
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini", "downgrade", "0081"],
            cwd=repo_root,
            check=True,
        )
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini", "upgrade", "head"],
            cwd=repo_root,
            check=True,
        )
        yield url


async def test_0122_downgrade_retains_repaired_constraint(postgres_url: str) -> None:
    """Re-upgrade repairs nullable coordinates and preserves the registry constraint."""
    repo_root = Path(__file__).resolve().parents[2]
    try:
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini", "downgrade", "0120"],
            cwd=repo_root,
            check=True,
        )

        engine = create_engine(postgres_url)
        try:
            with engine.connect() as connection:
                revision = connection.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar_one()
            assert revision == "0120"
        finally:
            engine.dispose()

        engine = create_async_engine(postgres_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        owner_id = uuid4()
        team_id = uuid4()
        candidate_id = uuid4()
        coordinate_name = "coordinate-repair"
        now = datetime.now(UTC)
        candidate = PersonalDevCandidateRecord(
            id=candidate_id,
            owner_user_id=owner_id,
            owner_team_id=team_id,
            candidate_sha="1" * 64,
            source_sha256="2" * 64,
            archive_sha256="3" * 64,
            build_contract_sha256="4" * 64,
            source_commit="5" * 40,
            dirty=True,
            manifest_json={"schema_version": 1, "attestation_scope": "personal-dev-only"},
            object_bucket="artifacts",
            object_key=(
                f"personal-dev/sources/{team_id}/{owner_id}/{'1' * 64}/{candidate_id}/{'3' * 64}.tar"
            ),
            source_generation_id=candidate_id,
            archive_size_bytes=10240,
            status="uploaded",
            created_at=now,
            updated_at=now,
        )
        try:
            async with sessions() as session:
                session.add(Team(id=team_id, name=f"downgrade-registry-{team_id}"))
                session.add(
                    User(
                        id=owner_id,
                        email=f"{owner_id}@example.test",
                        username=f"downgrade-registry-{owner_id}",
                        username_normalized=f"downgrade-registry-{owner_id}",
                        status="active",
                    )
                )
                await session.commit()

            async with sessions() as session:
                await SqlAlchemyPersonalDevCandidateStore(session).register(candidate)

            async with sessions() as session:
                await session.execute(
                    text(
                        "UPDATE personal_dev_candidates "
                        "SET registry_prefix = :registry_prefix WHERE id = :candidate_id"
                    ),
                    {"registry_prefix": "r" * 309, "candidate_id": candidate_id},
                )
                await session.commit()

            async with sessions() as session:
                with pytest.raises(DBAPIError) as exc_info:
                    await session.execute(
                        text(
                            "UPDATE personal_dev_candidates "
                            "SET registry_prefix = :registry_prefix WHERE id = :candidate_id"
                        ),
                        {"registry_prefix": "r" * 310, "candidate_id": candidate_id},
                    )
                assert exc_info.value.orig.sqlstate == "23514"
                await session.rollback()

            async with sessions() as session:
                await session.execute(
                    text(
                        "INSERT INTO dev_instances "
                        "(name, owner_user_id, owner_team_id, max_slots, "
                        "deployment_generation, candidate_id, candidate_sha, operation_id) "
                        "VALUES (:name, :owner_id, :team_id, 1, 1, :candidate_id, "
                        ":candidate_sha, :operation_id)"
                    ),
                    {
                        "name": coordinate_name,
                        "owner_id": owner_id,
                        "team_id": team_id,
                        "candidate_id": candidate_id,
                        "candidate_sha": candidate.candidate_sha,
                        "operation_id": uuid4(),
                    },
                )
                await session.commit()
        finally:
            await engine.dispose()

        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini", "upgrade", "head"],
            cwd=repo_root,
            check=True,
        )
        engine = create_engine(postgres_url)
        try:
            with engine.connect() as connection:
                revision = connection.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar_one()
                coordinates = connection.execute(
                    text(
                        "SELECT capacity_namespace, capacity_database "
                        "FROM dev_instances WHERE name = :name"
                    ),
                    {"name": coordinate_name},
                ).one()
            assert revision == "0123"
            assert tuple(coordinates) == (
                f"loom-dev-{coordinate_name}",
                "loom_dev_coordinate_repair",
            )
        finally:
            engine.dispose()
    finally:
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini", "upgrade", "head"],
            cwd=repo_root,
            check=True,
        )


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
        "trial_resource_usage",
        "execution_classes",
        "execution_targets",
        "execution_leases",
        "execution_commands",
        "execution_events",
        "execution_lease_history",
        "execution_admission_policies",
        "execution_admission_reservations",
        "execution_price_snapshots",
        "execution_target_price_bindings",
        "execution_budget_policies",
        "execution_cost_reservations",
        "execution_cost_reservation_debits",
        "execution_node_cost_records",
        "execution_node_cost_allocations",
        "execution_capacity_policies",
        "execution_capacity_observations",
        "execution_resource_calibrations",
        "execution_resource_profile_bindings",
        "execution_provisioning_authorizations",
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
        "dev_lifecycle_operations",
        "dev_lifecycle_operation_attempts",
        "dev_lifecycle_activation_acknowledgements",
        "personal_dev_candidates",
        "personal_dev_candidate_artifact_collections",
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
    images: dict[str, object] = {
        component: {
            "index": f"registry.example/loom-{component}@sha256:" + "7" * 64,
            "platforms": {
                platform: "sha256:" + ("8" if platform.endswith("amd64") else "9") * 64
                for platform in PERSONAL_DEV_PLATFORMS
            },
        }
        for component in PERSONAL_DEV_COMPONENTS
    }
    return {
        "schema_version": 1,
        "attestation_scope": "personal-dev-only",
        "candidate_sha": candidate.candidate_sha,
        "source_sha256": candidate.source_sha256,
        "archive_sha256": candidate.archive_sha256,
        "build_contract_sha256": candidate.build_contract_sha256,
        "image_set_manifest_digest": personal_dev_image_set_manifest_digest(images),
        "images": images,
        "supported_pools": ["gb10", "oldlab"],
        "supported_architectures": list(PERSONAL_DEV_PLATFORMS),
        "protocol_versions": {
            "capacity-agent": "v1",
            "claim-guard": "v1",
            "control-plane-worker": "v1",
        },
        "trusted_launcher_profile_sha256": "4" * 64,
        "safety_evidence": {
            "bucket": candidate.object_bucket,
            "content_type": "application/vnd.loom.personal-dev-safety-evidence.v1+json",
            "key": (f"personal-dev/evidence/{candidate.candidate_sha}/test/safety-evidence.json"),
            "sha256": "5" * 64,
            "size_bytes": 1024,
        },
        "safety_evidence_sha256": "5" * 64,
        "publisher_identity": "system:serviceaccount:loom-dev:candidate-builder",
        "published_at": now.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    }


def _reupload(
    candidate: PersonalDevCandidateRecord,
    **changes: object,
) -> PersonalDevCandidateRecord:
    generation_id = uuid4()
    requested = replace(
        candidate,
        id=generation_id,
        source_generation_id=generation_id,
        **changes,
    )
    return replace(
        requested,
        object_key=(
            f"personal-dev/sources/{requested.owner_team_id}/"
            f"{requested.owner_user_id}/{requested.candidate_sha}/"
            f"{generation_id}/{requested.archive_sha256}.tar"
        ),
    )


async def _claim_candidate_build(
    store: SqlAlchemyPersonalDevCandidateStore,
    *,
    candidate_id: UUID,
    builder_id: str,
    now: datetime,
) -> CandidateRegistration:
    """Claim the requested candidate while terminalizing older test queue rows."""
    for _ in range(20):
        claimed = await store.claim_next_build(
            builder_id=builder_id,
            now=now,
            lease_seconds=60,
        )
        assert claimed is not None and claimed.build_attempt is not None
        if claimed.candidate.id == candidate_id:
            return claimed
        running = await store.start_build(
            attempt_id=claimed.build_attempt.id,
            builder_id=builder_id,
            lease_epoch=claimed.build_attempt.lease_epoch,
            now=now,
        )
        await store.finish_build(
            attempt_id=running.id,
            builder_id=builder_id,
            lease_epoch=running.lease_epoch,
            now=now,
            failure_reason="test_cleanup",
        )
    raise AssertionError("target personal-dev candidate was not in the bounded build queue")


async def test_personal_dev_candidate_registration_and_build_lease(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id = uuid4()
    team_id = uuid4()
    now = datetime.now(UTC)
    candidate_id = uuid4()
    requested = PersonalDevCandidateRecord(
        id=candidate_id,
        owner_user_id=owner_id,
        owner_team_id=team_id,
        candidate_sha="a" * 64,
        source_sha256="b" * 64,
        archive_sha256="c" * 64,
        build_contract_sha256="d" * 64,
        source_commit="e" * 40,
        dirty=True,
        manifest_json={
            "schema_version": 1,
            "attestation_scope": "personal-dev-only",
            "captured_paths": ("app.py", "package.json"),
        },
        object_bucket="artifacts",
        object_key=(
            f"personal-dev/sources/{team_id}/{owner_id}/{'a' * 64}/{candidate_id}/{'c' * 64}.tar"
        ),
        source_generation_id=candidate_id,
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

        async with sessions() as session:
            store = SqlAlchemyPersonalDevCandidateStore(session)
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
                registry_prefix="ghcr.io/qianyi-sun/loom-dev",
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
            failed = await store.finish_build(
                attempt_id=running.id,
                builder_id="builder-a",
                lease_epoch=1,
                now=now,
                failure_reason="build_failed",
            )
            assert failed.candidate.status == "failed"
            assert failed.build_attempt is not None
            assert failed.build_attempt.state == "failed"
            with pytest.raises(PersonalDevBuildLeaseFencedError):
                await store.heartbeat_build(
                    attempt_id=running.id,
                    builder_id="builder-a",
                    lease_epoch=1,
                    now=now,
                    lease_seconds=60,
                )

            replay = await store.register(_reupload(requested))
            assert replay.created is False
            assert replay.candidate.id == requested.id
            assert replay.candidate.status == "failed"
            assert replay.candidate.object_key == requested.object_key
            assert replay.build_attempt is None

        async with sessions() as session:
            store = SqlAlchemyPersonalDevCandidateStore(session)
            retry_candidate = await store.register(
                _reupload(
                    requested,
                    candidate_sha="1" * 64,
                    source_sha256="2" * 64,
                    archive_sha256="3" * 64,
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


@pytest.mark.parametrize(
    ("prefix_length", "accepted"),
    ((309, True), (310, False)),
)
async def test_personal_dev_candidate_registry_prefix_length_boundary(
    postgres_url: str,
    prefix_length: int,
    accepted: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id = uuid4()
    team_id = uuid4()
    candidate_id = uuid4()
    now = datetime.now(UTC)
    requested = PersonalDevCandidateRecord(
        id=candidate_id,
        owner_user_id=owner_id,
        owner_team_id=team_id,
        candidate_sha="1" * 64,
        source_sha256="2" * 64,
        archive_sha256="3" * 64,
        build_contract_sha256="4" * 64,
        source_commit="5" * 40,
        dirty=True,
        manifest_json={"schema_version": 1, "attestation_scope": "personal-dev-only"},
        object_bucket="artifacts",
        object_key=(
            f"personal-dev/sources/{team_id}/{owner_id}/{'1' * 64}/{candidate_id}/{'3' * 64}.tar"
        ),
        source_generation_id=candidate_id,
        archive_size_bytes=10240,
        status="uploaded",
        created_at=now,
        updated_at=now,
    )
    try:
        async with sessions() as session:
            session.add(Team(id=team_id, name=f"registry-boundary-{team_id}"))
            session.add(
                User(
                    id=owner_id,
                    email=f"{owner_id}@example.test",
                    username=f"registry-boundary-{owner_id}",
                    username_normalized=f"registry-boundary-{owner_id}",
                    status="active",
                )
            )
            await session.commit()

        async with sessions() as session:
            await SqlAlchemyPersonalDevCandidateStore(session).register(requested)

        async with sessions() as session:
            statement = text(
                "UPDATE personal_dev_candidates "
                "SET registry_prefix = :registry_prefix WHERE id = :candidate_id"
            )
            parameters = {
                "registry_prefix": "r" * prefix_length,
                "candidate_id": candidate_id,
            }
            if accepted:
                await session.execute(statement, parameters)
                await session.commit()
            else:
                with pytest.raises(DBAPIError) as exc_info:
                    await session.execute(statement, parameters)
                assert exc_info.value.orig.sqlstate == "23514"
                await session.rollback()
    finally:
        await engine.dispose()


async def test_personal_dev_candidate_registration_enforces_owner_retention_quota(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id = uuid4()
    team_id = uuid4()
    now = datetime.now(UTC)

    def candidate(digit: str) -> PersonalDevCandidateRecord:
        candidate_id = uuid4()
        return PersonalDevCandidateRecord(
            id=candidate_id,
            owner_user_id=owner_id,
            owner_team_id=team_id,
            candidate_sha=digit * 64,
            source_sha256=digit * 63 + "a",
            archive_sha256=digit * 63 + "b",
            build_contract_sha256="f" * 64,
            source_commit="e" * 40,
            dirty=True,
            manifest_json={"schema_version": 1},
            object_bucket="artifacts",
            object_key=(
                f"personal-dev/sources/{team_id}/{owner_id}/{digit * 64}/"
                f"{candidate_id}/{digit * 63 + 'b'}.tar"
            ),
            source_generation_id=candidate_id,
            archive_size_bytes=10240,
            status="uploaded",
            created_at=now,
            updated_at=now,
        )

    try:
        async with sessions() as session:
            session.add(Team(id=team_id, name=f"quota-{team_id}"))
            session.add(
                User(
                    id=owner_id,
                    email=f"{owner_id}@example.test",
                    username=f"user-{owner_id}",
                    username_normalized=f"user-{owner_id}",
                    status="active",
                )
            )
            await session.commit()

        first_request = candidate("2")
        async with sessions() as session:
            store = SqlAlchemyPersonalDevCandidateStore(
                session,
                limits=PersonalDevCandidateLimits(
                    per_owner_retained_candidates=1,
                    per_owner_retained_archive_bytes=10240,
                ),
            )
            first = await store.register(first_request)
            assert first.created is True
            replay = await store.register(_reupload(first_request))
            assert replay.candidate.id == first.candidate.id
            with pytest.raises(ValueError, match="immutable binding"):
                await store.register(
                    _reupload(
                        first_request,
                        candidate_sha="4" * 64,
                    )
                )
            with pytest.raises(PersonalDevCandidateQuotaError, match="count"):
                await store.register(candidate("3"))
    finally:
        await engine.dispose()


async def test_personal_dev_artifact_gc_is_grace_delayed_lease_fenced_and_rehydratable(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id = uuid4()
    team_id = uuid4()
    now = datetime(2020, 1, 1, tzinfo=UTC)
    candidate_sha = "8" * 64
    archive_sha = "9" * 64
    candidate_id = uuid4()
    requested = PersonalDevCandidateRecord(
        id=candidate_id,
        owner_user_id=owner_id,
        owner_team_id=team_id,
        candidate_sha=candidate_sha,
        source_sha256="a" * 64,
        archive_sha256=archive_sha,
        build_contract_sha256="b" * 64,
        source_commit="c" * 40,
        dirty=True,
        manifest_json={"schema_version": 1},
        object_bucket="artifacts",
        object_key=(
            f"personal-dev/sources/{team_id}/{owner_id}/{candidate_sha}/"
            f"{candidate_id}/{archive_sha}.tar"
        ),
        source_generation_id=candidate_id,
        archive_size_bytes=10240,
        status="uploaded",
        created_at=now,
        updated_at=now,
    )
    limits = PersonalDevCandidateLimits(
        per_owner_retained_candidates=1,
        per_owner_retained_archive_bytes=10240,
    )
    try:
        async with sessions() as session:
            session.add(Team(id=team_id, name=f"candidate-gc-{team_id}"))
            session.add(
                User(
                    id=owner_id,
                    email=f"{owner_id}@example.test",
                    username=f"user-{owner_id}",
                    username_normalized=f"user-{owner_id}",
                    status="active",
                )
            )
            await session.commit()

        async with sessions() as session:
            store = SqlAlchemyPersonalDevCandidateStore(session, limits=limits)
            registered = await store.register(requested)
            assert await store.mark_next_artifact_gc(now=now) is True
            assert (
                await store.claim_next_artifact_gc(
                    collector_id="collector-a",
                    now=now + timedelta(seconds=59),
                    retention_seconds=60,
                    lease_seconds=300,
                )
                is None
            )
            claim = await store.claim_next_artifact_gc(
                collector_id="collector-a",
                now=now + timedelta(seconds=60),
                retention_seconds=60,
                lease_seconds=300,
            )
            assert claim is not None
            assert claim.candidate_id == registered.candidate.id
            assert claim.lease_epoch == 1
            assert claim.manifest.source_object_key == requested.object_key
            with pytest.raises(PersonalDevArtifactGcLeaseFencedError):
                await store.finish_artifact_gc(
                    candidate_id=claim.candidate_id,
                    collector_id="collector-a",
                    lease_epoch=2,
                    manifest_sha256=claim.manifest.manifest_sha256,
                    now=now + timedelta(seconds=61),
                )
            with pytest.raises(PersonalDevArtifactGcLeaseFencedError):
                await store.finish_artifact_gc(
                    candidate_id=claim.candidate_id,
                    collector_id="collector-a",
                    lease_epoch=claim.lease_epoch,
                    manifest_sha256=claim.manifest.manifest_sha256,
                    now=now + timedelta(seconds=361),
                )
            reclaimed = await store.claim_next_artifact_gc(
                collector_id="collector-b",
                now=now + timedelta(seconds=361),
                retention_seconds=60,
                lease_seconds=300,
            )
            assert reclaimed is not None
            assert reclaimed.lease_epoch == claim.lease_epoch + 1
            await store.finish_artifact_gc(
                candidate_id=reclaimed.candidate_id,
                collector_id="collector-b",
                lease_epoch=reclaimed.lease_epoch,
                manifest_sha256=reclaimed.manifest.manifest_sha256,
                now=now + timedelta(seconds=362),
            )
            first_evidence = (
                (
                    await session.execute(
                        select(PersonalDevCandidateArtifactCollection).where(
                            PersonalDevCandidateArtifactCollection.candidate_id
                            == registered.candidate.id
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert [item.collection_sequence for item in first_evidence] == [1]
            assert first_evidence[0].manifest_sha256 == reclaimed.manifest.manifest_sha256

        replacement_sha = "d" * 64
        replacement_archive = "e" * 64
        async with sessions() as session:
            store = SqlAlchemyPersonalDevCandidateStore(session, limits=limits)
            replacement = await store.register(
                _reupload(
                    requested,
                    candidate_sha=replacement_sha,
                    source_sha256="f" * 64,
                    archive_sha256=replacement_archive,
                )
            )
            assert replacement.created is True
            assert replacement.candidate.artifact_state == "retained"

        async with sessions() as session:
            store = SqlAlchemyPersonalDevCandidateStore(session, limits=limits)
            assert await store.mark_next_artifact_gc(now=now + timedelta(minutes=2)) is True
            claim = await store.claim_next_artifact_gc(
                collector_id="collector-a",
                now=now + timedelta(minutes=3),
                retention_seconds=60,
                lease_seconds=300,
            )
            assert claim is not None
            await store.finish_artifact_gc(
                candidate_id=claim.candidate_id,
                collector_id="collector-a",
                lease_epoch=claim.lease_epoch,
                manifest_sha256=claim.manifest.manifest_sha256,
                now=now + timedelta(minutes=3, seconds=1),
            )
            rehydration_request = _reupload(requested)
            rehydrated = await store.register(rehydration_request)
            assert rehydrated.created is False
            assert rehydrated.candidate.id == requested.id
            assert rehydrated.candidate.artifact_state == "retained"
            assert rehydrated.candidate.status == "uploaded"
            assert rehydrated.candidate.artifact_gc_manifest_sha256 is None
            assert rehydrated.candidate.object_key == rehydration_request.object_key
            assert (
                rehydrated.candidate.source_generation_id
                == rehydration_request.source_generation_id
            )
            assert rehydrated.candidate.object_key != requested.object_key
            preserved = (
                (
                    await session.execute(
                        select(PersonalDevCandidateArtifactCollection).where(
                            PersonalDevCandidateArtifactCollection.candidate_id == requested.id
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert [item.collection_sequence for item in preserved] == [1]

            assert await store.mark_next_artifact_gc(now=now + timedelta(minutes=4)) is True
            second_claim = await store.claim_next_artifact_gc(
                collector_id="collector-b",
                now=now + timedelta(minutes=4),
                retention_seconds=0,
                lease_seconds=300,
            )
            assert second_claim is not None
            assert second_claim.candidate_id == requested.id
            assert second_claim.manifest.source_object_key == rehydration_request.object_key
            await store.finish_artifact_gc(
                candidate_id=second_claim.candidate_id,
                collector_id="collector-b",
                lease_epoch=second_claim.lease_epoch,
                manifest_sha256=second_claim.manifest.manifest_sha256,
                now=now + timedelta(minutes=4, seconds=1),
            )
            completed = (
                (
                    await session.execute(
                        select(PersonalDevCandidateArtifactCollection)
                        .where(PersonalDevCandidateArtifactCollection.candidate_id == requested.id)
                        .order_by(PersonalDevCandidateArtifactCollection.collection_sequence)
                    )
                )
                .scalars()
                .all()
            )
            assert [item.collection_sequence for item in completed] == [1, 2]

            for mutation in (
                "UPDATE personal_dev_candidate_artifact_collections "
                "SET collector_id = 'mutated' WHERE candidate_id = :candidate_id",
                "DELETE FROM personal_dev_candidate_artifact_collections "
                "WHERE candidate_id = :candidate_id",
            ):
                with pytest.raises(DBAPIError, match="append-only"):
                    await session.execute(
                        text(mutation),
                        {"candidate_id": requested.id},
                    )
                    await session.commit()
                await session.rollback()
    finally:
        await engine.dispose()


async def test_personal_dev_build_claim_global_limit_is_concurrency_safe(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    limits = PersonalDevCandidateLimits(
        global_active_builds=1,
        per_owner_active_builds=1,
    )

    async with sessions() as session:
        seeded: list[PersonalDevCandidateRecord] = []
        for digit in ("6", "7"):
            owner_id = uuid4()
            team_id = uuid4()
            candidate_id = uuid4()
            session.add(Team(id=team_id, name=f"claim-quota-{team_id}"))
            session.add(
                User(
                    id=owner_id,
                    email=f"{owner_id}@example.test",
                    username=f"user-{owner_id}",
                    username_normalized=f"user-{owner_id}",
                    status="active",
                )
            )
            seeded.append(
                PersonalDevCandidateRecord(
                    id=candidate_id,
                    owner_user_id=owner_id,
                    owner_team_id=team_id,
                    candidate_sha=digit * 64,
                    source_sha256=digit * 63 + "a",
                    archive_sha256=digit * 63 + "b",
                    build_contract_sha256="f" * 64,
                    source_commit="e" * 40,
                    dirty=True,
                    manifest_json={"schema_version": 1},
                    object_bucket="artifacts",
                    object_key=(
                        f"personal-dev/sources/{team_id}/{owner_id}/{digit * 64}/"
                        f"{candidate_id}/{digit * 63 + 'b'}.tar"
                    ),
                    source_generation_id=candidate_id,
                    archive_size_bytes=10240,
                    status="uploaded",
                    created_at=now,
                    updated_at=now,
                )
            )
        await session.commit()
        store = SqlAlchemyPersonalDevCandidateStore(session, limits=limits)
        for candidate in seeded:
            registered = await store.register(candidate)
            await store.enqueue_build(
                candidate_id=registered.candidate.id,
                subject_id=uuid4(),
                subject_incarnation=uuid4(),
                operation_id=uuid4(),
                operation_epoch=1,
                now=now,
            )

    async def claim(builder_id: str) -> CandidateRegistration | None:
        async with sessions() as session:
            return await SqlAlchemyPersonalDevCandidateStore(
                session,
                limits=limits,
            ).claim_next_build(
                builder_id=builder_id,
                now=now,
                lease_seconds=60,
            )

    try:
        first, second = await asyncio.gather(claim("quota-builder-a"), claim("quota-builder-b"))
        claimed = [item for item in (first, second) if item is not None]
        assert len(claimed) == 1
        registration = claimed[0]
        assert registration.build_attempt is not None
        async with sessions() as session:
            store = SqlAlchemyPersonalDevCandidateStore(session, limits=limits)
            running = await store.start_build(
                attempt_id=registration.build_attempt.id,
                builder_id=str(registration.build_attempt.claimed_by),
                lease_epoch=registration.build_attempt.lease_epoch,
                now=now,
            )
            await store.finish_build(
                attempt_id=running.id,
                builder_id=str(running.claimed_by),
                lease_epoch=running.lease_epoch,
                now=now,
                failure_reason="test_cleanup",
            )
    finally:
        await engine.dispose()


async def test_personal_dev_environment_apply_is_owner_bound_and_epoch_fenced(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id = uuid4()
    team_id = uuid4()
    now = datetime.now(UTC)
    candidate_id = uuid4()
    candidate = PersonalDevCandidateRecord(
        id=candidate_id,
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
        object_key=(
            f"personal-dev/sources/{team_id}/{owner_id}/{'a' * 64}/{candidate_id}/{'c' * 64}.tar"
        ),
        source_generation_id=candidate_id,
        archive_size_bytes=10240,
        status="uploaded",
        created_at=now,
        updated_at=now,
    )
    try:
        async with sessions() as session:
            session.add(Team(id=team_id, name=f"environment-{team_id}"))
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
            registered = await SqlAlchemyPersonalDevCandidateStore(session).register(candidate)
            candidate = registered.candidate

        idempotency_key = uuid4()
        request = PersonalDevEnvironmentApplyRequest(
            name="alice",
            owner_user_id=owner_id,
            owner_team_id=team_id,
            candidate_id=candidate.id,
            candidate_sha=candidate.candidate_sha,
            min_slots=0,
            max_slots=2,
            expected_operation_epoch=0,
            idempotency_key=idempotency_key,
        )
        async with sessions() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(session)
            missing_name = "missing-probe"
            with pytest.raises(PersonalDevEnvironmentNotFoundError):
                await authority.apply(
                    replace(
                        request,
                        name=missing_name,
                        expected_operation_epoch=1,
                        idempotency_key=uuid4(),
                    ),
                    access_binding=_PERSONAL_DEV_ACCESS,
                    now=now,
                )
            assert (
                await session.execute(
                    select(DevInstance).where(DevInstance.name == missing_name),
                )
            ).scalar_one_or_none() is None
            assert (
                await session.execute(
                    text(
                        "SELECT count(*) FROM dev_lifecycle_operations "
                        "WHERE environment_name = :name",
                    ),
                    {"name": missing_name},
                )
            ).scalar_one() == 0

            created = await authority.apply(
                request,
                access_binding=_PERSONAL_DEV_ACCESS,
                now=now,
            )
            assert created.acquired is True
            assert created.requires_build_binding is True
            assert created.environment.status == "provisioning"
            assert created.environment.subject_id == created.operation.subject_id
            assert created.environment.subject_incarnation == created.operation.subject_incarnation
            assert created.operation.operation_epoch == 1
            assert created.operation.kind == "create"
            attempt_binding = (
                await session.execute(
                    text(
                        "SELECT bootstrap_auth_kind, bootstrap_credential_hash, "
                        "credential_binding_version FROM dev_lifecycle_operation_attempts "
                        "WHERE id = :attempt_id",
                    ),
                    {"attempt_id": created.operation.attempt_id},
                )
            ).one()
            assert attempt_binding.bootstrap_auth_kind == "bearer"
            assert bytes(attempt_binding.bootstrap_credential_hash) == b"a" * 32
            assert attempt_binding.credential_binding_version == 1
            retry = await authority.apply(
                request,
                access_binding=_PERSONAL_DEV_ACCESS,
                now=now,
            )
            assert retry.acquired is False
            assert retry.operation.id == created.operation.id

            with pytest.raises(PersonalDevEnvironmentConflictError):
                await authority.apply(
                    replace(request, max_slots=3),
                    access_binding=_PERSONAL_DEV_ACCESS,
                    now=now,
                )
            content_retry = await authority.apply(
                replace(request, idempotency_key=uuid4()),
                access_binding=_PERSONAL_DEV_ACCESS,
                now=now,
            )
            assert content_retry.acquired is False
            assert content_retry.operation.id == created.operation.id
            with pytest.raises(PersonalDevEnvironmentEpochFencedError):
                await authority.apply(
                    replace(request, max_slots=3, idempotency_key=uuid4()),
                    access_binding=_PERSONAL_DEV_ACCESS,
                    now=now,
                )
            constrained = SqlAlchemyPersonalDevEnvironmentAuthority(
                session,
                limits=PersonalDevLifecycleLimits(
                    global_live_instances=16,
                    per_owner_live_instances=1,
                    per_owner_aggregate_min_slots=8,
                    per_owner_aggregate_max_slots=16,
                ),
            )
            with pytest.raises(PersonalDevEnvironmentConflictError):
                await constrained.apply(
                    replace(
                        request,
                        name="charlie",
                        idempotency_key=uuid4(),
                    ),
                    access_binding=_PERSONAL_DEV_ACCESS,
                    now=now,
                )

        async with sessions() as session:
            with pytest.raises(DBAPIError):
                await session.execute(
                    text(
                        "UPDATE dev_lifecycle_operations SET candidate_sha = :candidate_sha "
                        "WHERE id = :operation_id",
                    ),
                    {
                        "candidate_sha": "9" * 64,
                        "operation_id": created.operation.id,
                    },
                )
            await session.rollback()
            with pytest.raises(DBAPIError):
                await session.execute(
                    text(
                        "UPDATE dev_lifecycle_operation_attempts "
                        "SET bootstrap_credential_hash = :credential_hash "
                        "WHERE id = :attempt_id",
                    ),
                    {
                        "credential_hash": b"z" * 32,
                        "attempt_id": created.operation.attempt_id,
                    },
                )
            await session.rollback()

        async with sessions() as session:
            candidate_store = SqlAlchemyPersonalDevCandidateStore(session)
            candidate_state = await candidate_store.get(candidate.id)
            assert candidate_state is not None
            assert candidate_state.build_attempt is not None
            assert candidate_state.build_attempt.subject_id == created.environment.subject_id
            assert candidate_state.build_attempt.operation_id == created.operation.id
            assert candidate_state.build_attempt.operation_epoch == 1
            claimed = await _claim_candidate_build(
                candidate_store,
                candidate_id=candidate.id,
                builder_id="environment-test-cleanup",
                now=now,
            )
            assert claimed.build_attempt is not None
            await candidate_store.start_build(
                attempt_id=claimed.build_attempt.id,
                builder_id="environment-test-cleanup",
                lease_epoch=claimed.build_attempt.lease_epoch,
                now=now,
            )
            await candidate_store.finish_build(
                attempt_id=claimed.build_attempt.id,
                builder_id="environment-test-cleanup",
                lease_epoch=claimed.build_attempt.lease_epoch,
                now=now,
                failure_reason="test_cleanup",
            )

        async with sessions() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(session)
            reconcile_claim = await authority.claim_next_reconciliation(
                reconciler_id="environment-test-reconciler",
                now=now,
                lease_seconds=60,
            )
            assert reconcile_claim is not None
            failed = await authority.fail_pre_activation(
                operation_id=created.operation.id,
                operation_epoch=1,
                attempt_id=reconcile_claim.attempt.id,
                reconciler_id="environment-test-reconciler",
                lease_epoch=reconcile_claim.attempt.lease_epoch,
                failure_reason="candidate_build_failed",
                now=now,
            )
            assert failed.operation.state == "failed"
            assert failed.environment.status == "failed"
            retried = await authority.apply(
                request,
                access_binding=PersonalDevAccessBinding(
                    auth_kind="bearer",
                    credential_hash=b"r" * 32,
                ),
                now=now,
            )
            assert retried.acquired is True
            assert retried.operation.id == created.operation.id
            assert retried.operation.attempt_sequence == 1
            assert retried.operation.attempt_id != created.operation.attempt_id
            assert retried.environment.status == "provisioning"
            retry_binding = (
                await session.execute(
                    text(
                        "SELECT bootstrap_credential_hash "
                        "FROM dev_lifecycle_operation_attempts WHERE id = :attempt_id",
                    ),
                    {"attempt_id": retried.operation.attempt_id},
                )
            ).scalar_one()
            assert bytes(retry_binding) == b"r" * 32

        other_owner = uuid4()
        async with sessions() as session:
            session.add(
                User(
                    id=other_owner,
                    email=f"{other_owner}@example.test",
                    username=f"user-{other_owner}",
                    username_normalized=f"user-{other_owner}",
                    status="active",
                ),
            )
            await session.commit()
            with pytest.raises(PersonalDevEnvironmentConflictError):
                await SqlAlchemyPersonalDevEnvironmentAuthority(session).apply(
                    replace(
                        request,
                        owner_user_id=other_owner,
                        idempotency_key=uuid4(),
                    ),
                    access_binding=_PERSONAL_DEV_ACCESS,
                    now=now,
                )

        invalid_candidates = (
            replace(
                request,
                candidate_id=uuid4(),
                idempotency_key=uuid4(),
            ),
            replace(
                request,
                candidate_sha="9" * 64,
                idempotency_key=uuid4(),
            ),
        )
        for invalid_request in invalid_candidates:
            async with sessions() as session:
                with pytest.raises(PersonalDevEnvironmentConflictError):
                    await SqlAlchemyPersonalDevEnvironmentAuthority(session).apply(
                        invalid_request,
                        access_binding=_PERSONAL_DEV_ACCESS,
                        now=now,
                    )

        stale_candidate_id = uuid4()
        stale_candidate_sha = "8" * 64
        stale_created_at = now - timedelta(days=365)
        stale_candidate = PersonalDevCandidateRecord(
            id=stale_candidate_id,
            owner_user_id=owner_id,
            owner_team_id=team_id,
            candidate_sha=stale_candidate_sha,
            source_sha256="7" * 64,
            archive_sha256="6" * 64,
            build_contract_sha256="5" * 64,
            source_commit="4" * 40,
            dirty=False,
            manifest_json={"schema_version": 1, "attestation_scope": "personal-dev-only"},
            object_bucket="artifacts",
            object_key=(
                f"personal-dev/sources/{team_id}/{owner_id}/{stale_candidate_sha}/"
                f"{stale_candidate_id}/{'6' * 64}.tar"
            ),
            source_generation_id=stale_candidate_id,
            archive_size_bytes=10240,
            status="uploaded",
            created_at=stale_created_at,
            updated_at=stale_created_at,
        )
        async with sessions() as session:
            candidate_store = SqlAlchemyPersonalDevCandidateStore(session)
            await candidate_store.register(stale_candidate)
            assert await candidate_store.mark_next_artifact_gc(now=now) is True
            stale_claim = await candidate_store.claim_next_artifact_gc(
                collector_id="candidate-conflict-test",
                now=now,
                retention_seconds=0,
                lease_seconds=300,
            )
            assert stale_claim is not None
            assert stale_claim.candidate_id == stale_candidate_id
            with pytest.raises(PersonalDevEnvironmentConflictError):
                await SqlAlchemyPersonalDevEnvironmentAuthority(session).apply(
                    replace(
                        request,
                        candidate_id=stale_candidate_id,
                        candidate_sha=stale_candidate_sha,
                        idempotency_key=uuid4(),
                    ),
                    access_binding=_PERSONAL_DEV_ACCESS,
                    now=now,
                )
    finally:
        await engine.dispose()


async def test_personal_dev_environment_capacity_and_candidate_updates_are_atomic(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id = uuid4()
    team_id = uuid4()
    now = datetime.now(UTC)

    def candidate_record(digit: str) -> PersonalDevCandidateRecord:
        source_digit, archive_digit = {"2": ("4", "5"), "3": ("6", "7")}[digit]
        candidate_id = uuid4()
        return PersonalDevCandidateRecord(
            id=candidate_id,
            owner_user_id=owner_id,
            owner_team_id=team_id,
            candidate_sha=digit * 64,
            source_sha256=source_digit * 64,
            archive_sha256=archive_digit * 64,
            build_contract_sha256="f" * 64,
            source_commit="1" * 40,
            dirty=False,
            manifest_json={"schema_version": 1, "attestation_scope": "personal-dev-only"},
            object_bucket="artifacts",
            object_key=(
                f"personal-dev/sources/{team_id}/{owner_id}/{digit * 64}/"
                f"{candidate_id}/{archive_digit * 64}.tar"
            ),
            source_generation_id=candidate_id,
            archive_size_bytes=10240,
            status="uploaded",
            created_at=now,
            updated_at=now,
        )

    try:
        async with sessions() as session:
            session.add(Team(id=team_id, name=f"updates-{team_id}"))
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
            candidates = SqlAlchemyPersonalDevCandidateStore(session)
            first = (await candidates.register(candidate_record("2"))).candidate
            second = (await candidates.register(candidate_record("3"))).candidate

        async with sessions() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(session)
            create = await authority.apply(
                PersonalDevEnvironmentApplyRequest(
                    name="bob",
                    owner_user_id=owner_id,
                    owner_team_id=team_id,
                    candidate_id=first.id,
                    candidate_sha=first.candidate_sha,
                    min_slots=0,
                    max_slots=2,
                    expected_operation_epoch=0,
                    idempotency_key=uuid4(),
                ),
                access_binding=_PERSONAL_DEV_ACCESS,
                now=now,
            )

        async with sessions() as session:
            candidate_store = SqlAlchemyPersonalDevCandidateStore(session)
            claimed = await _claim_candidate_build(
                candidate_store,
                candidate_id=first.id,
                builder_id="environment-test-builder",
                now=now,
            )
            assert claimed.build_attempt is not None
            running = await candidate_store.start_build(
                attempt_id=claimed.build_attempt.id,
                builder_id="environment-test-builder",
                lease_epoch=claimed.build_attempt.lease_epoch,
                now=now,
            )
            await candidate_store.finish_build(
                attempt_id=running.id,
                builder_id="environment-test-builder",
                lease_epoch=running.lease_epoch,
                now=now,
                publication=_publication(claimed.candidate, now),
            )

        async with sessions() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(session)
            reconcile_claim = await authority.claim_next_reconciliation(
                reconciler_id="environment-test-reconciler",
                now=now,
                lease_seconds=60,
            )
            assert reconcile_claim is not None
            if reconcile_claim.operation.id != create.operation.id:
                assert reconcile_claim.candidate.status == "failed"
                await authority.fail_pre_activation(
                    operation_id=reconcile_claim.operation.id,
                    operation_epoch=reconcile_claim.operation.operation_epoch,
                    attempt_id=reconcile_claim.attempt.id,
                    reconciler_id="environment-test-reconciler",
                    lease_epoch=reconcile_claim.attempt.lease_epoch,
                    failure_reason="candidate_build_failed",
                    now=now,
                )
                reconcile_claim = await authority.claim_next_reconciliation(
                    reconciler_id="environment-test-reconciler",
                    now=now,
                    lease_seconds=60,
                )
                assert reconcile_claim is not None
            assert reconcile_claim.candidate.id == first.id
            assert (
                await authority.claim_next_reconciliation(
                    reconciler_id="competing-reconciler",
                    now=now,
                    lease_seconds=60,
                )
                is None
            )
            heartbeat = await authority.heartbeat_reconciliation(
                operation_id=create.operation.id,
                operation_epoch=1,
                attempt_id=reconcile_claim.attempt.id,
                reconciler_id="environment-test-reconciler",
                lease_epoch=reconcile_claim.attempt.lease_epoch,
                now=now + timedelta(seconds=30),
                lease_seconds=60,
            )
            assert heartbeat.lease_expires_at == now + timedelta(seconds=90)
            assert (
                await authority.claim_next_reconciliation(
                    reconciler_id="competing-reconciler",
                    now=now + timedelta(seconds=61),
                    lease_seconds=60,
                )
                is None
            )
            with pytest.raises(PersonalDevEnvironmentOperationFencedError):
                await authority.begin_activation(
                    operation_id=create.operation.id,
                    operation_epoch=1,
                    attempt_id=reconcile_claim.attempt.id,
                    reconciler_id="competing-reconciler",
                    lease_epoch=reconcile_claim.attempt.lease_epoch,
                    readiness_evidence_sha256="4" * 64,
                    now=now + timedelta(seconds=70),
                )
            activation = await authority.begin_activation(
                operation_id=create.operation.id,
                operation_epoch=1,
                attempt_id=reconcile_claim.attempt.id,
                reconciler_id="environment-test-reconciler",
                lease_epoch=reconcile_claim.attempt.lease_epoch,
                readiness_evidence_sha256="4" * 64,
                now=now + timedelta(seconds=70),
            )
            activation_retry = await authority.begin_activation(
                operation_id=create.operation.id,
                operation_epoch=1,
                attempt_id=reconcile_claim.attempt.id,
                reconciler_id="environment-test-reconciler",
                lease_epoch=reconcile_claim.attempt.lease_epoch,
                readiness_evidence_sha256="4" * 64,
                now=now + timedelta(seconds=70),
            )
            assert activation.acquired is True
            assert activation_retry.acquired is False
            activation_intent = await SqlAlchemyPersonalDevActivationIntentReader(
                session,
            ).next_intent()
            assert activation_intent is not None
            assert activation_intent.operation_id == activation.operation.id
            assert activation_intent.candidate_publication_sha256 == (
                reconcile_claim.candidate.publication_sha256
            )
            assert set(activation_intent.images) == set(PERSONAL_DEV_COMPONENTS)
            activation_signer = PersonalDevActivationSigner(
                keys={"personal-dev-agent-v1": b"k" * 32},
            )
            activation_verifier = PersonalDevActivationVerifier(
                keys={
                    "personal-dev-agent-v1": activation_signer.public_key_bytes(
                        "personal-dev-agent-v1",
                    ),
                },
            )
            activation_acknowledgement = PersonalDevActivationAcknowledgement(
                environment_name=activation.operation.environment_name,
                subject_id=activation.operation.subject_id,
                subject_incarnation=activation.operation.subject_incarnation,
                operation_id=activation.operation.id,
                operation_epoch=activation.operation.operation_epoch,
                attempt_id=activation.operation.attempt_id,
                candidate_id=activation.operation.candidate_id,
                candidate_sha=activation.operation.candidate_sha,
                deployment_generation=activation.operation.deployment_generation,
                readiness_evidence_sha256="4" * 64,
                local_activation_sha256="5" * 64,
                agent_key_id="personal-dev-agent-v1",
                observed_at=now + timedelta(seconds=70),
            )
            verified_acknowledgement = activation_verifier.verify(
                activation_acknowledgement,
                signature=activation_signer.sign(activation_acknowledgement),
                now=now + timedelta(seconds=70),
            )
            acknowledgement = await authority.acknowledge_activation(
                verified=verified_acknowledgement,
                now=now + timedelta(seconds=70),
            )
            acknowledgement_retry = await authority.acknowledge_activation(
                verified=verified_acknowledgement,
                now=now + timedelta(seconds=70),
            )
            assert acknowledgement.acquired is True
            assert acknowledgement_retry.acquired is False
            completion_claim = await authority.claim_next_reconciliation(
                reconciler_id="environment-test-reconciler",
                now=now + timedelta(seconds=70),
                lease_seconds=60,
            )
            assert completion_claim is not None
            reporter_incarnation = uuid4()
            await authority.prepare_capacity_projection(
                operation_id=create.operation.id,
                operation_epoch=1,
                attempt_id=completion_claim.attempt.id,
                reconciler_id="environment-test-reconciler",
                lease_epoch=completion_claim.attempt.lease_epoch,
                expected_configuration_epoch=1,
                projection_request_sha256="6" * 64,
                reporter_incarnation=reporter_incarnation,
                reporter_token_sha256="7" * 64,
                protected_admission_sha256="8" * 64,
                capacity_agent_installation_sha256="9" * 64,
                supported_pool_ids=("gb10", "oldlab"),
                supported_architectures=("arm64", "x86_64"),
                now=now + timedelta(seconds=70),
            )
            projection_claim = await authority.claim_next_reconciliation(
                reconciler_id="environment-test-reconciler",
                now=now + timedelta(seconds=70),
                lease_seconds=60,
            )
            assert projection_claim is not None
            await authority.record_capacity_projection(
                operation_id=create.operation.id,
                operation_epoch=1,
                attempt_id=projection_claim.attempt.id,
                reconciler_id="environment-test-reconciler",
                lease_epoch=projection_claim.attempt.lease_epoch,
                result=PersonalDevCapacityProjectionResult(
                    configuration_epoch=2,
                    configuration_digest="a" * 64,
                    subject_id=create.operation.subject_id,
                    subject_incarnation=create.operation.subject_incarnation,
                    configuration_generation=1,
                    deployment_generation=1,
                    reporter_incarnation=reporter_incarnation,
                    replayed=False,
                ),
                now=now + timedelta(seconds=70),
            )
            completion_claim = await authority.claim_next_reconciliation(
                reconciler_id="environment-test-reconciler",
                now=now + timedelta(seconds=70),
                lease_seconds=60,
            )
            assert completion_claim is not None
            ready = await authority.complete_activation(
                operation_id=create.operation.id,
                operation_epoch=1,
                attempt_id=completion_claim.attempt.id,
                reconciler_id="environment-test-reconciler",
                lease_epoch=completion_claim.attempt.lease_epoch,
                now=now + timedelta(seconds=70),
            )
            ready_retry = await authority.complete_activation(
                operation_id=create.operation.id,
                operation_epoch=1,
                attempt_id=completion_claim.attempt.id,
                reconciler_id="environment-test-reconciler",
                lease_epoch=completion_claim.attempt.lease_epoch,
                now=now,
            )
            assert ready.environment.status == "ready"
            assert ready.environment.candidate_id == first.id
            assert ready_retry.acquired is False

            with pytest.raises(DBAPIError):
                await session.execute(
                    text(
                        "UPDATE dev_instances SET "
                        "capacity_configuration_epoch = NULL, "
                        "capacity_configuration_sha256 = NULL, "
                        "capacity_reporter_incarnation = NULL, "
                        "capacity_reporter_token_sha256 = NULL, "
                        "local_activation_sha256 = NULL, "
                        "protected_admission_sha256 = NULL, "
                        "capacity_agent_installation_sha256 = NULL "
                        "WHERE name = 'bob'"
                    )
                )
            await session.rollback()
            with pytest.raises(DBAPIError):
                await session.execute(
                    text(
                        "UPDATE dev_lifecycle_operations "
                        "SET local_activation_sha256 = NULL WHERE id = :operation_id"
                    ),
                    {"operation_id": create.operation.id},
                )
            await session.rollback()

        async with sessions() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(session)
            capacity = await authority.apply(
                PersonalDevEnvironmentApplyRequest(
                    name="bob",
                    owner_user_id=owner_id,
                    owner_team_id=team_id,
                    candidate_id=first.id,
                    candidate_sha=first.candidate_sha,
                    min_slots=1,
                    max_slots=4,
                    expected_operation_epoch=1,
                    idempotency_key=uuid4(),
                ),
                access_binding=_PERSONAL_DEV_ACCESS,
                now=now,
            )
            assert capacity.operation.kind == "capacity"
            assert capacity.operation.state == "running"
            assert capacity.environment.operation_epoch == 2
            assert capacity.environment.deployment_generation == 1
            assert (capacity.environment.min_slots, capacity.environment.max_slots) == (0, 2)
            assert capacity.requires_build_binding is False

            capacity_claim = await authority.claim_next_reconciliation(
                reconciler_id="environment-test-reconciler",
                now=now + timedelta(seconds=80),
                lease_seconds=60,
            )
            assert capacity_claim is not None
            await authority.prepare_capacity_projection(
                operation_id=capacity.operation.id,
                operation_epoch=2,
                attempt_id=capacity_claim.attempt.id,
                reconciler_id="environment-test-reconciler",
                lease_epoch=capacity_claim.attempt.lease_epoch,
                expected_configuration_epoch=2,
                projection_request_sha256="b" * 64,
                reporter_incarnation=reporter_incarnation,
                reporter_token_sha256="7" * 64,
                protected_admission_sha256="8" * 64,
                capacity_agent_installation_sha256="9" * 64,
                supported_pool_ids=("gb10", "oldlab"),
                supported_architectures=("arm64", "x86_64"),
                now=now + timedelta(seconds=80),
            )
            capacity_projection_claim = await authority.claim_next_reconciliation(
                reconciler_id="environment-test-reconciler",
                now=now + timedelta(seconds=80),
                lease_seconds=60,
            )
            assert capacity_projection_claim is not None
            capacity = await authority.record_capacity_projection(
                operation_id=capacity.operation.id,
                operation_epoch=2,
                attempt_id=capacity_projection_claim.attempt.id,
                reconciler_id="environment-test-reconciler",
                lease_epoch=capacity_projection_claim.attempt.lease_epoch,
                result=PersonalDevCapacityProjectionResult(
                    configuration_epoch=3,
                    configuration_digest="c" * 64,
                    subject_id=capacity.operation.subject_id,
                    subject_incarnation=capacity.operation.subject_incarnation,
                    configuration_generation=2,
                    deployment_generation=1,
                    reporter_incarnation=reporter_incarnation,
                    replayed=False,
                ),
                now=now + timedelta(seconds=80),
            )
            assert capacity.operation.state == "succeeded"
            assert (capacity.environment.min_slots, capacity.environment.max_slots) == (1, 4)
            with pytest.raises(PersonalDevEnvironmentOperationFencedError):
                await authority.record_capacity_projection(
                    operation_id=capacity.operation.id,
                    operation_epoch=2,
                    attempt_id=capacity_projection_claim.attempt.id,
                    reconciler_id="environment-test-reconciler",
                    lease_epoch=capacity_projection_claim.attempt.lease_epoch,
                    result=PersonalDevCapacityProjectionResult(
                        configuration_epoch=3,
                        configuration_digest="c" * 64,
                        subject_id=uuid4(),
                        subject_incarnation=capacity.operation.subject_incarnation,
                        configuration_generation=2,
                        deployment_generation=1,
                        reporter_incarnation=reporter_incarnation,
                        replayed=True,
                    ),
                    now=now + timedelta(seconds=80),
                )
            with pytest.raises(PersonalDevEnvironmentOperationFencedError):
                await authority.complete_activation(
                    operation_id=capacity.operation.id,
                    operation_epoch=2,
                    attempt_id=capacity_projection_claim.attempt.id,
                    reconciler_id="environment-test-reconciler",
                    lease_epoch=capacity_projection_claim.attempt.lease_epoch,
                    now=now + timedelta(seconds=80),
                )

            noop_request = PersonalDevEnvironmentApplyRequest(
                name="bob",
                owner_user_id=owner_id,
                owner_team_id=team_id,
                candidate_id=first.id,
                candidate_sha=first.candidate_sha,
                min_slots=1,
                max_slots=4,
                expected_operation_epoch=2,
                idempotency_key=uuid4(),
            )
            noop = await authority.apply(
                noop_request,
                access_binding=_PERSONAL_DEV_ACCESS,
                now=now,
            )
            repeated_noop = await authority.apply(
                replace(noop_request, idempotency_key=uuid4()),
                access_binding=_PERSONAL_DEV_ACCESS,
                now=now,
            )
            assert noop.operation.kind == "noop"
            assert repeated_noop.acquired is False
            assert repeated_noop.operation.id == noop.operation.id

            update = await authority.apply(
                PersonalDevEnvironmentApplyRequest(
                    name="bob",
                    owner_user_id=owner_id,
                    owner_team_id=team_id,
                    candidate_id=second.id,
                    candidate_sha=second.candidate_sha,
                    min_slots=0,
                    max_slots=3,
                    expected_operation_epoch=2,
                    idempotency_key=uuid4(),
                ),
                access_binding=_PERSONAL_DEV_ACCESS,
                now=now,
            )
            assert update.operation.kind == "update"
            assert update.operation.deployment_generation == 2
            assert update.environment.status == "updating"
            assert update.environment.candidate_id == first.id
            assert update.environment.candidate_sha == first.candidate_sha
            assert (update.environment.min_slots, update.environment.max_slots) == (1, 4)
    finally:
        await engine.dispose()


@pytest.mark.parametrize("keep_data", [False, True])
async def test_personal_dev_destroy_is_manager_first_replayable_and_checkpointed(
    postgres_url: str,
    keep_data: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id = uuid4()
    team_id = uuid4()
    candidate_id = uuid4()
    subject_id = uuid4()
    subject_incarnation = uuid4()
    name = f"d{uuid4().hex[:10]}"
    now = datetime.now(UTC)
    publication = {
        "protocol_versions": {
            "capacity-agent": "v1",
            "claim-guard": "v1",
            "control-plane-worker": "v1",
        }
    }
    try:
        async with sessions() as session:
            session.add(Team(id=team_id, name=f"destroy-{team_id}"))
            session.add(
                User(
                    id=owner_id,
                    email=f"{owner_id}@example.test",
                    username=f"user-{owner_id}",
                    username_normalized=f"user-{owner_id}",
                    status="active",
                )
            )
            await session.flush()
            session.add(
                PersonalDevCandidate(
                    id=candidate_id,
                    owner_user_id=owner_id,
                    owner_team_id=team_id,
                    candidate_sha="a" * 64,
                    source_sha256="b" * 64,
                    archive_sha256="c" * 64,
                    build_contract_sha256="d" * 64,
                    source_commit="e" * 40,
                    dirty=True,
                    manifest_json={"schema_version": 1},
                    object_bucket="artifacts",
                    object_key=(
                        f"personal-dev/sources/{team_id}/{owner_id}/{'a' * 64}/{'c' * 64}.tar"
                    ),
                    source_generation_id=candidate_id,
                    archive_size_bytes=10240,
                    status="ready",
                    image_manifest_digest="sha256:" + "1" * 64,
                    publication_json=publication,
                    publication_sha256="f" * 64,
                    created_at=now,
                    updated_at=now,
                    ready_at=now,
                )
            )
            await session.flush()
            session.add(
                DevInstance(
                    name=name,
                    subject_id=subject_id,
                    subject_incarnation=subject_incarnation,
                    owner_user_id=owner_id,
                    owner_team_id=team_id,
                    min_slots=0,
                    max_slots=2,
                    status="ready",
                    deployment_generation=1,
                    candidate_id=candidate_id,
                    candidate_sha="a" * 64,
                    capacity_namespace=f"loom-dev-{name}",
                    capacity_database=f"loom_dev_{name}",
                    operation_epoch=4,
                    operation_id=uuid4(),
                    operation_step="complete",
                    capacity_configuration_epoch=7,
                    capacity_configuration_sha256="7" * 64,
                    capacity_reporter_incarnation=uuid4(),
                    capacity_reporter_token_sha256="6" * 64,
                    local_activation_sha256="5" * 64,
                    protected_admission_sha256="8" * 64,
                    capacity_agent_installation_sha256="9" * 64,
                    capacity_supported_pool_ids=["gb10", "oldlab"],
                    capacity_supported_architectures=["arm64", "x86_64"],
                    keep_data=False,
                    created_at=now,
                    updated_at=now,
                    ready_at=now,
                )
            )
            await session.commit()

        key = uuid4()
        request = PersonalDevEnvironmentDestroyRequest(
            name=name,
            owner_user_id=owner_id,
            owner_team_id=team_id,
            expected_operation_epoch=4,
            idempotency_key=key,
            keep_data=keep_data,
        )
        async with sessions() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(session)
            destroyed = await authority.destroy(
                request,
                access_binding=_PERSONAL_DEV_ACCESS,
                now=now,
            )
            replay = await authority.destroy(
                request,
                access_binding=_PERSONAL_DEV_ACCESS,
                now=now,
            )
            assert destroyed.acquired is True
            assert replay.acquired is False
            assert destroyed.operation.kind == "destroy"
            assert destroyed.environment.status == "deleting"
            assert destroyed.operation.checkpoint == "capacity_retirement_requested"

            claim = await authority.claim_next_reconciliation(
                reconciler_id="destroy-test",
                now=now,
                lease_seconds=60,
            )
            assert claim is not None and claim.operation.id == destroyed.operation.id
            await authority.prepare_capacity_projection(
                operation_id=destroyed.operation.id,
                operation_epoch=5,
                attempt_id=claim.attempt.id,
                reconciler_id="destroy-test",
                lease_epoch=claim.attempt.lease_epoch,
                expected_configuration_epoch=100,
                projection_request_sha256="1" * 64,
                reporter_incarnation=destroyed.operation.capacity_reporter_incarnation,  # type: ignore[arg-type]
                reporter_token_sha256="6" * 64,
                protected_admission_sha256="8" * 64,
                capacity_agent_installation_sha256="9" * 64,
                supported_pool_ids=("gb10", "oldlab"),
                supported_architectures=("arm64", "x86_64"),
                now=now,
            )
            claim = await authority.claim_next_reconciliation(
                reconciler_id="destroy-test",
                now=now,
                lease_seconds=60,
            )
            assert claim is not None and claim.operation.id == destroyed.operation.id
            retired = await authority.record_capacity_projection(
                operation_id=destroyed.operation.id,
                operation_epoch=5,
                attempt_id=claim.attempt.id,
                reconciler_id="destroy-test",
                lease_epoch=claim.attempt.lease_epoch,
                result=PersonalDevCapacityProjectionResult(
                    configuration_epoch=101,
                    configuration_digest="2" * 64,
                    subject_id=subject_id,
                    subject_incarnation=subject_incarnation,
                    configuration_generation=5,
                    deployment_generation=1,
                    reporter_incarnation=destroyed.operation.capacity_reporter_incarnation,  # type: ignore[arg-type]
                    replayed=False,
                ),
                now=now,
            )
            assert retired.operation.checkpoint == "capacity_retired"

            transitions = [
                ("capacity_retired", "local_authority_sealed"),
                ("local_authority_sealed", "namespace_deleted"),
            ]
            if not keep_data:
                transitions.extend(
                    [
                        ("namespace_deleted", "database_deleted"),
                        ("database_deleted", "buckets_deleted"),
                        ("buckets_deleted", "tenant_deleted"),
                    ]
                )
            else:
                transitions.append(("namespace_deleted", "tenant_deleted"))
            transitions.append(("tenant_deleted", "complete"))
            for expected, checkpoint in transitions:
                claim = await authority.claim_next_reconciliation(
                    reconciler_id="destroy-test",
                    now=now,
                    lease_seconds=60,
                )
                assert claim is not None and claim.operation.checkpoint == expected
                result = await authority.advance_destroy_checkpoint(
                    operation_id=destroyed.operation.id,
                    operation_epoch=5,
                    attempt_id=claim.attempt.id,
                    reconciler_id="destroy-test",
                    lease_epoch=claim.attempt.lease_epoch,
                    expected_checkpoint=expected,
                    checkpoint=checkpoint,
                    now=now,
                )
            assert result.operation.state == "succeeded"
            assert result.environment.status == "deleted"
            assert result.environment.deleted_at == now

        async with sessions() as session:
            await session.execute(
                text(
                    "UPDATE dev_instances SET capacity_supported_pool_ids = "
                    "'[\"untrusted\"]'::jsonb WHERE name = :name"
                ),
                {"name": name},
            )
            await session.commit()
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(session)
            with pytest.raises(
                PersonalDevEnvironmentOperationFencedError,
                match="capacity capability evidence is invalid",
            ):
                await authority.get(name)
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    ("unsafe_model", "unsafe_evidence"),
    (
        pytest.param(
            DevLifecycleOperation,
            (
                ("readiness_evidence_sha256", "1" * 64),
                ("activation_acknowledgement_sha256", "2" * 64),
                ("local_activation_sha256", "3" * 64),
                ("capacity_expected_configuration_epoch", 1),
                ("capacity_projection_request_sha256", "4" * 64),
                ("capacity_configuration_epoch", 1),
                ("capacity_configuration_sha256", "5" * 64),
                ("capacity_reporter_incarnation", uuid4()),
                ("capacity_reporter_token_sha256", "6" * 64),
                ("protected_admission_sha256", "7" * 64),
                ("capacity_agent_installation_sha256", "8" * 64),
                ("capacity_supported_pool_ids", ["gb10"]),
                ("capacity_supported_architectures", ["x86_64"]),
            ),
            id="operation-evidence",
        ),
        pytest.param(
            DevInstance,
            (
                ("capacity_configuration_epoch", 1),
                ("capacity_configuration_sha256", "1" * 64),
                ("capacity_reporter_incarnation", uuid4()),
                ("capacity_reporter_token_sha256", "2" * 64),
                ("local_activation_sha256", "3" * 64),
                ("protected_admission_sha256", "4" * 64),
                ("capacity_agent_installation_sha256", "5" * 64),
                ("capacity_supported_pool_ids", ["gb10"]),
                ("capacity_supported_architectures", ["x86_64"]),
            ),
            id="environment-evidence",
        ),
    ),
)
async def test_personal_dev_destroy_abandons_only_failed_pre_activation_create(
    postgres_url: str,
    unsafe_model: type[DevLifecycleOperation] | type[DevInstance],
    unsafe_evidence: tuple[tuple[str, object], ...],
) -> None:
    """A failed initial build can retire without invented teardown evidence."""
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id = uuid4()
    team_id = uuid4()
    other_team_id = uuid4()
    candidate_id = uuid4()
    now = datetime.now(UTC)
    name = f"a{uuid4().hex[:10]}"
    candidate = PersonalDevCandidateRecord(
        id=candidate_id,
        owner_user_id=owner_id,
        owner_team_id=team_id,
        candidate_sha="a" * 64,
        source_sha256="b" * 64,
        archive_sha256="c" * 64,
        build_contract_sha256="d" * 64,
        source_commit="e" * 40,
        dirty=False,
        manifest_json={"schema_version": 1, "attestation_scope": "personal-dev-only"},
        object_bucket="artifacts",
        object_key=(
            f"personal-dev/sources/{team_id}/{owner_id}/{'a' * 64}/{candidate_id}/{'c' * 64}.tar"
        ),
        source_generation_id=candidate_id,
        archive_size_bytes=10240,
        status="uploaded",
        created_at=now,
        updated_at=now,
    )
    try:
        async with sessions() as session:
            session.add(Team(id=team_id, name=f"abandon-{team_id}"))
            session.add(Team(id=other_team_id, name=f"abandon-other-{other_team_id}"))
            session.add(
                User(
                    id=owner_id,
                    email=f"{owner_id}@example.test",
                    username=f"user-{owner_id}",
                    username_normalized=f"user-{owner_id}",
                    status="active",
                )
            )
            await session.commit()
            await SqlAlchemyPersonalDevCandidateStore(session).register(candidate)

        async with sessions() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(session)
            created = await authority.apply(
                PersonalDevEnvironmentApplyRequest(
                    name=name,
                    owner_user_id=owner_id,
                    owner_team_id=team_id,
                    candidate_id=candidate_id,
                    candidate_sha=candidate.candidate_sha,
                    min_slots=0,
                    max_slots=1,
                    expected_operation_epoch=0,
                    idempotency_key=uuid4(),
                ),
                access_binding=_PERSONAL_DEV_ACCESS,
                now=now,
            )

        async with sessions() as session:
            candidates = SqlAlchemyPersonalDevCandidateStore(session)
            claim = await _claim_candidate_build(
                candidates,
                candidate_id=candidate_id,
                builder_id="abandon-test-builder",
                now=now,
            )
            assert claim.build_attempt is not None
            running = await candidates.start_build(
                attempt_id=claim.build_attempt.id,
                builder_id="abandon-test-builder",
                lease_epoch=claim.build_attempt.lease_epoch,
                now=now,
            )
            await candidates.finish_build(
                attempt_id=running.id,
                builder_id="abandon-test-builder",
                lease_epoch=running.lease_epoch,
                now=now,
                failure_reason="build failed",
            )

        async with sessions() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(session)
            claim = await authority.claim_next_reconciliation(
                reconciler_id="abandon-test-reconciler",
                now=now,
                lease_seconds=60,
            )
            assert claim is not None
            if claim.operation.id != created.operation.id:
                assert claim.candidate.status == "failed"
                await authority.fail_pre_activation(
                    operation_id=claim.operation.id,
                    operation_epoch=claim.operation.operation_epoch,
                    attempt_id=claim.attempt.id,
                    reconciler_id="abandon-test-reconciler",
                    lease_epoch=claim.attempt.lease_epoch,
                    failure_reason="candidate_build_failed",
                    now=now,
                )
                claim = await authority.claim_next_reconciliation(
                    reconciler_id="abandon-test-reconciler",
                    now=now,
                    lease_seconds=60,
                )
                assert claim is not None
            assert claim.operation.id == created.operation.id
            failed = await authority.fail_pre_activation(
                operation_id=created.operation.id,
                operation_epoch=created.operation.operation_epoch,
                attempt_id=claim.attempt.id,
                reconciler_id="abandon-test-reconciler",
                lease_epoch=claim.attempt.lease_epoch,
                failure_reason="candidate_build_failed",
                now=now,
            )
            assert failed.environment.status == "failed"
            assert failed.operation.failure_reason == "candidate_build_failed"

            base_columns = (
                "id, idempotency_key, environment_name, subject_id, subject_incarnation, "
                "owner_user_id, owner_team_id, operation_epoch, expected_operation_epoch, "
                "kind, state, attempt_id, attempt_sequence, request_sha256, candidate_id, "
                "candidate_sha, min_slots, max_slots, deployment_generation, keep_data, "
                "checkpoint, created_at, updated_at, started_at, finished_at"
            )
            base_values = (
                ":id, :key, :name, :subject_id, :subject_incarnation, :owner_id, :team_id, "
                "2, 1, 'destroy', 'succeeded', :attempt_id, 0, :request_sha256, "
                ":candidate_id, :candidate_sha, 0, 1, 1, false, :checkpoint, "
                ":now, :now, :now, :now"
            )
            failed_base_values = base_values.replace("'succeeded'", "'failed'")
            bindings = {
                "id": uuid4(),
                "key": uuid4(),
                "name": name,
                "subject_id": created.environment.subject_id,
                "subject_incarnation": created.environment.subject_incarnation,
                "owner_id": owner_id,
                "team_id": team_id,
                "attempt_id": uuid4(),
                "request_sha256": "f" * 64,
                "candidate_id": candidate_id,
                "candidate_sha": candidate.candidate_sha,
                "now": now,
            }
            with pytest.raises(DBAPIError):
                await session.execute(
                    text(
                        "INSERT INTO dev_lifecycle_operations ("
                        f"{base_columns}) VALUES ({base_values})"
                    ),
                    {**bindings, "checkpoint": "complete"},
                )
            await session.rollback()
            with pytest.raises(DBAPIError):
                await session.execute(
                    text(
                        "INSERT INTO dev_lifecycle_operations ("
                        f"{base_columns}) VALUES ({failed_base_values})"
                    ),
                    {**bindings, "checkpoint": "pre_activation_abandoned"},
                )
            await session.rollback()
            schema_unsafe_evidence = {
                "readiness_evidence_sha256": "1" * 64,
                "activation_acknowledgement_sha256": "2" * 64,
                "local_activation_sha256": "3" * 64,
                "capacity_expected_configuration_epoch": 1,
                "capacity_projection_request_sha256": "4" * 64,
                "capacity_configuration_epoch": 1,
                "capacity_configuration_sha256": "5" * 64,
                "capacity_reporter_incarnation": uuid4(),
                "capacity_reporter_token_sha256": "6" * 64,
                "protected_admission_sha256": "7" * 64,
                "capacity_agent_installation_sha256": "8" * 64,
                "capacity_supported_pool_ids": '["gb10"]',
                "capacity_supported_architectures": '["x86_64"]',
            }
            for field, value in schema_unsafe_evidence.items():
                with pytest.raises(DBAPIError):
                    await session.execute(
                        text(
                            "INSERT INTO dev_lifecycle_operations ("
                            f"{base_columns}, {field}) VALUES ({base_values}, :unsafe)"
                        ),
                        {
                            **bindings,
                            "id": uuid4(),
                            "key": uuid4(),
                            "attempt_id": uuid4(),
                            "checkpoint": "pre_activation_abandoned",
                            "unsafe": value,
                        },
                    )
                await session.rollback()
            coherent_capacity_evidence = {
                "local_activation_sha256": "1" * 64,
                "capacity_expected_configuration_epoch": 7,
                "capacity_projection_request_sha256": "2" * 64,
                "capacity_configuration_epoch": 8,
                "capacity_configuration_sha256": "3" * 64,
                "capacity_reporter_incarnation": uuid4(),
                "capacity_reporter_token_sha256": "4" * 64,
                "protected_admission_sha256": "5" * 64,
                "capacity_agent_installation_sha256": "6" * 64,
                "capacity_supported_pool_ids": '["gb10"]',
                "capacity_supported_architectures": '["x86_64"]',
            }
            with pytest.raises(DBAPIError):
                await session.execute(
                    text(
                        "INSERT INTO dev_lifecycle_operations ("
                        f"{base_columns}, {', '.join(coherent_capacity_evidence)}) "
                        f"VALUES ({base_values}, "
                        f"{', '.join(f':{field}' for field in coherent_capacity_evidence)})"
                    ),
                    {
                        **bindings,
                        "id": uuid4(),
                        "key": uuid4(),
                        "attempt_id": uuid4(),
                        "checkpoint": "pre_activation_abandoned",
                        **coherent_capacity_evidence,
                    },
                )
            await session.rollback()

        async with sessions() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(session)
            request = PersonalDevEnvironmentDestroyRequest(
                name=name,
                owner_user_id=owner_id,
                owner_team_id=team_id,
                expected_operation_epoch=1,
                idempotency_key=uuid4(),
                keep_data=True,
            )
            for field, value in (
                ("failure_reason", "provisioning_failed"),
                ("checkpoint", "candidate_build"),
            ):
                await session.execute(
                    text(
                        "UPDATE dev_lifecycle_operations SET "
                        f"{field} = :value WHERE id = :operation_id"
                    ),
                    {"value": value, "operation_id": created.operation.id},
                )
                with pytest.raises(PersonalDevEnvironmentConflictError):
                    await authority.destroy(
                        replace(request, idempotency_key=uuid4()),
                        access_binding=_PERSONAL_DEV_ACCESS,
                        now=now,
                    )
            for field, value in unsafe_evidence:
                row = await session.get(
                    unsafe_model,
                    created.operation.id if unsafe_model is DevLifecycleOperation else name,
                )
                assert row is not None
                setattr(row, field, value)
                with session.no_autoflush, pytest.raises(PersonalDevEnvironmentConflictError):
                    await authority.destroy(
                        replace(request, idempotency_key=uuid4()),
                        access_binding=_PERSONAL_DEV_ACCESS,
                        now=now,
                    )
            with pytest.raises(PersonalDevEnvironmentEpochFencedError):
                await authority.destroy(
                    replace(request, expected_operation_epoch=2, idempotency_key=uuid4()),
                    access_binding=_PERSONAL_DEV_ACCESS,
                    now=now,
                )
            abandoned = await authority.destroy(
                request,
                access_binding=_PERSONAL_DEV_ACCESS,
                now=now,
            )
            replay = await authority.destroy(
                request,
                access_binding=_PERSONAL_DEV_ACCESS,
                now=now,
            )
            assert abandoned.acquired is True
            assert replay.acquired is False
            assert abandoned.operation.kind == "destroy"
            assert abandoned.operation.state == "succeeded"
            assert abandoned.operation.checkpoint == "pre_activation_abandoned"
            assert abandoned.operation.operation_epoch == 2
            assert abandoned.operation.subject_id == created.operation.subject_id
            assert abandoned.operation.subject_incarnation == created.operation.subject_incarnation
            assert abandoned.environment.status == "deleted"
            assert abandoned.environment.candidate_id is None
            assert abandoned.environment.deleted_at == now
            assert (await authority.get_operation(created.operation.id)).state == "failed"  # type: ignore[union-attr]
            assert (
                await authority.claim_next_reconciliation(
                    reconciler_id="abandon-test-no-reconciliation",
                    now=now,
                    lease_seconds=60,
                )
                is None
            )
            terminal_attempt = (
                await session.execute(
                    text(
                        "SELECT state, checkpoint FROM dev_lifecycle_operation_attempts "
                        "WHERE operation_id = :operation_id"
                    ),
                    {"operation_id": abandoned.operation.id},
                )
            ).one()
            assert tuple(terminal_attempt) == ("succeeded", "pre_activation_abandoned")
            await session.execute(
                text("UPDATE dev_instances SET owner_team_id = :team_id WHERE name = :name"),
                {"team_id": other_team_id, "name": name},
            )
            await session.commit()
            try:
                with pytest.raises(PersonalDevEnvironmentOperationFencedError):
                    await authority.destroy(
                        request,
                        access_binding=_PERSONAL_DEV_ACCESS,
                        now=now,
                    )
            finally:
                await session.execute(
                    text("UPDATE dev_instances SET owner_team_id = :team_id WHERE name = :name"),
                    {"team_id": team_id, "name": name},
                )
                await session.commit()

        async with sessions() as session:
            candidates = SqlAlchemyPersonalDevCandidateStore(session)
            eligible = (
                await session.execute(
                    select(PersonalDevCandidate.id).where(
                        PersonalDevCandidate.id == candidate_id,
                        PersonalDevCandidate.artifact_state == "retained",
                        PersonalDevCandidate.artifact_gc_unreferenced_at.is_(None),
                        PersonalDevCandidate.artifact_gc_blocked_reason.is_(None),
                        PersonalDevCandidate.status.in_(("uploaded", "ready", "failed")),
                        candidates._artifact_unreferenced(),
                    )
                )
            ).scalar_one_or_none()
            assert eligible == candidate_id
        async with sessions() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(session)
            redeployed = await authority.apply(
                PersonalDevEnvironmentApplyRequest(
                    name=name,
                    owner_user_id=owner_id,
                    owner_team_id=team_id,
                    candidate_id=candidate_id,
                    candidate_sha=candidate.candidate_sha,
                    min_slots=0,
                    max_slots=1,
                    expected_operation_epoch=2,
                    idempotency_key=uuid4(),
                ),
                access_binding=_PERSONAL_DEV_ACCESS,
                now=now,
            )
            assert redeployed.environment.status == "provisioning"
            assert redeployed.environment.keep_data is False
            redeployed_row = await session.get(DevInstance, name)
            assert redeployed_row is not None
            assert redeployed_row.capacity_namespace == f"loom-dev-{name}"
            assert redeployed_row.capacity_database == f"loom_dev_{name}"
        downgrade = subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini", "downgrade", "0121"],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
        )
        assert downgrade.returncode != 0
        assert "cannot downgrade 0122 with pre-activation abandonment records" in (
            downgrade.stdout + downgrade.stderr
        )
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


@pytest.mark.parametrize(
    ("capacity_namespace", "capacity_database"),
    (
        pytest.param(None, None, id="missing"),
        pytest.param("loom-dev-other", "loom_dev_other", id="mismatched"),
    ),
)
def test_dev_instance_capacity_coordinates_are_derived_from_personal_name(
    postgres_url: str,
    capacity_namespace: str | None,
    capacity_database: str | None,
) -> None:
    """Direct SQL cannot omit or misbind coordinates for a personal environment."""

    engine = create_engine(postgres_url)
    user_id = uuid4()
    team_id = uuid4()
    candidate_id = uuid4()
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO users (id, username, username_normalized) "
                    "VALUES (:id, 'coordinate-user', 'coordinate-user')"
                ),
                {"id": user_id},
            )
            connection.execute(
                text("INSERT INTO teams (id, name) VALUES (:id, 'coordinate-team')"),
                {"id": team_id},
            )
            connection.execute(
                text(
                    "INSERT INTO personal_dev_candidates "
                    "(id, owner_user_id, owner_team_id, candidate_sha, source_sha256, "
                    "archive_sha256, build_contract_sha256, source_commit, dirty, "
                    "manifest_json, object_bucket, object_key, source_generation_id, "
                    "archive_size_bytes) VALUES "
                    "(:candidate_id, :user_id, :team_id, repeat('a', 64), repeat('b', 64), "
                    "repeat('c', 64), repeat('d', 64), repeat('e', 40), false, "
                    "'{}'::jsonb, 'personal-dev-sources', "
                    "'personal-dev/sources/' || :team_text || '/' || :user_text || '/' || "
                    "repeat('a', 64) || '/' || repeat('c', 64) || '.tar', "
                    ":candidate_id, 1)"
                ),
                {
                    "candidate_id": candidate_id,
                    "user_id": user_id,
                    "team_id": team_id,
                    "user_text": str(user_id),
                    "team_text": str(team_id),
                },
            )
            with pytest.raises(DBAPIError):
                connection.execute(
                    text(
                        "INSERT INTO dev_instances "
                        "(name, owner_user_id, owner_team_id, max_slots, "
                        "deployment_generation, candidate_id, candidate_sha, "
                        "capacity_namespace, capacity_database, operation_id) "
                        "VALUES ('alice', :user_id, :team_id, 1, 1, :candidate_id, "
                        "repeat('a', 64), :capacity_namespace, :capacity_database, "
                        ":operation_id)"
                    ),
                    {
                        "user_id": user_id,
                        "team_id": team_id,
                        "candidate_id": candidate_id,
                        "capacity_namespace": capacity_namespace,
                        "capacity_database": capacity_database,
                        "operation_id": uuid4(),
                    },
                )
    finally:
        engine.dispose()


def test_dev_instance_capacity_coordinate_constraint_matches_model_and_migration(
    postgres_url: str,
) -> None:
    """The current fail-closed coordinate rule must match ORM and database."""

    expected_model_sql = (
        "(candidate_id IS NULL AND capacity_namespace IS NULL AND capacity_database IS NULL) "
        "OR (candidate_id IS NOT NULL AND capacity_namespace IS NOT NULL "
        "AND capacity_database IS NOT NULL "
        "AND capacity_namespace = 'loom-dev-' || name "
        "AND capacity_database = 'loom_dev_' || replace(name, '-', '_'))"
    )
    model_checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in DevInstance.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }

    assert model_checks["dev_instances_personal_capacity_identity_check"] == expected_model_sql

    engine = create_engine(postgres_url)
    try:
        with engine.connect() as connection:
            database_checks = {
                constraint["name"]: constraint["sqltext"]
                for constraint in inspect(connection).get_check_constraints("dev_instances")
            }
    finally:
        engine.dispose()

    normalized_database_sql = (
        " ".join(database_checks["dev_instances_personal_capacity_identity_check"].lower().split())
        .replace("( ", "(")
        .replace(" )", ")")
    )
    assert "candidate_id is null" in normalized_database_sql
    assert "capacity_namespace is null" in normalized_database_sql
    assert "capacity_database is null" in normalized_database_sql
    assert "candidate_id is not null" in normalized_database_sql
    assert "capacity_namespace is not null" in normalized_database_sql
    assert "capacity_database is not null" in normalized_database_sql
    assert "capacity_namespace = ('loom-dev-'::text || name)" in normalized_database_sql
    assert (
        "capacity_database = ('loom_dev_'::text || replace(name, '-'::text, '_'::text))"
        in normalized_database_sql
    )
