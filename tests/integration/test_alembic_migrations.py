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
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from loom.db.schema import Team, User
from loom.personal_dev_activation import (
    PersonalDevActivationAcknowledgement,
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
    PersonalDevBuildLeaseFencedError,
    SqlAlchemyPersonalDevCandidateStore,
)
from loom.personal_dev_environment import (
    PersonalDevAccessBinding,
    PersonalDevEnvironmentApplyRequest,
    PersonalDevLifecycleLimits,
)
from loom.personal_dev_environment_store import (
    PersonalDevEnvironmentConflictError,
    PersonalDevEnvironmentEpochFencedError,
    PersonalDevEnvironmentNotFoundError,
    PersonalDevEnvironmentOperationFencedError,
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
            [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini", "upgrade", "head"],
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
        "dev_lifecycle_operations",
        "dev_lifecycle_operation_attempts",
        "dev_lifecycle_activation_acknowledgements",
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
            "key": (
                f"personal-dev/evidence/{candidate.candidate_sha}/"
                "test/safety-evidence.json"
            ),
            "sha256": "5" * 64,
            "size_bytes": 1024,
        },
        "safety_evidence_sha256": "5" * 64,
        "publisher_identity": "system:serviceaccount:loom-dev:candidate-builder",
        "published_at": now.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    }


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


async def test_personal_dev_candidate_registration_enforces_owner_retention_quota(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id = uuid4()
    team_id = uuid4()
    now = datetime.now(UTC)

    def candidate(digit: str) -> PersonalDevCandidateRecord:
        return PersonalDevCandidateRecord(
            id=uuid4(),
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
            object_key=f"personal-dev/sources/{team_id}/{owner_id}/{digit}.tar",
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
            replay = await store.register(replace(first_request, id=uuid4()))
            assert replay.candidate.id == first.candidate.id
            with pytest.raises(ValueError, match="immutable binding"):
                await store.register(
                    replace(
                        first_request,
                        id=uuid4(),
                        candidate_sha="4" * 64,
                    )
                )
            with pytest.raises(PersonalDevCandidateQuotaError, match="count"):
                await store.register(candidate("3"))
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
                    id=uuid4(),
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
                    object_key=f"personal-dev/sources/{team_id}/{owner_id}/{digit}.tar",
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
    candidate = PersonalDevCandidateRecord(
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
        object_key=f"personal-dev/sources/{team_id}/{owner_id}/environment.tar",
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
            with pytest.raises(PersonalDevEnvironmentNotFoundError):
                await SqlAlchemyPersonalDevEnvironmentAuthority(session).apply(
                    replace(
                        request,
                        owner_user_id=other_owner,
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
        return PersonalDevCandidateRecord(
            id=uuid4(),
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
            object_key=f"personal-dev/sources/{team_id}/{owner_id}/{digit}.tar",
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
            activation_verifier = PersonalDevActivationVerifier(
                keys={"personal-dev-agent-v1": b"k" * 32},
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
                signature=activation_verifier.sign(activation_acknowledgement),
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
            assert capacity.operation.state == "succeeded"
            assert capacity.environment.operation_epoch == 2
            assert capacity.environment.deployment_generation == 1
            assert (capacity.environment.min_slots, capacity.environment.max_slots) == (1, 4)
            assert capacity.requires_build_binding is False

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
