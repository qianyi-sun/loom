from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import (
    ExecutionAdmissionPolicy,
    ExecutionAdmissionReservation,
    Task,
    TaskImageMaterialization,
    Team,
    TeamQuota,
    Trial,
    TrialTaskImageMaterialization,
    Worker,
)
from loom.pipeline.work_protocol import TrialClaimV1, WorkClaimV1
from loom_control_plane.scheduler.claim import claim_one, claim_work


@pytest.mark.parametrize("unified", [False, True], ids=["legacy", "unified"])
async def test_pool_admission_ceiling_is_race_safe_and_releases_on_terminal_trial(
    postgres_url: str,
    unified: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_ids = (uuid4(), uuid4())
    trial_ids = (uuid4(), uuid4())
    task_id = f"admission-race/{uuid4()}"
    pool_name = f"admission-{uuid4()}"
    capability_digest = "sha256:" + "b" * 64
    token_hash = b"admission-race-worker-token"
    now = datetime.now(UTC)
    try:
        async with sessions() as session, session.begin():
            await session.execute(insert(Team).values(id=team_id, name=f"admission-{team_id}"))
            await session.execute(insert(TeamQuota).values(team_id=team_id))
            await session.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="b" * 64,
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {
                            "os": "linux",
                            "docker_image": "registry.example/task@sha256:" + "c" * 64,
                        },
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                    },
                )
            )
            for worker_id in worker_ids:
                await session.execute(
                    insert(Worker).values(
                        id=worker_id,
                        hostname=f"admission-worker-{worker_id}",
                        version="test",
                        pool_name=pool_name,
                        capabilities=[
                            {
                                "backend": "docker",
                                "os": "linux",
                                "cpu_arch": "x86_64",
                                "gpu_vendor": "none",
                                "network_policies": ["public"],
                            }
                        ],
                        supported_work_kinds=["trial"],
                        capability_snapshot_digest=capability_digest,
                        capability_snapshot_json={
                            "schema_version": "loom.worker-capabilities.v1",
                            "cpu_arch": "x86_64",
                            "container_runtime_features": [],
                        },
                        auth_token_hash=token_hash,
                        max_concurrent=1,
                        registered_at=now,
                        last_seen_at=now,
                        status="active",
                    )
                )
            for trial_id in trial_ids:
                await session.execute(
                    insert(Trial).values(
                        id=trial_id,
                        team_id=team_id,
                        task_id=task_id,
                        config={},
                        requires_caps={
                            "backend": "docker",
                            "os": "linux",
                            "cpu_arch": "x86_64",
                            "gpu_vendor": "none",
                            "network_policies": ["public"],
                            "worker_pool": pool_name,
                        },
                        state="queued",
                    )
                )
            await session.execute(
                insert(ExecutionAdmissionPolicy).values(
                    scope_kind="pool",
                    scope_key=pool_name,
                    max_concurrent=1,
                    enabled=True,
                    reason="race test",
                )
            )

        async def claim(worker_id):  # type: ignore[no-untyped-def]
            async with sessions() as session:
                row = await _claim_trial(
                    session,
                    unified=unified,
                    worker_id=worker_id,
                    capability_digest=capability_digest,
                    token_hash=token_hash,
                    worker_backends=["docker"],
                )
                await session.commit()
                return row

        raced = await asyncio.gather(*(claim(worker_id) for worker_id in worker_ids))
        winners = [row for row in raced if row is not None]
        assert len(winners) == 1

        async with sessions() as session, session.begin():
            reservations = (
                (
                    await session.execute(
                        select(ExecutionAdmissionReservation).where(
                            ExecutionAdmissionReservation.pool_id == pool_name
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(reservations) == 1
            assert reservations[0].state == "active"
            await session.execute(
                update(Trial)
                .where(Trial.id == winners[0]["id"])
                .values(state="failed", failure_reason="test_terminal")
            )

        remaining_worker = worker_ids[0] if raced[0] is None else worker_ids[1]
        released_claim = await claim(remaining_worker)
        assert released_claim is not None
        assert released_claim["id"] in trial_ids
        assert released_claim["id"] != winners[0]["id"]
    finally:
        async with sessions() as session, session.begin():
            await session.execute(delete(Trial).where(Trial.id.in_(trial_ids)))
            await session.execute(
                delete(ExecutionAdmissionPolicy).where(
                    ExecutionAdmissionPolicy.scope_key == pool_name
                )
            )
            await session.execute(delete(Worker).where(Worker.id.in_(worker_ids)))
            await session.execute(delete(Task).where(Task.id == task_id))
            await session.execute(delete(TeamQuota).where(TeamQuota.team_id == team_id))
            await session.execute(delete(Team).where(Team.id == team_id))
        await engine.dispose()


def test_unified_trial_payload_preserves_legacy_claim_fields_exactly() -> None:
    legacy = {
        "trial_id": uuid4(),
        "team_id": uuid4(),
        "task_id": "benchmark/task",
        "config": {"agent": {"name": "codex"}},
        "requires_caps": {"os": "linux", "gpu_vendor": "none"},
        "attempt_count": 1,
        "provider_connection_id": None,
        "family_key": None,
        "family_state_uri": None,
        "family_run_spec": None,
        "state": "claimed",
    }
    claim = TrialClaimV1.model_validate(legacy)
    assert claim.task_image_materialization is None
    assert claim.model_switch_plan is None
    assert set(claim.model_dump()) == {
        *legacy,
        "task_image_materialization",
        "model_switch_plan",
    }
    envelope = WorkClaimV1(schema_version="loom.work-claim.v1", work_kind="trial", payload=claim)
    assert envelope.payload == claim


async def _claim_trial(
    session: AsyncSession,
    *,
    unified: bool,
    worker_id,  # type: ignore[no-untyped-def]
    capability_digest: str,
    token_hash: bytes,
    worker_backends: list[str] | None = None,
):
    common = {
        "worker_id": worker_id,
        "worker_os": ["linux"],
        "worker_cpu_arches": ["x86_64"],
        "worker_gpu_vendors": ["none"],
        "worker_network_policies": ["public"],
        "worker_backends": worker_backends,
    }
    if not unified:
        return await claim_one(session, **common)
    result = await claim_work(
        session,
        capability_snapshot_digest=capability_digest,
        worker_token_hash=token_hash,
        supported_work_kinds=["trial"],
        free_slots=1,
        **common,
    )
    return result[0] if result is not None else None


@pytest.mark.parametrize("unified", [False, True], ids=["legacy", "unified"])
async def test_claim_requires_exact_sandbox_backend(
    postgres_url: str,
    unified: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    task_id = f"modal-backend-claim/{uuid4()}"
    capability_digest = "sha256:" + "d" * 64
    token_hash = b"modal-backend-worker-token"
    now = datetime.now(UTC)
    try:
        async with sessions() as session, session.begin():
            await session.execute(insert(Team).values(id=team_id, name=f"backend-{team_id}"))
            await session.execute(insert(TeamQuota).values(team_id=team_id))
            await session.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="e" * 64,
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {
                            "os": "linux",
                            "docker_image": "registry.example/task@sha256:" + "f" * 64,
                        },
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                    },
                )
            )
            await session.execute(
                insert(Worker).values(
                    id=worker_id,
                    hostname=f"modal-worker-{worker_id}",
                    version="test",
                    capabilities=[
                        {
                            "backend": "modal",
                            "os": "linux",
                            "cpu_arch": "x86_64",
                            "gpu_vendor": "none",
                            "network_policies": ["public"],
                        }
                    ],
                    supported_work_kinds=["trial"],
                    capability_snapshot_digest=capability_digest,
                    capability_snapshot_json={
                        "schema_version": "loom.worker-capabilities.v1",
                        "cpu_arch": "x86_64",
                        "container_runtime_features": [],
                    },
                    auth_token_hash=token_hash,
                    max_concurrent=1,
                    registered_at=now,
                    last_seen_at=now,
                    status="active",
                )
            )
            await session.execute(
                insert(Trial).values(
                    id=trial_id,
                    team_id=team_id,
                    task_id=task_id,
                    config={},
                    requires_caps={
                        "backend": "modal",
                        "os": "linux",
                        "cpu_arch": "x86_64",
                        "gpu_vendor": "none",
                        "network_policies": ["public"],
                    },
                    state="queued",
                )
            )

        async with sessions() as session:
            wrong = await _claim_trial(
                session,
                unified=unified,
                worker_id=worker_id,
                capability_digest=capability_digest,
                token_hash=token_hash,
                worker_backends=["docker"],
            )
            assert wrong is None

        async with sessions() as session:
            claimed = await _claim_trial(
                session,
                unified=unified,
                worker_id=worker_id,
                capability_digest=capability_digest,
                token_hash=token_hash,
                worker_backends=["modal"],
            )
            assert claimed is not None and claimed["id"] == trial_id
            await session.commit()
    finally:
        async with sessions() as session, session.begin():
            await session.execute(delete(Trial).where(Trial.id == trial_id))
            await session.execute(delete(Worker).where(Worker.id == worker_id))
            await session.execute(delete(Task).where(Task.id == task_id))
            await session.execute(delete(TeamQuota).where(TeamQuota.team_id == team_id))
            await session.execute(delete(Team).where(Team.id == team_id))
        await engine.dispose()


@pytest.mark.parametrize("unified", [False, True], ids=["legacy", "unified"])
async def test_claim_waits_for_matching_architecture_task_image(
    postgres_url: str,
    unified: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    task_id = f"task-image-claim/{uuid4()}"
    materialization_ids = {"x86_64": uuid4(), "arm64": uuid4()}
    capability_digest = "sha256:" + "c" * 64
    token_hash = b"task-image-worker-token-hash"
    now = datetime.now(UTC)
    try:
        async with sessions() as session, session.begin():
            await session.execute(insert(Team).values(id=team_id, name=f"image-{team_id}"))
            await session.execute(insert(TeamQuota).values(team_id=team_id))
            await session.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="a" * 64,
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {
                            "os": "linux",
                            "cpu_arch": "any",
                            "dockerfile": "environment/Dockerfile",
                        },
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                        "steps": [{"name": "main"}],
                    },
                )
            )
            await session.execute(
                insert(Worker).values(
                    id=worker_id,
                    hostname=f"image-worker-{worker_id}",
                    version="test",
                    capabilities=[
                        {
                            "os": "linux",
                            "cpu_arch": "x86_64",
                            "gpu_vendor": "none",
                            "network_policies": ["public"],
                        }
                    ],
                    supported_work_kinds=["trial"],
                    capability_snapshot_digest=capability_digest,
                    capability_snapshot_json={
                        "schema_version": "loom.worker-capabilities.v1",
                        "cpu_arch": "x86_64",
                        "container_runtime_features": [],
                    },
                    auth_token_hash=token_hash,
                    max_concurrent=1,
                    registered_at=now,
                    last_seen_at=now,
                    status="active",
                )
            )
            await session.execute(
                insert(Trial).values(
                    id=trial_id,
                    team_id=team_id,
                    task_id=task_id,
                    config={},
                    requires_caps={
                        "os": "linux",
                        "cpu_arch": "any",
                        "gpu_vendor": "none",
                        "network_policies": ["public"],
                    },
                    state="queued",
                )
            )
            for cpu_arch, state in (("x86_64", "queued"), ("arm64", "ready")):
                await session.execute(
                    insert(TaskImageMaterialization).values(
                        id=materialization_ids[cpu_arch],
                        materialization_key=("1" if cpu_arch == "x86_64" else "2") * 64,
                        task_id=task_id,
                        task_checksum="a" * 64,
                        cpu_arch=cpu_arch,
                        task_config={},
                        state=state,
                    )
                )

        async with sessions() as session:
            assert (
                await _claim_trial(
                    session,
                    unified=unified,
                    worker_id=worker_id,
                    capability_digest=capability_digest,
                    token_hash=token_hash,
                )
                is None
            )

        async with sessions() as session, session.begin():
            for materialization_id in materialization_ids.values():
                await session.execute(
                    insert(TrialTaskImageMaterialization).values(
                        trial_id=trial_id,
                        materialization_id=materialization_id,
                    )
                )

        async with sessions() as session:
            assert (
                await _claim_trial(
                    session,
                    unified=unified,
                    worker_id=worker_id,
                    capability_digest=capability_digest,
                    token_hash=token_hash,
                )
                is None
            )

        async with sessions() as session, session.begin():
            await session.execute(
                update(TaskImageMaterialization)
                .where(TaskImageMaterialization.id == materialization_ids["x86_64"])
                .values(state="ready")
            )
            await session.execute(
                update(TaskImageMaterialization)
                .where(TaskImageMaterialization.id == materialization_ids["arm64"])
                .values(state="queued")
            )

        async with sessions() as session:
            claimed = await _claim_trial(
                session,
                unified=unified,
                worker_id=worker_id,
                capability_digest=capability_digest,
                token_hash=token_hash,
            )
            assert claimed is not None
            assert claimed["id"] == trial_id
            await session.commit()
    finally:
        async with sessions() as session, session.begin():
            await session.execute(delete(Trial).where(Trial.id == trial_id))
            await session.execute(
                delete(TaskImageMaterialization).where(TaskImageMaterialization.task_id == task_id)
            )
            await session.execute(delete(Worker).where(Worker.id == worker_id))
            await session.execute(delete(Task).where(Task.id == task_id))
            await session.execute(delete(TeamQuota).where(TeamQuota.team_id == team_id))
            await session.execute(delete(Team).where(Team.id == team_id))
        await engine.dispose()
