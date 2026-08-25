from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import (
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
        "backend_policy_snapshot",
        "backend_policy_digest",
        "selected_backend",
        "backend_selection_reason",
        "backend_selected_at",
        "backend_incompatibility_reasons",
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
    task_id = f"daytona-backend-claim/{uuid4()}"
    capability_digest = "sha256:" + "d" * 64
    token_hash = b"daytona-backend-worker-token"
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
                    hostname=f"daytona-worker-{worker_id}",
                    version="test",
                    capabilities=[
                        {
                            "backend": "daytona",
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
                        "backend": "daytona",
                        "os": "linux",
                        "cpu_arch": "x86_64",
                        "gpu_vendor": "none",
                        "network_policies": ["public"],
                    },
                    backend_policy_snapshot=_explicit_policy(),
                    backend_policy_digest="sha256:" + "e" * 64,
                    selected_backend="daytona",
                    backend_selection_reason="explicit_request",
                    backend_selected_at=now,
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
                worker_backends=["daytona"],
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


def _overflow_policy(*, delay_seconds: int = 0) -> dict[str, object]:
    return {
        "schema_version": "loom.daytona-backend-policy.v1",
        "mode": "overflow",
        "allowed_backends": ["docker", "daytona"],
        "spillover_after_queue_seconds": delay_seconds,
        "daytona_resources": {"cpu": 2, "memory_gib": 4, "disk_gib": 10},
        "daytona_price_snapshot": {
            "source": "operator-test",
            "version": "2026-08-25",
            "effective_at": "2026-08-25T00:00:00Z",
            "currency": "USD",
            "cpu_usd_per_hour": "0.10",
            "memory_gib_usd_per_hour": "0.01",
            "disk_gib_usd_per_hour": "0.001",
        },
        "max_cloud_cost_usd": "10.00",
        "max_runtime_seconds": 600,
        "max_attempts": 1,
        "expected_trial_count": 4,
        "worst_case_cloud_cost_usd": "0.168000",
        "authority": {"kind": "platform_admin", "actor": "test"},
        "accepted_at": "2026-08-25T00:00:00Z",
    }


def _explicit_policy() -> dict[str, object]:
    policy = _overflow_policy()
    policy.update(
        mode="explicit",
        allowed_backends=["daytona"],
        expected_trial_count=1,
        worst_case_cloud_cost_usd="0.042000",
    )
    return policy


@pytest.mark.parametrize("unified", [False, True], ids=["legacy", "unified"])
async def test_overflow_is_local_first_bounded_and_disabled_policy_stays_local(
    postgres_url: str,
    unified: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    task_id = f"daytona-overflow/{uuid4()}"
    docker_worker_id = uuid4()
    daytona_worker_id = uuid4()
    digest = "sha256:" + "a" * 64
    token_hashes = {
        docker_worker_id: b"overflow-docker-token",
        daytona_worker_id: b"overflow-daytona-token",
    }
    trial_ids = [uuid4() for _ in range(4)]
    now = datetime.now(UTC)
    cap_base = {
        "os": "linux",
        "cpu_arch": "x86_64",
        "gpu_vendor": "none",
        "network_policies": ["public"],
    }
    try:
        async with sessions() as session, session.begin():
            await session.execute(insert(Team).values(id=team_id, name=f"overflow-{team_id}"))
            await session.execute(insert(TeamQuota).values(team_id=team_id))
            await session.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="f" * 64,
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {
                            "os": "linux",
                            "docker_image": "registry.example/task@sha256:" + "e" * 64,
                        },
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                    },
                )
            )
            for worker_id, backend, max_concurrent in (
                (docker_worker_id, "docker", 1),
                (daytona_worker_id, "daytona", 3),
            ):
                await session.execute(
                    insert(Worker).values(
                        id=worker_id,
                        hostname=f"{backend}-{worker_id}",
                        version="test",
                        capabilities=[{**cap_base, "backend": backend}],
                        supported_work_kinds=["trial"],
                        capability_snapshot_digest=digest,
                        capability_snapshot_json={
                            "schema_version": "loom.worker-capabilities.v1",
                            "cpu_arch": "x86_64",
                            "container_runtime_features": [],
                        },
                        auth_token_hash=token_hashes[worker_id],
                        max_concurrent=max_concurrent,
                        registered_at=now,
                        last_seen_at=now,
                        status="active",
                    )
                )
            common_trial = {
                "team_id": team_id,
                "task_id": task_id,
                "config": {},
                "requires_caps": {**cap_base, "backend": "docker"},
                "state": "queued",
                "backend_policy_digest": "sha256:" + "b" * 64,
                "backend_incompatibility_reasons": [],
            }
            for trial_id in trial_ids[:2]:
                await session.execute(
                    insert(Trial).values(
                        id=trial_id,
                        submitted_at=now - timedelta(minutes=5),
                        backend_policy_snapshot=_overflow_policy(),
                        **common_trial,
                    )
                )
            await session.execute(
                insert(Trial).values(
                    id=trial_ids[2],
                    submitted_at=now,
                    backend_policy_snapshot=_overflow_policy(delay_seconds=60),
                    **common_trial,
                )
            )
            await session.execute(
                insert(Trial).values(
                    id=trial_ids[3],
                    submitted_at=now,
                    backend_policy_snapshot={
                        "schema_version": "loom.daytona-backend-policy.v1",
                        "mode": "local_only",
                        "allowed_backends": ["docker"],
                        "spillover_after_queue_seconds": 0,
                    },
                    backend_policy_digest="sha256:" + "c" * 64,
                    selected_backend="docker",
                    backend_selection_reason="policy_local_only",
                    backend_selected_at=now,
                    **{k: v for k, v in common_trial.items() if k != "backend_policy_digest"},
                )
            )

        async with sessions() as session:
            assert await _claim_trial(
                session,
                unified=unified,
                worker_id=daytona_worker_id,
                capability_digest=digest,
                token_hash=token_hashes[daytona_worker_id],
                worker_backends=["daytona"],
            ) is None

        async with sessions() as session:
            local = await _claim_trial(
                session,
                unified=unified,
                worker_id=docker_worker_id,
                capability_digest=digest,
                token_hash=token_hashes[docker_worker_id],
                worker_backends=["docker"],
            )
            assert local is not None
            assert local["selected_backend"] == "docker"
            assert local["backend_selection_reason"] == "local_capacity_available"
            await session.commit()

        async with sessions() as session:
            cloud = await _claim_trial(
                session,
                unified=unified,
                worker_id=daytona_worker_id,
                capability_digest=digest,
                token_hash=token_hashes[daytona_worker_id],
                worker_backends=["daytona"],
            )
            assert cloud is not None
            assert cloud["selected_backend"] == "daytona"
            assert cloud["backend_selection_reason"] == "spillover_threshold_met"
            await session.commit()

        async with sessions() as session:
            assert await _claim_trial(
                session,
                unified=unified,
                worker_id=daytona_worker_id,
                capability_digest=digest,
                token_hash=token_hashes[daytona_worker_id],
                worker_backends=["daytona"],
            ) is None
    finally:
        async with sessions() as session, session.begin():
            await session.execute(delete(Trial).where(Trial.id.in_(trial_ids)))
            await session.execute(delete(Worker).where(Worker.id.in_([docker_worker_id, daytona_worker_id])))
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
