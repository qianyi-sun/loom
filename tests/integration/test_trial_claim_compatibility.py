from __future__ import annotations

from datetime import UTC, datetime
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
    assert set(claim.model_dump()) == {*legacy, "task_image_materialization"}
    envelope = WorkClaimV1(schema_version="loom.work-claim.v1", work_kind="trial", payload=claim)
    assert envelope.payload == claim


async def _claim_trial(
    session: AsyncSession,
    *,
    unified: bool,
    worker_id,  # type: ignore[no-untyped-def]
    capability_digest: str,
    token_hash: bytes,
):
    common = {
        "worker_id": worker_id,
        "worker_os": ["linux"],
        "worker_cpu_arches": ["x86_64"],
        "worker_gpu_vendors": ["none"],
        "worker_network_policies": ["public"],
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
