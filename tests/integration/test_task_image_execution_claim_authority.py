"""Actual shared claims, not caller-asserted worker or refundable attempt identity."""

import importlib
from uuid import uuid4

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Trial, Worker
from loom.pipeline.keys import canonical_digest
from loom_control_plane.scheduler.claim import claim_work
from loom_task_image_authority.execution_grant import LegacyExecutionClaim
from tests.integration.test_trial_legacy_claim_identity import _TOKEN_HASH, _seed


def module():
    name = "loom_task_image_authority.execution_store"
    assert importlib.util.find_spec(name) is not None, "durable execution authority missing"
    return importlib.import_module(name)


async def seed(factory):
    snapshot = dict(
        schema_version="loom.worker-capabilities.v1", cpu_arch="x86_64", cpu_cores=2,
        memory_bytes=1024, scratch_bytes=1024, network_profiles=["gateway"],
        container_runtime_features=["task-image-execution-v2"], gpu_devices=[],
        input_cache_capacity_bytes=0, input_cache_reserved_bytes=0, input_cache_ready_bytes=0,
    )
    digest = canonical_digest(snapshot)
    async with factory.begin() as session:
        trial_id, worker_id = await _seed(session)
        await session.execute(update(Worker).where(Worker.id == worker_id).values(
            supported_work_kinds=["trial", "execution_attempt"],
            capability_snapshot_json=snapshot, capability_snapshot_digest=digest,
        ))
        result = await claim_work(
            session, worker_id=worker_id, capability_snapshot_digest=digest,
            worker_token_hash=_TOKEN_HASH, supported_work_kinds=["trial", "execution_attempt"],
            free_slots=1, worker_os=["linux"], worker_cpu_arches=["x86_64"],
            worker_gpu_vendors=["none"], worker_network_policies=["public"],
        )
        row, _ = result
        return LegacyExecutionClaim.model_validate(dict(
            kind="legacy", trial_id=str(trial_id), team_id=str(row["team_id"]),
            worker_id=str(worker_id), worker_lease_epoch=1,
            trial_attempt_count=row["attempt_count"], claim_id=str(row["claim_id"]),
        ))


@pytest.mark.parametrize("change", [
    "valid", "token", "claim", "epoch", "attempt", "worker", "team",
    "released", "running", "cancelled", "draining", "old_reader", "capability_drift",
])
async def test_locked_execution_claim_uses_current_registered_authority(
    isolated_migration_postgres_url, change,
):
    m = module()
    engine = create_async_engine(isolated_migration_postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        claim = await seed(factory)
        token = _TOKEN_HASH
        if change == "token":
            token = b"x" * 32
        elif change in {"claim", "worker", "team"}:
            claim = claim.model_copy(update={change + "_id": str(uuid4())})
        elif change == "epoch":
            claim = claim.model_copy(update={"worker_lease_epoch": 2})
        elif change == "attempt":
            claim = claim.model_copy(update={"trial_attempt_count": 2})
        elif change != "valid":
            async with factory.begin() as session:
                trial = await session.get(Trial, claim.trial_id)
                worker = await session.get(Worker, claim.worker_id)
                if change in {"released", "running", "cancelled"}:
                    trial.state = {"released": "queued", "running": "running", "cancelled": "cancelled"}[change]
                elif change == "draining":
                    worker.drain_state = "draining"
                else:
                    snapshot = dict(worker.capability_snapshot_json)
                    snapshot["container_runtime_features"] = []
                    worker.capability_snapshot_json = snapshot
                    if change == "old_reader":
                        worker.capability_snapshot_digest = canonical_digest(snapshot)
        async with factory.begin() as session:
            if change == "valid":
                locked = await m.lock_execution_claim(session, claim=claim, worker_token_hash=token)
                assert locked.claim == claim
                assert locked.cpu_arch == "x86_64"
                assert locked.task_id == await session.scalar(select(Trial.task_id))
            else:
                with pytest.raises(ValueError):
                    await m.lock_execution_claim(session, claim=claim, worker_token_hash=token)
    finally:
        await engine.dispose()
