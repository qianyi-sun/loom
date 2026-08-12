from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import SlurmWorkerJob
from loom.pipeline.policy_config import PolicyConfigRegistry
from loom.pipeline.resource_profiles import load_resource_profiles
from loom_control_plane.slurm_worker_jobs import (
    SlurmWorkerJobObservation,
    reconcile_slurm_worker_jobs,
    record_slurm_worker_job,
)


async def test_cluster_local_job_ids_and_reconcile_writers_are_isolated(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        oldlab, oldlab_duplicate = await record_slurm_worker_job(
            session,
            environment="test-dual-slurm",
            pool_name="behavior-gpu-oldlab",
            nodelist="trt-eai-oldlab-1",
            requested_cpus=16,
            requested_memory_mib=65_536,
            requested_concurrency=1,
            requested_gpu_tres="gpu:rtx5080:2",
            requested_gpus=2,
            job_id="77",
            slurm_state="RUNNING",
            pending_reason=None,
            env={},
            slurm_cluster_id="oldlab",
        )
        duplicate, was_duplicate = await record_slurm_worker_job(
            session,
            environment="test-dual-slurm",
            pool_name="behavior-gpu-oldlab",
            nodelist="trt-eai-oldlab-2",
            requested_cpus=16,
            requested_memory_mib=65_536,
            requested_concurrency=1,
            requested_gpu_tres="gpu:rtx5080:2",
            requested_gpus=2,
            job_id="77",
            slurm_state="RUNNING",
            pending_reason=None,
            env={},
            slurm_cluster_id="oldlab",
        )
        gb10, gb10_duplicate = await record_slurm_worker_job(
            session,
            environment="test-dual-slurm",
            pool_name="behavior-gpu-gb10",
            nodelist="trt-gb10-1",
            requested_cpus=16,
            requested_memory_mib=120_000,
            requested_concurrency=1,
            requested_gpu_tres="gpu:gb10:1",
            requested_gpus=1,
            job_id="77",
            slurm_state="RUNNING",
            pending_reason=None,
            env={},
            slurm_cluster_id="gb10",
        )
        assert oldlab_duplicate is False
        assert was_duplicate is True and duplicate.id == oldlab.id
        assert gb10_duplicate is False and gb10.id != oldlab.id

    async with sessions() as session, session.begin():
        result = await reconcile_slurm_worker_jobs(
            session,
            [SlurmWorkerJobObservation(job_id="77", slurm_state="RUNNING")],
            stale_after_seconds=1,
            now=now + timedelta(minutes=5),
            slurm_cluster_id="gb10",
            environment="test-dual-slurm",
            pool_name="behavior-gpu-gb10",
        )
        assert result.updated == 1
    async with sessions() as session:
        rows = (
            await session.execute(
                select(SlurmWorkerJob).where(
                    SlurmWorkerJob.environment == "test-dual-slurm"
                )
            )
        ).scalars().all()
        by_cluster = {row.slurm_cluster_id: row for row in rows}
        assert set(by_cluster) == {"oldlab", "gb10"}
        assert by_cluster["oldlab"].state == "running"
        assert by_cluster["gb10"].state == "running"
    async with sessions() as session, session.begin():
        for row in rows:
            await session.delete(row)
    await engine.dispose()


def test_policy_clusters_submit_targets_and_node_sets_are_disjoint() -> None:
    registry = PolicyConfigRegistry.load(resource_profiles=load_resource_profiles())
    oldlab = registry.get("behavior-gpu-oldlab")
    gb10 = registry.get("behavior-gpu-gb10")
    assert oldlab.cluster.submit_host_ref == "loom://slurm/oldlab"
    assert gb10.cluster.submit_host_ref == "loom://slurm/gb10"
    assert oldlab.cluster.slurm_conf_bundle_sha256 != gb10.cluster.slurm_conf_bundle_sha256
    assert set(oldlab.snapshot.allowed_nodes).isdisjoint(gb10.snapshot.allowed_nodes)
    assert all(not node.lower().startswith("lux") for node in (
        *oldlab.snapshot.allowed_nodes,
        *gb10.snapshot.allowed_nodes,
    ))
