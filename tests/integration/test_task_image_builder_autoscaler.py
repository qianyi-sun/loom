from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import SlurmWorkerJob, TaskImageMaterialization
from loom_control_plane.task_image_builder_autoscaler import (
    TaskImageBuilderPoolConfig,
    reconcile_task_image_builder_autoscaler_once,
)


def _config(namespace: str, *, cpu_arch: str = "arm64") -> TaskImageBuilderPoolConfig:
    cluster = "gb10" if cpu_arch == "arm64" else "oldlab"
    return TaskImageBuilderPoolConfig(
        environment="test",
        pool_name=f"task-image-builder-{cluster}-{namespace}",
        slurm_cluster_id=cluster,
        cpu_arch=cpu_arch,  # type: ignore[arg-type]
        allowed_nodes=(f"{namespace}-1", f"{namespace}-2"),
        env_file="/shared/loom/builder.env",
        repo_dir="/shared/loom/repo",
        partition="builder",
        time_limit="01:00:00",
        requested_cpus=8,
        requested_memory_mib=16384,
        requested_concurrency=1,
        max_jobs=2,
        pending_job_cap=2,
        idle_exit_after_seconds=60,
        sbatch_path="sbatch",
        squeue_path="squeue",
        sacct_path="sacct",
        scancel_path="scancel",
        command_timeout_seconds=20.0,
        registry_docker_config_dir="/secure/loom/task-image-builder-docker",
        exclusive=True,
        slurm_account="loom-test",
        slurm_qos="loom-builder",
        slurm_reservation="loom-builder-exclusive",
        job_output_dir="/shared/loom/job-output",
    )


class _FakeRunner:
    def __init__(self) -> None:
        self.submitted_nodes: list[str] = []
        self.cancelled_jobs: list[str] = []
        self.cancelled_active_jobs: list[str] = []

    async def submit_builder(self, *, node: str, config) -> str:  # type: ignore[no-untyped-def]
        self.submitted_nodes.append(node)
        return str(10_000 + len(self.submitted_nodes))

    async def cancel_pending_job(self, job_id: str) -> None:
        self.cancelled_jobs.append(job_id)

    async def cancel_job(self, job_id: str) -> None:
        self.cancelled_active_jobs.append(job_id)


class _MissingJobRunner(_FakeRunner):
    def __init__(self) -> None:
        super().__init__()
        self.queried_job_ids: list[tuple[str, ...]] = []

    async def query_jobs(self, job_ids: tuple[str, ...]) -> list[object]:
        self.queried_job_ids.append(job_ids)
        return []


async def test_reconcile_scales_from_zero_and_cancels_pending_without_demand(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    namespace = uuid4().hex[:8]
    config = _config(namespace)
    runner = _FakeRunner()
    materialization_id = uuid4()
    task_id = f"builder-autoscaler/{namespace}"
    try:
        async with sessions() as session, session.begin():
            await session.execute(
                insert(TaskImageMaterialization).values(
                    id=materialization_id,
                    materialization_key=hashlib.sha256(task_id.encode()).hexdigest(),
                    task_id=task_id,
                    task_checksum="a" * 64,
                    cpu_arch="arm64",
                    task_config={},
                    state="queued",
                )
            )

        async with sessions() as session, session.begin():
            result = await reconcile_task_image_builder_autoscaler_once(
                session,
                config=config,
                runner=runner,
            )
        assert result.submitted_job_ids == ("10001",)
        assert runner.submitted_nodes == [config.allowed_nodes[0]]

        async with sessions() as session, session.begin():
            second = await reconcile_task_image_builder_autoscaler_once(
                session,
                config=config,
                runner=runner,
            )
        assert second.submitted_job_ids == ()

        async with sessions() as session, session.begin():
            await session.execute(
                update(TaskImageMaterialization)
                .where(TaskImageMaterialization.id == materialization_id)
                .values(state="ready")
            )
        async with sessions() as session, session.begin():
            drained = await reconcile_task_image_builder_autoscaler_once(
                session,
                config=config,
                runner=runner,
            )
        assert drained.cancelled_job_ids == ("10001",)
        assert runner.cancelled_jobs == ["10001"]
    finally:
        async with sessions() as session, session.begin():
            await session.execute(
                delete(SlurmWorkerJob).where(SlurmWorkerJob.pool_name == config.pool_name)
            )
            await session.execute(
                delete(TaskImageMaterialization).where(TaskImageMaterialization.task_id == task_id)
            )
        await engine.dispose()


async def test_reconcile_isolates_demand_by_native_architecture(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    namespace = uuid4().hex[:8]
    config = _config(namespace, cpu_arch="x86_64")
    runner = _FakeRunner()
    task_id = f"builder-autoscaler/{namespace}"
    try:
        async with sessions() as session, session.begin():
            await session.execute(
                insert(TaskImageMaterialization).values(
                    id=uuid4(),
                    materialization_key=hashlib.sha256(task_id.encode()).hexdigest(),
                    task_id=task_id,
                    task_checksum="b" * 64,
                    cpu_arch="arm64",
                    task_config={},
                    state="queued",
                )
            )
        async with sessions() as session, session.begin():
            result = await reconcile_task_image_builder_autoscaler_once(
                session,
                config=config,
                runner=runner,
            )
        assert result.submitted_job_ids == ()
        assert runner.submitted_nodes == []
    finally:
        async with sessions() as session, session.begin():
            await session.execute(
                delete(SlurmWorkerJob).where(SlurmWorkerJob.pool_name == config.pool_name)
            )
            await session.execute(
                delete(TaskImageMaterialization).where(TaskImageMaterialization.task_id == task_id)
            )
        await engine.dispose()


async def test_reconcile_invalid_witness_mode_cancels_running_builder_capacity(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    namespace = uuid4().hex[:8]
    config = _config(namespace)
    runner = _FakeRunner()
    task_id = f"builder-autoscaler/{namespace}"
    active_job_id = f"active-{namespace}"
    try:
        async with sessions() as session, session.begin():
            session.add(
                TaskImageMaterialization(
                    id=uuid4(),
                    materialization_key=hashlib.sha256(task_id.encode()).hexdigest(),
                    task_id=task_id,
                    task_checksum="d" * 64,
                    cpu_arch="arm64",
                    task_config={},
                    state="queued",
                )
            )
            session.add(
                SlurmWorkerJob(
                    slurm_cluster_id="gb10",
                    environment=config.environment,
                    pool_name=config.pool_name,
                    nodelist=config.allowed_nodes[0],
                    requested_cpus=config.requested_cpus,
                    requested_memory_mib=config.requested_memory_mib,
                    requested_concurrency=1,
                    requested_gpus=0,
                    job_id=active_job_id,
                    slurm_state="RUNNING",
                    state="running",
                    submitted_at=datetime.now(UTC),
                )
            )

        async with sessions() as session, session.begin():
            result = await reconcile_task_image_builder_autoscaler_once(
                session,
                config=config,
                runner=runner,
                scale_up_allowed=False,
            )

        assert result.submitted_job_ids == ()
        assert result.cancelled_job_ids == (active_job_id,)
        assert runner.submitted_nodes == []
        assert runner.cancelled_active_jobs == [active_job_id]
        async with sessions() as session:
            job = await session.scalar(
                select(SlurmWorkerJob).where(SlurmWorkerJob.job_id == active_job_id)
            )
            assert job is not None
            assert job.state == "cancelled"
    finally:
        async with sessions() as session, session.begin():
            await session.execute(
                delete(SlurmWorkerJob).where(SlurmWorkerJob.pool_name == config.pool_name)
            )
            await session.execute(
                delete(TaskImageMaterialization).where(TaskImageMaterialization.task_id == task_id)
            )
        await engine.dispose()


async def test_reconcile_replaces_old_jobs_missing_from_slurm(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    namespace = uuid4().hex[:8]
    config = _config(namespace)
    runner = _MissingJobRunner()
    task_id = f"builder-autoscaler/{namespace}"
    stale_job_id = "old-missing-job"
    try:
        async with sessions() as session, session.begin():
            session.add(
                TaskImageMaterialization(
                    id=uuid4(),
                    materialization_key=hashlib.sha256(task_id.encode()).hexdigest(),
                    task_id=task_id,
                    task_checksum="c" * 64,
                    cpu_arch="arm64",
                    task_config={},
                    state="queued",
                )
            )
            session.add(
                SlurmWorkerJob(
                    slurm_cluster_id="gb10",
                    environment=config.environment,
                    pool_name=config.pool_name,
                    nodelist=config.allowed_nodes[0],
                    requested_cpus=config.requested_cpus,
                    requested_memory_mib=config.requested_memory_mib,
                    requested_concurrency=1,
                    requested_gpus=0,
                    job_id=stale_job_id,
                    slurm_state="PENDING",
                    state="pending",
                    submitted_at=datetime.now(UTC) - timedelta(minutes=10),
                )
            )

        async with sessions() as session, session.begin():
            result = await reconcile_task_image_builder_autoscaler_once(
                session,
                config=config,
                runner=runner,
            )

        assert runner.queried_job_ids == [(stale_job_id,)]
        assert result.submitted_job_ids == ("10001",)
        async with sessions() as session:
            stale_job = await session.scalar(
                select(SlurmWorkerJob).where(SlurmWorkerJob.job_id == stale_job_id)
            )
            assert stale_job is not None
            assert stale_job.state == "stale"
    finally:
        async with sessions() as session, session.begin():
            await session.execute(
                delete(SlurmWorkerJob).where(SlurmWorkerJob.pool_name == config.pool_name)
            )
            await session.execute(
                delete(TaskImageMaterialization).where(TaskImageMaterialization.task_id == task_id)
            )
        await engine.dispose()
