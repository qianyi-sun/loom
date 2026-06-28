from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    GB10WorkerNodeStatus,
    GB10WorkerPoolDesiredState,
    SlurmWorkerJob,
    Task,
    Team,
    TeamQuota,
    Trial,
    Worker,
    WorkerPoolAutoscalerPolicy,
)
from loom_control_plane.elastic_slurm_worker_controller import (
    ElasticSlurmWorkerControllerConfig,
    SlurmWorkerCommandRunner,
)
from loom_control_plane.slurm_worker_jobs import SlurmWorkerJobObservation
from loom_control_plane.worker_pool_autoscaler import (
    reconcile_worker_pool_autoscaler_once,
)


class FakeSlurmRunner(SlurmWorkerCommandRunner):
    def __init__(self) -> None:
        self.submitted_nodes: list[str] = []
        self.submitted_configs: list[ElasticSlurmWorkerControllerConfig] = []
        self.cancelled_job_ids: list[str] = []
        self.fail_submit_nodes: set[str] = set()
        self.job_observations: list[SlurmWorkerJobObservation] | None = None
        self.queried_job_ids: list[tuple[str, ...]] = []

    async def query_jobs(
        self,
        job_ids: tuple[str, ...],
    ) -> list[SlurmWorkerJobObservation]:
        self.queried_job_ids.append(job_ids)
        if self.job_observations is not None:
            return self.job_observations
        return [
            SlurmWorkerJobObservation(job_id=job_id, slurm_state="RUNNING")
            for job_id in job_ids
        ]

    async def submit_worker(
        self,
        *,
        node: str,
        config: ElasticSlurmWorkerControllerConfig,
    ) -> str:
        self.submitted_nodes.append(node)
        self.submitted_configs.append(config)
        if node in self.fail_submit_nodes:
            raise RuntimeError(f"sbatch failed for {node}")
        return f"job-{node}"

    async def cancel_job(self, job_id: str) -> None:
        self.cancelled_job_ids.append(job_id)


@pytest.fixture(autouse=True)
async def _cleanup_db(postgres_url: str) -> Iterator[None]:
    yield
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as s:
        await s.execute(delete(Trial))
        await s.execute(delete(SlurmWorkerJob))
        await s.execute(delete(GB10WorkerNodeStatus))
        await s.execute(delete(GB10WorkerPoolDesiredState))
        await s.execute(delete(WorkerPoolAutoscalerPolicy))
        await s.execute(delete(Worker))
        await s.execute(delete(Task))
        await s.execute(delete(TeamQuota))
        await s.execute(delete(Team))
        await s.commit()
    await engine.dispose()


async def test_reconcile_marks_one_idle_excess_worker_draining(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    worker_ids = [uuid4(), uuid4()]
    try:
        async with session_factory() as s:
            for idx, worker_id in enumerate(worker_ids, start=1):
                await s.execute(insert(Worker).values(
                    id=worker_id,
                    hostname=f"oldlab-{idx}",
                    version="test",
                    capabilities=[{
                        "backend": "docker",
                        "os": "linux",
                        "cpu_arch": "x86_64",
                        "gpu_vendor": "none",
                        "network_policies": ["none"],
                    }],
                    max_concurrent=6,
                    pool_name="oldlab",
                    drain_state="active",
                    registered_at=now,
                    last_seen_at=now,
                    status="active",
                ))
            await s.execute(insert(WorkerPoolAutoscalerPolicy).values(
                environment="production",
                pool_name="oldlab",
                actuator="slurm",
                enabled=True,
                min_slots=6,
                max_slots=12,
                scale_up_threshold_slots=1,
                scale_down_idle_seconds=600,
                scale_up_cooldown_seconds=60,
                scale_down_cooldown_seconds=300,
                drain_timeout_seconds=600,
                actuator_config={"backend": "docker", "cpu_arch": "x86_64"},
                idle_since_at=now - timedelta(seconds=601),
            ))
            await s.commit()

        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(s, now=now)
            await s.commit()

        assert len(results) == 1
        assert results[0].action == "request_drain"
        assert results[0].desired_slots == 6
        assert len(results[0].worker_ids_to_drain) == 1

        async with session_factory() as s:
            workers = (await s.execute(
                select(Worker).order_by(Worker.hostname),
            )).scalars().all()
            policy = (await s.execute(
                select(WorkerPoolAutoscalerPolicy),
            )).scalar_one()

        drain_states = [worker.drain_state for worker in workers]
        assert drain_states.count("draining") == 1
        assert drain_states.count("active") == 1
        assert policy.last_decision == "request_drain"
        assert policy.last_decision_reason == "idle_excess_capacity"
        assert policy.last_desired_slots == 6
        assert policy.last_actual_slots == 12
    finally:
        await engine.dispose()


async def test_reconcile_submits_slurm_jobs_for_scale_up_deficit(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    team_id = uuid4()
    try:
        async with session_factory() as s:
            await s.execute(insert(Team).values(id=team_id, name="team-a"))
            await s.execute(insert(Task).values(id="task-a", checksum="0" * 64, config={}))
            for idx in range(7):
                await s.execute(insert(Trial).values(
                    id=uuid4(),
                    team_id=team_id,
                    task_id="task-a",
                    config={},
                    requires_caps={"backend": "docker", "cpu_arch": "x86_64"},
                    state="queued",
                    idempotency_key=f"queued-{idx}",
                ))
            await s.execute(insert(WorkerPoolAutoscalerPolicy).values(
                environment="production",
                pool_name="oldlab",
                actuator="slurm",
                enabled=True,
                min_slots=0,
                max_slots=12,
                scale_up_threshold_slots=1,
                scale_down_idle_seconds=600,
                scale_up_cooldown_seconds=60,
                scale_down_cooldown_seconds=300,
                drain_timeout_seconds=600,
                actuator_config={
                    "backend": "docker",
                    "cpu_arch": "x86_64",
                    "allowed_nodes": ["oldlab-1", "oldlab-2"],
                    "env_file": "/secure/.env.remote-worker",
                    "repo_dir": "/opt/loom",
                    "requested_cpus": 12,
                    "requested_memory_mib": 58000,
                    "requested_concurrency": 6,
                    "max_jobs": 2,
                    "pending_job_cap": 2,
                    "time_limit": "7-00:00:00",
                },
            ))
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                now=now,
                slurm_runner=runner,
            )
            await s.commit()

        assert results[0].action == "scale_up"
        assert runner.submitted_nodes == ["oldlab-1", "oldlab-2"]

        async with session_factory() as s:
            jobs = (await s.execute(
                select(SlurmWorkerJob).order_by(SlurmWorkerJob.nodelist),
            )).scalars().all()
            policy = (await s.execute(
                select(WorkerPoolAutoscalerPolicy),
            )).scalar_one()

        assert [job.job_id for job in jobs] == ["job-oldlab-1", "job-oldlab-2"]
        assert {job.state for job in jobs} == {"pending"}
        assert policy.last_decision == "scale_up"
        assert policy.last_decision_reason == "queued_deficit"
        assert policy.last_pending_slots == 12
    finally:
        await engine.dispose()


async def test_reconcile_submits_gb10_capacity_through_slurm_partition(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    team_id = uuid4()
    try:
        async with session_factory() as s:
            await s.execute(insert(Team).values(id=team_id, name="team-a"))
            await s.execute(insert(Task).values(id="task-a", checksum="0" * 64, config={}))
            for idx in range(10):
                await s.execute(insert(Trial).values(
                    id=uuid4(),
                    team_id=team_id,
                    task_id="task-a",
                    config={},
                    requires_caps={"backend": "docker", "cpu_arch": "arm64"},
                    state="queued",
                    idempotency_key=f"queued-gb10-{idx}",
                ))
            await s.execute(insert(WorkerPoolAutoscalerPolicy).values(
                environment="production",
                pool_name="gb10-arm64",
                actuator="slurm",
                enabled=True,
                min_slots=0,
                max_slots=150,
                scale_up_threshold_slots=1,
                scale_down_idle_seconds=600,
                scale_up_cooldown_seconds=60,
                scale_down_cooldown_seconds=300,
                drain_timeout_seconds=600,
                actuator_config={
                    "backend": "docker",
                    "cpu_arch": "arm64",
                    "allowed_nodes": [
                        "trt-gb10-1",
                        "trt-gb10-2",
                        "trt-gb10-3",
                        "trt-gb10-4",
                        "trt-gb10-5",
                        "trt-gb10-6",
                        "trt-gb10-7",
                        "trt-gb10-8",
                        "trt-gb10-9",
                        "trt-gb10-10",
                        "trt-gb10-11",
                        "trt-gb10-12",
                        "trt-gb10-13",
                        "trt-gb10-14",
                        "trt-gb10-15",
                    ],
                    "env_file": "/secure/.env.gb10-worker",
                    "repo_dir": "/shared_work/qianyi/loom-remote-worker",
                    "partition": "gb10",
                    "requested_cpus": 20,
                    "requested_memory_mib": 115000,
                    "requested_concurrency": 10,
                    "max_jobs": 15,
                    "pending_job_cap": 2,
                    "time_limit": "2-00:00:00",
                },
            ))
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                now=now,
                slurm_runner=runner,
            )
            await s.commit()

        assert results[0].action == "scale_up"
        assert runner.submitted_nodes == ["trt-gb10-1"]
        assert runner.submitted_configs[0].partition == "gb10"
        assert runner.submitted_configs[0].pool_name == "gb10-arm64"
        assert runner.submitted_configs[0].requested_concurrency == 10

        async with session_factory() as s:
            job = (await s.execute(select(SlurmWorkerJob))).scalar_one()
            policy = (await s.execute(
                select(WorkerPoolAutoscalerPolicy),
            )).scalar_one()

        assert job.job_id == "job-trt-gb10-1"
        assert job.nodelist == "trt-gb10-1"
        assert job.requested_cpus == 20
        assert job.requested_memory_mib == 115000
        assert job.requested_concurrency == 10
        assert job.state == "pending"
        assert job.redacted_env["LOOM_WORKER_POOL_NAME"] == "gb10-arm64"
        assert policy.actuator == "slurm"
        assert policy.last_decision == "scale_up"
        assert policy.last_decision_reason == "queued_deficit"
        assert policy.last_desired_slots == 10
        assert policy.last_pending_slots == 10
    finally:
        await engine.dispose()


async def test_control_plane_reconcile_skips_external_slurm_runner_policies(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    team_id = uuid4()
    try:
        async with session_factory() as s:
            await s.execute(insert(Team).values(id=team_id, name="team-a"))
            await s.execute(insert(Task).values(id="task-a", checksum="0" * 64, config={}))
            await s.execute(insert(Trial).values(
                id=uuid4(),
                team_id=team_id,
                task_id="task-a",
                config={},
                requires_caps={"backend": "docker", "cpu_arch": "x86_64"},
                state="queued",
                idempotency_key="queued-external-runner",
            ))
            await s.execute(insert(WorkerPoolAutoscalerPolicy).values(
                environment="production",
                pool_name="oldlab",
                actuator="slurm",
                enabled=True,
                min_slots=0,
                max_slots=6,
                scale_up_threshold_slots=1,
                scale_down_idle_seconds=600,
                scale_up_cooldown_seconds=60,
                scale_down_cooldown_seconds=300,
                drain_timeout_seconds=600,
                actuator_config={
                    "backend": "docker",
                    "cpu_arch": "x86_64",
                    "external_runner": True,
                    "allowed_nodes": ["oldlab-1"],
                    "env_file": "/secure/.env.remote-worker",
                    "repo_dir": "/opt/loom",
                    "requested_cpus": 12,
                    "requested_memory_mib": 58000,
                    "requested_concurrency": 6,
                    "max_jobs": 1,
                    "pending_job_cap": 1,
                    "time_limit": "7-00:00:00",
                },
            ))
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                now=now,
                slurm_runner=runner,
            )
            await s.commit()

        assert results == []
        assert runner.submitted_nodes == []

        async with session_factory() as s:
            jobs = (await s.execute(select(SlurmWorkerJob))).scalars().all()
            policy = (await s.execute(select(WorkerPoolAutoscalerPolicy))).scalar_one()

        assert jobs == []
        assert policy.last_decision is None
    finally:
        await engine.dispose()


async def test_submit_host_reconcile_processes_external_slurm_runner_policies(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    team_id = uuid4()
    try:
        async with session_factory() as s:
            await s.execute(insert(Team).values(id=team_id, name="team-a"))
            await s.execute(insert(Task).values(id="task-a", checksum="0" * 64, config={}))
            await s.execute(insert(Trial).values(
                id=uuid4(),
                team_id=team_id,
                task_id="task-a",
                config={},
                requires_caps={"backend": "docker", "cpu_arch": "x86_64"},
                state="queued",
                idempotency_key="queued-external-runner",
            ))
            await s.execute(insert(WorkerPoolAutoscalerPolicy).values(
                environment="production",
                pool_name="oldlab",
                actuator="slurm",
                enabled=True,
                min_slots=0,
                max_slots=6,
                scale_up_threshold_slots=1,
                scale_down_idle_seconds=600,
                scale_up_cooldown_seconds=60,
                scale_down_cooldown_seconds=300,
                drain_timeout_seconds=600,
                actuator_config={
                    "backend": "docker",
                    "cpu_arch": "x86_64",
                    "external_runner": True,
                    "allowed_nodes": ["oldlab-1"],
                    "env_file": "/secure/.env.remote-worker",
                    "repo_dir": "/opt/loom",
                    "requested_cpus": 12,
                    "requested_memory_mib": 58000,
                    "requested_concurrency": 6,
                    "max_jobs": 1,
                    "pending_job_cap": 1,
                    "time_limit": "7-00:00:00",
                },
            ))
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                now=now,
                slurm_runner=runner,
                include_external_policies=True,
            )
            await s.commit()

        assert results[0].action == "scale_up"
        assert runner.submitted_nodes == ["oldlab-1"]

        async with session_factory() as s:
            job = (await s.execute(select(SlurmWorkerJob))).scalar_one()
            policy = (await s.execute(select(WorkerPoolAutoscalerPolicy))).scalar_one()

        assert job.job_id == "job-oldlab-1"
        assert job.state == "pending"
        assert policy.last_decision == "scale_up"
    finally:
        await engine.dispose()


async def test_external_slurm_runner_reconcile_refreshes_known_job_state(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    worker_id = uuid4()
    try:
        async with session_factory() as s:
            await s.execute(insert(Worker).values(
                id=worker_id,
                hostname="oldlab-1",
                version="test",
                capabilities=[{
                    "backend": "docker",
                    "os": "linux",
                    "cpu_arch": "x86_64",
                    "gpu_vendor": "none",
                    "network_policies": ["none"],
                }],
                max_concurrent=1,
                pool_name="oldlab",
                drain_state="active",
                registered_at=now,
                last_seen_at=now,
                status="active",
            ))
            await s.execute(insert(SlurmWorkerJob).values(
                environment="production",
                pool_name="oldlab",
                nodelist="oldlab-1",
                requested_cpus=2,
                requested_memory_mib=8000,
                requested_concurrency=1,
                job_id="9001",
                slurm_state="PENDING",
                state="pending",
                redacted_env={},
                submitted_at=now - timedelta(seconds=300),
            ))
            await s.execute(insert(WorkerPoolAutoscalerPolicy).values(
                environment="production",
                pool_name="oldlab",
                actuator="slurm",
                enabled=True,
                min_slots=1,
                max_slots=1,
                scale_up_threshold_slots=1,
                scale_down_idle_seconds=600,
                scale_up_cooldown_seconds=60,
                scale_down_cooldown_seconds=300,
                drain_timeout_seconds=600,
                actuator_config={
                    "backend": "docker",
                    "cpu_arch": "x86_64",
                    "external_runner": True,
                    "allowed_nodes": ["oldlab-1"],
                    "env_file": "/secure/.env.remote-worker",
                    "repo_dir": "/opt/loom",
                    "requested_cpus": 2,
                    "requested_memory_mib": 8000,
                    "requested_concurrency": 1,
                    "max_jobs": 1,
                    "pending_job_cap": 1,
                    "time_limit": "7-00:00:00",
                },
            ))
            await s.commit()

        runner = FakeSlurmRunner()
        runner.job_observations = [
            SlurmWorkerJobObservation(
                job_id="9001",
                slurm_state="RUNNING",
                nodelist="oldlab-1",
                worker_id=worker_id,
                observed_at=now,
            ),
        ]
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                now=now,
                slurm_runner=runner,
                include_external_policies=True,
                external_only=True,
            )
            await s.commit()

        assert runner.queried_job_ids == [("9001",)]
        assert results[0].action == "noop"
        assert results[0].actual_slots == 1
        assert results[0].pending_slots == 0

        async with session_factory() as s:
            job = (await s.execute(select(SlurmWorkerJob))).scalar_one()
            policy = (await s.execute(select(WorkerPoolAutoscalerPolicy))).scalar_one()

        assert job.state == "running"
        assert job.slurm_state == "RUNNING"
        assert job.worker_id == worker_id
        assert policy.last_actual_slots == 1
        assert policy.last_pending_slots == 0
    finally:
        await engine.dispose()


async def test_reconcile_releases_drained_slurm_worker_job(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    worker_id = uuid4()
    try:
        async with session_factory() as s:
            await s.execute(insert(Worker).values(
                id=worker_id,
                hostname="oldlab-1",
                version="test",
                capabilities=[{
                    "backend": "docker",
                    "os": "linux",
                    "cpu_arch": "x86_64",
                    "gpu_vendor": "none",
                    "network_policies": ["none"],
                }],
                max_concurrent=6,
                pool_name="oldlab",
                drain_state="draining",
                drain_requested_at=now - timedelta(seconds=601),
                registered_at=now,
                last_seen_at=now,
                status="active",
            ))
            await s.execute(insert(SlurmWorkerJob).values(
                environment="production",
                pool_name="oldlab",
                nodelist="oldlab-1",
                requested_cpus=12,
                requested_memory_mib=58000,
                requested_concurrency=6,
                job_id="9001",
                slurm_state="RUNNING",
                state="running",
                worker_id=worker_id,
                redacted_env={},
                submitted_at=now - timedelta(seconds=900),
                started_at=now - timedelta(seconds=800),
            ))
            await s.execute(insert(WorkerPoolAutoscalerPolicy).values(
                environment="production",
                pool_name="oldlab",
                actuator="slurm",
                enabled=True,
                min_slots=0,
                max_slots=6,
                scale_up_threshold_slots=1,
                scale_down_idle_seconds=600,
                scale_up_cooldown_seconds=60,
                scale_down_cooldown_seconds=300,
                drain_timeout_seconds=600,
                actuator_config={
                    "backend": "docker",
                    "cpu_arch": "x86_64",
                    "allowed_nodes": ["oldlab-1"],
                    "env_file": "/secure/.env.remote-worker",
                    "repo_dir": "/opt/loom",
                    "requested_cpus": 12,
                    "requested_memory_mib": 58000,
                    "requested_concurrency": 6,
                    "max_jobs": 1,
                    "pending_job_cap": 1,
                    "time_limit": "7-00:00:00",
                },
                idle_since_at=now - timedelta(seconds=601),
            ))
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                now=now,
                slurm_runner=runner,
            )
            await s.commit()

        assert results[0].action == "release_drained"
        assert runner.cancelled_job_ids == ["9001"]

        async with session_factory() as s:
            worker = await s.get(Worker, worker_id)
            job = (await s.execute(select(SlurmWorkerJob))).scalar_one()
            policy = (await s.execute(select(WorkerPoolAutoscalerPolicy))).scalar_one()

        assert worker is not None
        assert worker.drain_state == "drained"
        assert job.state == "cancelled"
        assert job.pending_reason == "cancelled after autoscaler drain"
        assert policy.last_decision == "release_drained"
        assert policy.last_draining_slots == 0
    finally:
        await engine.dispose()


async def test_reconcile_releases_drained_slurm_job_by_worker_hostname(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    worker_id = uuid4()
    try:
        async with session_factory() as s:
            await s.execute(insert(Worker).values(
                id=worker_id,
                hostname="oldlab-1",
                version="test",
                capabilities=[{
                    "backend": "docker",
                    "os": "linux",
                    "cpu_arch": "x86_64",
                    "gpu_vendor": "none",
                    "network_policies": ["none"],
                }],
                max_concurrent=6,
                pool_name="oldlab",
                drain_state="draining",
                drain_requested_at=now - timedelta(seconds=601),
                registered_at=now,
                last_seen_at=now,
                status="active",
            ))
            await s.execute(insert(SlurmWorkerJob).values(
                environment="production",
                pool_name="oldlab",
                nodelist="oldlab-1",
                requested_cpus=12,
                requested_memory_mib=58000,
                requested_concurrency=6,
                job_id="9001",
                slurm_state="RUNNING",
                state="running",
                redacted_env={},
                submitted_at=now - timedelta(seconds=900),
                started_at=now - timedelta(seconds=800),
            ))
            await s.execute(insert(WorkerPoolAutoscalerPolicy).values(
                environment="production",
                pool_name="oldlab",
                actuator="slurm",
                enabled=True,
                min_slots=0,
                max_slots=6,
                scale_up_threshold_slots=1,
                scale_down_idle_seconds=600,
                scale_up_cooldown_seconds=60,
                scale_down_cooldown_seconds=300,
                drain_timeout_seconds=600,
                actuator_config={
                    "backend": "docker",
                    "cpu_arch": "x86_64",
                    "allowed_nodes": ["oldlab-1"],
                    "env_file": "/secure/.env.remote-worker",
                    "repo_dir": "/opt/loom",
                    "requested_cpus": 12,
                    "requested_memory_mib": 58000,
                    "requested_concurrency": 6,
                    "max_jobs": 1,
                    "pending_job_cap": 1,
                    "time_limit": "7-00:00:00",
                },
                idle_since_at=now - timedelta(seconds=601),
            ))
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                now=now,
                slurm_runner=runner,
            )
            await s.commit()

        assert results[0].action == "release_drained"
        assert runner.cancelled_job_ids == ["9001"]

        async with session_factory() as s:
            job = (await s.execute(select(SlurmWorkerJob))).scalar_one()

        assert job.worker_id == worker_id
        assert job.state == "cancelled"
    finally:
        await engine.dispose()


async def test_reconcile_sets_gb10_host_intent_to_draining(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    worker_id = uuid4()
    try:
        async with session_factory() as s:
            await s.execute(insert(Worker).values(
                id=worker_id,
                hostname="trt-gb10-1",
                version="test",
                capabilities=[{
                    "backend": "docker",
                    "os": "linux",
                    "cpu_arch": "arm64",
                    "gpu_vendor": "none",
                    "network_policies": ["none"],
                }],
                max_concurrent=2,
                pool_name="gb10-arm64",
                drain_state="active",
                registered_at=now,
                last_seen_at=now,
                status="active",
            ))
            await s.execute(insert(GB10WorkerPoolDesiredState).values(
                environment="production",
                pool_name="gb10-arm64",
                image_tag="gb10-image",
                max_concurrent=2,
                env_config_version="gb10-env",
                target_slots=2,
                host_intents={"trt-gb10-1": "active"},
                rollout_policy={},
                env={},
            ))
            await s.execute(insert(GB10WorkerNodeStatus).values(
                environment="production",
                pool_name="gb10-arm64",
                hostname="trt-gb10-1",
                worker_id=worker_id,
                current_image_tag="gb10-image",
                current_max_concurrent=2,
                current_env_config_version="gb10-env",
                current_intent="active",
                desired_image_tag="gb10-image",
                desired_max_concurrent=2,
                desired_env_config_version="gb10-env",
                desired_intent="active",
                apply_state="applied",
            ))
            await s.execute(insert(WorkerPoolAutoscalerPolicy).values(
                environment="production",
                pool_name="gb10-arm64",
                actuator="gb10",
                enabled=True,
                min_slots=0,
                max_slots=2,
                scale_up_threshold_slots=1,
                scale_down_idle_seconds=600,
                scale_up_cooldown_seconds=60,
                scale_down_cooldown_seconds=300,
                drain_timeout_seconds=600,
                actuator_config={"backend": "docker", "cpu_arch": "arm64"},
                idle_since_at=now - timedelta(seconds=601),
            ))
            await s.commit()

        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(s, now=now)
            await s.commit()

        assert results[0].action == "request_drain"

        async with session_factory() as s:
            desired = (await s.execute(select(GB10WorkerPoolDesiredState))).scalar_one()
            worker = await s.get(Worker, worker_id)

        assert desired.target_slots == 0
        assert desired.host_intents == {"trt-gb10-1": "draining"}
        assert worker is not None
        assert worker.drain_state == "draining"
    finally:
        await engine.dispose()


async def test_reconcile_sets_gb10_host_intent_by_hostname_when_worker_id_missing(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    worker_id = uuid4()
    try:
        async with session_factory() as s:
            await s.execute(insert(Worker).values(
                id=worker_id,
                hostname="trt-gb10-1",
                version="test",
                capabilities=[{
                    "backend": "docker",
                    "os": "linux",
                    "cpu_arch": "arm64",
                    "gpu_vendor": "none",
                    "network_policies": ["none"],
                }],
                max_concurrent=2,
                pool_name="gb10-arm64",
                drain_state="active",
                registered_at=now,
                last_seen_at=now,
                status="active",
            ))
            await s.execute(insert(GB10WorkerPoolDesiredState).values(
                environment="production",
                pool_name="gb10-arm64",
                image_tag="gb10-image",
                max_concurrent=2,
                env_config_version="gb10-env",
                target_slots=2,
                host_intents={"trt-gb10-1": "active"},
                rollout_policy={},
                env={},
            ))
            await s.execute(insert(GB10WorkerNodeStatus).values(
                environment="production",
                pool_name="gb10-arm64",
                hostname="trt-gb10-1",
                worker_id=None,
                current_image_tag="gb10-image",
                current_max_concurrent=2,
                current_env_config_version="gb10-env",
                current_intent="active",
                desired_image_tag="gb10-image",
                desired_max_concurrent=2,
                desired_env_config_version="gb10-env",
                desired_intent="active",
                apply_state="applied",
            ))
            await s.execute(insert(WorkerPoolAutoscalerPolicy).values(
                environment="production",
                pool_name="gb10-arm64",
                actuator="gb10",
                enabled=True,
                min_slots=0,
                max_slots=2,
                scale_up_threshold_slots=1,
                scale_down_idle_seconds=600,
                scale_up_cooldown_seconds=60,
                scale_down_cooldown_seconds=300,
                drain_timeout_seconds=600,
                actuator_config={"backend": "docker", "cpu_arch": "arm64"},
                idle_since_at=now - timedelta(seconds=601),
            ))
            await s.commit()

        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(s, now=now)
            await s.commit()

        assert results[0].action == "request_drain"

        async with session_factory() as s:
            desired = (await s.execute(select(GB10WorkerPoolDesiredState))).scalar_one()
            worker = await s.get(Worker, worker_id)

        assert desired.target_slots == 0
        assert desired.host_intents == {"trt-gb10-1": "draining"}
        assert worker is not None
        assert worker.drain_state == "draining"
    finally:
        await engine.dispose()


async def test_reconcile_sets_gb10_stopped_hosts_active_for_scale_up(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    team_id = uuid4()
    try:
        async with session_factory() as s:
            await s.execute(insert(Team).values(id=team_id, name="team-a"))
            await s.execute(insert(Task).values(id="task-a", checksum="0" * 64, config={}))
            for idx in range(3):
                await s.execute(insert(Trial).values(
                    id=uuid4(),
                    team_id=team_id,
                    task_id="task-a",
                    config={},
                    requires_caps={"backend": "docker", "cpu_arch": "arm64"},
                    state="queued",
                    idempotency_key=f"gb10-queued-{idx}",
                ))
            await s.execute(insert(GB10WorkerPoolDesiredState).values(
                environment="production",
                pool_name="gb10-arm64",
                image_tag="gb10-image",
                max_concurrent=2,
                env_config_version="gb10-env",
                target_slots=0,
                host_intents={
                    "trt-gb10-1": "stopped",
                    "trt-gb10-2": "stopped",
                },
                rollout_policy={},
                env={},
            ))
            await s.execute(insert(WorkerPoolAutoscalerPolicy).values(
                environment="production",
                pool_name="gb10-arm64",
                actuator="gb10",
                enabled=True,
                min_slots=0,
                max_slots=4,
                scale_up_threshold_slots=1,
                scale_down_idle_seconds=600,
                scale_up_cooldown_seconds=60,
                scale_down_cooldown_seconds=300,
                drain_timeout_seconds=600,
                actuator_config={
                    "backend": "docker",
                    "cpu_arch": "arm64",
                    "hosts": ["trt-gb10-1", "trt-gb10-2"],
                    "max_concurrent": 2,
                    "image_tag": "gb10-image",
                    "env_config_version": "gb10-env",
                },
            ))
            await s.commit()

        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(s, now=now)
            await s.commit()

        assert results[0].action == "scale_up"

        async with session_factory() as s:
            desired = (await s.execute(select(GB10WorkerPoolDesiredState))).scalar_one()

        assert desired.target_slots == 3
        assert desired.host_intents == {
            "trt-gb10-1": "active",
            "trt-gb10-2": "active",
        }
    finally:
        await engine.dispose()


async def test_reconcile_only_marks_selected_gb10_hosts_active_for_scale_up(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    team_id = uuid4()
    hostnames = ["trt-gb10-1", "trt-gb10-2", "trt-gb10-3"]
    try:
        async with session_factory() as s:
            await s.execute(insert(Team).values(id=team_id, name="team-a"))
            await s.execute(insert(Task).values(id="task-a", checksum="0" * 64, config={}))
            await s.execute(insert(Trial).values(
                id=uuid4(),
                team_id=team_id,
                task_id="task-a",
                config={},
                requires_caps={"backend": "docker", "cpu_arch": "arm64"},
                state="queued",
                idempotency_key="gb10-select-one",
            ))
            await s.execute(insert(GB10WorkerPoolDesiredState).values(
                environment="production",
                pool_name="gb10-arm64",
                image_tag="gb10-image",
                max_concurrent=10,
                env_config_version="gb10-env",
                target_slots=0,
                host_intents={hostname: "stopped" for hostname in hostnames},
                rollout_policy={},
                env={},
            ))
            for hostname in hostnames:
                await s.execute(insert(GB10WorkerNodeStatus).values(
                    environment="production",
                    pool_name="gb10-arm64",
                    hostname=hostname,
                    current_image_tag="gb10-image",
                    current_max_concurrent=10,
                    current_env_config_version="gb10-env",
                    current_intent="stopped",
                    desired_image_tag="gb10-image",
                    desired_max_concurrent=10,
                    desired_env_config_version="gb10-env",
                    desired_intent="stopped",
                    apply_state="stopped",
                ))
            await s.execute(insert(WorkerPoolAutoscalerPolicy).values(
                environment="production",
                pool_name="gb10-arm64",
                actuator="gb10",
                enabled=True,
                min_slots=0,
                max_slots=30,
                scale_up_threshold_slots=1,
                scale_down_idle_seconds=600,
                scale_up_cooldown_seconds=60,
                scale_down_cooldown_seconds=300,
                drain_timeout_seconds=600,
                actuator_config={
                    "backend": "docker",
                    "cpu_arch": "arm64",
                    "hosts": hostnames,
                    "max_concurrent": 10,
                    "image_tag": "gb10-image",
                    "env_config_version": "gb10-env",
                },
            ))
            await s.commit()

        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(s, now=now)
            await s.commit()

        assert results[0].action == "scale_up"
        assert results[0].desired_slots == 1

        async with session_factory() as s:
            desired = (await s.execute(select(GB10WorkerPoolDesiredState))).scalar_one()
            nodes = (await s.execute(
                select(GB10WorkerNodeStatus).order_by(GB10WorkerNodeStatus.hostname),
            )).scalars().all()

        assert desired.target_slots == 1
        assert desired.host_intents == {
            "trt-gb10-1": "active",
            "trt-gb10-2": "stopped",
            "trt-gb10-3": "stopped",
        }
        assert {node.hostname: node.desired_intent for node in nodes} == {
            "trt-gb10-1": "active",
            "trt-gb10-2": "stopped",
            "trt-gb10-3": "stopped",
        }
    finally:
        await engine.dispose()


async def test_reconcile_records_slurm_actuator_failure_on_policy(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    team_id = uuid4()
    try:
        async with session_factory() as s:
            await s.execute(insert(Team).values(id=team_id, name="team-a"))
            await s.execute(insert(Task).values(id="task-a", checksum="0" * 64, config={}))
            await s.execute(insert(Trial).values(
                id=uuid4(),
                team_id=team_id,
                task_id="task-a",
                config={},
                requires_caps={"backend": "docker", "cpu_arch": "x86_64"},
                state="queued",
                idempotency_key="actuator-failure",
            ))
            await s.execute(insert(WorkerPoolAutoscalerPolicy).values(
                environment="production",
                pool_name="oldlab",
                actuator="slurm",
                enabled=True,
                min_slots=0,
                max_slots=6,
                scale_up_threshold_slots=1,
                scale_down_idle_seconds=600,
                scale_up_cooldown_seconds=60,
                scale_down_cooldown_seconds=300,
                drain_timeout_seconds=600,
                actuator_config={
                    "backend": "docker",
                    "cpu_arch": "x86_64",
                    "allowed_nodes": ["oldlab-1"],
                    "env_file": "/secure/.env.remote-worker",
                    "repo_dir": "/opt/loom",
                    "requested_cpus": 12,
                    "requested_memory_mib": 58000,
                    "requested_concurrency": 6,
                    "max_jobs": 1,
                    "pending_job_cap": 1,
                    "time_limit": "7-00:00:00",
                },
            ))
            await s.commit()

        runner = FakeSlurmRunner()
        runner.fail_submit_nodes.add("oldlab-1")
        async with session_factory() as s:
            await reconcile_worker_pool_autoscaler_once(
                s,
                now=now,
                slurm_runner=runner,
            )
            await s.commit()

        async with session_factory() as s:
            policy = (await s.execute(select(WorkerPoolAutoscalerPolicy))).scalar_one()
            job = (await s.execute(select(SlurmWorkerJob))).scalar_one()

        assert policy.last_decision == "scale_up"
        assert policy.last_error == "sbatch failed for oldlab-1"
        assert job.state == "failed"
        assert job.submission_error == "sbatch failed for oldlab-1"
    finally:
        await engine.dispose()
