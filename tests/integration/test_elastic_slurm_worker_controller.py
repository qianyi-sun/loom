from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import SlurmWorkerJob, Task, Team, Trial, Worker
from loom_control_plane.elastic_slurm_worker_controller import (
    ElasticSlurmWorkerControllerConfig,
    SlurmWorkerCommandRunner,
    run_elastic_slurm_worker_controller_once,
)
from loom_control_plane.slurm_worker_jobs import SlurmWorkerJobObservation


class FakeSlurmRunner(SlurmWorkerCommandRunner):
    def __init__(
        self,
        *,
        submit_results: dict[str, str] | None = None,
        observations: list[SlurmWorkerJobObservation] | None = None,
        submit_errors: dict[str, str] | None = None,
    ) -> None:
        self.submit_results = submit_results or {}
        self.observations = observations or []
        self.submit_errors = submit_errors or {}
        self.submitted_nodes: list[str] = []
        self.cancelled_job_ids: list[str] = []
        self.queried_job_ids: list[str] = []

    async def query_jobs(
        self,
        job_ids: tuple[str, ...],
    ) -> list[SlurmWorkerJobObservation]:
        self.queried_job_ids.extend(job_ids)
        return [obs for obs in self.observations if obs.job_id in job_ids]

    async def submit_worker(
        self,
        *,
        node: str,
        config: ElasticSlurmWorkerControllerConfig,
    ) -> str:
        self.submitted_nodes.append(node)
        if node in self.submit_errors:
            raise RuntimeError(self.submit_errors[node])
        return self.submit_results.get(node, f"job-{node}")

    async def cancel_job(self, job_id: str) -> None:
        self.cancelled_job_ids.append(job_id)


def _config(**overrides: object) -> ElasticSlurmWorkerControllerConfig:
    values: dict[str, object] = {
        "environment": "production",
        "pool_name": "oldlab",
        "allowed_nodes": ("oldlab-1", "oldlab-2", "oldlab-3"),
        "env_file": "/secure/.env.remote-worker",
        "repo_dir": "/opt/loom",
        "partition": "",
        "time_limit": "7-00:00:00",
        "requested_cpus": 12,
        "requested_memory_mib": 58000,
        "requested_concurrency": 6,
        "max_jobs": 3,
        "pending_job_cap": 2,
        "min_queued_trials": 1,
        "stale_after_seconds": 300,
        "sbatch_path": "sbatch",
        "squeue_path": "squeue",
        "sacct_path": "sacct",
        "scancel_path": "scancel",
        "command_timeout_seconds": 20.0,
    }
    values.update(overrides)
    return ElasticSlurmWorkerControllerConfig(**values)  # type: ignore[arg-type]


@pytest.fixture(autouse=True)
async def _cleanup_db(postgres_url: str) -> Iterator[None]:
    yield
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as s:
        await s.execute(delete(Trial))
        await s.execute(delete(SlurmWorkerJob))
        await s.execute(delete(Worker))
        await s.execute(delete(Task))
        await s.execute(delete(Team))
        await s.commit()
    await engine.dispose()


async def _seed_team_task_trials(
    session_factory: async_sessionmaker,
    *,
    queued: int,
    running: int = 0,
) -> None:
    team_id = uuid4()
    await _seed_team_task(session_factory, team_id=team_id, task_id="task-1")
    async with session_factory() as s:
        for idx in range(queued):
            await s.execute(insert(Trial).values(
                id=uuid4(),
                team_id=team_id,
                task_id="task-1",
                config={},
                requires_caps={},
                state="queued",
                idempotency_key=f"queued-{idx}",
            ))
        for idx in range(running):
            await s.execute(insert(Trial).values(
                id=uuid4(),
                team_id=team_id,
                task_id="task-1",
                config={},
                requires_caps={},
                state="running",
                idempotency_key=f"running-{idx}",
            ))
        await s.commit()


async def _seed_team_task(
    session_factory: async_sessionmaker,
    *,
    team_id,
    task_id: str,
) -> None:
    async with session_factory() as s:
        await s.execute(insert(Team).values(id=team_id, name=f"team-{team_id}"))
        await s.execute(insert(Task).values(id=task_id, checksum="0" * 64, config={}))
        await s.commit()


async def test_controller_submits_records_for_queued_backlog(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        await _seed_team_task_trials(session_factory, queued=13)
        runner = FakeSlurmRunner(submit_results={
            "oldlab-1": "501",
            "oldlab-2": "502",
            "oldlab-3": "503",
        })

        async with session_factory() as s:
            result = await run_elastic_slurm_worker_controller_once(
                s,
                config=_config(),
                runner=runner,
            )
            await s.commit()

        assert result.submitted_job_ids == ("501", "502", "503")
        assert runner.submitted_nodes == ["oldlab-1", "oldlab-2", "oldlab-3"]

        async with session_factory() as s:
            rows = (await s.execute(
                select(SlurmWorkerJob).order_by(SlurmWorkerJob.nodelist),
            )).scalars().all()

        assert [row.job_id for row in rows] == ["501", "502", "503"]
        assert {row.state for row in rows} == {"pending"}
        assert rows[0].redacted_env["LOOM_REMOTE_WORKER_ENV_FILE"] == "/secure/.env.remote-worker"
    finally:
        await engine.dispose()


async def test_controller_records_failed_submissions(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        await _seed_team_task_trials(session_factory, queued=1)
        runner = FakeSlurmRunner(submit_errors={"oldlab-1": "sbatch unavailable"})

        async with session_factory() as s:
            result = await run_elastic_slurm_worker_controller_once(
                s,
                config=_config(max_jobs=1, allowed_nodes=("oldlab-1",)),
                runner=runner,
            )
            await s.commit()

        assert result.submitted_job_ids == ()
        assert runner.submitted_nodes == ["oldlab-1"]

        async with session_factory() as s:
            row = (await s.execute(select(SlurmWorkerJob))).scalar_one()

        assert row.job_id is None
        assert row.state == "failed"
        assert row.submission_error == "sbatch unavailable"
        assert row.redacted_env["LOOM_WORKER_MAX_CONCURRENT"] == "6"
    finally:
        await engine.dispose()


async def test_controller_reconciles_existing_jobs_before_deciding_capacity(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    worker_id = uuid4()
    now = datetime.now(UTC)
    try:
        await _seed_team_task_trials(session_factory, queued=7)
        async with session_factory() as s:
            await s.execute(insert(Worker).values(
                id=worker_id,
                hostname="oldlab-1",
                version="0.1",
                capabilities=[],
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
                job_id="601",
                slurm_state="PENDING",
                state="pending",
                redacted_env={},
                submitted_at=now,
            ))
            await s.commit()

        runner = FakeSlurmRunner(
            submit_results={"oldlab-2": "602"},
            observations=[
                SlurmWorkerJobObservation(
                    job_id="601",
                    slurm_state="RUNNING",
                    nodelist="oldlab-1",
                    worker_id=worker_id,
                ),
            ],
        )

        async with session_factory() as s:
            result = await run_elastic_slurm_worker_controller_once(
                s,
                config=_config(),
                runner=runner,
            )
            await s.commit()

        assert runner.queried_job_ids == ["601"]
        assert runner.submitted_nodes == ["oldlab-2"]
        assert result.submitted_job_ids == ("602",)

        async with session_factory() as s:
            rows = (await s.execute(
                select(SlurmWorkerJob).order_by(SlurmWorkerJob.job_id),
            )).scalars().all()

        assert [(row.job_id, row.state, row.worker_id) for row in rows] == [
            ("601", "running", worker_id),
            ("602", "pending", None),
        ]
    finally:
        await engine.dispose()


async def test_controller_replaces_running_job_with_stale_worker_heartbeat(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    worker_id = uuid4()
    now = datetime.now(UTC)
    try:
        await _seed_team_task_trials(session_factory, queued=1)
        async with session_factory() as s:
            await s.execute(insert(Worker).values(
                id=worker_id,
                hostname="oldlab-1",
                version="0.1",
                capabilities=[],
                registered_at=now - timedelta(minutes=20),
                last_seen_at=now - timedelta(minutes=20),
                status="active",
            ))
            await s.execute(insert(SlurmWorkerJob).values(
                environment="production",
                pool_name="oldlab",
                nodelist="oldlab-1",
                requested_cpus=12,
                requested_memory_mib=58000,
                requested_concurrency=6,
                job_id="611",
                slurm_state="RUNNING",
                state="running",
                worker_id=worker_id,
                redacted_env={},
                submitted_at=now - timedelta(minutes=20),
                started_at=now - timedelta(minutes=19),
            ))
            await s.commit()

        runner = FakeSlurmRunner(
            submit_results={"oldlab-2": "612"},
            observations=[
                SlurmWorkerJobObservation(
                    job_id="611",
                    slurm_state="RUNNING",
                    nodelist="oldlab-1",
                    worker_id=worker_id,
                ),
            ],
        )

        async with session_factory() as s:
            result = await run_elastic_slurm_worker_controller_once(
                s,
                config=_config(stale_after_seconds=300),
                runner=runner,
            )
            await s.commit()

        assert runner.submitted_nodes == ["oldlab-2"]
        assert result.submitted_job_ids == ("612",)

        async with session_factory() as s:
            rows = (await s.execute(
                select(SlurmWorkerJob).order_by(SlurmWorkerJob.job_id),
            )).scalars().all()

        assert [(row.job_id, row.state) for row in rows] == [
            ("611", "stale"),
            ("612", "pending"),
        ]
    finally:
        await engine.dispose()


async def test_controller_cancels_pending_jobs_when_backlog_drains(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        await _seed_team_task_trials(session_factory, queued=0, running=4)
        async with session_factory() as s:
            await s.execute(insert(SlurmWorkerJob).values(
                environment="production",
                pool_name="oldlab",
                nodelist="oldlab-2",
                requested_cpus=12,
                requested_memory_mib=58000,
                requested_concurrency=6,
                job_id="701",
                slurm_state="PENDING",
                state="pending",
                redacted_env={},
                submitted_at=now,
            ))
            await s.commit()

        runner = FakeSlurmRunner(observations=[
            SlurmWorkerJobObservation(
                job_id="701",
                slurm_state="PENDING",
                nodelist="oldlab-2",
                pending_reason="Resources",
            ),
        ])
        async with session_factory() as s:
            result = await run_elastic_slurm_worker_controller_once(
                s,
                config=_config(),
                runner=runner,
            )
            await s.commit()

        assert runner.cancelled_job_ids == ["701"]
        assert result.cancelled_job_ids == ("701",)
        async with session_factory() as s:
            row = (await s.execute(select(SlurmWorkerJob))).scalar_one()
        assert row.state == "cancelled"
        assert row.pending_reason == "cancelled after Loom backlog drained"
    finally:
        await engine.dispose()
