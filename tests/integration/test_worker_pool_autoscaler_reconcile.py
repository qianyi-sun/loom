from __future__ import annotations

import base64
import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import delete, insert, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import loom_control_plane.worker_pool_autoscaler as autoscaler
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
    SlurmNodeResource,
    SlurmWorkerCommandRunner,
)
from loom_control_plane.global_execution_fence import (
    GlobalExecutionWitness,
    canonical_global_execution_witness_bytes,
)
from loom_control_plane.slurm_worker_jobs import (
    SlurmWorkerJobObservation,
    reconcile_slurm_worker_jobs,
)
from loom_control_plane.worker_pool_autoscaler import (
    reconcile_worker_pool_autoscaler_once,
)

_MATCHING_SLURM_RELEASE_ENV = {
    "LOOM_REMOTE_WORKER_ENV_FILE": "/secure/.env.remote-worker",
    "LOOM_REMOTE_WORKER_REPO_DIR": "/opt/loom",
}


def _witness(now: datetime, *, pool_id: str) -> GlobalExecutionWitness:
    private_key = Ed25519PrivateKey.from_private_bytes(bytes([7]) * 32)
    public_key = private_key.public_key()
    payload: dict[str, object] = {
        "authority": "global-capacity-manager",
        "pool_id": pool_id,
        "execution_epoch": 0,
        "execution_state": "shadow",
        "executable_new_capacity_ceiling": 0,
        "expires_at": (now + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
        "signing_key_id": "integration-test-manager",
    }
    canonical = canonical_global_execution_witness_bytes(payload)
    payload["canonical_digest"] = hashlib.sha256(canonical).hexdigest()
    payload["signature_base64"] = base64.b64encode(private_key.sign(canonical)).decode("ascii")
    return GlobalExecutionWitness.from_mapping(
        payload,
        public_key=public_key,
        expected_public_key_sha256=hashlib.sha256(
            public_key.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        ).hexdigest(),
    )


class FakeSlurmRunner(SlurmWorkerCommandRunner):
    def __init__(self) -> None:
        self.submitted_nodes: list[str] = []
        self.submitted_configs: list[ElasticSlurmWorkerControllerConfig] = []
        self.cancelled_job_ids: list[str] = []
        self.pending_cancelled_job_ids: list[str] = []
        self.pending_cancel_errors: set[str] = set()
        self.fail_submit_nodes: set[str] = set()
        self.job_observations: list[SlurmWorkerJobObservation] | None = None
        self.job_observation_batches: list[list[SlurmWorkerJobObservation] | Exception] = []
        self.queried_job_ids: list[tuple[str, ...]] = []
        self.node_resources: dict[str, SlurmNodeResource] = {}

    async def query_jobs(
        self,
        job_ids: tuple[str, ...],
    ) -> list[SlurmWorkerJobObservation]:
        self.queried_job_ids.append(job_ids)
        if self.job_observation_batches:
            result = self.job_observation_batches.pop(0)
            if isinstance(result, Exception):
                raise result
            return result
        if self.job_observations is not None:
            return self.job_observations
        return [
            SlurmWorkerJobObservation(job_id=job_id, slurm_state="RUNNING") for job_id in job_ids
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

    async def cancel_pending_job(self, job_id: str) -> None:
        self.pending_cancelled_job_ids.append(job_id)
        if job_id in self.pending_cancel_errors:
            raise RuntimeError(f"conditional scancel failed for {job_id}")

    async def query_node_resources(
        self,
        nodes: tuple[str, ...],
    ) -> dict[str, SlurmNodeResource]:
        return {node: resource for node, resource in self.node_resources.items() if node in nodes}


async def _insert_gb10_pending_policy(
    session: AsyncSession,
    *,
    now: datetime,
    job_ids: tuple[str | None, ...],
) -> None:
    allowed_nodes = [f"trt-gb10-{index + 6}" for index in range(len(job_ids))]
    for node, job_id in zip(allowed_nodes, job_ids, strict=True):
        await session.execute(
            insert(SlurmWorkerJob).values(
                environment="staging",
                pool_name="gb10",
                nodelist=node,
                requested_cpus=2,
                requested_memory_mib=11500,
                requested_concurrency=1,
                candidate_sha="a" * 40,
                job_id=job_id,
                slurm_state="PENDING",
                state="pending",
                redacted_env={
                    "LOOM_REMOTE_WORKER_ENV_FILE": "/secure/.env.remote-worker",
                    "LOOM_REMOTE_WORKER_REPO_DIR": "/opt/loom",
                },
                submitted_at=now - timedelta(seconds=60),
            )
        )
    await session.execute(
        insert(WorkerPoolAutoscalerPolicy).values(
            environment="staging",
            pool_name="gb10",
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
                "allowed_nodes": allowed_nodes,
                "env_file": "/secure/.env.remote-worker",
                "exclusive": False,
                "container_cpus": 2.0,
                "container_memory_mib": 11500,
                "container_pids": 512,
                "candidate_sha": "a" * 40,
                "job_pids_max": 5120,
                "repo_dir": "/opt/loom",
                "requested_cpus": 20,
                "requested_memory_mib": 115000,
                "requested_concurrency": 10,
                "max_jobs": max(1, len(allowed_nodes)),
                "pending_job_cap": max(1, len(allowed_nodes)),
                "time_limit": "1-00:00:00",
            },
        )
    )


async def _insert_gb10_requeue_hold_policy(
    session: AsyncSession,
    *,
    now: datetime,
    job_id: str = "16647",
) -> None:
    await _insert_gb10_pending_policy(session, now=now, job_ids=(job_id,))
    job = (await session.execute(select(SlurmWorkerJob))).scalar_one()
    job.slurm_cluster_id = "gb10"
    job.state = "failed"
    job.slurm_state = "REQUEUE_HOLD"
    job.pending_reason = "(launch failure limit exceeded requeued held)"
    job.started_at = now - timedelta(days=6)
    job.finished_at = now - timedelta(days=6)
    policy = (await session.execute(select(WorkerPoolAutoscalerPolicy))).scalar_one()
    policy.min_slots = 1


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


async def test_reconcile_without_selected_policy_never_assigns_neutral_work(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

    async def unexpected_assignment(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("neutral work must not be assigned without a selected policy")

    monkeypatch.setattr(autoscaler, "assign_neutral_queued_trials", unexpected_assignment)
    try:
        async with session_factory() as session:
            decisions = await reconcile_worker_pool_autoscaler_once(
                session,
                environment="staging",
                now=now,
            )
            await session.commit()
        assert decisions == []
    finally:
        await engine.dispose()


async def test_denied_execution_evidence_skips_pipeline_activation_calculation(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

    def unexpected_activation(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("denied execution evidence must clamp before activation calculation")

    monkeypatch.setattr(autoscaler, "_apply_pipeline_scoped_activation", unexpected_activation)
    try:
        async with session_factory() as session:
            await session.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
                    environment="staging",
                    pool_name="oldlab",
                    actuator="slurm",
                    enabled=True,
                    min_slots=1,
                    max_slots=1,
                    scale_up_threshold_slots=1,
                    scale_down_idle_seconds=60,
                    scale_up_cooldown_seconds=0,
                    scale_down_cooldown_seconds=0,
                    drain_timeout_seconds=60,
                    actuator_config={
                        "backend": "docker",
                        "cpu_arch": "x86_64",
                        "allowed_nodes": ["oldlab-1"],
                        "env_file": "/secure/.env.remote-worker",
                        "exclusive": False,
                        "container_cpus": 1.0,
                        "container_memory_mib": 1024,
                        "container_pids": 64,
                        "candidate_sha": "a" * 40,
                        "job_pids_max": 64,
                        "repo_dir": "/opt/loom",
                        "requested_cpus": 1,
                        "requested_memory_mib": 1024,
                        "requested_concurrency": 1,
                        "max_jobs": 1,
                        "pending_job_cap": 1,
                        "time_limit": "01:00:00",
                    },
                )
            )
            await session.commit()
        async with session_factory() as session:
            decisions = await reconcile_worker_pool_autoscaler_once(
                session,
                environment="staging",
                now=now,
            )
            await session.commit()
        assert len(decisions) == 1
        assert decisions[0].desired_slots == 0
    finally:
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
                await s.execute(
                    insert(Worker).values(
                        id=worker_id,
                        hostname=f"oldlab-{idx}",
                        version="test",
                        capabilities=[
                            {
                                "backend": "docker",
                                "os": "linux",
                                "cpu_arch": "x86_64",
                                "gpu_vendor": "none",
                                "network_policies": ["none"],
                            }
                        ],
                        max_concurrent=6,
                        pool_name="oldlab",
                        drain_state="active",
                        registered_at=now,
                        last_seen_at=now,
                        status="active",
                    )
                )
            # #1021: the Slurm actuator only releases workers it owns, so each
            # idle worker is linked to a running Slurm job on its node.
            for idx, worker_id in enumerate(worker_ids, start=1):
                await s.execute(
                    insert(SlurmWorkerJob).values(
                        environment="production",
                        pool_name="oldlab",
                        nodelist=f"oldlab-{idx}",
                        worker_id=worker_id,
                        requested_cpus=12,
                        requested_memory_mib=58000,
                        requested_concurrency=6,
                        job_id=f"job-oldlab-{idx}",
                        slurm_state="RUNNING",
                        state="running",
                        redacted_env=dict(_MATCHING_SLURM_RELEASE_ENV),
                        submitted_at=now - timedelta(seconds=300),
                    )
                )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
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
                    actuator_config={
                        "backend": "docker",
                        "cpu_arch": "x86_64",
                        "allowed_nodes": ["oldlab-1", "oldlab-2"],
                        "env_file": "/secure/.env.remote-worker",
                        "exclusive": False,
                        "container_cpus": 2.0,
                        "container_memory_mib": 4096,
                        "container_pids": 512,
                        "candidate_sha": "a" * 40,
                        "job_pids_max": 8192,
                        "repo_dir": "/opt/loom",
                        "requested_cpus": 12,
                        "requested_memory_mib": 58000,
                        "requested_concurrency": 6,
                        "max_jobs": 2,
                        "pending_job_cap": 2,
                        "time_limit": "7-00:00:00",
                    },
                    idle_since_at=now - timedelta(seconds=601),
                )
            )
            await s.commit()

        runner = FakeSlurmRunner()
        runner.job_observations = [
            SlurmWorkerJobObservation(
                job_id=f"job-oldlab-{idx}",
                slurm_state="RUNNING",
                pending_reason="",
                observed_at=now,
            )
            for idx in (1, 2)
        ]
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="oldlab"),
            )
            await s.commit()

        assert len(results) == 1
        assert results[0].action == "request_drain"
        assert results[0].desired_slots == 6
        assert len(results[0].worker_ids_to_drain) == 1

        async with session_factory() as s:
            workers = (
                (
                    await s.execute(
                        select(Worker).order_by(Worker.hostname),
                    )
                )
                .scalars()
                .all()
            )
            policy = (
                await s.execute(
                    select(WorkerPoolAutoscalerPolicy),
                )
            ).scalar_one()

        drain_states = [worker.drain_state for worker in workers]
        assert drain_states.count("draining") == 1
        assert drain_states.count("active") == 1
        assert policy.last_decision == "request_drain"
        assert policy.last_decision_reason == "idle_excess_capacity"
        assert policy.last_desired_slots == 6
        assert policy.last_actual_slots == 12
    finally:
        await engine.dispose()


async def test_reconcile_cancels_pending_job_without_staling_foreign_pools(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    try:
        async with session_factory() as s:
            await _insert_gb10_pending_policy(s, now=now, job_ids=("11630",))
            for environment, pool_name, job_id in (
                ("production", "gb10", "21630"),
                ("staging", "oldlab", "31630"),
            ):
                await s.execute(
                    insert(SlurmWorkerJob).values(
                        environment=environment,
                        pool_name=pool_name,
                        nodelist=f"foreign-{job_id}",
                        requested_cpus=2,
                        requested_memory_mib=4096,
                        requested_concurrency=1,
                        candidate_sha="b" * 40,
                        job_id=job_id,
                        slurm_state="PENDING",
                        state="pending",
                        redacted_env={},
                        submitted_at=now - timedelta(seconds=600),
                    )
                )
            await s.commit()

        runner = FakeSlurmRunner()
        runner.job_observation_batches = [
            [
                SlurmWorkerJobObservation(
                    job_id="11630",
                    slurm_state="PENDING",
                    pending_reason="(Resources)",
                    observed_at=now,
                )
            ],
            [
                SlurmWorkerJobObservation(
                    job_id="11630",
                    slurm_state="CANCELLED",
                    pending_reason="Cancelled",
                    observed_at=now,
                )
            ],
        ]
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="staging",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="gb10"),
            )
            await s.commit()

        assert len(results) == 1
        assert results[0].action == "cancel_pending"
        assert results[0].reason == "idle_excess_pending_capacity"
        assert results[0].desired_slots == 0
        assert results[0].pending_slots == 0
        assert runner.cancelled_job_ids == []
        assert runner.pending_cancelled_job_ids == ["11630"]

        async with session_factory() as s:
            policy = (await s.execute(select(WorkerPoolAutoscalerPolicy))).scalar_one()
            jobs = (
                (
                    await s.execute(
                        select(SlurmWorkerJob).order_by(SlurmWorkerJob.job_id),
                    )
                )
                .scalars()
                .all()
            )

        assert policy.last_decision == "cancel_pending"
        assert policy.last_decision_reason == "idle_excess_pending_capacity"
        assert policy.last_pending_slots == 0
        assert policy.last_scale_down_at == now
        assert [job.job_id for job in jobs] == ["11630", "21630", "31630"]
        assert [job.state for job in jobs] == ["cancelled", "pending", "pending"]
        assert jobs[0].slurm_state == "CANCELLED"
        assert jobs[0].pending_reason == "cancelled after autoscaler demand returned to zero"
        assert jobs[0].finished_at == now
    finally:
        await engine.dispose()


async def test_reconcile_recovers_and_cancels_requeue_hold_before_replacement(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 27, 12, 0, tzinfo=UTC)
    try:
        async with session_factory() as session:
            await _insert_gb10_requeue_hold_policy(session, now=now)
            await session.execute(
                insert(SlurmWorkerJob).values(
                    slurm_cluster_id="gb10",
                    environment="production",
                    pool_name="gb10",
                    nodelist="trt-gb10-13",
                    requested_cpus=2,
                    requested_memory_mib=11500,
                    requested_concurrency=1,
                    candidate_sha="b" * 40,
                    job_id="26647",
                    slurm_state="REQUEUE_HOLD",
                    state="failed",
                    redacted_env={},
                    submitted_at=now - timedelta(days=6),
                )
            )
            await session.commit()

        runner = FakeSlurmRunner()
        runner.job_observation_batches = [
            [
                SlurmWorkerJobObservation(
                    job_id="16647",
                    slurm_state="REQUEUE_HOLD",
                    pending_reason="(launch failure limit exceeded requeued held)",
                    observed_at=now,
                )
            ],
            [
                SlurmWorkerJobObservation(
                    job_id="16647",
                    slurm_state="CANCELLED",
                    pending_reason="Cancelled",
                    observed_at=now,
                )
            ],
        ]
        async with session_factory() as session:
            results = await reconcile_worker_pool_autoscaler_once(
                session,
                environment="staging",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="gb10"),
            )
            await session.commit()

        assert len(results) == 1
        assert results[0].action == "cancel_pending"
        assert results[0].reason == "unrecoverable_slurm_state"
        assert results[0].desired_slots == 1
        assert runner.queried_job_ids == [("16647",), ("16647",)]
        assert runner.cancelled_job_ids == ["16647"]
        assert runner.pending_cancelled_job_ids == []
        assert runner.submitted_nodes == []

        async with session_factory() as session:
            policy = (await session.execute(select(WorkerPoolAutoscalerPolicy))).scalar_one()
            jobs = (
                (
                    await session.execute(
                        select(SlurmWorkerJob).order_by(SlurmWorkerJob.job_id),
                    )
                )
                .scalars()
                .all()
            )
        job, foreign_job = jobs
        assert policy.last_decision == "cancel_pending"
        assert policy.last_decision_reason == "unrecoverable_slurm_state"
        assert job.state == "cancelled"
        assert job.slurm_state == "CANCELLED"
        assert job.pending_reason == "cancelled after unrecoverable Slurm state"
        assert job.finished_at == now
        assert foreign_job.job_id == "26647"
        assert foreign_job.state == "failed"
        assert foreign_job.slurm_state == "REQUEUE_HOLD"
    finally:
        await engine.dispose()


async def test_reconcile_blocks_replacement_when_requeue_hold_refresh_fails(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 27, 12, 0, tzinfo=UTC)
    try:
        async with session_factory() as session:
            await _insert_gb10_requeue_hold_policy(session, now=now)
            await session.commit()

        runner = FakeSlurmRunner()
        runner.job_observation_batches = [RuntimeError("squeue unavailable")]
        async with session_factory() as session:
            results = await reconcile_worker_pool_autoscaler_once(
                session,
                environment="staging",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="gb10"),
            )
            await session.commit()

        assert len(results) == 1
        assert results[0].action == "blocked"
        assert results[0].reason == "slurm_job_refresh_failed"
        assert results[0].blocked_reason == "slurm_job_refresh_failed"
        assert runner.queried_job_ids == [("16647",)]
        assert runner.cancelled_job_ids == []
        assert runner.pending_cancelled_job_ids == []
        assert runner.submitted_nodes == []

        async with session_factory() as session:
            job = (await session.execute(select(SlurmWorkerJob))).scalar_one()
        assert job.state == "failed"
        assert job.slurm_state == "REQUEUE_HOLD"
    finally:
        await engine.dispose()


async def test_reconcile_cancels_only_fresh_pending_release_drift_job(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    try:
        async with session_factory() as s:
            await _insert_gb10_pending_policy(
                s,
                now=now,
                job_ids=("16246", "current-release-job"),
            )
            stale_job = (
                await s.execute(
                    select(SlurmWorkerJob).where(SlurmWorkerJob.job_id == "16246"),
                )
            ).scalar_one()
            stale_job.redacted_env = {
                "LOOM_REMOTE_WORKER_ENV_FILE": "/shared_work2/loom-staging-rollout/worker-envs/stale.env",
                "LOOM_REMOTE_WORKER_REPO_DIR": "/shared_work2/loom-staging-rollout/worker-repos/stale",
            }
            await s.commit()

        runner = FakeSlurmRunner()
        runner.job_observation_batches = [
            [
                SlurmWorkerJobObservation(
                    job_id="16246",
                    slurm_state="PENDING",
                    pending_reason="(Resources)",
                    observed_at=now,
                ),
                SlurmWorkerJobObservation(
                    job_id="current-release-job",
                    slurm_state="PENDING",
                    pending_reason="(Resources)",
                    observed_at=now,
                ),
            ],
            [
                SlurmWorkerJobObservation(
                    job_id="16246",
                    slurm_state="CANCELLED",
                    pending_reason="Cancelled",
                    observed_at=now,
                )
            ],
        ]
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="staging",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="gb10"),
            )
            await s.commit()

        assert len(results) == 1
        assert results[0].action == "cancel_pending"
        assert results[0].reason == "release_state_drift"
        assert results[0].blocked_reason is None
        assert results[0].pending_slots == 1
        assert runner.pending_cancelled_job_ids == ["16246"]

        async with session_factory() as s:
            policy = (await s.execute(select(WorkerPoolAutoscalerPolicy))).scalar_one()
            jobs = (
                (
                    await s.execute(
                        select(SlurmWorkerJob).order_by(SlurmWorkerJob.job_id),
                    )
                )
                .scalars()
                .all()
            )

        assert policy.last_decision == "cancel_pending"
        assert policy.last_decision_reason == "release_state_drift"
        assert policy.last_blocked_reason is None
        assert policy.last_error is None
        assert [job.job_id for job in jobs] == ["16246", "current-release-job"]
        assert [job.state for job in jobs] == ["cancelled", "pending"]
        assert jobs[0].pending_reason == "cancelled after autoscaler release-state drift"
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    ("environment", "pool_name"),
    (("staging", None), (None, "gb10")),
)
async def test_slurm_reconcile_rejects_partial_pool_scope(
    postgres_url: str,
    environment: str | None,
    pool_name: str | None,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with session_factory() as session:
            with pytest.raises(
                ValueError,
                match="environment and pool_name must be provided together",
            ):
                await reconcile_slurm_worker_jobs(
                    session,
                    [],
                    stale_after_seconds=300,
                    environment=environment,
                    pool_name=pool_name,
                )
    finally:
        await engine.dispose()


async def test_reconcile_blocks_pending_cancel_when_slurm_refresh_fails(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    try:
        async with session_factory() as s:
            await _insert_gb10_pending_policy(s, now=now, job_ids=("11630",))
            await s.commit()

        runner = FakeSlurmRunner()
        runner.job_observation_batches = [RuntimeError("squeue unavailable")]
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="staging",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="gb10"),
            )
            await s.commit()

        assert len(results) == 1
        assert results[0].action == "blocked"
        assert results[0].reason == "slurm_job_refresh_failed"
        assert results[0].blocked_reason == "slurm_job_refresh_failed"
        assert results[0].error_message == "squeue unavailable"
        assert results[0].pending_slots == 1
        assert runner.pending_cancelled_job_ids == []

        async with session_factory() as s:
            policy = (await s.execute(select(WorkerPoolAutoscalerPolicy))).scalar_one()
            job = (await s.execute(select(SlurmWorkerJob))).scalar_one()
        assert policy.last_decision == "blocked"
        assert policy.last_scale_down_at is None
        assert policy.last_error == "squeue unavailable"
        assert job.state == "pending"
    finally:
        await engine.dispose()


async def test_reconcile_does_not_cancel_job_that_starts_after_pending_observation(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    try:
        async with session_factory() as s:
            await _insert_gb10_pending_policy(s, now=now, job_ids=("11630",))
            await s.commit()

        runner = FakeSlurmRunner()
        runner.job_observation_batches = [
            [
                SlurmWorkerJobObservation(
                    job_id="11630",
                    slurm_state="PENDING",
                    observed_at=now,
                )
            ],
            [
                SlurmWorkerJobObservation(
                    job_id="11630",
                    slurm_state="RUNNING",
                    observed_at=now,
                )
            ],
        ]
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="staging",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="gb10"),
            )
            await s.commit()

        assert results[0].action == "blocked"
        assert results[0].reason == "slurm_pending_cancel_failed"
        assert results[0].blocked_reason == "slurm_pending_cancel_failed"
        assert results[0].pending_slots == 0
        assert runner.cancelled_job_ids == []
        assert runner.pending_cancelled_job_ids == ["11630"]

        async with session_factory() as s:
            policy = (await s.execute(select(WorkerPoolAutoscalerPolicy))).scalar_one()
            job = (await s.execute(select(SlurmWorkerJob))).scalar_one()
        assert policy.last_scale_down_at is None
        assert policy.last_error is not None
        assert "11630" in policy.last_error
        assert job.state == "running"
        assert job.slurm_state == "RUNNING"
        assert job.finished_at is None
    finally:
        await engine.dispose()


async def test_reconcile_blocks_idless_pending_slurm_registry_row(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    try:
        async with session_factory() as s:
            await _insert_gb10_pending_policy(s, now=now, job_ids=(None,))
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="staging",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="gb10"),
            )
            await s.commit()

        assert results[0].action == "blocked"
        assert results[0].reason == "slurm_pending_observation_missing"
        assert results[0].pending_slots == 1
        assert runner.queried_job_ids == []
        assert runner.pending_cancelled_job_ids == []

        async with session_factory() as s:
            policy = (await s.execute(select(WorkerPoolAutoscalerPolicy))).scalar_one()
            job = (await s.execute(select(SlurmWorkerJob))).scalar_one()
        assert policy.last_scale_down_at is None
        assert policy.last_error is not None
        assert job.state == "pending"
    finally:
        await engine.dispose()


async def test_reconcile_blocks_when_pending_slurm_observation_is_incomplete(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    try:
        async with session_factory() as s:
            await _insert_gb10_pending_policy(
                s,
                now=now,
                job_ids=("11630", "11631"),
            )
            await s.commit()

        runner = FakeSlurmRunner()
        runner.job_observation_batches = [
            [
                SlurmWorkerJobObservation(
                    job_id="11630",
                    slurm_state="PENDING",
                    observed_at=now,
                )
            ]
        ]
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="staging",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="gb10"),
            )
            await s.commit()

        assert results[0].action == "blocked"
        assert results[0].reason == "slurm_pending_observation_missing"
        assert results[0].pending_slots == 2
        assert runner.pending_cancelled_job_ids == []

        async with session_factory() as s:
            policy = (await s.execute(select(WorkerPoolAutoscalerPolicy))).scalar_one()
            jobs = (await s.execute(select(SlurmWorkerJob))).scalars().all()
        assert policy.last_scale_down_at is None
        assert policy.last_error is not None
        assert {job.state for job in jobs} == {"pending"}
    finally:
        await engine.dispose()


async def test_reconcile_reports_partial_pending_slurm_cancellation(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    try:
        async with session_factory() as s:
            await _insert_gb10_pending_policy(
                s,
                now=now,
                job_ids=("11630", "11631"),
            )
            await s.commit()

        runner = FakeSlurmRunner()
        runner.pending_cancel_errors.add("11631")
        runner.job_observation_batches = [
            [
                SlurmWorkerJobObservation(
                    job_id="11630",
                    slurm_state="PENDING",
                    observed_at=now,
                ),
                SlurmWorkerJobObservation(
                    job_id="11631",
                    slurm_state="PENDING",
                    observed_at=now,
                ),
            ],
            [
                SlurmWorkerJobObservation(
                    job_id="11630",
                    slurm_state="CANCELLED",
                    observed_at=now,
                )
            ],
        ]
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="staging",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="gb10"),
            )
            await s.commit()

        assert results[0].action == "cancel_pending_partial"
        assert results[0].reason == "slurm_pending_cancel_partial"
        assert results[0].blocked_reason == "slurm_pending_cancel_partial"
        assert results[0].pending_slots == 1
        assert runner.pending_cancelled_job_ids == ["11630", "11631"]

        async with session_factory() as s:
            policy = (await s.execute(select(WorkerPoolAutoscalerPolicy))).scalar_one()
            jobs = (
                (
                    await s.execute(
                        select(SlurmWorkerJob).order_by(SlurmWorkerJob.job_id),
                    )
                )
                .scalars()
                .all()
            )
        assert policy.last_scale_down_at == now
        assert policy.last_error is not None
        assert "11631" in policy.last_error
        assert [job.state for job in jobs] == ["cancelled", "pending"]
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
                await s.execute(
                    insert(Trial).values(
                        id=uuid4(),
                        team_id=team_id,
                        task_id="task-a",
                        config={},
                        requires_caps={"backend": "docker", "cpu_arch": "x86_64"},
                        state="queued",
                        idempotency_key=f"queued-{idx}",
                    )
                )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
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
                        # Non-exclusive Loom Slurm workers require the full containment contract.
                        "exclusive": False,
                        "container_cpus": 2.0,
                        "container_memory_mib": 4096,
                        "container_pids": 512,
                        "candidate_sha": "a" * 40,
                        "job_pids_max": 8192,
                        "repo_dir": "/opt/loom",
                        "requested_cpus": 12,
                        "requested_memory_mib": 58000,
                        "requested_concurrency": 6,
                        "max_jobs": 2,
                        "pending_job_cap": 2,
                        "time_limit": "7-00:00:00",
                    },
                )
            )
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="oldlab"),
            )
            await s.commit()

        assert results[0].action == "scale_up"
        assert runner.submitted_nodes == ["oldlab-1", "oldlab-2"]

        async with session_factory() as s:
            jobs = (
                (
                    await s.execute(
                        select(SlurmWorkerJob).order_by(SlurmWorkerJob.nodelist),
                    )
                )
                .scalars()
                .all()
            )
            policy = (
                await s.execute(
                    select(WorkerPoolAutoscalerPolicy),
                )
            ).scalar_one()

        assert [job.job_id for job in jobs] == ["job-oldlab-1", "job-oldlab-2"]
        assert {job.state for job in jobs} == {"pending"}
        assert policy.last_decision == "scale_up"
        assert policy.last_decision_reason == "queued_deficit"
        assert policy.last_pending_slots == 12
    finally:
        await engine.dispose()


async def test_reconcile_clamps_scale_up_slots_to_max_slots(
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
            for idx in range(4):
                await s.execute(
                    insert(Trial).values(
                        id=uuid4(),
                        team_id=team_id,
                        task_id="task-a",
                        config={},
                        requires_caps={"backend": "docker", "cpu_arch": "x86_64"},
                        state="queued",
                        idempotency_key=f"clamp-queued-{idx}",
                    )
                )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
                    environment="production",
                    pool_name="oldlab",
                    actuator="slurm",
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
                        "cpu_arch": "x86_64",
                        "allowed_nodes": ["oldlab-1", "oldlab-2"],
                        "env_file": "/secure/.env.remote-worker",
                        # Non-exclusive Loom Slurm workers require the full containment contract.
                        "exclusive": False,
                        "container_cpus": 2.0,
                        "container_memory_mib": 4096,
                        "container_pids": 512,
                        "candidate_sha": "a" * 40,
                        "job_pids_max": 8192,
                        "repo_dir": "/opt/loom",
                        "requested_cpus": 20,
                        "requested_memory_mib": 115000,
                        "requested_concurrency": 10,
                        "cpu_per_slot": 2,
                        "memory_mib_per_slot": 8192,
                        "max_jobs": 2,
                        "pending_job_cap": 2,
                        "time_limit": "7-00:00:00",
                    },
                )
            )
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="oldlab"),
            )
            await s.commit()

        assert results[0].action == "scale_up"
        # A single 10-slot worker would overshoot the 4-slot budget. When
        # clamped to 4 slots, CPU/memory scale PROPORTIONALLY from the 10-slot
        # request (staging GB10 profile: 20 CPU / 115000 MiB), not from the
        # per-slot defaults. So memory = 115000 * 4 / 10 = 46000 MiB, NOT the
        # 4 * 8192 = 32768 MiB that per-slot scaling would (wrongly) produce.
        assert runner.submitted_configs[0].requested_concurrency == 4
        assert runner.submitted_configs[0].requested_cpus == 8  # 20 * 4 / 10
        assert runner.submitted_configs[0].requested_memory_mib == 46000  # 115000 * 4 / 10
        assert sum(c.requested_concurrency for c in runner.submitted_configs) <= 4

        async with session_factory() as s:
            jobs = (await s.execute(select(SlurmWorkerJob))).scalars().all()
        assert sum(job.requested_concurrency for job in jobs) <= 4
    finally:
        await engine.dispose()


async def test_reconcile_clamp_uses_ceiling_not_bankers_rounding(
    postgres_url: str,
) -> None:
    # Odd ratio: 5 CPU / 2 slots clamped to 1 slot must ceil(2.5)=3, never
    # round(2.5)=2 (banker's rounding would under-request Slurm resources).
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    team_id = uuid4()
    try:
        async with session_factory() as s:
            await s.execute(insert(Team).values(id=team_id, name="team-ceil"))
            await s.execute(insert(Task).values(id="task-ceil", checksum="0" * 64, config={}))
            await s.execute(
                insert(Trial).values(
                    id=uuid4(),
                    team_id=team_id,
                    task_id="task-ceil",
                    config={},
                    requires_caps={"backend": "docker", "cpu_arch": "x86_64"},
                    state="queued",
                    idempotency_key="clamp-ceil-0",
                )
            )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
                    environment="production",
                    pool_name="oldlab",
                    actuator="slurm",
                    enabled=True,
                    min_slots=0,
                    max_slots=1,
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
                        # Non-exclusive Loom Slurm workers require the full containment contract.
                        "exclusive": False,
                        "container_cpus": 2.0,
                        "container_memory_mib": 4096,
                        "container_pids": 512,
                        "candidate_sha": "a" * 40,
                        "job_pids_max": 8192,
                        "repo_dir": "/opt/loom",
                        "requested_cpus": 5,
                        "requested_memory_mib": 5000,
                        "requested_concurrency": 2,
                        "cpu_per_slot": 2,
                        "memory_mib_per_slot": 8192,
                        "max_jobs": 1,
                        "pending_job_cap": 1,
                        "time_limit": "7-00:00:00",
                    },
                )
            )
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="oldlab"),
            )
            await s.commit()

        assert results[0].action == "scale_up"
        assert runner.submitted_configs[0].requested_concurrency == 1
        assert runner.submitted_configs[0].requested_cpus == 3  # ceil(5 * 1 / 2), not round()==2
        assert runner.submitted_configs[0].requested_memory_mib == 2500  # ceil(5000 * 1 / 2)
    finally:
        await engine.dispose()


async def test_reconcile_qos_is_per_submission_and_prefers_qos_normal(
    postgres_url: str,
) -> None:
    # Two bugs in one regression: (1) QoS is chosen PER submission from committed
    # slots, so a reconcile starting below min_slots and crossing it mid-loop
    # gives boost only to the sub-floor jobs; (2) qos_normal wins over legacy
    # slurm_qos. min_slots=2, 4 one-slot workers: committed 0,1 -> boost;
    # committed 2,3 -> normal (=qos_normal "normal-qos", NOT slurm_qos "legacy").
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    team_id = uuid4()
    try:
        async with session_factory() as s:
            await s.execute(insert(Team).values(id=team_id, name="team-qos"))
            await s.execute(insert(Task).values(id="task-qos", checksum="0" * 64, config={}))
            for idx in range(4):
                await s.execute(
                    insert(Trial).values(
                        id=uuid4(),
                        team_id=team_id,
                        task_id="task-qos",
                        config={},
                        requires_caps={"backend": "docker", "cpu_arch": "x86_64"},
                        state="queued",
                        idempotency_key=f"qos-queued-{idx}",
                    )
                )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
                    environment="production",
                    pool_name="oldlab",
                    actuator="slurm",
                    enabled=True,
                    min_slots=2,
                    max_slots=4,
                    scale_up_threshold_slots=1,
                    scale_down_idle_seconds=600,
                    scale_up_cooldown_seconds=60,
                    scale_down_cooldown_seconds=300,
                    drain_timeout_seconds=600,
                    actuator_config={
                        "backend": "docker",
                        "cpu_arch": "x86_64",
                        "allowed_nodes": ["oldlab-1", "oldlab-2", "oldlab-3", "oldlab-4"],
                        "env_file": "/secure/.env.remote-worker",
                        # Non-exclusive Loom Slurm workers require the full containment contract.
                        "exclusive": False,
                        "container_cpus": 2.0,
                        "container_memory_mib": 4096,
                        "container_pids": 512,
                        "candidate_sha": "a" * 40,
                        "job_pids_max": 8192,
                        "repo_dir": "/opt/loom",
                        "requested_cpus": 2,
                        "requested_memory_mib": 8192,
                        "requested_concurrency": 1,
                        "max_jobs": 4,
                        "pending_job_cap": 4,
                        "time_limit": "7-00:00:00",
                        "qos_boost": "boost-qos",
                        "qos_normal": "normal-qos",
                        "slurm_qos": "legacy-qos",
                    },
                )
            )
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="oldlab"),
            )
            await s.commit()

        assert results[0].action == "scale_up"
        qoses = [c.slurm_qos for c in runner.submitted_configs]
        assert len(qoses) == 4
        assert qoses[0] == "boost-qos"  # committed 0 < min_slots 2
        assert qoses[1] == "boost-qos"  # committed 1 < 2
        assert qoses[2] == "normal-qos"  # committed 2 >= 2 -> normal, and normal wins over legacy
        assert qoses[3] == "normal-qos"  # committed 3 >= 2
        assert "legacy-qos" not in qoses
    finally:
        await engine.dispose()


async def test_reconcile_clamps_resource_aware_scale_up_to_max_slots(
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
            for idx in range(4):
                await s.execute(
                    insert(Trial).values(
                        id=uuid4(),
                        team_id=team_id,
                        task_id="task-a",
                        config={},
                        requires_caps={"backend": "docker", "cpu_arch": "x86_64"},
                        state="queued",
                        idempotency_key=f"clamp-ra-queued-{idx}",
                    )
                )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
                    environment="production",
                    pool_name="oldlab",
                    actuator="slurm",
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
                        "cpu_arch": "x86_64",
                        "allowed_nodes": ["oldlab-1"],
                        "env_file": "/secure/.env.remote-worker",
                        # Non-exclusive Loom Slurm workers require the full containment contract.
                        "exclusive": False,
                        "container_cpus": 2.0,
                        "container_memory_mib": 4096,
                        "container_pids": 512,
                        "candidate_sha": "a" * 40,
                        "job_pids_max": 8192,
                        "repo_dir": "/opt/loom",
                        "requested_cpus": 2,
                        "requested_memory_mib": 8192,
                        "requested_concurrency": 1,
                        "pending_job_cap": 1,
                        "resource_aware": True,
                        "cpu_per_slot": 2,
                        "memory_mib_per_slot": 8192,
                        "reserved_cpus": 4,
                        "reserved_memory_mib": 24_576,
                        "max_concurrency_per_node": 8,
                        "time_limit": "7-00:00:00",
                    },
                )
            )
            await s.commit()

        runner = FakeSlurmRunner()
        runner.node_resources = {
            "oldlab-1": SlurmNodeResource("oldlab-1", "mixed", 24, 120_000, 4.0),
        }
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="oldlab"),
            )
            await s.commit()

        assert results[0].action == "scale_up"
        # safe_slots would be 8 for this node; clamp to the 4-slot budget.
        assert runner.submitted_configs[0].requested_concurrency == 4
        assert runner.submitted_configs[0].requested_cpus == 8
        assert sum(c.requested_concurrency for c in runner.submitted_configs) <= 4
    finally:
        await engine.dispose()


async def test_reconcile_persists_no_safe_slurm_node_blocker(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    try:
        async with session_factory() as s:
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
                    environment="production",
                    pool_name="oldlab",
                    actuator="slurm",
                    enabled=True,
                    min_slots=1,
                    max_slots=40,
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
                        # Non-exclusive Loom Slurm workers require the full containment contract.
                        "exclusive": False,
                        "container_cpus": 2.0,
                        "container_memory_mib": 4096,
                        "container_pids": 512,
                        "candidate_sha": "a" * 40,
                        "job_pids_max": 8192,
                        "repo_dir": "/opt/loom",
                        "requested_cpus": 2,
                        "requested_memory_mib": 8192,
                        "requested_concurrency": 1,
                        "max_jobs": 2,
                        "pending_job_cap": 2,
                        "resource_aware": True,
                        "cpu_per_slot": 2,
                        "memory_mib_per_slot": 8192,
                        "reserved_cpus": 4,
                        "reserved_memory_mib": 24_576,
                        "max_concurrency_per_node": 8,
                    },
                )
            )
            await s.commit()

        runner = FakeSlurmRunner()
        runner.node_resources = {
            "oldlab-1": SlurmNodeResource("oldlab-1", "mixed", 24, 8_000, 4.0),
            "oldlab-2": SlurmNodeResource("oldlab-2", "drain", 24, 120_000, 4.0),
        }
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="oldlab"),
            )
            await s.commit()

        assert results[0].action == "blocked"
        assert results[0].blocked_reason == "no_safe_slurm_nodes"
        assert runner.submitted_nodes == []

        async with session_factory() as s:
            policy = (
                await s.execute(
                    select(WorkerPoolAutoscalerPolicy),
                )
            ).scalar_one()

        assert policy.last_decision == "blocked"
        assert policy.last_decision_reason == "no_safe_slurm_nodes"
        assert policy.last_blocked_reason == "no_safe_slurm_nodes"
        assert policy.last_blocked_details["node_exclusions"] == [
            {
                "hostname": "oldlab-1",
                "reason": "insufficient_memory",
                "safe_slots": 0,
                "state": "mixed",
                "cpus_total": 24,
                "idle_cpus": None,
                "cpu_load": 4.0,
                "free_memory_mib": 8000,
            },
            {
                "hostname": "oldlab-2",
                "reason": "unsafe_state",
                "safe_slots": 0,
                "state": "drain",
                "cpus_total": 24,
                "idle_cpus": None,
                "cpu_load": 4.0,
                "free_memory_mib": 120000,
            },
        ]
        assert policy.last_scale_up_at is None
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
                await s.execute(
                    insert(Trial).values(
                        id=uuid4(),
                        team_id=team_id,
                        task_id="task-a",
                        config={},
                        requires_caps={"backend": "docker", "cpu_arch": "arm64"},
                        state="queued",
                        idempotency_key=f"queued-gb10-{idx}",
                    )
                )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
                    environment="production",
                    pool_name="gb10",
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
                        # Non-exclusive Loom Slurm workers require the full containment contract.
                        "exclusive": False,
                        "container_cpus": 2.0,
                        "container_memory_mib": 4096,
                        "container_pids": 512,
                        "candidate_sha": "a" * 40,
                        "job_pids_max": 8192,
                        "repo_dir": "/shared_work/qianyi/loom-remote-worker",
                        "partition": "gb10",
                        "requested_cpus": 20,
                        "requested_memory_mib": 115000,
                        "requested_concurrency": 10,
                        "max_jobs": 15,
                        "pending_job_cap": 2,
                        "time_limit": "2-00:00:00",
                    },
                )
            )
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="gb10"),
            )
            await s.commit()

        assert results[0].action == "scale_up"
        assert runner.submitted_nodes == ["trt-gb10-1"]
        assert runner.submitted_configs[0].partition == "gb10"
        assert runner.submitted_configs[0].pool_name == "gb10"
        assert runner.submitted_configs[0].requested_concurrency == 10

        async with session_factory() as s:
            job = (await s.execute(select(SlurmWorkerJob))).scalar_one()
            policy = (
                await s.execute(
                    select(WorkerPoolAutoscalerPolicy),
                )
            ).scalar_one()

        assert job.job_id == "job-trt-gb10-1"
        assert job.nodelist == "trt-gb10-1"
        assert job.requested_cpus == 20
        assert job.requested_memory_mib == 115000
        assert job.requested_concurrency == 10
        assert job.state == "pending"
        assert job.redacted_env["LOOM_WORKER_POOL_NAME"] == "gb10"
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
            await s.execute(
                insert(Trial).values(
                    id=uuid4(),
                    team_id=team_id,
                    task_id="task-a",
                    config={},
                    requires_caps={"backend": "docker", "cpu_arch": "x86_64"},
                    state="queued",
                    idempotency_key="queued-external-runner",
                )
            )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
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
                        # Non-exclusive Loom Slurm workers require the full containment contract.
                        "exclusive": False,
                        "container_cpus": 2.0,
                        "container_memory_mib": 4096,
                        "container_pids": 512,
                        "candidate_sha": "a" * 40,
                        "job_pids_max": 8192,
                        "repo_dir": "/opt/loom",
                        "requested_cpus": 12,
                        "requested_memory_mib": 58000,
                        "requested_concurrency": 6,
                        "max_jobs": 1,
                        "pending_job_cap": 1,
                        "time_limit": "7-00:00:00",
                    },
                )
            )
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="oldlab"),
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
            await s.execute(
                insert(Trial).values(
                    id=uuid4(),
                    team_id=team_id,
                    task_id="task-a",
                    config={},
                    requires_caps={"backend": "docker", "cpu_arch": "x86_64"},
                    state="queued",
                    idempotency_key="queued-external-runner",
                )
            )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
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
                        # Non-exclusive Loom Slurm workers require the full containment contract.
                        "exclusive": False,
                        "container_cpus": 2.0,
                        "container_memory_mib": 4096,
                        "container_pids": 512,
                        "candidate_sha": "a" * 40,
                        "job_pids_max": 8192,
                        "repo_dir": "/opt/loom",
                        "requested_cpus": 12,
                        "requested_memory_mib": 58000,
                        "requested_concurrency": 6,
                        "max_jobs": 1,
                        "pending_job_cap": 1,
                        "time_limit": "7-00:00:00",
                    },
                )
            )
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                slurm_runner=runner,
                include_external_policies=True,
                global_execution_witness=_witness(now, pool_id="oldlab"),
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


async def test_external_slurm_runner_reconcile_can_be_scoped_to_one_pool(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    try:
        async with session_factory() as s:
            for pool_name, node, cpu_arch in (
                ("gb10", "gb10-1", "arm64"),
                ("oldlab", "oldlab-1", "x86_64"),
            ):
                await s.execute(
                    insert(WorkerPoolAutoscalerPolicy).values(
                        environment="production",
                        pool_name=pool_name,
                        actuator="slurm",
                        enabled=True,
                        min_slots=1,
                        max_slots=6,
                        scale_up_threshold_slots=1,
                        scale_down_idle_seconds=600,
                        scale_up_cooldown_seconds=60,
                        scale_down_cooldown_seconds=300,
                        drain_timeout_seconds=600,
                        actuator_config={
                            "backend": "docker",
                            "cpu_arch": cpu_arch,
                            "external_runner": True,
                            "allowed_nodes": [node],
                            "env_file": "/secure/.env.remote-worker",
                            # Non-exclusive Loom Slurm workers require the full containment contract.
                            "exclusive": False,
                            "container_cpus": 2.0,
                            "container_memory_mib": 4096,
                            "container_pids": 512,
                            "candidate_sha": "a" * 40,
                            "job_pids_max": 8192,
                            "repo_dir": "/opt/loom",
                            "requested_cpus": 2,
                            "requested_memory_mib": 8000,
                            "requested_concurrency": 1,
                            "max_jobs": 1,
                            "pending_job_cap": 1,
                            "time_limit": "04:00:00",
                        },
                    )
                )
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                slurm_runner=runner,
                include_external_policies=True,
                external_only=True,
                pool_names=("oldlab",),
                global_execution_witness=_witness(now, pool_id="oldlab"),
            )
            await s.commit()

        assert [result.action for result in results] == ["scale_up"]
        assert runner.submitted_nodes == ["oldlab-1"]

        async with session_factory() as s:
            jobs = (await s.execute(select(SlurmWorkerJob))).scalars().all()

        assert [job.pool_name for job in jobs] == ["oldlab"]
    finally:
        await engine.dispose()


async def test_missing_witness_skips_neutral_trial_assignment_for_mixed_pools(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    team_id = uuid4()
    trial_id = uuid4()
    try:
        async with session_factory() as s:
            await s.execute(insert(Team).values(id=team_id, name="neutral-team"))
            await s.execute(
                insert(Task).values(id="neutral-task", checksum="0" * 64, config={}),
            )
            await s.execute(
                insert(Trial).values(
                    id=trial_id,
                    team_id=team_id,
                    task_id="neutral-task",
                    config={},
                    requires_caps={"backend": "docker", "cpu_arch": "any"},
                    state="queued",
                    idempotency_key="neutral-trial",
                ),
            )
            for pool_name, node, cpu_arch in (
                ("gb10", "gb10-1", "arm64"),
                ("oldlab", "oldlab-1", "x86_64"),
            ):
                await s.execute(
                    insert(WorkerPoolAutoscalerPolicy).values(
                        environment="production",
                        pool_name=pool_name,
                        actuator="slurm",
                        enabled=True,
                        min_slots=0,
                        max_slots=1,
                        scale_up_threshold_slots=1,
                        scale_down_idle_seconds=600,
                        scale_up_cooldown_seconds=60,
                        scale_down_cooldown_seconds=300,
                        drain_timeout_seconds=600,
                        actuator_config={
                            "backend": "docker",
                            "cpu_arch": cpu_arch,
                            "external_runner": True,
                            "allowed_nodes": [node],
                            "env_file": "/secure/.env.remote-worker",
                            "exclusive": False,
                            "container_cpus": 2.0,
                            "container_memory_mib": 4096,
                            "container_pids": 512,
                            "candidate_sha": "a" * 40,
                            "job_pids_max": 8192,
                            "repo_dir": "/opt/loom",
                            "requested_cpus": 2,
                            "requested_memory_mib": 8000,
                            "requested_concurrency": 1,
                            "max_jobs": 1,
                            "pending_job_cap": 1,
                            "time_limit": "04:00:00",
                        },
                    ),
                )
            await s.commit()

        async with session_factory() as s:
            control_plane_results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
            )
            await s.commit()

        assert control_plane_results == []
        async with session_factory() as s:
            assigned_pool = (
                await s.execute(
                    select(Trial.autoscaler_pool_name).where(Trial.id == trial_id),
                )
            ).scalar_one()

        assert assigned_pool is None

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            external_results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                slurm_runner=runner,
                include_external_policies=True,
                external_only=True,
            )
            await s.commit()

        assert {result.action for result in external_results} == {"noop"}
        assert runner.submitted_nodes == []
        async with session_factory() as s:
            jobs = (await s.execute(select(SlurmWorkerJob))).scalars().all()

        assert jobs == []
    finally:
        await engine.dispose()


async def test_scoped_external_witness_only_routes_neutral_trial_to_its_selected_pool(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 21, 18, 0, tzinfo=UTC)
    team_id = uuid4()
    trial_id = uuid4()
    try:
        async with session_factory() as s:
            await s.execute(insert(Team).values(id=team_id, name="external-neutral-team"))
            await s.execute(
                insert(Task).values(id="external-neutral-task", checksum="0" * 64, config={}),
            )
            await s.execute(
                insert(Trial).values(
                    id=trial_id,
                    team_id=team_id,
                    task_id="external-neutral-task",
                    config={},
                    requires_caps={"backend": "docker", "cpu_arch": "any"},
                    state="queued",
                    idempotency_key="external-neutral-trial",
                ),
            )
            for pool_name, node, cpu_arch in (
                ("gb10", "gb10-1", "arm64"),
                ("oldlab", "oldlab-1", "x86_64"),
            ):
                await s.execute(
                    insert(WorkerPoolAutoscalerPolicy).values(
                        environment="staging",
                        pool_name=pool_name,
                        actuator="slurm",
                        enabled=True,
                        min_slots=0,
                        max_slots=1,
                        scale_up_threshold_slots=1,
                        scale_down_idle_seconds=600,
                        scale_up_cooldown_seconds=0,
                        scale_down_cooldown_seconds=0,
                        drain_timeout_seconds=600,
                        actuator_config={
                            "backend": "docker",
                            "cpu_arch": cpu_arch,
                            "external_runner": True,
                            "allowed_nodes": [node],
                            "env_file": "/secure/.env.remote-worker",
                            "exclusive": False,
                            "container_cpus": 2.0,
                            "container_memory_mib": 4096,
                            "container_pids": 512,
                            "candidate_sha": "a" * 40,
                            "job_pids_max": 8192,
                            "repo_dir": "/opt/loom",
                            "requested_cpus": 2,
                            "requested_memory_mib": 8000,
                            "requested_concurrency": 1,
                            "max_jobs": 1,
                            "pending_job_cap": 1,
                            "time_limit": "04:00:00",
                        },
                    ),
                )
            await s.commit()

        foreign_runner = FakeSlurmRunner()
        async with session_factory() as s:
            foreign_results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="staging",
                now=now,
                slurm_runner=foreign_runner,
                include_external_policies=True,
                external_only=True,
                pool_names=("oldlab",),
                global_execution_witness=_witness(now, pool_id="oldlab"),
            )
            await s.commit()

        assert [result.action for result in foreign_results] == ["noop"]
        assert foreign_runner.submitted_nodes == []
        async with session_factory() as s:
            assigned_by_foreign_pool = (
                await s.execute(
                    select(Trial.autoscaler_pool_name).where(Trial.id == trial_id),
                )
            ).scalar_one()

        assert assigned_by_foreign_pool is None

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="staging",
                now=now,
                slurm_runner=runner,
                include_external_policies=True,
                external_only=True,
                pool_names=("gb10",),
                global_execution_witness=_witness(now, pool_id="gb10"),
            )
            await s.commit()

        assert [result.action for result in results] == ["scale_up"]
        assert runner.submitted_nodes == ["gb10-1"]
        async with session_factory() as s:
            assigned_pool = (
                await s.execute(
                    select(Trial.autoscaler_pool_name).where(Trial.id == trial_id),
                )
            ).scalar_one()
            jobs = (await s.execute(select(SlurmWorkerJob))).scalars().all()

        assert assigned_pool == "gb10"
        assert [(job.pool_name, job.nodelist) for job in jobs] == [("gb10", "gb10-1")]
    finally:
        await engine.dispose()


async def test_external_reconcile_never_executes_same_pool_from_foreign_environment(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    try:
        async with session_factory() as s:
            for environment, node in (
                ("production", "foreign-oldlab-1"),
                ("staging", "staging-oldlab-1"),
            ):
                await s.execute(
                    insert(WorkerPoolAutoscalerPolicy).values(
                        environment=environment,
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
                            "allowed_nodes": [node],
                            "env_file": "/secure/.env.remote-worker",
                            # Non-exclusive Loom Slurm workers require the full containment contract.
                            "exclusive": False,
                            "container_cpus": 2.0,
                            "container_memory_mib": 4096,
                            "container_pids": 512,
                            "candidate_sha": "a" * 40,
                            "job_pids_max": 8192,
                            "repo_dir": "/opt/loom",
                            "requested_cpus": 2,
                            "requested_memory_mib": 8000,
                            "requested_concurrency": 1,
                            "max_jobs": 1,
                            "pending_job_cap": 1,
                            "time_limit": "04:00:00",
                        },
                    )
                )
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            only_foreign = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="development",
                now=now,
                slurm_runner=runner,
                include_external_policies=True,
                external_only=True,
                pool_names=("oldlab",),
                global_execution_witness=_witness(now, pool_id="oldlab"),
            )
            await s.commit()

        assert only_foreign == []
        assert runner.submitted_nodes == []

        async with session_factory() as s:
            before = (
                (
                    await s.execute(
                        select(WorkerPoolAutoscalerPolicy).order_by(
                            WorkerPoolAutoscalerPolicy.environment
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert [(row.environment, row.last_decision) for row in before] == [
            ("production", None),
            ("staging", None),
        ]

        async with session_factory() as s:
            intended = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="staging",
                now=now,
                slurm_runner=runner,
                include_external_policies=True,
                external_only=True,
                pool_names=("oldlab",),
                global_execution_witness=_witness(now, pool_id="oldlab"),
            )
            await s.commit()

        assert [decision.action for decision in intended] == ["scale_up"]
        assert runner.submitted_nodes == ["staging-oldlab-1"]

        async with session_factory() as s:
            after = (
                (
                    await s.execute(
                        select(WorkerPoolAutoscalerPolicy).order_by(
                            WorkerPoolAutoscalerPolicy.environment
                        )
                    )
                )
                .scalars()
                .all()
            )
            jobs = (await s.execute(select(SlurmWorkerJob))).scalars().all()
        assert [(row.environment, row.last_decision) for row in after] == [
            ("production", None),
            ("staging", "scale_up"),
        ]
        assert [(job.environment, job.nodelist) for job in jobs] == [
            ("staging", "staging-oldlab-1")
        ]
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
            await s.execute(
                insert(Worker).values(
                    id=worker_id,
                    hostname="oldlab-1",
                    version="test",
                    capabilities=[
                        {
                            "backend": "docker",
                            "os": "linux",
                            "cpu_arch": "x86_64",
                            "gpu_vendor": "none",
                            "network_policies": ["none"],
                        }
                    ],
                    max_concurrent=1,
                    pool_name="oldlab",
                    drain_state="active",
                    registered_at=now,
                    last_seen_at=now,
                    status="active",
                )
            )
            await s.execute(
                insert(SlurmWorkerJob).values(
                    environment="production",
                    pool_name="oldlab",
                    nodelist="oldlab-1",
                    requested_cpus=2,
                    requested_memory_mib=8000,
                    requested_concurrency=1,
                    job_id="9001",
                    slurm_state="PENDING",
                    state="pending",
                    redacted_env=dict(_MATCHING_SLURM_RELEASE_ENV),
                    submitted_at=now - timedelta(seconds=300),
                )
            )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
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
                        # Non-exclusive Loom Slurm workers require the full containment contract.
                        "exclusive": False,
                        "container_cpus": 2.0,
                        "container_memory_mib": 4096,
                        "container_pids": 512,
                        "candidate_sha": "a" * 40,
                        "job_pids_max": 8192,
                        "repo_dir": "/opt/loom",
                        "requested_cpus": 2,
                        "requested_memory_mib": 8000,
                        "requested_concurrency": 1,
                        "max_jobs": 1,
                        "pending_job_cap": 1,
                        "time_limit": "7-00:00:00",
                    },
                )
            )
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
                environment="production",
                now=now,
                slurm_runner=runner,
                include_external_policies=True,
                external_only=True,
                global_execution_witness=_witness(now, pool_id="oldlab"),
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
        # Slurm observations alone cannot claim an unlinked worker registration.
        assert job.worker_id is None
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
            await s.execute(
                insert(Worker).values(
                    id=worker_id,
                    hostname="oldlab-1",
                    version="test",
                    capabilities=[
                        {
                            "backend": "docker",
                            "os": "linux",
                            "cpu_arch": "x86_64",
                            "gpu_vendor": "none",
                            "network_policies": ["none"],
                        }
                    ],
                    max_concurrent=6,
                    pool_name="oldlab",
                    drain_state="draining",
                    drain_requested_at=now - timedelta(seconds=601),
                    registered_at=now,
                    last_seen_at=now,
                    status="active",
                )
            )
            await s.execute(
                insert(SlurmWorkerJob).values(
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
                    redacted_env=dict(_MATCHING_SLURM_RELEASE_ENV),
                    submitted_at=now - timedelta(seconds=900),
                    started_at=now - timedelta(seconds=800),
                )
            )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
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
                        # Non-exclusive Loom Slurm workers require the full containment contract.
                        "exclusive": False,
                        "container_cpus": 2.0,
                        "container_memory_mib": 4096,
                        "container_pids": 512,
                        "candidate_sha": "a" * 40,
                        "job_pids_max": 8192,
                        "repo_dir": "/opt/loom",
                        "requested_cpus": 12,
                        "requested_memory_mib": 58000,
                        "requested_concurrency": 6,
                        "max_jobs": 1,
                        "pending_job_cap": 1,
                        "time_limit": "7-00:00:00",
                    },
                    idle_since_at=now - timedelta(seconds=601),
                )
            )
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="oldlab"),
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


async def test_reconcile_cancels_unlinked_slurm_job_by_unique_hostname(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    worker_id = uuid4()
    try:
        async with session_factory() as s:
            await s.execute(
                insert(Worker).values(
                    id=worker_id,
                    hostname="oldlab-1",
                    version="test",
                    capabilities=[
                        {
                            "backend": "docker",
                            "os": "linux",
                            "cpu_arch": "x86_64",
                            "gpu_vendor": "none",
                            "network_policies": ["none"],
                        }
                    ],
                    max_concurrent=6,
                    pool_name="oldlab",
                    drain_state="draining",
                    drain_requested_at=now - timedelta(seconds=601),
                    registered_at=now,
                    last_seen_at=now,
                    status="active",
                )
            )
            await s.execute(
                insert(SlurmWorkerJob).values(
                    environment="production",
                    pool_name="oldlab",
                    nodelist="oldlab-1",
                    requested_cpus=12,
                    requested_memory_mib=58000,
                    requested_concurrency=6,
                    job_id="9001",
                    slurm_state="RUNNING",
                    state="running",
                    redacted_env=dict(_MATCHING_SLURM_RELEASE_ENV),
                    submitted_at=now - timedelta(seconds=900),
                    started_at=now - timedelta(seconds=800),
                )
            )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
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
                        # Non-exclusive Loom Slurm workers require the full containment contract.
                        "exclusive": False,
                        "container_cpus": 2.0,
                        "container_memory_mib": 4096,
                        "container_pids": 512,
                        "candidate_sha": "a" * 40,
                        "job_pids_max": 8192,
                        "repo_dir": "/opt/loom",
                        "requested_cpus": 12,
                        "requested_memory_mib": 58000,
                        "requested_concurrency": 6,
                        "max_jobs": 1,
                        "pending_job_cap": 1,
                        "time_limit": "7-00:00:00",
                    },
                    idle_since_at=now - timedelta(seconds=601),
                )
            )
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="oldlab"),
            )
            await s.commit()

        assert results[0].action == "release_drained"
        assert runner.cancelled_job_ids == ["9001"]

        async with session_factory() as s:
            worker = await s.get(Worker, worker_id)
            job = (await s.execute(select(SlurmWorkerJob))).scalar_one()

        assert worker is not None
        assert worker.drain_state == "drained"
        assert job.worker_id is None
        assert job.state == "cancelled"
        assert job.pending_reason == "cancelled after autoscaler drain"
    finally:
        await engine.dispose()


async def test_reconcile_drains_and_cancels_running_job_outside_allowed_nodes(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    worker_id = uuid4()
    try:
        async with session_factory() as s:
            await s.execute(
                insert(Worker).values(
                    id=worker_id,
                    hostname="trt-gb10-7",
                    version="test",
                    capabilities=[
                        {
                            "backend": "docker",
                            "os": "linux",
                            "cpu_arch": "arm64",
                            "gpu_vendor": "none",
                            "network_policies": ["none"],
                        }
                    ],
                    max_concurrent=10,
                    pool_name="gb10",
                    drain_state="active",
                    registered_at=now,
                    last_seen_at=now,
                    status="active",
                )
            )
            await s.execute(
                insert(SlurmWorkerJob).values(
                    environment="staging",
                    pool_name="gb10",
                    nodelist="trt-gb10-7",
                    requested_cpus=20,
                    requested_memory_mib=115000,
                    requested_concurrency=10,
                    job_id="gb10-job-7",
                    slurm_state="RUNNING",
                    state="running",
                    worker_id=worker_id,
                    redacted_env=dict(_MATCHING_SLURM_RELEASE_ENV),
                    submitted_at=now - timedelta(seconds=900),
                    started_at=now - timedelta(seconds=800),
                )
            )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
                    environment="staging",
                    pool_name="gb10",
                    actuator="slurm",
                    enabled=True,
                    min_slots=0,
                    max_slots=10,
                    scale_up_threshold_slots=1,
                    scale_down_idle_seconds=600,
                    scale_up_cooldown_seconds=60,
                    scale_down_cooldown_seconds=300,
                    drain_timeout_seconds=600,
                    actuator_config={
                        "backend": "docker",
                        "cpu_arch": "arm64",
                        "allowed_nodes": ["trt-gb10-1"],
                        "env_file": "/secure/.env.remote-worker",
                        # Non-exclusive Loom Slurm workers require the full containment contract.
                        "exclusive": False,
                        "container_cpus": 2.0,
                        "container_memory_mib": 4096,
                        "container_pids": 512,
                        "candidate_sha": "a" * 40,
                        "job_pids_max": 8192,
                        "repo_dir": "/opt/loom",
                        "requested_cpus": 20,
                        "requested_memory_mib": 115000,
                        "requested_concurrency": 10,
                        "max_jobs": 1,
                        "pending_job_cap": 1,
                        "time_limit": "2-00:00:00",
                    },
                )
            )
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            first = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="staging",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="gb10"),
            )
            await s.commit()

        assert first[0].action == "request_drain"
        assert first[0].reason == "release_state_drift"
        assert first[0].actual_slots == 0
        assert first[0].blocked_reason == "release_state_drift"
        assert runner.cancelled_job_ids == []

        async with session_factory() as s:
            worker = await s.get(Worker, worker_id)
            policy = (await s.execute(select(WorkerPoolAutoscalerPolicy))).scalar_one()

        assert worker is not None
        assert worker.drain_state == "draining"
        assert policy.last_actual_slots == 0

        async with session_factory() as s:
            second = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="staging",
                now=now + timedelta(seconds=1),
                slurm_runner=runner,
                global_execution_witness=_witness(now + timedelta(seconds=1), pool_id="gb10"),
            )
            await s.commit()

        assert second[0].action == "release_drained"
        assert second[0].reason == "release_state_drift"
        assert second[0].actual_slots == 0
        assert runner.cancelled_job_ids == ["gb10-job-7"]

        async with session_factory() as s:
            worker = await s.get(Worker, worker_id)
            job = (await s.execute(select(SlurmWorkerJob))).scalar_one()
            policy = (await s.execute(select(WorkerPoolAutoscalerPolicy))).scalar_one()

        assert worker is not None
        assert worker.drain_state == "drained"
        assert job.state == "cancelled"
        assert job.pending_reason == "cancelled after autoscaler drain"
        assert policy.last_blocked_reason is None
        assert policy.last_actual_slots == 0
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
            await s.execute(
                insert(Worker).values(
                    id=worker_id,
                    hostname="trt-gb10-1",
                    version="test",
                    capabilities=[
                        {
                            "backend": "docker",
                            "os": "linux",
                            "cpu_arch": "arm64",
                            "gpu_vendor": "none",
                            "network_policies": ["none"],
                        }
                    ],
                    max_concurrent=2,
                    pool_name="gb10",
                    drain_state="active",
                    registered_at=now,
                    last_seen_at=now,
                    status="active",
                )
            )
            await s.execute(
                insert(GB10WorkerPoolDesiredState).values(
                    environment="production",
                    pool_name="gb10",
                    image_tag="gb10-image",
                    max_concurrent=2,
                    env_config_version="gb10-env",
                    target_slots=2,
                    host_intents={"trt-gb10-1": "active"},
                    rollout_policy={},
                    env={},
                )
            )
            await s.execute(
                insert(GB10WorkerNodeStatus).values(
                    environment="production",
                    pool_name="gb10",
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
                )
            )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
                    environment="production",
                    pool_name="gb10",
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
                )
            )
            await s.commit()

        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                global_execution_witness=_witness(now, pool_id="gb10"),
            )
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
            await s.execute(
                insert(Worker).values(
                    id=worker_id,
                    hostname="trt-gb10-1",
                    version="test",
                    capabilities=[
                        {
                            "backend": "docker",
                            "os": "linux",
                            "cpu_arch": "arm64",
                            "gpu_vendor": "none",
                            "network_policies": ["none"],
                        }
                    ],
                    max_concurrent=2,
                    pool_name="gb10",
                    drain_state="active",
                    registered_at=now,
                    last_seen_at=now,
                    status="active",
                )
            )
            await s.execute(
                insert(GB10WorkerPoolDesiredState).values(
                    environment="production",
                    pool_name="gb10",
                    image_tag="gb10-image",
                    max_concurrent=2,
                    env_config_version="gb10-env",
                    target_slots=2,
                    host_intents={"trt-gb10-1": "active"},
                    rollout_policy={},
                    env={},
                )
            )
            await s.execute(
                insert(GB10WorkerNodeStatus).values(
                    environment="production",
                    pool_name="gb10",
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
                )
            )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
                    environment="production",
                    pool_name="gb10",
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
                )
            )
            await s.commit()

        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                global_execution_witness=_witness(now, pool_id="gb10"),
            )
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
                await s.execute(
                    insert(Trial).values(
                        id=uuid4(),
                        team_id=team_id,
                        task_id="task-a",
                        config={},
                        requires_caps={"backend": "docker", "cpu_arch": "arm64"},
                        state="queued",
                        idempotency_key=f"gb10-queued-{idx}",
                    )
                )
            await s.execute(
                insert(GB10WorkerPoolDesiredState).values(
                    environment="production",
                    pool_name="gb10",
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
                )
            )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
                    environment="production",
                    pool_name="gb10",
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
                )
            )
            await s.commit()

        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                global_execution_witness=_witness(now, pool_id="gb10"),
            )
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
            await s.execute(
                insert(Trial).values(
                    id=uuid4(),
                    team_id=team_id,
                    task_id="task-a",
                    config={},
                    requires_caps={"backend": "docker", "cpu_arch": "arm64"},
                    state="queued",
                    idempotency_key="gb10-select-one",
                )
            )
            await s.execute(
                insert(GB10WorkerPoolDesiredState).values(
                    environment="production",
                    pool_name="gb10",
                    image_tag="gb10-image",
                    max_concurrent=10,
                    env_config_version="gb10-env",
                    target_slots=0,
                    host_intents={hostname: "stopped" for hostname in hostnames},
                    rollout_policy={},
                    env={},
                )
            )
            for hostname in hostnames:
                await s.execute(
                    insert(GB10WorkerNodeStatus).values(
                        environment="production",
                        pool_name="gb10",
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
                    )
                )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
                    environment="production",
                    pool_name="gb10",
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
                )
            )
            await s.commit()

        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                global_execution_witness=_witness(now, pool_id="gb10"),
            )
            await s.commit()

        assert results[0].action == "scale_up"
        assert results[0].desired_slots == 1

        async with session_factory() as s:
            desired = (await s.execute(select(GB10WorkerPoolDesiredState))).scalar_one()
            nodes = (
                (
                    await s.execute(
                        select(GB10WorkerNodeStatus).order_by(GB10WorkerNodeStatus.hostname),
                    )
                )
                .scalars()
                .all()
            )

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
            await s.execute(
                insert(Trial).values(
                    id=uuid4(),
                    team_id=team_id,
                    task_id="task-a",
                    config={},
                    requires_caps={"backend": "docker", "cpu_arch": "x86_64"},
                    state="queued",
                    idempotency_key="actuator-failure",
                )
            )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
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
                        # Non-exclusive Loom Slurm workers require the full containment contract.
                        "exclusive": False,
                        "container_cpus": 2.0,
                        "container_memory_mib": 4096,
                        "container_pids": 512,
                        "candidate_sha": "a" * 40,
                        "job_pids_max": 8192,
                        "repo_dir": "/opt/loom",
                        "requested_cpus": 12,
                        "requested_memory_mib": 58000,
                        "requested_concurrency": 6,
                        "max_jobs": 1,
                        "pending_job_cap": 1,
                        "time_limit": "7-00:00:00",
                    },
                )
            )
            await s.commit()

        runner = FakeSlurmRunner()
        runner.fail_submit_nodes.add("oldlab-1")
        async with session_factory() as s:
            await reconcile_worker_pool_autoscaler_once(
                s,
                environment="production",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now, pool_id="oldlab"),
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
