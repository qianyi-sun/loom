"""Prod-pressure Slurm-aware drain (#892).

Covers the three collaborators of the Slurm prod-pressure drain that share the
``WorkerPoolAutoscalerPolicy.prod_pressure_state`` field:

* the CP request handler ``apply_prod_pressure_signal`` (records/clears intent),
* the external autoscaler actor ``reconcile_worker_pool_autoscaler_once``
  (the single writer that scancels jobs + flips ``Worker.drain_state``),
* the scheduler claim path ``claim_one`` (reads the intent to fence claims).
"""

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
    SlurmNodeResource,
    SlurmWorkerCommandRunner,
)
from loom_control_plane.global_execution_fence import (
    GlobalExecutionWitness,
    canonical_global_execution_witness_bytes,
)
from loom_control_plane.prod_pressure_control import (
    ProdPressureSignal,
    apply_prod_pressure_signal,
)
from loom_control_plane.scheduler.claim import claim_one
from loom_control_plane.slurm_worker_jobs import SlurmWorkerJobObservation
from loom_control_plane.worker_pool_autoscaler import (
    reconcile_worker_pool_autoscaler_once,
)

_SLURM_ACTUATOR_CONFIG = {
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
    # Non-exclusive Loom Slurm workers require the full containment contract:
    # positive per-container caps, an exact candidate SHA, and a job cgroup PID
    # ceiling (>= container_pids * concurrency = 512 * 6).
    "exclusive": False,
    "container_cpus": 2.0,
    "container_memory_mib": 4096,
    "container_pids": 512,
    "candidate_sha": "a" * 40,
    "job_pids_max": 3072,
}


def _witness(now: datetime) -> GlobalExecutionWitness:
    private_key = Ed25519PrivateKey.from_private_bytes(bytes([11]) * 32)
    public_key = private_key.public_key()
    payload: dict[str, object] = {
        "authority": "global-capacity-manager",
        "pool_id": "oldlab",
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
        self.fail_submit_nodes: set[str] = set()
        self.job_observations: list[SlurmWorkerJobObservation] | None = None
        self.queried_job_ids: list[tuple[str, ...]] = []
        self.node_resources: dict[str, SlurmNodeResource] = {}

    async def query_jobs(
        self,
        job_ids: tuple[str, ...],
    ) -> list[SlurmWorkerJobObservation]:
        self.queried_job_ids.append(job_ids)
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

    async def query_node_resources(
        self,
        nodes: tuple[str, ...],
    ) -> dict[str, SlurmNodeResource]:
        return {node: res for node, res in self.node_resources.items() if node in nodes}


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


def _slurm_policy_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "environment": "staging",
        "pool_name": "oldlab",
        "actuator": "slurm",
        "enabled": True,
        "min_slots": 0,
        "max_slots": 6,
        "scale_up_threshold_slots": 1,
        "scale_down_idle_seconds": 600,
        "scale_up_cooldown_seconds": 60,
        "scale_down_cooldown_seconds": 300,
        "drain_timeout_seconds": 600,
        "actuator_config": dict(_SLURM_ACTUATOR_CONFIG),
    }
    values.update(overrides)
    return values


# --------------------------------------------------------------------------
# Handler: apply_prod_pressure_signal on a Slurm pool records/clears intent
# --------------------------------------------------------------------------


async def test_handler_slurm_pool_records_drain_intent_without_gb10_state(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 7, 21, 12, 0, tzinfo=UTC)
    try:
        async with session_factory() as s:
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
                    **_slurm_policy_values(),
                )
            )
            await s.commit()

        async with session_factory() as s:
            # No GB10 desired state exists: the Slurm dispatch must NOT raise
            # "GB10 desired state must exist".
            result = await apply_prod_pressure_signal(
                s,
                environment="staging",
                pool_name="oldlab",
                signal=ProdPressureSignal(
                    prod_pending_count=3,
                    prod_active_count=1,
                    prod_capacity_shortfall=2,
                ),
                preemptible=True,
                grace_period_seconds=600,
                now=now,
            )
            await s.commit()

        assert result["action"] == "draining"
        assert result["actuator"] == "slurm"
        assert result["drain_intent_active"] is True
        assert result["new_staging_claims_allowed"] is False

        async with session_factory() as s:
            policy = (
                await s.execute(
                    select(WorkerPoolAutoscalerPolicy),
                )
            ).scalar_one()
        assert policy.prod_pressure_state is not None
        assert policy.prod_pressure_state["state"] == "draining"
    finally:
        await engine.dispose()


async def test_handler_slurm_pool_recovers_and_clears_intent(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 7, 21, 12, 0, tzinfo=UTC)
    try:
        async with session_factory() as s:
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
                    **_slurm_policy_values(),
                )
            )
            await s.commit()

        async with session_factory() as s:
            await apply_prod_pressure_signal(
                s,
                environment="staging",
                pool_name="oldlab",
                signal=ProdPressureSignal(1, 0, 1),
                preemptible=True,
                grace_period_seconds=600,
                now=now,
            )
            await s.commit()

        async with session_factory() as s:
            result = await apply_prod_pressure_signal(
                s,
                environment="staging",
                pool_name="oldlab",
                signal=ProdPressureSignal(0, 0, 0),
                preemptible=True,
                grace_period_seconds=600,
                now=now + timedelta(seconds=30),
            )
            await s.commit()

        assert result["action"] == "recovered"
        assert result["drain_intent_active"] is False
        assert result["new_staging_claims_allowed"] is True

        async with session_factory() as s:
            policy = (
                await s.execute(
                    select(WorkerPoolAutoscalerPolicy),
                )
            ).scalar_one()
        assert policy.prod_pressure_state is None
    finally:
        await engine.dispose()


async def test_handler_slurm_grace_progresses_from_wait_to_cancel_retryable(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    start = datetime(2026, 7, 21, 12, 0, tzinfo=UTC)
    try:
        async with session_factory() as s:
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
                    **_slurm_policy_values(),
                )
            )
            await s.commit()

        async with session_factory() as s:
            first = await apply_prod_pressure_signal(
                s,
                environment="staging",
                pool_name="oldlab",
                signal=ProdPressureSignal(1, 0, 1),
                preemptible=True,
                grace_period_seconds=600,
                now=start,
            )
            await s.commit()
        assert first["grace"]["action"] == "wait"

        async with session_factory() as s:
            policy = (
                await s.execute(
                    select(WorkerPoolAutoscalerPolicy),
                )
            ).scalar_one()
            started_at = policy.prod_pressure_state["started_at"]

        async with session_factory() as s:
            second = await apply_prod_pressure_signal(
                s,
                environment="staging",
                pool_name="oldlab",
                signal=ProdPressureSignal(1, 0, 1),
                preemptible=True,
                grace_period_seconds=600,
                now=start + timedelta(seconds=601),
            )
            await s.commit()
        assert second["grace"]["action"] == "cancel_retryable"

        async with session_factory() as s:
            policy = (
                await s.execute(
                    select(WorkerPoolAutoscalerPolicy),
                )
            ).scalar_one()
        # started_at is preserved across ticks (grace clock is not reset).
        assert policy.prod_pressure_state["started_at"] == started_at
        assert policy.prod_pressure_state["last_grace_action"] == "cancel_retryable"
    finally:
        await engine.dispose()


async def test_handler_gb10_pool_still_requires_desired_state(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 7, 21, 12, 0, tzinfo=UTC)
    try:
        async with session_factory() as s:
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
                    **_slurm_policy_values(
                        pool_name="gb10",
                        actuator="gb10",
                        actuator_config={"backend": "docker", "cpu_arch": "arm64"},
                    ),
                )
            )
            await s.commit()

        # GB10 dispatch is unchanged: without a desired state it raises.
        async with session_factory() as s:
            with pytest.raises(ValueError, match="GB10 desired state must exist"):
                await apply_prod_pressure_signal(
                    s,
                    environment="staging",
                    pool_name="gb10",
                    signal=ProdPressureSignal(1, 0, 1),
                    preemptible=True,
                    grace_period_seconds=600,
                    now=now,
                )
    finally:
        await engine.dispose()


# --------------------------------------------------------------------------
# Actor: reconcile_worker_pool_autoscaler_once consumes the drain intent
# --------------------------------------------------------------------------


def _running_job_observation(
    *, job_id: str, nodelist: str, worker_id: object, now: datetime
) -> SlurmWorkerJobObservation:
    return SlurmWorkerJobObservation(
        job_id=job_id,
        slurm_state="RUNNING",
        nodelist=nodelist,
        worker_id=worker_id,
        observed_at=now,
    )


async def _seed_running_slurm_worker(
    session_factory,
    *,
    now: datetime,
    prod_pressure_state: dict | None,
) -> tuple[object, str]:
    worker_id = uuid4()
    job_id = "9001"
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
                drain_state="active",
                registered_at=now,
                last_seen_at=now,
                status="active",
            )
        )
        await s.execute(
            insert(SlurmWorkerJob).values(
                environment="staging",
                pool_name="oldlab",
                nodelist="oldlab-1",
                requested_cpus=12,
                requested_memory_mib=58000,
                requested_concurrency=6,
                job_id=job_id,
                slurm_state="RUNNING",
                state="running",
                worker_id=worker_id,
                redacted_env={
                    "LOOM_REMOTE_WORKER_ENV_FILE": "/secure/.env.remote-worker",
                    "LOOM_REMOTE_WORKER_REPO_DIR": "/opt/loom",
                },
                submitted_at=now - timedelta(seconds=900),
                started_at=now - timedelta(seconds=800),
            )
        )
        await s.execute(
            insert(WorkerPoolAutoscalerPolicy).values(
                **_slurm_policy_values(prod_pressure_state=prod_pressure_state),
            )
        )
        await s.commit()
    return worker_id, job_id


async def test_actor_cancels_jobs_when_grace_is_cancel_retryable(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 7, 21, 12, 0, tzinfo=UTC)
    try:
        worker_id, job_id = await _seed_running_slurm_worker(
            session_factory,
            now=now,
            prod_pressure_state={
                "state": "draining",
                "last_grace_action": "cancel_retryable",
            },
        )

        runner = FakeSlurmRunner()
        runner.job_observations = [
            _running_job_observation(
                job_id=job_id,
                nodelist="oldlab-1",
                worker_id=worker_id,
                now=now,
            ),
        ]
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="staging",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now),
            )
            await s.commit()

        assert results[0].action == "prod_pressure_drain"
        assert results[0].desired_slots == 0
        assert runner.cancelled_job_ids == [job_id]
        assert runner.submitted_nodes == []

        async with session_factory() as s:
            worker = await s.get(Worker, worker_id)
            job = (await s.execute(select(SlurmWorkerJob))).scalar_one()
            policy = (
                await s.execute(
                    select(WorkerPoolAutoscalerPolicy),
                )
            ).scalar_one()

        assert worker is not None
        assert worker.drain_state == "drained"
        assert worker.drain_owner == "prod-pressure-controller"
        assert job.state == "cancelled"
        assert job.slurm_state == "CANCELLED"
        assert policy.last_decision == "prod_pressure_drain"
    finally:
        await engine.dispose()


async def test_actor_holds_without_cancel_when_grace_is_wait(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 7, 21, 12, 0, tzinfo=UTC)
    try:
        worker_id, job_id = await _seed_running_slurm_worker(
            session_factory,
            now=now,
            prod_pressure_state={
                "state": "draining",
                "last_grace_action": "wait",
            },
        )

        runner = FakeSlurmRunner()
        runner.job_observations = [
            _running_job_observation(
                job_id=job_id,
                nodelist="oldlab-1",
                worker_id=worker_id,
                now=now,
            ),
        ]
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="staging",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now),
            )
            await s.commit()

        assert results[0].action == "prod_pressure_hold"
        assert runner.cancelled_job_ids == []
        assert runner.submitted_nodes == []

        async with session_factory() as s:
            worker = await s.get(Worker, worker_id)
            job = (await s.execute(select(SlurmWorkerJob))).scalar_one()

        assert worker is not None
        assert worker.drain_state == "active"
        assert job.state == "running"
    finally:
        await engine.dispose()


async def test_actor_scales_normally_when_no_drain_intent(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 7, 21, 12, 0, tzinfo=UTC)
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
                        requires_caps={"backend": "docker", "cpu_arch": "x86_64"},
                        state="queued",
                        idempotency_key=f"no-intent-{idx}",
                    )
                )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
                    **_slurm_policy_values(prod_pressure_state=None),
                )
            )
            await s.commit()

        runner = FakeSlurmRunner()
        async with session_factory() as s:
            results = await reconcile_worker_pool_autoscaler_once(
                s,
                environment="staging",
                now=now,
                slurm_runner=runner,
                global_execution_witness=_witness(now),
            )
            await s.commit()

        assert results[0].action == "scale_up"
        assert results[0].action not in {"prod_pressure_drain", "prod_pressure_hold"}
        assert runner.submitted_nodes == ["oldlab-1"]
    finally:
        await engine.dispose()


# --------------------------------------------------------------------------
# Claim fence: claim_one skips a Slurm pool that is draining
# --------------------------------------------------------------------------


async def test_claim_is_fenced_while_slurm_pool_is_draining(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 7, 21, 12, 0, tzinfo=UTC)
    team_id = uuid4()
    worker_id = uuid4()
    try:
        async with session_factory() as s:
            await s.execute(insert(Team).values(id=team_id, name=f"a-{team_id}"))
            await s.execute(insert(TeamQuota).values(team_id=team_id))
            await s.execute(insert(Task).values(id="t", checksum="0" * 64, config={}))
            await s.execute(
                insert(Trial).values(
                    id=uuid4(),
                    team_id=team_id,
                    task_id="t",
                    config={},
                    requires_caps={
                        "os": "linux",
                        "gpu_vendor": "none",
                        "network_policies": ["public"],
                        "worker_pool": "oldlab",
                    },
                    state="queued",
                )
            )
            await s.execute(
                insert(Worker).values(
                    id=worker_id,
                    hostname="oldlab-1",
                    version="v",
                    capabilities=[
                        {
                            "os": "linux",
                            "gpu_vendor": "none",
                            "network_policies": ["public"],
                            "dynamic_network_policy": True,
                            "mounted_fs": True,
                            "resource_modes": ["auto"],
                        }
                    ],
                    pool_name="oldlab",
                    drain_state="active",
                    registered_at=now,
                    last_seen_at=now,
                    status="active",
                )
            )
            await s.execute(
                insert(WorkerPoolAutoscalerPolicy).values(
                    **_slurm_policy_values(
                        prod_pressure_state={"state": "draining"},
                    ),
                )
            )
            await s.commit()

        claim_kwargs = {
            "worker_id": worker_id,
            "worker_os": ["linux"],
            "worker_cpu_arches": ["x86_64"],
            "worker_gpu_vendors": ["none"],
            "worker_network_policies": ["public"],
        }

        # Fenced while the pool's policy is draining.
        async with session_factory() as s:
            fenced = await claim_one(s, **claim_kwargs)
            await s.commit()
        assert fenced is None

        # Clear the drain intent; the same claim now succeeds.
        async with session_factory() as s:
            policy = (
                await s.execute(
                    select(WorkerPoolAutoscalerPolicy),
                )
            ).scalar_one()
            policy.prod_pressure_state = None
            await s.commit()

        async with session_factory() as s:
            claimed = await claim_one(s, **claim_kwargs)
            await s.commit()
        assert claimed is not None
        assert claimed["task_id"] == "t"
    finally:
        await engine.dispose()
