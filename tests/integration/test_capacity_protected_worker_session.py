"""Protected staging workers project into the public scheduler exactly once."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from loom.auth import verify_step_jwt
from loom.db.schema import Task, Team, TeamQuota, Token, User
from loom.pipeline.keys import canonical_digest, canonical_document
from loom_capacity_agent.admission import (
    ExecutableDrainRequestV2,
    ExecutableReleaseRequestV2,
    ExecutableWorkerRegistrationV2,
    ExecutableWorkerWithdrawalRequestV2,
    PhysicalJobBindingV2,
)
from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_agent.executable_admission import ExecutableAdmissionStore
from loom_capacity_agent.store import import_executable_terminal_inventory_evidence
from loom_capacity_manager.contracts import ResourceVectorV1
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableBootstrapRegistrationV2,
    ExecutableInventoryRecordV2,
    ExecutableOwnershipMetadataV2,
    ExecutableTerminalInventoryEvidenceV2,
    ExecutionContextV2,
    SignedExecutableOwnershipProofV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings
from loom_control_plane.protected_worker_session import (
    ProtectedWorkerSessionRejected,
    ProtectedWorkerSessionStore,
)
from loom_control_plane.scheduler.crash_detector import (
    reclaim_expired_workers,
    reclaim_stale_running_trials,
)
from tests.integration.test_capacity_agent_executable_admission import (
    _assign_protected_attempt,
    _bootstrap,
    _initialize_manager_bound_admission_agent,
    _protect_bootstrap,
    _serializable_executor_session,
)

_CANDIDATE_SHA = "a" * 40
_HOSTNAME = "trt-eai-oldlab-3"
_JOB_ID = "12345"
_WORKER_CREDENTIAL = "protected-worker-credential"
_WORKER_ID = UUID("00000000-0000-4000-8000-000000000301")
_WORKER_INCARNATION = UUID("00000000-0000-4000-8000-000000000302")


@dataclass(frozen=True)
class _SeededProtectedWorker:
    registration: AgentRegistrationV1
    bootstrap: ExecutableBootstrapRegistrationV2
    worker: ExecutableWorkerRegistrationV2


@dataclass(frozen=True)
class _ProtectedAttemptSeed:
    team_id: UUID
    run_id: UUID
    stage_id: UUID
    attempt_id: UUID
    claim_id: UUID
    lease_token: str
    execution_spec_digest: str
    execution_authorization_digest: str


@dataclass(frozen=True)
class _ClaimedProtectedTrialSeed:
    app: FastAPI
    worker: _SeededProtectedWorker
    trial_id: UUID
    submit_token: str
    claim_headers: dict[str, str]
    claim_payload: dict[str, object]
    first_attempt: dict[str, object]


def _value(database: dict[str, object], key: str) -> str:
    value = database[key]
    assert isinstance(value, str)
    return value


@asynccontextmanager
async def _runtime_session(
    database: dict[str, object],
) -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(make_url(_value(database, "runtime_url")))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session, session.begin():
            yield session
    finally:
        await engine.dispose()


async def _seed_protected_worker(
    database: dict[str, object],
    *,
    environment_id: str = "staging",
    tier_id: str = "staging",
    resources: ResourceVectorV1 | None = None,
) -> _SeededProtectedWorker:
    template = _bootstrap(UUID(int=301), UUID(int=302))
    binding = template.binding.model_copy(
        update={
            "tier_id": tier_id,
            "candidate": CandidateBindingV2(
                algorithm="git-sha1",
                identity=_CANDIDATE_SHA,
                publication_sha256="b" * 64,
            ),
            "pool_id": "oldlab",
            "shape_instance_id": "staging-oldlab-node-3",
            "shape_id": "staging-oldlab-cpu",
            "concurrency_slots": 1,
            "resources": resources
            or ResourceVectorV1(
                slots=1,
                cpu_millicores=1000,
                memory_bytes=1024,
            ),
            "node_ids": (_HOSTNAME,),
        }
    )
    request = ExecutableBootstrapRegistrationV2(
        binding=binding,
        command_sequence=template.command_sequence,
        bootstrap_registration_epoch=template.bootstrap_registration_epoch,
        bootstrap_evidence_sha256=template.bootstrap_evidence_sha256,
    )
    registration, _configuration = await _initialize_manager_bound_admission_agent(
        database,
        binding=binding,
        reporter_incarnation=UUID(int=303),
        protected_admission_sha256="c" * 64,
        environment_id=environment_id,
    )
    bootstrap_capability = "single-use-staging-bootstrap-capability"
    protected = await _protect_bootstrap(
        database,
        registration,
        bootstrap_sha256=hashlib.sha256(bootstrap_capability.encode("ascii")).hexdigest(),
        request=request,
    )
    physical = PhysicalJobBindingV2(
        operation_id=UUID(int=304),
        binding=protected.binding,
        bootstrap_registration_epoch=protected.bootstrap_registration_epoch,
        slurm_job_id=_JOB_ID,
        ownership_evidence_sha256="d" * 64,
    )
    worker = ExecutableWorkerRegistrationV2(
        operation_id=UUID(int=305),
        binding=protected.binding,
        bootstrap_registration_epoch=protected.bootstrap_registration_epoch,
        protected_registration_epoch=protected.bootstrap_registration_epoch + 1,
        slurm_job_id=_JOB_ID,
        worker_id=_WORKER_ID,
        worker_incarnation=_WORKER_INCARNATION,
        worker_credential_sha256=hashlib.sha256(_WORKER_CREDENTIAL.encode("ascii")).hexdigest(),
    )
    async with _serializable_executor_session(database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(
            protected,
            bootstrap_sha256=hashlib.sha256(bootstrap_capability.encode("ascii")).hexdigest(),
        )
        await store.bind_slurm_job(physical)
        await store.register_worker(worker, bootstrap_capability=bootstrap_capability)
    return _SeededProtectedWorker(
        registration=registration,
        bootstrap=protected,
        worker=worker,
    )


def _seed_public_slurm_job(
    database: dict[str, object],
    *,
    requested_cpus: int | None = 1,
    requested_memory_mib: int | None = None,
    requested_pids: int | None = None,
    requested_gpu_tres: str | None = None,
    requested_gpus: int = 0,
    slurm_state: str = "RUNNING",
) -> None:
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO public.slurm_worker_jobs "
                    "(id, slurm_cluster_id, environment, pool_name, nodelist, "
                    "requested_cpus, requested_memory_mib, requested_pids, "
                    "requested_gpu_tres, requested_gpus, requested_concurrency, "
                    "sandbox_identity, "
                    "candidate_sha, compose_project, job_id, slurm_state, state) "
                    "VALUES (:id, 'oldlab', 'staging', 'oldlab', :hostname, "
                    ":requested_cpus, :requested_memory_mib, :requested_pids, "
                    ":requested_gpu_tres, :requested_gpus, 1, "
                    "'staging', :candidate_sha, :compose_project, :job_id, "
                    ":slurm_state, 'running')"
                ),
                {
                    "id": UUID(int=306),
                    "hostname": _HOSTNAME,
                    "requested_cpus": requested_cpus,
                    "requested_memory_mib": requested_memory_mib,
                    "requested_pids": requested_pids,
                    "requested_gpu_tres": requested_gpu_tres,
                    "requested_gpus": requested_gpus,
                    "candidate_sha": _CANDIDATE_SHA,
                    "compose_project": "loom-staging-protected-12345",
                    "job_id": _JOB_ID,
                    "slurm_state": slurm_state,
                },
            )
    finally:
        engine.dispose()


def _public_registration_payload(
    *,
    sandbox_identity: str = "staging",
) -> dict[str, object]:
    return {
        "hostname": _HOSTNAME,
        "version": "0.0.1",
        "capabilities": [
            {
                "os": "linux",
                "gpu_vendor": "none",
                "network_policies": ["public"],
                "dynamic_network_policy": False,
                "mounted_fs": False,
                "resource_modes": ["auto"],
            }
        ],
        "supported_work_kinds": ["trial"],
        "capability_snapshot_digest": "sha256:" + "e" * 64,
        "capability_snapshot_json": None,
        "slurm_gpu_allocation_evidence_json": None,
        "slurm_gpu_allocation_evidence_digest": None,
        "max_concurrent": 1,
        "pool_name": "oldlab",
        "input_cache_capacity_bytes": 0,
        "input_cache_reserved_bytes": 0,
        "input_cache_ready_bytes": 0,
        "sandbox_identity": sandbox_identity,
        "candidate_sha": _CANDIDATE_SHA,
        "slurm_job_id": _JOB_ID,
        "compose_project": "loom-staging-protected-12345",
    }


@pytest.mark.asyncio
async def test_protected_registration_projects_manager_owned_slurm_job(
    capacity_guard_database: dict[str, object],
) -> None:
    seeded = await _seed_protected_worker(capacity_guard_database)

    registered = await _project_worker(capacity_guard_database)

    assert registered["worker_id"] == str(seeded.worker.worker_id)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            job = (
                connection.execute(
                    text(
                        "SELECT id, slurm_cluster_id, environment, pool_name, nodelist, "
                        "requested_cpus, requested_memory_mib, requested_pids, "
                        "requested_gpu_tres, requested_gpus, requested_concurrency, "
                        "sandbox_identity, candidate_sha, "
                        "compose_project, job_id, state, worker_id "
                        "FROM public.slurm_worker_jobs"
                    )
                )
                .mappings()
                .one()
            )
    finally:
        engine.dispose()
    assert dict(job) == {
        "id": seeded.worker.binding.intent_id,
        "slurm_cluster_id": "oldlab",
        "environment": "staging",
        "pool_name": "oldlab",
        "nodelist": _HOSTNAME,
        "requested_cpus": 1,
        "requested_memory_mib": None,
        "requested_pids": None,
        "requested_gpu_tres": None,
        "requested_gpus": 0,
        "requested_concurrency": 1,
        "sandbox_identity": "staging",
        "candidate_sha": _CANDIDATE_SHA,
        "compose_project": "loom-staging-protected-12345",
        "job_id": _JOB_ID,
        "state": "running",
        "worker_id": seeded.worker.worker_id,
    }


@pytest.mark.asyncio
async def test_protected_registration_accepts_personal_development_binding(
    capacity_guard_database: dict[str, object],
) -> None:
    seeded = await _seed_protected_worker(
        capacity_guard_database,
        environment_id="dev-alice",
        tier_id="development",
    )

    registered = await _project_worker(
        capacity_guard_database,
        payload=_public_registration_payload(sandbox_identity="loom-dev-alice"),
    )

    assert registered["worker_id"] == str(seeded.worker.worker_id)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            job = (
                connection.execute(
                    text(
                        "SELECT environment, sandbox_identity, worker_id "
                        "FROM public.slurm_worker_jobs"
                    )
                )
                .mappings()
                .one()
            )
    finally:
        engine.dispose()
    assert dict(job) == {
        "environment": "loom-dev-alice",
        "sandbox_identity": "loom-dev-alice",
        "worker_id": seeded.worker.worker_id,
    }


@pytest.mark.asyncio
async def test_protected_registration_projects_exact_worker_and_replays(
    capacity_guard_database: dict[str, object],
) -> None:
    seeded = await _seed_protected_worker(capacity_guard_database)
    _seed_public_slurm_job(capacity_guard_database)
    payload = _public_registration_payload()

    async with _runtime_session(capacity_guard_database) as session:
        first = (
            await session.execute(
                text(
                    "SELECT loom_capacity_guard.register_staging_public_worker("
                    ":credential, CAST(:payload AS jsonb))"
                ),
                {
                    "credential": _WORKER_CREDENTIAL,
                    "payload": json.dumps(payload, sort_keys=True),
                },
            )
        ).scalar_one()
        replay = (
            await session.execute(
                text(
                    "SELECT loom_capacity_guard.register_staging_public_worker("
                    ":credential, CAST(:payload AS jsonb))"
                ),
                {
                    "credential": _WORKER_CREDENTIAL,
                    "payload": json.dumps(payload, sort_keys=True),
                },
            )
        ).scalar_one()
        asserted = (
            await session.execute(
                text(
                    "SELECT loom_capacity_guard.assert_staging_worker_session("
                    ":worker_id, :credential)"
                ),
                {
                    "worker_id": seeded.worker.worker_id,
                    "credential": _WORKER_CREDENTIAL,
                },
            )
        ).scalar_one()

    assert replay == first
    assert asserted["worker_id"] == str(seeded.worker.worker_id)
    assert asserted["worker_incarnation"] == str(seeded.worker.worker_incarnation)
    assert first["worker_id"] == str(seeded.worker.worker_id)
    assert first["worker_incarnation"] == str(seeded.worker.worker_incarnation)

    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            projection = (
                connection.execute(
                    text(
                        "SELECT w.id, w.hostname, w.pool_name, w.max_concurrent, "
                        "encode(w.auth_token_hash, 'hex') AS auth_sha256, j.worker_id "
                        "FROM public.workers AS w "
                        "JOIN public.slurm_worker_jobs AS j ON j.worker_id = w.id"
                    )
                )
                .mappings()
                .one()
            )
            assert dict(projection) == {
                "id": seeded.worker.worker_id,
                "hostname": _HOSTNAME,
                "pool_name": "oldlab",
                "max_concurrent": 1,
                "auth_sha256": seeded.worker.worker_credential_sha256,
                "worker_id": seeded.worker.worker_id,
            }
            assert connection.execute(text("SELECT count(*) FROM public.workers")).scalar_one() == 1
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_protected_registration_rejects_legacy_slurm_resource_drift(
    capacity_guard_database: dict[str, object],
) -> None:
    await _seed_protected_worker(capacity_guard_database)
    _seed_public_slurm_job(capacity_guard_database, requested_cpus=2)

    with pytest.raises(DBAPIError, match="no exact active Slurm job"):
        await _project_worker(capacity_guard_database)


@pytest.mark.asyncio
async def test_protected_registration_replay_rejects_projected_slurm_resource_drift(
    capacity_guard_database: dict[str, object],
) -> None:
    await _seed_protected_worker(capacity_guard_database)
    await _project_worker(capacity_guard_database)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE public.slurm_worker_jobs SET requested_cpus = 2"),
            )
    finally:
        engine.dispose()

    with pytest.raises(DBAPIError, match="no exact active Slurm job"):
        await _project_worker(capacity_guard_database)


@pytest.mark.asyncio
async def test_protected_cpu_registration_rejects_gpu_allocation_evidence(
    capacity_guard_database: dict[str, object],
) -> None:
    await _seed_protected_worker(capacity_guard_database)
    payload = _public_registration_payload()
    evidence = _oldlab_gpu_evidence()
    payload["slurm_gpu_allocation_evidence_json"] = evidence
    payload["slurm_gpu_allocation_evidence_digest"] = canonical_digest(evidence)

    with pytest.raises(DBAPIError, match="projection differs from protected binding"):
        await _project_worker(capacity_guard_database, payload=payload)


@pytest.mark.asyncio
async def test_protected_gpu_registration_projects_exact_slurm_resources(
    capacity_guard_database: dict[str, object],
) -> None:
    seeded = await _seed_protected_worker(
        capacity_guard_database,
        resources=ResourceVectorV1(
            slots=1,
            cpu_millicores=2000,
            memory_bytes=2 * 1024 * 1024,
            gpu_count=2,
        ),
    )
    payload = _public_registration_payload()
    evidence = _oldlab_gpu_evidence()
    payload["slurm_gpu_allocation_evidence_json"] = evidence
    payload["slurm_gpu_allocation_evidence_digest"] = canonical_digest(evidence)

    await _project_worker(capacity_guard_database, payload=payload)
    await _project_worker(capacity_guard_database, payload=payload)

    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            job = (
                connection.execute(
                    text(
                        "SELECT id, requested_cpus, requested_memory_mib, requested_pids, "
                        "requested_gpu_tres, requested_gpus, requested_concurrency, worker_id "
                        "FROM public.slurm_worker_jobs"
                    )
                )
                .mappings()
                .one()
            )
    finally:
        engine.dispose()
    assert dict(job) == {
        "id": seeded.worker.binding.intent_id,
        "requested_cpus": 2,
        "requested_memory_mib": 2,
        "requested_pids": None,
        "requested_gpu_tres": "gpu:rtx5080:2",
        "requested_gpus": 2,
        "requested_concurrency": 1,
        "worker_id": seeded.worker.worker_id,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("partition", "gb10"),
        ("gpu_tres", "gpu:gb10:1"),
        ("variant_id", "gb10-shared-1gpu"),
    ),
)
@pytest.mark.asyncio
async def test_protected_gpu_registration_rejects_allocation_contract_drift(
    capacity_guard_database: dict[str, object],
    field: str,
    value: str,
) -> None:
    await _seed_protected_worker(
        capacity_guard_database,
        resources=ResourceVectorV1(
            slots=1,
            cpu_millicores=2000,
            memory_bytes=2 * 1024 * 1024,
            gpu_count=2,
        ),
    )
    payload = _public_registration_payload()
    evidence = _oldlab_gpu_evidence()
    evidence[field] = value
    payload["slurm_gpu_allocation_evidence_json"] = evidence
    payload["slurm_gpu_allocation_evidence_digest"] = canonical_digest(evidence)

    with pytest.raises(DBAPIError, match="projection differs from protected binding"):
        await _project_worker(capacity_guard_database, payload=payload)


async def _project_worker(
    database: dict[str, object],
    *,
    credential: str = _WORKER_CREDENTIAL,
    payload: dict[str, object] | None = None,
) -> dict[str, object]:
    async with _runtime_session(database) as session:
        returned = (
            await session.execute(
                text(
                    "SELECT loom_capacity_guard.register_staging_public_worker("
                    ":credential, CAST(:payload AS jsonb))"
                ),
                {
                    "credential": credential,
                    "payload": json.dumps(
                        payload or _public_registration_payload(),
                        sort_keys=True,
                    ),
                },
            )
        ).scalar_one()
    assert isinstance(returned, dict)
    return returned


@pytest.mark.asyncio
async def test_protected_registration_rejects_wrong_credential(
    capacity_guard_database: dict[str, object],
) -> None:
    await _seed_protected_worker(capacity_guard_database)
    _seed_public_slurm_job(capacity_guard_database)

    with pytest.raises(DBAPIError, match="credential is not current"):
        await _project_worker(
            capacity_guard_database,
            credential="wrong-protected-worker-credential",
        )


@pytest.mark.asyncio
async def test_withdrawn_unregistered_worker_cannot_project_public_session(
    capacity_guard_database: dict[str, object],
) -> None:
    template = _bootstrap(UUID(int=321), UUID(int=322))
    binding = template.binding.model_copy(
        update={
            "tier_id": "staging",
            "candidate": CandidateBindingV2(
                algorithm="git-sha1",
                identity=_CANDIDATE_SHA,
                publication_sha256="b" * 64,
            ),
            "pool_id": "oldlab",
            "shape_instance_id": "staging-oldlab-node-3",
            "shape_id": "staging-oldlab-cpu",
            "concurrency_slots": 1,
            "resources": ResourceVectorV1(
                slots=1,
                cpu_millicores=1000,
                memory_bytes=1024,
            ),
            "node_ids": (_HOSTNAME,),
        }
    )
    request = ExecutableBootstrapRegistrationV2(
        binding=binding,
        command_sequence=template.command_sequence,
        bootstrap_registration_epoch=template.bootstrap_registration_epoch,
        bootstrap_evidence_sha256=template.bootstrap_evidence_sha256,
    )
    registration, _configuration = await _initialize_manager_bound_admission_agent(
        capacity_guard_database,
        binding=binding,
        reporter_incarnation=UUID(int=323),
        protected_admission_sha256="c" * 64,
        environment_id="staging",
    )
    bootstrap_capability = "withdrawn-single-use-staging-bootstrap-capability"
    protected = await _protect_bootstrap(
        capacity_guard_database,
        registration,
        bootstrap_sha256=hashlib.sha256(bootstrap_capability.encode("ascii")).hexdigest(),
        request=request,
    )
    physical = PhysicalJobBindingV2(
        operation_id=UUID(int=324),
        binding=protected.binding,
        bootstrap_registration_epoch=protected.bootstrap_registration_epoch,
        slurm_job_id=_JOB_ID,
        ownership_evidence_sha256="d" * 64,
    )
    withdrawal = ExecutableWorkerWithdrawalRequestV2(
        operation_id=UUID(int=325),
        binding=protected.binding,
        bootstrap_registration_epoch=protected.bootstrap_registration_epoch,
        protected_registration_epoch=protected.bootstrap_registration_epoch + 1,
        slurm_job_id=_JOB_ID,
        ownership_evidence_sha256=physical.ownership_evidence_sha256,
        expected_claim_high_water=0,
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(session, registration=registration)
        await store.prepare_worker(
            protected,
            bootstrap_sha256=hashlib.sha256(bootstrap_capability.encode("ascii")).hexdigest(),
        )
        await store.bind_slurm_job(physical)
        await store.withdraw_unregistered_worker(withdrawal)
    _seed_public_slurm_job(capacity_guard_database)

    with pytest.raises(DBAPIError, match="credential is not current"):
        await _project_worker(capacity_guard_database)

    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert connection.execute(text("SELECT count(*) FROM public.workers")).scalar_one() == 0
            assert (
                connection.execute(
                    text("SELECT worker_id FROM public.slurm_worker_jobs WHERE job_id = :job_id"),
                    {"job_id": _JOB_ID},
                ).scalar_one()
                is None
            )
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("candidate_sha", "f" * 40),
        ("pool_name", "gb10"),
        ("hostname", "trt-eai-oldlab-4"),
        ("max_concurrent", 2),
    ),
)
async def test_protected_registration_rejects_public_binding_drift(
    capacity_guard_database: dict[str, object],
    field: str,
    value: object,
) -> None:
    await _seed_protected_worker(capacity_guard_database)
    _seed_public_slurm_job(capacity_guard_database)
    payload = _public_registration_payload()
    payload[field] = value

    with pytest.raises(DBAPIError, match="differs from protected binding"):
        await _project_worker(capacity_guard_database, payload=payload)


@pytest.mark.asyncio
async def test_protected_registration_rejects_conflicting_public_replay(
    capacity_guard_database: dict[str, object],
) -> None:
    await _seed_protected_worker(capacity_guard_database)
    _seed_public_slurm_job(capacity_guard_database)
    await _project_worker(capacity_guard_database)
    conflict = _public_registration_payload()
    conflict["version"] = "conflicting-version"

    with pytest.raises(DBAPIError, match="conflicting staging worker projection replay"):
        await _project_worker(capacity_guard_database, payload=conflict)


@pytest.mark.asyncio
async def test_successor_incarnation_revokes_predecessor_public_session(
    capacity_guard_database: dict[str, object],
) -> None:
    seeded = await _seed_protected_worker(capacity_guard_database)
    _seed_public_slurm_job(capacity_guard_database)
    await _project_worker(capacity_guard_database)
    successor_credential = "protected-worker-credential-successor"
    successor = seeded.worker.model_copy(
        update={
            "operation_id": UUID(int=307),
            "protected_registration_epoch": (seeded.worker.protected_registration_epoch + 1),
            "worker_id": UUID(int=308),
            "worker_incarnation": UUID(int=309),
            "worker_credential_sha256": hashlib.sha256(
                successor_credential.encode("ascii")
            ).hexdigest(),
            "predecessor_worker_incarnation": seeded.worker.worker_incarnation,
        }
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(
            session,
            registration=seeded.registration,
        )
        await store.register_worker(
            successor,
            predecessor_worker_credential=_WORKER_CREDENTIAL,
        )

    with pytest.raises(DBAPIError, match="session is not current"):
        async with _runtime_session(capacity_guard_database) as session:
            await session.execute(
                text(
                    "SELECT loom_capacity_guard.assert_staging_worker_session("
                    ":worker_id, :credential)"
                ),
                {
                    "worker_id": seeded.worker.worker_id,
                    "credential": _WORKER_CREDENTIAL,
                },
            )

    projected = await _project_worker(
        capacity_guard_database,
        credential=successor_credential,
    )
    assert projected["worker_id"] == str(successor.worker_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_state", ("draining", "released"))
async def test_protected_lifecycle_terminal_state_revokes_public_session(
    capacity_guard_database: dict[str, object],
    terminal_state: str,
) -> None:
    seeded = await _seed_protected_worker(capacity_guard_database)
    _seed_public_slurm_job(capacity_guard_database)
    await _project_worker(capacity_guard_database)
    drain = ExecutableDrainRequestV2(
        operation_id=UUID(int=310),
        binding=seeded.worker.binding,
        worker_id=seeded.worker.worker_id,
        worker_incarnation=seeded.worker.worker_incarnation,
        expected_claim_high_water=0,
        drain_epoch=1,
    )
    async with _serializable_executor_session(capacity_guard_database) as session:
        store = ExecutableAdmissionStore(
            session,
            registration=seeded.registration,
        )
        await store.begin_drain(drain)
        if terminal_state == "released":
            await store.acknowledge_release(
                ExecutableReleaseRequestV2(
                    operation_id=UUID(int=311),
                    binding=seeded.worker.binding,
                    reporter_incarnation=seeded.registration.reporter_incarnation,
                    bootstrap_registration_epoch=(seeded.worker.bootstrap_registration_epoch),
                    expected_claim_high_water=0,
                    protected_registration_epoch=(seeded.worker.protected_registration_epoch),
                    release_epoch=1,
                ),
                current_worker_credential=_WORKER_CREDENTIAL,
            )

    with pytest.raises(DBAPIError, match="session is not current"):
        async with _runtime_session(capacity_guard_database) as session:
            await session.execute(
                text(
                    "SELECT loom_capacity_guard.assert_staging_worker_session("
                    ":worker_id, :credential)"
                ),
                {
                    "worker_id": seeded.worker.worker_id,
                    "credential": _WORKER_CREDENTIAL,
                },
            )


def test_guard_0023_refuses_downgrade_with_protected_public_projection(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asyncio.run(_seed_protected_worker(capacity_guard_database))
    _seed_public_slurm_job(capacity_guard_database)
    asyncio.run(_project_worker(capacity_guard_database))
    root = Path(__file__).resolve().parents[2]
    config = AlembicConfig(str(root / "capacity_guard_migrations" / "alembic.ini"))
    config.set_main_option("script_location", str(root / "capacity_guard_migrations"))
    for name, key in (
        ("LOOM_CAPACITY_GUARD_DB_URL", "migrator_url"),
        ("LOOM_CAPACITY_GUARD_OWNER_ROLE", "owner_role"),
        ("LOOM_CAPACITY_GUARD_AGENT_ROLE", "agent_role"),
        ("LOOM_CAPACITY_GUARD_EXECUTOR_ROLE", "executor_role"),
        ("LOOM_CAPACITY_GUARD_OBSERVER_ROLE", "observer_role"),
        ("LOOM_CAPACITY_GUARD_RUNTIME_ROLE", "runtime_role"),
    ):
        monkeypatch.setenv(name, _value(capacity_guard_database, key))

    with pytest.raises(
        RuntimeError,
        match="cannot downgrade guard_0023 while protected staging worker projections exist",
    ):
        command.downgrade(config, "guard_0022")

    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0029"
            )
            assert (
                connection.execute(
                    text("SELECT count(*) FROM public.workers WHERE id = :worker_id"),
                    {"worker_id": _WORKER_ID},
                ).scalar_one()
                == 1
            )
    finally:
        engine.dispose()


def test_staging_worker_functions_are_runtime_only(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    signatures = (
        "loom_capacity_guard.register_staging_public_worker(text,jsonb)",
        "loom_capacity_guard.assert_staging_worker_session(uuid,text)",
    )
    try:
        with engine.connect() as connection:
            for signature in signatures:
                privileges = {
                    role: connection.execute(
                        text("SELECT has_function_privilege(:role, :signature, 'EXECUTE')"),
                        {"role": _value(capacity_guard_database, role), "signature": signature},
                    ).scalar_one()
                    for role in (
                        "runtime_role",
                        "agent_role",
                        "executor_role",
                        "observer_role",
                    )
                }
                privileges["public"] = connection.execute(
                    text("SELECT has_function_privilege('public', :signature, 'EXECUTE')"),
                    {"signature": signature},
                ).scalar_one()
                assert privileges == {
                    "runtime_role": True,
                    "agent_role": False,
                    "executor_role": False,
                    "observer_role": False,
                    "public": False,
                }
            assert (
                connection.execute(
                    text(
                        "SELECT has_table_privilege("
                        ":runtime, "
                        "'loom_capacity_guard.executable_admission_events', "
                        "'SELECT')"
                    ),
                    {"runtime": _value(capacity_guard_database, "runtime_role")},
                ).scalar_one()
                is False
            )
    finally:
        engine.dispose()


def _runtime_store(
    database: dict[str, object],
) -> tuple[ProtectedWorkerSessionStore, AsyncEngine]:
    engine = create_async_engine(make_url(_value(database, "runtime_url")))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    return ProtectedWorkerSessionStore(factory), engine


@pytest.mark.asyncio
async def test_protected_worker_session_store_registers_through_runtime_role(
    capacity_guard_database: dict[str, object],
) -> None:
    seeded = await _seed_protected_worker(capacity_guard_database)
    _seed_public_slurm_job(capacity_guard_database)
    store, engine = _runtime_store(capacity_guard_database)
    try:
        registered = await store.register(
            worker_credential=_WORKER_CREDENTIAL,
            projection=_public_registration_payload(),
        )
    finally:
        await engine.dispose()

    assert registered.worker_id == seeded.worker.worker_id
    assert registered.worker_incarnation == seeded.worker.worker_incarnation
    assert registered.capability_snapshot_digest == "sha256:" + "e" * 64
    assert registered.supported_work_kinds == ("trial",)


@pytest.mark.asyncio
async def test_protected_worker_session_store_rejects_wrong_credential(
    capacity_guard_database: dict[str, object],
) -> None:
    seeded = await _seed_protected_worker(capacity_guard_database)
    _seed_public_slurm_job(capacity_guard_database)
    await _project_worker(capacity_guard_database)
    store, engine = _runtime_store(capacity_guard_database)
    try:
        with pytest.raises(ProtectedWorkerSessionRejected):
            async with store.assert_session(
                worker_id=seeded.worker.worker_id,
                worker_credential="wrong-protected-worker-credential",
            ):
                pytest.fail("rejected protected session yielded control")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_protected_worker_session_store_holds_lifecycle_lock_until_mutation_exits(
    capacity_guard_database: dict[str, object],
) -> None:
    seeded = await _seed_protected_worker(capacity_guard_database)
    _seed_public_slurm_job(capacity_guard_database)
    await _project_worker(capacity_guard_database)
    store, engine = _runtime_store(capacity_guard_database)
    drain_started = asyncio.Event()

    async def begin_drain() -> None:
        drain = ExecutableDrainRequestV2(
            operation_id=UUID(int=320),
            binding=seeded.worker.binding,
            worker_id=seeded.worker.worker_id,
            worker_incarnation=seeded.worker.worker_incarnation,
            expected_claim_high_water=0,
            drain_epoch=1,
        )
        async with _serializable_executor_session(capacity_guard_database) as session:
            drain_started.set()
            await ExecutableAdmissionStore(
                session,
                registration=seeded.registration,
            ).begin_drain(drain)

    drain_task: asyncio.Task[None] | None = None
    try:
        async with store.assert_session(
            worker_id=seeded.worker.worker_id,
            worker_credential=_WORKER_CREDENTIAL,
        ) as identity:
            assert identity.worker_id == seeded.worker.worker_id
            assert identity.credential_digest == bytes.fromhex(
                seeded.worker.worker_credential_sha256
            )
            drain_task = asyncio.create_task(begin_drain())
            await drain_started.wait()
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(drain_task), timeout=0.1)
        assert drain_task is not None
        await asyncio.wait_for(drain_task, timeout=5)
    finally:
        if drain_task is not None and not drain_task.done():
            drain_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await drain_task
        await engine.dispose()


def _seed_worker_bearer(database: dict[str, object]) -> str:
    raw = "ordinary-scoped-worker-bearer"
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.begin() as connection:
            connection.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(raw.encode("ascii")).digest(),
                    type="worker",
                    scopes=["worker:report", "worker:claim"],
                    team_id=None,
                    issued_at=datetime.now(UTC),
                    expires_at=None,
                )
            )
    finally:
        engine.dispose()
    return raw


def _seed_bearer(
    database: dict[str, object],
    *,
    raw: str,
    token_type: str,
    scopes: list[str],
) -> str:
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.begin() as connection:
            connection.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(raw.encode("ascii")).digest(),
                    type=token_type,
                    scopes=scopes,
                    team_id=None,
                    issued_at=datetime.now(UTC),
                    expires_at=None,
                )
            )
    finally:
        engine.dispose()
    return raw


def _protected_cp_app(
    database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    configure_runtime: bool = True,
):
    values = {
        "LOOM_CP_DB_URL": _value(database, "admin_url"),
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }
    if configure_runtime:
        runtime_url_file = tmp_path / "protected-worker-runtime-database-url"
        runtime_url_file.write_text(
            _value(database, "runtime_url") + "\n",
            encoding="ascii",
        )
        runtime_url_file.chmod(0o600)
        values["LOOM_CP_PROTECTED_WORKER_RUNTIME_DB_URL_FILE"] = str(runtime_url_file)

    async def idle_background_loop(**_kwargs: object) -> None:
        await asyncio.Event().wait()

    for name in (
        "run_crash_detector_loop",
        "run_live_preview_reconciler_loop",
        "run_metrics_refresher_loop",
        "run_retry_exhausted_sweeper_loop",
        "run_service_execution_materializer_loop",
        "run_worker_pool_autoscaler_loop",
    ):
        monkeypatch.setattr(
            f"loom_control_plane.app.{name}",
            idle_background_loop,
            raising=False,
        )
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    return create_app(ControlPlaneSettings(_env_file=None))


def _worker_registration_request() -> dict[str, object]:
    return {
        "hostname": _HOSTNAME,
        "version": "0.0.1",
        "capabilities": [
            {
                "os": "linux",
                "gpu_vendor": "none",
                "network_policies": ["public"],
                "dynamic_network_policy": False,
                "mounted_fs": False,
                "resource_modes": ["auto"],
            }
        ],
        "supported_work_kinds": ["trial"],
        "max_concurrent": 1,
        "pool_name": "oldlab",
        "input_cache_capacity_bytes": 0,
        "input_cache_reserved_bytes": 0,
        "input_cache_ready_bytes": 0,
        "sandbox_identity": "staging",
        "candidate_sha": _CANDIDATE_SHA,
        "slurm_job_id": _JOB_ID,
        "compose_project": "loom-staging-protected-12345",
    }


def _oldlab_gpu_worker_registration_request() -> dict[str, object]:
    evidence = _oldlab_gpu_evidence()
    return {
        **_worker_registration_request(),
        "capabilities": [
            {
                "os": "linux",
                "cpu_arch": "x86_64",
                "gpu_vendor": "nvidia",
                "network_policies": ["allowlist"],
                "dynamic_network_policy": False,
                "mounted_fs": False,
                "resource_modes": ["auto"],
            }
        ],
        "supported_work_kinds": ["trial", "execution_attempt"],
        "pool_name": "behavior-gpu-oldlab",
        "capability_snapshot": {
            "schema_version": "loom.worker-capabilities.v1",
            "cpu_arch": "x86_64",
            "cpu_cores": 2,
            "memory_bytes": 2 * 1024 * 1024,
            "scratch_bytes": 1024 * 1024,
            "network_profiles": ["gateway"],
            "container_runtime_features": ["nvidia-container-runtime"],
            "gpu_devices": [
                {
                    "allocation_id": f"oldlab:{_JOB_ID}",
                    "device_uuid": "GPU-a",
                    "vendor": "nvidia",
                    "model": "NVIDIA GeForce RTX 5080",
                    "memory_kind": "dedicated",
                    "memory_mb": 16_384,
                    "unified_memory_mb": None,
                    "nvidia_driver_version": "580.12.0",
                    "mig_mode": "disabled",
                },
                {
                    "allocation_id": f"oldlab:{_JOB_ID}",
                    "device_uuid": "GPU-b",
                    "vendor": "nvidia",
                    "model": "NVIDIA GeForce RTX 5080",
                    "memory_kind": "dedicated",
                    "memory_mb": 16_384,
                    "unified_memory_mb": None,
                    "nvidia_driver_version": "580.12.0",
                    "mig_mode": "disabled",
                },
            ],
            "input_cache_capacity_bytes": 0,
            "input_cache_reserved_bytes": 0,
            "input_cache_ready_bytes": 0,
        },
        "slurm_gpu_allocation_evidence": evidence,
    }


def _oldlab_gpu_evidence() -> dict[str, object]:
    return {
        "allocation_id": f"oldlab:{_JOB_ID}",
        "slurm_cluster_id": "oldlab",
        "job_id": _JOB_ID,
        "node_name": _HOSTNAME,
        "partition": "all",
        "gpu_tres": "gpu:rtx5080:2",
        "allocated_device_ids": [0, 1],
        "device_uuids": ["GPU-a", "GPU-b"],
        "variant_id": "oldlab-rtx5080-2gpu",
    }


def _seed_protected_execution_attempt(
    database: dict[str, object],
) -> _ProtectedAttemptSeed:
    team_id = uuid4()
    run_id = uuid4()
    stage_id = uuid4()
    attempt_id = uuid4()
    claim_id = uuid4()
    lease_token = "protected-lease-" + uuid4().hex
    binding_digest = "sha256:" + "7" * 64
    execution_spec = {
        "container_node": {
            "network_profile": "gateway",
            "timeout_seconds": 300,
        },
        "control_binding_snapshots": [{"snapshot_sha256": binding_digest}],
    }
    execution_spec_digest = canonical_digest(execution_spec)
    execution_authorization = {"schema_version": "protected-test-authorization.v1"}
    execution_authorization_digest = canonical_digest(execution_authorization)
    seed = _ProtectedAttemptSeed(
        team_id=team_id,
        run_id=run_id,
        stage_id=stage_id,
        attempt_id=attempt_id,
        claim_id=claim_id,
        lease_token=lease_token,
        execution_spec_digest=execution_spec_digest,
        execution_authorization_digest=execution_authorization_digest,
    )
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams(id,name) VALUES (:id,:name)"),
                {"id": team_id, "name": f"protected-attempt-{team_id}"},
            )
            connection.execute(
                text("INSERT INTO team_quotas(team_id,in_flight_count) VALUES (:id,0)"),
                {"id": team_id},
            )
            connection.execute(
                text(
                    "INSERT INTO pipeline_runs ("
                    "id,team_id,submission_policy,recipe_name,recipe_version,recipe_digest,"
                    "graph_spec_json,graph_spec_digest,parameters_json,parameters_digest,"
                    "resolved_inputs_json,budget_json,request_digest,idempotency_key,state,"
                    "started_at) VALUES ("
                    ":id,:team,'ordinary','protected-test',1,:digest,'{}'::jsonb,:digest,"
                    "'{}'::jsonb,:digest,'[]'::jsonb,'{}'::jsonb,:digest,:key,'running',now())"
                ),
                {
                    "id": run_id,
                    "team": team_id,
                    "digest": "sha256:" + "8" * 64,
                    "key": f"protected-attempt-{run_id}",
                },
            )
            connection.execute(
                text(
                    "INSERT INTO pipeline_stage_runs ("
                    "id,pipeline_run_id,node_key,shard_key,node_kind,state,"
                    "resolved_execution_spec_json,resolved_execution_spec_bytes,"
                    "execution_spec_digest,resolved_input_bindings_json,"
                    "resolved_input_bindings_digest,resource_profile_json,"
                    "resource_profile_digest,failure_policy,ready_at,claimed_at,attempt_count) "
                    "VALUES ("
                    ":id,:run,'generate_card_00','singleton','container','claimed',"
                    "CAST(:spec AS jsonb),:spec_bytes,:spec_digest,'[]'::jsonb,"
                    ":bindings_digest,'{}'::jsonb,:profile_digest,'fail_run',now(),now(),1)"
                ),
                {
                    "id": stage_id,
                    "run": run_id,
                    "spec": json.dumps(execution_spec, sort_keys=True),
                    "spec_bytes": canonical_document(execution_spec),
                    "spec_digest": execution_spec_digest,
                    "bindings_digest": canonical_digest([]),
                    "profile_digest": "sha256:" + "9" * 64,
                },
            )
            connection.execute(
                text(
                    "INSERT INTO execution_attempts ("
                    "id,stage_run_id,attempt_number,state,worker_id,claim_id,lease_epoch,"
                    "lease_token_digest,lease_expires_at,execution_authorization_json,"
                    "execution_authorization_bytes,execution_authorization_digest,queued_at,"
                    "claimed_at) VALUES ("
                    ":id,:stage,1,'claimed',:worker,:claim,4,:lease_digest,:expires,"
                    "CAST(:authorization AS jsonb),:authorization_bytes,"
                    ":authorization_digest,now(),now())"
                ),
                {
                    "id": attempt_id,
                    "stage": stage_id,
                    "worker": _WORKER_ID,
                    "claim": claim_id,
                    "lease_digest": hashlib.sha256(lease_token.encode("ascii")).hexdigest(),
                    "expires": datetime.now(UTC) + timedelta(minutes=10),
                    "authorization": json.dumps(execution_authorization, sort_keys=True),
                    "authorization_bytes": canonical_document(execution_authorization),
                    "authorization_digest": execution_authorization_digest,
                },
            )
    finally:
        engine.dispose()
    return seed


def test_protected_registration_route_uses_runtime_guard_projection(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = asyncio.run(_seed_protected_worker(capacity_guard_database))
    _seed_public_slurm_job(capacity_guard_database)
    bearer = _seed_worker_bearer(capacity_guard_database)
    app = _protected_cp_app(capacity_guard_database, monkeypatch, tmp_path)

    with TestClient(app) as client:
        response = client.post(
            "/workers/register",
            headers={
                "Authorization": f"Bearer {bearer}",
                "X-Loom-Executor-Worker-Credential": _WORKER_CREDENTIAL,
            },
            json=_worker_registration_request(),
        )

    assert response.status_code == 200, response.text
    assert response.json()["worker_id"] == str(seeded.worker.worker_id)


def test_protected_gpu_registration_route_preserves_physical_and_scheduling_pools(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = asyncio.run(
        _seed_protected_worker(
            capacity_guard_database,
            resources=ResourceVectorV1(
                slots=1,
                cpu_millicores=2000,
                memory_bytes=2 * 1024 * 1024,
                gpu_count=2,
            ),
        )
    )
    bearer = _seed_worker_bearer(capacity_guard_database)
    app = _protected_cp_app(capacity_guard_database, monkeypatch, tmp_path)

    with TestClient(app) as client:
        response = client.post(
            "/workers/register",
            headers={
                "Authorization": f"Bearer {bearer}",
                "X-Loom-Executor-Worker-Credential": _WORKER_CREDENTIAL,
            },
            json=_oldlab_gpu_worker_registration_request(),
        )

    assert response.status_code == 200, response.text
    assert response.json()["worker_id"] == str(seeded.worker.worker_id)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT j.slurm_cluster_id, j.pool_name, j.requested_gpu_tres, "
                        "j.requested_gpus, w.pool_name AS worker_pool_name "
                        "FROM public.slurm_worker_jobs AS j "
                        "JOIN public.workers AS w ON w.id = j.worker_id"
                    )
                )
                .mappings()
                .one()
            )
    finally:
        engine.dispose()
    assert dict(row) == {
        "slurm_cluster_id": "oldlab",
        "pool_name": "behavior-gpu-oldlab",
        "requested_gpu_tres": "gpu:rtx5080:2",
        "requested_gpus": 2,
        "worker_pool_name": "behavior-gpu-oldlab",
    }


@pytest.mark.parametrize("credential", (None, "wrong-protected-worker-credential"))
def test_configured_protected_registration_route_rejects_missing_or_wrong_credential(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    credential: str | None,
) -> None:
    asyncio.run(_seed_protected_worker(capacity_guard_database))
    _seed_public_slurm_job(capacity_guard_database)
    bearer = _seed_worker_bearer(capacity_guard_database)
    app = _protected_cp_app(capacity_guard_database, monkeypatch, tmp_path)
    headers = {"Authorization": f"Bearer {bearer}"}
    if credential is not None:
        headers["X-Loom-Executor-Worker-Credential"] = credential

    with TestClient(app) as client:
        response = client.post(
            "/workers/register",
            headers=headers,
            json=_worker_registration_request(),
        )

    assert response.status_code == 401
    assert response.json() == {"detail": "protected worker session rejected"}


def test_protected_registration_header_without_runtime_store_never_falls_back(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    asyncio.run(_seed_protected_worker(capacity_guard_database))
    _seed_public_slurm_job(capacity_guard_database)
    bearer = _seed_worker_bearer(capacity_guard_database)
    app = _protected_cp_app(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
        configure_runtime=False,
    )

    with TestClient(app) as client:
        response = client.post(
            "/workers/register",
            headers={
                "Authorization": f"Bearer {bearer}",
                "X-Loom-Executor-Worker-Credential": _WORKER_CREDENTIAL,
            },
            json=_worker_registration_request(),
        )

    assert response.status_code == 503
    assert response.json() == {"detail": "protected worker runtime unavailable"}


@pytest.mark.parametrize("path", ("/trials/claim", "/work/claim"))
def test_protected_claim_routes_accept_guard_credential_instead_of_bearer_hash(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    path: str,
) -> None:
    seeded = asyncio.run(_seed_protected_worker(capacity_guard_database))
    _seed_public_slurm_job(capacity_guard_database)
    registration_projection = _public_registration_payload()
    registration_projection["supported_work_kinds"] = ["trial", "execution_attempt"]
    projected = asyncio.run(
        _project_worker(
            capacity_guard_database,
            payload=registration_projection,
        )
    )
    bearer = _seed_worker_bearer(capacity_guard_database)
    app = _protected_cp_app(capacity_guard_database, monkeypatch, tmp_path)
    if path == "/trials/claim":
        payload = {
            "worker_id": str(seeded.worker.worker_id),
            "caps": _worker_registration_request()["capabilities"],
        }
    else:
        payload = {
            "schema_version": "loom.work-claim-request.v1",
            "worker_id": str(seeded.worker.worker_id),
            "capability_snapshot_digest": projected["capability_snapshot_digest"],
            "supported_work_kinds": ["trial", "execution_attempt"],
            "free_slots": 1,
        }

    with TestClient(app) as client:
        response = client.post(
            path,
            headers={
                "Authorization": f"Bearer {bearer}",
                "X-Loom-Executor-Worker-Credential": _WORKER_CREDENTIAL,
            },
            json=payload,
        )

    assert response.status_code == 204, response.text


def test_protected_pending_trial_cancel_is_guarded_atomic_and_idempotent(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    asyncio.run(_seed_protected_worker(capacity_guard_database))
    team_id = uuid4()
    user_id = uuid4()
    submit_token = f"loom_team_{uuid4().hex}"
    task_id = f"protected-cancel-{uuid4().hex}"
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.begin() as connection:
            connection.execute(
                insert(Team).values(id=team_id, name=f"protected-cancel-{team_id}")
            )
            connection.execute(
                insert(User).values(
                    id=user_id,
                    username=f"protected-cancel-{user_id.hex[:8]}",
                    username_normalized=f"protected-cancel-{user_id.hex[:8]}",
                    status="active",
                    is_platform_admin=False,
                )
            )
            connection.execute(insert(TeamQuota).values(team_id=team_id))
            connection.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(submit_token.encode("ascii")).digest(),
                    type="team",
                    scopes=["submit"],
                    team_id=team_id,
                    created_by_user_id=user_id,
                    issued_at=datetime.now(UTC),
                    expires_at=None,
                )
            )
            connection.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="7" * 64,
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {"os": "linux", "docker_image": "alpine"},
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                        "steps": [{"name": "main"}],
                    },
                )
            )

        app = _protected_cp_app(capacity_guard_database, monkeypatch, tmp_path)
        with TestClient(app) as client:
            submitted = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {submit_token}"},
                json={
                    "task_id": task_id,
                    "required_worker_pool": "oldlab",
                    "config": {"agent_name": "oracle", "agent_model": None},
                },
            )
            assert submitted.status_code == 201, submitted.text
            trial_id = UUID(submitted.json()["trial_id"])

            cancelled = client.post(
                f"/trials/{trial_id}/cancel",
                headers={"Authorization": f"Bearer {submit_token}"},
            )
            replay = client.post(
                f"/trials/{trial_id}/cancel",
                headers={"Authorization": f"Bearer {submit_token}"},
            )

        assert cancelled.status_code == 200, cancelled.text
        assert cancelled.json() == {
            "trial_id": str(trial_id),
            "state": "cancelled",
        }
        assert replay.status_code == 200, replay.text
        assert replay.json() == cancelled.json()

        with engine.connect() as connection:
            state = (
                connection.execute(
                    text(
                        "SELECT trial.state, trial.worker_id, trial.attempt_count, "
                        "trial.cancellation_requested_at IS NOT NULL "
                        "AS cancellation_requested, "
                        "trial.cancellation_observed_at IS NOT NULL "
                        "AS cancellation_observed, "
                        "trial.finished_at IS NOT NULL AS finished, "
                        "attempt.protected_attempt_id, head.transition_sequence, "
                        "head.lifecycle_state, terminal.operation, "
                        "terminal.previous_state, terminal.executable, "
                        "terminal.payload->>'transition_reason' AS transition_reason, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.attempt_lifecycle_events AS event "
                        "WHERE event.protected_attempt_id = attempt.protected_attempt_id "
                        "AND event.operation = 'cancel') AS cancellation_events, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.executable_claim_leases AS claim "
                        "WHERE claim.protected_attempt_id = attempt.protected_attempt_id) "
                        "AS executable_claims, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.executable_claim_terminal_events AS event "
                        "WHERE event.protected_attempt_id = attempt.protected_attempt_id) "
                        "AS executable_terminal_events, "
                        "(SELECT count(*) FROM public.execution_admission_reservations "
                        "AS reservation WHERE reservation.trial_id = trial.id) "
                        "AS admission_reservations "
                        "FROM public.trials AS trial "
                        "JOIN loom_capacity_guard.protected_runtime_trial_submissions "
                        "AS runtime ON runtime.trial_id = trial.id "
                        "JOIN loom_capacity_guard.trial_attempts AS attempt "
                        "ON attempt.trial_id = runtime.trial_id "
                        "AND attempt.protected_attempt_id = runtime.protected_attempt_id "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS head "
                        "ON head.protected_attempt_id = attempt.protected_attempt_id "
                        "JOIN loom_capacity_guard.attempt_lifecycle_events AS terminal "
                        "ON terminal.transition_id = head.transition_id "
                        "WHERE trial.id = :trial_id"
                    ),
                    {"trial_id": trial_id},
                )
                .mappings()
                .one()
            )
        assert dict(state) | {"protected_attempt_id": "attempt"} == {
            "state": "cancelled",
            "worker_id": None,
            "attempt_count": 0,
            "cancellation_requested": True,
            "cancellation_observed": True,
            "finished": True,
            "protected_attempt_id": "attempt",
            "transition_sequence": 1,
            "lifecycle_state": "cancelled-terminal",
            "operation": "cancel",
            "previous_state": "pending-unassigned",
            "executable": False,
            "transition_reason": "protected-runtime-user-cancel",
            "cancellation_events": 1,
            "executable_claims": 0,
            "executable_terminal_events": 0,
            "admission_reservations": 0,
        }
    finally:
        engine.dispose()


def test_family_orchestrator_cancels_assigned_unclaimed_protected_trial(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = asyncio.run(_seed_protected_worker(capacity_guard_database))
    team_id = uuid4()
    user_id = uuid4()
    batch_id = uuid4()
    submit_token = f"loom_team_{uuid4().hex}"
    family_token = f"loom_fo_{uuid4().hex}"
    task_id = f"protected-family-cancel-{uuid4().hex}"
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.begin() as connection:
            connection.execute(
                insert(Team).values(id=team_id, name=f"protected-family-{team_id}")
            )
            connection.execute(
                insert(User).values(
                    id=user_id,
                    username=f"protected-family-{user_id.hex[:8]}",
                    username_normalized=f"protected-family-{user_id.hex[:8]}",
                    status="active",
                    is_platform_admin=False,
                )
            )
            connection.execute(insert(TeamQuota).values(team_id=team_id))
            connection.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(submit_token.encode("ascii")).digest(),
                    type="team",
                    scopes=["submit"],
                    team_id=team_id,
                    created_by_user_id=user_id,
                    issued_at=datetime.now(UTC),
                    expires_at=None,
                )
            )
            connection.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(family_token.encode("ascii")).digest(),
                    type="family_orchestrator",
                    scopes=["family:cancel", "family:evolve"],
                    team_id=None,
                    issued_at=datetime.now(UTC),
                    expires_at=None,
                )
            )
            connection.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="8" * 64,
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {"os": "linux", "docker_image": "alpine"},
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                        "steps": [{"name": "main"}],
                    },
                )
            )
            connection.execute(
                text(
                    "INSERT INTO public.batches "
                    "(id, team_id, name, task_filter, trial_config, state, "
                    "created_by_token_prefix) VALUES "
                    "(:batch_id, :team_id, :name, '{}'::jsonb, '{}'::jsonb, "
                    "'running', 'family')"
                ),
                {
                    "batch_id": batch_id,
                    "team_id": team_id,
                    "name": f"protected-family-{batch_id}",
                },
            )

        app = _protected_cp_app(capacity_guard_database, monkeypatch, tmp_path)
        with TestClient(app) as client:
            submitted = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {submit_token}"},
                json={
                    "task_id": task_id,
                    "required_worker_pool": "oldlab",
                    "config": {"agent_name": "oracle", "agent_model": None},
                },
            )
            assert submitted.status_code == 201, submitted.text
            trial_id = UUID(submitted.json()["trial_id"])
            with engine.connect() as connection:
                attempt = (
                    connection.execute(
                        text(
                            "SELECT attempt.protected_attempt_id, "
                            "attempt.execution_generation, attempt.requirements_digest "
                            "FROM loom_capacity_guard.atomic_trial_submissions AS submission "
                            "JOIN loom_capacity_guard.trial_attempts AS attempt "
                            "ON attempt.protected_attempt_id = "
                            "submission.protected_attempt_id "
                            "WHERE submission.trial_id = :trial_id"
                        ),
                        {"trial_id": trial_id},
                    )
                    .mappings()
                    .one()
                )
            asyncio.run(
                _assign_protected_attempt(
                    capacity_guard_database,
                    registration=seeded.registration,
                    request=seeded.bootstrap,
                    protected_attempt_id=attempt["protected_attempt_id"],
                    execution_generation=attempt["execution_generation"],
                    requirements_digest=attempt["requirements_digest"],
                )
            )
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "UPDATE public.trials SET batch_id = :batch_id, "
                        "family_key = 'family-a' WHERE id = :trial_id"
                    ),
                    {"batch_id": batch_id, "trial_id": trial_id},
                )

            cancelled = client.post(
                f"/trials/{trial_id}/cancel",
                headers={"Authorization": f"Bearer {family_token}"},
            )

        assert cancelled.status_code == 200, cancelled.text
        assert cancelled.json() == {
            "trial_id": str(trial_id),
            "state": "cancelled",
        }
        with engine.connect() as connection:
            state = (
                connection.execute(
                    text(
                        "SELECT trial.state, head.lifecycle_state, event.operation, "
                        "event.previous_state, event.executable "
                        "FROM public.trials AS trial "
                        "JOIN loom_capacity_guard.protected_runtime_trial_submissions "
                        "AS runtime ON runtime.trial_id = trial.id "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS head "
                        "ON head.protected_attempt_id = runtime.protected_attempt_id "
                        "JOIN loom_capacity_guard.attempt_lifecycle_events AS event "
                        "ON event.transition_id = head.transition_id "
                        "WHERE trial.id = :trial_id"
                    ),
                    {"trial_id": trial_id},
                )
                .mappings()
                .one()
            )
        assert dict(state) == {
            "state": "cancelled",
            "lifecycle_state": "cancelled-terminal",
            "operation": "cancel",
            "previous_state": "assigned",
            "executable": False,
        }
    finally:
        engine.dispose()


def test_protected_work_claim_consumes_exact_manager_assignment(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = asyncio.run(_seed_protected_worker(capacity_guard_database))
    projection = _public_registration_payload()
    projection["supported_work_kinds"] = ["trial", "execution_attempt"]
    projected = asyncio.run(_project_worker(capacity_guard_database, payload=projection))
    team_id = uuid4()
    user_id = uuid4()
    submit_token = f"loom_team_{uuid4().hex}"
    worker_bearer = _seed_worker_bearer(capacity_guard_database)
    task_id = f"protected-claim-{uuid4().hex}"
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.begin() as connection:
            connection.execute(insert(Team).values(id=team_id, name=f"protected-claim-{team_id}"))
            connection.execute(
                insert(User).values(
                    id=user_id,
                    username=f"protected-claim-{user_id.hex[:8]}",
                    username_normalized=f"protected-claim-{user_id.hex[:8]}",
                    status="active",
                    is_platform_admin=False,
                )
            )
            connection.execute(insert(TeamQuota).values(team_id=team_id))
            connection.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(submit_token.encode("ascii")).digest(),
                    type="team",
                    scopes=["submit"],
                    team_id=team_id,
                    created_by_user_id=user_id,
                    issued_at=datetime.now(UTC),
                    expires_at=None,
                )
            )
            connection.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="8" * 64,
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {"os": "linux", "docker_image": "alpine"},
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                        "steps": [{"name": "main"}],
                    },
                )
            )

        app = _protected_cp_app(capacity_guard_database, monkeypatch, tmp_path)
        with TestClient(app) as client:
            submission_response = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {submit_token}"},
                json={
                    "task_id": task_id,
                    "required_worker_pool": "oldlab",
                    "config": {"agent_name": "oracle", "agent_model": None},
                },
            )
        assert submission_response.status_code == 201, submission_response.text
        trial_id = UUID(submission_response.json()["trial_id"])
        with engine.connect() as connection:
            attempt = (
                connection.execute(
                    text(
                        "SELECT attempt.protected_attempt_id, "
                        "attempt.execution_generation, attempt.requirements_digest "
                        "FROM loom_capacity_guard.atomic_trial_submissions AS submission "
                        "JOIN loom_capacity_guard.trial_attempts AS attempt "
                        "ON attempt.protected_attempt_id = submission.protected_attempt_id "
                        "WHERE submission.trial_id = :trial_id"
                    ),
                    {"trial_id": trial_id},
                )
                .mappings()
                .one()
            )
        asyncio.run(
            _assign_protected_attempt(
                capacity_guard_database,
                registration=seeded.registration,
                request=seeded.bootstrap,
                protected_attempt_id=attempt["protected_attempt_id"],
                execution_generation=attempt["execution_generation"],
                requirements_digest=attempt["requirements_digest"],
            )
        )

        with TestClient(app) as client:
            claim_response = client.post(
                "/work/claim",
                headers={
                    "Authorization": f"Bearer {worker_bearer}",
                    "X-Loom-Executor-Worker-Credential": _WORKER_CREDENTIAL,
                },
                json={
                    "schema_version": "loom.work-claim-request.v1",
                    "worker_id": str(seeded.worker.worker_id),
                    "capability_snapshot_digest": projected["capability_snapshot_digest"],
                    "supported_work_kinds": ["trial", "execution_attempt"],
                    "free_slots": 1,
                },
            )

        assert claim_response.status_code == 200, claim_response.text
        assert claim_response.json()["work_kind"] == "trial"
        assert claim_response.json()["payload"]["trial_id"] == str(trial_id)
        with engine.connect() as connection:
            state = (
                connection.execute(
                    text(
                        "SELECT trial.state, trial.worker_id, trial.attempt_count, "
                        "claim.claim_high_water, claim.intent_id, "
                        "claim.protected_attempt_id "
                        "FROM public.trials AS trial "
                        "JOIN loom_capacity_guard.executable_claim_leases AS claim "
                        "ON claim.protected_attempt_id = :protected_attempt_id "
                        "WHERE trial.id = :trial_id"
                    ),
                    {
                        "trial_id": trial_id,
                        "protected_attempt_id": attempt["protected_attempt_id"],
                    },
                )
                .mappings()
                .one()
            )
        assert dict(state) == {
            "state": "claimed",
            "worker_id": seeded.worker.worker_id,
            "attempt_count": 1,
            "claim_high_water": 1,
            "intent_id": seeded.worker.binding.intent_id,
            "protected_attempt_id": attempt["protected_attempt_id"],
        }

        with TestClient(app) as client:
            terminal_response = client.patch(
                f"/trials/{trial_id}/state",
                headers={
                    "Authorization": f"Bearer {worker_bearer}",
                    "X-Loom-Executor-Worker-Credential": _WORKER_CREDENTIAL,
                },
                json={
                    "worker_id": str(seeded.worker.worker_id),
                    "state": "failed",
                    "failure_reason": "agent_error",
                },
            )

        assert terminal_response.status_code == 200, terminal_response.text
        with engine.connect() as connection:
            terminal = (
                connection.execute(
                    text(
                        "SELECT trial.state, reservation.state AS reservation_state, "
                        "reservation.release_reason, head.lifecycle_state, "
                        "claim_state.claim_high_water, claim_state.terminal_high_water, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.executable_claim_terminal_events AS event "
                        "WHERE event.protected_attempt_id = :protected_attempt_id) "
                        "AS terminal_events "
                        "FROM public.trials AS trial "
                        "JOIN public.execution_admission_reservations AS reservation "
                        "ON reservation.trial_id = trial.id "
                        "AND reservation.owner_kind = 'protected_worker_claim' "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS head "
                        "ON head.protected_attempt_id = :protected_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_leases AS claim "
                        "ON claim.protected_attempt_id = head.protected_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_state AS claim_state "
                        "ON claim_state.intent_id = claim.intent_id "
                        "WHERE trial.id = :trial_id"
                    ),
                    {
                        "trial_id": trial_id,
                        "protected_attempt_id": attempt["protected_attempt_id"],
                    },
                )
                .mappings()
                .one()
            )
        assert dict(terminal) == {
            "state": "failed",
            "reservation_state": "released",
            "release_reason": "trial_left_active_state",
            "lifecycle_state": "cancelled-terminal",
            "claim_high_water": 1,
            "terminal_high_water": 1,
            "terminal_events": 1,
        }
    finally:
        engine.dispose()


def _seed_claimed_protected_trial(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    max_attempts_ceiling: int | None = None,
) -> _ClaimedProtectedTrialSeed:
    seeded = asyncio.run(_seed_protected_worker(capacity_guard_database))
    projection = _public_registration_payload()
    projection["supported_work_kinds"] = ["trial", "execution_attempt"]
    projected = asyncio.run(_project_worker(capacity_guard_database, payload=projection))
    team_id = uuid4()
    user_id = uuid4()
    submit_token = f"loom_team_{uuid4().hex}"
    worker_bearer = _seed_worker_bearer(capacity_guard_database)
    task_id = f"protected-retry-{uuid4().hex}"
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.begin() as connection:
            connection.execute(insert(Team).values(id=team_id, name=f"protected-retry-{team_id}"))
            connection.execute(
                insert(User).values(
                    id=user_id,
                    username=f"protected-retry-{user_id.hex[:8]}",
                    username_normalized=f"protected-retry-{user_id.hex[:8]}",
                    status="active",
                    is_platform_admin=False,
                )
            )
            quota_values: dict[str, object] = {"team_id": team_id}
            if max_attempts_ceiling is not None:
                quota_values["max_attempts_ceiling"] = max_attempts_ceiling
            connection.execute(insert(TeamQuota).values(**quota_values))
            connection.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(submit_token.encode("ascii")).digest(),
                    type="team",
                    scopes=["submit"],
                    team_id=team_id,
                    created_by_user_id=user_id,
                    issued_at=datetime.now(UTC),
                    expires_at=None,
                )
            )
            connection.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="9" * 64,
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {
                            "os": "linux",
                            "docker_image": "alpine",
                        },
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                        "steps": [{"name": "main"}],
                    },
                )
            )

        app = _protected_cp_app(capacity_guard_database, monkeypatch, tmp_path)
        claim_headers = {
            "Authorization": f"Bearer {worker_bearer}",
            "X-Loom-Executor-Worker-Credential": _WORKER_CREDENTIAL,
        }
        claim_payload: dict[str, object] = {
            "schema_version": "loom.work-claim-request.v1",
            "worker_id": str(seeded.worker.worker_id),
            "capability_snapshot_digest": projected["capability_snapshot_digest"],
            "supported_work_kinds": ["trial", "execution_attempt"],
            "free_slots": 1,
        }
        with TestClient(app) as client:
            submission_response = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {submit_token}"},
                json={
                    "task_id": task_id,
                    "required_worker_pool": "oldlab",
                    "config": {"agent_name": "oracle", "agent_model": None},
                },
            )
        assert submission_response.status_code == 201, submission_response.text
        trial_id = UUID(submission_response.json()["trial_id"])
        with engine.connect() as connection:
            first_attempt = dict(
                connection.execute(
                    text(
                        "SELECT protected_attempt_id, execution_generation, "
                        "requirements_digest FROM loom_capacity_guard.trial_attempts "
                        "WHERE trial_id = :trial_id AND attempt_sequence = 0"
                    ),
                    {"trial_id": trial_id},
                )
                .mappings()
                .one()
            )
        asyncio.run(
            _assign_protected_attempt(
                capacity_guard_database,
                registration=seeded.registration,
                request=seeded.bootstrap,
                protected_attempt_id=first_attempt["protected_attempt_id"],
                execution_generation=first_attempt["execution_generation"],
                requirements_digest=first_attempt["requirements_digest"],
            )
        )
        with TestClient(app) as client:
            first_claim = client.post(
                "/work/claim",
                headers=claim_headers,
                json=claim_payload,
            )
        assert first_claim.status_code == 200, first_claim.text
        assert first_claim.json()["payload"]["trial_id"] == str(trial_id)
        return _ClaimedProtectedTrialSeed(
            app=app,
            worker=seeded,
            trial_id=trial_id,
            submit_token=submit_token,
            claim_headers=claim_headers,
            claim_payload=claim_payload,
            first_attempt=first_attempt,
        )
    finally:
        engine.dispose()


def _assign_retry_attempt(
    capacity_guard_database: dict[str, object],
    seeded: _ClaimedProtectedTrialSeed,
) -> dict[str, object]:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            retry_attempt = dict(
                connection.execute(
                    text(
                        "SELECT protected_attempt_id, execution_generation, "
                        "requirements_digest FROM loom_capacity_guard.trial_attempts "
                        "WHERE trial_id = :trial_id AND attempt_sequence = 1"
                    ),
                    {"trial_id": seeded.trial_id},
                )
                .mappings()
                .one()
            )
    finally:
        engine.dispose()
    retry_execution = seeded.worker.bootstrap.binding.execution.model_copy(
        update={
            "allocation_epoch": (seeded.worker.bootstrap.binding.execution.allocation_epoch + 1)
        }
    )
    retry_request = seeded.worker.bootstrap.model_copy(
        update={
            "binding": seeded.worker.bootstrap.binding.model_copy(
                update={"execution": retry_execution}
            )
        }
    )
    asyncio.run(
        _assign_protected_attempt(
            capacity_guard_database,
            registration=seeded.worker.registration,
            request=retry_request,
            protected_attempt_id=retry_attempt["protected_attempt_id"],
            execution_generation=retry_attempt["execution_generation"],
            requirements_digest=retry_attempt["requirements_digest"],
        )
    )
    return retry_attempt


async def _reclaim_expired_protected_trials(
    database: dict[str, object],
    *,
    expiry_sec: int = 300,
    claimed_without_start_expiry_sec: int | None = 300,
) -> int:
    engine = create_async_engine(make_url(_value(database, "admin_url")))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            count = await reclaim_expired_workers(
                session,
                expiry_sec=expiry_sec,
                claimed_without_start_expiry_sec=claimed_without_start_expiry_sec,
            )
            await session.commit()
        return count
    finally:
        await engine.dispose()


def _terminal_inventory_evidence(
    seeded: _ClaimedProtectedTrialSeed,
) -> ExecutableTerminalInventoryEvidenceV2:
    binding = seeded.worker.bootstrap.binding
    metadata = ExecutableOwnershipMetadataV2(
        binding=binding,
        controller_authority_sha256="6" * 64,
        trusted_launcher_sha256=binding.execution.trusted_fleet_release_sha256,
        slurm_cluster="oldlab-controller",
        submitter_identity="loom",
        association="loom",
        submitted_at=datetime(2026, 9, 5, 12, 0, tzinfo=UTC),
    )
    record = ExecutableInventoryRecordV2(
        physical_identity=_JOB_ID,
        physical_kind="slurm-job",
        authority_scope="dedicated-loom-association",
        state="terminal",
        resources=binding.resources,
        node_ids=binding.node_ids,
        controller_evidence_sha256="7" * 64,
        ownership_proof=SignedExecutableOwnershipProofV2(
            metadata=metadata,
            signing_key_id="oldlab-key",
            signature_base64=base64.b64encode(b"\0" * 64).decode("ascii"),
        ),
        terminal_evidence_sha256="8" * 64,
    )
    return ExecutableTerminalInventoryEvidenceV2(
        binding=binding,
        inventory_execution=ExecutionContextV2.model_validate(
            binding.execution.model_dump(
                mode="python",
                exclude={"allocation_epoch", "executable"},
            )
        ),
        inventory_sequence=4,
        inventory_digest="9" * 64,
        journal_sequence=5,
        journal_digest="b" * 64,
        record=record,
        observed_at=datetime(2026, 9, 5, 12, 1, tzinfo=UTC),
    )


async def _import_terminal_inventory_evidence(
    database: dict[str, object],
    seeded: _ClaimedProtectedTrialSeed,
    evidence: ExecutableTerminalInventoryEvidenceV2,
) -> dict[str, object]:
    engine = create_async_engine(
        make_url(_value(database, "agent_url")),
        isolation_level="SERIALIZABLE",
    )
    payload = canonical_executable_bytes(evidence)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            returned = (
                await session.execute(
                    text(
                        "SELECT loom_capacity_guard.import_executable_terminal_inventory_evidence("
                        ":agent_incarnation, :protected_attempt_id, CAST(:payload AS jsonb), "
                        "CAST(:canonical_payload AS bytea), :evidence_digest)"
                    ),
                    {
                        "agent_incarnation": seeded.worker.registration.agent_incarnation,
                        "protected_attempt_id": seeded.first_attempt["protected_attempt_id"],
                        "payload": payload.decode("ascii"),
                        "canonical_payload": payload,
                        "evidence_digest": canonical_executable_digest(evidence),
                    },
                )
            ).scalar_one()
            await session.commit()
    finally:
        await engine.dispose()
    assert isinstance(returned, dict)
    return returned


async def _import_terminal_inventory_payload(
    database: dict[str, object],
    seeded: _ClaimedProtectedTrialSeed,
    payload: dict[str, object],
    *,
    protected_attempt_id: UUID | None = None,
    canonical_payload: bytes | None = None,
    evidence_digest: str | None = None,
    caller_url_key: str = "agent_url",
) -> dict[str, object]:
    canonical = canonical_payload or json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    digest = evidence_digest or hashlib.sha256(canonical).hexdigest()
    engine = create_async_engine(
        make_url(_value(database, caller_url_key)),
        isolation_level="SERIALIZABLE",
    )
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            returned = (
                await session.execute(
                    text(
                        "SELECT loom_capacity_guard.import_executable_terminal_inventory_evidence("
                        ":agent_incarnation, :protected_attempt_id, CAST(:payload AS jsonb), "
                        "CAST(:canonical_payload AS bytea), :evidence_digest)"
                    ),
                    {
                        "agent_incarnation": seeded.worker.registration.agent_incarnation,
                        "protected_attempt_id": (
                            protected_attempt_id
                            or seeded.first_attempt["protected_attempt_id"]
                        ),
                        "payload": json.dumps(payload),
                        "canonical_payload": canonical,
                        "evidence_digest": digest,
                    },
                )
            ).scalar_one()
            await session.commit()
    finally:
        await engine.dispose()
    assert isinstance(returned, dict)
    return returned


def test_terminal_inventory_import_latches_exact_live_claim_as_draining(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
    )
    evidence = _terminal_inventory_evidence(seeded)

    receipt = asyncio.run(
        _import_terminal_inventory_evidence(
            capacity_guard_database,
            seeded,
            evidence,
        )
    )

    assert receipt == {
        "schema_version": 2,
        "intent_id": str(evidence.binding.intent_id),
        "protected_attempt_id": str(seeded.first_attempt["protected_attempt_id"]),
        "worker_id": str(seeded.worker.worker.worker_id),
        "worker_incarnation": str(seeded.worker.worker.worker_incarnation),
        "physical_job_id": _JOB_ID,
        "inventory_sequence": evidence.inventory_sequence,
        "terminal_evidence_sha256": evidence.record.terminal_evidence_sha256,
        "evidence_digest": canonical_executable_digest(evidence),
        "import_state": "imported",
        "executable": False,
    }
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            state = (
                connection.execute(
                    text(
                        "SELECT trial.state, trial.worker_id, "
                        "claim_state.claim_high_water, claim_state.terminal_high_water, "
                        "claim_state.draining, claim.operation_id AS claim_operation_id, "
                        "evidence.claim_operation_id AS evidence_claim_operation_id, "
                        "evidence.intent_id, "
                        "evidence.protected_attempt_id, "
                        "evidence.worker_id AS evidence_worker_id, "
                        "evidence.worker_incarnation AS evidence_worker_incarnation, "
                        "evidence.physical_job_id, evidence.inventory_sequence, "
                        "evidence.terminal_evidence_sha256, evidence.evidence_digest "
                        "FROM public.trials AS trial "
                        "JOIN loom_capacity_guard.executable_claim_leases AS claim "
                        "ON claim.protected_attempt_id = :protected_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_state AS claim_state "
                        "ON claim_state.intent_id = claim.intent_id "
                        "JOIN loom_capacity_guard.executable_terminal_inventory_evidence "
                        "AS evidence ON evidence.claim_operation_id = claim.operation_id "
                        "WHERE trial.id = :trial_id"
                    ),
                    {
                        "trial_id": seeded.trial_id,
                        "protected_attempt_id": seeded.first_attempt["protected_attempt_id"],
                    },
                )
                .mappings()
                .one()
            )
    finally:
        engine.dispose()
    assert state["evidence_claim_operation_id"] == state["claim_operation_id"]
    state = dict(state)
    state.pop("evidence_claim_operation_id")
    assert dict(state) == {
        "state": "claimed",
        "worker_id": seeded.worker.worker.worker_id,
        "claim_high_water": 1,
        "terminal_high_water": 0,
        "draining": True,
        "claim_operation_id": state["claim_operation_id"],
        "intent_id": evidence.binding.intent_id,
        "protected_attempt_id": seeded.first_attempt["protected_attempt_id"],
        "evidence_worker_id": seeded.worker.worker.worker_id,
        "evidence_worker_incarnation": seeded.worker.worker.worker_incarnation,
        "physical_job_id": _JOB_ID,
        "inventory_sequence": evidence.inventory_sequence,
        "terminal_evidence_sha256": evidence.record.terminal_evidence_sha256,
        "evidence_digest": canonical_executable_digest(evidence),
    }


def test_agent_store_terminal_inventory_import_is_typed_and_restart_idempotent(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
    )
    evidence = _terminal_inventory_evidence(seeded)

    async def import_twice():  # type: ignore[no-untyped-def]
        engine = create_async_engine(
            make_url(_value(capacity_guard_database, "agent_url")),
            isolation_level="SERIALIZABLE",
        )
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            async with factory() as session, session.begin():
                first = await import_executable_terminal_inventory_evidence(
                    session,
                    registration=seeded.worker.registration,
                    protected_attempt_id=seeded.first_attempt["protected_attempt_id"],
                    evidence=evidence,
                )
            async with factory() as session, session.begin():
                replay = await import_executable_terminal_inventory_evidence(
                    session,
                    registration=seeded.worker.registration,
                    protected_attempt_id=seeded.first_attempt["protected_attempt_id"],
                    evidence=evidence,
                )
        finally:
            await engine.dispose()
        return first, replay

    first, replay = asyncio.run(import_twice())

    assert replay == first
    assert first.intent_id == evidence.binding.intent_id
    assert first.protected_attempt_id == seeded.first_attempt["protected_attempt_id"]
    assert first.worker_id == seeded.worker.worker.worker_id
    assert first.worker_incarnation == seeded.worker.worker.worker_incarnation
    assert first.physical_job_id == _JOB_ID
    assert first.inventory_sequence == evidence.inventory_sequence
    assert first.terminal_evidence_sha256 == evidence.record.terminal_evidence_sha256
    assert first.evidence_digest == canonical_executable_digest(evidence)
    assert first.import_state == "imported"
    assert first.executable is False


def test_terminal_inventory_import_rejects_inexact_untrusted_or_noncanonical_evidence(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
    )
    evidence = _terminal_inventory_evidence(seeded)
    base = evidence.model_dump(mode="json", exclude_none=False)

    def changed_binding(field: str, value: object) -> dict[str, object]:
        payload = json.loads(json.dumps(base))
        payload["binding"][field] = value
        payload["record"]["ownership_proof"]["metadata"]["binding"] = json.loads(
            json.dumps(payload["binding"])
        )
        return payload

    cases: list[tuple[str, dict[str, object], UUID | None]] = [
        ("wrong intent", changed_binding("intent_id", str(uuid4())), None),
        ("wrong subject", changed_binding("subject_id", str(uuid4())), None),
        (
            "wrong subject incarnation",
            changed_binding("subject_incarnation", str(uuid4())),
            None,
        ),
        ("wrong executor", changed_binding("executor_id", "other-executor"), None),
        ("wrong pool", changed_binding("pool_id", "gb10"), None),
        (
            "wrong physical job",
            json.loads(json.dumps(base)),
            None,
        ),
        (
            "zero inventory sequence",
            json.loads(json.dumps(base)),
            None,
        ),
        ("invalid journal head", json.loads(json.dumps(base)), None),
        ("foreign record", json.loads(json.dumps(base)), None),
        ("wrong protected attempt", json.loads(json.dumps(base)), uuid4()),
    ]
    cases[5][1]["record"]["physical_identity"] = "99999"
    cases[6][1]["inventory_sequence"] = 0
    cases[7][1]["journal_sequence"] = 0
    cases[8][1]["record"]["authority_scope"] = "foreign"
    cases[8][1]["record"]["ownership_proof"] = None

    for _label, payload, protected_attempt_id in cases:
        with pytest.raises(DBAPIError):
            asyncio.run(
                _import_terminal_inventory_payload(
                    capacity_guard_database,
                    seeded,
                    payload,
                    protected_attempt_id=protected_attempt_id,
                )
            )

    canonical = canonical_executable_bytes(evidence)
    with pytest.raises(DBAPIError):
        asyncio.run(
            _import_terminal_inventory_payload(
                capacity_guard_database,
                seeded,
                base,
                canonical_payload=canonical + b" ",
                evidence_digest=hashlib.sha256(canonical + b" ").hexdigest(),
            )
        )
    with pytest.raises(DBAPIError):
        asyncio.run(
            _import_terminal_inventory_payload(
                capacity_guard_database,
                seeded,
                base,
                caller_url_key="runtime_url",
            )
        )

    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT claim_state.draining, "
                    "(SELECT count(*) FROM "
                    "loom_capacity_guard.executable_terminal_inventory_evidence) "
                    "AS evidence_rows "
                    "FROM loom_capacity_guard.executable_claim_state AS claim_state "
                    "WHERE claim_state.intent_id = :intent_id"
                ),
                {"intent_id": evidence.binding.intent_id},
            ).mappings().one()
        assert dict(row) == {"draining": False, "evidence_rows": 0}
    finally:
        engine.dispose()


def test_terminal_inventory_import_is_idempotent_but_rejects_equivocation(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
    )
    evidence = _terminal_inventory_evidence(seeded)
    first = asyncio.run(
        _import_terminal_inventory_evidence(capacity_guard_database, seeded, evidence)
    )
    replay = asyncio.run(
        _import_terminal_inventory_evidence(capacity_guard_database, seeded, evidence)
    )
    assert replay == first

    equivocation = evidence.model_dump(mode="json", exclude_none=False)
    equivocation["observed_at"] = "2026-09-05T12:02:00Z"
    with pytest.raises(DBAPIError):
        asyncio.run(
            _import_terminal_inventory_payload(
                capacity_guard_database,
                seeded,
                equivocation,
            )
        )

    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert connection.execute(
                text(
                    "SELECT count(*) FROM "
                    "loom_capacity_guard.executable_terminal_inventory_evidence"
                )
            ).scalar_one() == 1
    finally:
        engine.dispose()


def test_guard_0028_refuses_downgrade_after_terminal_evidence_import(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
    )
    asyncio.run(
        _import_terminal_inventory_evidence(
            capacity_guard_database,
            seeded,
            _terminal_inventory_evidence(seeded),
        )
    )
    root = Path(__file__).resolve().parents[2]
    config = AlembicConfig(str(root / "capacity_guard_migrations" / "alembic.ini"))
    config.set_main_option("script_location", str(root / "capacity_guard_migrations"))
    for name, key in (
        ("LOOM_CAPACITY_GUARD_DB_URL", "migrator_url"),
        ("LOOM_CAPACITY_GUARD_OWNER_ROLE", "owner_role"),
        ("LOOM_CAPACITY_GUARD_AGENT_ROLE", "agent_role"),
        ("LOOM_CAPACITY_GUARD_EXECUTOR_ROLE", "executor_role"),
        ("LOOM_CAPACITY_GUARD_OBSERVER_ROLE", "observer_role"),
        ("LOOM_CAPACITY_GUARD_RUNTIME_ROLE", "runtime_role"),
    ):
        monkeypatch.setenv(name, _value(capacity_guard_database, key))

    with pytest.raises(
        RuntimeError,
        match="cannot downgrade guard_0028 while terminal inventory evidence exists",
    ):
        command.downgrade(config, "guard_0027")

    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert connection.execute(
                text(
                    "SELECT version_num FROM "
                    "loom_capacity_guard.capacity_guard_alembic_version"
                )
            ).scalar_one() == "guard_0029"
            assert connection.execute(
                text(
                    "SELECT count(*) FROM "
                    "loom_capacity_guard.executable_terminal_inventory_evidence"
                )
            ).scalar_one() == 1
    finally:
        engine.dispose()


def test_protected_prestart_retry_creates_a_new_inert_attempt_before_reclaim(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = asyncio.run(_seed_protected_worker(capacity_guard_database))
    projection = _public_registration_payload()
    projection["supported_work_kinds"] = ["trial", "execution_attempt"]
    projected = asyncio.run(_project_worker(capacity_guard_database, payload=projection))
    team_id = uuid4()
    user_id = uuid4()
    submit_token = f"loom_team_{uuid4().hex}"
    worker_bearer = _seed_worker_bearer(capacity_guard_database)
    task_id = f"protected-retry-{uuid4().hex}"
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.begin() as connection:
            connection.execute(insert(Team).values(id=team_id, name=f"protected-retry-{team_id}"))
            connection.execute(
                insert(User).values(
                    id=user_id,
                    username=f"protected-retry-{user_id.hex[:8]}",
                    username_normalized=f"protected-retry-{user_id.hex[:8]}",
                    status="active",
                    is_platform_admin=False,
                )
            )
            connection.execute(insert(TeamQuota).values(team_id=team_id))
            connection.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(submit_token.encode("ascii")).digest(),
                    type="team",
                    scopes=["submit"],
                    team_id=team_id,
                    created_by_user_id=user_id,
                    issued_at=datetime.now(UTC),
                    expires_at=None,
                )
            )
            connection.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="9" * 64,
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {"os": "linux", "docker_image": "alpine"},
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                        "steps": [{"name": "main"}],
                    },
                )
            )

        app = _protected_cp_app(capacity_guard_database, monkeypatch, tmp_path)
        claim_headers = {
            "Authorization": f"Bearer {worker_bearer}",
            "X-Loom-Executor-Worker-Credential": _WORKER_CREDENTIAL,
        }
        claim_payload = {
            "schema_version": "loom.work-claim-request.v1",
            "worker_id": str(seeded.worker.worker_id),
            "capability_snapshot_digest": projected["capability_snapshot_digest"],
            "supported_work_kinds": ["trial", "execution_attempt"],
            "free_slots": 1,
        }
        with TestClient(app) as client:
            submission_response = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {submit_token}"},
                json={
                    "task_id": task_id,
                    "required_worker_pool": "oldlab",
                    "config": {"agent_name": "oracle", "agent_model": None},
                },
            )
        assert submission_response.status_code == 201, submission_response.text
        trial_id = UUID(submission_response.json()["trial_id"])
        with engine.connect() as connection:
            first_attempt = (
                connection.execute(
                    text(
                        "SELECT protected_attempt_id, execution_generation, "
                        "requirements_digest FROM loom_capacity_guard.trial_attempts "
                        "WHERE trial_id = :trial_id AND attempt_sequence = 0"
                    ),
                    {"trial_id": trial_id},
                )
                .mappings()
                .one()
            )
        asyncio.run(
            _assign_protected_attempt(
                capacity_guard_database,
                registration=seeded.registration,
                request=seeded.bootstrap,
                protected_attempt_id=first_attempt["protected_attempt_id"],
                execution_generation=first_attempt["execution_generation"],
                requirements_digest=first_attempt["requirements_digest"],
            )
        )
        with TestClient(app) as client:
            first_claim = client.post(
                "/work/claim",
                headers=claim_headers,
                json=claim_payload,
            )
            retry = client.post(
                f"/trials/{trial_id}/retry",
                headers=claim_headers,
                json={
                    "worker_id": str(seeded.worker.worker_id),
                    "failure_reason": "env_start_failure",
                    "failure_message": "transient setup failure",
                    "retry_after_sec": 0,
                },
            )

        assert first_claim.status_code == 200, first_claim.text
        assert retry.status_code == 200, retry.text
        assert retry.json()["state"] == "protected-pending"

        with engine.connect() as connection:
            state = (
                connection.execute(
                    text(
                        "SELECT trial.state, trial.worker_id, trial.attempt_count, "
                        "trial.failure_reason, trial.failure_message, "
                        "trial.next_attempt_at <= statement_timestamp() AS retry_due, "
                        "reservation.state AS reservation_state, "
                        "old_head.lifecycle_state AS old_lifecycle_state, "
                        "claim_state.claim_high_water, claim_state.terminal_high_water, "
                        "next_attempt.protected_attempt_id, "
                        "next_attempt.execution_generation, "
                        "next_attempt.requirements_digest, "
                        "next_attempt.attempt_sequence, "
                        "next_head.lifecycle_state AS next_lifecycle_state, "
                        "runtime.public_attempt_count, "
                        "runtime.not_before = trial.next_attempt_at AS retry_time_bound, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts AS item "
                        "WHERE item.trial_id = trial.id) AS attempt_rows, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.protected_runtime_trial_submissions AS item "
                        "WHERE item.trial_id = trial.id) AS runtime_rows, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.protected_runtime_trial_readiness AS item "
                        "WHERE item.trial_id = trial.id) AS readiness_rows, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.executable_claim_terminal_events AS item "
                        "WHERE item.protected_attempt_id = :first_attempt_id) "
                        "AS terminal_events "
                        "FROM public.trials AS trial "
                        "JOIN public.execution_admission_reservations AS reservation "
                        "ON reservation.trial_id = trial.id "
                        "AND reservation.attempt = 1 "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS old_head "
                        "ON old_head.protected_attempt_id = :first_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_leases AS claim "
                        "ON claim.protected_attempt_id = :first_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_state AS claim_state "
                        "ON claim_state.intent_id = claim.intent_id "
                        "JOIN loom_capacity_guard.trial_attempts AS next_attempt "
                        "ON next_attempt.trial_id = trial.id "
                        "AND next_attempt.attempt_sequence = 1 "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS next_head "
                        "ON next_head.protected_attempt_id = next_attempt.protected_attempt_id "
                        "JOIN loom_capacity_guard.protected_runtime_trial_submissions AS runtime "
                        "ON runtime.trial_id = next_attempt.trial_id "
                        "AND runtime.protected_attempt_id = next_attempt.protected_attempt_id "
                        "WHERE trial.id = :trial_id"
                    ),
                    {
                        "trial_id": trial_id,
                        "first_attempt_id": first_attempt["protected_attempt_id"],
                    },
                )
                .mappings()
                .one()
            )
        assert state["state"] == "protected-pending"
        assert state["worker_id"] is None
        assert state["attempt_count"] == 1
        assert state["failure_reason"] == "env_start_failure"
        assert state["failure_message"] == "transient setup failure"
        assert state["retry_due"] is True
        assert state["reservation_state"] == "released"
        assert state["old_lifecycle_state"] == "cancelled-terminal"
        assert state["claim_high_water"] == 1
        assert state["terminal_high_water"] == 1
        assert state["attempt_sequence"] == 1
        assert state["execution_generation"] == first_attempt["execution_generation"] + 1
        assert state["requirements_digest"] == first_attempt["requirements_digest"]
        assert state["next_lifecycle_state"] == "pending-unassigned"
        assert state["public_attempt_count"] == 1
        assert state["retry_time_bound"] is True
        assert state["attempt_rows"] == 2
        assert state["runtime_rows"] == 2
        assert state["readiness_rows"] == 2
        assert state["terminal_events"] == 1

        retry_execution = seeded.bootstrap.binding.execution.model_copy(
            update={"allocation_epoch": (seeded.bootstrap.binding.execution.allocation_epoch + 1)}
        )
        retry_request = seeded.bootstrap.model_copy(
            update={
                "binding": seeded.bootstrap.binding.model_copy(
                    update={"execution": retry_execution}
                )
            }
        )
        asyncio.run(
            _assign_protected_attempt(
                capacity_guard_database,
                registration=seeded.registration,
                request=retry_request,
                protected_attempt_id=state["protected_attempt_id"],
                execution_generation=state["execution_generation"],
                requirements_digest=state["requirements_digest"],
            )
        )
        with TestClient(app) as client:
            second_claim = client.post(
                "/work/claim",
                headers=claim_headers,
                json=claim_payload,
            )
        assert second_claim.status_code == 200, second_claim.text
        assert second_claim.json()["payload"]["trial_id"] == str(trial_id)
        assert second_claim.json()["payload"]["attempt_count"] == 2
    finally:
        engine.dispose()


def test_protected_prestart_retry_rejects_wrong_credential_without_mutation(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
    )
    headers = dict(seeded.claim_headers)
    headers["X-Loom-Executor-Worker-Credential"] = "wrong-worker-credential"

    with TestClient(seeded.app) as client:
        response = client.post(
            f"/trials/{seeded.trial_id}/retry",
            headers=headers,
            json={
                "worker_id": str(seeded.worker.worker.worker_id),
                "failure_reason": "env_start_failure",
                "retry_after_sec": 0,
            },
        )

    assert response.status_code == 401
    assert response.json() == {"detail": "protected worker session rejected"}
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT trial.state, trial.worker_id, trial.attempt_count, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts AS item "
                        "WHERE item.trial_id = trial.id) AS attempt_rows, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.executable_claim_terminal_events AS item "
                        "WHERE item.protected_attempt_id = :attempt_id) AS terminal_rows "
                        "FROM public.trials AS trial WHERE trial.id = :trial_id"
                    ),
                    {
                        "trial_id": seeded.trial_id,
                        "attempt_id": seeded.first_attempt["protected_attempt_id"],
                    },
                )
                .mappings()
                .one()
            )
    finally:
        engine.dispose()
    assert dict(row) == {
        "state": "claimed",
        "worker_id": seeded.worker.worker.worker_id,
        "attempt_count": 1,
        "attempt_rows": 1,
        "terminal_rows": 0,
    }


def test_protected_prestart_retry_is_exactly_once_under_concurrency(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
    )
    retry_request = {
        "schema_version": 1,
        "trial_id": str(seeded.trial_id),
        "worker_id": str(seeded.worker.worker.worker_id),
        "failure_reason": "env_start_failure",
        "failure_message": None,
        "retry_after_sec": 0,
    }

    async def race_retry() -> list[object]:
        engine = create_async_engine(
            make_url(_value(capacity_guard_database, "runtime_url")),
            isolation_level="SERIALIZABLE",
        )
        store = ProtectedWorkerSessionStore(async_sessionmaker(engine, expire_on_commit=False))

        async def attempt_retry() -> object:
            try:
                return await store.retry_claimed_trial(
                    worker_id=seeded.worker.worker.worker_id,
                    worker_credential=_WORKER_CREDENTIAL,
                    retry_request=retry_request,
                )
            except ProtectedWorkerSessionRejected as exc:
                return exc

        try:
            return list(await asyncio.gather(attempt_retry(), attempt_retry()))
        finally:
            await engine.dispose()

    results = asyncio.run(race_retry())
    successes = [result for result in results if isinstance(result, dict)]
    losers = [result for result in results if not isinstance(result, dict)]
    assert len(successes) == 1
    assert successes[0]["state"] == "protected-pending"
    assert len(losers) == 1
    assert losers[0] is None or isinstance(
        losers[0],
        ProtectedWorkerSessionRejected,
    )

    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT trial.state, trial.worker_id, trial.attempt_count, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts AS item "
                        "WHERE item.trial_id = trial.id) AS attempt_rows, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.protected_runtime_trial_submissions AS item "
                        "WHERE item.trial_id = trial.id) AS runtime_rows, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.executable_claim_terminal_events AS item "
                        "WHERE item.protected_attempt_id = :attempt_id) AS terminal_rows "
                        "FROM public.trials AS trial WHERE trial.id = :trial_id"
                    ),
                    {
                        "trial_id": seeded.trial_id,
                        "attempt_id": seeded.first_attempt["protected_attempt_id"],
                    },
                )
                .mappings()
                .one()
            )
    finally:
        engine.dispose()
    assert dict(row) == {
        "state": "protected-pending",
        "worker_id": None,
        "attempt_count": 1,
        "attempt_rows": 2,
        "runtime_rows": 2,
        "terminal_rows": 1,
    }


def test_protected_prestart_retry_respects_attempt_quota(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
        max_attempts_ceiling=1,
    )

    with TestClient(seeded.app) as client:
        response = client.post(
            f"/trials/{seeded.trial_id}/retry",
            headers=seeded.claim_headers,
            json={
                "worker_id": str(seeded.worker.worker.worker_id),
                "failure_reason": "env_start_failure",
                "retry_after_sec": 0,
            },
        )

    assert response.status_code == 409
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT trial.state, trial.worker_id, trial.attempt_count, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts AS item "
                        "WHERE item.trial_id = trial.id) AS attempt_rows, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.executable_claim_terminal_events AS item "
                        "WHERE item.protected_attempt_id = :attempt_id) AS terminal_rows "
                        "FROM public.trials AS trial WHERE trial.id = :trial_id"
                    ),
                    {
                        "trial_id": seeded.trial_id,
                        "attempt_id": seeded.first_attempt["protected_attempt_id"],
                    },
                )
                .mappings()
                .one()
            )
    finally:
        engine.dispose()
    assert dict(row) == {
        "state": "claimed",
        "worker_id": seeded.worker.worker.worker_id,
        "attempt_count": 1,
        "attempt_rows": 1,
        "terminal_rows": 0,
    }


def test_protected_node_setup_retry_refunds_attempt_before_reclaim(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
        max_attempts_ceiling=1,
    )

    with TestClient(seeded.app) as client:
        retry = client.post(
            f"/trials/{seeded.trial_id}/retry",
            headers=seeded.claim_headers,
            json={
                "worker_id": str(seeded.worker.worker.worker_id),
                "failure_reason": "node_setup_health",
                "retry_after_sec": 0,
            },
        )
    assert retry.status_code == 200, retry.text

    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            before_reclaim = (
                connection.execute(
                    text(
                        "SELECT trial.state, trial.attempt_count, "
                        "runtime.public_attempt_count, attempt.attempt_sequence "
                        "FROM public.trials AS trial "
                        "JOIN loom_capacity_guard.trial_attempts AS attempt "
                        "ON attempt.trial_id = trial.id AND attempt.attempt_sequence = 1 "
                        "JOIN loom_capacity_guard.protected_runtime_trial_submissions AS runtime "
                        "ON runtime.trial_id = attempt.trial_id "
                        "AND runtime.protected_attempt_id = attempt.protected_attempt_id "
                        "WHERE trial.id = :trial_id"
                    ),
                    {"trial_id": seeded.trial_id},
                )
                .mappings()
                .one()
            )
    finally:
        engine.dispose()
    assert dict(before_reclaim) == {
        "state": "protected-pending",
        "attempt_count": 0,
        "public_attempt_count": 0,
        "attempt_sequence": 1,
    }

    _assign_retry_attempt(capacity_guard_database, seeded)
    with TestClient(seeded.app) as client:
        second_claim = client.post(
            "/work/claim",
            headers=seeded.claim_headers,
            json=seeded.claim_payload,
        )
    assert second_claim.status_code == 200, second_claim.text
    assert second_claim.json()["payload"]["attempt_count"] == 1


def test_protected_retry_backoff_blocks_reclaim_until_due(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
    )

    with TestClient(seeded.app) as client:
        retry = client.post(
            f"/trials/{seeded.trial_id}/retry",
            headers=seeded.claim_headers,
            json={
                "worker_id": str(seeded.worker.worker.worker_id),
                "failure_reason": "env_start_failure",
                "retry_after_sec": 600,
            },
        )
    assert retry.status_code == 200, retry.text

    with TestClient(seeded.app) as client:
        premature_claim = client.post(
            "/work/claim",
            headers=seeded.claim_headers,
            json=seeded.claim_payload,
        )
    assert premature_claim.status_code == 204, premature_claim.text

    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT trial.state, trial.worker_id, trial.attempt_count, "
                        "trial.next_attempt_at > statement_timestamp() AS retry_in_future, "
                        "head.lifecycle_state "
                        "FROM public.trials AS trial "
                        "JOIN loom_capacity_guard.trial_attempts AS attempt "
                        "ON attempt.trial_id = trial.id AND attempt.attempt_sequence = 1 "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS head "
                        "ON head.protected_attempt_id = attempt.protected_attempt_id "
                        "WHERE trial.id = :trial_id"
                    ),
                    {"trial_id": seeded.trial_id},
                )
                .mappings()
                .one()
            )
    finally:
        engine.dispose()
    assert dict(row) == {
        "state": "protected-pending",
        "worker_id": None,
        "attempt_count": 1,
        "retry_in_future": True,
        "lifecycle_state": "pending-unassigned",
    }


def test_protected_retry_never_exposes_trial_to_legacy_claim(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
    )
    with TestClient(seeded.app) as client:
        retry = client.post(
            f"/trials/{seeded.trial_id}/retry",
            headers=seeded.claim_headers,
            json={
                "worker_id": str(seeded.worker.worker.worker_id),
                "failure_reason": "env_start_failure",
                "retry_after_sec": 0,
            },
        )
    assert retry.status_code == 200, retry.text

    _seed_bearer(
        capacity_guard_database,
        raw=_WORKER_CREDENTIAL,
        token_type="worker",
        scopes=["worker:report", "worker:claim"],
    )
    monkeypatch.delenv(
        "LOOM_CP_PROTECTED_WORKER_RUNTIME_DB_URL_FILE",
        raising=False,
    )
    legacy_app = _protected_cp_app(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
        configure_runtime=False,
    )
    with TestClient(legacy_app) as client:
        legacy_claim = client.post(
            "/work/claim",
            headers={"Authorization": f"Bearer {_WORKER_CREDENTIAL}"},
            json=seeded.claim_payload,
        )
    assert legacy_claim.status_code == 204, legacy_claim.text

    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT state, worker_id, attempt_count "
                        "FROM public.trials WHERE id = :trial_id"
                    ),
                    {"trial_id": seeded.trial_id},
                )
                .mappings()
                .one()
            )
    finally:
        engine.dispose()
    assert dict(row) == {
        "state": "protected-pending",
        "worker_id": None,
        "attempt_count": 1,
    }


def test_protected_prestart_timeout_retains_claim_without_physical_terminal_evidence(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
    )
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE public.trials SET claimed_at = statement_timestamp() "
                    "- interval '2 hours', pre_start_heartbeat_at = NULL "
                    "WHERE id = :trial_id"
                ),
                {"trial_id": seeded.trial_id},
            )

        assert asyncio.run(_reclaim_expired_protected_trials(capacity_guard_database)) == 0
        assert asyncio.run(_reclaim_expired_protected_trials(capacity_guard_database)) == 0

        with engine.connect() as connection:
            state = (
                connection.execute(
                    text(
                        "SELECT trial.state, trial.worker_id, trial.attempt_count, "
                        "trial.failure_reason, "
                        "reservation.state AS reservation_state, "
                        "head.lifecycle_state, "
                        "claim_state.claim_high_water, "
                        "claim_state.terminal_high_water, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts item "
                        "WHERE item.trial_id = trial.id) AS attempt_rows, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.protected_runtime_trial_readiness item "
                        "WHERE item.trial_id = trial.id) AS readiness_rows, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.executable_claim_terminal_events item "
                        "WHERE item.protected_attempt_id = :first_attempt_id) "
                        "AS terminal_events "
                        "FROM public.trials AS trial "
                        "JOIN public.execution_admission_reservations AS reservation "
                        "ON reservation.trial_id = trial.id "
                        "AND reservation.owner_kind = 'protected_worker_claim' "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS head "
                        "ON head.protected_attempt_id = :first_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_leases AS claim "
                        "ON claim.protected_attempt_id = :first_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_state AS claim_state "
                        "ON claim_state.intent_id = claim.intent_id "
                        "WHERE trial.id = :trial_id"
                    ),
                    {
                        "trial_id": seeded.trial_id,
                        "first_attempt_id": seeded.first_attempt["protected_attempt_id"],
                    },
                )
                .mappings()
                .one()
            )
        assert dict(state) == {
            "state": "claimed",
            "worker_id": seeded.worker.worker.worker_id,
            "attempt_count": 1,
            "failure_reason": None,
            "reservation_state": "active",
            "lifecycle_state": "assigned",
            "claim_high_water": 1,
            "terminal_high_water": 0,
            "attempt_rows": 1,
            "readiness_rows": 1,
            "terminal_events": 0,
        }
    finally:
        engine.dispose()


def test_protected_prestart_timeout_at_retry_ceiling_retains_unproven_claim(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
        max_attempts_ceiling=1,
    )
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE public.trials SET claimed_at = statement_timestamp() "
                    "- interval '2 hours', pre_start_heartbeat_at = NULL "
                    "WHERE id = :trial_id"
                ),
                {"trial_id": seeded.trial_id},
            )

        assert asyncio.run(_reclaim_expired_protected_trials(capacity_guard_database)) == 0
        with engine.connect() as connection:
            state = (
                connection.execute(
                    text(
                        "SELECT trial.state, trial.worker_id, trial.failure_reason, "
                        "trial.finished_at IS NOT NULL AS finished, "
                        "reservation.state AS reservation_state, "
                        "head.lifecycle_state, claim_state.claim_high_water, "
                        "claim_state.terminal_high_water, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts item "
                        "WHERE item.trial_id = trial.id) AS attempt_rows, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.executable_claim_terminal_events event "
                        "WHERE event.protected_attempt_id = :first_attempt_id) "
                        "AS terminal_events "
                        "FROM public.trials AS trial "
                        "JOIN public.execution_admission_reservations AS reservation "
                        "ON reservation.trial_id = trial.id "
                        "AND reservation.owner_kind = 'protected_worker_claim' "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS head "
                        "ON head.protected_attempt_id = :first_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_leases AS claim "
                        "ON claim.protected_attempt_id = :first_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_state AS claim_state "
                        "ON claim_state.intent_id = claim.intent_id "
                        "WHERE trial.id = :trial_id"
                    ),
                    {
                        "trial_id": seeded.trial_id,
                        "first_attempt_id": seeded.first_attempt["protected_attempt_id"],
                    },
                )
                .mappings()
                .one()
            )
        assert dict(state) == {
            "state": "claimed",
            "worker_id": seeded.worker.worker.worker_id,
            "failure_reason": None,
            "finished": False,
            "reservation_state": "active",
            "lifecycle_state": "assigned",
            "claim_high_water": 1,
            "terminal_high_water": 0,
            "attempt_rows": 1,
            "terminal_events": 0,
        }
    finally:
        engine.dispose()


def test_protected_dead_worker_after_start_retains_claim_without_terminal_evidence(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
    )
    with TestClient(seeded.app) as client:
        running = client.patch(
            f"/trials/{seeded.trial_id}/state",
            headers=seeded.claim_headers,
            json={
                "worker_id": str(seeded.worker.worker.worker_id),
                "state": "running",
            },
        )
    assert running.status_code == 200, running.text

    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE public.workers SET last_seen_at = statement_timestamp() "
                    "- interval '2 hours' WHERE id = :worker_id"
                ),
                {"worker_id": seeded.worker.worker.worker_id},
            )

        assert asyncio.run(
            _reclaim_expired_protected_trials(
                capacity_guard_database,
                claimed_without_start_expiry_sec=None,
            )
        ) == 0

        with engine.connect() as connection:
            state = (
                connection.execute(
                    text(
                        "SELECT trial.state, trial.worker_id, trial.failure_reason, "
                        "reservation.state AS reservation_state, "
                        "head.lifecycle_state, claim_state.claim_high_water, "
                        "claim_state.terminal_high_water, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts item "
                        "WHERE item.trial_id = trial.id) AS attempt_rows, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.executable_claim_terminal_events event "
                        "WHERE event.protected_attempt_id = :first_attempt_id) "
                        "AS terminal_events "
                        "FROM public.trials AS trial "
                        "JOIN public.execution_admission_reservations AS reservation "
                        "ON reservation.trial_id = trial.id "
                        "AND reservation.owner_kind = 'protected_worker_claim' "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS head "
                        "ON head.protected_attempt_id = :first_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_leases AS claim "
                        "ON claim.protected_attempt_id = :first_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_state AS claim_state "
                        "ON claim_state.intent_id = claim.intent_id "
                        "WHERE trial.id = :trial_id"
                    ),
                    {
                        "trial_id": seeded.trial_id,
                        "first_attempt_id": seeded.first_attempt["protected_attempt_id"],
                    },
                )
                .mappings()
                .one()
            )
        assert dict(state) == {
            "state": "running",
            "worker_id": seeded.worker.worker.worker_id,
            "failure_reason": None,
            "reservation_state": "active",
            "lifecycle_state": "assigned",
            "claim_high_water": 1,
            "terminal_high_water": 0,
            "attempt_rows": 1,
            "terminal_events": 0,
        }
    finally:
        engine.dispose()


def test_protected_dead_worker_reclaims_once_after_exact_terminal_evidence(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
    )
    evidence = _terminal_inventory_evidence(seeded)
    asyncio.run(
        _import_terminal_inventory_evidence(
            capacity_guard_database,
            seeded,
            evidence,
        )
    )

    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE public.workers SET last_seen_at = statement_timestamp() "
                    "- interval '2 hours' WHERE id = :worker_id"
                ),
                {"worker_id": seeded.worker.worker.worker_id},
            )

        assert asyncio.run(
            _reclaim_expired_protected_trials(
                capacity_guard_database,
                claimed_without_start_expiry_sec=None,
            )
        ) == 1
        assert asyncio.run(
            _reclaim_expired_protected_trials(
                capacity_guard_database,
                claimed_without_start_expiry_sec=None,
            )
        ) == 0

        with engine.connect() as connection:
            state = (
                connection.execute(
                    text(
                        "SELECT trial.state, trial.worker_id, trial.attempt_count, "
                        "trial.failure_reason, "
                        "reservation.state AS reservation_state, "
                        "head.lifecycle_state, claim_state.draining, "
                        "claim_state.claim_high_water, "
                        "claim_state.terminal_high_water, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts item "
                        "WHERE item.trial_id = trial.id) AS attempt_rows, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.protected_runtime_trial_submissions item "
                        "WHERE item.trial_id = trial.id) AS submission_rows, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.executable_claim_terminal_events item "
                        "WHERE item.protected_attempt_id = :first_attempt_id) "
                        "AS terminal_events, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.executable_terminal_inventory_evidence item "
                        "WHERE item.protected_attempt_id = :first_attempt_id) "
                        "AS evidence_rows "
                        "FROM public.trials AS trial "
                        "JOIN public.execution_admission_reservations AS reservation "
                        "ON reservation.trial_id = trial.id "
                        "AND reservation.owner_kind = 'protected_worker_claim' "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS head "
                        "ON head.protected_attempt_id = :first_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_leases AS claim "
                        "ON claim.protected_attempt_id = :first_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_state AS claim_state "
                        "ON claim_state.intent_id = claim.intent_id "
                        "WHERE trial.id = :trial_id"
                    ),
                    {
                        "trial_id": seeded.trial_id,
                        "first_attempt_id": seeded.first_attempt["protected_attempt_id"],
                    },
                )
                .mappings()
                .one()
            )
            successor = (
                connection.execute(
                    text(
                        "SELECT protected_attempt_id, attempt_sequence, "
                        "execution_generation, claim_state "
                        "FROM loom_capacity_guard.trial_attempts "
                        "WHERE trial_id = :trial_id AND attempt_sequence = 1"
                    ),
                    {"trial_id": seeded.trial_id},
                )
                .mappings()
                .one()
            )
        assert dict(state) == {
            "state": "protected-pending",
            "worker_id": None,
            "attempt_count": 1,
            "failure_reason": "worker_lost_claim",
            "reservation_state": "released",
            "lifecycle_state": "cancelled-terminal",
            "draining": True,
            "claim_high_water": 1,
            "terminal_high_water": 1,
            "attempt_rows": 2,
            "submission_rows": 2,
            "terminal_events": 1,
            "evidence_rows": 1,
        }
        assert successor["protected_attempt_id"] != seeded.first_attempt[
            "protected_attempt_id"
        ]
        assert dict(successor) | {"protected_attempt_id": "successor"} == {
            "protected_attempt_id": "successor",
            "attempt_sequence": 1,
            "execution_generation": seeded.first_attempt["execution_generation"] + 1,
            "claim_state": "queued",
        }
    finally:
        engine.dispose()


def test_protected_user_cancel_retains_claim_until_worker_acknowledges_teardown(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
    )
    with TestClient(seeded.app) as client:
        cancelled = client.post(
            f"/trials/{seeded.trial_id}/cancel",
            headers={"Authorization": f"Bearer {seeded.submit_token}"},
        )
        replay = client.post(
            f"/trials/{seeded.trial_id}/cancel",
            headers={"Authorization": f"Bearer {seeded.submit_token}"},
        )
    assert cancelled.status_code == 200, cancelled.text
    assert replay.status_code == 200, replay.text
    assert cancelled.json() == {
        "trial_id": str(seeded.trial_id),
        "state": "cancellation-requested",
    }
    assert replay.json() == cancelled.json()

    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            state = (
                connection.execute(
                    text(
                        "SELECT trial.state, "
                        "trial.cancellation_requested_at IS NOT NULL AS cancellation_requested, "
                        "trial.finished_at IS NOT NULL AS finished, "
                        "reservation.state AS reservation_state, "
                        "head.lifecycle_state, claim_state.claim_high_water, "
                        "claim_state.terminal_high_water, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.executable_claim_terminal_events event "
                        "WHERE event.protected_attempt_id = :first_attempt_id) "
                        "AS terminal_events "
                        "FROM public.trials AS trial "
                        "JOIN public.execution_admission_reservations AS reservation "
                        "ON reservation.trial_id = trial.id "
                        "AND reservation.owner_kind = 'protected_worker_claim' "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS head "
                        "ON head.protected_attempt_id = :first_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_leases AS claim "
                        "ON claim.protected_attempt_id = :first_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_state AS claim_state "
                        "ON claim_state.intent_id = claim.intent_id "
                        "WHERE trial.id = :trial_id"
                    ),
                    {
                        "trial_id": seeded.trial_id,
                        "first_attempt_id": seeded.first_attempt["protected_attempt_id"],
                    },
                )
                .mappings()
                .one()
            )
        assert dict(state) == {
            "state": "claimed",
            "cancellation_requested": True,
            "finished": False,
            "reservation_state": "active",
            "lifecycle_state": "assigned",
            "claim_high_water": 1,
            "terminal_high_water": 0,
            "terminal_events": 0,
        }
    finally:
        engine.dispose()


def test_competing_protected_terminal_updates_close_claim_once(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
    )

    async def race_terminal_updates() -> list[int]:
        engine = create_async_engine(make_url(_value(capacity_guard_database, "admin_url")))
        factory = async_sessionmaker(engine, expire_on_commit=False)

        async def update_state(target: str) -> int:
            async with factory() as session:
                result = await session.execute(
                    text(
                        "UPDATE public.trials SET state = :target, "
                        "finished_at = statement_timestamp() "
                        "WHERE id = :trial_id AND state IN ('claimed', 'running') "
                        "RETURNING id"
                    ),
                    {"trial_id": seeded.trial_id, "target": target},
                )
                rows = result.all()
                await session.commit()
            return len(rows)

        try:
            return list(
                await asyncio.gather(
                    update_state("failed"),
                    update_state("cancelled"),
                )
            )
        finally:
            await engine.dispose()

    assert sorted(asyncio.run(race_terminal_updates())) == [0, 1]

    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            state = (
                connection.execute(
                    text(
                        "SELECT trial.state, reservation.state AS reservation_state, "
                        "head.lifecycle_state, claim_state.claim_high_water, "
                        "claim_state.terminal_high_water, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.executable_claim_terminal_events event "
                        "WHERE event.protected_attempt_id = :first_attempt_id) "
                        "AS terminal_events "
                        "FROM public.trials AS trial "
                        "JOIN public.execution_admission_reservations AS reservation "
                        "ON reservation.trial_id = trial.id "
                        "AND reservation.owner_kind = 'protected_worker_claim' "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS head "
                        "ON head.protected_attempt_id = :first_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_leases AS claim "
                        "ON claim.protected_attempt_id = :first_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_state AS claim_state "
                        "ON claim_state.intent_id = claim.intent_id "
                        "WHERE trial.id = :trial_id"
                    ),
                    {
                        "trial_id": seeded.trial_id,
                        "first_attempt_id": seeded.first_attempt["protected_attempt_id"],
                    },
                )
                .mappings()
                .one()
            )
        assert state["state"] in {"failed", "cancelled"}
        assert dict(state) | {"state": "terminal"} == {
            "state": "terminal",
            "reservation_state": "released",
            "lifecycle_state": "cancelled-terminal",
            "claim_high_water": 1,
            "terminal_high_water": 1,
            "terminal_events": 1,
        }
    finally:
        engine.dispose()


def test_protected_stale_running_timeout_retains_claim_without_terminal_evidence(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
    )
    with TestClient(seeded.app) as client:
        running = client.patch(
            f"/trials/{seeded.trial_id}/state",
            headers=seeded.claim_headers,
            json={
                "worker_id": str(seeded.worker.worker.worker_id),
                "state": "running",
            },
        )
    assert running.status_code == 200, running.text

    observed_at = datetime.now(UTC)
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE public.trials SET claimed_at = :started_at, "
                    "started_at = :started_at, "
                    "config = config || "
                    "'{\"agent_timeout_multiplier\": 1.0}'::jsonb "
                    "WHERE id = :trial_id"
                ),
                {
                    "trial_id": seeded.trial_id,
                    "started_at": observed_at - timedelta(hours=2),
                },
            )
            connection.execute(
                text(
                    "UPDATE public.tasks SET config = "
                    '\'{"task": {"id": "protected-timeout"}, '
                    '"agent": {"name": "opencode", '
                    '"timeout_sec": 10.0}}\'::jsonb '
                    "WHERE id = (SELECT task_id FROM public.trials "
                    "WHERE id = :trial_id)"
                ),
                {"trial_id": seeded.trial_id},
            )

        async def reclaim_stale() -> int:
            async_engine = create_async_engine(
                make_url(_value(capacity_guard_database, "admin_url"))
            )
            factory = async_sessionmaker(async_engine, expire_on_commit=False)
            try:
                async with factory() as session:
                    count = await reclaim_stale_running_trials(
                        session,
                        now=observed_at,
                        worker_heartbeat_expiry_sec=300,
                        timeout_multiplier=1.0,
                        grace_sec=10.0,
                        silence_sec=10.0,
                    )
                    await session.commit()
                return count
            finally:
                await async_engine.dispose()

        assert asyncio.run(reclaim_stale()) == 0
        with engine.connect() as connection:
            state = (
                connection.execute(
                    text(
                        "SELECT trial.state, trial.failure_reason, "
                        "reservation.state AS reservation_state, "
                        "head.lifecycle_state, claim_state.claim_high_water, "
                        "claim_state.terminal_high_water, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.executable_claim_terminal_events event "
                        "WHERE event.protected_attempt_id = :first_attempt_id) "
                        "AS terminal_events "
                        "FROM public.trials AS trial "
                        "JOIN public.execution_admission_reservations AS reservation "
                        "ON reservation.trial_id = trial.id "
                        "AND reservation.owner_kind = 'protected_worker_claim' "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS head "
                        "ON head.protected_attempt_id = :first_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_leases AS claim "
                        "ON claim.protected_attempt_id = :first_attempt_id "
                        "JOIN loom_capacity_guard.executable_claim_state AS claim_state "
                        "ON claim_state.intent_id = claim.intent_id "
                        "WHERE trial.id = :trial_id"
                    ),
                    {
                        "trial_id": seeded.trial_id,
                        "first_attempt_id": seeded.first_attempt["protected_attempt_id"],
                    },
                )
                .mappings()
                .one()
            )
        assert dict(state) == {
            "state": "running",
            "failure_reason": None,
            "reservation_state": "active",
            "lifecycle_state": "assigned",
            "claim_high_water": 1,
            "terminal_high_water": 0,
            "terminal_events": 0,
        }
    finally:
        engine.dispose()


def test_guard_0026_refuses_downgrade_with_requeueable_protected_claim(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seeded = _seed_claimed_protected_trial(
        capacity_guard_database,
        monkeypatch,
        tmp_path,
    )
    root = Path(__file__).resolve().parents[2]
    config = AlembicConfig(str(root / "capacity_guard_migrations" / "alembic.ini"))
    config.set_main_option("script_location", str(root / "capacity_guard_migrations"))
    for name, key in (
        ("LOOM_CAPACITY_GUARD_DB_URL", "migrator_url"),
        ("LOOM_CAPACITY_GUARD_OWNER_ROLE", "owner_role"),
        ("LOOM_CAPACITY_GUARD_AGENT_ROLE", "agent_role"),
        ("LOOM_CAPACITY_GUARD_EXECUTOR_ROLE", "executor_role"),
        ("LOOM_CAPACITY_GUARD_OBSERVER_ROLE", "observer_role"),
        ("LOOM_CAPACITY_GUARD_RUNTIME_ROLE", "runtime_role"),
    ):
        monkeypatch.setenv(name, _value(capacity_guard_database, key))

    with pytest.raises(
        RuntimeError,
        match="cannot downgrade guard_0026 while protected claims can be requeued",
    ):
        command.downgrade(config, "guard_0025")

    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            state = (
                connection.execute(
                    text(
                        "SELECT version.version_num, trial.state, "
                        "reservation.state AS reservation_state, "
                        "head.lifecycle_state, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.executable_claim_terminal_events event "
                        "WHERE event.protected_attempt_id = :first_attempt_id) "
                        "AS terminal_events "
                        "FROM loom_capacity_guard.capacity_guard_alembic_version version "
                        "CROSS JOIN public.trials AS trial "
                        "JOIN public.execution_admission_reservations AS reservation "
                        "ON reservation.trial_id = trial.id "
                        "AND reservation.owner_kind = 'protected_worker_claim' "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS head "
                        "ON head.protected_attempt_id = :first_attempt_id "
                        "WHERE trial.id = :trial_id"
                    ),
                    {
                        "trial_id": seeded.trial_id,
                        "first_attempt_id": seeded.first_attempt["protected_attempt_id"],
                    },
                )
                .mappings()
                .one()
            )
        assert dict(state) == {
            "version_num": "guard_0029",
            "state": "claimed",
            "reservation_state": "active",
            "lifecycle_state": "assigned",
            "terminal_events": 0,
        }
    finally:
        engine.dispose()


def test_protected_execution_attempt_route_uses_guard_credential_digest(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    asyncio.run(_seed_protected_worker(capacity_guard_database))
    _seed_public_slurm_job(capacity_guard_database)
    projection = _public_registration_payload()
    projection["supported_work_kinds"] = ["trial", "execution_attempt"]
    asyncio.run(_project_worker(capacity_guard_database, payload=projection))
    attempt = _seed_protected_execution_attempt(capacity_guard_database)
    bearer = _seed_worker_bearer(capacity_guard_database)
    app = _protected_cp_app(capacity_guard_database, monkeypatch, tmp_path)

    with TestClient(app) as client:
        response = client.post(
            f"/execution-attempts/{attempt.attempt_id}/heartbeats",
            headers={
                "Authorization": f"Bearer {bearer}",
                "X-Loom-Executor-Worker-Credential": _WORKER_CREDENTIAL,
                "X-Loom-Claim-Id": str(attempt.claim_id),
                "X-Loom-Lease-Epoch": "4",
                "X-Loom-Lease-Token": attempt.lease_token,
                "X-Loom-Request-Id": str(uuid4()),
            },
            json={
                "schema_version": "loom.execution-heartbeat.v1",
                "phase": "running",
                "monotonic_runtime_seconds": 1,
                "active_upload_session_ids": [],
            },
        )

    assert response.status_code == 200, response.text
    assert response.json()["state"] == "claimed"
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT attempt.heartbeat_phase, worker.auth_token_hash "
                    "FROM execution_attempts AS attempt "
                    "JOIN workers AS worker ON worker.id = attempt.worker_id "
                    "WHERE attempt.id = :attempt_id"
                ),
                {"attempt_id": attempt.attempt_id},
            ).one()
        assert row.heartbeat_phase == "running"
        assert (
            bytes(row.auth_token_hash)
            == hashlib.sha256(_WORKER_CREDENTIAL.encode("ascii")).digest()
        )
        assert bytes(row.auth_token_hash) != hashlib.sha256(bearer.encode("ascii")).digest()
    finally:
        engine.dispose()


def test_protected_execution_attempt_step_token_passes_guard_session(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    asyncio.run(_seed_protected_worker(capacity_guard_database))
    _seed_public_slurm_job(capacity_guard_database)
    projection = _public_registration_payload()
    projection["supported_work_kinds"] = ["trial", "execution_attempt"]
    asyncio.run(_project_worker(capacity_guard_database, payload=projection))
    attempt = _seed_protected_execution_attempt(capacity_guard_database)
    bearer = _seed_worker_bearer(capacity_guard_database)
    app = _protected_cp_app(capacity_guard_database, monkeypatch, tmp_path)

    with TestClient(app) as client:
        response = client.post(
            "/admin/step-tokens",
            headers={
                "Authorization": f"Bearer {bearer}",
                "X-Loom-Executor-Worker-Credential": _WORKER_CREDENTIAL,
                "X-Loom-Claim-Id": str(attempt.claim_id),
                "X-Loom-Lease-Epoch": "4",
                "X-Loom-Lease-Token": attempt.lease_token,
            },
            json={
                "team_id": str(attempt.team_id),
                "execution_attempt_id": str(attempt.attempt_id),
                "step_id": "generate_card_00",
                "ttl_sec": 600,
            },
        )

    assert response.status_code == 201, response.text
    ctx = verify_step_jwt(
        response.json()["token"],
        signing_key=app.state.settings.step_jwt_signing_key.get_secret_value(),
    )
    assert ctx.execution_attempt_id == attempt.attempt_id
    assert ctx.execution_attempt_lease_epoch == 4
    assert ctx.execution_spec_digest == attempt.execution_spec_digest
    assert ctx.execution_authorization_digest == attempt.execution_authorization_digest
    assert ctx.step_jwt_id is not None


@pytest.mark.parametrize(
    ("path", "credential"),
    (
        ("/trials/claim", None),
        ("/trials/claim", "wrong-protected-worker-credential"),
        ("/work/claim", None),
        ("/work/claim", "wrong-protected-worker-credential"),
    ),
)
def test_configured_protected_claim_routes_reject_missing_or_wrong_credential(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    path: str,
    credential: str | None,
) -> None:
    seeded = asyncio.run(_seed_protected_worker(capacity_guard_database))
    _seed_public_slurm_job(capacity_guard_database)
    registration_projection = _public_registration_payload()
    registration_projection["supported_work_kinds"] = ["trial", "execution_attempt"]
    projected = asyncio.run(
        _project_worker(
            capacity_guard_database,
            payload=registration_projection,
        )
    )
    bearer = _seed_worker_bearer(capacity_guard_database)
    app = _protected_cp_app(capacity_guard_database, monkeypatch, tmp_path)
    payload: dict[str, object] = {
        "worker_id": str(seeded.worker.worker_id),
        "caps": _worker_registration_request()["capabilities"],
    }
    if path == "/work/claim":
        payload = {
            "schema_version": "loom.work-claim-request.v1",
            "worker_id": str(seeded.worker.worker_id),
            "capability_snapshot_digest": projected["capability_snapshot_digest"],
            "supported_work_kinds": ["trial", "execution_attempt"],
            "free_slots": 1,
        }
    headers = {"Authorization": f"Bearer {bearer}"}
    if credential is not None:
        headers["X-Loom-Executor-Worker-Credential"] = credential

    with TestClient(app) as client:
        response = client.post(path, headers=headers, json=payload)

    assert response.status_code == 401
    assert response.json() == {"detail": "protected worker session rejected"}


@pytest.mark.parametrize("credential", (None, "wrong-protected-worker-credential"))
def test_configured_protected_heartbeat_rejects_missing_or_wrong_credential(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    credential: str | None,
) -> None:
    seeded = asyncio.run(_seed_protected_worker(capacity_guard_database))
    _seed_public_slurm_job(capacity_guard_database)
    asyncio.run(_project_worker(capacity_guard_database))
    bearer = _seed_worker_bearer(capacity_guard_database)
    app = _protected_cp_app(capacity_guard_database, monkeypatch, tmp_path)
    headers = {"Authorization": f"Bearer {bearer}"}
    if credential is not None:
        headers["X-Loom-Executor-Worker-Credential"] = credential

    with TestClient(app) as client:
        response = client.post(
            f"/workers/{seeded.worker.worker_id}/heartbeat",
            headers=headers,
            json={"status": "active"},
        )

    assert response.status_code == 401
    assert response.json() == {"detail": "protected worker session rejected"}


@pytest.mark.parametrize(
    ("method", "path", "payload"),
    (
        (
            "post",
            f"/trials/{UUID(int=900)}/retry",
            {
                "worker_id": str(_WORKER_ID),
                "failure_reason": "node_setup_health",
                "retry_after_sec": 0,
            },
        ),
        (
            "post",
            f"/trials/{UUID(int=900)}/pre-start-heartbeat",
            {"worker_id": str(_WORKER_ID)},
        ),
        (
            "patch",
            f"/trials/{UUID(int=900)}/state",
            {"worker_id": str(_WORKER_ID), "state": "running"},
        ),
        (
            "patch",
            f"/trials/{UUID(int=900)}/trajectory_index",
            {"worker_id": str(_WORKER_ID)},
        ),
        (
            "post",
            f"/trials/{UUID(int=900)}/events",
            {
                "worker_id": str(_WORKER_ID),
                "events": [
                    {
                        "seq": 0,
                        "kind": "trial_start",
                        "source": "worker",
                        "schema_version": 1,
                        "payload": {},
                    }
                ],
            },
        ),
        (
            "post",
            "/api/v1/internal/trial-cache/claim",
            {
                "cache_key": "sha256:protected-cache-key",
                "worker_id": str(_WORKER_ID),
                "ttl_sec": 60,
            },
        ),
        (
            "post",
            "/api/v1/internal/trial-cache/sha256:protected-cache-key/refresh",
            {"worker_id": str(_WORKER_ID), "ttl_sec": 60},
        ),
        (
            "put",
            f"/trials/{UUID(int=900)}/resource-usage",
            {
                "schema_version": 1,
                "trial_id": str(UUID(int=900)),
                "attempt_count": 1,
                "worker_id": str(_WORKER_ID),
                "execution_key": "f" * 64,
                "container_role": "agent",
                "role_name": "agent",
                "backend": "docker",
                "source": "docker_stats",
                "observation_seq": 0,
                "first_observed_at": "2026-09-02T00:00:00Z",
                "last_observed_at": "2026-09-02T00:00:00Z",
                "completeness": "partial",
            },
        ),
    ),
)
def test_configured_protected_body_worker_mutations_require_session_credential(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    method: str,
    path: str,
    payload: dict[str, object],
) -> None:
    asyncio.run(_seed_protected_worker(capacity_guard_database))
    _seed_public_slurm_job(capacity_guard_database)
    asyncio.run(_project_worker(capacity_guard_database))
    bearer = _seed_worker_bearer(capacity_guard_database)
    app = _protected_cp_app(capacity_guard_database, monkeypatch, tmp_path)

    with TestClient(app) as client:
        response = client.request(
            method,
            path,
            headers={"Authorization": f"Bearer {bearer}"},
            json=payload,
        )

    assert response.status_code == 401
    assert response.json() == {"detail": "protected worker session rejected"}


def test_configured_protected_query_worker_mutation_requires_session_credential(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    asyncio.run(_seed_protected_worker(capacity_guard_database))
    _seed_public_slurm_job(capacity_guard_database)
    asyncio.run(_project_worker(capacity_guard_database))
    bearer = _seed_worker_bearer(capacity_guard_database)
    app = _protected_cp_app(capacity_guard_database, monkeypatch, tmp_path)

    with TestClient(app) as client:
        response = client.delete(
            "/api/v1/internal/trial-cache/sha256:protected-cache-key",
            headers={"Authorization": f"Bearer {bearer}"},
            params={"worker_id": str(_WORKER_ID)},
        )

    assert response.status_code == 401
    assert response.json() == {"detail": "protected worker session rejected"}


def test_configured_protected_mixed_principal_routes_reject_worker_without_credential(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    asyncio.run(_seed_protected_worker(capacity_guard_database))
    _seed_public_slurm_job(capacity_guard_database)
    asyncio.run(_project_worker(capacity_guard_database))
    bearer = _seed_worker_bearer(capacity_guard_database)
    app = _protected_cp_app(capacity_guard_database, monkeypatch, tmp_path)
    headers = {"Authorization": f"Bearer {bearer}"}

    with TestClient(app) as client:
        responses = (
            client.post(
                "/admin/step-tokens",
                headers=headers,
                json={
                    "team_id": str(UUID(int=901)),
                    "trial_id": str(UUID(int=900)),
                    "step_id": "main",
                    "ttl_sec": 60,
                },
            ),
            client.post(
                f"/trials/{UUID(int=900)}/terminus/reclaim",
                headers=headers,
                json={"step_id": "main", "worker_id": str(_WORKER_ID)},
            ),
            client.post(
                f"/trials/{UUID(int=900)}/terminus/episode-checkpoints",
                headers=headers,
                json={
                    "execution_id": str(UUID(int=902)),
                    "run_attempt_id": str(UUID(int=903)),
                    "episode": 1,
                    "active_role": "main",
                    "last_call_ordinal": 0,
                    "last_seq": 0,
                },
            ),
        )

    assert [response.status_code for response in responses] == [401, 401, 401]
    assert all(
        response.json() == {"detail": "protected worker session rejected"} for response in responses
    )


def test_configured_protected_mixed_principal_routes_preserve_non_worker_callers(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    asyncio.run(_seed_protected_worker(capacity_guard_database))
    family_bearer = _seed_bearer(
        capacity_guard_database,
        raw="protected-test-family-orchestrator",
        token_type="family_orchestrator",
        scopes=["family:cancel", "family:evolve"],
    )
    team_bearer = _seed_bearer(
        capacity_guard_database,
        raw="protected-test-team-reader",
        token_type="team",
        scopes=["read:own"],
    )
    app = _protected_cp_app(capacity_guard_database, monkeypatch, tmp_path)

    with TestClient(app) as client:
        responses = (
            client.post(
                "/admin/step-tokens",
                headers={"Authorization": f"Bearer {family_bearer}"},
                json={
                    "team_id": str(UUID(int=901)),
                    "trial_id": str(UUID(int=900)),
                    "step_id": "family_evolver",
                    "ttl_sec": 60,
                    "provider_connection_id": None,
                },
            ),
            client.post(
                f"/trials/{UUID(int=900)}/terminus/reclaim",
                headers={"Authorization": f"Bearer {team_bearer}"},
                json={"step_id": "main"},
            ),
            client.post(
                f"/trials/{UUID(int=900)}/terminus/episode-checkpoints",
                headers={"Authorization": f"Bearer {team_bearer}"},
                json={
                    "execution_id": str(UUID(int=902)),
                    "run_attempt_id": str(UUID(int=903)),
                    "episode": 1,
                    "active_role": "main",
                    "last_call_ordinal": 0,
                    "last_seq": 0,
                },
            ),
        )

    assert [response.status_code for response in responses] == [404, 404, 404]
    assert all(response.json() == {"detail": "trial not found"} for response in responses)
