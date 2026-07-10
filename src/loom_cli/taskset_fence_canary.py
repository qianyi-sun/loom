"""Deployment-only cooperative TaskSet lease-fencing canary helpers.

This module deliberately has no public ``loom`` command or HTTP route.  The
cluster rollout command invokes its internal mode inside the deployed
``loom-service`` container, where it can reuse the normal materializer's
database and object-store configuration.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import os
import re
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import (
    Task,
    TaskSet,
    TaskSetFenceCanaryAuthorization,
    TaskSetManifest,
    TaskSetMaterializationJob,
)
from loom.taskset.materialize import MaterializeOutput
from loom.taskset.transform_sandbox import TransformSandboxConfig
from loom_service import taskset_materializer
from loom_service.config import LoomServiceSettings
from loom_service.storage import create_minio_client

_SHA40_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA64_RE = re.compile(r"^[0-9a-f]{64}$")
_TASK_SET_ID_RE = re.compile(
    r"^ts/(?P<team_id>[0-9a-f-]{36})/"
    r"(?P<slug>[a-z0-9](?:[a-z0-9_-]*[a-z0-9])?)$",
)
_NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{43}$")
_ALLOWED_CONTRACT_FIELDS = frozenset({
    "candidate_sha",
    "image_tag",
    "task_set_id",
    "expected_task_checksum",
    "authorization_token",
    "nonce",
})


class TaskSetFenceCanaryContractError(ValueError):
    """A deployment canary contract failed its non-sensitive validation."""


class TaskSetFenceCanaryAuthorizationError(PermissionError):
    """The deployment-only capability did not authorize this handoff."""


@dataclass(frozen=True, slots=True)
class TaskSetFenceCanaryContract:
    """The only values a deployment-side fence handoff may accept."""

    candidate_sha: str
    image_tag: str
    task_set_id: str
    expected_task_checksum: str
    authorization_token: str
    nonce: str

    @classmethod
    def from_mapping(
        cls,
        payload: Mapping[str, Any],
    ) -> TaskSetFenceCanaryContract:
        if set(payload) != _ALLOWED_CONTRACT_FIELDS:
            raise TaskSetFenceCanaryContractError("invalid canary contract fields")

        candidate_sha = payload.get("candidate_sha")
        image_tag = payload.get("image_tag")
        if (
            not isinstance(candidate_sha, str)
            or not _SHA40_RE.fullmatch(candidate_sha)
            or not isinstance(image_tag, str)
            or image_tag != f"staging-{candidate_sha[:7]}"
        ):
            raise TaskSetFenceCanaryContractError("invalid candidate identity")

        task_set_id = payload.get("task_set_id")
        try:
            task_set_id_text = str(task_set_id)
            match = _TASK_SET_ID_RE.fullmatch(task_set_id_text)
            if match is None:
                raise ValueError("invalid TaskSet id")
            UUID(match.group("team_id"))
        except (TypeError, ValueError, AttributeError) as exc:
            raise TaskSetFenceCanaryContractError("invalid disposable task set") from exc

        expected_task_checksum = payload.get("expected_task_checksum")
        if (
            not isinstance(expected_task_checksum, str)
            or not _SHA64_RE.fullmatch(expected_task_checksum)
        ):
            raise TaskSetFenceCanaryContractError("invalid expected task checksum")

        authorization_token = payload.get("authorization_token")
        if not isinstance(authorization_token, str) or not authorization_token:
            raise TaskSetFenceCanaryContractError("missing canary authorization")

        nonce = payload.get("nonce")
        if not isinstance(nonce, str) or not _NONCE_RE.fullmatch(nonce):
            raise TaskSetFenceCanaryContractError("invalid canary nonce")

        return cls(
            candidate_sha=candidate_sha,
            image_tag=image_tag,
            task_set_id=task_set_id_text,
            expected_task_checksum=expected_task_checksum,
            authorization_token=authorization_token,
            nonce=nonce,
        )


def validate_contract_authorization(
    contract: TaskSetFenceCanaryContract,
    *,
    configured_token: str | None,
) -> None:
    """Require the separate capability mounted in the service Deployment."""
    if not configured_token or not hmac.compare_digest(
        contract.authorization_token,
        configured_token,
    ):
        raise TaskSetFenceCanaryAuthorizationError("canary authorization rejected")


class TaskSetFenceCanaryRuntimeError(RuntimeError):
    """A safe deployment canary precondition or invariant was not met."""


def _owner_fingerprint(owner: str) -> str:
    return f"sha256:{hashlib.sha256(owner.encode()).hexdigest()[:12]}"


def _utc_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _new_owner() -> str:
    from uuid import uuid4

    return f"taskset-fence-canary-{uuid4()}"


def _nonce_digest(contract: TaskSetFenceCanaryContract) -> bytes:
    return hashlib.sha256(contract.nonce.encode("ascii")).digest()


def _is_initial_disposable_job(
    *,
    task_set: TaskSet | None,
    manifest: TaskSetManifest | None,
    jobs: list[TaskSetMaterializationJob],
    task_exists: bool,
) -> bool:
    if (
        task_set is None
        or manifest is None
        or task_set.soft_deleted_at is not None
        or task_set.status != "materializing"
        or task_set.task_count != 0
        or task_set.evaluation_ready
        or task_exists
        or len(jobs) != 1
    ):
        return False
    job = jobs[0]
    return not (
        job.state != "queued"
        or job.attempt_count != 0
        or job.lease_epoch != 0
        or job.published_materialization_generation != 0
        or job.claimed_by is not None
        or job.claimed_at is not None
        or job.lease_heartbeat_at is not None
        or job.started_at is not None
        or job.finished_at is not None
        or job.next_attempt_at is not None
    )


async def _locked_disposable_rows(
    session: AsyncSession,
    *,
    task_set_id: str,
) -> tuple[
    TaskSet | None,
    TaskSetManifest | None,
    list[TaskSetMaterializationJob],
    bool,
]:
    """Lock mutable job/TaskSet rows in the materializer's job-first order."""
    jobs = (await session.execute(
        select(TaskSetMaterializationJob)
        .where(TaskSetMaterializationJob.task_set_id == task_set_id)
        .order_by(TaskSetMaterializationJob.enqueued_at)
        .with_for_update(),
    )).scalars().all()
    task_set = (await session.execute(
        select(TaskSet)
        .where(TaskSet.id == task_set_id)
        .with_for_update(),
    )).scalar_one_or_none()
    manifest = (await session.execute(
        select(TaskSetManifest)
        .where(TaskSetManifest.task_set_id == task_set_id)
        .with_for_update(),
    )).scalar_one_or_none()
    task_exists = (await session.execute(
        select(Task.id).where(Task.task_set_id == task_set_id).limit(1),
    )).scalar_one_or_none() is not None
    return task_set, manifest, list(jobs), task_exists


async def register_deployment_fence_authorization(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    contract: TaskSetFenceCanaryContract,
    configured_token: str | None,
) -> None:
    """Persist one service-authorized canary binding before it can be claimed.

    This entrypoint is only reached by the deployment-only internal mode after
    the service-mounted capability has been compared.  The nonce is retained
    only as a digest, so a durable row is neither a bearer credential nor a
    reusable TaskSet control surface.
    """
    validate_contract_authorization(contract, configured_token=configured_token)
    async with session_factory() as session:
        task_set, manifest, jobs, task_exists = await _locked_disposable_rows(
            session,
            task_set_id=contract.task_set_id,
        )
        authorization = (await session.execute(
            select(TaskSetFenceCanaryAuthorization)
            .where(TaskSetFenceCanaryAuthorization.task_set_id == contract.task_set_id)
            .with_for_update(),
        )).scalar_one_or_none()
        if not _is_initial_disposable_job(
            task_set=task_set,
            manifest=manifest,
            jobs=jobs,
            task_exists=task_exists,
        ):
            await session.rollback()
            raise TaskSetFenceCanaryRuntimeError("canary task set is not disposable")
        if authorization is not None:
            await session.rollback()
            raise TaskSetFenceCanaryRuntimeError("canary authorization was not available")
        session.add(TaskSetFenceCanaryAuthorization(
            task_set_id=contract.task_set_id,
            materialization_job_id=jobs[0].id,
            candidate_sha=contract.candidate_sha,
            image_tag=contract.image_tag,
            expected_task_checksum=contract.expected_task_checksum,
            nonce_digest=_nonce_digest(contract),
        ))
        await session.commit()


async def _claim_authorized_disposable_job(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    contract: TaskSetFenceCanaryContract,
    owner: str,
) -> taskset_materializer.MaterializationLease:
    """Consume one durable binding and claim exactly its fresh queued job.

    The authorization and job transition commit together while every mutable
    TaskSet row is locked.  A user-created TaskSet cannot reach this path
    without the deployment-created authorization row and matching nonce.
    """
    async with session_factory() as session:
        task_set, manifest, jobs, task_exists = await _locked_disposable_rows(
            session,
            task_set_id=contract.task_set_id,
        )
        authorization = (await session.execute(
            select(TaskSetFenceCanaryAuthorization)
            .where(TaskSetFenceCanaryAuthorization.task_set_id == contract.task_set_id)
            .with_for_update(),
        )).scalar_one_or_none()
        expected_nonce_digest = _nonce_digest(contract)
        if (
            authorization is None
            or authorization.consumed_at is not None
            or authorization.candidate_sha != contract.candidate_sha
            or authorization.image_tag != contract.image_tag
            or authorization.expected_task_checksum != contract.expected_task_checksum
            or not hmac.compare_digest(authorization.nonce_digest, expected_nonce_digest)
            or len(jobs) != 1
            or authorization.materialization_job_id != jobs[0].id
            or not _is_initial_disposable_job(
                task_set=task_set,
                manifest=manifest,
                jobs=jobs,
                task_exists=task_exists,
            )
        ):
            await session.rollback()
            raise TaskSetFenceCanaryRuntimeError("canary authorization was not available")

        now = datetime.now(UTC)
        claimed = (await session.execute(
            update(TaskSetMaterializationJob)
            .where(
                TaskSetMaterializationJob.id == authorization.materialization_job_id,
                TaskSetMaterializationJob.state == "queued",
                TaskSetMaterializationJob.attempt_count == 0,
                TaskSetMaterializationJob.lease_epoch == 0,
                TaskSetMaterializationJob.next_attempt_at.is_(None),
            )
            .values(
                state="claimed",
                claimed_at=now,
                claimed_by=owner,
                lease_epoch=1,
                lease_heartbeat_at=now,
                attempt_count=1,
                updated_at=now,
            )
            .returning(
                TaskSetMaterializationJob.id,
                TaskSetMaterializationJob.lease_epoch,
            ),
        )).one_or_none()
        if claimed is None:
            await session.rollback()
            raise TaskSetFenceCanaryRuntimeError("canary authorization was not available")
        consumed = (await session.execute(
            update(TaskSetFenceCanaryAuthorization)
            .where(
                TaskSetFenceCanaryAuthorization.task_set_id == contract.task_set_id,
                TaskSetFenceCanaryAuthorization.consumed_at.is_(None),
                TaskSetFenceCanaryAuthorization.materialization_job_id == claimed.id,
                TaskSetFenceCanaryAuthorization.nonce_digest == expected_nonce_digest,
            )
            .values(consumed_at=now, consumed_lease_epoch=claimed.lease_epoch)
            .returning(TaskSetFenceCanaryAuthorization.task_set_id),
        )).one_or_none()
        if consumed is None:
            await session.rollback()
            raise TaskSetFenceCanaryRuntimeError("canary authorization was not available")
        await session.commit()
    return taskset_materializer.MaterializationLease(
        job_id=claimed.id,
        lease_epoch=claimed.lease_epoch,
        claimed_by=owner,
    )


async def _claim_exact_job(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    job_id: UUID,
    owner: str,
) -> taskset_materializer.MaterializationLease:
    """Claim the canary's already-authorized job for its second owner only."""
    async with session_factory() as session:
        leases = await taskset_materializer._claim_jobs(
            session,
            batch_size=1,
            claimed_by=owner,
            job_id=job_id,
        )
    if len(leases) != 1:
        raise TaskSetFenceCanaryRuntimeError("canary lease claim was not available")
    return leases[0]


async def _relinquish_current_lease(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    lease: taskset_materializer.MaterializationLease,
) -> None:
    """Return an interrupted canary lease to normal materialization safely."""
    try:
        async with session_factory() as session:
            await taskset_materializer.relinquish_if_current(session, lease=lease)
    except taskset_materializer.LeaseLost:
        # A delete, reclaim, or winner already revoked this lease.  It cannot
        # remain a running canary job, so there is nothing safe to mutate.
        return


async def _stage_lease(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    lease: taskset_materializer.MaterializationLease,
    minio_client: Any,
    artifacts_bucket: str,
    upstream_cache_root: Path,
    transform_config: TransformSandboxConfig,
    max_bundle_bytes: int | None,
    max_team_storage_bytes: int | None,
    claim_ttl_sec: int,
) -> tuple[str, MaterializeOutput]:
    staged = await taskset_materializer.stage_claimed_job(
        session_factory,
        job_id=lease.job_id,
        lease=lease,
        minio_client=minio_client,
        artifacts_bucket=artifacts_bucket,
        upstream_cache_root=upstream_cache_root,
        transform_config=transform_config,
        max_bundle_bytes=max_bundle_bytes,
        max_team_storage_bytes=max_team_storage_bytes,
        claim_ttl_sec=claim_ttl_sec,
    )
    if staged is None:
        raise TaskSetFenceCanaryRuntimeError("canary materialization did not stage")
    _staged_lease, task_set_id, output = staged
    if _staged_lease != lease:
        raise TaskSetFenceCanaryRuntimeError("canary staged an unexpected lease")
    return task_set_id, output


def _matches_disposable_output(
    output: MaterializeOutput,
    *,
    expected_checksum: str,
) -> bool:
    return (
        output.task_count == 1
        and len(output.task_rows) == 1
        and output.task_rows[0].checksum == expected_checksum
    )


async def run_deployment_fence_canary(
    contract: TaskSetFenceCanaryContract,
    *,
    configured_token: str | None,
    session_factory: async_sessionmaker[AsyncSession],
    minio_client: Any,
    artifacts_bucket: str,
    upstream_cache_root: Path,
    transform_config: TransformSandboxConfig,
    max_bundle_bytes: int | None,
    max_team_storage_bytes: int | None,
    claim_ttl_sec: int,
    owner_factory: Callable[[], str] = _new_owner,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, object]:
    """Collect one authorized two-owner handoff using only materializer CASes.

    It deliberately targets exactly one fresh, queued TaskSet job and has no
    object-store cleanup, process control, public API, or generic pause path.
    A race with the normal materializer fails closed before an evidence record
    is emitted.
    """
    validate_contract_authorization(contract, configured_token=configured_token)
    owner_a = owner_factory()
    owner_b = owner_factory()
    if not (
        isinstance(owner_a, str)
        and owner_a
        and isinstance(owner_b, str)
        and owner_b
        and owner_a != owner_b
    ):
        raise TaskSetFenceCanaryRuntimeError("canary owners were not available")

    active_lease: taskset_materializer.MaterializationLease | None = None
    try:
        lease_a = await _claim_authorized_disposable_job(
            session_factory,
            contract=contract,
            owner=owner_a,
        )
        active_lease = lease_a
        staged_a_task_set_id, output_a = await _stage_lease(
            session_factory,
            lease=lease_a,
            minio_client=minio_client,
            artifacts_bucket=artifacts_bucket,
            upstream_cache_root=upstream_cache_root,
            transform_config=transform_config,
            max_bundle_bytes=max_bundle_bytes,
            max_team_storage_bytes=max_team_storage_bytes,
            claim_ttl_sec=claim_ttl_sec,
        )
        if staged_a_task_set_id != contract.task_set_id:
            raise TaskSetFenceCanaryRuntimeError("canary staged an unexpected task set")
        if not _matches_disposable_output(
            output_a,
            expected_checksum=contract.expected_task_checksum,
        ):
            raise TaskSetFenceCanaryRuntimeError("canary staged output did not match contract")
        a_staged_at = now()

        await _relinquish_current_lease(session_factory, lease=lease_a)
        active_lease = None
        lease_b = await _claim_exact_job(
            session_factory,
            job_id=lease_a.job_id,
            owner=owner_b,
        )
        active_lease = lease_b
        staged_b_task_set_id, output_b = await _stage_lease(
            session_factory,
            lease=lease_b,
            minio_client=minio_client,
            artifacts_bucket=artifacts_bucket,
            upstream_cache_root=upstream_cache_root,
            transform_config=transform_config,
            max_bundle_bytes=max_bundle_bytes,
            max_team_storage_bytes=max_team_storage_bytes,
            claim_ttl_sec=claim_ttl_sec,
        )
        if staged_b_task_set_id != contract.task_set_id:
            raise TaskSetFenceCanaryRuntimeError("canary staged an unexpected task set")
        if not _matches_disposable_output(
            output_b,
            expected_checksum=contract.expected_task_checksum,
        ):
            raise TaskSetFenceCanaryRuntimeError("canary staged output did not match contract")
        async with session_factory() as session:
            await taskset_materializer.publish_if_current(
                session,
                lease=lease_b,
                task_set_id=contract.task_set_id,
                output=output_b,
                claim_ttl_sec=claim_ttl_sec,
            )
        active_lease = None
        b_published_at = now()

        async with session_factory() as session:
            winner_job = await session.get(TaskSetMaterializationJob, lease_a.job_id)
            winner_rows = (await session.execute(
                select(Task).where(Task.task_set_id == contract.task_set_id),
            )).scalars().all()
        if (
            winner_job is None
            or winner_job.state != "succeeded"
            or winner_job.lease_epoch != lease_b.lease_epoch
            or winner_job.published_materialization_generation != lease_b.lease_epoch
            or len(winner_rows) != 1
            or winner_rows[0].checksum != contract.expected_task_checksum
        ):
            raise TaskSetFenceCanaryRuntimeError("canary winner did not match contract")

        try:
            async with session_factory() as session:
                await taskset_materializer.publish_if_current(
                    session,
                    lease=lease_a,
                    task_set_id=contract.task_set_id,
                    output=output_a,
                    claim_ttl_sec=claim_ttl_sec,
                )
        except taskset_materializer.LeaseLost:
            pass
        else:
            raise TaskSetFenceCanaryRuntimeError("canary stale lease was not fenced")
        a_lease_lost_at = now()
    finally:
        if active_lease is not None:
            await _relinquish_current_lease(session_factory, lease=active_lease)

    return {
        "schema_version": 1,
        "candidate_sha": contract.candidate_sha,
        "image_tag": contract.image_tag,
        "task_set_id": contract.task_set_id,
        "winner": {
            "job_id": str(lease_b.job_id),
            "lease_epoch": lease_b.lease_epoch,
            "owner_fingerprint": _owner_fingerprint(owner_b),
            "published_generation": winner_job.published_materialization_generation,
            "outcome": "published",
        },
        "loser": {
            "job_id": str(lease_a.job_id),
            "lease_epoch": lease_a.lease_epoch,
            "owner_fingerprint": _owner_fingerprint(owner_a),
            "outcome": "fenced_before_publish",
            "gc_eligible": True,
        },
        "published_task": {
            "task_count": len(winner_rows),
            "checksum": winner_rows[0].checksum,
        },
        "stale_cas_outcome": "LeaseLost",
        "timestamps": {
            "a_staged_at": _utc_timestamp(a_staged_at),
            "b_published_at": _utc_timestamp(b_published_at),
            "a_lease_lost_at": _utc_timestamp(a_lease_lost_at),
        },
    }


async def _run_internal_contract(
    contract: TaskSetFenceCanaryContract,
    *,
    settings: LoomServiceSettings,
    authorize_only: bool = False,
) -> dict[str, object]:
    configured_token = (
        settings.taskset_fence_canary_token.get_secret_value()
        if settings.taskset_fence_canary_token is not None
        else None
    )
    if not configured_token:
        raise TaskSetFenceCanaryAuthorizationError("canary authorization rejected")
    engine = create_async_engine(
        settings.db_engine_url,
        connect_args=settings.db_engine_connect_args,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        if authorize_only:
            await register_deployment_fence_authorization(
                session_factory,
                contract=contract,
                configured_token=configured_token,
            )
            return {"status": "authorized"}
        return await run_deployment_fence_canary(
            contract,
            configured_token=configured_token,
            session_factory=session_factory,
            minio_client=create_minio_client(settings, endpoint_url=settings.minio_endpoint),
            artifacts_bucket=settings.artifacts_bucket,
            upstream_cache_root=settings.taskset_materializer_upstream_cache_root,
            transform_config=TransformSandboxConfig(
                enabled=settings.taskset_materializer_transforms_enabled,
                network_isolated=settings.taskset_materializer_transform_network_isolated,
                workload_contract=settings.workload_contract,
                wall_timeout_sec=settings.taskset_materializer_transform_wall_timeout_sec,
                cpu_limit_sec=settings.taskset_materializer_transform_cpu_limit_sec,
                memory_limit_mb=settings.taskset_materializer_transform_memory_limit_mb,
            ),
            max_bundle_bytes=settings.taskset_quota_max_bundle_bytes,
            max_team_storage_bytes=settings.taskset_quota_max_storage_bytes_per_team,
            claim_ttl_sec=settings.taskset_materializer_claim_ttl_sec,
        )
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None) -> int:
    """Internal-only process entrypoint invoked through ``kubectl exec``."""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--internal", action="store_true")
    parser.add_argument("--authorize", action="store_true")
    try:
        args = parser.parse_args(argv)
    except SystemExit:
        return 2
    if not args.internal or os.environ.get("LOOM_ENV") != "staging":
        sys.stderr.write("error: deployment canary runner unavailable\n")
        return 2
    try:
        payload = json.loads(sys.stdin.read())
        if not isinstance(payload, Mapping):
            raise TaskSetFenceCanaryContractError("invalid canary contract fields")
        contract = TaskSetFenceCanaryContract.from_mapping(payload)
        evidence = asyncio.run(_run_internal_contract(
            contract,
            settings=LoomServiceSettings(),
            authorize_only=args.authorize,
        ))
    except (
        TaskSetFenceCanaryContractError,
        TaskSetFenceCanaryAuthorizationError,
        TaskSetFenceCanaryRuntimeError,
        ValueError,
        json.JSONDecodeError,
    ):
        sys.stderr.write("error: deployment canary runner rejected contract\n")
        return 2
    except Exception:
        sys.stderr.write("error: deployment canary runner failed\n")
        return 1
    sys.stdout.write(json.dumps(evidence, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
