"""State-first execution transactions; no signing I/O under database locks."""

from __future__ import annotations

import hmac
from dataclasses import dataclass
from uuid import UUID

import rfc8785
from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import TaskImageExecutionStart, TaskImagePublicationState, Trial, Worker
from loom.models.worker_capabilities import WorkerCapabilitySnapshotV1
from loom.pipeline.keys import canonical_digest, canonical_document
from loom_task_image_authority.execution_grant import LegacyExecutionClaim, decode_execution_claim
from loom_task_image_authority.publication_keyset_store import _transaction
from loom_task_image_authority.publication_signing import PublicationState

EXECUTION_READER_FEATURE = "task-image-execution-v2"


@dataclass(frozen=True)
class LockedExecutionClaim:
    """Snapshot valid only in the caller's current lock-holding transaction."""

    claim: LegacyExecutionClaim
    publication_state: PublicationState
    task_id: str
    cpu_arch: str


async def lock_execution_claim(
    session: AsyncSession, *, claim: LegacyExecutionClaim, worker_token_hash: bytes,
) -> LockedExecutionClaim:
    """Lock publication state -> worker -> Trial, matching claim worker/Trial order.

    The API must first authenticate the token and its scope. This independently
    binds that actual token to the current registered worker and durable claim.
    No request-body capability, retained UUID or refundable counter is authority.
    Errors require rollback; the result is neither a signed grant nor a start.
    """
    if type(claim) is not LegacyExecutionClaim or type(worker_token_hash) is not bytes or len(worker_token_hash) != 32:
        raise ValueError("invalid execution claim authentication")
    checked = decode_execution_claim(rfc8785.dumps(claim.model_dump(mode="json", exclude_none=True)))
    if not isinstance(checked, LegacyExecutionClaim):
        raise ValueError("legacy execution claim required")
    await _transaction(session)
    state = await session.scalar(select(TaskImagePublicationState)
        .where(TaskImagePublicationState.singleton_id == 1)
        .execution_options(populate_existing=True).with_for_update())
    if state is None:
        raise ValueError("publication authority unavailable")
    worker = await session.scalar(select(Worker).where(Worker.id == UUID(checked.worker_id))
        .execution_options(populate_existing=True).with_for_update())
    if (
        worker is None or worker.status != "active" or worker.drain_state != "active"
        or worker.lease_epoch != checked.worker_lease_epoch
        or worker.auth_token_hash is None
        or not hmac.compare_digest(bytes(worker.auth_token_hash), worker_token_hash)
        or worker.supported_work_kinds != ["trial", "execution_attempt"]
        or worker.capability_snapshot_json is None
    ):
        raise ValueError("execution worker is stale or unauthenticated")
    snapshot = WorkerCapabilitySnapshotV1.model_validate_json(
        canonical_document(worker.capability_snapshot_json)
    )
    if (
        EXECUTION_READER_FEATURE not in snapshot.container_runtime_features
        or canonical_digest(snapshot.model_dump(mode="json")) != worker.capability_snapshot_digest
    ):
        raise ValueError("worker execution reader capability is absent or changed")
    trial = await session.scalar(select(Trial).where(Trial.id == UUID(checked.trial_id))
        .execution_options(populate_existing=True).with_for_update())
    if (
        trial is None or trial.state != "claimed" or trial.started_at is not None
        or trial.cancellation_requested_at is not None
        or trial.worker_id != worker.id or trial.team_id != UUID(checked.team_id)
        or trial.legacy_claim_id != UUID(checked.claim_id)
        or trial.attempt_count != checked.trial_attempt_count
    ):
        raise ValueError("execution trial claim is stale or no longer pre-start")
    consumed = await session.scalar(select(exists().where(
        TaskImageExecutionStart.claim_id == UUID(checked.claim_id),
    )))
    if consumed:
        raise ValueError("execution claim has already consumed its one-use start")
    return LockedExecutionClaim(
        checked, PublicationState(state.revocation_epoch, state.keyset_version),
        trial.task_id, snapshot.cpu_arch,
    )
