"""State-first execution transactions; no signing I/O under database locks."""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import rfc8785
from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    TaskImageExecutionGrant,
    TaskImageExecutionStart,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImagePublicationEnvelope,
    TaskImagePublicationJob,
    TaskImagePublicationState,
    Trial,
    TrialTaskImageMaterialization,
    Worker,
)
from loom.models.worker_capabilities import WorkerCapabilitySnapshotV1
from loom.pipeline.keys import canonical_digest, canonical_document
from loom.task_image_materialization import admit_task_image_source
from loom_task_image_authority.contracts import BuildPurpose
from loom_task_image_authority.execution_delivery import TaskImageExecutionDelivery
from loom_task_image_authority.execution_grant import (
    MAX_EXECUTION_GRANT_BYTES,
    MAX_EXECUTION_GRANT_ENVELOPE_BYTES,
    ExecutionGrantEnvelope,
    LegacyExecutionClaim,
    TaskImageExecutionGrantV2,
    VerifiedExecutionGrant,
    _decode,
    canonical_execution_grant_bytes,
    decode_execution_claim,
    verify_execution_grant,
)
from loom_task_image_authority.execution_signing_request import ExecutionSigningRequest
from loom_task_image_authority.execution_start import ExecutionStartReceipt, ExecutionStartRequest
from loom_task_image_authority.publication_completion import replay_completed_publication
from loom_task_image_authority.publication_contracts import (
    PublicationEnvelope,
    canonical_publication_bytes,
    decode_publication_statement,
)
from loom_task_image_authority.publication_keyset import ExecutionGrantTrustRoot, _instant
from loom_task_image_authority.publication_keyset_store import (
    StoredPublicationKeyset,
    _transaction,
    read_keyset,
)
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
        or not any(cap.get("cpu_arch", "x86_64") == snapshot.cpu_arch for cap in worker.capabilities)
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
        or trial.requires_caps.get("cpu_arch", snapshot.cpu_arch) != snapshot.cpu_arch
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


def _clock() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _stamp(value: datetime) -> str:
    if value.utcoffset() != timedelta(0) or value.microsecond:
        raise ValueError("execution authority clock must be whole UTC seconds")
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class _Inputs:
    image: TaskImageMaterialization
    operation_id: UUID
    plan: bytes
    publications: tuple[bytes, ...]
    keyset: StoredPublicationKeyset


async def _inputs(
    session: AsyncSession, authority: LockedExecutionClaim, root: ExecutionGrantTrustRoot,
    clock: Callable[[], datetime],
) -> _Inputs:
    images = (await session.scalars(select(TaskImageMaterialization)
        .join(TrialTaskImageMaterialization, TrialTaskImageMaterialization.materialization_id == TaskImageMaterialization.id)
        .where(TrialTaskImageMaterialization.trial_id == UUID(authority.claim.trial_id),
               TaskImageMaterialization.task_id == authority.task_id,
               TaskImageMaterialization.cpu_arch == authority.cpu_arch)
        .order_by(TaskImageMaterialization.id).limit(2)
        .execution_options(populate_existing=True).with_for_update(of=TaskImageMaterialization))).all()
    if len(images) != 1:
        raise ValueError("execution requires one exact architecture materialization")
    image = images[0]
    if image.state != "ready" or image.ready_publication_operation_id is None:
        raise ValueError("execution materialization is not signed-ready")
    if await admit_task_image_source(session, row=image) is None:
        raise ValueError("execution requires retained strong source authority")
    operation_id = image.ready_publication_operation_id
    await replay_completed_publication(session, operation_id=operation_id)
    job = await session.get(TaskImagePublicationJob, operation_id, populate_existing=True)
    if job is None or job.materialization_id != image.id:
        raise ValueError("execution publication operation differs from materialization")
    attempt = await session.get(TaskImageMaterializationAttempt, job.materialization_attempt_id, populate_existing=True)
    if attempt is None or attempt.claim_plan_json is None:
        raise ValueError("execution historical build plan is absent")
    plan = rfc8785.dumps(attempt.claim_plan_json)
    if hashlib.sha256(plan).hexdigest() != attempt.claim_plan_sha256:
        raise ValueError("execution historical plan digest differs")
    envelopes = (await session.scalars(select(TaskImagePublicationEnvelope)
        .where(TaskImagePublicationEnvelope.materialization_attempt_id == attempt.id)
        .order_by(TaskImagePublicationEnvelope.component != "task", TaskImagePublicationEnvelope.component)
        .limit(129).execution_options(populate_existing=True))).all()
    if not 1 <= len(envelopes) <= 128:
        raise ValueError("execution publication set exceeds bounds")
    wires = tuple(canonical_publication_bytes(PublicationEnvelope.model_validate(dict(
        canonical_statement=item.canonical_statement.decode(), statement_sha256=item.statement_sha256,
        key_id=item.key_id, algorithm=item.algorithm, signature=item.signature,
    ))) for item in envelopes)
    keyset = await read_keyset(session, trust_root=root, expected_state=authority.publication_state, clock=clock)
    return _Inputs(image, operation_id, plan, wires, keyset)


def _candidate(
    values: _Inputs, authority: LockedExecutionClaim, root: ExecutionGrantTrustRoot,
    purpose: BuildPurpose, campaign: str | None, grant_id: str, revision: int,
    issued: datetime, expires: datetime,
) -> TaskImageExecutionGrantV2:
    image = values.image
    components = []
    for wire in values.publications:
        envelope = PublicationEnvelope.model_validate_json(wire)
        statement = decode_publication_statement(envelope.canonical_statement.encode())
        component = statement.component
        components.append(dict(
            component=component, envelope_sha256=hashlib.sha256(wire).hexdigest(),
            image=image.registry_images[component],
        ))
    return TaskImageExecutionGrantV2.model_validate(dict(
        schema="loom.task-image-execution-grant/v2", grant_id=grant_id, revision=revision,
        claim=authority.claim.model_dump(mode="json", exclude_none=True), environment=root.environment,
        purpose=purpose, **({"shadow_campaign_id": campaign} if campaign is not None else {}),
        materialization_id=str(image.id), materialization_key=image.materialization_key,
        task_checksum=image.task_checksum, cpu_arch=image.cpu_arch,
        canonical_task_config=rfc8785.dumps(image.task_config).decode(),
        canonical_source_provenance=rfc8785.dumps(image.task_source_provenance).decode(),
        task_source=image.task_source, frozen_plan_sha256=hashlib.sha256(values.plan).hexdigest(),
        components=components, keyset_sha256=values.keyset.snapshot_sha256,
        keyset_version=authority.publication_state.keyset_version,
        revocation_epoch=authority.publication_state.revocation_epoch,
        issued_at=_stamp(issued), expires_at=_stamp(expires),
    ))


async def _latest(session: AsyncSession, claim: LegacyExecutionClaim) -> TaskImageExecutionGrant | None:
    row: TaskImageExecutionGrant | None = await session.scalar(select(TaskImageExecutionGrant)
        .where(TaskImageExecutionGrant.claim_id == UUID(claim.claim_id))
        .order_by(TaskImageExecutionGrant.revision.desc()).limit(1)
        .execution_options(populate_existing=True).with_for_update())
    return row


def _retained(row: TaskImageExecutionGrant, claim: LegacyExecutionClaim) -> TaskImageExecutionGrantV2:
    grant = _decode(row.canonical_grant, TaskImageExecutionGrantV2, MAX_EXECUTION_GRANT_BYTES)
    if (
        grant.claim != claim or grant.grant_id != str(row.grant_id) or grant.revision != row.revision
        or row.claim_id != UUID(claim.claim_id)
        or row.trial_id != UUID(claim.trial_id) or row.worker_id != UUID(claim.worker_id)
        or grant.keyset_version != row.keyset_version
        or hashlib.sha256(row.canonical_grant).hexdigest() != row.grant_sha256
        or row.revoked_at is not None
    ):
        raise ValueError("retained execution grant is revoked or inconsistent")
    return grant


async def prepare_execution_grant(
    session: AsyncSession, *, claim: LegacyExecutionClaim, worker_token_hash: bytes,
    trust_root: ExecutionGrantTrustRoot, purpose: BuildPurpose, shadow_campaign_id: str | None,
    clock: Callable[[], datetime] = _clock, lifetime_seconds: int = 120,
) -> TaskImageExecutionGrantV2:
    """Persist an immutable request, then COMMIT before calling the dedicated signer."""
    if type(lifetime_seconds) is not int or not 1 <= lifetime_seconds <= 900:
        raise ValueError("invalid execution grant lifetime")
    authority = await lock_execution_claim(session, claim=claim, worker_token_hash=worker_token_hash)
    values = await _inputs(session, authority, trust_root, clock)
    previous = await _latest(session, claim)
    now = clock()
    grant_id, revision = str(uuid4()), 1
    if previous is not None:
        old = _retained(previous, claim)
        if previous.operation_id != values.operation_id or old.purpose != purpose or old.shadow_campaign_id != shadow_campaign_id:
            raise ValueError("execution refresh cannot change publication or purpose")
        current = _candidate(values, authority, trust_root, purpose, shadow_campaign_id,
                             old.grant_id, old.revision, _instant(old.issued_at), _instant(old.expires_at))
        if canonical_execution_grant_bytes(current) == previous.canonical_grant and _instant(old.issued_at) <= now < _instant(old.expires_at):
            return old
        grant_id, revision = old.grant_id, old.revision + 1
    grant = _candidate(values, authority, trust_root, purpose, shadow_campaign_id,
                       grant_id, revision, now, min(now + timedelta(seconds=lifetime_seconds), values.keyset.expires_at, trust_root.expires_at))
    wire = canonical_execution_grant_bytes(grant)
    session.add(TaskImageExecutionGrant(
        grant_id=UUID(grant.grant_id), revision=grant.revision, claim_id=UUID(claim.claim_id),
        trial_id=UUID(claim.trial_id), worker_id=UUID(claim.worker_id), operation_id=values.operation_id,
        keyset_version=grant.keyset_version, canonical_grant=wire, grant_sha256=hashlib.sha256(wire).hexdigest(),
        created_at=now,
    ))
    await session.flush()
    if not now <= clock() < _instant(grant.expires_at):
        raise ValueError("execution grant expired during persistence")
    return grant


async def prepare_execution_signing_request(
    session: AsyncSession, *, claim: LegacyExecutionClaim, worker_token_hash: bytes,
    trust_root: ExecutionGrantTrustRoot, purpose: BuildPurpose, shadow_campaign_id: str | None,
    clock: Callable[[], datetime] = _clock, lifetime_seconds: int = 120,
) -> ExecutionSigningRequest:
    """Prepare fixed signer input in the caller transaction; COMMIT before sending."""
    grant = await prepare_execution_grant(
        session, claim=claim, worker_token_hash=worker_token_hash, trust_root=trust_root,
        purpose=purpose, shadow_campaign_id=shadow_campaign_id, clock=clock,
        lifetime_seconds=lifetime_seconds,
    )
    authority = await lock_execution_claim(session, claim=claim, worker_token_hash=worker_token_hash)
    values = await _inputs(session, authority, trust_root, clock)
    return ExecutionSigningRequest.model_validate(dict(
        schema="loom.task-image-execution-signing-request/v1", grant_id=grant.grant_id,
        revision=grant.revision, grant_sha256=hashlib.sha256(canonical_execution_grant_bytes(grant)).hexdigest(),
        frozen_plan=values.plan.decode(), publications=tuple(item.decode() for item in values.publications),
    ))


def _verify_record(
    row: TaskImageExecutionGrant, wire: bytes, values: _Inputs, authority: LockedExecutionClaim,
    root: ExecutionGrantTrustRoot, purpose: BuildPurpose, campaign: str | None, now: datetime,
) -> VerifiedExecutionGrant:
    grant = _retained(row, authority.claim)
    current = _candidate(values, authority, root, purpose, campaign, grant.grant_id, grant.revision,
                         _instant(grant.issued_at), _instant(grant.expires_at))
    envelope = _decode(wire, ExecutionGrantEnvelope, MAX_EXECUTION_GRANT_ENVELOPE_BYTES)
    if (
        row.operation_id != values.operation_id
        or canonical_execution_grant_bytes(current) != row.canonical_grant
        or envelope.canonical_grant.encode() != row.canonical_grant
    ):
        raise ValueError("execution signature or current authority differs from preparation")
    return verify_execution_grant(
        wire=wire, plan_wire=values.plan, publication_wires=values.publications,
        keyset_wire=values.keyset.wire, trust_root=root, expected_claim=authority.claim,
        expected_purpose=purpose, expected_shadow_campaign_id=campaign, now=now,
    )


def _delivery(wire: bytes, values: _Inputs, claim: LegacyExecutionClaim) -> TaskImageExecutionDelivery:
    return TaskImageExecutionDelivery.model_validate(dict(
        schema="loom.task-image-execution-delivery/v2", claim=claim,
        grant_envelope=wire.decode(), frozen_plan=values.plan.decode(),
        publications=tuple(item.decode() for item in values.publications), keyset=values.keyset.wire.decode(),
    ))


async def finalize_execution_grant(
    session: AsyncSession, *, wire: bytes, claim: LegacyExecutionClaim, worker_token_hash: bytes,
    trust_root: ExecutionGrantTrustRoot, purpose: BuildPurpose, shadow_campaign_id: str | None,
    clock: Callable[[], datetime] = _clock,
) -> TaskImageExecutionDelivery:
    """Fill exactly the current request's signature; successful return still needs COMMIT."""
    authority = await lock_execution_claim(session, claim=claim, worker_token_hash=worker_token_hash)
    values = await _inputs(session, authority, trust_root, clock)
    row = await _latest(session, claim)
    if row is None:
        raise ValueError("execution preparation is missing")
    _verify_record(row, wire, values, authority, trust_root, purpose, shadow_campaign_id, clock())
    if row.canonical_envelope is not None and row.canonical_envelope != wire:
        raise ValueError("execution signature differs from retained reply")
    row.canonical_envelope, row.envelope_sha256 = wire, hashlib.sha256(wire).hexdigest()
    await session.flush()
    _verify_record(row, wire, values, authority, trust_root, purpose, shadow_campaign_id, clock())
    return _delivery(wire, values, claim)


async def consume_execution_start(
    session: AsyncSession, *, request: ExecutionStartRequest, claim: LegacyExecutionClaim,
    worker_token_hash: bytes, trust_root: ExecutionGrantTrustRoot, purpose: BuildPurpose,
    shadow_campaign_id: str | None, clock: Callable[[], datetime] = _clock,
) -> ExecutionStartReceipt:
    """Serialize one-use consumption with revocation and claim changes; COMMIT before reply."""
    _ = request.digest
    if request.claim != claim:
        raise ValueError("execution start request differs from authenticated claim")
    authority = await lock_execution_claim(session, claim=claim, worker_token_hash=worker_token_hash)
    values = await _inputs(session, authority, trust_root, clock)
    row = await _latest(session, claim)
    if row is None or row.canonical_envelope is None:
        raise ValueError("signed execution grant is missing")
    verified = _verify_record(row, row.canonical_envelope, values, authority, trust_root, purpose, shadow_campaign_id, clock())
    grant = verified.grant
    if (
        request.grant_id != grant.grant_id or request.revision != grant.revision
        or request.envelope_sha256 != verified.envelope_sha256
        or row.envelope_sha256 != verified.envelope_sha256
        or request.keyset_sha256 != grant.keyset_sha256
        or request.keyset_version != grant.keyset_version or request.revocation_epoch != grant.revocation_epoch
    ):
        raise ValueError("execution start request is stale or substituted")
    now = clock()
    expiry = min(now + timedelta(seconds=30), _instant(grant.expires_at))
    receipt = ExecutionStartReceipt.model_validate(dict(
        schema="loom.task-image-execution-start-receipt/v1", start_id=str(uuid4()),
        request_sha256=request.digest, consumed_at=_stamp(now), expires_at=_stamp(expiry),
    ))
    wire = rfc8785.dumps(receipt.model_dump(mode="json", by_alias=True, exclude_none=True))
    session.add(TaskImageExecutionStart(
        claim_id=UUID(claim.claim_id), grant_id=row.grant_id, revision=row.revision,
        start_id=UUID(receipt.start_id), request_sha256=request.digest, canonical_receipt=wire,
        receipt_sha256=hashlib.sha256(wire).hexdigest(), consumed_at=now, expires_at=expiry,
    ))
    await session.flush()
    if not now <= clock() < expiry:
        raise ValueError("execution start expired during commit preparation")
    return receipt
