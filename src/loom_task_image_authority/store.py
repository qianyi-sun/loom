"""Locked durable transitions for task-image credential projection."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Callable
from datetime import datetime
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    TaskImageBuildContainmentAttestation,
    TaskImageBuildGrant,
    TaskImageBuildProjection,
    TaskImageBuildProjectionEvent,
)
from loom.security.secret_store import SecretStore
from loom_task_image_authority.contracts import (
    MAX_ATTESTATION_LIFETIME,
    MAX_CHALLENGE_LIFETIME,
    TaskImageAttachmentProofV1,
    TaskImageBuildGrantAuthorityV1,
    TaskImageContainmentAttestationV1,
    TaskImageGuardPrincipalV1,
    TaskImageProjectionChallengeV1,
    TaskImageProjectionReceiptV1,
    TaskImageProjectionRequestV1,
    canonical_authority_sha256,
)

_SUPERVISOR_UID = 993
_SUPERVISOR_GID = 980
_PROJECT_SCOPE = "task-image:project"


class TaskImageProjectionConflictError(RuntimeError):
    """An idempotency identity was reused with different canonical input."""


class TaskImageProjectionAuthorizationError(RuntimeError):
    """The caller or observed allocation does not match durable authority."""


class TaskImageProjectionExpiredError(TaskImageProjectionAuthorizationError):
    """A required grant, challenge, proof, or replay deadline has elapsed."""


def _validated_principal(
    principal: TaskImageGuardPrincipalV1,
) -> TaskImageGuardPrincipalV1:
    try:
        return TaskImageGuardPrincipalV1.model_validate(
            principal.model_dump(mode="python")
        )
    except ValidationError as exc:
        raise TaskImageProjectionAuthorizationError(
            "task-image guard principal is invalid"
        ) from exc


def _validated_request(
    request: TaskImageProjectionRequestV1,
) -> TaskImageProjectionRequestV1:
    try:
        return TaskImageProjectionRequestV1.model_validate(
            request.model_dump(mode="python")
        )
    except ValidationError as exc:
        raise TaskImageProjectionAuthorizationError(
            "task-image projection request is invalid"
        ) from exc


def _validated_proof(proof: TaskImageAttachmentProofV1) -> TaskImageAttachmentProofV1:
    try:
        return TaskImageAttachmentProofV1.model_validate(
            proof.model_dump(mode="python")
        )
    except ValidationError as exc:
        raise TaskImageProjectionAuthorizationError(
            "task-image attachment proof is invalid"
        ) from exc


async def _locked_grant(session: AsyncSession, *, grant_id: UUID) -> TaskImageBuildGrant:
    row = await session.scalar(
        select(TaskImageBuildGrant)
        .where(TaskImageBuildGrant.id == grant_id)
        .with_for_update()
    )
    if row is None:
        raise TaskImageProjectionAuthorizationError(
            "task-image build grant is unavailable"
        )
    return row


async def _locked_projection(
    session: AsyncSession,
    *,
    grant_id: UUID,
) -> TaskImageBuildProjection | None:
    row: TaskImageBuildProjection | None = await session.scalar(
        select(TaskImageBuildProjection)
        .where(TaskImageBuildProjection.grant_id == grant_id)
        .with_for_update()
    )
    return row


def _grant_authority(
    row: TaskImageBuildGrant,
    *,
    now: datetime,
) -> TaskImageBuildGrantAuthorityV1:
    try:
        authority = TaskImageBuildGrantAuthorityV1.model_validate_json(
            json.dumps(row.authority_spec)
        )
    except ValidationError as exc:
        raise TaskImageProjectionAuthorizationError(
            "task-image build grant authority is invalid"
        ) from exc
    if (
        canonical_authority_sha256(authority) != row.authority_sha256
        or authority.environment != row.environment
        or authority.slurm_cluster_id != row.slurm_cluster_id
        or authority.cpu_arch != row.cpu_arch
        or authority.slurm_request_sha256 != row.request_sha256
        or authority.expires_at != row.grant_expires_at
    ):
        raise TaskImageProjectionAuthorizationError(
            "task-image build grant authority binding changed"
        )
    if now >= authority.expires_at:
        raise TaskImageProjectionExpiredError("task-image build grant authority expired")
    return authority


def _require_released_grant(row: TaskImageBuildGrant) -> None:
    if row.state != "released" or row.released_at is None or row.revoked_at is not None:
        raise TaskImageProjectionAuthorizationError(
            "task-image build grant is not released"
        )


def _require_principal(
    principal: TaskImageGuardPrincipalV1,
    *,
    cluster: str,
    node_name: str,
    principal_sha256: str | None = None,
) -> str:
    digest = canonical_authority_sha256(principal)
    if (
        _PROJECT_SCOPE not in principal.scopes
        or principal.slurm_cluster_id != cluster
        or principal.node_name != node_name
        or (principal_sha256 is not None and digest != principal_sha256)
    ):
        raise TaskImageProjectionAuthorizationError(
            "task-image guard principal is not authorized"
        )
    return digest


def _require_request_binding(
    row: TaskImageBuildGrant,
    authority: TaskImageBuildGrantAuthorityV1,
    principal: TaskImageGuardPrincipalV1,
    request: TaskImageProjectionRequestV1,
    *,
    now: datetime,
) -> str:
    principal_sha256 = _require_principal(
        principal,
        cluster=row.slurm_cluster_id,
        node_name=request.node_name,
    )
    if (
        request.grant_id != row.id
        or request.slurm_cluster_id != row.slurm_cluster_id
        or request.slurm_job_id != row.slurm_job_id
        or request.submitting_identity != row.submitting_identity
        or request.slurm_account != row.slurm_account
        or request.slurm_partition != row.slurm_partition
        or request.slurm_qos != row.slurm_qos
        or request.cpu_arch != row.cpu_arch
        or request.slurm_request_sha256 != row.request_sha256
        or request.supervisor_uid != _SUPERVISOR_UID
        or request.supervisor_gid != _SUPERVISOR_GID
        or request.supervisor_executable_sha256 != authority.builder_release_sha256
    ):
        raise TaskImageProjectionAuthorizationError(
            "task-image projection request differs from released grant"
        )
    if request.observed_at > now:
        raise TaskImageProjectionAuthorizationError(
            "task-image projection observation is in the future"
        )
    if now >= request.observed_at + MAX_CHALLENGE_LIFETIME:
        raise TaskImageProjectionExpiredError(
            "task-image projection observation is stale"
        )
    return principal_sha256


def _append_event(
    session: AsyncSession,
    *,
    row: TaskImageBuildProjection,
    event_type: str,
    event_key: str,
    payload: dict[str, object],
    now: datetime,
) -> None:
    row.event_sequence += 1
    row.updated_at = now
    session.add(
        TaskImageBuildProjectionEvent(
            grant_id=row.grant_id,
            event_sequence=row.event_sequence,
            event_type=event_type,
            event_key=event_key,
            payload_json=payload,
            created_at=now,
        )
    )


async def _append_event_once(
    session: AsyncSession,
    *,
    row: TaskImageBuildProjection,
    event_type: str,
    event_key: str,
    payload: dict[str, object],
    now: datetime,
) -> None:
    event_id = await session.scalar(
        select(TaskImageBuildProjectionEvent.id)
        .where(
            TaskImageBuildProjectionEvent.grant_id == row.grant_id,
            TaskImageBuildProjectionEvent.event_type == event_type,
            TaskImageBuildProjectionEvent.event_key == event_key,
        )
        .limit(1)
    )
    if event_id is None:
        _append_event(
            session,
            row=row,
            event_type=event_type,
            event_key=event_key,
            payload=payload,
            now=now,
        )


def _stored_challenge(row: TaskImageBuildProjection) -> TaskImageProjectionChallengeV1:
    try:
        challenge = TaskImageProjectionChallengeV1.model_validate_json(
            json.dumps(row.challenge_json)
        )
    except ValidationError as exc:
        raise TaskImageProjectionAuthorizationError(
            "stored task-image projection challenge is invalid"
        ) from exc
    if (
        canonical_authority_sha256(challenge) != row.challenge_sha256
        or challenge.challenge_nonce != row.challenge_nonce
        or challenge.request_id != row.request_id
        or challenge.request_sha256 != row.request_sha256
    ):
        raise TaskImageProjectionAuthorizationError(
            "stored task-image projection challenge changed"
        )
    return challenge


async def request_task_image_projection(
    session: AsyncSession,
    *,
    principal: TaskImageGuardPrincipalV1,
    request: TaskImageProjectionRequestV1,
    now: datetime,
    challenge_nonce_factory: Callable[[], UUID],
) -> TaskImageProjectionChallengeV1:
    """Issue or exactly replay one attachment challenge without committing."""

    principal = _validated_principal(principal)
    request = _validated_request(request)
    grant = await _locked_grant(session, grant_id=request.grant_id)
    authority = _grant_authority(grant, now=now)
    _require_released_grant(grant)
    principal_sha256 = _require_request_binding(
        grant,
        authority,
        principal,
        request,
        now=now,
    )
    request_sha256 = canonical_authority_sha256(request)
    row = await _locked_projection(session, grant_id=grant.id)
    if row is not None:
        if (
            row.request_id != request.request_id
            or row.request_sha256 != request_sha256
            or row.principal_sha256 != principal_sha256
        ):
            raise TaskImageProjectionConflictError(
                "task-image projection request idempotency conflict"
            )
        challenge = _stored_challenge(row)
        if now >= challenge.expires_at:
            raise TaskImageProjectionExpiredError(
                "task-image projection challenge expired"
            )
        await _append_event_once(
            session,
            row=row,
            event_type="challenge_replayed",
            event_key="challenge",
            payload={"request_sha256": request_sha256},
            now=now,
        )
        await session.flush()
        return challenge

    challenge = TaskImageProjectionChallengeV1(
        request_id=request.request_id,
        grant_id=grant.id,
        request_sha256=request_sha256,
        challenge_nonce=challenge_nonce_factory(),
        containment_policy_sha256=authority.containment_policy_sha256,
        resource_profile_sha256=authority.resource_profile_sha256,
        issued_at=now,
        expires_at=min(now + MAX_CHALLENGE_LIFETIME, authority.expires_at),
    )
    challenge_sha256 = canonical_authority_sha256(challenge)
    row = TaskImageBuildProjection(
        grant_id=grant.id,
        state="challenged",
        principal_id=principal.principal_id,
        principal_sha256=principal_sha256,
        request_id=request.request_id,
        request_json=request.model_dump(mode="json", exclude_none=False),
        request_sha256=request_sha256,
        node_name=request.node_name,
        node_boot_id=request.node_boot_id,
        slurm_cluster_id=request.slurm_cluster_id,
        slurm_job_id=request.slurm_job_id,
        supervisor_pid=request.supervisor_pid,
        supervisor_uid=request.supervisor_uid,
        supervisor_gid=request.supervisor_gid,
        supervisor_executable_sha256=request.supervisor_executable_sha256,
        cgroup_path=request.cgroup_path,
        cgroup_inode=request.cgroup_inode,
        challenge_nonce=challenge.challenge_nonce,
        challenge_json=challenge.model_dump(mode="json", exclude_none=False),
        challenge_sha256=challenge_sha256,
        challenge_issued_at=challenge.issued_at,
        challenge_expires_at=challenge.expires_at,
        event_sequence=0,
        created_at=now,
        updated_at=now,
    )
    session.add(row)
    _append_event(
        session,
        row=row,
        event_type="challenged",
        event_key="challenge",
        payload={"request_sha256": request_sha256},
        now=now,
    )
    await session.flush()
    return challenge


def _require_proof_binding(
    row: TaskImageBuildProjection,
    authority: TaskImageBuildGrantAuthorityV1,
    challenge: TaskImageProjectionChallengeV1,
    proof: TaskImageAttachmentProofV1,
    *,
    now: datetime,
) -> None:
    if (
        proof.grant_id != row.grant_id
        or proof.request_id != row.request_id
        or proof.request_sha256 != row.request_sha256
        or proof.challenge_nonce != row.challenge_nonce
        or proof.node_name != row.node_name
        or proof.node_boot_id != row.node_boot_id
        or proof.slurm_cluster_id != row.slurm_cluster_id
        or proof.slurm_job_id != row.slurm_job_id
        or proof.cgroup_path != row.cgroup_path
        or proof.cgroup_inode != row.cgroup_inode
        or proof.attachment.cgroup_inode != row.cgroup_inode
        or proof.attachment.containment_policy_sha256
        != authority.containment_policy_sha256
        or proof.attachment.resource_limits_sha256 != authority.resource_profile_sha256
    ):
        raise TaskImageProjectionAuthorizationError(
            "task-image attachment proof differs from challenge authority"
        )
    if not (challenge.issued_at <= proof.observed_at <= now):
        raise TaskImageProjectionAuthorizationError(
            "task-image attachment proof observation is invalid"
        )
    if now >= challenge.expires_at:
        raise TaskImageProjectionExpiredError("task-image projection challenge expired")
    if proof.observed_at >= challenge.expires_at:
        raise TaskImageProjectionExpiredError(
            "task-image attachment proof missed its challenge"
        )
    if now >= proof.attestation_expires_at:
        raise TaskImageProjectionExpiredError(
            "task-image containment attestation expired"
        )
    if proof.attestation_expires_at > authority.expires_at:
        raise TaskImageProjectionAuthorizationError(
            "task-image containment attestation exceeds grant authority"
        )


async def _replay_projection(
    session: AsyncSession,
    *,
    row: TaskImageBuildProjection,
    proof: TaskImageAttachmentProofV1,
    proof_sha256: str,
    now: datetime,
    secret_store: SecretStore,
) -> TaskImageProjectionReceiptV1:
    if row.proof_id != proof.proof_id or row.proof_sha256 != proof_sha256:
        raise TaskImageProjectionConflictError(
            "task-image attachment proof idempotency conflict"
        )
    if (
        row.bootstrap_secret_ref is None
        or row.bootstrap_token_hash is None
        or row.bootstrap_issued_at is None
        or row.bootstrap_expires_at is None
        or row.attestation_expires_at is None
    ):
        raise TaskImageProjectionAuthorizationError(
            "stored task-image bootstrap receipt is incomplete"
        )
    if now >= min(row.bootstrap_expires_at, row.attestation_expires_at):
        raise TaskImageProjectionExpiredError("task-image bootstrap replay expired")
    token = await secret_store.get(row.bootstrap_secret_ref)
    if not hmac.compare_digest(
        hashlib.sha256(token.encode("utf-8")).digest(),
        row.bootstrap_token_hash,
    ):
        raise TaskImageProjectionAuthorizationError(
            "stored task-image bootstrap receipt changed"
        )
    receipt = TaskImageProjectionReceiptV1(
        grant_id=row.grant_id,
        proof_id=proof.proof_id,
        proof_sha256=proof_sha256,
        bootstrap_token=token,
        issued_at=row.bootstrap_issued_at,
        expires_at=row.bootstrap_expires_at,
    )
    await _append_event_once(
        session,
        row=row,
        event_type="projection_replayed",
        event_key="projection",
        payload={"proof_sha256": proof_sha256},
        now=now,
    )
    await session.flush()
    return receipt


async def complete_task_image_projection(
    session: AsyncSession,
    *,
    principal: TaskImageGuardPrincipalV1,
    proof: TaskImageAttachmentProofV1,
    now: datetime,
    secret_store: SecretStore,
    bootstrap_token_factory: Callable[[], str],
) -> TaskImageProjectionReceiptV1:
    """Consume a challenge and persist one encrypted bootstrap projection."""

    principal = _validated_principal(principal)
    proof = _validated_proof(proof)
    grant = await _locked_grant(session, grant_id=proof.grant_id)
    authority = _grant_authority(grant, now=now)
    _require_released_grant(grant)
    row = await _locked_projection(session, grant_id=grant.id)
    if row is None:
        raise TaskImageProjectionAuthorizationError(
            "task-image projection challenge is unavailable"
        )
    _require_principal(
        principal,
        cluster=row.slurm_cluster_id,
        node_name=row.node_name,
        principal_sha256=row.principal_sha256,
    )
    challenge = _stored_challenge(row)
    proof_sha256 = canonical_authority_sha256(proof)
    if row.state == "projected":
        return await _replay_projection(
            session,
            row=row,
            proof=proof,
            proof_sha256=proof_sha256,
            now=now,
            secret_store=secret_store,
        )
    if row.state != "challenged":
        raise TaskImageProjectionConflictError(
            "task-image projection is not awaiting a proof"
        )
    _require_proof_binding(row, authority, challenge, proof, now=now)

    token = bootstrap_token_factory()
    expires_at = min(
        now + MAX_ATTESTATION_LIFETIME,
        proof.attestation_expires_at,
        authority.expires_at,
    )
    receipt = TaskImageProjectionReceiptV1(
        grant_id=grant.id,
        proof_id=proof.proof_id,
        proof_sha256=proof_sha256,
        bootstrap_token=token,
        issued_at=now,
        expires_at=expires_at,
    )
    secret_ref = await secret_store.put(
        namespace="task-image-bootstrap",
        value=token,
    )
    if not secret_ref.startswith("loom://task-image-bootstrap/"):
        raise ValueError("task-image bootstrap secret reference has wrong namespace")

    attestation = TaskImageContainmentAttestationV1(
        attestation_id=proof.proof_id,
        grant_id=proof.grant_id,
        generation=proof.attestation_generation,
        node_name=proof.node_name,
        node_boot_id=proof.node_boot_id,
        slurm_cluster_id=proof.slurm_cluster_id,
        slurm_job_id=proof.slurm_job_id,
        cgroup_path=proof.cgroup_path,
        cgroup_inode=proof.cgroup_inode,
        attachment=proof.attachment,
        issued_at=proof.observed_at,
        expires_at=proof.attestation_expires_at,
    )
    attestation_sha256 = canonical_authority_sha256(attestation)
    session.add(
        TaskImageBuildContainmentAttestation(
            id=attestation.attestation_id,
            grant_id=grant.id,
            generation=attestation.generation,
            attestation_json=attestation.model_dump(mode="json", exclude_none=False),
            attestation_sha256=attestation_sha256,
            issued_at=attestation.issued_at,
            expires_at=attestation.expires_at,
            recorded_at=now,
        )
    )
    row.state = "projected"
    row.proof_id = proof.proof_id
    row.proof_json = proof.model_dump(mode="json", exclude_none=False)
    row.proof_sha256 = proof_sha256
    row.bootstrap_token_hash = hashlib.sha256(token.encode("utf-8")).digest()
    row.bootstrap_secret_ref = secret_ref
    row.bootstrap_issued_at = receipt.issued_at
    row.bootstrap_expires_at = receipt.expires_at
    row.attestation_generation = attestation.generation
    row.attestation_sha256 = attestation_sha256
    row.attestation_expires_at = attestation.expires_at
    _append_event(
        session,
        row=row,
        event_type="projected",
        event_key="projection",
        payload={
            "proof_sha256": proof_sha256,
            "attestation_sha256": attestation_sha256,
        },
        now=now,
    )
    await session.flush()
    return receipt


__all__ = [
    "TaskImageProjectionAuthorizationError",
    "TaskImageProjectionConflictError",
    "TaskImageProjectionExpiredError",
    "complete_task_image_projection",
    "request_task_image_projection",
]
