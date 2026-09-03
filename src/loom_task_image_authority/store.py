"""Locked durable transitions for task-image credential projection."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID, uuid4

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    TaskImageBuildContainmentAttestation,
    TaskImageBuildGrant,
    TaskImageBuildProjection,
    TaskImageBuildProjectionEvent,
    TaskImageBuildSessionGeneration,
)
from loom.security.secret_store import SecretStore
from loom_task_image_authority.contracts import (
    MAX_ATTESTATION_LIFETIME,
    MAX_CHALLENGE_LIFETIME,
    MAX_SESSION_LIFETIME,
    TaskImageAttachmentProofV1,
    TaskImageBootstrapExchangeV1,
    TaskImageBuildGrantAuthorityV1,
    TaskImageBuildGrantAuthorityV2,
    TaskImageBuildSessionV1,
    TaskImageBuildSessionV2,
    TaskImageContainmentAttestationV1,
    TaskImageGuardPrincipalV1,
    TaskImageProjectionChallengeV1,
    TaskImageProjectionReceiptV1,
    TaskImageProjectionRequestV1,
    TaskImageProjectionRevocationV1,
    TaskImageSessionRenewalV1,
    canonical_authority_sha256,
    canonical_public_binding_sha256,
)

_SUPERVISOR_UID = 993
_SUPERVISOR_GID = 980
_PROJECT_SCOPE = "task-image:project"
_ATTEST_SCOPE = "task-image:attest"

GrantAuthority = TaskImageBuildGrantAuthorityV1 | TaskImageBuildGrantAuthorityV2
BuildSession = TaskImageBuildSessionV1 | TaskImageBuildSessionV2


class TaskImageProjectionConflictError(RuntimeError):
    """An idempotency identity was reused with different canonical input."""


class TaskImageProjectionEquivocationError(TaskImageProjectionConflictError):
    """A conflict that staged a mandatory durable projection quarantine."""


class TaskImageProjectionAuthorizationError(RuntimeError):
    """The caller or observed allocation does not match durable authority."""


class TaskImageProjectionExpiredError(TaskImageProjectionAuthorizationError):
    """A required grant, challenge, proof, or replay deadline has elapsed."""


@dataclass(frozen=True)
class TaskImageBuildSessionAuthorization:
    """Nonsecret authority returned after bearer and attestation validation."""

    grant_id: UUID
    session_id: UUID
    session_generation: int
    authority_version: Literal[1, 2]
    builder_release_sha256: str | None
    supervisor_executable_sha256: str
    purpose: Literal["production", "shadow"]
    shadow_campaign_id: UUID | None
    environment: str
    pool_id: str
    cpu_arch: Literal["x86_64", "arm64"]
    attestation_generation: int
    attestation_sha256: str
    attestation_expires_at: datetime
    session_expires_at: datetime
    grant_expires_at: datetime


def _validated_principal(
    principal: TaskImageGuardPrincipalV1,
) -> TaskImageGuardPrincipalV1:
    try:
        return TaskImageGuardPrincipalV1.model_validate(principal.model_dump(mode="python"))
    except ValidationError as exc:
        raise TaskImageProjectionAuthorizationError(
            "task-image guard principal is invalid"
        ) from exc


def _validated_request(
    request: TaskImageProjectionRequestV1,
) -> TaskImageProjectionRequestV1:
    try:
        return TaskImageProjectionRequestV1.model_validate(request.model_dump(mode="python"))
    except ValidationError as exc:
        raise TaskImageProjectionAuthorizationError(
            "task-image projection request is invalid"
        ) from exc


def _validated_proof(proof: TaskImageAttachmentProofV1) -> TaskImageAttachmentProofV1:
    try:
        return TaskImageAttachmentProofV1.model_validate(proof.model_dump(mode="python"))
    except ValidationError as exc:
        raise TaskImageProjectionAuthorizationError(
            "task-image attachment proof is invalid"
        ) from exc


def _validated_exchange(
    request: TaskImageBootstrapExchangeV1,
) -> TaskImageBootstrapExchangeV1:
    try:
        return TaskImageBootstrapExchangeV1.model_validate(request.model_dump(mode="python"))
    except ValidationError:
        raise TaskImageProjectionAuthorizationError(
            "task-image bootstrap exchange is invalid"
        ) from None


def _validated_revocation(
    request: TaskImageProjectionRevocationV1,
) -> TaskImageProjectionRevocationV1:
    try:
        return TaskImageProjectionRevocationV1.model_validate(request.model_dump(mode="python"))
    except ValidationError:
        raise TaskImageProjectionAuthorizationError(
            "task-image projection revocation is invalid"
        ) from None


def _validated_renewal(request: TaskImageSessionRenewalV1) -> TaskImageSessionRenewalV1:
    try:
        return TaskImageSessionRenewalV1.model_validate(request.model_dump(mode="python"))
    except ValidationError:
        raise TaskImageProjectionAuthorizationError(
            "task-image session renewal is invalid"
        ) from None


def _validated_attestation(
    attestation: TaskImageContainmentAttestationV1,
) -> TaskImageContainmentAttestationV1:
    try:
        return TaskImageContainmentAttestationV1.model_validate(
            attestation.model_dump(mode="python")
        )
    except ValidationError as exc:
        raise TaskImageProjectionAuthorizationError(
            "task-image containment attestation is invalid"
        ) from exc


async def _locked_grant(session: AsyncSession, *, grant_id: UUID) -> TaskImageBuildGrant:
    row = await session.scalar(
        select(TaskImageBuildGrant).where(TaskImageBuildGrant.id == grant_id).with_for_update()
    )
    if row is None:
        raise TaskImageProjectionAuthorizationError("task-image build grant is unavailable")
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
    require_live: bool = True,
) -> GrantAuthority:
    if not isinstance(row.authority_spec, dict):
        raise TaskImageProjectionAuthorizationError("task-image build grant authority is invalid")
    model: type[TaskImageBuildGrantAuthorityV1] | type[TaskImageBuildGrantAuthorityV2]
    model = (
        TaskImageBuildGrantAuthorityV2
        if row.authority_spec.get("schema_version") == 2
        else TaskImageBuildGrantAuthorityV1
    )
    try:
        authority = model.model_validate_json(json.dumps(row.authority_spec))
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
    if require_live and now >= authority.expires_at:
        raise TaskImageProjectionExpiredError("task-image build grant authority expired")
    return authority


def _require_released_grant(row: TaskImageBuildGrant) -> None:
    if row.state != "released" or row.released_at is None or row.revoked_at is not None:
        raise TaskImageProjectionAuthorizationError("task-image build grant is not released")


def _require_principal(
    principal: TaskImageGuardPrincipalV1,
    *,
    cluster: str,
    node_name: str,
    principal_id: str | None = None,
    principal_sha256: str | None = None,
    required_scope: str = _PROJECT_SCOPE,
) -> str:
    digest = canonical_authority_sha256(principal)
    if (
        required_scope not in principal.scopes
        or principal.slurm_cluster_id != cluster
        or principal.node_name != node_name
        or (principal_id is not None and principal.principal_id != principal_id)
        or (principal_sha256 is not None and digest != principal_sha256)
    ):
        raise TaskImageProjectionAuthorizationError("task-image guard principal is not authorized")
    return digest


def _require_request_grant_binding(
    row: TaskImageBuildGrant,
    authority: GrantAuthority,
    request: TaskImageProjectionRequestV1,
) -> None:
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
        or request.supervisor_executable_sha256
        != (
            authority.supervisor_executable_sha256
            if isinstance(authority, TaskImageBuildGrantAuthorityV2)
            else authority.builder_release_sha256
        )
    ):
        raise TaskImageProjectionAuthorizationError(
            "task-image projection request differs from released grant"
        )


def _require_request_binding(
    row: TaskImageBuildGrant,
    authority: GrantAuthority,
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
    _require_request_grant_binding(row, authority, request)
    if request.observed_at > now:
        raise TaskImageProjectionAuthorizationError(
            "task-image projection observation is in the future"
        )
    if now >= request.observed_at + MAX_CHALLENGE_LIFETIME:
        raise TaskImageProjectionExpiredError("task-image projection observation is stale")
    if row.released_at is None or request.observed_at < row.released_at:
        raise TaskImageProjectionAuthorizationError(
            "task-image projection observation predates grant release"
        )
    return principal_sha256


def _stored_request(row: TaskImageBuildProjection) -> TaskImageProjectionRequestV1:
    try:
        request = TaskImageProjectionRequestV1.model_validate_json(json.dumps(row.request_json))
    except ValidationError as exc:
        raise TaskImageProjectionAuthorizationError(
            "stored task-image projection request is invalid"
        ) from exc
    if (
        canonical_authority_sha256(request) != row.request_sha256
        or request.request_id != row.request_id
        or request.grant_id != row.grant_id
        or request.node_name != row.node_name
        or request.node_boot_id != row.node_boot_id
        or request.slurm_cluster_id != row.slurm_cluster_id
        or request.slurm_job_id != row.slurm_job_id
        or request.supervisor_pid != row.supervisor_pid
        or request.supervisor_uid != row.supervisor_uid
        or request.supervisor_gid != row.supervisor_gid
        or request.supervisor_executable_sha256 != row.supervisor_executable_sha256
        or request.cgroup_path != row.cgroup_path
        or request.cgroup_inode != row.cgroup_inode
    ):
        raise TaskImageProjectionAuthorizationError("stored task-image projection request changed")
    return request


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


def _stored_challenge(
    row: TaskImageBuildProjection,
    *,
    authority: GrantAuthority,
) -> TaskImageProjectionChallengeV1:
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
        or challenge.grant_id != row.grant_id
        or challenge.challenge_nonce != row.challenge_nonce
        or challenge.request_id != row.request_id
        or challenge.request_sha256 != row.request_sha256
        or challenge.containment_policy_sha256 != authority.containment_policy_sha256
        or challenge.resource_profile_sha256 != authority.resource_profile_sha256
        or challenge.issued_at != row.challenge_issued_at
        or challenge.expires_at != row.challenge_expires_at
    ):
        raise TaskImageProjectionAuthorizationError(
            "stored task-image projection challenge changed"
        )
    return challenge


def _require_challenge_request_binding(
    request: TaskImageProjectionRequestV1,
    challenge: TaskImageProjectionChallengeV1,
    authority: GrantAuthority,
) -> None:
    if challenge.issued_at < request.observed_at or challenge.expires_at > authority.expires_at:
        raise TaskImageProjectionAuthorizationError(
            "stored task-image projection challenge changed"
        )


def _stored_attestation(
    row: TaskImageBuildContainmentAttestation,
) -> TaskImageContainmentAttestationV1:
    try:
        attestation = TaskImageContainmentAttestationV1.model_validate_json(
            json.dumps(row.attestation_json)
        )
    except ValidationError as exc:
        raise TaskImageProjectionAuthorizationError(
            "stored task-image containment attestation is invalid"
        ) from exc
    if (
        attestation.attestation_id != row.id
        or attestation.grant_id != row.grant_id
        or attestation.generation != row.generation
        or attestation.issued_at != row.issued_at
        or attestation.expires_at != row.expires_at
        or canonical_authority_sha256(attestation) != row.attestation_sha256
    ):
        raise TaskImageProjectionAuthorizationError(
            "stored task-image containment attestation changed"
        )
    return attestation


async def _current_attestation(
    session: AsyncSession,
    *,
    row: TaskImageBuildProjection,
) -> TaskImageContainmentAttestationV1:
    if row.attestation_generation is None:
        raise TaskImageProjectionAuthorizationError(
            "task-image projection lacks containment attestation"
        )
    stored = await session.scalar(
        select(TaskImageBuildContainmentAttestation)
        .where(
            TaskImageBuildContainmentAttestation.grant_id == row.grant_id,
            TaskImageBuildContainmentAttestation.generation == row.attestation_generation,
        )
        .with_for_update()
    )
    if stored is None:
        raise TaskImageProjectionAuthorizationError(
            "task-image containment attestation is unavailable"
        )
    attestation = _stored_attestation(stored)
    if (
        row.attestation_sha256 != stored.attestation_sha256
        or row.attestation_expires_at != attestation.expires_at
    ):
        raise TaskImageProjectionAuthorizationError(
            "task-image containment attestation high-water changed"
        )
    return attestation


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
        if row.state in {"revoked", "expired"}:
            raise TaskImageProjectionAuthorizationError("task-image projection is terminal")
        if (
            row.request_id != request.request_id
            or row.request_sha256 != request_sha256
            or row.principal_sha256 != principal_sha256
        ):
            raise TaskImageProjectionConflictError(
                "task-image projection request idempotency conflict"
            )
        stored_request = _stored_request(row)
        _require_principal(
            principal,
            cluster=row.slurm_cluster_id,
            node_name=row.node_name,
            principal_id=row.principal_id,
            principal_sha256=row.principal_sha256,
        )
        challenge = _stored_challenge(row, authority=authority)
        _require_challenge_request_binding(stored_request, challenge, authority)
        if now >= challenge.expires_at:
            raise TaskImageProjectionExpiredError("task-image projection challenge expired")
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
    authority: GrantAuthority,
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
        or proof.attachment.containment_policy_sha256 != authority.containment_policy_sha256
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
        raise TaskImageProjectionExpiredError("task-image attachment proof missed its challenge")
    if now >= proof.attestation_expires_at:
        raise TaskImageProjectionExpiredError("task-image containment attestation expired")
    if proof.attestation_expires_at > authority.expires_at:
        raise TaskImageProjectionAuthorizationError(
            "task-image containment attestation exceeds grant authority"
        )


def _attestation_from_proof(
    proof: TaskImageAttachmentProofV1,
) -> TaskImageContainmentAttestationV1:
    return TaskImageContainmentAttestationV1(
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


async def _stored_proof(
    session: AsyncSession,
    *,
    row: TaskImageBuildProjection,
    authority: GrantAuthority,
    challenge: TaskImageProjectionChallengeV1,
) -> TaskImageAttachmentProofV1:
    if row.proof_id is None or row.proof_json is None or row.proof_sha256 is None:
        raise TaskImageProjectionAuthorizationError(
            "stored task-image attachment proof is incomplete"
        )
    try:
        proof = TaskImageAttachmentProofV1.model_validate_json(json.dumps(row.proof_json))
    except ValidationError as exc:
        raise TaskImageProjectionAuthorizationError(
            "stored task-image attachment proof is invalid"
        ) from exc
    if proof.proof_id != row.proof_id or canonical_authority_sha256(proof) != row.proof_sha256:
        raise TaskImageProjectionAuthorizationError("stored task-image attachment proof changed")
    try:
        _require_proof_binding(
            row,
            authority,
            challenge,
            proof,
            now=proof.observed_at,
        )
    except TaskImageProjectionAuthorizationError as exc:
        raise TaskImageProjectionAuthorizationError(
            "stored task-image attachment proof changed"
        ) from exc
    initial_row = await session.scalar(
        select(TaskImageBuildContainmentAttestation)
        .where(
            TaskImageBuildContainmentAttestation.grant_id == row.grant_id,
            TaskImageBuildContainmentAttestation.generation == proof.attestation_generation,
        )
        .with_for_update()
    )
    if initial_row is None:
        raise TaskImageProjectionAuthorizationError(
            "stored task-image attachment proof attestation is unavailable"
        )
    initial_attestation = _stored_attestation(initial_row)
    if initial_attestation != _attestation_from_proof(proof):
        raise TaskImageProjectionAuthorizationError("stored task-image attachment proof changed")
    return proof


async def _require_stored_projection_chain(
    session: AsyncSession,
    *,
    grant: TaskImageBuildGrant,
    row: TaskImageBuildProjection,
    authority: GrantAuthority,
) -> None:
    request = _stored_request(row)
    _require_request_grant_binding(grant, authority, request)
    challenge = _stored_challenge(row, authority=authority)
    _require_challenge_request_binding(request, challenge, authority)
    proof = await _stored_proof(
        session,
        row=row,
        authority=authority,
        challenge=challenge,
    )
    _require_projected_receipt_binding(row, authority=authority, proof=proof)


async def _replay_projection(
    session: AsyncSession,
    *,
    row: TaskImageBuildProjection,
    authority: GrantAuthority,
    challenge: TaskImageProjectionChallengeV1,
    proof: TaskImageAttachmentProofV1,
    proof_sha256: str,
    now: datetime,
    secret_store: SecretStore,
) -> TaskImageProjectionReceiptV1:
    if row.proof_id != proof.proof_id or row.proof_sha256 != proof_sha256:
        raise TaskImageProjectionConflictError("task-image attachment proof idempotency conflict")
    stored_proof = await _stored_proof(
        session,
        row=row,
        authority=authority,
        challenge=challenge,
    )
    _require_projected_receipt_binding(
        row,
        authority=authority,
        proof=stored_proof,
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
        raise TaskImageProjectionAuthorizationError("stored task-image bootstrap receipt changed")
    try:
        receipt = TaskImageProjectionReceiptV1(
            grant_id=row.grant_id,
            proof_id=proof.proof_id,
            proof_sha256=proof_sha256,
            bootstrap_token=token,
            issued_at=row.bootstrap_issued_at,
            expires_at=row.bootstrap_expires_at,
        )
    except ValidationError:
        raise TaskImageProjectionAuthorizationError(
            "stored task-image bootstrap receipt is invalid"
        ) from None
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
        principal_id=row.principal_id,
        principal_sha256=row.principal_sha256,
    )
    stored_request = _stored_request(row)
    _require_request_grant_binding(grant, authority, stored_request)
    challenge = _stored_challenge(row, authority=authority)
    _require_challenge_request_binding(stored_request, challenge, authority)
    proof_sha256 = canonical_authority_sha256(proof)
    if row.state == "projected":
        return await _replay_projection(
            session,
            row=row,
            authority=authority,
            challenge=challenge,
            proof=proof,
            proof_sha256=proof_sha256,
            now=now,
            secret_store=secret_store,
        )
    if row.state != "challenged":
        raise TaskImageProjectionConflictError("task-image projection is not awaiting a proof")
    _require_proof_binding(row, authority, challenge, proof, now=now)

    token = bootstrap_token_factory()
    expires_at = min(
        now + MAX_ATTESTATION_LIFETIME,
        proof.attestation_expires_at,
        authority.expires_at,
    )
    try:
        receipt = TaskImageProjectionReceiptV1(
            grant_id=grant.id,
            proof_id=proof.proof_id,
            proof_sha256=proof_sha256,
            bootstrap_token=token,
            issued_at=now,
            expires_at=expires_at,
        )
    except ValidationError:
        raise TaskImageProjectionAuthorizationError(
            "generated task-image bootstrap receipt is invalid"
        ) from None
    secret_ref = await secret_store.put(
        namespace="task-image-bootstrap",
        value=token,
    )
    if not secret_ref.startswith("loom://task-image-bootstrap/"):
        raise ValueError("task-image bootstrap secret reference has wrong namespace")

    attestation = _attestation_from_proof(proof)
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


def _require_projected_secrets(row: TaskImageBuildProjection) -> None:
    if (
        row.proof_id is None
        or row.proof_sha256 is None
        or row.bootstrap_token_hash is None
        or row.bootstrap_secret_ref is None
        or row.bootstrap_issued_at is None
        or row.bootstrap_expires_at is None
        or row.attestation_generation is None
        or row.attestation_sha256 is None
        or row.attestation_expires_at is None
    ):
        raise TaskImageProjectionAuthorizationError(
            "task-image projection lacks bootstrap authority"
        )


def _require_projected_receipt_binding(
    row: TaskImageBuildProjection,
    *,
    authority: GrantAuthority,
    proof: TaskImageAttachmentProofV1,
) -> None:
    _require_projected_secrets(row)
    if row.bootstrap_issued_at is None or row.bootstrap_expires_at is None:
        raise TaskImageProjectionAuthorizationError(
            "stored task-image bootstrap receipt is incomplete"
        )
    if (
        row.bootstrap_issued_at < proof.observed_at
        or row.bootstrap_expires_at <= row.bootstrap_issued_at
        or row.bootstrap_expires_at
        > min(
            row.bootstrap_issued_at + MAX_ATTESTATION_LIFETIME,
            proof.attestation_expires_at,
            authority.expires_at,
        )
    ):
        raise TaskImageProjectionAuthorizationError("stored task-image bootstrap receipt changed")


def _require_exchange_timing(
    row: TaskImageBuildProjection,
    request: TaskImageBootstrapExchangeV1,
    *,
    now: datetime,
) -> None:
    _require_projected_secrets(row)
    if row.bootstrap_issued_at is None:
        raise TaskImageProjectionAuthorizationError("task-image projection lacks bootstrap timing")
    if not (row.bootstrap_issued_at <= request.observed_at <= now):
        raise TaskImageProjectionAuthorizationError(
            "task-image bootstrap exchange observation is invalid"
        )
    deadlines = (row.bootstrap_expires_at, row.attestation_expires_at)
    if any(deadline is None or now >= deadline for deadline in deadlines):
        raise TaskImageProjectionExpiredError("task-image bootstrap or attestation expired")


def _require_bootstrap_token(
    row: TaskImageBuildProjection,
    *,
    raw_token: str,
) -> None:
    if row.bootstrap_token_hash is None or not hmac.compare_digest(
        hashlib.sha256(raw_token.encode("utf-8")).digest(),
        row.bootstrap_token_hash,
    ):
        raise TaskImageProjectionAuthorizationError(
            "task-image bootstrap credential is not authorized"
        )


def _parse_stored_build_session(
    *,
    grant_id: UUID,
    generation: int,
    session_id: UUID,
    token_hash: bytes,
    session_json: dict[str, object],
    session_sha256: str,
    issued_at: datetime,
    expires_at: datetime,
    authority: GrantAuthority,
    raw_session_token: str,
    initial_not_before: datetime | None = None,
    initial_not_after: datetime | None = None,
) -> BuildSession:
    if not hmac.compare_digest(
        hashlib.sha256(raw_session_token.encode("utf-8")).digest(), token_hash
    ):
        raise TaskImageProjectionAuthorizationError(
            "task-image build session credential is not authorized"
        )
    public_binding = dict(session_json)
    token_sha256 = public_binding.pop("session_token_sha256", None)
    if not isinstance(token_sha256, str) or not hmac.compare_digest(token_sha256, token_hash.hex()):
        raise TaskImageProjectionAuthorizationError("stored task-image build session changed")
    public_binding["session_token"] = raw_session_token
    model: type[TaskImageBuildSessionV1] | type[TaskImageBuildSessionV2]
    model = (
        TaskImageBuildSessionV2
        if public_binding.get("schema_version") == 2
        else TaskImageBuildSessionV1
    )
    try:
        build_session = model.model_validate_json(json.dumps(public_binding))
    except ValidationError:
        raise TaskImageProjectionAuthorizationError(
            "stored task-image build session is invalid"
        ) from None
    effective_generation = (
        build_session.generation if isinstance(build_session, TaskImageBuildSessionV2) else 1
    )
    if (
        build_session.public_binding() != session_json
        or canonical_public_binding_sha256(build_session) != session_sha256
        or build_session.grant_id != grant_id
        or effective_generation != generation
        or build_session.session_id != session_id
        or build_session.purpose != authority.purpose
        or build_session.shadow_campaign_id != authority.shadow_campaign_id
        or build_session.pool_id != authority.pool_id
        or build_session.cpu_arch != authority.cpu_arch
        or build_session.issued_at != issued_at
        or build_session.expires_at != expires_at
        or build_session.expires_at > authority.expires_at
        or (initial_not_before is not None and build_session.issued_at < initial_not_before)
        or (initial_not_after is not None and build_session.expires_at > initial_not_after)
    ):
        raise TaskImageProjectionAuthorizationError("stored task-image build session changed")
    return build_session


def _stored_build_session(
    row: TaskImageBuildProjection,
    *,
    authority: GrantAuthority,
    raw_session_token: str,
) -> BuildSession:
    if (
        row.session_id is None
        or row.session_generation is None
        or row.session_token_hash is None
        or row.session_secret_ref is None
        or not isinstance(row.session_json, dict)
        or row.session_sha256 is None
        or row.session_issued_at is None
        or row.session_expires_at is None
        or row.bootstrap_issued_at is None
        or row.bootstrap_expires_at is None
    ):
        raise TaskImageProjectionAuthorizationError("stored task-image build session is incomplete")
    return _parse_stored_build_session(
        grant_id=row.grant_id,
        generation=row.session_generation,
        session_id=row.session_id,
        token_hash=row.session_token_hash,
        session_json=row.session_json,
        session_sha256=row.session_sha256,
        issued_at=row.session_issued_at,
        expires_at=row.session_expires_at,
        authority=authority,
        raw_session_token=raw_session_token,
        initial_not_before=(row.bootstrap_issued_at if row.session_generation == 1 else None),
        initial_not_after=(row.bootstrap_expires_at if row.session_generation == 1 else None),
    )


def _stored_generation_session(
    row: TaskImageBuildSessionGeneration,
    *,
    authority: GrantAuthority,
    raw_session_token: str,
) -> BuildSession:
    if not isinstance(row.session_json, dict):
        raise TaskImageProjectionAuthorizationError("stored task-image build session is invalid")
    return _parse_stored_build_session(
        grant_id=row.grant_id,
        generation=row.generation,
        session_id=row.session_id,
        token_hash=row.session_token_hash,
        session_json=row.session_json,
        session_sha256=row.session_sha256,
        issued_at=row.issued_at,
        expires_at=row.expires_at,
        authority=authority,
        raw_session_token=raw_session_token,
    )


async def _require_session_attestation(
    session: AsyncSession,
    *,
    build_session: BuildSession,
) -> None:
    stored = await session.scalar(
        select(TaskImageBuildContainmentAttestation).where(
            TaskImageBuildContainmentAttestation.grant_id == build_session.grant_id,
            TaskImageBuildContainmentAttestation.generation == build_session.attestation_generation,
        )
    )
    if stored is None:
        raise TaskImageProjectionAuthorizationError(
            "stored task-image build session attestation is unavailable"
        )
    attestation = _stored_attestation(stored)
    if (
        canonical_authority_sha256(attestation) != build_session.attestation_sha256
        or build_session.issued_at < attestation.issued_at
        or build_session.expires_at > attestation.expires_at
    ):
        raise TaskImageProjectionAuthorizationError(
            "stored task-image build session attestation changed"
        )


async def _locked_current_session_generation(
    session: AsyncSession,
    *,
    row: TaskImageBuildProjection,
) -> TaskImageBuildSessionGeneration:
    if row.session_id is None or row.session_generation is None:
        raise TaskImageProjectionAuthorizationError(
            "task-image build session authority is incomplete"
        )
    generation = await session.scalar(
        select(TaskImageBuildSessionGeneration)
        .where(
            TaskImageBuildSessionGeneration.grant_id == row.grant_id,
            TaskImageBuildSessionGeneration.generation == row.session_generation,
            TaskImageBuildSessionGeneration.session_id == row.session_id,
        )
        .with_for_update()
    )
    if generation is None:
        raise TaskImageProjectionAuthorizationError(
            "task-image build session generation is unavailable"
        )
    if (
        generation.session_token_hash != row.session_token_hash
        or generation.session_secret_ref != row.session_secret_ref
        or generation.session_json != row.session_json
        or generation.session_sha256 != row.session_sha256
        or generation.attestation_generation != row.attestation_generation
        or generation.attestation_sha256 != row.attestation_sha256
        or generation.issued_at != row.session_issued_at
        or generation.expires_at != row.session_expires_at
    ):
        raise TaskImageProjectionAuthorizationError(
            "task-image build session current pointer changed"
        )
    return generation


def _require_stored_exchange(
    row: TaskImageBuildProjection,
    *,
    request: TaskImageBootstrapExchangeV1,
) -> None:
    if row.exchange_id is None or row.exchange_json is None or row.exchange_sha256 is None:
        raise TaskImageProjectionAuthorizationError(
            "stored task-image bootstrap exchange is incomplete"
        )
    if (
        request.public_binding() != row.exchange_json
        or canonical_public_binding_sha256(request) != row.exchange_sha256
        or request.exchange_id != row.exchange_id
        or request.grant_id != row.grant_id
        or request.proof_sha256 != row.proof_sha256
    ):
        raise TaskImageProjectionAuthorizationError("stored task-image bootstrap exchange changed")


async def _replay_exchange(
    session: AsyncSession,
    *,
    row: TaskImageBuildProjection,
    authority: GrantAuthority,
    request: TaskImageBootstrapExchangeV1,
    exchange_sha256: str,
    now: datetime,
    secret_store: SecretStore,
) -> TaskImageBuildSessionV2:
    if row.exchange_id != request.exchange_id or row.exchange_sha256 != exchange_sha256:
        raise TaskImageProjectionConflictError("task-image bootstrap exchange idempotency conflict")
    _require_stored_exchange(row, request=request)
    current_generation = await _locked_current_session_generation(session, row=row)
    if current_generation.generation != 1 or current_generation.renewal_id is not None:
        raise TaskImageProjectionConflictError(
            "task-image bootstrap exchange session is no longer current"
        )
    if row.session_secret_ref is None or row.session_expires_at is None:
        raise TaskImageProjectionAuthorizationError("stored task-image build session is incomplete")
    if now >= row.session_expires_at:
        raise TaskImageProjectionExpiredError("task-image build session replay expired")
    token = await secret_store.get(row.session_secret_ref)
    build_session = _stored_build_session(
        row,
        authority=authority,
        raw_session_token=token,
    )
    await _require_session_attestation(session, build_session=build_session)
    if not isinstance(build_session, TaskImageBuildSessionV2):
        raise TaskImageProjectionAuthorizationError(
            "task-image bootstrap exchange session version changed"
        )
    await _append_event_once(
        session,
        row=row,
        event_type="exchange_replayed",
        event_key="exchange",
        payload={"exchange_sha256": exchange_sha256},
        now=now,
    )
    await session.flush()
    return build_session


async def exchange_task_image_bootstrap(
    session: AsyncSession,
    *,
    principal: TaskImageGuardPrincipalV1,
    request: TaskImageBootstrapExchangeV1,
    now: datetime,
    secret_store: SecretStore,
    session_token_factory: Callable[[], str],
) -> TaskImageBuildSessionV2:
    """Consume one semantic bootstrap and issue one encrypted build session."""

    principal = _validated_principal(principal)
    request = _validated_exchange(request)
    grant = await _locked_grant(session, grant_id=request.grant_id)
    authority = _grant_authority(grant, now=now)
    _require_released_grant(grant)
    row = await _locked_projection(session, grant_id=grant.id)
    if row is None or row.state not in {"projected", "exchanged"}:
        raise TaskImageProjectionAuthorizationError(
            "task-image bootstrap projection is unavailable"
        )
    _require_principal(
        principal,
        cluster=row.slurm_cluster_id,
        node_name=row.node_name,
        principal_id=row.principal_id,
        principal_sha256=row.principal_sha256,
    )
    await _require_stored_projection_chain(
        session,
        grant=grant,
        row=row,
        authority=authority,
    )
    _require_exchange_timing(row, request, now=now)
    _require_bootstrap_token(row, raw_token=request.bootstrap_token)
    current_attestation = await _current_attestation(session, row=row)
    exchange_sha256 = canonical_public_binding_sha256(request)
    if row.state == "exchanged":
        return await _replay_exchange(
            session,
            row=row,
            authority=authority,
            request=request,
            exchange_sha256=exchange_sha256,
            now=now,
            secret_store=secret_store,
        )
    if request.proof_sha256 != row.proof_sha256:
        raise TaskImageProjectionAuthorizationError("task-image bootstrap exchange proof changed")

    token = session_token_factory()
    session_id = uuid4()
    if row.bootstrap_expires_at is None or row.attestation_sha256 is None:
        raise TaskImageProjectionAuthorizationError(
            "task-image projection lacks attestation authority"
        )
    expires_at = min(
        now + MAX_SESSION_LIFETIME,
        authority.expires_at,
        row.bootstrap_expires_at,
        current_attestation.expires_at,
    )
    try:
        build_session = TaskImageBuildSessionV2(
            schema_version=2,
            grant_id=grant.id,
            generation=1,
            session_id=session_id,
            purpose=authority.purpose,
            shadow_campaign_id=authority.shadow_campaign_id,
            pool_id=authority.pool_id,
            cpu_arch=authority.cpu_arch,
            session_token=token,
            attestation_generation=current_attestation.generation,
            attestation_sha256=row.attestation_sha256,
            issued_at=now,
            expires_at=expires_at,
        )
    except ValidationError:
        raise TaskImageProjectionAuthorizationError(
            "generated task-image build session is invalid"
        ) from None
    secret_ref = await secret_store.put(namespace="task-image-session", value=token)
    if not secret_ref.startswith("loom://task-image-session/"):
        raise ValueError("task-image session secret reference has wrong namespace")

    session_sha256 = canonical_public_binding_sha256(build_session)
    token_hash = hashlib.sha256(token.encode("utf-8")).digest()
    generation = TaskImageBuildSessionGeneration(
        grant_id=grant.id,
        generation=1,
        session_id=build_session.session_id,
        session_token_hash=token_hash,
        session_secret_ref=secret_ref,
        session_json=build_session.public_binding(),
        session_sha256=session_sha256,
        attestation_generation=current_attestation.generation,
        attestation_sha256=build_session.attestation_sha256,
        renewal_id=None,
        renewal_sha256=None,
        predecessor_session_id=None,
        issued_at=build_session.issued_at,
        expires_at=build_session.expires_at,
        recorded_at=now,
    )
    session.add(generation)
    await session.flush([generation])
    row.state = "exchanged"
    row.exchange_id = request.exchange_id
    row.exchange_json = request.public_binding()
    row.exchange_sha256 = exchange_sha256
    row.session_id = build_session.session_id
    row.session_generation = build_session.generation
    row.session_token_hash = token_hash
    row.session_secret_ref = secret_ref
    row.session_json = build_session.public_binding()
    row.session_sha256 = session_sha256
    row.session_issued_at = build_session.issued_at
    row.session_expires_at = build_session.expires_at
    _append_event(
        session,
        row=row,
        event_type="exchanged",
        event_key="exchange",
        payload={
            "exchange_sha256": exchange_sha256,
            "session_id": str(build_session.session_id),
        },
        now=now,
    )
    await session.flush()
    return build_session


def _same_attachment_identity(
    left: TaskImageContainmentAttestationV1,
    right: TaskImageContainmentAttestationV1,
) -> bool:
    return (
        left.grant_id == right.grant_id
        and left.node_name == right.node_name
        and left.node_boot_id == right.node_boot_id
        and left.slurm_cluster_id == right.slurm_cluster_id
        and left.slurm_job_id == right.slurm_job_id
        and left.cgroup_path == right.cgroup_path
        and left.cgroup_inode == right.cgroup_inode
        and left.attachment == right.attachment
    )


async def record_task_image_containment_attestation(
    session: AsyncSession,
    *,
    principal: TaskImageGuardPrincipalV1,
    attestation: TaskImageContainmentAttestationV1,
    now: datetime,
) -> TaskImageContainmentAttestationV1:
    """Append the next exact containment generation or replay the current one."""

    principal = _validated_principal(principal)
    attestation = _validated_attestation(attestation)
    grant = await _locked_grant(session, grant_id=attestation.grant_id)
    authority = _grant_authority(grant, now=now)
    _require_released_grant(grant)
    row = await _locked_projection(session, grant_id=grant.id)
    if row is None or row.state not in {"projected", "exchanged"}:
        raise TaskImageProjectionAuthorizationError(
            "task-image projection cannot accept attestations"
        )
    _require_principal(
        principal,
        cluster=row.slurm_cluster_id,
        node_name=row.node_name,
        principal_id=row.principal_id,
        principal_sha256=row.principal_sha256,
        required_scope=_ATTEST_SCOPE,
    )
    await _require_stored_projection_chain(
        session,
        grant=grant,
        row=row,
        authority=authority,
    )
    current = await _current_attestation(session, row=row)
    candidate_sha256 = canonical_authority_sha256(attestation)
    if attestation.generation == current.generation:
        if candidate_sha256 != row.attestation_sha256:
            row.state = "revoked"
            row.revoked_at = now
            row.revoke_reason = "attestation_equivocation"
            _append_event(
                session,
                row=row,
                event_type="revoked",
                event_key="revocation",
                payload={"reason": "attestation_equivocation"},
                now=now,
            )
            await session.flush()
            raise TaskImageProjectionEquivocationError(
                "task-image containment attestation equivocated"
            )
        if now >= current.expires_at:
            raise TaskImageProjectionExpiredError("task-image containment attestation expired")
        await _append_event_once(
            session,
            row=row,
            event_type="attestation_replayed",
            event_key=str(attestation.generation),
            payload={"attestation_sha256": candidate_sha256},
            now=now,
        )
        await session.flush()
        return current
    if row.state == "exchanged":
        raise TaskImageProjectionAuthorizationError(
            "task-image session attestation requires atomic renewal"
        )
    if attestation.generation != current.generation + 1:
        raise TaskImageProjectionConflictError(
            "task-image containment attestation generation skipped"
        )
    if not _same_attachment_identity(attestation, current):
        raise TaskImageProjectionAuthorizationError(
            "task-image containment attachment identity changed"
        )
    if now >= current.expires_at:
        raise TaskImageProjectionExpiredError("task-image containment attestation renewal is stale")
    if not (current.issued_at < attestation.issued_at <= now):
        raise TaskImageProjectionAuthorizationError(
            "task-image containment attestation observation is invalid"
        )
    if attestation.expires_at <= current.expires_at:
        raise TaskImageProjectionAuthorizationError(
            "task-image containment attestation did not extend liveness"
        )
    if attestation.expires_at > authority.expires_at:
        raise TaskImageProjectionAuthorizationError(
            "task-image containment attestation exceeds grant authority"
        )

    session.add(
        TaskImageBuildContainmentAttestation(
            id=attestation.attestation_id,
            grant_id=grant.id,
            generation=attestation.generation,
            attestation_json=attestation.model_dump(mode="json", exclude_none=False),
            attestation_sha256=candidate_sha256,
            issued_at=attestation.issued_at,
            expires_at=attestation.expires_at,
            recorded_at=now,
        )
    )
    row.attestation_generation = attestation.generation
    row.attestation_sha256 = candidate_sha256
    row.attestation_expires_at = attestation.expires_at
    _append_event(
        session,
        row=row,
        event_type="attested",
        event_key=str(attestation.generation),
        payload={"attestation_sha256": candidate_sha256},
        now=now,
    )
    await session.flush()
    return attestation


async def _locked_session_generation(
    session: AsyncSession,
    *,
    grant_id: UUID,
    session_id: UUID,
    generation: int,
) -> TaskImageBuildSessionGeneration | None:
    stored_generation: TaskImageBuildSessionGeneration | None = await session.scalar(
        select(TaskImageBuildSessionGeneration)
        .where(
            TaskImageBuildSessionGeneration.grant_id == grant_id,
            TaskImageBuildSessionGeneration.session_id == session_id,
            TaskImageBuildSessionGeneration.generation == generation,
        )
        .with_for_update()
    )
    return stored_generation


def _require_next_renewal_attestation(
    *,
    current: TaskImageContainmentAttestationV1,
    candidate: TaskImageContainmentAttestationV1,
    authority: GrantAuthority,
    now: datetime,
) -> str:
    if candidate.generation != current.generation + 1:
        raise TaskImageProjectionConflictError(
            "task-image session renewal attestation generation skipped"
        )
    if not _same_attachment_identity(candidate, current):
        raise TaskImageProjectionAuthorizationError(
            "task-image session renewal attachment identity changed"
        )
    if now >= current.expires_at:
        raise TaskImageProjectionExpiredError("task-image session renewal attestation is stale")
    if not (current.issued_at < candidate.issued_at <= now):
        raise TaskImageProjectionAuthorizationError(
            "task-image session renewal observation is invalid"
        )
    if candidate.expires_at <= current.expires_at:
        raise TaskImageProjectionAuthorizationError(
            "task-image session renewal did not extend liveness"
        )
    if candidate.expires_at > authority.expires_at:
        raise TaskImageProjectionAuthorizationError(
            "task-image session renewal exceeds grant authority"
        )
    return canonical_authority_sha256(candidate)


async def renew_task_image_build_session(
    session: AsyncSession,
    *,
    principal: TaskImageGuardPrincipalV1,
    request: TaskImageSessionRenewalV1,
    now: datetime,
    secret_store: SecretStore,
    session_token_factory: Callable[[], str],
    session_id_factory: Callable[[], UUID],
) -> TaskImageBuildSessionV2:
    """Atomically advance containment and session authority by one generation."""

    principal = _validated_principal(principal)
    request = _validated_renewal(request)
    grant = await _locked_grant(session, grant_id=request.grant_id)
    authority = _grant_authority(grant, now=now)
    _require_released_grant(grant)
    projection = await _locked_projection(session, grant_id=grant.id)
    if projection is None or projection.state != "exchanged":
        raise TaskImageProjectionAuthorizationError("task-image build session is unavailable")
    _require_principal(
        principal,
        cluster=projection.slurm_cluster_id,
        node_name=projection.node_name,
        principal_id=projection.principal_id,
        principal_sha256=projection.principal_sha256,
        required_scope=_ATTEST_SCOPE,
    )
    current_generation = await _locked_current_session_generation(session, row=projection)
    await _require_stored_projection_chain(
        session,
        grant=grant,
        row=projection,
        authority=authority,
    )
    current_attestation = await _current_attestation(session, row=projection)
    renewal_sha256 = canonical_public_binding_sha256(request)
    replay_generation = await session.scalar(
        select(TaskImageBuildSessionGeneration)
        .where(TaskImageBuildSessionGeneration.renewal_id == request.renewal_id)
        .with_for_update()
    )
    if replay_generation is not None:
        if (
            replay_generation.grant_id != grant.id
            or replay_generation.renewal_sha256 != renewal_sha256
            or replay_generation.predecessor_session_id != request.session_id
            or replay_generation.generation != request.session_generation + 1
            or current_generation.id != replay_generation.id
        ):
            raise TaskImageProjectionConflictError(
                "task-image session renewal idempotency conflict"
            )
        predecessor = await _locked_session_generation(
            session,
            grant_id=grant.id,
            session_id=request.session_id,
            generation=request.session_generation,
        )
        if predecessor is None:
            raise TaskImageProjectionAuthorizationError(
                "task-image session renewal predecessor is unavailable"
            )
        _stored_generation_session(
            predecessor,
            authority=authority,
            raw_session_token=request.session_token,
        )
        if now >= replay_generation.expires_at or now >= current_attestation.expires_at:
            raise TaskImageProjectionExpiredError("task-image session renewal replay expired")
        token = await secret_store.get(replay_generation.session_secret_ref)
        build_session = _stored_generation_session(
            replay_generation,
            authority=authority,
            raw_session_token=token,
        )
        if not isinstance(build_session, TaskImageBuildSessionV2):
            raise TaskImageProjectionAuthorizationError(
                "task-image renewed session version changed"
            )
        await _require_session_attestation(session, build_session=build_session)
        await _append_event_once(
            session,
            row=projection,
            event_type="renewal_replayed",
            event_key=str(request.renewal_id),
            payload={"renewal_sha256": renewal_sha256},
            now=now,
        )
        await session.flush()
        return build_session

    if (
        request.session_id != current_generation.session_id
        or request.session_generation != current_generation.generation
    ):
        raise TaskImageProjectionAuthorizationError(
            "task-image session renewal credential is not authorized"
        )
    current_session = _stored_generation_session(
        current_generation,
        authority=authority,
        raw_session_token=request.session_token,
    )
    if (
        now >= current_session.expires_at
        or current_session.attestation_generation != current_attestation.generation
        or current_session.attestation_sha256 != canonical_authority_sha256(current_attestation)
    ):
        raise TaskImageProjectionExpiredError("task-image session renewal authority expired")
    next_attestation_sha256 = _require_next_renewal_attestation(
        current=current_attestation,
        candidate=request.attestation,
        authority=authority,
        now=now,
    )
    token = session_token_factory()
    next_session_id = session_id_factory()
    expires_at = min(
        now + MAX_SESSION_LIFETIME,
        authority.expires_at,
        request.attestation.expires_at,
    )
    try:
        build_session = TaskImageBuildSessionV2(
            schema_version=2,
            grant_id=grant.id,
            generation=current_generation.generation + 1,
            session_id=next_session_id,
            purpose=authority.purpose,
            shadow_campaign_id=authority.shadow_campaign_id,
            pool_id=authority.pool_id,
            cpu_arch=authority.cpu_arch,
            session_token=token,
            attestation_generation=request.attestation.generation,
            attestation_sha256=next_attestation_sha256,
            issued_at=now,
            expires_at=expires_at,
        )
    except ValidationError:
        raise TaskImageProjectionAuthorizationError(
            "generated task-image renewed session is invalid"
        ) from None
    if build_session.session_id == current_generation.session_id:
        raise TaskImageProjectionAuthorizationError(
            "generated task-image renewed session reused its predecessor"
        )
    secret_ref = await secret_store.put(namespace="task-image-session", value=token)
    if not secret_ref.startswith("loom://task-image-session/"):
        raise ValueError("task-image session secret reference has wrong namespace")
    token_hash = hashlib.sha256(token.encode("utf-8")).digest()
    session_sha256 = canonical_public_binding_sha256(build_session)
    next_attestation = TaskImageBuildContainmentAttestation(
        id=request.attestation.attestation_id,
        grant_id=grant.id,
        generation=request.attestation.generation,
        attestation_json=request.attestation.model_dump(mode="json", exclude_none=False),
        attestation_sha256=next_attestation_sha256,
        issued_at=request.attestation.issued_at,
        expires_at=request.attestation.expires_at,
        recorded_at=now,
    )
    session.add(next_attestation)
    await session.flush([next_attestation])
    next_generation = TaskImageBuildSessionGeneration(
        grant_id=grant.id,
        generation=build_session.generation,
        session_id=build_session.session_id,
        session_token_hash=token_hash,
        session_secret_ref=secret_ref,
        session_json=build_session.public_binding(),
        session_sha256=session_sha256,
        attestation_generation=request.attestation.generation,
        attestation_sha256=next_attestation_sha256,
        renewal_id=request.renewal_id,
        renewal_sha256=renewal_sha256,
        predecessor_session_id=current_generation.session_id,
        issued_at=build_session.issued_at,
        expires_at=build_session.expires_at,
        recorded_at=now,
    )
    session.add(next_generation)
    await session.flush([next_generation])
    projection.session_id = build_session.session_id
    projection.session_generation = build_session.generation
    projection.session_token_hash = token_hash
    projection.session_secret_ref = secret_ref
    projection.session_json = build_session.public_binding()
    projection.session_sha256 = session_sha256
    projection.session_issued_at = build_session.issued_at
    projection.session_expires_at = build_session.expires_at
    projection.attestation_generation = request.attestation.generation
    projection.attestation_sha256 = next_attestation_sha256
    projection.attestation_expires_at = request.attestation.expires_at
    _append_event(
        session,
        row=projection,
        event_type="renewed",
        event_key=str(request.renewal_id),
        payload={
            "renewal_sha256": renewal_sha256,
            "session_id": str(build_session.session_id),
            "session_generation": build_session.generation,
            "attestation_sha256": next_attestation_sha256,
        },
        now=now,
    )
    await session.flush()
    return build_session


async def authorize_task_image_build_session(
    session: AsyncSession,
    *,
    grant_id: UUID,
    session_id: UUID,
    session_generation: int,
    raw_session_token: str,
    now: datetime,
) -> TaskImageBuildSessionAuthorization:
    """Validate one bearer against its live grant and current attestation."""

    grant = await _locked_grant(session, grant_id=grant_id)
    authority = _grant_authority(grant, now=now)
    _require_released_grant(grant)
    row = await _locked_projection(session, grant_id=grant.id)
    if row is None or row.state != "exchanged":
        raise TaskImageProjectionAuthorizationError("task-image build session is unavailable")
    current_generation = await _locked_current_session_generation(session, row=row)
    await _require_stored_projection_chain(
        session,
        grant=grant,
        row=row,
        authority=authority,
    )
    if (
        row.session_id is None
        or row.session_generation is None
        or row.session_token_hash is None
        or row.session_expires_at is None
        or row.attestation_generation is None
        or row.attestation_sha256 is None
        or row.attestation_expires_at is None
    ):
        raise TaskImageProjectionAuthorizationError(
            "task-image build session authority is incomplete"
        )
    if (
        session_id != row.session_id
        or session_generation != row.session_generation
        or current_generation.session_id != row.session_id
        or current_generation.generation != row.session_generation
    ):
        raise TaskImageProjectionAuthorizationError(
            "task-image build session credential is not authorized"
        )
    if now >= row.session_expires_at or now >= row.attestation_expires_at:
        raise TaskImageProjectionExpiredError("task-image build session or attestation expired")
    build_session = _stored_build_session(
        row,
        authority=authority,
        raw_session_token=raw_session_token,
    )
    await _require_session_attestation(session, build_session=build_session)
    current = await _current_attestation(session, row=row)
    if (
        current.generation != row.attestation_generation
        or canonical_authority_sha256(current) != row.attestation_sha256
        or current.expires_at != row.attestation_expires_at
    ):
        raise TaskImageProjectionAuthorizationError(
            "task-image build session attestation binding changed"
        )
    return TaskImageBuildSessionAuthorization(
        grant_id=grant.id,
        session_id=build_session.session_id,
        session_generation=row.session_generation,
        authority_version=authority.schema_version,
        builder_release_sha256=(
            authority.builder_release_sha256
            if isinstance(authority, TaskImageBuildGrantAuthorityV2)
            else None
        ),
        supervisor_executable_sha256=(
            authority.supervisor_executable_sha256
            if isinstance(authority, TaskImageBuildGrantAuthorityV2)
            else authority.builder_release_sha256
        ),
        purpose=authority.purpose,
        shadow_campaign_id=authority.shadow_campaign_id,
        environment=authority.environment,
        pool_id=authority.pool_id,
        cpu_arch=authority.cpu_arch,
        attestation_generation=row.attestation_generation,
        attestation_sha256=row.attestation_sha256,
        attestation_expires_at=row.attestation_expires_at,
        session_expires_at=build_session.expires_at,
        grant_expires_at=authority.expires_at,
    )


async def revoke_task_image_projection(
    session: AsyncSession,
    *,
    principal: TaskImageGuardPrincipalV1,
    request: TaskImageProjectionRevocationV1,
    now: datetime,
) -> None:
    """Permanently revoke one projection while preserving all evidence."""

    principal = _validated_principal(principal)
    request = _validated_revocation(request)
    grant = await _locked_grant(session, grant_id=request.grant_id)
    authority = _grant_authority(grant, now=now)
    _require_released_grant(grant)
    row = await _locked_projection(session, grant_id=grant.id)
    if row is None:
        raise TaskImageProjectionAuthorizationError("task-image projection is unavailable")
    _require_principal(
        principal,
        cluster=row.slurm_cluster_id,
        node_name=row.node_name,
        principal_id=row.principal_id,
        principal_sha256=row.principal_sha256,
    )
    if request.observed_at > now:
        raise TaskImageProjectionAuthorizationError(
            "task-image projection revocation observation is in the future"
        )
    if now >= request.observed_at + MAX_CHALLENGE_LIFETIME:
        raise TaskImageProjectionExpiredError(
            "task-image projection revocation observation is stale"
        )
    if now >= min(_revocation_deadlines(row, authority)):
        raise TaskImageProjectionExpiredError("task-image projection revocation authority expired")
    if row.state == "revoked":
        if row.revoke_reason != request.reason:
            raise TaskImageProjectionConflictError(
                "task-image projection revocation reason changed"
            )
        return
    if row.state == "expired":
        raise TaskImageProjectionConflictError("task-image projection is expired")
    row.state = "revoked"
    row.revoked_at = now
    row.revoke_reason = request.reason
    _append_event(
        session,
        row=row,
        event_type="revoked",
        event_key="revocation",
        payload={
            "reason": request.reason,
            "request_sha256": canonical_authority_sha256(request),
        },
        now=now,
    )
    await session.flush()


def _revocation_deadlines(
    row: TaskImageBuildProjection,
    authority: GrantAuthority,
) -> tuple[datetime, ...]:
    deadlines = [authority.expires_at]
    if row.session_expires_at is not None:
        if row.attestation_expires_at is None:
            raise TaskImageProjectionAuthorizationError(
                "task-image session revocation deadline is incomplete"
            )
        deadlines.extend((row.session_expires_at, row.attestation_expires_at))
    elif row.bootstrap_expires_at is not None:
        if row.attestation_expires_at is None:
            raise TaskImageProjectionAuthorizationError(
                "task-image bootstrap revocation deadline is incomplete"
            )
        deadlines.extend((row.bootstrap_expires_at, row.attestation_expires_at))
    else:
        deadlines.append(row.challenge_expires_at)
    return tuple(deadlines)


def _projection_deadlines(
    row: TaskImageBuildProjection,
    authority: GrantAuthority,
) -> tuple[datetime, ...]:
    deadlines = [authority.expires_at]
    if row.state == "challenged":
        deadlines.append(row.challenge_expires_at)
    elif row.state == "projected":
        _require_projected_secrets(row)
        if row.bootstrap_expires_at is None or row.attestation_expires_at is None:
            raise TaskImageProjectionAuthorizationError(
                "task-image projection deadlines are incomplete"
            )
        deadlines.extend((row.bootstrap_expires_at, row.attestation_expires_at))
    elif row.state == "exchanged":
        _require_projected_secrets(row)
        if row.session_expires_at is None or row.attestation_expires_at is None:
            raise TaskImageProjectionAuthorizationError(
                "task-image session deadline is unavailable"
            )
        deadlines.extend((row.session_expires_at, row.attestation_expires_at))
    return tuple(deadlines)


async def expire_task_image_projection(
    session: AsyncSession,
    *,
    grant_id: UUID,
    now: datetime,
) -> None:
    """Record expiry only after the earliest applicable durable deadline."""

    grant = await _locked_grant(session, grant_id=grant_id)
    authority = _grant_authority(grant, now=now, require_live=False)
    row = await _locked_projection(session, grant_id=grant.id)
    if row is None:
        raise TaskImageProjectionAuthorizationError("task-image projection is unavailable")
    if row.state == "expired":
        return
    if row.state == "revoked":
        raise TaskImageProjectionConflictError("task-image projection is revoked")
    if now < min(_projection_deadlines(row, authority)):
        raise TaskImageProjectionConflictError(
            "task-image projection has not reached an expiry deadline"
        )
    row.state = "expired"
    row.expired_at = now
    _append_event(
        session,
        row=row,
        event_type="expired",
        event_key="expiration",
        payload={},
        now=now,
    )
    await session.flush()


__all__ = [
    "TaskImageBuildSessionAuthorization",
    "TaskImageProjectionAuthorizationError",
    "TaskImageProjectionConflictError",
    "TaskImageProjectionEquivocationError",
    "TaskImageProjectionExpiredError",
    "authorize_task_image_build_session",
    "complete_task_image_projection",
    "exchange_task_image_bootstrap",
    "expire_task_image_projection",
    "record_task_image_containment_attestation",
    "renew_task_image_build_session",
    "request_task_image_projection",
    "revoke_task_image_projection",
]
