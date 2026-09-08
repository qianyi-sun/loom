"""Renewable exact-repository publication credentials and inert candidates."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import UUID

import rfc8785
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImageMaterializationOperationEvent,
    TaskImagePublicationCandidate,
    TaskImageRegistryCredentialGeneration,
)
from loom.security.secret_store import InvalidRefError, SecretStore, parse_ref
from loom.task_image_build_plan import TaskImageBuildPlanV1
from loom_task_image_authority.contracts import (
    TaskImagePublicationCandidateRequestV1,
    TaskImageRegistryCredentialRequestV1,
    TaskImageRegistryCredentialV1,
    canonical_public_binding_sha256,
)
from loom_task_image_authority.http_contracts import (
    TaskImagePublicationCandidateResponseV1,
)
from loom_task_image_authority.materializations import (
    TaskImageBuildSessionAuthorization,
    TaskImageSessionMaterializationAuthorizationError,
    TaskImageSessionMaterializationConflictError,
    lock_current_task_image_build_session_authority,
    lock_session_materialization_lease,
)
from loom_task_image_authority.registry_token import (
    DistributionRegistryTokenIssuer,
    publication_repository,
)

_REGISTRY_CREDENTIAL_SECRET_NAMESPACE = "task-image-registry-credential"
_MAX_CREDENTIAL_LIFETIME = timedelta(seconds=45)


class TaskImageRegistryCredentialUnavailableError(RuntimeError):
    """Registry signing or encrypted replay state is unavailable."""


def _utc(value: datetime) -> datetime:
    if type(value) is not datetime or value.utcoffset() is None:
        raise ValueError("task-image registry operation time must be timezone-aware")
    return value.astimezone(UTC)


def _validate_credential_request(
    request: TaskImageRegistryCredentialRequestV1,
) -> TaskImageRegistryCredentialRequestV1:
    try:
        return TaskImageRegistryCredentialRequestV1.model_validate(
            request.model_dump(mode="python")
        )
    except (AttributeError, ValidationError):
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image registry credential request is invalid"
        ) from None


def _validate_candidate_request(
    request: TaskImagePublicationCandidateRequestV1,
) -> TaskImagePublicationCandidateRequestV1:
    try:
        return TaskImagePublicationCandidateRequestV1.model_validate(
            request.model_dump(mode="python")
        )
    except (AttributeError, ValidationError):
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image publication candidate request is invalid"
        ) from None


def _require_request_session(
    request: TaskImageRegistryCredentialRequestV1
    | TaskImagePublicationCandidateRequestV1,
    authorization: TaskImageBuildSessionAuthorization,
) -> None:
    if (
        request.grant_id != authorization.grant_id
        or request.session_id != authorization.session_id
        or request.session_generation != authorization.session_generation
    ):
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image registry session binding is unavailable"
        )


def _stored_claim_component(
    attempt: TaskImageMaterializationAttempt,
    row: TaskImageMaterialization,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    component: str,
) -> TaskImageBuildPlanV1:
    if attempt.claim_plan_json is None or attempt.claim_plan_sha256 is None:
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image claim receipt is unavailable"
        )
    try:
        plan = TaskImageBuildPlanV1.model_validate_json(json.dumps(attempt.claim_plan_json))
        payload = plan.model_dump(mode="json", exclude_none=False)
        digest = hashlib.sha256(rfc8785.dumps(payload)).hexdigest()
    except (TypeError, ValueError, ValidationError):
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image claim receipt is unavailable"
        ) from None
    if (
        payload != attempt.claim_plan_json
        or digest != attempt.claim_plan_sha256
        or attempt.grant_id != authorization.grant_id
        or plan.grant_id != authorization.grant_id
        or plan.session_id != attempt.session_id
        or plan.session_generation != attempt.session_generation
        or plan.materialization_id != row.id
        or plan.builder_id != attempt.builder_id
        or plan.cpu_arch != row.cpu_arch
        or plan.cpu_arch != authorization.cpu_arch
        or component not in {item.name for item in plan.components}
    ):
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image claim component is unavailable"
        )
    return plan


def _secret_ref_is_exact(ref: str) -> bool:
    try:
        parsed = parse_ref(ref)
    except (InvalidRefError, TypeError):
        return False
    return (
        parsed.namespace == _REGISTRY_CREDENTIAL_SECRET_NAMESPACE
        and parsed.secret_id.version == 4
        and parsed.as_string() == ref
    )


async def _delete_secret_quietly(secret_store: SecretStore, ref: str) -> None:
    try:
        await secret_store.delete(ref)
    except Exception:
        pass


def _credential_row_matches_request(
    row: TaskImageRegistryCredentialGeneration,
    *,
    request: TaskImageRegistryCredentialRequestV1,
    request_sha256: str,
) -> bool:
    return (
        hmac.compare_digest(row.request_sha256, request_sha256)
        and row.request_id == request.request_id
        and row.grant_id == request.grant_id
        and row.session_id == request.session_id
        and row.session_generation == request.session_generation
        and row.materialization_id == request.materialization_id
        and row.materialization_attempt_id == request.attempt_id
        and row.lease_epoch == request.lease_epoch
        and row.component == request.component
        and row.predecessor_credential_id == request.predecessor_credential_id
        and (row.generation - 1 if row.generation > 1 else None)
        == request.predecessor_generation
    )


async def _replay_credential(
    *,
    row: TaskImageRegistryCredentialGeneration,
    request: TaskImageRegistryCredentialRequestV1,
    request_sha256: str,
    now: datetime,
    secret_store: SecretStore,
) -> TaskImageRegistryCredentialV1:
    if not _credential_row_matches_request(
        row,
        request=request,
        request_sha256=request_sha256,
    ):
        raise TaskImageSessionMaterializationConflictError(
            "task-image registry credential request identity was already used"
        )
    if row.expires_at <= now:
        raise TaskImageSessionMaterializationConflictError(
            "task-image registry credential replay expired"
        )
    try:
        if not _secret_ref_is_exact(row.secret_response_ref):
            raise ValueError("stored registry credential reference changed")
        payload = await secret_store.get(row.secret_response_ref)
        credential = TaskImageRegistryCredentialV1.model_validate_json(payload)
        token_hash = hashlib.sha256(credential.bearer_token.encode("utf-8")).digest()
        if (
            credential.public_binding() != row.response_public_json
            or not hmac.compare_digest(
                canonical_public_binding_sha256(credential), row.response_sha256
            )
            or not hmac.compare_digest(token_hash, row.token_hash)
            or credential.credential_id != row.credential_id
            or credential.request_id != row.request_id
            or credential.grant_id != row.grant_id
            or credential.session_id != row.session_id
            or credential.session_generation != row.session_generation
            or credential.attestation_generation != row.attestation_generation
            or credential.attestation_sha256 != row.attestation_sha256
            or credential.materialization_id != row.materialization_id
            or credential.attempt_id != row.materialization_attempt_id
            or credential.attempt_number != row.attempt_number
            or credential.lease_epoch != row.lease_epoch
            or credential.builder_id != row.builder_id
            or credential.component != row.component
            or credential.generation != row.generation
            or credential.predecessor_credential_id
            != row.predecessor_credential_id
            or credential.predecessor_generation
            != (row.generation - 1 if row.generation > 1 else None)
            or credential.lease_heartbeat_operation_id
            != row.lease_heartbeat_operation_id
            or credential.repository != row.repository
            or credential.registry_origin != row.registry_origin
            or credential.registry_service != row.registry_service
            or credential.registry_issuer != row.registry_issuer
            or credential.registry_key_id != row.registry_key_id
            or credential.issued_at != row.issued_at
            or credential.expires_at != row.expires_at
        ):
            raise ValueError("stored registry credential changed")
    except Exception:
        raise TaskImageRegistryCredentialUnavailableError(
            "task-image registry credential replay is unavailable"
        ) from None
    return credential


async def issue_session_registry_credential(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    request: TaskImageRegistryCredentialRequestV1,
    now: datetime,
    issuer: DistributionRegistryTokenIssuer,
    secret_store: SecretStore,
    credential_id_factory: Callable[[], UUID],
) -> TaskImageRegistryCredentialV1:
    """Issue or exactly replay one renewable attempt/component capability."""

    now = _utc(now)
    request = _validate_credential_request(request)
    _require_request_session(request, authorization)
    await lock_current_task_image_build_session_authority(
        session,
        authorization=authorization,
        now=now,
    )
    row, attempt = await lock_session_materialization_lease(
        session,
        authorization=authorization,
        materialization_id=request.materialization_id,
        attempt_id=request.attempt_id,
        lease_epoch=request.lease_epoch,
        allowed_states=("claimed", "running"),
        now=now,
    )
    request_sha256 = canonical_public_binding_sha256(request)
    existing_request = await session.scalar(
        select(TaskImageRegistryCredentialGeneration).where(
            TaskImageRegistryCredentialGeneration.request_id == request.request_id
        )
    )
    if existing_request is not None and not _credential_row_matches_request(
        existing_request,
        request=request,
        request_sha256=request_sha256,
    ):
        raise TaskImageSessionMaterializationConflictError(
            "task-image registry credential request identity was already used"
        )
    plan = _stored_claim_component(
        attempt,
        row,
        authorization=authorization,
        component=request.component,
    )
    latest = await session.scalar(
        select(TaskImageRegistryCredentialGeneration)
        .where(
            TaskImageRegistryCredentialGeneration.materialization_attempt_id
            == attempt.id,
            TaskImageRegistryCredentialGeneration.component == request.component,
        )
        .order_by(TaskImageRegistryCredentialGeneration.generation.desc())
        .limit(1)
        .with_for_update()
    )
    replay = await session.scalar(
        select(TaskImageRegistryCredentialGeneration)
        .where(TaskImageRegistryCredentialGeneration.request_id == request.request_id)
        .with_for_update()
    )
    if replay is not None:
        return await _replay_credential(
            row=replay,
            request=request,
            request_sha256=request_sha256,
            now=now,
            secret_store=secret_store,
        )

    heartbeat: TaskImageMaterializationOperationEvent | None = None
    if latest is None:
        if request.predecessor_credential_id is not None:
            raise TaskImageSessionMaterializationConflictError(
                "task-image registry credential predecessor is unavailable"
            )
        generation = 1
    else:
        if (
            request.predecessor_credential_id != latest.credential_id
            or request.predecessor_generation != latest.generation
        ):
            raise TaskImageSessionMaterializationConflictError(
                "task-image registry credential predecessor is unavailable"
            )
        generation = latest.generation + 1
        if generation > 512:
            raise TaskImageSessionMaterializationConflictError(
                "task-image registry credential generation limit reached"
            )
        if (
            authorization.session_generation <= latest.session_generation
            or authorization.attestation_generation <= latest.attestation_generation
        ):
            raise TaskImageSessionMaterializationAuthorizationError(
                "task-image registry credential renewal requires newer liveness"
            )
        heartbeat = await session.scalar(
            select(TaskImageMaterializationOperationEvent)
            .where(
                TaskImageMaterializationOperationEvent.operation_type == "heartbeat",
                TaskImageMaterializationOperationEvent.materialization_attempt_id
                == attempt.id,
                TaskImageMaterializationOperationEvent.materialization_id == row.id,
                TaskImageMaterializationOperationEvent.attempt_number
                == attempt.attempt_number,
                TaskImageMaterializationOperationEvent.lease_epoch == attempt.lease_epoch,
                TaskImageMaterializationOperationEvent.builder_id == attempt.builder_id,
                TaskImageMaterializationOperationEvent.grant_id
                == authorization.grant_id,
                TaskImageMaterializationOperationEvent.session_id
                == authorization.session_id,
                TaskImageMaterializationOperationEvent.session_generation
                == authorization.session_generation,
                TaskImageMaterializationOperationEvent.recorded_at > latest.issued_at,
            )
            .order_by(TaskImageMaterializationOperationEvent.recorded_at.desc())
            .limit(1)
            .with_for_update()
        )
        if heartbeat is None:
            raise TaskImageSessionMaterializationAuthorizationError(
                "task-image registry credential renewal heartbeat is unavailable"
            )

    issued_at = now.replace(microsecond=0)
    if row.lease_expires_at is None:
        raise TaskImageSessionMaterializationConflictError(
            "stale task-image session materialization lease"
        )
    deadlines = (
        issued_at + _MAX_CREDENTIAL_LIFETIME,
        authorization.grant_expires_at,
        authorization.session_expires_at,
        authorization.attestation_expires_at,
        row.lease_expires_at,
    )
    expires_at = min(deadline.replace(microsecond=0) for deadline in deadlines)
    if expires_at <= now or expires_at <= issued_at:
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image registry credential lifetime is unavailable"
        )
    credential_id = credential_id_factory()
    repository = publication_repository(
        purpose=authorization.purpose,
        shadow_campaign_id=authorization.shadow_campaign_id,
        cpu_arch=plan.cpu_arch,
        attempt_id=attempt.id,
        component=request.component,
    )
    issued = issuer.issue(
        credential_id=credential_id,
        repository=repository,
        issued_at=issued_at,
        expires_at=expires_at,
    )
    try:
        credential = TaskImageRegistryCredentialV1(
            credential_id=credential_id,
            request_id=request.request_id,
            grant_id=authorization.grant_id,
            session_id=authorization.session_id,
            session_generation=authorization.session_generation,
            attestation_generation=authorization.attestation_generation,
            attestation_sha256=authorization.attestation_sha256,
            materialization_id=row.id,
            attempt_id=attempt.id,
            attempt_number=attempt.attempt_number,
            lease_epoch=attempt.lease_epoch,
            builder_id=attempt.builder_id,
            purpose="production",
            shadow_campaign_id=None,
            cpu_arch=plan.cpu_arch,
            platform=plan.platform,
            component=request.component,
            generation=generation,
            predecessor_credential_id=(latest.credential_id if latest is not None else None),
            predecessor_generation=(latest.generation if latest is not None else None),
            lease_heartbeat_operation_id=(
                heartbeat.operation_id if heartbeat is not None else None
            ),
            registry_origin=issued.registry_origin,
            registry_service=issued.service,
            registry_issuer=issued.issuer,
            repository=repository,
            actions=("pull", "push"),
            registry_key_id=issued.key_id,
            bearer_token=issued.token,
            issued_at=issued_at,
            expires_at=expires_at,
        )
    except ValidationError:
        raise TaskImageRegistryCredentialUnavailableError(
            "generated task-image registry credential is invalid"
        ) from None

    payload = credential.model_dump_json()
    secret_ref = await secret_store.put(
        namespace=_REGISTRY_CREDENTIAL_SECRET_NAMESPACE,
        value=payload,
    )
    if not _secret_ref_is_exact(secret_ref):
        await _delete_secret_quietly(secret_store, secret_ref)
        raise TaskImageRegistryCredentialUnavailableError(
            "task-image registry credential storage is unavailable"
        )
    public_binding = credential.public_binding()
    stored = TaskImageRegistryCredentialGeneration(
        credential_id=credential.credential_id,
        request_id=request.request_id,
        materialization_attempt_id=attempt.id,
        materialization_id=row.id,
        attempt_number=attempt.attempt_number,
        lease_epoch=attempt.lease_epoch,
        builder_id=attempt.builder_id,
        grant_id=authorization.grant_id,
        session_id=authorization.session_id,
        session_generation=authorization.session_generation,
        attestation_generation=authorization.attestation_generation,
        attestation_sha256=authorization.attestation_sha256,
        component=request.component,
        generation=generation,
        predecessor_credential_id=(latest.credential_id if latest is not None else None),
        lease_heartbeat_operation_id=(
            heartbeat.operation_id if heartbeat is not None else None
        ),
        repository=repository,
        registry_origin=issued.registry_origin,
        registry_service=issued.service,
        registry_issuer=issued.issuer,
        registry_key_id=issued.key_id,
        request_sha256=request_sha256,
        response_public_json=public_binding,
        response_sha256=canonical_public_binding_sha256(credential),
        token_hash=hashlib.sha256(credential.bearer_token.encode("utf-8")).digest(),
        secret_response_ref=secret_ref,
        issued_at=issued_at,
        expires_at=expires_at,
        recorded_at=now,
    )
    session.add(stored)
    try:
        await session.flush([stored])
    except BaseException:
        await _delete_secret_quietly(secret_store, secret_ref)
        raise
    return credential


def _candidate_response_from_row(
    row: TaskImagePublicationCandidate,
    *,
    credential_generation: int,
) -> TaskImagePublicationCandidateResponseV1:
    try:
        response = TaskImagePublicationCandidateResponseV1.model_validate_json(
            json.dumps(row.response_json)
        )
    except ValidationError:
        raise TaskImageSessionMaterializationConflictError(
            "stored task-image publication candidate changed"
        ) from None
    payload = rfc8785.dumps(response.model_dump(mode="json", exclude_none=False))
    if (
        response.candidate_id != row.candidate_id
        or response.operation_id != row.operation_id
        or response.credential_id != row.credential_id
        or response.credential_generation != credential_generation
        or response.grant_id != row.grant_id
        or response.session_id != row.session_id
        or response.session_generation != row.session_generation
        or response.materialization_id != row.materialization_id
        or response.attempt_id != row.materialization_attempt_id
        or response.attempt_number != row.attempt_number
        or response.lease_epoch != row.lease_epoch
        or response.builder_id != row.builder_id
        or response.component != row.component
        or response.repository != row.repository
        or response.manifest_digest != row.manifest_digest
        or response.manifest_size != row.manifest_size
        or response.oci_file_sha256 != row.oci_file_sha256
        or response.oci_file_size != row.oci_file_size
        or response.platform != row.platform
        or response.recorded_at != row.recorded_at
        or not hmac.compare_digest(hashlib.sha256(payload).hexdigest(), row.response_sha256)
    ):
        raise TaskImageSessionMaterializationConflictError(
            "stored task-image publication candidate changed"
        )
    return response


def _candidate_matches_request(
    response: TaskImagePublicationCandidateResponseV1,
    request: TaskImagePublicationCandidateRequestV1,
) -> bool:
    return (
        response.operation_id == request.operation_id
        and response.credential_id == request.credential_id
        and response.credential_generation == request.credential_generation
        and response.grant_id == request.grant_id
        and response.session_id == request.session_id
        and response.session_generation == request.session_generation
        and response.materialization_id == request.materialization_id
        and response.attempt_id == request.attempt_id
        and response.lease_epoch == request.lease_epoch
        and response.component == request.component
        and response.manifest_digest == request.manifest_digest
        and response.manifest_size == request.manifest_size
        and response.oci_file_sha256 == request.oci_file_sha256
        and response.oci_file_size == request.oci_file_size
        and response.platform == request.platform
    )


async def record_session_publication_candidate(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    request: TaskImagePublicationCandidateRequestV1,
    now: datetime,
    candidate_id_factory: Callable[[], UUID],
) -> TaskImagePublicationCandidateResponseV1:
    """Record inert upload evidence without granting readiness."""

    now = _utc(now)
    request = _validate_candidate_request(request)
    _require_request_session(request, authorization)
    await lock_current_task_image_build_session_authority(
        session,
        authorization=authorization,
        now=now,
    )
    materialization, attempt = await lock_session_materialization_lease(
        session,
        authorization=authorization,
        materialization_id=request.materialization_id,
        attempt_id=request.attempt_id,
        lease_epoch=request.lease_epoch,
        allowed_states=("claimed", "running"),
        now=now,
    )
    plan = _stored_claim_component(
        attempt,
        materialization,
        authorization=authorization,
        component=request.component,
    )
    repository = publication_repository(
        purpose=authorization.purpose,
        shadow_campaign_id=authorization.shadow_campaign_id,
        cpu_arch=plan.cpu_arch,
        attempt_id=attempt.id,
        component=request.component,
    )
    if request.platform != plan.platform:
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image publication candidate platform is unavailable"
        )
    credential = await session.scalar(
        select(TaskImageRegistryCredentialGeneration)
        .where(TaskImageRegistryCredentialGeneration.credential_id == request.credential_id)
        .with_for_update()
    )
    if (
        credential is None
        or credential.materialization_attempt_id != attempt.id
        or credential.materialization_id != materialization.id
        or credential.lease_epoch != attempt.lease_epoch
        or credential.grant_id != authorization.grant_id
        or credential.component != request.component
        or credential.generation != request.credential_generation
        or credential.repository != repository
    ):
        raise TaskImageSessionMaterializationAuthorizationError(
            "task-image publication credential is unavailable"
        )

    replay = await session.scalar(
        select(TaskImagePublicationCandidate)
        .where(TaskImagePublicationCandidate.operation_id == request.operation_id)
        .with_for_update()
    )
    if replay is not None:
        response = _candidate_response_from_row(
            replay,
            credential_generation=credential.generation,
        )
        if not _candidate_matches_request(response, request):
            raise TaskImageSessionMaterializationConflictError(
                "task-image publication operation identity was already used"
            )
        return response
    existing = await session.scalar(
        select(TaskImagePublicationCandidate)
        .where(
            TaskImagePublicationCandidate.materialization_attempt_id == attempt.id,
            TaskImagePublicationCandidate.component == request.component,
        )
        .with_for_update()
    )
    if existing is not None:
        raise TaskImageSessionMaterializationConflictError(
            "task-image publication component candidate already exists"
        )

    try:
        response = TaskImagePublicationCandidateResponseV1(
            candidate_id=candidate_id_factory(),
            operation_id=request.operation_id,
            credential_id=credential.credential_id,
            credential_generation=credential.generation,
            grant_id=authorization.grant_id,
            session_id=authorization.session_id,
            session_generation=authorization.session_generation,
            materialization_id=materialization.id,
            attempt_id=attempt.id,
            attempt_number=attempt.attempt_number,
            lease_epoch=attempt.lease_epoch,
            builder_id=attempt.builder_id,
            component=request.component,
            repository=repository,
            manifest_digest=request.manifest_digest,
            manifest_size=request.manifest_size,
            oci_file_sha256=request.oci_file_sha256,
            oci_file_size=request.oci_file_size,
            platform=request.platform,
            recorded_at=now,
        )
    except ValidationError:
        raise TaskImageSessionMaterializationAuthorizationError(
            "generated task-image publication candidate is invalid"
        ) from None
    response_json = response.model_dump(mode="json", exclude_none=False)
    response_sha256 = hashlib.sha256(rfc8785.dumps(response_json)).hexdigest()
    stored = TaskImagePublicationCandidate(
        candidate_id=response.candidate_id,
        operation_id=response.operation_id,
        credential_id=response.credential_id,
        materialization_attempt_id=attempt.id,
        materialization_id=materialization.id,
        attempt_number=attempt.attempt_number,
        lease_epoch=attempt.lease_epoch,
        builder_id=attempt.builder_id,
        grant_id=authorization.grant_id,
        session_id=authorization.session_id,
        session_generation=authorization.session_generation,
        component=request.component,
        repository=repository,
        manifest_digest=request.manifest_digest,
        manifest_size=request.manifest_size,
        oci_file_sha256=request.oci_file_sha256,
        oci_file_size=request.oci_file_size,
        platform=request.platform,
        response_json=response_json,
        response_sha256=response_sha256,
        recorded_at=now,
    )
    session.add(stored)
    await session.flush([stored])
    return response


__all__ = [
    "TaskImageRegistryCredentialUnavailableError",
    "issue_session_registry_credential",
    "record_session_publication_candidate",
]
