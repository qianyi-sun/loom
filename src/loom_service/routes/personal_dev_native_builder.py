"""Signature-gated internal API for the isolated native personal-dev builder."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal, Protocol, cast
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import PersonalDevNativeBuildGrant
from loom.personal_dev_builder_runtime import PersonalDevBuildCapabilityProvider
from loom.personal_dev_candidate import CandidateRegistration, PersonalDevPlatform
from loom.personal_dev_candidate_store import SqlAlchemyPersonalDevCandidateStore
from loom.personal_dev_native_builder_protocol import (
    NATIVE_BUILDER_PLATFORM,
    NativeBuilderAgentStatus,
    NativeBuilderCompletion,
    NativeBuilderGrantPayload,
    NativeBuilderHeartbeatRequest,
    NativeBuilderPollRequest,
    NativeBuilderRuntimeEvidence,
    PersonalDevNativeBuilderVerifier,
)
from loom.personal_dev_native_builder_store import (
    NativeBuilderArtifactHead,
    NativeBuilderGrantFencedError,
    NativeBuilderPollResult,
    complete_native_build_grant,
    get_native_build_grant,
    heartbeat_native_build_grant,
    poll_native_build_grant,
)

logger = logging.getLogger(__name__)
router = APIRouter(include_in_schema=False)


class NativeBuilderAgentStatusPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent_instance_id: UUID
    agent_key_id: str
    provider: str
    platform: str
    protocol_version: int
    host_name: str
    host_architecture: str
    host_boot_id: UUID
    agent_image: str
    builder_image: str
    runtime_profile_sha256: str
    max_concurrency: int
    managed_grant_ids: list[UUID] = Field(max_length=64)
    active_grant_ids: list[UUID] = Field(max_length=2)
    available: bool
    unavailable_reason: str | None
    readiness_evidence_sha256: str


class NativeBuilderPollPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    status: NativeBuilderAgentStatusPayload
    requested_at: datetime
    request_nonce: UUID


class NativeBuilderGrantResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    grant_id: UUID
    candidate_id: UUID
    candidate_sha: str
    attempt_id: UUID
    attempt_lease_epoch: int
    platform: str
    provider: str
    agent_instance_id: UUID
    agent_key_id: str
    builder_image: str
    runtime_profile_sha256: str
    contract_json: str
    contract_sha256: str
    source_get_url: str
    artifact_upload_url: str
    artifact_upload_fields: dict[str, str]
    artifact_max_bytes: int
    capability_expires_at: datetime
    active_deadline_seconds: int


class NativeBuilderPollResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    grant: NativeBuilderGrantResponse | None
    cancel_grant_ids: list[UUID]


class NativeBuilderHeartbeatPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    agent_instance_id: UUID
    agent_key_id: str
    grant_id: UUID
    attempt_id: UUID
    attempt_lease_epoch: int
    requested_at: datetime
    request_nonce: UUID


class NativeBuilderHeartbeatResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    continue_build: bool = Field(serialization_alias="continue")


class NativeBuilderRuntimeEvidencePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent_instance_id: UUID
    grant_id: UUID
    attempt_id: UUID
    attempt_lease_epoch: int
    provider: str
    platform: str
    host_name: str
    host_architecture: str
    host_boot_id: UUID
    agent_image: str
    builder_image: str
    runtime_profile_sha256: str
    contract_sha256: str
    runtime_name: str
    client_container_id: str
    buildkit_container_id: str
    network_id: str
    client_inspect_sha256: str
    buildkit_inspect_sha256: str
    network_inspect_sha256: str
    client_exit_code: int
    client_oom_killed: bool
    client_restart_count: int
    buildkit_restart_count: int
    buildkit_running: bool
    observed_at: datetime


class NativeBuilderCompletionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    agent_instance_id: UUID
    agent_key_id: str
    grant_id: UUID
    attempt_id: UUID
    attempt_lease_epoch: int
    outcome: Literal["succeeded", "failed"]
    failure_reason: str | None
    evidence: NativeBuilderRuntimeEvidencePayload | None
    requested_at: datetime
    request_nonce: UUID


class NativeBuilderCompletionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    accepted: Literal[True] = True
    state: Literal["succeeded", "failed"]


class NativeBuilderPollStore(Protocol):
    async def poll(
        self,
        request: NativeBuilderPollRequest,
        now: datetime,
    ) -> NativeBuilderPollResult: ...

    async def registration_for_grant(
        self,
        grant: PersonalDevNativeBuildGrant,
    ) -> CandidateRegistration | None: ...

    async def heartbeat(
        self,
        request: NativeBuilderHeartbeatRequest,
        now: datetime,
    ) -> bool: ...

    async def grant_for_completion(
        self,
        completion: NativeBuilderCompletion,
    ) -> PersonalDevNativeBuildGrant | None: ...

    async def complete(
        self,
        completion: NativeBuilderCompletion,
        now: datetime,
        artifact_head: NativeBuilderArtifactHead | None,
    ) -> PersonalDevNativeBuildGrant: ...


class SqlAlchemyNativeBuilderPollStore:
    """Narrow route adapter over the durable native-builder store."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def poll(
        self,
        request: NativeBuilderPollRequest,
        now: datetime,
    ) -> NativeBuilderPollResult:
        return await poll_native_build_grant(self._session, request, now)

    async def registration_for_grant(
        self,
        grant: PersonalDevNativeBuildGrant,
    ) -> CandidateRegistration | None:
        registration = await SqlAlchemyPersonalDevCandidateStore(self._session).get(
            grant.candidate_id
        )
        if registration is None or registration.build_attempt is None:
            return None
        candidate = registration.candidate
        attempt = registration.build_attempt
        if (
            candidate.id != grant.candidate_id
            or candidate.object_bucket != grant.source_bucket
            or candidate.object_key != grant.source_object_key
            or attempt.id != grant.attempt_id
            or attempt.lease_epoch != grant.attempt_lease_epoch
            or attempt.state != "running"
        ):
            return None
        return registration

    async def heartbeat(
        self,
        request: NativeBuilderHeartbeatRequest,
        now: datetime,
    ) -> bool:
        return await heartbeat_native_build_grant(self._session, request, now)

    async def grant_for_completion(
        self,
        completion: NativeBuilderCompletion,
    ) -> PersonalDevNativeBuildGrant | None:
        grant = await get_native_build_grant(
            self._session,
            completion.attempt_id,
            completion.attempt_lease_epoch,
            cast(PersonalDevPlatform, NATIVE_BUILDER_PLATFORM),
        )
        if grant is None or (
            grant.id != completion.grant_id
            or grant.required_agent_instance_id != completion.agent_instance_id
            or grant.required_agent_key_id != completion.agent_key_id
        ):
            return None
        return grant

    async def complete(
        self,
        completion: NativeBuilderCompletion,
        now: datetime,
        artifact_head: NativeBuilderArtifactHead | None,
    ) -> PersonalDevNativeBuildGrant:
        return await complete_native_build_grant(
            self._session,
            completion,
            now,
            artifact_head,
        )


NativeBuilderPollStoreFactory = Callable[[AsyncSession], NativeBuilderPollStore]


def _poll_request(payload: NativeBuilderPollPayload) -> NativeBuilderPollRequest:
    status = payload.status
    return NativeBuilderPollRequest(
        status=NativeBuilderAgentStatus(
            agent_instance_id=status.agent_instance_id,
            agent_key_id=status.agent_key_id,
            provider=status.provider,
            platform=status.platform,
            protocol_version=status.protocol_version,
            host_name=status.host_name,
            host_architecture=status.host_architecture,
            host_boot_id=status.host_boot_id,
            agent_image=status.agent_image,
            builder_image=status.builder_image,
            runtime_profile_sha256=status.runtime_profile_sha256,
            max_concurrency=status.max_concurrency,
            managed_grant_ids=tuple(status.managed_grant_ids),
            active_grant_ids=tuple(status.active_grant_ids),
            available=status.available,
            unavailable_reason=status.unavailable_reason,
            readiness_evidence_sha256=status.readiness_evidence_sha256,
        ),
        requested_at=payload.requested_at,
        request_nonce=payload.request_nonce,
    )


def _heartbeat_request(
    payload: NativeBuilderHeartbeatPayload,
) -> NativeBuilderHeartbeatRequest:
    return NativeBuilderHeartbeatRequest(
        agent_instance_id=payload.agent_instance_id,
        agent_key_id=payload.agent_key_id,
        grant_id=payload.grant_id,
        attempt_id=payload.attempt_id,
        attempt_lease_epoch=payload.attempt_lease_epoch,
        requested_at=payload.requested_at,
        request_nonce=payload.request_nonce,
    )


def _runtime_evidence(
    payload: NativeBuilderRuntimeEvidencePayload,
) -> NativeBuilderRuntimeEvidence:
    return NativeBuilderRuntimeEvidence(
        agent_instance_id=payload.agent_instance_id,
        grant_id=payload.grant_id,
        attempt_id=payload.attempt_id,
        attempt_lease_epoch=payload.attempt_lease_epoch,
        provider=payload.provider,
        platform=payload.platform,
        host_name=payload.host_name,
        host_architecture=payload.host_architecture,
        host_boot_id=payload.host_boot_id,
        agent_image=payload.agent_image,
        builder_image=payload.builder_image,
        runtime_profile_sha256=payload.runtime_profile_sha256,
        contract_sha256=payload.contract_sha256,
        runtime_name=payload.runtime_name,
        client_container_id=payload.client_container_id,
        buildkit_container_id=payload.buildkit_container_id,
        network_id=payload.network_id,
        client_inspect_sha256=payload.client_inspect_sha256,
        buildkit_inspect_sha256=payload.buildkit_inspect_sha256,
        network_inspect_sha256=payload.network_inspect_sha256,
        client_exit_code=payload.client_exit_code,
        client_oom_killed=payload.client_oom_killed,
        client_restart_count=payload.client_restart_count,
        buildkit_restart_count=payload.buildkit_restart_count,
        buildkit_running=payload.buildkit_running,
        observed_at=payload.observed_at,
    )


def _completion_request(
    payload: NativeBuilderCompletionPayload,
) -> NativeBuilderCompletion:
    return NativeBuilderCompletion(
        agent_instance_id=payload.agent_instance_id,
        agent_key_id=payload.agent_key_id,
        grant_id=payload.grant_id,
        attempt_id=payload.attempt_id,
        attempt_lease_epoch=payload.attempt_lease_epoch,
        outcome=payload.outcome,
        failure_reason=payload.failure_reason,
        evidence=_runtime_evidence(payload.evidence) if payload.evidence is not None else None,
        requested_at=payload.requested_at,
        request_nonce=payload.request_nonce,
    )


def _store(request: Request, session: AsyncSession) -> NativeBuilderPollStore:
    factory = getattr(
        request.app.state,
        "personal_dev_native_builder_store_factory",
        None,
    )
    if factory is None:
        return SqlAlchemyNativeBuilderPollStore(session)
    if not callable(factory):
        raise HTTPException(
            status_code=503,
            detail="personal-dev native builder authority unavailable",
        )
    return cast(NativeBuilderPollStoreFactory, factory)(session)


def _verifier(request: Request) -> PersonalDevNativeBuilderVerifier:
    value = getattr(
        request.app.state,
        "personal_dev_native_builder_verifier",
        None,
    )
    if not isinstance(value, PersonalDevNativeBuilderVerifier):
        raise HTTPException(
            status_code=503,
            detail="personal-dev native builder authority unavailable",
        )
    return value


def _capabilities(request: Request) -> PersonalDevBuildCapabilityProvider:
    value = getattr(
        request.app.state,
        "personal_dev_native_builder_capabilities",
        None,
    )
    if value is None or not callable(getattr(value, "issue", None)):
        raise HTTPException(
            status_code=503,
            detail="personal-dev native builder capability authority unavailable",
        )
    return cast(PersonalDevBuildCapabilityProvider, value)


async def _verify_poll(
    payload: NativeBuilderPollPayload,
    *,
    request: Request,
    signature: str,
    now: datetime,
) -> NativeBuilderPollRequest:
    try:
        signed = _poll_request(payload)
        if await request.body() != signed.canonical_bytes():
            raise ValueError("noncanonical native builder poll")
        _verifier(request).verify_poll(signed, signature=signature, now=now)
        return signed
    except HTTPException:
        raise
    except ValueError:
        raise HTTPException(
            status_code=403,
            detail="personal-dev native builder poll is invalid",
        ) from None


async def _verify_heartbeat(
    payload: NativeBuilderHeartbeatPayload,
    *,
    grant_id: UUID,
    request: Request,
    signature: str,
    now: datetime,
) -> NativeBuilderHeartbeatRequest:
    try:
        signed = _heartbeat_request(payload)
        if grant_id != signed.grant_id or await request.body() != signed.canonical_bytes():
            raise ValueError("native builder heartbeat binding is invalid")
        _verifier(request).verify_heartbeat(signed, signature=signature, now=now)
        return signed
    except HTTPException:
        raise
    except ValueError:
        raise HTTPException(
            status_code=403,
            detail="personal-dev native builder heartbeat is invalid",
        ) from None


async def _verify_completion(
    payload: NativeBuilderCompletionPayload,
    *,
    grant_id: UUID,
    request: Request,
    signature: str,
    now: datetime,
) -> NativeBuilderCompletion:
    try:
        signed = _completion_request(payload)
        if grant_id != signed.grant_id or await request.body() != signed.canonical_bytes():
            raise ValueError("native builder completion binding is invalid")
        _verifier(request).verify_completion(signed, signature=signature, now=now)
        return signed
    except HTTPException:
        raise
    except ValueError:
        raise HTTPException(
            status_code=403,
            detail="personal-dev native builder completion is invalid",
        ) from None


async def _artifact_head(
    request: Request,
    grant: PersonalDevNativeBuildGrant,
) -> NativeBuilderArtifactHead:
    client = getattr(request.app.state, "minio_client", None)
    head_object = getattr(client, "head_object", None)
    if not callable(head_object):
        raise HTTPException(
            status_code=503,
            detail="personal-dev native builder object authority unavailable",
        )
    try:
        raw = await asyncio.to_thread(
            head_object,
            Bucket=grant.artifact_bucket,
            Key=grant.artifact_object_key,
        )
        if not isinstance(raw, Mapping):
            raise ValueError("native builder object head is invalid")
        raw_metadata = raw.get("Metadata")
        if not isinstance(raw_metadata, Mapping) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in raw_metadata.items()
        ):
            raise ValueError("native builder object metadata is invalid")
        content_type = raw.get("ContentType")
        size_bytes = raw.get("ContentLength")
        if not isinstance(content_type, str) or type(size_bytes) is not int:
            raise ValueError("native builder object head is invalid")
        return NativeBuilderArtifactHead(
            bucket=grant.artifact_bucket,
            object_key=grant.artifact_object_key,
            content_type=content_type,
            size_bytes=size_bytes,
            metadata=cast(Mapping[str, str], raw_metadata),
        )
    except HTTPException:
        raise
    except Exception:
        logger.error("personal_dev_native_builder_artifact_head_failed")
        raise HTTPException(
            status_code=503,
            detail="personal-dev native builder artifact head failed",
        ) from None


def _grant_response(payload: NativeBuilderGrantPayload) -> NativeBuilderGrantResponse:
    return NativeBuilderGrantResponse(
        grant_id=payload.grant_id,
        candidate_id=payload.candidate_id,
        candidate_sha=payload.candidate_sha,
        attempt_id=payload.attempt_id,
        attempt_lease_epoch=payload.attempt_lease_epoch,
        platform=payload.platform,
        provider=payload.provider,
        agent_instance_id=payload.agent_instance_id,
        agent_key_id=payload.agent_key_id,
        builder_image=payload.builder_image,
        runtime_profile_sha256=payload.runtime_profile_sha256,
        contract_json=payload.contract_json,
        contract_sha256=payload.contract_sha256,
        source_get_url=payload.source_get_url,
        artifact_upload_url=payload.artifact_upload_url,
        artifact_upload_fields=dict(payload.artifact_upload_fields),
        artifact_max_bytes=payload.artifact_max_bytes,
        capability_expires_at=payload.capability_expires_at,
        active_deadline_seconds=payload.active_deadline_seconds,
    )


@router.post(
    "/personal-dev/native-builder/poll",
    response_model=NativeBuilderPollResponse,
    responses={204: {"description": "No grant or cancellation is available"}},
)
async def poll_native_builder(
    payload: NativeBuilderPollPayload,
    request: Request,
    response: Response,
    signature: Annotated[str, Header(alias="X-Loom-Native-Builder-Signature")],
) -> NativeBuilderPollResponse | Response:
    """Persist one authenticated inventory and return at most one capability."""
    now = datetime.now(UTC)
    signed = await _verify_poll(payload, request=request, signature=signature, now=now)
    session_factory = getattr(request.app.state, "session_factory", None)
    if session_factory is None or not callable(session_factory):
        raise HTTPException(
            status_code=503,
            detail="personal-dev native builder authority unavailable",
        )
    try:
        async with session_factory() as session:
            store = _store(request, session)
            result = await store.poll(signed, now)
            registration = (
                await store.registration_for_grant(result.grant)
                if result.grant is not None
                else None
            )
    except NativeBuilderGrantFencedError:
        raise HTTPException(
            status_code=409,
            detail="personal-dev native builder poll was fenced",
        ) from None
    except HTTPException:
        raise
    except Exception:
        logger.error("personal_dev_native_builder_poll_failed")
        raise HTTPException(
            status_code=503,
            detail="personal-dev native builder poll failed",
        ) from None

    if result.grant is None:
        if result.cancel_grant_ids:
            return NativeBuilderPollResponse(
                grant=None,
                cancel_grant_ids=list(result.cancel_grant_ids),
            )
        return Response(status_code=204)
    grant = result.grant
    if registration is None or registration.build_attempt is None:
        raise HTTPException(
            status_code=409,
            detail="personal-dev native builder grant was fenced",
        )
    try:
        capability = await _capabilities(request).issue(
            registration,
            platform=NATIVE_BUILDER_PLATFORM,
        )
        if (
            capability.artifact_max_bytes != grant.artifact_max_bytes
            or capability.expires_at
            < now + timedelta(seconds=grant.active_deadline_seconds + 60)
        ):
            raise ValueError("native builder capability does not cover active deadline")
        secret = NativeBuilderGrantPayload(
            grant_id=grant.id,
            candidate_id=registration.candidate.id,
            candidate_sha=registration.candidate.candidate_sha,
            attempt_id=grant.attempt_id,
            attempt_lease_epoch=grant.attempt_lease_epoch,
            platform=grant.platform,
            provider=grant.provider,
            agent_instance_id=grant.required_agent_instance_id,
            agent_key_id=grant.required_agent_key_id,
            builder_image=grant.builder_image,
            runtime_profile_sha256=grant.runtime_profile_sha256,
            contract_json=grant.contract_json,
            contract_sha256=grant.contract_sha256,
            source_get_url=capability.source_get_url,
            artifact_upload_url=capability.artifact_upload_url,
            artifact_upload_fields=capability.artifact_upload_fields,
            artifact_max_bytes=capability.artifact_max_bytes,
            capability_expires_at=capability.expires_at,
            active_deadline_seconds=grant.active_deadline_seconds,
        )
    except HTTPException:
        raise
    except Exception:
        logger.error("personal_dev_native_builder_capability_issue_failed")
        raise HTTPException(
            status_code=503,
            detail="personal-dev native builder capability issue failed",
        ) from None
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return NativeBuilderPollResponse(
        grant=_grant_response(secret),
        cancel_grant_ids=list(result.cancel_grant_ids),
    )


@router.post(
    "/personal-dev/native-builder/grants/{grant_id}/heartbeat",
    response_model=NativeBuilderHeartbeatResponse,
)
async def heartbeat_native_builder(
    grant_id: UUID,
    payload: NativeBuilderHeartbeatPayload,
    request: Request,
    signature: Annotated[str, Header(alias="X-Loom-Native-Builder-Signature")],
) -> NativeBuilderHeartbeatResponse:
    """Refresh a running grant or direct the agent to cancel it."""
    now = datetime.now(UTC)
    signed = await _verify_heartbeat(
        payload,
        grant_id=grant_id,
        request=request,
        signature=signature,
        now=now,
    )
    session_factory = getattr(request.app.state, "session_factory", None)
    if session_factory is None or not callable(session_factory):
        raise HTTPException(
            status_code=503,
            detail="personal-dev native builder authority unavailable",
        )
    try:
        async with session_factory() as session:
            continue_build = await _store(request, session).heartbeat(signed, now)
    except NativeBuilderGrantFencedError:
        raise HTTPException(
            status_code=409,
            detail="personal-dev native builder heartbeat was fenced",
        ) from None
    except HTTPException:
        raise
    except Exception:
        logger.error("personal_dev_native_builder_heartbeat_failed")
        raise HTTPException(
            status_code=503,
            detail="personal-dev native builder heartbeat failed",
        ) from None
    return NativeBuilderHeartbeatResponse(continue_build=continue_build)


@router.post(
    "/personal-dev/native-builder/grants/{grant_id}/complete",
    response_model=NativeBuilderCompletionResponse,
)
async def complete_native_builder(
    grant_id: UUID,
    payload: NativeBuilderCompletionPayload,
    request: Request,
    signature: Annotated[str, Header(alias="X-Loom-Native-Builder-Signature")],
) -> NativeBuilderCompletionResponse:
    """Commit a signed terminal report after authoritative artifact verification."""
    now = datetime.now(UTC)
    completion = await _verify_completion(
        payload,
        grant_id=grant_id,
        request=request,
        signature=signature,
        now=now,
    )
    session_factory = getattr(request.app.state, "session_factory", None)
    if session_factory is None or not callable(session_factory):
        raise HTTPException(
            status_code=503,
            detail="personal-dev native builder authority unavailable",
        )

    artifact_head: NativeBuilderArtifactHead | None = None
    if completion.outcome == "succeeded":
        try:
            async with session_factory() as session:
                grant = await _store(request, session).grant_for_completion(completion)
        except HTTPException:
            raise
        except Exception:
            logger.error("personal_dev_native_builder_completion_lookup_failed")
            raise HTTPException(
                status_code=503,
                detail="personal-dev native builder completion lookup failed",
            ) from None
        if grant is None:
            raise HTTPException(
                status_code=409,
                detail="personal-dev native builder completion was fenced",
            )
        artifact_head = await _artifact_head(request, grant)

    try:
        async with session_factory() as session:
            completed = await _store(request, session).complete(
                completion,
                now,
                artifact_head,
            )
    except NativeBuilderGrantFencedError:
        raise HTTPException(
            status_code=409,
            detail="personal-dev native builder completion was fenced",
        ) from None
    except HTTPException:
        raise
    except Exception:
        logger.error("personal_dev_native_builder_completion_failed")
        raise HTTPException(
            status_code=503,
            detail="personal-dev native builder completion failed",
        ) from None
    if completed.state != completion.outcome:
        raise HTTPException(
            status_code=409,
            detail="personal-dev native builder completion was fenced",
        )
    return NativeBuilderCompletionResponse(state=completion.outcome)


__all__ = [
    "NativeBuilderAgentStatusPayload",
    "NativeBuilderCompletionPayload",
    "NativeBuilderCompletionResponse",
    "NativeBuilderGrantResponse",
    "NativeBuilderHeartbeatPayload",
    "NativeBuilderHeartbeatResponse",
    "NativeBuilderPollPayload",
    "NativeBuilderPollResponse",
    "NativeBuilderRuntimeEvidencePayload",
    "router",
]
