"""Controller-authenticated native admission and explicitly enabled bounded source IO."""

from __future__ import annotations

import asyncio
import base64
from typing import Literal
from uuid import UUID

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, HTTPException, Request, Response
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.ext.asyncio import async_sessionmaker

from loom_capacity_agent.admission import (
    ExecutableDrainRequestV2,
    ExecutablePreparedBootstrapRevocationV2,
    ExecutableWorkerWithdrawalRequestV2,
    PhysicalJobBindingV2,
)
from loom_capacity_agent.build_admission import (
    BuildAllocatedClaimExchangeV1,
    BuildClaimExchangeV1,
    BuildOutcomeExchangeV1,
    BuildPreparationRequestV1,
    BuildRegistrationRequestV1,
    BuildReleaseExchangeV1,
    BuildSourceReadExchangeV1,
    BuildSourceReadReceiptV1,
)
from loom_capacity_agent.build_artifact_stream import (
    ARTIFACT_STREAM_CONTENT_TYPE,
    BuildArtifactUploadReceiptV1,
    decode_artifact_stream,
)
from loom_capacity_build_guard.artifact_writer import BuildArtifactWriter
from loom_capacity_build_guard.execution_store import BuildGuardExecutionStore
from loom_capacity_build_guard.source_reader import BuildSourceReader
from loom_capacity_manager.auth import AuthorizationError, CapacityPrincipalVerifier
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from loom_capacity_manager.executable_contracts import (
    ExecutableIntentBindingV2,
    canonical_executable_bytes,
)

router = APIRouter(include_in_schema=False)
_MAX_REQUEST_BYTES = 1024 * 1024


async def _admit(
    request: Request,
    *,
    pool_id: str,
    intent_id: UUID,
    operation_name: Literal["prepare", "bind", "observe", "revoke-bootstrap", "withdraw", "register", "claim", "drain", "outcome", "release", "source", "context", "claim-assigned"],
) -> Response:
    sessions = getattr(request.app.state, "personal_dev_build_admission_sessions", None)
    verifier = getattr(request.app.state, "personal_dev_build_admission_verifier", None)
    if not isinstance(sessions, async_sessionmaker) or not isinstance(
        verifier, CapacityPrincipalVerifier
    ):
        raise HTTPException(503, "build admission unavailable")
    if operation_name in {"register", "drain", "release"} and getattr(
        request.app.state, "personal_dev_build_admission_mode", None
    ) not in {"native-registration", "native-claims", "native-source", "native-artifacts"}:
        raise HTTPException(503, "build registration unavailable")
    if operation_name in {"claim", "outcome"} and getattr(
        request.app.state, "personal_dev_build_admission_mode", None
    ) not in {"native-claims", "native-source", "native-artifacts"}:
        raise HTTPException(503, "native claims unavailable")
    source_reader = getattr(request.app.state, "personal_dev_build_source_reader", None)
    if operation_name in {"context", "claim-assigned"} and getattr(request.app.state, "personal_dev_build_admission_mode", None) not in {"native-source", "native-artifacts"}:
        raise HTTPException(503, "native context unavailable")
    if operation_name == "source" and (
        getattr(request.app.state, "personal_dev_build_admission_mode", None) not in {"native-source", "native-artifacts"}
        or not isinstance(source_reader, BuildSourceReader)
    ):
        raise HTTPException(503, "native source unavailable")
    if request.url.scheme != "https":
        raise HTTPException(403, "build admission requires TLS")
    if len(request.headers.getlist("authorization")) != 1:
        raise HTTPException(401, "invalid build admission credentials")
    try:
        principal = verifier.verify_bearer(request.headers.get("authorization"))
    except AuthorizationError:
        raise HTTPException(401, "invalid build admission credentials") from None
    if principal.scopes != frozenset({"capacity:execute:pool"}) or principal.pool_id != pool_id:
        raise HTTPException(403, "build admission identity changed")
    try:
        async with asyncio.timeout(30):
            body = bytearray()
            async for chunk in request.stream():
                if len(body) + len(chunk) > _MAX_REQUEST_BYTES:
                    raise HTTPException(413, "build admission request exceeds byte bound")
                body.extend(chunk)
            try:
                assigned_claim = (
                    BuildAllocatedClaimExchangeV1.model_validate_json(bytes(body))
                    if operation_name == "claim-assigned" else None
                )
                source_read = (
                    BuildSourceReadExchangeV1.model_validate_json(bytes(body))
                    if operation_name == "source" else None
                )
                release = (
                    BuildReleaseExchangeV1.model_validate_json(bytes(body))
                    if operation_name == "release" else None
                )
                outcome = (
                    BuildOutcomeExchangeV1.model_validate_json(bytes(body))
                    if operation_name == "outcome" else None
                )
                claim = (
                    BuildClaimExchangeV1.model_validate_json(bytes(body))
                    if operation_name in {"claim", "context"} else None
                )
                registration = (
                    BuildRegistrationRequestV1.model_validate_json(bytes(body))
                    if operation_name == "register" else None
                )
                preparation = (
                    BuildPreparationRequestV1.model_validate_json(bytes(body))
                    if operation_name == "prepare"
                    else None
                )
                operation = (
                    assigned_claim.claim
                    if assigned_claim is not None
                    else source_read.claim
                    if source_read is not None
                    else release.release
                    if release is not None
                    else outcome.outcome.claim
                    if outcome is not None
                    else claim.claim
                    if claim is not None
                    else registration.registration
                    if registration is not None
                    else preparation.registration
                    if preparation is not None
                    else ExecutableIntentBindingV2.model_validate_json(bytes(body))
                    if operation_name == "observe"
                    else ExecutablePreparedBootstrapRevocationV2.model_validate_json(bytes(body))
                    if operation_name == "revoke-bootstrap"
                    else ExecutableWorkerWithdrawalRequestV2.model_validate_json(bytes(body))
                    if operation_name == "withdraw"
                    else ExecutableDrainRequestV2.model_validate_json(bytes(body))
                    if operation_name == "drain"
                    else PhysicalJobBindingV2.model_validate_json(bytes(body))
                )
            except ValueError:
                raise HTTPException(400, "invalid build admission request") from None
            binding = (
                operation if isinstance(operation, ExecutableIntentBindingV2) else operation.binding
            )
            if (
                binding.intent_id != intent_id
                or binding.pool_id != pool_id
                or not principal.matches_executor(
                    pool_id=binding.pool_id,
                    executor_id=binding.executor_id,
                    executor_incarnation=binding.executor_incarnation,
                    pool_generation=binding.pool_generation,
                )
            ):
                raise HTTPException(403, "build admission identity changed")
            if source_read is not None:
                assert isinstance(source_reader, BuildSourceReader)
                source_chunk = await source_reader.read(source_read.claim, worker_credential=source_read.worker_credential,
                    offset=source_read.offset, length=source_read.length)
                result = BuildSourceReadReceiptV1(claim_digest=source_chunk.source.claim_digest,
                    source_binding_sha256=source_chunk.source.source_binding_sha256,
                    archive_sha256=source_chunk.source.archive_sha256, archive_size_bytes=source_chunk.source.archive_size_bytes,
                    offset=source_chunk.offset, data_base64=base64.b64encode(source_chunk.data).decode("ascii"))
                return Response(canonical_bytes(result), media_type="application/json",
                    headers={"Cache-Control": "no-store"})
            async with sessions.begin() as session:
                await session.execute(text("SET LOCAL statement_timeout='10000ms'"))
                await session.execute(text("SET LOCAL lock_timeout='5000ms'"))
                store = BuildGuardExecutionStore(session, binding=binding)
                if assigned_claim is not None:
                    wire = canonical_bytes(await store.claim_assigned_platform(
                        assigned_claim.claim, worker_credential=assigned_claim.worker_credential))
                elif release is not None:
                    wire = canonical_executable_bytes(await store.acknowledge_release(
                        release.release, current_worker_credential=release.worker_credential))
                elif outcome is not None:
                    wire = canonical_bytes(await store.record_outcome(
                        outcome.outcome, worker_credential=outcome.worker_credential))
                elif claim is not None and operation_name == "context":
                    wire = canonical_bytes(await store.read_source_context(
                        claim.claim, worker_credential=claim.worker_credential))
                elif claim is not None:
                    wire = canonical_bytes(await store.claim_platform(
                        claim.claim, worker_credential=claim.worker_credential))
                elif registration is not None:
                    wire = canonical_executable_bytes(await store.register_worker(
                        registration.registration, bootstrap_capability=registration.bootstrap_capability
                    ))
                elif preparation is not None:
                    wire = canonical_executable_bytes(
                        await store.prepare_worker(
                            preparation.registration, bootstrap_sha256=preparation.bootstrap_sha256
                        )
                    )
                elif isinstance(operation, ExecutableIntentBindingV2):
                    wire = canonical_executable_bytes(await store.observe_intent(operation))
                elif isinstance(operation, ExecutablePreparedBootstrapRevocationV2):
                    wire = canonical_executable_bytes(
                        await store.revoke_prepared_bootstrap(operation)
                    )
                elif isinstance(operation, ExecutableWorkerWithdrawalRequestV2):
                    wire = canonical_executable_bytes(await store.withdraw_unregistered_worker(operation))
                elif isinstance(operation, ExecutableDrainRequestV2):
                    wire = canonical_executable_bytes(await store.begin_drain(operation))
                else:
                    assert isinstance(operation, PhysicalJobBindingV2)
                    wire = canonical_executable_bytes(await store.bind_slurm_job(operation))
            # Context exit commits. Never send a preparation receipt from an
            # uncommitted transaction that could be followed by scheduler submit.
            return Response(wire, media_type="application/json",
                headers={"Cache-Control": "no-store"} if operation_name in {"context", "claim-assigned"} else None)
    except (DBAPIError, ValueError):
        raise HTTPException(409, "build admission evidence unavailable or changed") from None
    except (TimeoutError, PoolTimeoutError, BotoCoreError, ClientError):
        raise HTTPException(503, "build admission deadline exceeded") from None


@router.post("/capacity-build/pools/{pool_id}/intents/{intent_id}/prepare")
async def prepare_build(request: Request, pool_id: str, intent_id: UUID) -> Response:
    return await _admit(request, pool_id=pool_id, intent_id=intent_id, operation_name="prepare")


@router.post("/capacity-build/pools/{pool_id}/intents/{intent_id}/artifact")
async def upload_build_artifact(request: Request, pool_id: str, intent_id: UUID) -> Response:
    writer = getattr(request.app.state, "personal_dev_build_artifact_writer", None)
    verifier = getattr(request.app.state, "personal_dev_build_admission_verifier", None)
    if (getattr(request.app.state, "personal_dev_build_admission_mode", None) != "native-artifacts"
        or not isinstance(writer, BuildArtifactWriter) or not isinstance(verifier, CapacityPrincipalVerifier)):
        raise HTTPException(503, "native artifact upload unavailable")
    if request.url.scheme != "https":
        raise HTTPException(403, "build admission requires TLS")
    if len(request.headers.getlist("authorization")) != 1:
        raise HTTPException(401, "invalid build admission credentials")
    try:
        principal = verifier.verify_bearer(request.headers.get("authorization"))
    except AuthorizationError:
        raise HTTPException(401, "invalid build admission credentials") from None
    if principal.scopes != frozenset({"capacity:execute:pool"}) or principal.pool_id != pool_id:
        raise HTTPException(403, "build admission identity changed")
    if request.headers.getlist("content-type") != [ARTIFACT_STREAM_CONTENT_TYPE]:
        raise HTTPException(415, "native artifact stream content type required")
    try:
        async with asyncio.timeout(30):
            envelope, chunks = await decode_artifact_stream(request.stream())
        binding = envelope.claim.binding
        if (binding.intent_id != intent_id or binding.pool_id != pool_id
            or not principal.matches_executor(pool_id=binding.pool_id, executor_id=binding.executor_id,
                executor_incarnation=binding.executor_incarnation, pool_generation=binding.pool_generation)):
            raise HTTPException(403, "build admission identity changed")
        artifact = await writer.write(envelope.claim, worker_credential=envelope.worker_credential,
            artifact=envelope.artifact, chunks=chunks)
        receipt = BuildArtifactUploadReceiptV1(claim_digest=canonical_digest(envelope.claim), artifact=artifact)
        return Response(canonical_bytes(receipt), media_type="application/json", headers={"Cache-Control": "no-store"})
    except (DBAPIError, ValueError):
        raise HTTPException(409, "native artifact evidence unavailable or changed") from None
    except (TimeoutError, PoolTimeoutError, BotoCoreError, ClientError, RuntimeError):
        raise HTTPException(503, "native artifact IO did not complete") from None


@router.post("/capacity-build/pools/{pool_id}/intents/{intent_id}/source")
async def read_build_source(request: Request, pool_id: str, intent_id: UUID) -> Response:
    return await _admit(request, pool_id=pool_id, intent_id=intent_id, operation_name="source")


@router.post("/capacity-build/pools/{pool_id}/intents/{intent_id}/context")
async def read_build_context(request: Request, pool_id: str, intent_id: UUID) -> Response:
    return await _admit(request, pool_id=pool_id, intent_id=intent_id, operation_name="context")


@router.post("/capacity-build/pools/{pool_id}/intents/{intent_id}/bind")
async def bind_build(request: Request, pool_id: str, intent_id: UUID) -> Response:
    return await _admit(request, pool_id=pool_id, intent_id=intent_id, operation_name="bind")


@router.post("/capacity-build/pools/{pool_id}/intents/{intent_id}/observe")
async def observe_build(request: Request, pool_id: str, intent_id: UUID) -> Response:
    return await _admit(request, pool_id=pool_id, intent_id=intent_id, operation_name="observe")


@router.post("/capacity-build/pools/{pool_id}/intents/{intent_id}/revoke-bootstrap")
async def revoke_build_bootstrap(request: Request, pool_id: str, intent_id: UUID) -> Response:
    return await _admit(
        request, pool_id=pool_id, intent_id=intent_id, operation_name="revoke-bootstrap"
    )


@router.post("/capacity-build/pools/{pool_id}/intents/{intent_id}/withdraw")
async def withdraw_build_worker(request: Request, pool_id: str, intent_id: UUID) -> Response:
    return await _admit(request, pool_id=pool_id, intent_id=intent_id, operation_name="withdraw")


@router.post("/capacity-build/pools/{pool_id}/intents/{intent_id}/register")
async def register_build_worker(request: Request, pool_id: str, intent_id: UUID) -> Response:
    return await _admit(request, pool_id=pool_id, intent_id=intent_id, operation_name="register")


@router.post("/capacity-build/pools/{pool_id}/intents/{intent_id}/claim")
async def claim_build_platform(request: Request, pool_id: str, intent_id: UUID) -> Response:
    return await _admit(request, pool_id=pool_id, intent_id=intent_id, operation_name="claim")


@router.post("/capacity-build/pools/{pool_id}/intents/{intent_id}/claim-assigned")
async def claim_assigned_build_platform(request: Request, pool_id: str, intent_id: UUID) -> Response:
    return await _admit(request, pool_id=pool_id, intent_id=intent_id, operation_name="claim-assigned")


@router.post("/capacity-build/pools/{pool_id}/intents/{intent_id}/drain")
async def drain_build_worker(request: Request, pool_id: str, intent_id: UUID) -> Response:
    return await _admit(request, pool_id=pool_id, intent_id=intent_id, operation_name="drain")


@router.post("/capacity-build/pools/{pool_id}/intents/{intent_id}/outcome")
async def record_build_outcome(request: Request, pool_id: str, intent_id: UUID) -> Response:
    return await _admit(request, pool_id=pool_id, intent_id=intent_id, operation_name="outcome")


@router.post("/capacity-build/pools/{pool_id}/intents/{intent_id}/release")
async def release_build_worker(request: Request, pool_id: str, intent_id: UUID) -> Response:
    return await _admit(request, pool_id=pool_id, intent_id=intent_id, operation_name="release")
