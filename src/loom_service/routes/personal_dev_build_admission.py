"""Controller-authenticated native build admission; no application/source grants."""

from __future__ import annotations

import asyncio
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker

from loom_capacity_agent.admission import (
    ExecutablePreparedBootstrapRevocationV2,
    ExecutableWorkerWithdrawalRequestV2,
    PhysicalJobBindingV2,
)
from loom_capacity_agent.build_admission import (
    BuildClaimExchangeV1,
    BuildPreparationRequestV1,
    BuildRegistrationRequestV1,
)
from loom_capacity_build_guard.execution_store import BuildGuardExecutionStore
from loom_capacity_manager.auth import AuthorizationError, CapacityPrincipalVerifier
from loom_capacity_manager.contracts import canonical_bytes
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
    operation_name: Literal["prepare", "bind", "observe", "revoke-bootstrap", "withdraw", "register", "claim"],
) -> Response:
    sessions = getattr(request.app.state, "personal_dev_build_admission_sessions", None)
    verifier = getattr(request.app.state, "personal_dev_build_admission_verifier", None)
    if not isinstance(sessions, async_sessionmaker) or not isinstance(
        verifier, CapacityPrincipalVerifier
    ):
        raise HTTPException(503, "build admission unavailable")
    if operation_name == "register" and getattr(
        request.app.state, "personal_dev_build_admission_mode", None
    ) not in {"native-registration", "native-claims"}:
        raise HTTPException(503, "build registration unavailable")
    if operation_name == "claim" and getattr(
        request.app.state, "personal_dev_build_admission_mode", None
    ) != "native-claims":
        raise HTTPException(503, "native claims unavailable")
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
                claim = (
                    BuildClaimExchangeV1.model_validate_json(bytes(body))
                    if operation_name == "claim" else None
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
                    claim.claim
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
            async with sessions.begin() as session:
                await session.execute(text("SET LOCAL statement_timeout='10000ms'"))
                await session.execute(text("SET LOCAL lock_timeout='5000ms'"))
                store = BuildGuardExecutionStore(session, binding=binding)
                if claim is not None:
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
                else:
                    assert isinstance(operation, PhysicalJobBindingV2)
                    wire = canonical_executable_bytes(await store.bind_slurm_job(operation))
            # Context exit commits. Never send a preparation receipt from an
            # uncommitted transaction that could be followed by scheduler submit.
            return Response(wire, media_type="application/json")
    except (DBAPIError, ValueError):
        raise HTTPException(409, "build admission evidence unavailable or changed") from None
    except TimeoutError:
        raise HTTPException(503, "build admission deadline exceeded") from None


@router.post("/capacity-build/pools/{pool_id}/intents/{intent_id}/prepare")
async def prepare_build(request: Request, pool_id: str, intent_id: UUID) -> Response:
    return await _admit(request, pool_id=pool_id, intent_id=intent_id, operation_name="prepare")


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
