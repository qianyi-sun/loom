"""Authenticated zero-ceiling HTTP surface for global capacity evidence."""

# ruff: noqa: B008 - FastAPI declares dependency injection in parameter defaults.

from __future__ import annotations

import asyncio
import json
from collections import Counter, defaultdict
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, TypeVar, cast
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from loom_capacity_manager.allocator import allocate_shadow
from loom_capacity_manager.auth import (
    AuthorizationError,
    CapacityPrincipal,
    CapacityPrincipalVerifier,
    CapacityScope,
    bearer_token_sha256,
)
from loom_capacity_manager.config import CapacityManagerSettings, read_owner_only_secret
from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    MAX_FIXED_CLAIMS_PER_REPORT,
    MAX_POOLS,
    ConfigurationActivationV1,
    DemandSnapshotV1,
    DynamicDevelopmentSubjectProjectionV1,
    FleetManifestV1,
    PoolObservationV1,
    ShadowEpochV1,
    SubjectConfigurationV1,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapAcknowledgementV2,
    ExecutableBootstrapProposalV2,
    ExecutableExecutorHeartbeatV2,
    ExecutableExecutorInventoryV2,
    ExecutableExecutorRegistrationV2,
    ExecutableIntentBindingV2,
    ExecutableIntentCloseV2,
    ExecutablePartialReleaseV2,
    ExecutablePermitConsumptionV2,
    ExecutableProtectedReleaseV2,
    ExecutableReservationAcceptanceV2,
    ExecutableSubmissionRecoveryV2,
    ExecutionContextV2,
    ExecutionPreparationAbortV2,
    ExecutionPreparationV2,
    PreparedExecutorBindingV2,
    canonical_executable_digest,
)
from loom_capacity_manager.execution_policy import load_execution_preparation_policy
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.grant_contracts import (
    DryRunBootstrapRegistrationV1,
    DryRunExecutorHeartbeatV1,
    DryRunExecutorInventoryV1,
    DryRunExecutorRegistrationV1,
    DryRunIntentCloseV1,
    DryRunLaunchPermitV1,
    DryRunPartialReleaseV1,
    DryRunPermitConsumptionV1,
    DryRunProtectedReleaseAcknowledgementV1,
    DryRunReservationAcceptanceV1,
    DryRunReservationProposalV1,
)
from loom_capacity_manager.grant_store import (
    CapacityGrantStore,
    ExecutorEquivocationError,
    ExecutorJournalError,
    GrantConflictError,
    LaunchOrderError,
    PermitExpiredError,
    ProposalExpiredError,
    ProposalSupersededError,
    RateLimitError,
    StaleCommandError,
    StaleExecutorError,
)
from loom_capacity_manager.metrics import (
    FRESHNESS_STATES,
    POOL_SLOT_STATES,
    REPORT_KINDS,
    CapacityMetrics,
)
from loom_capacity_manager.models import (
    CapacityAllocation,
    CapacityAllocationEpoch,
    CapacityAuditEvent,
    CapacityAuthorityState,
    CapacityExecutableExecutorState,
    CapacityExecutableIntent,
    CapacityExecutor,
    CapacityPool,
    CapacityReservationShape,
    CapacityReservationTranche,
    CapacitySubject,
    CapacitySubmissionIntent,
)
from loom_capacity_manager.ownership import OwnershipKeyring
from loom_capacity_manager.preparation_readiness import (
    load_prepared_execution_readiness,
)
from loom_capacity_manager.reconciler import (
    ShadowAllocator,
    ShadowRunResult,
    reconcile_shadow_once,
)
from loom_capacity_manager.schema_startup import assert_capacity_schema_at_head
from loom_capacity_manager.store import (
    CapacityManagementStore,
    CapacityStoreError,
    ConfigurationConflictError,
    ExecutionConflictError,
    IdempotencyConflictError,
    ReportEquivocationError,
    StaleAllocationInputError,
    StaleReportError,
    StaleWriterError,
    UnknownReporterError,
    WriterFence,
)

_ContractT = TypeVar("_ContractT", bound=BaseModel)


class RequestBodyLimitMiddleware:
    """Count streamed ASGI body chunks even when Content-Length is absent."""

    def __init__(self, app: Any, *, maximum_bytes: int) -> None:
        self.app = app
        self.maximum_bytes = maximum_bytes

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        content_lengths = [
            value for key, value in scope.get("headers", ()) if key.lower() == b"content-length"
        ]
        if content_lengths:
            supplied = content_lengths[0]
            if len(content_lengths) != 1 or not supplied.isdigit():
                await self._reject(send, status.HTTP_400_BAD_REQUEST, "invalid content length")
                return
            length = int(supplied)
            if length < 0 or length > self.maximum_bytes:
                await self._reject(send, status.HTTP_413_CONTENT_TOO_LARGE, "request too large")
                return
        received = 0
        messages: list[dict[str, Any]] = []
        while True:
            message = await receive()
            messages.append(cast(dict[str, Any], message))
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.maximum_bytes:
                    await self._reject(
                        send,
                        status.HTTP_413_CONTENT_TOO_LARGE,
                        "request too large",
                    )
                    return
                if not message.get("more_body", False):
                    break
            elif message["type"] == "http.disconnect":
                break
        index = 0

        async def replay_receive() -> dict[str, Any]:
            nonlocal index
            if index >= len(messages):
                return {"type": "http.disconnect"}
            message = messages[index]
            index += 1
            return message

        await self.app(scope, replay_receive, send)

    @staticmethod
    async def _reject(send: Any, response_status: int, detail: str) -> None:
        payload = ('{"detail":"' + detail + '"}').encode("ascii")
        await send(
            {
                "type": "http.response.start",
                "status": response_status,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(payload)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": payload})


def _bounded_limit(limit: int) -> int:
    if not 1 <= limit <= 500:
        raise HTTPException(status_code=422, detail="limit must be between 1 and 500")
    return limit


def _store_error(exc: CapacityStoreError) -> HTTPException:
    if isinstance(exc, UnknownReporterError):
        return HTTPException(status_code=403, detail="forbidden")
    if isinstance(
        exc,
        (
            ConfigurationConflictError,
            ExecutionConflictError,
            IdempotencyConflictError,
            ReportEquivocationError,
            StaleReportError,
            StaleAllocationInputError,
        ),
    ):
        return HTTPException(status_code=409, detail="capacity state conflict")
    if isinstance(exc, StaleWriterError):
        return HTTPException(status_code=503, detail="capacity writer is not ready")
    if isinstance(exc, RateLimitError):
        return HTTPException(status_code=429, detail="capacity launch rate is exhausted")
    if isinstance(
        exc,
        (
            ExecutorEquivocationError,
            ExecutorJournalError,
            GrantConflictError,
            LaunchOrderError,
            PermitExpiredError,
            ProposalExpiredError,
            ProposalSupersededError,
            StaleCommandError,
            StaleExecutorError,
        ),
    ):
        return HTTPException(status_code=409, detail="capacity state conflict")
    return HTTPException(status_code=503, detail="capacity manager unavailable")


def _run_reason(result: ShadowRunResult) -> str:
    if result.status == "committed":
        return "none"
    reason = result.reason or ""
    if "deadline" in reason:
        return "timeout"
    if "input changed" in reason:
        return "input-contention"
    if "writer fence" in reason:
        return "writer-fenced"
    if "transaction" in reason:
        return "transaction-failed"
    if "input" in reason:
        return "invalid-input"
    return "unexpected"


def _writer_matches_authority(writer: WriterFence, authority: CapacityAuthorityState) -> bool:
    return (
        authority.authority_incarnation == writer.authority_incarnation
        and authority.writer_epoch == writer.writer_epoch
    )


def _health_payload(*, ready: bool, executable_new_capacity_ceiling: int) -> bytes:
    return json.dumps(
        {
            "status": "ready" if ready else "not-ready",
            "executable_new_capacity_ceiling": executable_new_capacity_ceiling,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _manager_execution_blockers(authority: CapacityAuthorityState) -> list[str]:
    if authority.execution_state == "shadow":
        return ["manager-shadow"]
    if authority.execution_state == "prepared":
        return ["manager-prepared", "zero-executable-ceiling"]
    if authority.execution_state == "drain-only":
        return ["manager-drain-only", "zero-executable-ceiling"]
    return []


def _validated_executable_inventory(
    row: CapacityExecutableExecutorState,
) -> ExecutableExecutorInventoryV2 | None:
    payload = row.inventory_payload
    if payload is None or row.last_inventory_digest is None:
        return None
    try:
        # JSONB returns UUID values as strings; validate through the wire form
        # so the strict executable contracts restore their exact UUID types.
        inventory = ExecutableExecutorInventoryV2.model_validate_json(json.dumps(payload))
    except ValidationError:
        return None
    if (
        inventory.execution.execution_epoch != row.execution_epoch
        or inventory.execution.execution_manifest_sha256 != row.execution_manifest_sha256
        or inventory.executor_id != row.executor_id
        or inventory.executor_incarnation != row.executor_incarnation
        or inventory.pool_id != row.pool_id
        or inventory.pool_generation != row.pool_generation
        or inventory.inventory_sequence != row.inventory_high_water
        or canonical_executable_digest(inventory) != row.last_inventory_digest
    ):
        return None
    return inventory


def _executor_status_item(
    row: CapacityExecutableExecutorState,
    *,
    now: datetime,
    freshness_seconds: int,
) -> tuple[dict[str, Any], ExecutableExecutorInventoryV2 | None]:
    inventory = _validated_executable_inventory(row)
    blockers: list[str] = []
    if row.state != "current":
        blockers.append(f"executor-{row.state}")
    if row.lease_expires_at <= now:
        blockers.append("executor-lease-expired")
    if inventory is None:
        blockers.append(
            "executor-inventory-missing"
            if row.inventory_high_water == 0
            else "executor-inventory-invalid"
        )
    elif (
        row.last_inventory_at is None
        or row.last_inventory_at + timedelta(seconds=freshness_seconds) <= now
    ):
        blockers.append("executor-inventory-stale")
    counts: Counter[str] = Counter()
    quarantine_count = 0
    if inventory is not None:
        for record in inventory.records:
            counts[f"{record.physical_kind}:{record.state}"] += 1
            if (
                record.state == "unknown"
                or record.authority_scope == "foreign"
                or record.ownership_proof is None
            ):
                quarantine_count += 1
    if quarantine_count:
        blockers.append("executor-inventory-quarantine")
    item = {
        "executor_id": row.executor_id,
        "executor_incarnation": row.executor_incarnation,
        "pool_id": row.pool_id,
        "pool_generation": row.pool_generation,
        "state": row.state,
        "lease_expires_at": row.lease_expires_at,
        "last_heartbeat_at": row.last_heartbeat_at,
        "heartbeat_sequence": row.heartbeat_high_water,
        "command_sequence": row.command_high_water,
        "journal_sequence": row.journal_high_water,
        "journal_digest": row.journal_digest,
        "inventory_sequence": row.inventory_high_water,
        "inventory_digest": row.last_inventory_digest,
        "inventory_observed_at": row.last_inventory_at,
        "inventory_record_counts": dict(sorted(counts.items())),
        "quarantine_count": quarantine_count,
        "retirement_safe": row.retirement_safe,
        "blockers": sorted(set(blockers)),
    }
    return item, inventory


def create_app(
    settings: CapacityManagerSettings,
    *,
    verifier: CapacityPrincipalVerifier | None = None,
    allocator: ShadowAllocator = allocate_shadow,
    management_store: CapacityManagementStore | None = None,
    grant_store: CapacityGrantStore | None = None,
    execution_store: CapacityExecutionStore | None = None,
) -> FastAPI:
    """Build one process-local API; DB fencing remains the cross-process boundary."""

    resolved_verifier = verifier or CapacityPrincipalVerifier.from_file(settings.principals_file)
    metrics = CapacityMetrics()
    resolved_execution_policy_sha256: str | None = None
    if management_store is None:
        execution_policy = (
            None
            if settings.execution_policy_file is None
            else load_execution_preparation_policy(
                settings.execution_policy_file,
                cast(str, settings.execution_policy_sha256),
            )
        )
        resolved_management_store = CapacityManagementStore(
            freshness_seconds=settings.freshness_seconds,
            execution_policy=execution_policy,
        )
        if execution_policy is not None:
            resolved_execution_policy_sha256 = cast(str, settings.execution_policy_sha256)
    else:
        resolved_management_store = management_store
    ownership_keyring = OwnershipKeyring()
    if settings.ownership_public_keys_file is not None:
        ownership_keyring = OwnershipKeyring.from_json(
            read_owner_only_secret(
                settings.ownership_public_keys_file,
                max_bytes=1024 * 1024,
            )
        )
    if grant_store is None:
        resolved_grant_store = CapacityGrantStore(
            ownership_keyring=ownership_keyring,
            pool_observation_freshness_seconds=settings.freshness_seconds,
        )
    else:
        resolved_grant_store = grant_store
    resolved_execution_store = execution_store or CapacityExecutionStore(
        inventory_freshness_seconds=settings.freshness_seconds,
        ownership_keyring=ownership_keyring,
    )
    reconciliation_lock = asyncio.Lock()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.ready = False
        app.state.initialization_error = True
        app.state.engine = None
        try:
            database_url = read_owner_only_secret(settings.db_url_file)
            engine = create_async_engine(database_url, isolation_level="SERIALIZABLE")
            app.state.engine = engine
            session_factory = async_sessionmaker(engine, expire_on_commit=False)
            async with session_factory() as session:
                await assert_capacity_schema_at_head(engine)
                authority = (
                    await session.execute(
                        select(CapacityAuthorityState).where(
                            CapacityAuthorityState.singleton_id == 1
                        )
                    )
                ).scalar_one()
                if authority.authority_incarnation != settings.expected_authority_incarnation:
                    raise RuntimeError("capacity authority incarnation mismatch")
                writer = await resolved_management_store.register_writer(
                    session,
                    settings.expected_authority_incarnation,
                    expected_epoch=authority.writer_epoch,
                )
                await session.commit()
            app.state.engine = engine
            app.state.session_factory = session_factory
            app.state.store = resolved_management_store
            app.state.grant_store = resolved_grant_store
            app.state.execution_store = resolved_execution_store
            app.state.writer = writer
            app.state.ready = True
            app.state.initialization_error = False
            metrics.ready.set(1)
        except Exception:
            metrics.ready.set(0)
        try:
            yield
        finally:
            app.state.ready = False
            metrics.ready.set(0)
            resolved_engine = cast(AsyncEngine | None, app.state.engine)
            if resolved_engine is not None:
                await resolved_engine.dispose()

    app = FastAPI(
        title="Loom Global Capacity Manager",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.add_middleware(RequestBodyLimitMiddleware, maximum_bytes=MAX_CONTRACT_BYTES)
    app.state.verifier = resolved_verifier
    app.state.metrics = metrics

    def principal(authorization: str | None = Header(default=None)) -> CapacityPrincipal:
        try:
            return resolved_verifier.verify_bearer(authorization)
        except AuthorizationError as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid capacity credentials",
            ) from exc

    def require(scope: CapacityScope) -> Callable[[CapacityPrincipal], CapacityPrincipal]:
        def dependency(value: CapacityPrincipal = Depends(principal)) -> CapacityPrincipal:
            if not value.has_scope(scope):
                raise HTTPException(status_code=403, detail="forbidden")
            return value

        return dependency

    def contract_body(
        model: type[_ContractT],
    ) -> Callable[[Request], Awaitable[_ContractT]]:
        async def dependency(request: Request) -> _ContractT:
            try:
                return model.model_validate_json(await request.body())
            except (ValidationError, ValueError) as exc:
                raise HTTPException(status_code=422, detail="invalid capacity contract") from exc

        return dependency

    fleet_body = contract_body(FleetManifestV1)
    subject_body = contract_body(SubjectConfigurationV1)
    activation_body = contract_body(ConfigurationActivationV1)
    development_projection_body = contract_body(DynamicDevelopmentSubjectProjectionV1)
    demand_body = contract_body(DemandSnapshotV1)
    pool_body = contract_body(PoolObservationV1)
    executor_registration_body = contract_body(DryRunExecutorRegistrationV1)
    executor_heartbeat_body = contract_body(DryRunExecutorHeartbeatV1)
    executor_inventory_body = contract_body(DryRunExecutorInventoryV1)
    reservation_proposal_body = contract_body(DryRunReservationProposalV1)
    reservation_acceptance_body = contract_body(DryRunReservationAcceptanceV1)
    bootstrap_registration_body = contract_body(DryRunBootstrapRegistrationV1)
    launch_permit_body = contract_body(DryRunLaunchPermitV1)
    permit_consumption_body = contract_body(DryRunPermitConsumptionV1)
    intent_close_body = contract_body(DryRunIntentCloseV1)
    partial_release_body = contract_body(DryRunPartialReleaseV1)
    protected_release_acknowledgement_body = contract_body(DryRunProtectedReleaseAcknowledgementV1)
    executable_heartbeat_body = contract_body(ExecutableExecutorHeartbeatV2)
    executable_inventory_body = contract_body(ExecutableExecutorInventoryV2)
    execution_preparation_body = contract_body(ExecutionPreparationV2)
    execution_registration_body = contract_body(ExecutableExecutorRegistrationV2)
    execution_abort_body = contract_body(ExecutionPreparationAbortV2)
    executable_acceptance_body = contract_body(ExecutableReservationAcceptanceV2)
    executable_bootstrap_proposal_body = contract_body(ExecutableBootstrapProposalV2)
    executable_bootstrap_acknowledgement_body = contract_body(ExecutableBootstrapAcknowledgementV2)
    executable_consumption_body = contract_body(ExecutablePermitConsumptionV2)
    executable_recovery_body = contract_body(ExecutableSubmissionRecoveryV2)
    executable_close_body = contract_body(ExecutableIntentCloseV2)
    executable_release_body = contract_body(ExecutablePartialReleaseV2)
    executable_protected_release_body = contract_body(ExecutableProtectedReleaseV2)

    def runtime(
        request: Request,
    ) -> tuple[
        async_sessionmaker[AsyncSession],
        CapacityManagementStore,
        WriterFence,
    ]:
        if not getattr(request.app.state, "ready", False):
            raise HTTPException(status_code=503, detail="capacity manager not ready")
        return (
            request.app.state.session_factory,
            request.app.state.store,
            request.app.state.writer,
        )

    def grant_runtime(
        request: Request,
    ) -> tuple[async_sessionmaker[AsyncSession], CapacityGrantStore, WriterFence]:
        if not getattr(request.app.state, "ready", False):
            raise HTTPException(status_code=503, detail="capacity manager not ready")
        return (
            request.app.state.session_factory,
            request.app.state.grant_store,
            request.app.state.writer,
        )

    def execution_runtime(
        request: Request,
    ) -> tuple[async_sessionmaker[AsyncSession], CapacityExecutionStore]:
        if not getattr(request.app.state, "ready", False):
            raise HTTPException(status_code=503, detail="capacity manager not ready")
        return request.app.state.session_factory, request.app.state.execution_store

    async def current_writer_ceiling(request: Request) -> int | None:
        if not getattr(request.app.state, "ready", False):
            return None
        session_factory = cast(
            async_sessionmaker[AsyncSession],
            request.app.state.session_factory,
        )
        writer = cast(WriterFence, request.app.state.writer)
        try:
            async with session_factory() as session:
                authority = (
                    await session.execute(
                        select(CapacityAuthorityState).where(
                            CapacityAuthorityState.singleton_id == 1
                        )
                    )
                ).scalar_one()
            current = _writer_matches_authority(writer, authority)
        except Exception:
            current = False
        if not current:
            request.app.state.ready = False
            metrics.ready.set(0)
            return None
        return authority.executable_new_capacity_ceiling

    @app.get("/healthz")
    async def health(request: Request) -> Response:
        current_ceiling = await current_writer_ceiling(request)
        ready = current_ceiling is not None
        return Response(
            content=_health_payload(
                ready=ready,
                executable_new_capacity_ceiling=current_ceiling or 0,
            ),
            media_type="application/json",
            status_code=200 if ready else 503,
        )

    @app.put("/v1/config-proposals/fleet")
    async def propose_fleet(
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:configure:fleet")),
        value: FleetManifestV1 = Depends(fleet_body),
        idempotency_key: UUID = Header(alias="Idempotency-Key"),
    ) -> Any:
        session_factory, store, _writer = runtime(request)
        try:
            async with session_factory() as session:
                result = await store.propose_fleet_configuration(
                    session,
                    value,
                    actor=actor.principal_id,
                    idempotency_key=idempotency_key,
                )
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.put("/v1/config-proposals/subjects/{subject_id}")
    async def propose_subject(
        subject_id: UUID,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:configure:subject")),
        value: SubjectConfigurationV1 = Depends(subject_body),
        idempotency_key: UUID = Header(alias="Idempotency-Key"),
    ) -> Any:
        if value.subject_id != subject_id:
            raise HTTPException(status_code=409, detail="capacity state conflict")
        session_factory, store, _writer = runtime(request)
        try:
            async with session_factory() as session:
                result = await store.propose_subject_configuration(
                    session,
                    value,
                    actor=actor.principal_id,
                    idempotency_key=idempotency_key,
                )
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.post("/v1/config-activations")
    async def activate(
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:configure:activate")),
        value: ConfigurationActivationV1 = Depends(activation_body),
        idempotency_key: UUID = Header(alias="Idempotency-Key"),
    ) -> Any:
        session_factory, store, _writer = runtime(request)
        try:
            async with session_factory() as session:
                result = await store.activate_configuration(
                    session,
                    value,
                    actor=actor.principal_id,
                    idempotency_key=idempotency_key,
                )
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.put("/v1/development-projections/{subject_id}")
    async def project_development_subject(
        subject_id: UUID,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:project:development")),
        value: DynamicDevelopmentSubjectProjectionV1 = Depends(development_projection_body),
        idempotency_key: UUID = Header(alias="Idempotency-Key"),
    ) -> Any:
        if value.subject_id != subject_id:
            raise HTTPException(status_code=409, detail="capacity state conflict")
        session_factory, store, _writer = runtime(request)
        try:
            async with session_factory() as session:
                result = await store.project_development_subject(
                    session,
                    value,
                    actor=actor.principal_id,
                    idempotency_key=idempotency_key,
                )
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.put("/v1/reports/demand/{subject_id}")
    async def ingest_demand(
        subject_id: UUID,
        request: Request,
        value: DemandSnapshotV1 = Depends(demand_body),
        authorization: str | None = Header(default=None),
    ) -> Any:
        if value.subject_id != subject_id:
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, store, _writer = runtime(request)
        try:
            actor_id: str
            try:
                actor = resolved_verifier.verify_bearer(authorization)
            except AuthorizationError:
                try:
                    token_sha256 = bearer_token_sha256(authorization)
                except AuthorizationError as exc:
                    raise HTTPException(
                        status_code=401,
                        detail="invalid capacity credentials",
                    ) from exc
                async with session_factory() as session:
                    try:
                        actor_id = await store.authenticate_dynamic_demand_reporter(
                            session,
                            value,
                            token_sha256=token_sha256,
                        )
                    except UnknownReporterError as exc:
                        raise HTTPException(
                            status_code=401,
                            detail="invalid capacity credentials",
                        ) from exc
            else:
                if (
                    not actor.has_scope("capacity:report:demand")
                    or actor.subject_id != subject_id
                    or actor.subject_incarnation != value.subject_incarnation
                    or actor.demand_reporter_incarnation != value.reporter_incarnation
                ):
                    raise HTTPException(status_code=403, detail="forbidden")
                actor_id = actor.principal_id
            async with session_factory() as session:
                result = await store.ingest_demand_snapshot(
                    session,
                    value,
                    actor=actor_id,
                )
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.put("/v1/reports/pools/{pool_id}")
    async def ingest_pool(
        pool_id: str,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:report:pool")),
        value: PoolObservationV1 = Depends(pool_body),
    ) -> Any:
        if (
            value.pool_id != pool_id
            or actor.pool_id != pool_id
            or actor.pool_reporter_incarnation != value.reporter_incarnation
        ):
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, store, _writer = runtime(request)
        try:
            async with session_factory() as session:
                result = await store.ingest_pool_observation(
                    session,
                    value,
                    actor=actor.principal_id,
                )
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.put("/v1/reports/protected-releases/{subject_id}/{shape_instance_id}")
    async def acknowledge_protected_release(
        subject_id: UUID,
        shape_instance_id: str,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:report:demand")),
        value: DryRunProtectedReleaseAcknowledgementV1 = Depends(
            protected_release_acknowledgement_body
        ),
        idempotency_key: UUID = Header(alias="Idempotency-Key"),
    ) -> Any:
        if (
            value.subject_id != subject_id
            or value.shape_instance_id != shape_instance_id
            or actor.subject_id != subject_id
            or actor.subject_incarnation != value.subject_incarnation
            or actor.demand_reporter_incarnation != value.reporter_incarnation
        ):
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, grants, _writer = grant_runtime(request)
        try:
            async with session_factory() as session:
                result = await grants.acknowledge_protected_release(
                    session,
                    value,
                    actor=actor.principal_id,
                    idempotency_key=idempotency_key,
                )
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    def assert_executor_actor(
        actor: CapacityPrincipal,
        *,
        pool_id: str,
        executor_id: str,
        executor_incarnation: UUID,
        pool_generation: int | None = None,
    ) -> None:
        if not actor.matches_executor(
            pool_id=pool_id,
            executor_id=executor_id,
            executor_incarnation=executor_incarnation,
            pool_generation=pool_generation,
        ):
            raise HTTPException(status_code=403, detail="forbidden")

    def assert_unbound_execution_actor(actor: CapacityPrincipal) -> None:
        if any(
            value is not None
            for value in (
                actor.subject_id,
                actor.subject_incarnation,
                actor.demand_reporter_incarnation,
                actor.pool_id,
                actor.pool_reporter_incarnation,
                actor.executor_id,
                actor.executor_incarnation,
                actor.executor_pool_generation,
            )
        ):
            raise HTTPException(status_code=403, detail="forbidden")

    def executor_binding(actor: CapacityPrincipal, *, pool_id: str) -> PreparedExecutorBindingV2:
        if (
            actor.pool_id != pool_id
            or actor.executor_id is None
            or actor.executor_incarnation is None
            or actor.executor_pool_generation is None
        ):
            raise HTTPException(status_code=403, detail="forbidden")
        return PreparedExecutorBindingV2(
            pool_id=cast(Any, pool_id),
            pool_generation=actor.executor_pool_generation,
            executor_id=actor.executor_id,
            executor_incarnation=actor.executor_incarnation,
            signing_key_sha256="0" * 64,
            local_authority_sha256="0" * 64,
            controller_authority_sha256="0" * 64,
        )

    @app.post("/v2/execution-preparations")
    async def prepare_execution_epoch(
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execution:prepare")),
        value: ExecutionPreparationV2 = Depends(execution_preparation_body),
        idempotency_key: UUID = Header(alias="Idempotency-Key"),
    ) -> Any:
        assert_unbound_execution_actor(actor)
        session_factory, store, _writer = runtime(request)
        try:
            async with session_factory() as session:
                result = await store.prepare_execution_epoch(
                    session,
                    value,
                    actor=actor.principal_id,
                    idempotency_key=idempotency_key,
                )
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.put("/v2/executors/{pool_id}/registration")
    async def register_execution_executor(
        pool_id: str,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
        value: ExecutableExecutorRegistrationV2 = Depends(execution_registration_body),
        idempotency_key: UUID = Header(alias="Idempotency-Key"),
    ) -> Any:
        assert_executor_actor(
            actor,
            pool_id=pool_id,
            executor_id=value.executor_id,
            executor_incarnation=value.executor_incarnation,
            pool_generation=value.pool_generation,
        )
        if value.pool_id != pool_id:
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, store, _writer = runtime(request)
        try:
            async with session_factory() as session:
                result = await store.register_execution_executor(
                    session,
                    value,
                    actor=actor.principal_id,
                    idempotency_key=idempotency_key,
                )
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.post("/v2/execution-preparations/{execution_epoch}/abort")
    async def abort_execution_preparation(
        execution_epoch: int,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execution:abort")),
        value: ExecutionPreparationAbortV2 = Depends(execution_abort_body),
        idempotency_key: UUID = Header(alias="Idempotency-Key"),
    ) -> Any:
        assert_unbound_execution_actor(actor)
        if value.execution_epoch != execution_epoch:
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, store, _writer = runtime(request)
        try:
            async with session_factory() as session:
                result = await store.abort_prepared_execution_epoch(
                    session,
                    value,
                    actor=actor.principal_id,
                    idempotency_key=idempotency_key,
                )
                current_writer = cast(WriterFence, request.app.state.writer)
                if (
                    current_writer.authority_incarnation == value.authority_incarnation
                    and current_writer.writer_epoch == value.expected_writer_epoch
                ):
                    request.app.state.writer = WriterFence(
                        authority_incarnation=value.authority_incarnation,
                        writer_epoch=value.expected_writer_epoch + 1,
                    )
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.put("/v1/executors/{pool_id}/registration")
    async def register_executor(
        pool_id: str,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:grant:manage")),
        value: DryRunExecutorRegistrationV1 = Depends(executor_registration_body),
        idempotency_key: UUID = Header(alias="Idempotency-Key"),
    ) -> Any:
        if value.pool_id != pool_id:
            raise HTTPException(status_code=409, detail="capacity state conflict")
        session_factory, grants, writer = grant_runtime(request)
        try:
            async with session_factory() as session:
                result = await grants.register_executor(
                    session,
                    writer,
                    value,
                    actor=actor.principal_id,
                    idempotency_key=idempotency_key,
                )
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.put("/v1/executors/{pool_id}/heartbeat")
    async def heartbeat_executor(
        pool_id: str,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
        value: DryRunExecutorHeartbeatV1 = Depends(executor_heartbeat_body),
    ) -> Any:
        assert_executor_actor(
            actor,
            pool_id=pool_id,
            executor_id=value.executor_id,
            executor_incarnation=value.executor_incarnation,
        )
        if value.pool_id != pool_id:
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, grants, _writer = grant_runtime(request)
        try:
            async with session_factory() as session:
                result = await grants.heartbeat_executor(session, value)
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.get("/v1/executors/{pool_id}/checkpoint")
    async def executor_checkpoint(
        pool_id: str,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
    ) -> Any:
        if (
            actor.pool_id != pool_id
            or actor.executor_id is None
            or actor.executor_incarnation is None
        ):
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, grants, writer = grant_runtime(request)
        try:
            async with session_factory() as session:
                result = await grants.executor_checkpoint(
                    session,
                    authority_incarnation=writer.authority_incarnation,
                    writer_epoch=writer.writer_epoch,
                    executor_id=actor.executor_id,
                    executor_incarnation=actor.executor_incarnation,
                    pool_id=pool_id,
                )
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.put("/v1/executors/{pool_id}/inventory")
    async def ingest_executor_inventory(
        pool_id: str,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
        value: DryRunExecutorInventoryV1 = Depends(executor_inventory_body),
    ) -> Any:
        assert_executor_actor(
            actor,
            pool_id=pool_id,
            executor_id=value.executor_id,
            executor_incarnation=value.executor_incarnation,
            pool_generation=value.pool_generation,
        )
        if value.pool_id != pool_id:
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, grants, _writer = grant_runtime(request)
        try:
            async with session_factory() as session:
                result = await grants.ingest_executor_inventory(session, value)
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.put("/v1/grants/reservations/{tranche_id}")
    async def propose_reservation(
        tranche_id: UUID,
        request: Request,
        _actor: CapacityPrincipal = Depends(require("capacity:grant:manage")),
        value: DryRunReservationProposalV1 = Depends(reservation_proposal_body),
        idempotency_key: UUID = Header(alias="Idempotency-Key"),
    ) -> Any:
        if value.tranche_id != tranche_id:
            raise HTTPException(status_code=409, detail="capacity state conflict")
        session_factory, grants, writer = grant_runtime(request)
        try:
            async with session_factory() as session:
                result = await grants.propose_reservation(
                    session,
                    writer,
                    value,
                    idempotency_key=idempotency_key,
                )
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.post("/v1/executors/{pool_id}/reservations/{tranche_id}/accept")
    async def accept_reservation(
        pool_id: str,
        tranche_id: UUID,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
        value: DryRunReservationAcceptanceV1 = Depends(reservation_acceptance_body),
    ) -> Any:
        assert_executor_actor(
            actor,
            pool_id=pool_id,
            executor_id=value.executor_id,
            executor_incarnation=value.executor_incarnation,
            pool_generation=actor.executor_pool_generation,
        )
        if value.tranche_id != tranche_id:
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, grants, _writer = grant_runtime(request)
        try:
            async with session_factory() as session:
                result = await grants.accept_reservation(session, value)
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.post("/v1/executors/{pool_id}/intents/{intent_id}/bootstrap")
    async def register_bootstrap(
        pool_id: str,
        intent_id: UUID,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
        value: DryRunBootstrapRegistrationV1 = Depends(bootstrap_registration_body),
    ) -> Any:
        assert_executor_actor(
            actor,
            pool_id=pool_id,
            executor_id=value.executor_id,
            executor_incarnation=value.executor_incarnation,
        )
        if value.intent_id != intent_id:
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, grants, _writer = grant_runtime(request)
        try:
            async with session_factory() as session:
                result = await grants.register_bootstrap(session, value)
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.put("/v1/grants/launch-permits/{permit_id}")
    async def issue_launch_permit(
        permit_id: UUID,
        request: Request,
        _actor: CapacityPrincipal = Depends(require("capacity:grant:manage")),
        value: DryRunLaunchPermitV1 = Depends(launch_permit_body),
        idempotency_key: UUID = Header(alias="Idempotency-Key"),
    ) -> Any:
        if value.permit_id != permit_id:
            raise HTTPException(status_code=409, detail="capacity state conflict")
        session_factory, grants, writer = grant_runtime(request)
        try:
            async with session_factory() as session:
                result = await grants.issue_launch_permit(
                    session,
                    writer,
                    value,
                    idempotency_key=idempotency_key,
                )
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.post("/v1/executors/{pool_id}/permits/{permit_id}/consume")
    async def consume_launch_permit(
        pool_id: str,
        permit_id: UUID,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
        value: DryRunPermitConsumptionV1 = Depends(permit_consumption_body),
    ) -> Any:
        assert_executor_actor(
            actor,
            pool_id=pool_id,
            executor_id=value.executor_id,
            executor_incarnation=value.executor_incarnation,
        )
        if value.permit_id != permit_id:
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, grants, _writer = grant_runtime(request)
        try:
            async with session_factory() as session:
                result = await grants.consume_launch_permit(session, value)
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.post("/v1/executors/{pool_id}/intents/{intent_id}/close")
    async def begin_intent_close(
        pool_id: str,
        intent_id: UUID,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
        value: DryRunIntentCloseV1 = Depends(intent_close_body),
    ) -> Any:
        assert_executor_actor(
            actor,
            pool_id=pool_id,
            executor_id=value.executor_id,
            executor_incarnation=value.executor_incarnation,
        )
        if value.intent_id != intent_id:
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, grants, _writer = grant_runtime(request)
        try:
            async with session_factory() as session:
                result = await grants.begin_intent_close(session, value)
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.post("/v1/executors/{pool_id}/reservations/{tranche_id}/release")
    async def release_shapes(
        pool_id: str,
        tranche_id: UUID,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
        value: DryRunPartialReleaseV1 = Depends(partial_release_body),
    ) -> Any:
        assert_executor_actor(
            actor,
            pool_id=pool_id,
            executor_id=value.executor_id,
            executor_incarnation=value.executor_incarnation,
        )
        if value.tranche_id != tranche_id:
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, grants, _writer = grant_runtime(request)
        try:
            async with session_factory() as session:
                result = await grants.release_shapes(session, value)
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.put("/v2/executors/{pool_id}/heartbeat")
    async def heartbeat_executable_executor(
        pool_id: str,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
        value: ExecutableExecutorHeartbeatV2 = Depends(executable_heartbeat_body),
    ) -> Any:
        assert_executor_actor(
            actor,
            pool_id=pool_id,
            executor_id=value.executor_id,
            executor_incarnation=value.executor_incarnation,
            pool_generation=value.pool_generation,
        )
        if value.pool_id != pool_id:
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, executions = execution_runtime(request)
        try:
            async with session_factory() as session:
                result = await executions.heartbeat_executor(session, value)
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.get("/v2/executors/{pool_id}/checkpoint")
    async def executable_executor_checkpoint(
        pool_id: str,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
    ) -> Any:
        binding = executor_binding(actor, pool_id=pool_id)
        session_factory, executions = execution_runtime(request)
        try:
            async with session_factory() as session:
                result = await executions.executor_checkpoint(session, binding)
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.get("/v2/executors/{pool_id}/context")
    async def executable_executor_current_context(
        pool_id: str,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
    ) -> Any:
        binding = executor_binding(actor, pool_id=pool_id)
        session_factory, store, _writer = runtime(request)
        del binding
        try:
            async with session_factory() as session:
                result = await store.execution_authority(session)
            if result is None:
                raise HTTPException(status_code=409, detail="execution context not available")
            public_payload = result.model_dump(include=set(ExecutionContextV2.model_fields))
            return jsonable_encoder(ExecutionContextV2.model_validate(public_payload))
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.get("/v2/executors/{pool_id}/work")
    async def next_executable_pool_work(
        pool_id: str,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
    ) -> Any:
        binding = executor_binding(actor, pool_id=pool_id)
        session_factory, executions = execution_runtime(request)
        try:
            async with session_factory() as session:
                result = await executions.next_pool_work(session, binding)
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.put("/v2/executors/{pool_id}/inventory")
    async def ingest_executable_inventory(
        pool_id: str,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
        value: ExecutableExecutorInventoryV2 = Depends(executable_inventory_body),
    ) -> Any:
        assert_executor_actor(
            actor,
            pool_id=pool_id,
            executor_id=value.executor_id,
            executor_incarnation=value.executor_incarnation,
            pool_generation=value.pool_generation,
        )
        if value.pool_id != pool_id:
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, executions = execution_runtime(request)
        try:
            async with session_factory() as session:
                result = await executions.ingest_executor_inventory(session, value)
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.post("/v2/executors/{pool_id}/reservations/{tranche_id}/accept")
    async def accept_executable_reservation(
        pool_id: str,
        tranche_id: UUID,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
        value: ExecutableReservationAcceptanceV2 = Depends(executable_acceptance_body),
    ) -> Any:
        assert_executor_actor(
            actor,
            pool_id=pool_id,
            executor_id=value.executor_id,
            executor_incarnation=value.executor_incarnation,
            pool_generation=value.pool_generation,
        )
        if value.tranche_id != tranche_id or value.pool_id != pool_id:
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, executions = execution_runtime(request)
        try:
            async with session_factory() as session:
                result = await executions.accept_reservation(session, value)
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.post("/v2/executors/{pool_id}/intents/{intent_id}/bootstrap-proposals")
    async def propose_executable_bootstrap(
        pool_id: str,
        intent_id: UUID,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
        value: ExecutableBootstrapProposalV2 = Depends(executable_bootstrap_proposal_body),
    ) -> Any:
        binding = value.binding
        assert_executor_actor(
            actor,
            pool_id=pool_id,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_generation=binding.pool_generation,
        )
        if binding.pool_id != pool_id or binding.intent_id != intent_id:
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, executions = execution_runtime(request)
        try:
            async with session_factory() as session:
                result = await executions.propose_bootstrap(session, value)
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.get("/v2/subjects/{subject_id}/bootstrap-work")
    async def next_subject_bootstrap_work(
        subject_id: UUID,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:report:demand")),
    ) -> Any:
        if (
            actor.subject_id != subject_id
            or actor.subject_incarnation is None
            or actor.demand_reporter_incarnation is None
        ):
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, executions = execution_runtime(request)
        try:
            async with session_factory() as session:
                result = await executions.next_subject_bootstrap(
                    session,
                    subject_id=subject_id,
                    subject_incarnation=actor.subject_incarnation,
                    reporter_incarnation=actor.demand_reporter_incarnation,
                )
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.put("/v2/subjects/{subject_id}/intents/{intent_id}/bootstrap-acknowledgements")
    async def acknowledge_executable_bootstrap(
        subject_id: UUID,
        intent_id: UUID,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:report:demand")),
        value: ExecutableBootstrapAcknowledgementV2 = Depends(
            executable_bootstrap_acknowledgement_body
        ),
        idempotency_key: UUID = Header(alias="Idempotency-Key"),
    ) -> Any:
        binding = value.binding
        if (
            binding.subject_id != subject_id
            or binding.intent_id != intent_id
            or actor.subject_id != subject_id
            or actor.subject_incarnation != binding.subject_incarnation
            or actor.demand_reporter_incarnation != value.reporter_incarnation
        ):
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, executions = execution_runtime(request)
        try:
            async with session_factory() as session:
                result = await executions.acknowledge_bootstrap(
                    session,
                    value,
                    actor=actor.principal_id,
                    idempotency_key=idempotency_key,
                )
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.post("/v2/executors/{pool_id}/permits/{permit_id}/consume")
    async def consume_executable_permit(
        pool_id: str,
        permit_id: UUID,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
        value: ExecutablePermitConsumptionV2 = Depends(executable_consumption_body),
    ) -> Any:
        binding = value.binding
        assert_executor_actor(
            actor,
            pool_id=pool_id,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_generation=binding.pool_generation,
        )
        if binding.pool_id != pool_id or value.permit_id != permit_id:
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, executions = execution_runtime(request)
        try:
            async with session_factory() as session:
                result = await executions.consume_launch_permit(session, value)
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.post("/v2/executors/{pool_id}/permits/{permit_id}/recover")
    async def recover_executable_submission(
        pool_id: str,
        permit_id: UUID,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
        value: ExecutableSubmissionRecoveryV2 = Depends(executable_recovery_body),
    ) -> Any:
        binding = value.binding
        assert_executor_actor(
            actor,
            pool_id=pool_id,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_generation=binding.pool_generation,
        )
        if binding.pool_id != pool_id or value.permit_id != permit_id:
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, executions = execution_runtime(request)
        try:
            async with session_factory() as session:
                result = await executions.recover_unsubmitted_permit(session, value)
            payload = jsonable_encoder(result)
            observed_state = payload.get("state")
            if observed_state is None:
                payload["state"] = "quarantined"
            elif observed_state != "quarantined":
                raise HTTPException(status_code=409, detail="capacity state conflict")
            return payload
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.post("/v2/executors/{pool_id}/intents/{intent_id}/close")
    async def close_executable_intent(
        pool_id: str,
        intent_id: UUID,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
        value: ExecutableIntentCloseV2 = Depends(executable_close_body),
    ) -> Any:
        binding = value.binding
        assert_executor_actor(
            actor,
            pool_id=pool_id,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_generation=binding.pool_generation,
        )
        if binding.pool_id != pool_id or binding.intent_id != intent_id:
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, executions = execution_runtime(request)
        try:
            async with session_factory() as session:
                result = await executions.begin_intent_close(session, value)
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.post("/v2/executors/{pool_id}/reservations/{tranche_id}/release")
    async def release_executable_shapes(
        pool_id: str,
        tranche_id: UUID,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:execute:pool")),
        value: ExecutablePartialReleaseV2 = Depends(executable_release_body),
    ) -> Any:
        first_binding = value.releases[0].binding
        assert_executor_actor(
            actor,
            pool_id=pool_id,
            executor_id=value.executor_id,
            executor_incarnation=value.executor_incarnation,
            pool_generation=first_binding.pool_generation,
        )
        if value.tranche_id != tranche_id or any(
            item.binding.pool_id != pool_id
            or item.binding.pool_generation != first_binding.pool_generation
            for item in value.releases
        ):
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, executions = execution_runtime(request)
        try:
            async with session_factory() as session:
                result = await executions.release_shapes(session, value)
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.put("/v2/reports/protected-releases/{subject_id}/{shape_instance_id}")
    async def acknowledge_executable_protected_release(
        subject_id: UUID,
        shape_instance_id: str,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:report:demand")),
        value: ExecutableProtectedReleaseV2 = Depends(executable_protected_release_body),
        idempotency_key: UUID = Header(alias="Idempotency-Key"),
    ) -> Any:
        binding = value.binding
        if (
            binding.subject_id != subject_id
            or binding.shape_instance_id != shape_instance_id
            or actor.subject_id != subject_id
            or actor.subject_incarnation != binding.subject_incarnation
            or actor.demand_reporter_incarnation != value.reporter_incarnation
        ):
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, executions = execution_runtime(request)
        try:
            async with session_factory() as session:
                result = await executions.acknowledge_protected_release(
                    session,
                    value,
                    actor=actor.principal_id,
                    idempotency_key=idempotency_key,
                )
            return jsonable_encoder(result)
        except CapacityStoreError as exc:
            raise _store_error(exc) from exc

    @app.post("/v1/shadow-reconciliations")
    async def reconcile(
        request: Request,
        _actor: CapacityPrincipal = Depends(require("capacity:reconcile")),
    ) -> Any:
        if reconciliation_lock.locked():
            raise HTTPException(status_code=409, detail="shadow reconciliation already running")
        await reconciliation_lock.acquire()
        try:
            session_factory, store, writer = runtime(request)
            result = await reconcile_shadow_once(
                session_factory,
                writer,
                allocator=allocator,
                max_attempts=settings.reconciliation_max_attempts,
                allocation_timeout_seconds=settings.allocation_timeout_seconds,
                store=store,
            )
            metrics.observe_run(result.status, _run_reason(result))
            if result.reason == "capacity writer fence changed":
                request.app.state.ready = False
                metrics.ready.set(0)
                raise HTTPException(status_code=503, detail="capacity writer is not ready")
            return jsonable_encoder(result)
        finally:
            reconciliation_lock.release()

    async def _allocation_input(request: Request):  # type: ignore[no-untyped-def]
        session_factory, store, writer = runtime(request)
        try:
            async with session_factory() as session:
                return await store.load_allocation_input(session, writer)
        except StaleWriterError as exc:
            request.app.state.ready = False
            metrics.ready.set(0)
            raise HTTPException(status_code=503, detail="capacity writer is not ready") from exc

    @app.get("/v1/status")
    async def aggregate_status(
        request: Request,
        _actor: CapacityPrincipal = Depends(require("capacity:read")),
    ) -> Any:
        allocation_input = await _allocation_input(request)
        session_factory, _store, _writer = runtime(request)
        async with session_factory() as session:
            authority = (await session.execute(select(CapacityAuthorityState))).scalar_one()
            latest = (
                (
                    await session.execute(
                        select(CapacityAllocationEpoch)
                        .where(CapacityAllocationEpoch.status == "shadow")
                        .order_by(CapacityAllocationEpoch.allocation_epoch.desc())
                        .limit(1)
                    )
                )
                .scalars()
                .first()
            )
        freshness: Counter[str] = Counter(
            f"subject:{item.freshness.state}" for item in allocation_input.subjects
        )
        freshness.update(f"pool:{item.freshness.state}" for item in allocation_input.pools)
        accounts: defaultdict[str, int] = defaultdict(int)
        tiers: defaultdict[str, int] = defaultdict(int)
        pools: defaultdict[str, int] = defaultdict(int)
        blockers: Counter[str] = Counter()
        latest_digest: str | None = None
        latest_epoch: int | None = None
        if latest is not None:
            shadow = ShadowEpochV1.model_validate(latest.complete_payload)
            latest_digest = shadow.input_digest
            latest_epoch = latest.allocation_epoch
            subject_config = {
                item.configuration.subject_id: item.configuration
                for item in allocation_input.subjects
            }
            for item in shadow.allocations:
                config = subject_config[item.subject_id]
                accounts[config.account_id] += item.desired_slots
                tiers[config.tier_id] += item.desired_slots
                pools[item.pool_id] += item.desired_slots
                blockers.update(item.blockers)
            blockers.update(shadow.blockers)
        return {
            "schema_version": 1,
            "authority_incarnation": authority.authority_incarnation,
            "writer_epoch": authority.writer_epoch,
            "configuration_epoch": allocation_input.configuration.configuration_epoch,
            "configuration_digest": canonical_digest(allocation_input.configuration),
            "report_freshness_counts": dict(sorted(freshness.items())),
            "latest_shadow_epoch": latest_epoch,
            "latest_shadow_input_digest": latest_digest,
            "account_slots": dict(sorted(accounts.items())),
            "tier_slots": dict(sorted(tiers.items())),
            "pool_slots": dict(sorted(pools.items())),
            "blocker_counts": dict(sorted(blockers.items())),
            "increase_freeze": authority.increase_freeze,
            "execution_epoch": authority.execution_epoch,
            "execution_state": authority.execution_state,
            "execution_manifest_sha256": authority.execution_manifest_sha256,
            "executable_new_capacity_ceiling": authority.executable_new_capacity_ceiling,
        }

    @app.get("/v1/status/subjects")
    async def subject_status(
        request: Request,
        cursor: UUID | None = None,
        limit: int = Query(default=100),
        _actor: CapacityPrincipal = Depends(require("capacity:read")),
    ) -> Any:
        limit = _bounded_limit(limit)
        session_factory, _store, _writer = runtime(request)
        async with session_factory() as session:
            active_epoch = (
                await session.execute(select(func.max(CapacitySubject.configuration_epoch)))
            ).scalar_one()
            query = select(CapacitySubject).where(
                CapacitySubject.configuration_epoch == active_epoch
            )
            if cursor is not None:
                query = query.where(CapacitySubject.subject_id > cursor)
            rows = (
                (await session.execute(query.order_by(CapacitySubject.subject_id).limit(limit + 1)))
                .scalars()
                .all()
            )
        page = rows[:limit]
        return {
            "items": [
                {
                    "subject_id": row.subject_id,
                    "subject_incarnation": row.subject_incarnation,
                    "account_id": row.account_id,
                    "tier_id": row.tier_id,
                    "min_slots": row.min_slots,
                    "max_slots": row.max_slots,
                    "lifecycle_state": row.lifecycle_state,
                }
                for row in page
            ],
            "next_cursor": str(page[-1].subject_id) if len(rows) > limit and page else None,
        }

    @app.get("/v1/status/pools")
    async def pool_status(
        request: Request,
        _actor: CapacityPrincipal = Depends(require("capacity:read")),
    ) -> Any:
        session_factory, _store, _writer = runtime(request)
        async with session_factory() as session:
            active_epoch = (
                await session.execute(select(func.max(CapacityPool.configuration_epoch)))
            ).scalar_one()
            rows = (
                (
                    await session.execute(
                        select(CapacityPool)
                        .where(CapacityPool.configuration_epoch == active_epoch)
                        .order_by(CapacityPool.pool_id)
                        .limit(MAX_POOLS)
                    )
                )
                .scalars()
                .all()
            )
        return {
            "items": [
                {
                    "pool_id": row.pool_id,
                    "pool_generation": row.pool_generation,
                    "health": row.health,
                    "max_slots": row.max_slots,
                    "max_pending_slots": row.max_pending_slots,
                    "max_pending_jobs": row.max_pending_jobs,
                }
                for row in rows
            ]
        }

    @app.get("/v1/status/executors")
    async def executor_status(
        request: Request,
        _actor: CapacityPrincipal = Depends(require("capacity:read")),
    ) -> Any:
        session_factory, _store, _writer = runtime(request)
        async with session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(CapacityExecutor)
                        .order_by(CapacityExecutor.pool_id, CapacityExecutor.created_at)
                        .limit(MAX_POOLS * 16)
                    )
                )
                .scalars()
                .all()
            )
        return {
            "items": [
                {
                    "executor_id": row.executor_id,
                    "executor_incarnation": row.executor_incarnation,
                    "pool_id": row.pool_id,
                    "pool_generation": row.pool_generation,
                    "state": row.state,
                    "command_high_water": row.command_high_water,
                    "heartbeat_high_water": row.heartbeat_high_water,
                    "inventory_high_water": row.inventory_high_water,
                    "journal_high_water": row.journal_high_water,
                    "lease_expires_at": row.lease_expires_at,
                    "executable": False,
                }
                for row in rows
            ]
        }

    @app.get("/v2/status/execution-preparation")
    async def execution_preparation_status(
        request: Request,
        _actor: CapacityPrincipal = Depends(require("capacity:read")),
    ) -> Any:
        session_factory, store, _writer = runtime(request)
        policy = store.execution_policy
        policy_sha256 = resolved_execution_policy_sha256 if policy is not None else None
        if policy is not None and policy_sha256 is None:
            raise HTTPException(
                status_code=503,
                detail="execution preparation status unavailable",
            )
        try:
            async with session_factory() as session:
                result = await load_prepared_execution_readiness(
                    session,
                    execution_policy=policy,
                    execution_policy_sha256=policy_sha256,
                    freshness_seconds=settings.freshness_seconds,
                )
            return jsonable_encoder(result)
        except ValueError as exc:
            raise HTTPException(
                status_code=503,
                detail="execution preparation status unavailable",
            ) from exc

    @app.get("/v2/status/executors")
    async def executable_executor_status(
        request: Request,
        _actor: CapacityPrincipal = Depends(require("capacity:read")),
    ) -> Any:
        session_factory, _store, _writer = runtime(request)
        async with session_factory() as session:
            authority = (await session.execute(select(CapacityAuthorityState))).scalar_one()
            now = (await session.execute(select(func.now()))).scalar_one()
            rows = (
                (
                    await session.execute(
                        select(CapacityExecutableExecutorState)
                        .where(
                            CapacityExecutableExecutorState.execution_epoch
                            == authority.execution_epoch
                        )
                        .order_by(CapacityExecutableExecutorState.pool_id)
                        .limit(MAX_POOLS + 1)
                    )
                )
                .scalars()
                .all()
            )
        if len(rows) > MAX_POOLS:
            raise HTTPException(status_code=503, detail="executable executor status is unbounded")
        items = [
            _executor_status_item(
                row,
                now=now,
                freshness_seconds=settings.freshness_seconds,
            )[0]
            for row in rows
        ]
        blockers = _manager_execution_blockers(authority)
        if authority.execution_epoch > 0:
            observed_pools = {row.pool_id for row in rows}
            blockers.extend(
                f"executor-missing:{pool_id}"
                for pool_id in ("gb10", "oldlab")
                if pool_id not in observed_pools
            )
        return jsonable_encoder(
            {
                "schema_version": 2,
                "execution_epoch": authority.execution_epoch,
                "execution_state": authority.execution_state,
                "executable_new_capacity_ceiling": (authority.executable_new_capacity_ceiling),
                "items": items,
                "blockers": sorted(set(blockers)),
            }
        )

    @app.get("/v2/status/subjects/{subject_id}")
    async def executable_subject_status(
        subject_id: UUID,
        request: Request,
        _actor: CapacityPrincipal = Depends(require("capacity:read")),
    ) -> Any:
        session_factory, _store, _writer = runtime(request)
        async with session_factory() as session:
            authority = (await session.execute(select(CapacityAuthorityState))).scalar_one()
            configuration_epoch = (
                await session.execute(select(func.max(CapacitySubject.configuration_epoch)))
            ).scalar_one()
            subject = (
                await session.execute(
                    select(CapacitySubject).where(
                        CapacitySubject.configuration_epoch == configuration_epoch,
                        CapacitySubject.subject_id == subject_id,
                    )
                )
            ).scalar_one_or_none()
            if subject is None:
                raise HTTPException(status_code=404, detail="capacity subject not found")
            intents = (
                (
                    await session.execute(
                        select(CapacityExecutableIntent)
                        .where(
                            CapacityExecutableIntent.execution_epoch == authority.execution_epoch,
                            CapacityExecutableIntent.subject_id == subject.subject_id,
                            CapacityExecutableIntent.subject_incarnation
                            == subject.subject_incarnation,
                        )
                        .order_by(CapacityExecutableIntent.launch_rank)
                        .limit(MAX_FIXED_CLAIMS_PER_REPORT + 1)
                    )
                )
                .scalars()
                .all()
            )
            executor_rows = (
                (
                    await session.execute(
                        select(CapacityExecutableExecutorState)
                        .where(
                            CapacityExecutableExecutorState.execution_epoch
                            == authority.execution_epoch
                        )
                        .order_by(CapacityExecutableExecutorState.pool_id)
                        .limit(MAX_POOLS + 1)
                    )
                )
                .scalars()
                .all()
            )
            now = (await session.execute(select(func.now()))).scalar_one()
        if len(intents) > MAX_FIXED_CLAIMS_PER_REPORT or len(executor_rows) > MAX_POOLS:
            raise HTTPException(status_code=503, detail="capacity subject status is unbounded")

        intent_state_counts = Counter(intent.state for intent in intents)
        quarantined = sum(intent.state == "quarantined" for intent in intents)
        intents_by_id = {intent.intent_id: intent for intent in intents}
        active_capacity: dict[UUID, int] = {}
        active_bindings: dict[UUID, ExecutableIntentBindingV2] = {}
        executor_blockers: list[str] = []
        for executor_row in executor_rows:
            item, inventory = _executor_status_item(
                executor_row,
                now=now,
                freshness_seconds=settings.freshness_seconds,
            )
            row_blockers = cast(list[str], item["blockers"])
            executor_blockers.extend(
                f"{executor_row.pool_id}:{blocker}" for blocker in row_blockers
            )
            critical = set(row_blockers) - {"executor-inventory-quarantine"}
            if inventory is None or critical:
                continue
            for record in inventory.records:
                proof = record.ownership_proof
                if (
                    record.physical_kind != "slurm-job"
                    or record.state != "active"
                    or record.authority_scope != "dedicated-loom-association"
                    or proof is None
                ):
                    continue
                binding = proof.metadata.binding
                intent = intents_by_id.get(binding.intent_id)
                if intent is None:
                    continue
                try:
                    stored_binding = ExecutableIntentBindingV2.model_validate_json(
                        json.dumps(intent.binding_payload)
                    )
                except ValidationError:
                    continue
                if (
                    binding != stored_binding
                    or binding.subject_id != subject.subject_id
                    or binding.subject_incarnation != subject.subject_incarnation
                    or binding.deployment_generation != subject.deployment_generation
                    or intent.inventory_sequence != inventory.inventory_sequence
                    or intent.observed_state != "active"
                    or intent.state != "observed"
                ):
                    continue
                active_capacity[binding.intent_id] = binding.concurrency_slots
                active_bindings[binding.intent_id] = binding

        # Scheduler evidence establishes active physical intent only.  A
        # protected personal guard registration is deliberately not readable
        # by this global manager, so it can never turn scheduler evidence into
        # worker availability on its own.
        worker_available = False
        if authority.execution_state == "active":
            capacity_status = "waiting"
        elif authority.execution_state == "shadow":
            capacity_status = "shadow"
        else:
            capacity_status = "prepared"
        blockers = _manager_execution_blockers(authority)
        if not worker_available and authority.execution_state == "active":
            if quarantined:
                blockers.append("quarantined-intent")
            elif active_capacity:
                blockers.append("worker-registration-pending")
            elif not intents:
                blockers.append("allocation-pending")
            else:
                blockers.extend(
                    f"intent-{state_name}" for state_name in sorted(intent_state_counts)
                )
            blockers.extend(executor_blockers)
        return jsonable_encoder(
            {
                "schema_version": 2,
                "subject_id": subject.subject_id,
                "subject_incarnation": subject.subject_incarnation,
                "deployment_generation": subject.deployment_generation,
                "configuration_epoch": subject.configuration_epoch,
                "execution_epoch": authority.execution_epoch,
                "execution_state": authority.execution_state,
                "executable_new_capacity_ceiling": (authority.executable_new_capacity_ceiling),
                "capacity_prepared": True,
                "capacity_status": capacity_status,
                "worker_available": worker_available,
                # This is deliberately the exact canonical binding accepted by
                # the fresh manager inventory, not a reconstructed summary.
                # The personal status observer compares it byte-for-byte with
                # its protected local observation before reporting availability.
                "active_capacity_intents": [
                    active_bindings[intent_id].model_dump(mode="json")
                    for intent_id in sorted(active_bindings, key=str)
                ],
                "active_capacity_intent_count": len(active_capacity),
                "active_capacity_slots": sum(active_capacity.values()),
                "quarantined_intent_count": quarantined,
                "intent_state_counts": dict(sorted(intent_state_counts.items())),
                "blockers": sorted(set(blockers)),
            }
        )

    @app.get("/v1/status/reservations")
    async def reservation_status(
        request: Request,
        cursor: UUID | None = None,
        limit: int = Query(default=100),
        _actor: CapacityPrincipal = Depends(require("capacity:read")),
    ) -> Any:
        limit = _bounded_limit(limit)
        session_factory, _store, _writer = runtime(request)
        query = select(CapacityReservationTranche)
        if cursor is not None:
            query = query.where(CapacityReservationTranche.id > cursor)
        async with session_factory() as session:
            rows = (
                (
                    await session.execute(
                        query.order_by(CapacityReservationTranche.id).limit(limit + 1)
                    )
                )
                .scalars()
                .all()
            )
            page = rows[:limit]
            tranche_ids = [row.id for row in page]
            shape_counts = (
                await session.execute(
                    select(
                        CapacityReservationShape.tranche_id,
                        CapacityReservationShape.state,
                        func.count(),
                    )
                    .where(CapacityReservationShape.tranche_id.in_(tranche_ids))
                    .group_by(
                        CapacityReservationShape.tranche_id,
                        CapacityReservationShape.state,
                    )
                )
            ).all()
            intent_counts = (
                await session.execute(
                    select(
                        CapacitySubmissionIntent.tranche_id,
                        CapacitySubmissionIntent.state,
                        func.count(),
                    )
                    .where(CapacitySubmissionIntent.tranche_id.in_(tranche_ids))
                    .group_by(
                        CapacitySubmissionIntent.tranche_id,
                        CapacitySubmissionIntent.state,
                    )
                )
            ).all()
        shapes: defaultdict[UUID, dict[str, int]] = defaultdict(dict)
        intents: defaultdict[UUID, dict[str, int]] = defaultdict(dict)
        for tranche_id, state_name, count in shape_counts:
            shapes[tranche_id][state_name] = count
        for tranche_id, state_name, count in intent_counts:
            intents[tranche_id][state_name] = count
        return {
            "items": [
                {
                    "tranche_id": row.id,
                    "subject_id": row.subject_id,
                    "pool_id": row.pool_id,
                    "pool_generation": row.pool_generation,
                    "allocation_epoch": row.allocation_epoch,
                    "state": row.state,
                    "closure_reason": row.closure_reason,
                    "shape_counts": dict(sorted(shapes[row.id].items())),
                    "intent_counts": dict(sorted(intents[row.id].items())),
                    "executable": False,
                }
                for row in page
            ],
            "next_cursor": str(page[-1].id) if len(rows) > limit and page else None,
        }

    @app.get("/v1/shadow-epochs/{allocation_epoch}")
    async def shadow_epoch_status(
        allocation_epoch: int,
        request: Request,
        _actor: CapacityPrincipal = Depends(require("capacity:read")),
    ) -> Any:
        session_factory, _store, _writer = runtime(request)
        async with session_factory() as session:
            row = await session.get(CapacityAllocationEpoch, allocation_epoch)
        if (
            row is None
            or row.status != "shadow"
            or row.executable
            or row.execution_epoch is not None
            or row.execution_manifest_sha256 is not None
        ):
            raise HTTPException(status_code=404, detail="shadow epoch not found")
        return {
            "allocation_epoch": row.allocation_epoch,
            "status": row.status,
            "input_digest": row.input_digest,
            "failure_reason": row.failure_reason,
            "complete_payload": row.complete_payload,
            "executable": False,
        }

    @app.get("/v1/shadow-epochs/{allocation_epoch}/allocations")
    async def shadow_allocations(
        allocation_epoch: int,
        request: Request,
        cursor: UUID | None = None,
        limit: int = Query(default=100),
        _actor: CapacityPrincipal = Depends(require("capacity:read")),
    ) -> Any:
        limit = _bounded_limit(limit)
        session_factory, _store, _writer = runtime(request)
        async with session_factory() as session:
            epoch = await session.get(CapacityAllocationEpoch, allocation_epoch)
            if (
                epoch is None
                or epoch.status != "shadow"
                or epoch.executable
                or epoch.execution_epoch is not None
                or epoch.execution_manifest_sha256 is not None
            ):
                raise HTTPException(status_code=404, detail="shadow epoch not found")
            query = select(CapacityAllocation).where(
                CapacityAllocation.allocation_epoch == allocation_epoch,
                CapacityAllocation.mode == "shadow",
                CapacityAllocation.executable.is_(False),
                CapacityAllocation.execution_epoch.is_(None),
                CapacityAllocation.execution_manifest_sha256.is_(None),
            )
            if cursor is not None:
                query = query.where(CapacityAllocation.id > cursor)
            rows = (
                (await session.execute(query.order_by(CapacityAllocation.id).limit(limit + 1)))
                .scalars()
                .all()
            )
        page = rows[:limit]
        return {
            "items": [
                {
                    "id": row.id,
                    "subject_id": row.subject_id,
                    "subject_incarnation": row.subject_incarnation,
                    "deployment_generation": row.deployment_generation,
                    "pool_id": row.pool_id,
                    "desired_shapes": row.desired_shapes,
                    "commitments": row.commitments,
                    "drains": row.drains,
                    "allowances": row.allowances,
                    "witness": row.witness,
                    "executable": False,
                }
                for row in page
            ],
            "next_cursor": str(page[-1].id) if len(rows) > limit and page else None,
        }

    @app.get("/v1/audit-events")
    async def audit_events(
        request: Request,
        cursor: int | None = None,
        limit: int = Query(default=100),
        _actor: CapacityPrincipal = Depends(require("capacity:read")),
    ) -> Any:
        limit = _bounded_limit(limit)
        if cursor is not None and cursor < 0:
            raise HTTPException(status_code=422, detail="cursor must be nonnegative")
        session_factory, _store, _writer = runtime(request)
        query = select(CapacityAuditEvent)
        if cursor is not None:
            query = query.where(CapacityAuditEvent.id > cursor)
        async with session_factory() as session:
            rows = (
                (await session.execute(query.order_by(CapacityAuditEvent.id).limit(limit + 1)))
                .scalars()
                .all()
            )
        page = rows[:limit]
        return {
            "items": [
                {
                    "id": row.id,
                    "actor_kind": row.actor_kind,
                    "actor_id": row.actor_id,
                    "event_kind": row.event_kind,
                    "object_binding": row.object_binding,
                    "detail": row.detail,
                    "created_at": row.created_at,
                }
                for row in page
            ],
            "next_cursor": page[-1].id if len(rows) > limit and page else None,
        }

    @app.get("/metrics")
    async def prometheus_metrics(
        request: Request,
        _actor: CapacityPrincipal = Depends(require("capacity:read")),
    ) -> Response:
        allocation_input = await _allocation_input(request)
        session_factory, _store, _writer = runtime(request)
        async with session_factory() as session:
            authority = (await session.execute(select(CapacityAuthorityState))).scalar_one()
        metrics.ready.set(1)
        metrics.increase_freeze.set(1 if authority.increase_freeze else 0)
        metrics.executable_new_capacity_ceiling.set(authority.executable_new_capacity_ceiling)
        metrics.report_freshness.clear()
        freshness = Counter(("demand", item.freshness.state) for item in allocation_input.subjects)
        freshness.update(("pool", item.freshness.state) for item in allocation_input.pools)
        for report_kind in REPORT_KINDS:
            for freshness_state in FRESHNESS_STATES:
                metrics.report_freshness.labels(
                    report_kind=report_kind,
                    state=freshness_state,
                ).set(freshness[(report_kind, freshness_state)])
        metrics.pool_slots.clear()
        for pool in allocation_input.fleet.pools:
            for pool_state in POOL_SLOT_STATES:
                value = pool.max_slots if pool_state == "configured" else 0
                metrics.pool_slots.labels(pool_id=pool.pool_id, state=pool_state).set(value)
        return Response(content=metrics.render(), media_type="text/plain; version=0.0.4")

    return app


__all__ = ["RequestBodyLimitMiddleware", "create_app"]
