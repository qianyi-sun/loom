"""Authenticated, shadow-only HTTP surface for global capacity evidence."""

# ruff: noqa: B008 - FastAPI declares dependency injection in parameter defaults.

from __future__ import annotations

import asyncio
from collections import Counter, defaultdict
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any, TypeVar, cast
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.encoders import jsonable_encoder
from pydantic import ValidationError
from sqlalchemy import func, select, text
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
)
from loom_capacity_manager.config import CapacityManagerSettings, read_owner_only_secret
from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    MAX_POOLS,
    ConfigurationActivationV1,
    DemandSnapshotV1,
    FleetManifestV1,
    PoolObservationV1,
    ShadowEpochV1,
    StrictV1Model,
    SubjectConfigurationV1,
    canonical_digest,
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
    CapacityPool,
    CapacitySubject,
)
from loom_capacity_manager.reconciler import (
    ShadowAllocator,
    ShadowRunResult,
    reconcile_shadow_once,
)
from loom_capacity_manager.store import (
    CapacityManagementStore,
    CapacityStoreError,
    ConfigurationConflictError,
    IdempotencyConflictError,
    ReportEquivocationError,
    StaleAllocationInputError,
    StaleReportError,
    StaleWriterError,
    UnknownReporterError,
    WriterFence,
)

_ContractT = TypeVar("_ContractT", bound=StrictV1Model)


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
            IdempotencyConflictError,
            ReportEquivocationError,
            StaleReportError,
            StaleAllocationInputError,
        ),
    ):
        return HTTPException(status_code=409, detail="capacity state conflict")
    if isinstance(exc, StaleWriterError):
        return HTTPException(status_code=503, detail="capacity writer is not ready")
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


def create_app(
    settings: CapacityManagerSettings,
    *,
    verifier: CapacityPrincipalVerifier | None = None,
    allocator: ShadowAllocator = allocate_shadow,
) -> FastAPI:
    """Build one process-local API; DB fencing remains the cross-process boundary."""

    resolved_verifier = verifier or CapacityPrincipalVerifier.from_file(settings.principals_file)
    metrics = CapacityMetrics()
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
            store = CapacityManagementStore(freshness_seconds=settings.freshness_seconds)
            async with session_factory() as session:
                revision = (
                    await session.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar_one()
                if revision != "capacity_0001":
                    raise RuntimeError("capacity database schema mismatch")
                authority = (
                    await session.execute(
                        select(CapacityAuthorityState).where(
                            CapacityAuthorityState.singleton_id == 1
                        )
                    )
                ).scalar_one()
                if authority.authority_incarnation != settings.expected_authority_incarnation:
                    raise RuntimeError("capacity authority incarnation mismatch")
                writer = await store.register_writer(
                    session,
                    settings.expected_authority_incarnation,
                    expected_epoch=authority.writer_epoch,
                )
                await session.commit()
            app.state.engine = engine
            app.state.session_factory = session_factory
            app.state.store = store
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
    demand_body = contract_body(DemandSnapshotV1)
    pool_body = contract_body(PoolObservationV1)

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

    async def writer_is_current(request: Request) -> bool:
        if not getattr(request.app.state, "ready", False):
            return False
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
            current = (
                authority.authority_incarnation == writer.authority_incarnation
                and authority.writer_epoch == writer.writer_epoch
                and authority.executable_new_capacity_ceiling == 0
            )
        except Exception:
            current = False
        if not current:
            request.app.state.ready = False
            metrics.ready.set(0)
        return current

    @app.get("/healthz")
    async def health(request: Request) -> Response:
        ready = await writer_is_current(request)
        return Response(
            content=(
                b'{"status":"ready","executable_new_capacity_ceiling":0}'
                if ready
                else b'{"status":"not-ready","executable_new_capacity_ceiling":0}'
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

    @app.put("/v1/reports/demand/{subject_id}")
    async def ingest_demand(
        subject_id: UUID,
        request: Request,
        actor: CapacityPrincipal = Depends(require("capacity:report:demand")),
        value: DemandSnapshotV1 = Depends(demand_body),
    ) -> Any:
        if (
            value.subject_id != subject_id
            or actor.subject_id != subject_id
            or actor.subject_incarnation != value.subject_incarnation
            or actor.demand_reporter_incarnation != value.reporter_incarnation
        ):
            raise HTTPException(status_code=403, detail="forbidden")
        session_factory, store, _writer = runtime(request)
        try:
            async with session_factory() as session:
                result = await store.ingest_demand_snapshot(
                    session,
                    value,
                    actor=actor.principal_id,
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
            "executable_new_capacity_ceiling": 0,
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

    @app.get("/v1/shadow-epochs/{allocation_epoch}")
    async def shadow_epoch_status(
        allocation_epoch: int,
        request: Request,
        _actor: CapacityPrincipal = Depends(require("capacity:read")),
    ) -> Any:
        session_factory, _store, _writer = runtime(request)
        async with session_factory() as session:
            row = await session.get(CapacityAllocationEpoch, allocation_epoch)
        if row is None:
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
        query = select(CapacityAllocation).where(
            CapacityAllocation.allocation_epoch == allocation_epoch
        )
        if cursor is not None:
            query = query.where(CapacityAllocation.id > cursor)
        async with session_factory() as session:
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
