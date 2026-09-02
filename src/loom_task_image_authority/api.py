"""Bounded mutually authenticated HTTP surface for task-image projection."""

# ruff: noqa: B008 - FastAPI dependency injection uses parameter defaults.

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any, TypeVar, cast
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest
from pydantic import BaseModel, ValidationError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from loom.db.schema_startup import assert_schema_at_head
from loom.security.secret_store import LocalEncryptedSecretStore
from loom_task_image_authority.auth import (
    TaskImageAuthorityAuthorizationError,
    TaskImagePrincipalVerifier,
)
from loom_task_image_authority.config import (
    TaskImageAuthoritySettings,
    TaskImageSecretStoreKeyring,
    load_secret_store_keyring,
    read_owner_only_secret,
)
from loom_task_image_authority.contracts import (
    MAX_CONTRACT_BYTES,
    GuardScope,
    TaskImageAttachmentProofV1,
    TaskImageBootstrapExchangeV1,
    TaskImageContainmentAttestationV1,
    TaskImageGuardPrincipalV1,
    TaskImageProjectionRequestV1,
    TaskImageProjectionRevocationV1,
    new_bootstrap_token,
    new_session_token,
)
from loom_task_image_authority.store import (
    TaskImageProjectionAuthorizationError,
    TaskImageProjectionConflictError,
    TaskImageProjectionEquivocationError,
    complete_task_image_projection,
    exchange_task_image_bootstrap,
    record_task_image_containment_attestation,
    request_task_image_projection,
    revoke_task_image_projection,
)

_ContractT = TypeVar("_ContractT", bound=BaseModel)
_TransitionT = TypeVar("_TransitionT")


class RequestBodyLimitMiddleware:
    """Bound streamed bodies before JSON parsing, including chunked requests."""

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
                await self._reject(
                    send,
                    status.HTTP_400_BAD_REQUEST,
                    "task-image authority invalid content length",
                )
                return
            if int(supplied) > self.maximum_bytes:
                await self._reject(
                    send,
                    status.HTTP_413_CONTENT_TOO_LARGE,
                    "task-image authority request too large",
                )
                return

        received = 0
        messages: list[dict[str, Any]] = []
        while True:
            message = cast(dict[str, Any], await receive())
            messages.append(message)
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.maximum_bytes:
                    await self._reject(
                        send,
                        status.HTTP_413_CONTENT_TOO_LARGE,
                        "task-image authority request too large",
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
        payload = (f'{{"detail":"{detail}"}}').encode("ascii")
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


class AuthorityTrafficLimitMiddleware:
    """Apply fixed per-process request-rate and in-flight ceilings to mutations."""

    def __init__(
        self,
        app: Any,
        *,
        requests_per_second: int,
        concurrency: int,
    ) -> None:
        self.app = app
        self.requests_per_second = requests_per_second
        self.concurrency = concurrency
        self._lock = asyncio.Lock()
        self._window = time.monotonic()
        self._window_count = 0
        self._in_flight = 0

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http" or not scope.get("path", "").startswith("/v1/"):
            await self.app(scope, receive, send)
            return

        admitted = False
        async with self._lock:
            now = time.monotonic()
            if now - self._window >= 1.0:
                self._window = now
                self._window_count = 0
            if self._window_count >= self.requests_per_second:
                response_status = status.HTTP_429_TOO_MANY_REQUESTS
                detail = "task-image authority rate limited"
            elif self._in_flight >= self.concurrency:
                response_status = status.HTTP_503_SERVICE_UNAVAILABLE
                detail = "task-image authority concurrency exhausted"
            else:
                self._window_count += 1
                self._in_flight += 1
                admitted = True
        if not admitted:
            await RequestBodyLimitMiddleware._reject(send, response_status, detail)
            return
        try:
            await self.app(scope, receive, send)
        finally:
            async with self._lock:
                self._in_flight -= 1


class TaskImageAuthorityMetrics:
    """Process-local aggregate metrics with a deliberately closed label set."""

    def __init__(self) -> None:
        self.registry = CollectorRegistry(auto_describe=True)
        self.ready = Gauge(
            "loom_task_image_authority_ready",
            "Whether task-image authority startup checks passed.",
            registry=self.registry,
        )
        self.in_flight = Gauge(
            "loom_task_image_authority_in_flight",
            "Number of task-image authority transitions currently running.",
            registry=self.registry,
        )
        self.requests = Counter(
            "loom_task_image_authority_requests_total",
            "Bounded task-image authority transition outcomes.",
            labelnames=("route", "outcome"),
            registry=self.registry,
        )

    def render(self) -> bytes:
        return generate_latest(self.registry)


class AuthorityMetricsMiddleware:
    """Count every bounded route outcome without using request-derived labels."""

    def __init__(self, app: Any, *, metrics: TaskImageAuthorityMetrics) -> None:
        self.app = app
        self.metrics = metrics

    @staticmethod
    def _route(scope: dict[str, Any]) -> str | None:
        path = str(scope.get("path", ""))
        method = str(scope.get("method", ""))
        if method == "GET" and path == "/healthz":
            return "healthz"
        if method == "GET" and path == "/metrics":
            return "metrics"
        if method != "PUT":
            return None
        parts = path.strip("/").split("/")
        if len(parts) == 4 and parts[:2] == ["v1", "projections"]:
            return {
                "challenge": "challenge",
                "attachment": "attachment",
                "exchange": "exchange",
                "revocation": "revocation",
            }.get(parts[3])
        if (
            len(parts) == 5
            and parts[:2] == ["v1", "projections"]
            and parts[3] == "attestations"
        ):
            return "attestation"
        return None

    @staticmethod
    def _outcome(response_status: int) -> str:
        if 200 <= response_status < 300:
            return "success"
        if response_status in {401, 403}:
            return "rejected"
        if response_status == 409:
            return "conflict"
        if response_status == 429:
            return "limited"
        if response_status in {400, 413, 422}:
            return "invalid"
        return "unavailable"

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        route = self._route(scope)
        if scope["type"] != "http" or route is None:
            await self.app(scope, receive, send)
            return

        response_status = 500

        async def metrics_send(message: dict[str, Any]) -> None:
            nonlocal response_status
            if message["type"] == "http.response.start":
                response_status = int(message["status"])
            await send(message)

        self.metrics.in_flight.inc()
        try:
            await self.app(scope, receive, metrics_send)
        finally:
            self.metrics.in_flight.dec()
            self.metrics.requests.labels(
                route=route,
                outcome=self._outcome(response_status),
            ).inc()


def create_app(
    settings: TaskImageAuthoritySettings,
    *,
    verifier: TaskImagePrincipalVerifier | None = None,
    now_factory: Callable[[], datetime] | None = None,
    challenge_nonce_factory: Callable[[], UUID] | None = None,
    bootstrap_token_factory: Callable[[], str] | None = None,
    session_token_factory: Callable[[], str] | None = None,
) -> FastAPI:
    """Create the independent projection service; it owns no Slurm client."""

    resolved_verifier = verifier or TaskImagePrincipalVerifier.from_file(
        settings.principals_file
    )
    resolved_now = now_factory or (lambda: datetime.now(UTC))
    resolved_challenge_nonce = challenge_nonce_factory or uuid4
    resolved_bootstrap_token = bootstrap_token_factory or new_bootstrap_token
    resolved_session_token = session_token_factory or new_session_token
    metrics = TaskImageAuthorityMetrics()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.ready = False
        app.state.engine = None
        app.state.session_factory = None
        app.state.keyring = None
        try:
            database_url = read_owner_only_secret(settings.db_url_file)
            keyring = load_secret_store_keyring(settings.secret_store_keyring_file)
            engine = create_async_engine(database_url, isolation_level="SERIALIZABLE")
            app.state.engine = engine
            await assert_schema_at_head(
                engine,
                db_url_env_var="LOOM_TASK_IMAGE_AUTHORITY_DB_URL",
            )
            app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
            app.state.keyring = keyring
            app.state.ready = True
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
        title="Loom Task-Image Authority",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.add_middleware(RequestBodyLimitMiddleware, maximum_bytes=MAX_CONTRACT_BYTES)
    app.add_middleware(
        AuthorityTrafficLimitMiddleware,
        requests_per_second=settings.request_rate_limit_per_second,
        concurrency=settings.request_concurrency_limit,
    )
    app.add_middleware(AuthorityMetricsMiddleware, metrics=metrics)
    app.state.verifier = resolved_verifier
    app.state.metrics = metrics

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        del request, exc
        return JSONResponse(
            status_code=422,
            content={"detail": "invalid task-image authority contract"},
        )

    def ready(request: Request) -> None:
        if not getattr(request.app.state, "ready", False):
            raise HTTPException(
                status_code=503,
                detail="task-image authority not ready",
            )

    def principal(
        request: Request,
        authorization: str | None = Header(default=None),
    ) -> TaskImageGuardPrincipalV1:
        ready(request)
        try:
            return resolved_verifier.verify_bearer(authorization)
        except TaskImageAuthorityAuthorizationError:
            raise HTTPException(
                status_code=401,
                detail="invalid task-image authority credentials",
            ) from None

    def require(
        scope: GuardScope,
    ) -> Callable[[TaskImageGuardPrincipalV1], TaskImageGuardPrincipalV1]:
        def dependency(
            value: TaskImageGuardPrincipalV1 = Depends(principal),
        ) -> TaskImageGuardPrincipalV1:
            if scope not in value.scopes:
                raise HTTPException(
                    status_code=403,
                    detail="task-image authority forbidden",
                )
            return value

        return dependency

    def contract_body(
        model: type[_ContractT],
    ) -> Callable[[Request], Awaitable[_ContractT]]:
        async def dependency(request: Request) -> _ContractT:
            try:
                return model.model_validate_json(await request.body())
            except (ValidationError, ValueError):
                raise HTTPException(
                    status_code=422,
                    detail="invalid task-image authority contract",
                ) from None

        return dependency

    project_principal = require("task-image:project")
    attest_principal = require("task-image:attest")
    projection_body = contract_body(TaskImageProjectionRequestV1)
    attachment_body = contract_body(TaskImageAttachmentProofV1)
    exchange_body = contract_body(TaskImageBootstrapExchangeV1)
    attestation_body = contract_body(TaskImageContainmentAttestationV1)
    revocation_body = contract_body(TaskImageProjectionRevocationV1)

    async def transition(
        operation: Callable[
            [AsyncSession, LocalEncryptedSecretStore],
            Awaitable[_TransitionT],
        ],
    ) -> _TransitionT:
        if app.state.session_factory is None or app.state.keyring is None:
            raise HTTPException(status_code=503, detail="task-image authority not ready")
        factory = cast(async_sessionmaker[AsyncSession], app.state.session_factory)
        keyring = cast(TaskImageSecretStoreKeyring, app.state.keyring)
        async with factory() as session:
            secret_store = LocalEncryptedSecretStore(
                session,
                master_key=keyring.primary_key,
                master_key_version=keyring.primary_version,
                fallback_keys=dict(keyring.fallback_keys),
            )
            try:
                result = await operation(session, secret_store)
                await session.commit()
            except TaskImageProjectionEquivocationError:
                try:
                    await session.commit()
                except Exception:
                    await session.rollback()
                    raise HTTPException(
                        status_code=503,
                        detail="task-image authority unavailable",
                    ) from None
                raise HTTPException(
                    status_code=409,
                    detail="task-image authority conflict",
                ) from None
            except TaskImageProjectionConflictError:
                await session.rollback()
                raise HTTPException(
                    status_code=409,
                    detail="task-image authority conflict",
                ) from None
            except TaskImageProjectionAuthorizationError:
                await session.rollback()
                raise HTTPException(
                    status_code=403,
                    detail="task-image authority rejected",
                ) from None
            except Exception:
                await session.rollback()
                raise HTTPException(
                    status_code=503,
                    detail="task-image authority unavailable",
                ) from None
        return result

    @app.get("/healthz")
    async def healthz(request: Request) -> dict[str, str]:
        ready(request)
        return {"status": "ok"}

    @app.get("/metrics")
    async def prometheus_metrics() -> PlainTextResponse:
        return PlainTextResponse(
            metrics.render(),
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )

    @app.put("/v1/projections/{grant_id}/challenge")
    async def challenge(
        grant_id: UUID,
        guard: TaskImageGuardPrincipalV1 = Depends(project_principal),
        body: TaskImageProjectionRequestV1 = Depends(projection_body),
    ) -> JSONResponse:
        if grant_id != body.grant_id:
            raise HTTPException(status_code=409, detail="task-image authority conflict")
        result = await transition(
            lambda session, secret_store: request_task_image_projection(
                session,
                principal=guard,
                request=body,
                now=resolved_now(),
                challenge_nonce_factory=resolved_challenge_nonce,
            ),
        )
        return JSONResponse(content=jsonable_encoder(result))

    @app.put("/v1/projections/{grant_id}/attachment")
    async def attachment(
        grant_id: UUID,
        guard: TaskImageGuardPrincipalV1 = Depends(project_principal),
        body: TaskImageAttachmentProofV1 = Depends(attachment_body),
    ) -> JSONResponse:
        if grant_id != body.grant_id:
            raise HTTPException(status_code=409, detail="task-image authority conflict")
        result = await transition(
            lambda session, secret_store: complete_task_image_projection(
                session,
                principal=guard,
                proof=body,
                now=resolved_now(),
                secret_store=secret_store,
                bootstrap_token_factory=resolved_bootstrap_token,
            ),
        )
        return JSONResponse(content=jsonable_encoder(result))

    @app.put("/v1/projections/{grant_id}/exchange")
    async def exchange(
        grant_id: UUID,
        guard: TaskImageGuardPrincipalV1 = Depends(project_principal),
        body: TaskImageBootstrapExchangeV1 = Depends(exchange_body),
    ) -> JSONResponse:
        if grant_id != body.grant_id:
            raise HTTPException(status_code=409, detail="task-image authority conflict")
        result = await transition(
            lambda session, secret_store: exchange_task_image_bootstrap(
                session,
                principal=guard,
                request=body,
                now=resolved_now(),
                secret_store=secret_store,
                session_token_factory=resolved_session_token,
            ),
        )
        return JSONResponse(content=jsonable_encoder(result))

    @app.put("/v1/projections/{grant_id}/attestations/{generation}")
    async def attestation(
        grant_id: UUID,
        generation: int,
        guard: TaskImageGuardPrincipalV1 = Depends(attest_principal),
        body: TaskImageContainmentAttestationV1 = Depends(attestation_body),
    ) -> JSONResponse:
        if grant_id != body.grant_id or generation != body.generation:
            raise HTTPException(status_code=409, detail="task-image authority conflict")
        result = await transition(
            lambda session, secret_store: record_task_image_containment_attestation(
                session,
                principal=guard,
                attestation=body,
                now=resolved_now(),
            ),
        )
        return JSONResponse(content=jsonable_encoder(result))

    @app.put("/v1/projections/{grant_id}/revocation", status_code=204)
    async def revocation(
        grant_id: UUID,
        guard: TaskImageGuardPrincipalV1 = Depends(project_principal),
        body: TaskImageProjectionRevocationV1 = Depends(revocation_body),
    ) -> Response:
        if grant_id != body.grant_id:
            raise HTTPException(status_code=409, detail="task-image authority conflict")
        await transition(
            lambda session, secret_store: revoke_task_image_projection(
                session,
                principal=guard,
                request=body,
                now=resolved_now(),
            ),
        )
        return Response(status_code=204)

    return app


__all__ = [
    "AuthorityMetricsMiddleware",
    "AuthorityTrafficLimitMiddleware",
    "RequestBodyLimitMiddleware",
    "TaskImageAuthorityMetrics",
    "create_app",
]
