from __future__ import annotations

from http import HTTPStatus
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy import Engine
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware

from agentic_data_platform.persistence import create_database_engine
from agentic_data_platform.service.benchmark_resources import register_benchmark_routes
from agentic_data_platform.service.config import ServiceSettings, load_service_settings
from agentic_data_platform.service.dependencies import build_session_dependency
from agentic_data_platform.service.project_resources import register_project_routes
from agentic_data_platform.service.run_resources import register_run_routes


REQUEST_ID_HEADER = "X-Request-ID"


def create_app(
    settings: ServiceSettings | None = None,
    *,
    database_engine: Engine | None = None,
) -> FastAPI:
    service_settings = settings or load_service_settings()
    engine = database_engine
    if engine is None and service_settings.database_url:
        engine = create_database_engine(service_settings.database_url, pool_pre_ping=True)

    app = FastAPI(
        title="Agentic Data Platform API",
        version="0.0.0",
        description="Internal backend API for agentic data generation and evaluation runs.",
    )
    app.state.settings = service_settings
    app.state.database_engine = engine
    app.state.session_dependency = build_session_dependency(engine)
    app.add_middleware(RequestIDMiddleware)
    register_project_routes(app, app.state.session_dependency)
    register_benchmark_routes(app, app.state.session_dependency)
    register_run_routes(app, app.state.session_dependency)

    @app.get("/healthz", tags=["operations"])
    def healthz(request: Request) -> dict[str, str]:
        return {
            "status": "ok",
            "service": service_settings.app_name,
            "environment": service_settings.environment,
            "request_id": request.state.request_id,
        }

    @app.get("/readyz", tags=["operations"])
    def readyz(request: Request) -> JSONResponse:
        dependencies = {
            "database": _configured(engine),
            "redis": _configured(service_settings.redis_url),
            "object_storage": _configured(
                service_settings.object_storage_endpoint
                and service_settings.object_storage_bucket
                and service_settings.object_storage_access_key
                and service_settings.object_storage_secret_key
            ),
        }
        is_ready = all(status == "configured" for status in dependencies.values())
        return JSONResponse(
            status_code=200 if is_ready else 503,
            content={
                "status": "ready" if is_ready else "not_ready",
                "dependencies": dependencies,
                "request_id": request.state.request_id,
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        return _error_response(
            request=request,
            status_code=exc.status_code,
            code=_http_error_code(exc.status_code),
            message=str(exc.detail),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        return _error_response(
            request=request,
            status_code=422,
            code="validation_error",
            message=str(exc),
        )

    return app


class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get(REQUEST_ID_HEADER) or str(uuid4())
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers[REQUEST_ID_HEADER] = request_id
        return response


def _configured(value: object) -> str:
    return "configured" if bool(value) else "missing"


def _error_response(*, request: Request, status_code: int, code: str, message: str) -> JSONResponse:
    request_id = getattr(request.state, "request_id", "")
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": code,
                "message": message,
                "request_id": request_id,
            }
        },
        headers={REQUEST_ID_HEADER: request_id} if request_id else None,
    )


def _http_error_code(status_code: int) -> str:
    try:
        phrase = HTTPStatus(status_code).phrase.lower().replace(" ", "_")
    except ValueError:
        return "http_error"
    return phrase


app = create_app()
