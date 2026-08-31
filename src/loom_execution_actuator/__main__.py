from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

import uvicorn
from fastapi import FastAPI, Response, status
from prometheus_client import make_asgi_app
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema_startup import assert_schema_at_head
from loom_execution_actuator.config import ExecutionActuatorSettings
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.kubernetes_api import InClusterKubernetesJobApi
from loom_execution_actuator.renderer import ExecutionTargetRuntime

_LOG = logging.getLogger(__name__)


@dataclass
class ActuatorRuntimeHealth:
    stale_after_seconds: float
    started_at: float = field(default_factory=time.monotonic)
    last_command_success: float | None = None
    last_reconcile_success: float | None = None

    def mark_success(self, loop: str) -> None:
        if loop == "command":
            self.last_command_success = time.monotonic()
        elif loop == "reconcile":
            self.last_reconcile_success = time.monotonic()

    def ready(self) -> bool:
        now = time.monotonic()
        return all(
            observed is not None and now - observed <= self.stale_after_seconds
            for observed in (self.last_command_success, self.last_reconcile_success)
        )


def _health_app(runtime_health: ActuatorRuntimeHealth) -> FastAPI:
    app = FastAPI(title="Loom Execution Actuator", docs_url=None, redoc_url=None)

    @app.get("/healthz")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def ready(response: Response) -> dict[str, str]:
        if not runtime_health.ready():
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return {"status": "not_ready"}
        return {"status": "ready"}

    app.mount("/metrics", make_asgi_app())
    return app


async def _command_loop(
    actuator: ExecutionActuator,
    poll_seconds: float,
    runtime_health: ActuatorRuntimeHealth,
) -> None:
    while True:
        try:
            await actuator.run_commands_once()
            runtime_health.mark_success("command")
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOG.exception("execution actuator command loop iteration failed")
        await asyncio.sleep(poll_seconds)


async def _reconcile_loop(
    actuator: ExecutionActuator,
    interval_seconds: float,
    runtime_health: ActuatorRuntimeHealth,
) -> None:
    while True:
        started = datetime.now(UTC)
        try:
            await actuator.reconcile_full_once(now=started)
            runtime_health.mark_success("reconcile")
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOG.exception("execution actuator full reconciliation failed")
        elapsed = (datetime.now(UTC) - started).total_seconds()
        await asyncio.sleep(max(0.25, interval_seconds - elapsed))


async def _watch_loop(actuator: ExecutionActuator, timeout_seconds: int) -> None:
    retry_seconds = 0.25
    while True:
        try:
            await actuator.watch_once(timeout_seconds=timeout_seconds)
            retry_seconds = 0.25
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOG.exception("execution actuator watch iteration failed")
            await asyncio.sleep(retry_seconds)
            retry_seconds = min(30.0, retry_seconds * 2)
            continue
        await asyncio.sleep(0.25)


async def _run() -> None:
    settings = ExecutionActuatorSettings()
    engine = create_async_engine(settings.db_url, pool_pre_ping=True)
    await assert_schema_at_head(engine, db_url_env_var="LOOM_EXECUTION_ACTUATOR_DB_URL")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    actuator = ExecutionActuator(
        sessions=sessions,
        kubernetes=InClusterKubernetesJobApi(),
        target=ExecutionTargetRuntime(
            target_id=settings.target_id,
            namespace=settings.namespace,
            runtime_class_name=settings.runtime_class_name,
            node_selector=settings.node_selector,
            tolerations=settings.tolerations,
            service_account_name=settings.service_account_name,
            credential_broker_url=settings.credential_broker_url,
        ),
        controller_id=settings.controller_id,
        command_limit=settings.command_limit,
        command_lease_seconds=settings.command_lease_seconds,
        delete_grace_seconds=settings.delete_grace_seconds,
    )
    runtime_health = ActuatorRuntimeHealth(
        stale_after_seconds=max(15.0, settings.full_reconcile_seconds * 3)
    )
    server = uvicorn.Server(
        uvicorn.Config(
            _health_app(runtime_health),
            host=settings.health_host,
            port=settings.health_port,
            log_level="info",
        )
    )
    tasks = (
        asyncio.create_task(server.serve()),
        asyncio.create_task(_command_loop(actuator, settings.poll_seconds, runtime_health)),
        asyncio.create_task(
            _reconcile_loop(actuator, settings.full_reconcile_seconds, runtime_health)
        ),
        asyncio.create_task(_watch_loop(actuator, settings.watch_timeout_seconds)),
    )
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await engine.dispose()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
