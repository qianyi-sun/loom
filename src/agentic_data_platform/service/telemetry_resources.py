from __future__ import annotations

import os
import shutil
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import Depends, FastAPI, HTTPException, Request
from sqlalchemy.orm import Session

from agentic_data_platform.domain.run_records import RunRecord, RunStatus
from agentic_data_platform.persistence.repositories import RunRepository
from agentic_data_platform.service.security import accessible_project_ids, require_authenticated_user, require_project_role


def register_telemetry_routes(app: FastAPI, session_dependency: Callable) -> None:
    @app.get("/runs/{run_id}/telemetry", tags=["telemetry"], responses=_example_response(_RUN_TELEMETRY_EXAMPLE))
    def get_run_telemetry(
        run_id: str,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        auth = require_authenticated_user(request, session)
        try:
            run = RunRepository(session).get_run(run_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Unknown run: {run_id}") from exc
        require_project_role(session, auth, run.project_id, minimum_role="viewer")
        visible_runs = [
            candidate
            for candidate in RunRepository(session).list_runs()
            if candidate.project_id in accessible_project_ids(session, auth)
        ]
        return _with_request_id(
            request,
            {
                "run": _run_telemetry(run),
                "worker": _worker_telemetry(run, visible_runs),
                "host": _host_telemetry(),
                "sandbox": _sandbox_telemetry(run),
                "checked_at": _now(),
            },
        )


def _run_telemetry(run: RunRecord) -> dict[str, Any]:
    return {
        "run_id": run.run_id,
        "project_id": run.project_id,
        "status": run.status.value,
        "updated_at": _datetime(run.updated_at),
        "turn_count": len(run.trajectory),
        "artifact_count": len(run.artifacts),
        "current_attempt": 1,
        "failure_reason": run.failure_reason,
        "last_error": run.failure_reason,
    }


def _worker_telemetry(run: RunRecord, visible_runs: list[RunRecord]) -> dict[str, Any]:
    counts = Counter(candidate.status.value for candidate in visible_runs)
    return {
        "worker_id": "worker-dev-1",
        "state": _worker_state(run.status),
        "queue_depth": counts.get(RunStatus.QUEUED.value, 0),
        "running_count": counts.get(RunStatus.RUNNING.value, 0),
        "evaluating_count": counts.get(RunStatus.EVALUATING.value, 0),
        "last_heartbeat_at": _datetime(run.updated_at),
    }


def _host_telemetry() -> dict[str, Any]:
    cpu = _cpu_telemetry()
    memory = _memory_telemetry()
    disk = _disk_telemetry()
    return {
        "cpu": cpu,
        "memory": memory,
        "disk": disk,
        "saturation": {
            "cpu": bool(cpu.get("load_per_cpu") is not None and cpu["load_per_cpu"] >= 0.85),
            "memory": bool(memory.get("used_ratio") is not None and memory["used_ratio"] >= 0.9),
            "disk": disk["used_ratio"] >= 0.9,
        },
    }


def _sandbox_telemetry(run: RunRecord) -> dict[str, Any]:
    return {
        "sandbox_backend": run.runner.sandbox_backend.value,
        "container_id": run.metadata.get("container_id"),
        "image": run.runner.image,
        "status": _sandbox_status(run.status),
        "exit_code": _last_exit_code(run),
        "internet_access": run.runner.internet_access,
        "resource_limits": dict(run.runner.resource_limits),
        "last_error": run.failure_reason,
    }


def _cpu_telemetry() -> dict[str, Any]:
    cpu_count = os.cpu_count() or 1
    try:
        load_1m, load_5m, load_15m = os.getloadavg()
    except OSError:
        return {"cpu_count": cpu_count, "load_average": None, "load_per_cpu": None}
    return {
        "cpu_count": cpu_count,
        "load_average": {"1m": round(load_1m, 4), "5m": round(load_5m, 4), "15m": round(load_15m, 4)},
        "load_per_cpu": round(load_1m / cpu_count, 4),
    }


def _memory_telemetry() -> dict[str, Any]:
    page_size = _sysconf("SC_PAGE_SIZE")
    total_pages = _sysconf("SC_PHYS_PAGES")
    available_pages = _sysconf("SC_AVPHYS_PAGES")
    if page_size is None or total_pages is None or available_pages is None:
        return {"total_bytes": None, "available_bytes": None, "used_ratio": None}
    total_bytes = page_size * total_pages
    available_bytes = page_size * available_pages
    used_ratio = 1 - (available_bytes / total_bytes) if total_bytes else None
    return {
        "total_bytes": total_bytes,
        "available_bytes": available_bytes,
        "used_ratio": round(used_ratio, 4) if used_ratio is not None else None,
    }


def _disk_telemetry() -> dict[str, Any]:
    usage = shutil.disk_usage(".")
    return {
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "used_ratio": round(usage.used / usage.total, 4),
    }


def _sysconf(name: str) -> int | None:
    try:
        return int(os.sysconf(name))
    except (AttributeError, ValueError, OSError):
        return None


def _worker_state(status: RunStatus) -> str:
    if status in {RunStatus.QUEUED, RunStatus.PROVISIONING}:
        return "waiting"
    if status in {RunStatus.RUNNING, RunStatus.EVALUATING}:
        return "active"
    return "idle"


def _sandbox_status(status: RunStatus) -> str:
    if status in {RunStatus.QUEUED, RunStatus.PROVISIONING}:
        return "pending"
    if status is RunStatus.RUNNING:
        return "running"
    if status is RunStatus.FAILED:
        return "failed"
    if status is RunStatus.CANCELED:
        return "canceled"
    return "exited"


def _last_exit_code(run: RunRecord) -> int | None:
    if not run.trajectory:
        return None
    return run.trajectory[-1].exit_code


def _with_request_id(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        payload["request_id"] = request_id
    return payload


def _datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _now() -> str:
    return _datetime(datetime.now(timezone.utc))


def _example_response(example: dict[str, Any]) -> dict[int, dict[str, Any]]:
    return {200: {"content": {"application/json": {"example": example}}}}


_RUN_TELEMETRY_EXAMPLE = {
    "run": {
        "run_id": "run_001",
        "project_id": "latent-skill-pilot",
        "status": "running",
        "updated_at": "2026-05-29T12:00:00Z",
        "turn_count": 2,
        "artifact_count": 0,
        "current_attempt": 1,
        "failure_reason": None,
        "last_error": None,
    },
    "worker": {
        "worker_id": "worker-dev-1",
        "state": "active",
        "queue_depth": 1,
        "running_count": 1,
        "evaluating_count": 0,
        "last_heartbeat_at": "2026-05-29T12:00:00Z",
    },
    "host": {
        "cpu": {"cpu_count": 8, "load_average": {"1m": 1.0, "5m": 1.0, "15m": 1.0}, "load_per_cpu": 0.125},
        "memory": {"total_bytes": 8589934592, "available_bytes": 4294967296, "used_ratio": 0.5},
        "disk": {"total_bytes": 100, "used_bytes": 50, "free_bytes": 50, "used_ratio": 0.5},
        "saturation": {"cpu": False, "memory": False, "disk": False},
    },
    "sandbox": {
        "sandbox_backend": "docker_terminal",
        "container_id": None,
        "image": "python:3.12-slim",
        "status": "running",
        "exit_code": None,
        "internet_access": True,
        "resource_limits": {"cpu": 1, "memory_mb": 512, "timeout_seconds": 60},
        "last_error": None,
    },
    "checked_at": "2026-05-29T12:00:00Z",
    "request_id": "req_123",
}
