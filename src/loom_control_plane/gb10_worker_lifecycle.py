"""GB10 Docker Compose worker lifecycle state helpers.

The Control Plane owns desired pool configuration and receives status reports
from node-local pull agents. It does not SSH into GB10 hosts.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import GB10WorkerNodeStatus, GB10WorkerPoolDesiredState

_SECRET_KEY_PARTS = (
    "TOKEN",
    "SECRET",
    "PASSWORD",
    "PASS",
    "API_KEY",
    "CREDENTIAL",
)
_SECRET_VALUE_PREFIXES = (
    "loom_w_",
    "loom_admin_",
    "sk-",
)
_SECRET_VALUE_PATTERNS = (
    re.compile(r"loom_(?:w|admin|br)_[A-Za-z0-9._~+/=-]+"),
    re.compile(r"sk-[A-Za-z0-9._~+/=-]+"),
    re.compile(
        r"(?i)\b(token|secret|password|credential|api[_-]?key)=\S+",
    ),
)


class UnsafeDesiredEnvError(ValueError):
    """Desired env contains data that looks like raw credential material."""


@dataclass(frozen=True)
class GB10NodeReport:
    current_image_tag: str | None = None
    current_max_concurrent: int | None = None
    current_env_config_version: str | None = None
    apply_state: str = "unknown"
    last_apply_result: str | None = None
    error_message: str | None = None
    agent_version: str | None = None
    compose_project_dir: str | None = None
    worker_id: UUID | None = None
    last_apply_at: datetime | None = None


def _clean_nonempty(value: str, field: str) -> str:
    cleaned = str(value).strip()
    if not cleaned:
        raise ValueError(f"{field} must be a non-empty string")
    return cleaned


def validate_safe_env(env: dict[str, str] | None) -> dict[str, str]:
    if not env:
        return {}
    out: dict[str, str] = {}
    for raw_key, raw_value in env.items():
        key = _clean_nonempty(str(raw_key), "env key")
        value = str(raw_value)
        upper_key = key.upper()
        if any(part in upper_key for part in _SECRET_KEY_PARTS):
            raise UnsafeDesiredEnvError(
                f"desired env key {key!r} is secret-looking; use host-local "
                "env files for credentials",
            )
        stripped = value.strip()
        if any(stripped.startswith(prefix) for prefix in _SECRET_VALUE_PREFIXES):
            raise UnsafeDesiredEnvError(
                f"desired env value for {key!r} is secret-looking; use a "
                "host-local env file or secret reference",
            )
        out[key] = value
    return out


def _redact_status_match(match: re.Match[str]) -> str:
    if match.lastindex:
        return f"{match.group(1)}=<redacted>"
    return "<redacted>"


def redact_status_text(value: str | None) -> str | None:
    if value is None:
        return None
    redacted = value
    for pattern in _SECRET_VALUE_PATTERNS:
        redacted = pattern.sub(_redact_status_match, redacted)
    return redacted


def _dt(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def desired_state_to_dict(row: GB10WorkerPoolDesiredState) -> dict[str, object]:
    return {
        "id": str(row.id),
        "environment": row.environment,
        "pool_name": row.pool_name,
        "image_tag": row.image_tag,
        "max_concurrent": row.max_concurrent,
        "env_config_version": row.env_config_version,
        "rollout_policy": row.rollout_policy,
        "env": row.env,
        "force": row.force,
        "previous_image_tag": row.previous_image_tag,
        "previous_max_concurrent": row.previous_max_concurrent,
        "previous_env_config_version": row.previous_env_config_version,
        "previous_env": row.previous_env,
        "created_at": _dt(row.created_at),
        "updated_at": _dt(row.updated_at),
    }


def node_status_to_dict(row: GB10WorkerNodeStatus) -> dict[str, object]:
    return {
        "id": str(row.id),
        "environment": row.environment,
        "pool_name": row.pool_name,
        "hostname": row.hostname,
        "worker_id": str(row.worker_id) if row.worker_id is not None else None,
        "current_image_tag": row.current_image_tag,
        "current_max_concurrent": row.current_max_concurrent,
        "current_env_config_version": row.current_env_config_version,
        "desired_image_tag": row.desired_image_tag,
        "desired_max_concurrent": row.desired_max_concurrent,
        "desired_env_config_version": row.desired_env_config_version,
        "apply_state": row.apply_state,
        "last_apply_result": row.last_apply_result,
        "error_message": row.error_message,
        "agent_version": row.agent_version,
        "compose_project_dir": row.compose_project_dir,
        "last_heartbeat_at": _dt(row.last_heartbeat_at),
        "last_apply_at": _dt(row.last_apply_at),
        "created_at": _dt(row.created_at),
        "updated_at": _dt(row.updated_at),
    }


async def get_desired_state(
    session: AsyncSession,
    *,
    environment: str,
    pool_name: str,
) -> GB10WorkerPoolDesiredState | None:
    environment = _clean_nonempty(environment, "environment")
    pool_name = _clean_nonempty(pool_name, "pool_name")
    return (await session.execute(
        select(GB10WorkerPoolDesiredState).where(
            GB10WorkerPoolDesiredState.environment == environment,
            GB10WorkerPoolDesiredState.pool_name == pool_name,
        ),
    )).scalar_one_or_none()


async def upsert_desired_state(
    session: AsyncSession,
    *,
    environment: str,
    pool_name: str,
    image_tag: str,
    max_concurrent: int,
    env_config_version: str,
    rollout_policy: dict[str, Any] | None = None,
    env: dict[str, str] | None = None,
    force: bool = False,
    now: datetime | None = None,
) -> GB10WorkerPoolDesiredState:
    environment = _clean_nonempty(environment, "environment")
    pool_name = _clean_nonempty(pool_name, "pool_name")
    image_tag = _clean_nonempty(image_tag, "image_tag")
    env_config_version = _clean_nonempty(env_config_version, "env_config_version")
    if max_concurrent <= 0:
        raise ValueError("max_concurrent must be positive")
    safe_env = validate_safe_env(env)
    policy = dict(rollout_policy or {})
    now = now or datetime.now(UTC)
    row = await get_desired_state(
        session,
        environment=environment,
        pool_name=pool_name,
    )
    if row is None:
        row = GB10WorkerPoolDesiredState(
            environment=environment,
            pool_name=pool_name,
            image_tag=image_tag,
            max_concurrent=max_concurrent,
            env_config_version=env_config_version,
            rollout_policy=policy,
            env=safe_env,
            force=bool(force),
            updated_at=now,
        )
        session.add(row)
    else:
        changed = (
            row.image_tag != image_tag
            or row.max_concurrent != max_concurrent
            or row.env_config_version != env_config_version
            or row.env != safe_env
        )
        if changed:
            row.previous_image_tag = row.image_tag
            row.previous_max_concurrent = row.max_concurrent
            row.previous_env_config_version = row.env_config_version
            row.previous_env = dict(row.env or {})
        row.image_tag = image_tag
        row.max_concurrent = max_concurrent
        row.env_config_version = env_config_version
        row.rollout_policy = policy
        row.env = safe_env
        row.force = bool(force)
        row.updated_at = now
    await session.flush()
    return row


async def record_node_report(
    session: AsyncSession,
    *,
    environment: str,
    pool_name: str,
    hostname: str,
    report: GB10NodeReport,
    now: datetime | None = None,
) -> GB10WorkerNodeStatus:
    environment = _clean_nonempty(environment, "environment")
    pool_name = _clean_nonempty(pool_name, "pool_name")
    hostname = _clean_nonempty(hostname, "hostname")
    now = now or datetime.now(UTC)
    desired = await get_desired_state(
        session,
        environment=environment,
        pool_name=pool_name,
    )
    row = (await session.execute(
        select(GB10WorkerNodeStatus).where(
            GB10WorkerNodeStatus.environment == environment,
            GB10WorkerNodeStatus.pool_name == pool_name,
            GB10WorkerNodeStatus.hostname == hostname,
        ),
    )).scalar_one_or_none()
    if row is None:
        row = GB10WorkerNodeStatus(
            environment=environment,
            pool_name=pool_name,
            hostname=hostname,
        )
        session.add(row)

    row.worker_id = report.worker_id
    row.current_image_tag = report.current_image_tag
    row.current_max_concurrent = report.current_max_concurrent
    row.current_env_config_version = report.current_env_config_version
    if desired is not None:
        row.desired_image_tag = desired.image_tag
        row.desired_max_concurrent = desired.max_concurrent
        row.desired_env_config_version = desired.env_config_version
    row.apply_state = report.apply_state
    row.last_apply_result = redact_status_text(report.last_apply_result)
    row.error_message = redact_status_text(report.error_message)
    row.agent_version = report.agent_version
    row.compose_project_dir = report.compose_project_dir
    row.last_heartbeat_at = now
    if report.last_apply_at is not None:
        row.last_apply_at = report.last_apply_at
    elif report.apply_state in {"applied", "failed", "blocked", "rolled_back"}:
        row.last_apply_at = now
    row.updated_at = now
    await session.flush()
    return row


async def fetch_lifecycle_status(
    session: AsyncSession,
    *,
    environment: str | None = None,
    pool_name: str | None = None,
) -> dict[str, list[dict[str, object]]]:
    desired_stmt = select(GB10WorkerPoolDesiredState)
    node_stmt = select(GB10WorkerNodeStatus)
    if environment:
        desired_stmt = desired_stmt.where(
            GB10WorkerPoolDesiredState.environment == environment,
        )
        node_stmt = node_stmt.where(GB10WorkerNodeStatus.environment == environment)
    if pool_name:
        desired_stmt = desired_stmt.where(
            GB10WorkerPoolDesiredState.pool_name == pool_name,
        )
        node_stmt = node_stmt.where(GB10WorkerNodeStatus.pool_name == pool_name)
    desired_rows = (await session.execute(
        desired_stmt.order_by(
            GB10WorkerPoolDesiredState.environment,
            GB10WorkerPoolDesiredState.pool_name,
        ),
    )).scalars().all()
    node_rows = (await session.execute(
        node_stmt.order_by(
            GB10WorkerNodeStatus.environment,
            GB10WorkerNodeStatus.pool_name,
            GB10WorkerNodeStatus.hostname,
        ),
    )).scalars().all()
    return {
        "desired_states": [desired_state_to_dict(row) for row in desired_rows],
        "nodes": [node_status_to_dict(row) for row in node_rows],
    }
