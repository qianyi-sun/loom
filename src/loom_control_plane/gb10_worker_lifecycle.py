"""GB10 Docker Compose worker lifecycle state helpers.

The Control Plane owns desired pool configuration and receives status reports
from node-local pull agents. It does not SSH into GB10 hosts.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import GB10WorkerNodeStatus, GB10WorkerPoolDesiredState, Worker

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
_CAPACITY_INTENTS = {"active", "draining", "stopped"}
_WORKER_FRESHNESS_SEC = 30


class UnsafeDesiredEnvError(ValueError):
    """Desired env contains data that looks like raw credential material."""


@dataclass(frozen=True)
class GB10NodeReport:
    current_image_tag: str | None = None
    current_max_concurrent: int | None = None
    current_env_config_version: str | None = None
    current_intent: str | None = None
    apply_state: str = "unknown"
    last_apply_result: str | None = None
    error_message: str | None = None
    agent_version: str | None = None
    compose_project_dir: str | None = None
    source_git_commit: str | None = None
    source_git_dirty: bool | None = None
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


def validate_host_intents(host_intents: dict[str, str] | None) -> dict[str, str]:
    if not host_intents:
        return {}
    out: dict[str, str] = {}
    for raw_host, raw_intent in host_intents.items():
        host = _clean_nonempty(str(raw_host), "host_intents hostname")
        intent = str(raw_intent).strip()
        if intent not in _CAPACITY_INTENTS:
            raise ValueError(
                "host_intents values must be one of: active, draining, stopped",
            )
        out[host] = intent
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


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _worker_is_fresh(row: Worker, *, now: datetime) -> bool:
    last_seen_at = _aware(row.last_seen_at)
    if last_seen_at is None:
        return False
    return row.status == "active" and last_seen_at >= now - timedelta(
        seconds=_WORKER_FRESHNESS_SEC,
    )


def _worker_backend_names(capabilities: object) -> list[str]:
    if not isinstance(capabilities, list):
        return []
    names: set[str] = set()
    for cap in capabilities:
        if not isinstance(cap, dict):
            continue
        backend = cap.get("backend", "docker")
        if isinstance(backend, str) and backend:
            names.add(backend)
    return sorted(names)


def desired_state_to_dict(row: GB10WorkerPoolDesiredState) -> dict[str, object]:
    return {
        "id": str(row.id),
        "environment": row.environment,
        "pool_name": row.pool_name,
        "image_tag": row.image_tag,
        "max_concurrent": row.max_concurrent,
        "env_config_version": row.env_config_version,
        "source_git_commit": row.source_git_commit,
        "target_slots": row.target_slots,
        "host_intents": row.host_intents,
        "rollout_policy": row.rollout_policy,
        "env": row.env,
        "force": row.force,
        "previous_image_tag": row.previous_image_tag,
        "previous_max_concurrent": row.previous_max_concurrent,
        "previous_env_config_version": row.previous_env_config_version,
        "previous_source_git_commit": row.previous_source_git_commit,
        "previous_env": row.previous_env,
        "created_at": _dt(_aware(row.created_at)),
        "updated_at": _dt(_aware(row.updated_at)),
    }


def node_status_to_dict(
    row: GB10WorkerNodeStatus,
    *,
    worker: Worker | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    now = now or datetime.now(UTC)
    worker_id = row.worker_id
    worker_status = None
    worker_last_seen_at = None
    worker_fresh = False
    worker_backend_names: list[str] = []
    worker_drain_state = None
    if worker is not None:
        worker_id = worker.id
        worker_status = worker.status
        worker_last_seen_at = _aware(worker.last_seen_at)
        worker_fresh = _worker_is_fresh(worker, now=now)
        worker_backend_names = _worker_backend_names(worker.capabilities)
        worker_drain_state = worker.drain_state
    return {
        "id": str(row.id),
        "environment": row.environment,
        "pool_name": row.pool_name,
        "hostname": row.hostname,
        "worker_id": str(worker_id) if worker_id is not None else None,
        "worker_status": worker_status,
        "worker_last_seen_at": _dt(worker_last_seen_at),
        "worker_fresh": worker_fresh,
        "worker_backend_names": worker_backend_names,
        "worker_drain_state": worker_drain_state,
        "current_image_tag": row.current_image_tag,
        "current_max_concurrent": row.current_max_concurrent,
        "current_env_config_version": row.current_env_config_version,
        "current_intent": row.current_intent,
        "desired_image_tag": row.desired_image_tag,
        "desired_max_concurrent": row.desired_max_concurrent,
        "desired_env_config_version": row.desired_env_config_version,
        "desired_source_git_commit": row.desired_source_git_commit,
        "desired_intent": row.desired_intent,
        "apply_state": row.apply_state,
        "last_apply_result": row.last_apply_result,
        "error_message": row.error_message,
        "agent_version": row.agent_version,
        "compose_project_dir": row.compose_project_dir,
        "source_git_commit": row.source_git_commit,
        "source_git_dirty": row.source_git_dirty,
        "last_heartbeat_at": _dt(_aware(row.last_heartbeat_at)),
        "last_apply_at": _dt(_aware(row.last_apply_at)),
        "created_at": _dt(_aware(row.created_at)),
        "updated_at": _dt(_aware(row.updated_at)),
    }


async def get_desired_state(
    session: AsyncSession,
    *,
    environment: str,
    pool_name: str,
) -> GB10WorkerPoolDesiredState | None:
    environment = _clean_nonempty(environment, "environment")
    pool_name = _clean_nonempty(pool_name, "pool_name")
    return (
        await session.execute(
            select(GB10WorkerPoolDesiredState).where(
                GB10WorkerPoolDesiredState.environment == environment,
                GB10WorkerPoolDesiredState.pool_name == pool_name,
            ),
        )
    ).scalar_one_or_none()


async def upsert_desired_state(
    session: AsyncSession,
    *,
    environment: str,
    pool_name: str,
    image_tag: str,
    max_concurrent: int,
    env_config_version: str,
    source_git_commit: str | None = None,
    target_slots: int | None = None,
    host_intents: dict[str, str] | None = None,
    rollout_policy: dict[str, Any] | None = None,
    env: dict[str, str] | None = None,
    force: bool = False,
    now: datetime | None = None,
) -> GB10WorkerPoolDesiredState:
    environment = _clean_nonempty(environment, "environment")
    pool_name = _clean_nonempty(pool_name, "pool_name")
    image_tag = _clean_nonempty(image_tag, "image_tag")
    env_config_version = _clean_nonempty(env_config_version, "env_config_version")
    cleaned_source_git_commit = (
        _clean_nonempty(source_git_commit, "source_git_commit")
        if source_git_commit is not None
        else None
    )
    if max_concurrent <= 0:
        raise ValueError("max_concurrent must be positive")
    if target_slots is not None and target_slots < 0:
        raise ValueError("target_slots must be >= 0")
    safe_env = validate_safe_env(env)
    safe_host_intents = validate_host_intents(host_intents)
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
            source_git_commit=cleaned_source_git_commit,
            target_slots=target_slots,
            host_intents=safe_host_intents,
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
            or row.source_git_commit != cleaned_source_git_commit
            or row.target_slots != target_slots
            or row.host_intents != safe_host_intents
            or row.env != safe_env
        )
        if changed:
            row.previous_image_tag = row.image_tag
            row.previous_max_concurrent = row.max_concurrent
            row.previous_env_config_version = row.env_config_version
            row.previous_source_git_commit = row.source_git_commit
            row.previous_env = dict(row.env or {})
        row.image_tag = image_tag
        row.max_concurrent = max_concurrent
        row.env_config_version = env_config_version
        row.source_git_commit = cleaned_source_git_commit
        row.target_slots = target_slots
        row.host_intents = safe_host_intents
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
    row = (
        await session.execute(
            select(GB10WorkerNodeStatus).where(
                GB10WorkerNodeStatus.environment == environment,
                GB10WorkerNodeStatus.pool_name == pool_name,
                GB10WorkerNodeStatus.hostname == hostname,
            ),
        )
    ).scalar_one_or_none()
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
    row.current_intent = report.current_intent
    if desired is not None:
        row.desired_image_tag = desired.image_tag
        row.desired_max_concurrent = desired.max_concurrent
        row.desired_env_config_version = desired.env_config_version
        row.desired_source_git_commit = desired.source_git_commit
        row.desired_intent = (desired.host_intents or {}).get(hostname, "active")
    row.apply_state = report.apply_state
    row.last_apply_result = redact_status_text(report.last_apply_result)
    row.error_message = redact_status_text(report.error_message)
    row.agent_version = report.agent_version
    row.compose_project_dir = report.compose_project_dir
    row.source_git_commit = report.source_git_commit
    row.source_git_dirty = report.source_git_dirty
    row.last_heartbeat_at = now
    if report.last_apply_at is not None:
        row.last_apply_at = report.last_apply_at
    elif report.apply_state in {"applied", "failed", "blocked", "rolled_back"}:
        row.last_apply_at = now
    row.updated_at = now

    await _reconcile_worker_drain_state_for_stopped_host(
        session,
        row,
        now=now,
    )

    await session.flush()
    return row


async def _reconcile_worker_drain_state_for_stopped_host(
    session: AsyncSession,
    row: GB10WorkerNodeStatus,
    *,
    now: datetime,
) -> None:
    """#368 defense: force the worker registry to reflect a stopped
    host intent, even if the node-agent's "applied" report was a no-op
    (env file already matched, container never actually stopped) and
    the worker keeps heartbeating.

    Without this reconciliation, the scheduler sees `Worker.drain_state
    = 'active'` and continues claiming trials on a host the lifecycle
    layer says is stopped. `loom resources status` also reports the
    host in `active_workers` / `total_slots`.

    Two triggers force the worker to `drained`:
    1. Desired + current + apply all report `stopped` → the node has
       affirmatively confirmed shutdown. Worker registry should match.
    2. Desired = `stopped` and the row has an attached `worker_id` →
       even if apply_state is `already current` (the #368 bug), the
       operator has expressed intent and the CP must not treat the
       host as active capacity.
    """
    if row.worker_id is None:
        return
    desired_intent = row.desired_intent
    if desired_intent != "stopped":
        return
    worker_row = await session.get(Worker, row.worker_id)
    if worker_row is None or worker_row.drain_state == "drained":
        return
    worker_row.drain_state = "drained"
    worker_row.drain_requested_at = worker_row.drain_requested_at or now
    worker_row.drain_reason = (
        f"gb10 host {row.hostname} desired_intent=stopped "
        f"(current_intent={row.current_intent or '-'}, "
        f"apply_state={row.apply_state})"
    )
    worker_row.drain_owner = "gb10-lifecycle"


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
    desired_rows = (
        (
            await session.execute(
                desired_stmt.order_by(
                    GB10WorkerPoolDesiredState.environment,
                    GB10WorkerPoolDesiredState.pool_name,
                ),
            )
        )
        .scalars()
        .all()
    )
    node_rows = (
        (
            await session.execute(
                node_stmt.order_by(
                    GB10WorkerNodeStatus.environment,
                    GB10WorkerNodeStatus.pool_name,
                    GB10WorkerNodeStatus.hostname,
                ),
            )
        )
        .scalars()
        .all()
    )
    now = datetime.now(UTC)
    worker_by_id, workers_by_node, worker_rows = await _fetch_matching_workers(
        session,
        node_rows=node_rows,
        pool_names={
            *(row.pool_name for row in desired_rows),
            *(row.pool_name for row in node_rows),
            *([pool_name] if pool_name else []),
        },
    )
    selected_workers = [
        _worker_for_node(
            row,
            worker_by_id=worker_by_id,
            workers_by_node=workers_by_node,
            now=now,
        )
        for row in node_rows
    ]
    linked_worker_ids = {worker.id for worker in selected_workers if worker is not None}
    return {
        "desired_states": [desired_state_to_dict(row) for row in desired_rows],
        "nodes": [
            node_status_to_dict(
                row,
                worker=worker,
                now=now,
            )
            for row, worker in zip(node_rows, selected_workers, strict=True)
        ],
        "unlinked_workers": [
            _unlinked_worker_to_dict(worker, now=now)
            for worker in worker_rows
            if worker.id not in linked_worker_ids
        ],
    }


async def _fetch_matching_workers(
    session: AsyncSession,
    *,
    node_rows: Sequence[GB10WorkerNodeStatus],
    pool_names: set[str],
) -> tuple[
    dict[UUID, Worker],
    dict[tuple[str, str], list[Worker]],
    list[Worker],
]:
    conditions: list[Any] = []
    if pool_names:
        conditions.append(Worker.pool_name.in_(sorted(pool_names)))
    for row in node_rows:
        if row.worker_id is not None:
            conditions.append(Worker.id == row.worker_id)
    if not conditions:
        return {}, {}, []
    worker_rows = (
        (
            await session.execute(
                select(Worker).where(or_(*conditions)),
            )
        )
        .scalars()
        .all()
    )
    worker_by_id = {row.id: row for row in worker_rows}
    workers_by_node: dict[tuple[str, str], list[Worker]] = defaultdict(list)
    for worker_row in worker_rows:
        workers_by_node[(worker_row.hostname, worker_row.pool_name)].append(
            worker_row,
        )
    return worker_by_id, dict(workers_by_node), list(worker_rows)


def _unlinked_worker_to_dict(worker: Worker, *, now: datetime) -> dict[str, object]:
    return {
        "worker_id": str(worker.id),
        "hostname": worker.hostname,
        "pool_name": worker.pool_name,
        "worker_status": worker.status,
        "worker_last_seen_at": _dt(_aware(worker.last_seen_at)),
        "worker_fresh": _worker_is_fresh(worker, now=now),
        "worker_backend_names": _worker_backend_names(worker.capabilities),
        "worker_drain_state": worker.drain_state,
        "max_concurrent": worker.max_concurrent,
    }


def _worker_for_node(
    row: GB10WorkerNodeStatus,
    *,
    worker_by_id: dict[UUID, Worker],
    workers_by_node: dict[tuple[str, str], list[Worker]],
    now: datetime,
) -> Worker | None:
    if row.worker_id is not None:
        worker = worker_by_id.get(row.worker_id)
        if (
            worker is not None
            and worker.hostname == row.hostname
            and worker.pool_name == row.pool_name
        ):
            return worker
    candidates = workers_by_node.get((row.hostname, row.pool_name), [])
    if not candidates:
        return None

    def _rank(worker: Worker) -> tuple[int, int, datetime, datetime]:
        last_seen_at = _aware(worker.last_seen_at) or datetime.min.replace(tzinfo=UTC)
        registered_at = _aware(worker.registered_at) or datetime.min.replace(tzinfo=UTC)
        return (
            1 if _worker_is_fresh(worker, now=now) else 0,
            1 if worker.status == "active" else 0,
            last_seen_at,
            registered_at,
        )

    return max(candidates, key=_rank)
