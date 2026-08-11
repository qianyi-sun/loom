"""`loom worker` — worker-host-local operator commands.

Available commands:
- ``loom worker cache stats`` — inspect the trial-cache layered images
  on the worker's local Docker daemon (label=loom.trial-cache=true).
- ``loom worker setup status`` — inspect setup-health admission and
  Loom-labeled setup/trial containers on the worker host.

Run these commands on the worker host where the Docker daemon is.
Fleet-wide aggregation is not provided; operators run the commands
per host through SSH or their existing orchestration.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import sys
from collections.abc import Callable
from typing import Any

from loom_worker.setup_admission import (
    NodeHealthPolicy,
    NodeHealthSnapshot,
    read_node_health_snapshot,
)


def _human_age(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        return f"{int(seconds / 60)}m"
    if seconds < 86_400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86_400:.1f}d"


def _human_size(num_bytes: int) -> str:
    n = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024 or unit == "TiB":
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TiB"  # unreachable, satisfies type checker


def _parse_docker_created(created: str) -> _dt.datetime:
    """Docker's Created timestamp is an ISO-8601 with nanosecond
    precision sometimes (`...Z` or `+00:00`). Strip subsecond past
    microseconds since fromisoformat() rejects nanoseconds."""
    # Trim trailing 'Z'
    s = created.rstrip("Z")
    # Truncate fractional seconds to 6 digits if longer
    if "." in s:
        head, frac = s.split(".", 1)
        # frac may contain a trailing offset (+00:00); split it off
        offset = ""
        for sign in ("+", "-"):
            if sign in frac:
                idx = frac.index(sign)
                offset = frac[idx:]
                frac = frac[:idx]
                break
        s = f"{head}.{frac[:6]}{offset}"
    if not (s.endswith("+00:00") or "+" in s or "-" in s[10:]):
        s = s + "+00:00"
    return _dt.datetime.fromisoformat(s)


def _gather_cache_images() -> list[dict[str, Any]]:
    """Return one dict per cached layered image. Empty list if no
    Docker daemon is reachable (returns gracefully — operator sees
    an empty table rather than a stack trace)."""
    import docker
    from docker.errors import DockerException

    try:
        client = docker.from_env()
    except DockerException as exc:
        raise RuntimeError(f"cannot reach Docker daemon: {exc}") from exc

    images = client.images.list(
        filters={"label": "loom.trial-cache=true"},
    )
    now = _dt.datetime.now(_dt.UTC)
    rows: list[dict[str, Any]] = []
    for img in images:
        attrs = img.attrs
        created_raw = attrs.get("Created", "")
        try:
            created_dt = _parse_docker_created(created_raw)
            age_sec = (now - created_dt).total_seconds()
        except (ValueError, TypeError):
            age_sec = 0.0
        size_bytes = int(attrs.get("Size", 0))
        labels = attrs.get("Config", {}).get("Labels", {}) or {}
        cache_key = labels.get("loom.cache-key", "")
        tags = img.tags or []
        rows.append({
            "cache_key": cache_key,
            "tags": tags,
            "size_bytes": size_bytes,
            "age_sec": age_sec,
            "created": created_raw,
        })
    # Newest first
    rows.sort(key=lambda r: r["age_sec"])
    return rows


def _setup_policy_from_env() -> NodeHealthPolicy:
    return NodeHealthPolicy(
        enabled=_env_bool("LOOM_WORKER_SETUP_HEALTH_GUARD_ENABLED", True),
        io_full_avg10_max=_env_float(
            "LOOM_WORKER_SETUP_HEALTH_IO_FULL_AVG10_MAX",
            50.0,
        ),
        min_swap_free_mb=_env_int(
            "LOOM_WORKER_SETUP_HEALTH_MIN_SWAP_FREE_MB",
            1024,
        ),
        d_state_process_max=_env_int("LOOM_WORKER_SETUP_HEALTH_DSTATE_MAX", 32),
        wait_timeout_sec=_env_float(
            "LOOM_WORKER_SETUP_HEALTH_WAIT_TIMEOUT_SEC",
            300.0,
        ),
        poll_interval_sec=_env_float(
            "LOOM_WORKER_SETUP_HEALTH_POLL_INTERVAL_SEC",
            5.0,
        ),
    )


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _gather_setup_status(
    *,
    read_snapshot: Callable[[], NodeHealthSnapshot] | None = None,
) -> dict[str, Any]:
    import docker
    from docker.errors import DockerException

    policy = _setup_policy_from_env()
    decision = policy.evaluate((read_snapshot or read_node_health_snapshot)())
    try:
        client = docker.from_env()
    except DockerException as exc:
        raise RuntimeError(f"cannot reach Docker daemon: {exc}") from exc

    containers_by_id: dict[str, Any] = {}
    for label in ("loom.setup-container=true", "loom.trial-container=true"):
        for container in client.containers.list(
            all=True,
            filters={"label": label},
        ):
            container_id = str(getattr(container, "id", "") or id(container))
            containers_by_id[container_id] = container
    containers = list(containers_by_id.values())
    rows = [_setup_container_row(container) for container in containers]
    rows.sort(key=lambda row: (row["kind"], row["name"]))
    return {
        "health": {
            "ok": decision.ok,
            "reason": decision.reason,
            "detail": decision.detail,
        },
        "containers": rows,
    }


def _setup_container_row(container: Any) -> dict[str, str]:
    labels = getattr(container, "attrs", {}).get("Config", {}).get("Labels", {}) or {}
    is_setup = labels.get("loom.setup-container") == "true"
    kind = "setup-sidecar" if is_setup and labels.get("loom.task-sidecar") else "setup"
    if not is_setup and labels.get("loom.trial-container") == "true":
        kind = "trial"
    detail = labels.get("loom.task_sidecar") or ""
    container_id = str(getattr(container, "id", "") or "")
    return {
        "id": container_id[:12],
        "name": str(getattr(container, "name", "") or ""),
        "status": str(getattr(container, "status", "") or ""),
        "kind": kind,
        "trial_id": str(labels.get("loom.trial_id") or ""),
        "task_id": str(labels.get("loom.task_id") or ""),
        "detail": str(detail),
    }


def _cache_stats_handler(args: argparse.Namespace) -> int:
    try:
        rows = _gather_cache_images()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(rows, indent=2))
        return 0

    if not rows:
        print("no trial-cache layered images on this host.")
        return 0

    total_bytes = sum(r["size_bytes"] for r in rows)
    print(
        f"{len(rows)} cached image(s) "
        f"({_human_size(total_bytes)} total)\n",
    )
    print(f"{'CACHE_KEY':<34}  {'AGE':>6}  {'SIZE':>10}  TAG")
    for r in rows:
        key = r["cache_key"] or "(unlabeled)"
        tag = r["tags"][0] if r["tags"] else "(none)"
        print(
            f"{key:<34}  {_human_age(r['age_sec']):>6}  "
            f"{_human_size(r['size_bytes']):>10}  {tag}",
        )
    return 0


def _setup_status_handler(args: argparse.Namespace) -> int:
    try:
        status = _gather_setup_status()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(status, indent=2))
        return 0

    health = status["health"]
    print(
        "setup health: "
        f"{'ok' if health['ok'] else 'blocked'} "
        f"{health['reason']} ({health['detail']})",
    )
    rows = status["containers"]
    if not rows:
        print("no Loom setup/trial containers on this host.")
        return 0

    print(f"\n{'KIND':<14} {'STATUS':<12} {'NAME':<36} {'TRIAL_ID':<36} DETAIL")
    for row in rows:
        print(
            f"{row['kind']:<14} {row['status']:<12} {row['name']:<36} "
            f"{row['trial_id']:<36} {row['detail']}",
        )
    return 0


def dispatch(argv: list[str]) -> int:
    if argv and argv[0] == "gb10-agent":
        from loom_cli.gb10_agent import dispatch as gb10_agent_dispatch

        return gb10_agent_dispatch(argv[1:])

    parser = argparse.ArgumentParser(
        prog="loom worker",
        description=(
            "Worker-host-local commands. Run on the host whose "
            "Docker daemon hosts the trial-cache layered images."
        ),
    )
    sub = parser.add_subparsers(dest="worker_cmd", required=True)

    p_cache = sub.add_parser(
        "cache", help="Inspect the trial image cache.",
    )
    cache_sub = p_cache.add_subparsers(dest="cache_cmd", required=True)

    p_stats = cache_sub.add_parser(
        "stats",
        help=(
            "List trial-cache layered images on the local Docker "
            "daemon. Filtered by label=loom.trial-cache=true."
        ),
    )
    p_stats.add_argument(
        "--json", action="store_true",
        help="Emit JSON instead of a human-readable table.",
    )
    p_stats.set_defaults(handler=_cache_stats_handler)

    p_setup = sub.add_parser(
        "setup", help="Inspect setup/build admission and Loom containers.",
    )
    setup_sub = p_setup.add_subparsers(dest="setup_cmd", required=True)
    p_setup_status = setup_sub.add_parser(
        "status",
        help=(
            "Show node setup-health admission state and Loom-labeled "
            "setup/trial containers on this worker host."
        ),
    )
    p_setup_status.add_argument(
        "--json", action="store_true",
        help="Emit JSON instead of a human-readable table.",
    )
    p_setup_status.set_defaults(handler=_setup_status_handler)

    args = parser.parse_args(argv)
    return int(args.handler(args))
