"""`loom worker` — worker-host-local operator commands (#317 Phase 3b).

Currently:
- ``loom worker cache stats`` — inspect the trial-cache layered images
  on the worker's local Docker daemon (label=loom.trial-cache=true).

Runs on the worker host (where the Docker daemon is). Fleet-wide
aggregation is not in v1 — operators run this per-host via ssh or
their existing orchestration.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import sys
from typing import Any


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


def dispatch(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="loom worker",
        description=(
            "Worker-host-local commands. Run on the host whose "
            "Docker daemon hosts the trial-cache layered images."
        ),
    )
    sub = parser.add_subparsers(dest="worker_cmd", required=True)

    p_cache = sub.add_parser(
        "cache", help="Trial-cache (#317) inspection.",
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

    args = parser.parse_args(argv)
    return int(args.handler(args))
