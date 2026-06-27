"""`loom resources status` exposes Monitor resource-pool slots."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, cast

import httpx

from loom_cli.server_client import (
    HttpStatusError,
    NotLoggedInError,
    assert_2xx,
    authed_client,
    require_logged_in,
)


def _format_slots(pool: dict[str, Any]) -> str:
    return f"{int(pool.get('occupied_slots', 0))}/{int(pool.get('total_slots', 0))}"


def _format_idle(pool: dict[str, Any]) -> str:
    value = pool.get("autoscaler_idle_seconds")
    return "-" if value is None else f"idle={int(value)}s"


def _print_text(resources: dict[str, Any]) -> None:
    aggregate = cast(dict[str, Any], resources.get("aggregate") or {})
    pools = cast(list[dict[str, Any]], resources.get("pools") or [])

    occupied = int(aggregate.get("occupied_slots", 0))
    total = int(aggregate.get("total_slots", 0))
    running = int(aggregate.get("running_tasks", 0))
    starting = int(aggregate.get("starting_tasks", 0))
    queued = int(aggregate.get("queued_tasks", 0))
    active_workers = int(aggregate.get("active_workers", 0))
    draining_workers = int(aggregate.get("draining_workers", 0))
    desired = int(aggregate.get("desired_slots", 0))
    pending = int(aggregate.get("pending_slots", 0))
    draining = int(aggregate.get("draining_slots", 0))
    free = int(aggregate.get("free_slots", 0))

    print(f"Concurrent tasks: {occupied} / {total}")
    print(f"Running: {running} · Starting: {starting} · Queued: {queued}")
    print(f"Workers: {active_workers} active · Free slots: {free}")
    print(
        f"Autoscaler: desired {desired} · pending {pending} · "
        f"draining {draining} slots / {draining_workers} workers",
    )
    print()
    print("Pools:")
    if not pools:
        print("  no active resource pools")
        return
    print(
        "  Pool                Backend  Arch    Slots   Desired  Pending  "
        "Draining  Idle       Running  Starting  Queued  Workers  Decision"
    )
    for pool in pools:
        decision = pool.get("last_autoscaler_decision") or "-"
        print(
            "  "
            f"{pool.get('pool_name', 'default')!s:<19} "
            f"{pool.get('backend', 'docker')!s:<8} "
            f"{pool.get('cpu_arch', 'x86_64')!s:<7} "
            f"{_format_slots(pool):<7} "
            f"{int(pool.get('desired_slots', 0)):<8} "
            f"{int(pool.get('pending_slots', 0)):<8} "
            f"{int(pool.get('draining_slots', 0)):<9} "
            f"{_format_idle(pool):<10} "
            f"{int(pool.get('running_tasks', 0)):<8} "
            f"{int(pool.get('starting_tasks', 0)):<9} "
            f"{int(pool.get('queued_tasks', 0)):<7} "
            f"{int(pool.get('active_workers', 0)):<7} "
            f"{decision}",
        )


def _status(args: argparse.Namespace) -> int:
    try:
        cfg = require_logged_in()
    except NotLoggedInError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    try:
        with authed_client(cfg) as c:
            body = assert_2xx(
                c.get("/api/v1/monitor/summary", params={"view": "trials"}),
                action="fetch resource summary",
            )
    except HttpStatusError as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except httpx.RequestError as e:
        sys.stderr.write(f"error: could not reach {cfg.server_url}: {e}\n")
        return 2

    resources = cast(dict[str, Any], body.get("resources") or {})
    if args.format == "json":
        json.dump(resources, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    else:
        _print_text(resources)
    return 0


def dispatch(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="loom resources",
        description="Inspect concurrent task slots and resource-pool pressure.",
    )
    sub = parser.add_subparsers(dest="resources_cmd", required=True)

    p_status = sub.add_parser(
        "status",
        help="Show aggregate and per-pool execution-slot capacity.",
    )
    p_status.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format. Default: text.",
    )
    p_status.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Shortcut for --format json.",
    )
    p_status.set_defaults(handler=_status)

    args = parser.parse_args(argv)
    if getattr(args, "json_output", False):
        args.format = "json"
    return cast(int, args.handler(args))
