"""Self-service shared-fleet development environments.

This is deliberately a thin client over the guarded loom-service lifecycle
API. It never shells out to kubectl, writes autoscaler policy directly, or
reuses the local Docker Compose ``loom service`` path.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Callable
from typing import Any, cast

import httpx

from loom.dev_instance import PER_INSTANCE_CAP, InvalidDevInstanceNameError, validate_name
from loom_cli.server_client import (
    HttpStatusError,
    NotLoggedInError,
    assert_2xx,
    authed_client,
    require_logged_in,
)

_DEFAULT_WAIT_TIMEOUT = 600.0
_DEFAULT_POLL_INTERVAL = 2.0


def _dev_instance_name(value: str) -> str:
    try:
        validate_name(value)
    except InvalidDevInstanceNameError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    return value


def _positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _print_instance(instance: dict[str, Any], *, output_format: str) -> None:
    if output_format == "json":
        json.dump(instance, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
        return
    identity = cast(dict[str, Any], instance.get("identity") or {})
    print(f"Name: {instance.get('name', '-')}")
    print(f"Status: {instance.get('status', '-')}")
    print(
        "Capacity: "
        f"min {instance.get('min_slots', 0)} · max {instance.get('max_slots', 0)} shared slots",
    )
    print(f"Environment: {identity.get('environment', '-')}")
    print(f"Namespace: {identity.get('namespace', '-')}")
    host = str(identity.get("route_host") or "")
    if host:
        print(f"URL: https://{host}")
    failure = instance.get("failure_reason")
    if failure:
        print(f"Failure: {failure}")


def _print_list(body: dict[str, Any], *, output_format: str) -> None:
    items = cast(list[dict[str, Any]], body.get("items") or [])
    if output_format == "json":
        json.dump(items, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
        return
    if not items:
        print("No development environments.")
        return
    print("NAME                 STATUS        MIN  MAX  ENVIRONMENT")
    for item in items:
        identity = cast(dict[str, Any], item.get("identity") or {})
        print(
            f"{item.get('name', '-')!s:<20} "
            f"{item.get('status', '-')!s:<13} "
            f"{int(item.get('min_slots', 0)):<4} "
            f"{int(item.get('max_slots', 0)):<4} "
            f"{identity.get('environment', '-')}",
        )


def _wait_for_status(
    client: httpx.Client,
    name: str,
    *,
    target: str,
    timeout: float,
    poll_interval: float,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    deadline = monotonic() + timeout
    while True:
        body = assert_2xx(
            client.get(f"/api/v1/dev-instances/{name}"),
            action=f"check development environment {name!r}",
        )
        status = body.get("status")
        if status == target:
            return body
        if status == "failed":
            reason = body.get("failure_reason") or "unknown failure"
            raise HttpStatusError(
                f"development environment {name!r} failed: {reason}",
            )
        if monotonic() >= deadline:
            raise HttpStatusError(
                f"timed out after {timeout:g}s waiting for development "
                f"environment {name!r} to become {target}",
            )
        sleep(min(poll_interval, max(0.0, deadline - monotonic())))


def _with_client(action: Callable[[httpx.Client], int], *, timeout: float = 30.0) -> int:
    try:
        cfg = require_logged_in()
    except NotLoggedInError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    try:
        with authed_client(cfg, timeout=timeout) as client:
            return action(client)
    except HttpStatusError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    except httpx.RequestError as exc:
        sys.stderr.write(f"error: could not reach {cfg.server_url}: {exc}\n")
        return 2


def _create(args: argparse.Namespace) -> int:
    def action(client: httpx.Client) -> int:
        body = assert_2xx(
            client.post(
                "/api/v1/dev-instances",
                json={
                    "name": args.name,
                    "min_slots": args.min_slots,
                    "max_slots": args.max_slots,
                },
            ),
            action=f"create development environment {args.name!r}",
        )
        if not args.no_wait and body.get("status") != "ready":
            body = _wait_for_status(
                client,
                args.name,
                target="ready",
                timeout=args.timeout,
                poll_interval=args.poll_interval,
            )
        _print_instance(body, output_format=args.format)
        return 0

    return _with_client(action, timeout=max(30.0, args.timeout))


def _list(args: argparse.Namespace) -> int:
    def action(client: httpx.Client) -> int:
        body = assert_2xx(
            client.get(
                "/api/v1/dev-instances",
                params={
                    "mine": str(args.mine).lower(),
                    "include_deleted": str(args.include_deleted).lower(),
                },
            ),
            action="list development environments",
        )
        _print_list(body, output_format=args.format)
        return 0

    return _with_client(action)


def _status(args: argparse.Namespace) -> int:
    def action(client: httpx.Client) -> int:
        body = assert_2xx(
            client.get(f"/api/v1/dev-instances/{args.name}"),
            action=f"fetch development environment {args.name!r}",
        )
        _print_instance(body, output_format=args.format)
        return 0

    return _with_client(action)


def _destroy(args: argparse.Namespace) -> int:
    def action(client: httpx.Client) -> int:
        body = assert_2xx(
            client.delete(
                f"/api/v1/dev-instances/{args.name}",
                params={"keep_data": str(args.keep_data).lower()},
            ),
            action=f"destroy development environment {args.name!r}",
        )
        if not args.no_wait and body.get("status") != "deleted":
            body = _wait_for_status(
                client,
                args.name,
                target="deleted",
                timeout=args.timeout,
                poll_interval=args.poll_interval,
            )
        _print_instance(body, output_format=args.format)
        return 0

    return _with_client(action, timeout=max(30.0, args.timeout))


def _add_format(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format. Default: text.",
    )


def _add_wait(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Return after the lifecycle request is accepted.",
    )
    parser.add_argument(
        "--timeout",
        type=_positive_float,
        default=_DEFAULT_WAIT_TIMEOUT,
        help=f"Readiness wait timeout in seconds. Default: {_DEFAULT_WAIT_TIMEOUT:g}.",
    )
    parser.add_argument(
        "--poll-interval",
        type=_positive_float,
        default=_DEFAULT_POLL_INTERVAL,
        help=f"Readiness poll interval in seconds. Default: {_DEFAULT_POLL_INTERVAL:g}.",
    )


def dispatch(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="loom dev",
        description=(
            "Manage isolated persistent development environments on the shared "
            "fleet. For the local Docker Compose stack, use `loom service`; "
            "`loom service up` is intentionally local-only and has no dev/staging/prod selector."
        ),
    )
    sub = parser.add_subparsers(dest="dev_cmd", required=True)

    create = sub.add_parser("create", help="Create or converge one shared-fleet environment.")
    create.add_argument(
        "name",
        type=_dev_instance_name,
        help="Lowercase developer/environment slug.",
    )
    create.add_argument("--min-slots", type=int, default=0)
    create.add_argument("--max-slots", type=int, default=2, choices=range(PER_INSTANCE_CAP + 1))
    _add_wait(create)
    _add_format(create)
    create.set_defaults(handler=_create)

    listing = sub.add_parser("list", help="List visible development environments.")
    listing.add_argument(
        "--mine", action="store_true", help="Limit an admin listing to owned rows."
    )
    listing.add_argument("--include-deleted", action="store_true")
    _add_format(listing)
    listing.set_defaults(handler=_list)

    status = sub.add_parser("status", help="Show one development environment.")
    status.add_argument("name", type=_dev_instance_name)
    _add_format(status)
    status.set_defaults(handler=_status)

    destroy = sub.add_parser("destroy", help="Destroy one owned development environment.")
    destroy.add_argument("name", type=_dev_instance_name)
    destroy.add_argument(
        "--keep-data",
        action="store_true",
        help="Keep the dedicated database, buckets, and credentials for later recovery.",
    )
    _add_wait(destroy)
    _add_format(destroy)
    destroy.set_defaults(handler=_destroy)

    args = parser.parse_args(argv)
    if hasattr(args, "min_slots") and args.min_slots < 0:
        parser.error("--min-slots must be non-negative")
    if hasattr(args, "max_slots") and args.min_slots > args.max_slots:
        parser.error("--min-slots must not exceed --max-slots")
    return cast(int, args.handler(args))


__all__ = ["dispatch"]
