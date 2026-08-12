"""Site-admin CLI for immutable official-Recipe control bindings."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import httpx

from loom_cli.server_client import (
    HttpStatusError,
    NotLoggedInError,
    assert_2xx,
    authed_client,
    require_logged_in,
)


def _recipe(value: str) -> tuple[str, int]:
    try:
        name, raw_version = value.rsplit("@", 1)
        version = int(raw_version)
    except (ValueError, TypeError) as exc:
        raise ValueError("recipe must be NAME@positive-version") from exc
    if not name or version < 1:
        raise ValueError("recipe must be NAME@positive-version")
    return name, version


def _path(args: argparse.Namespace) -> str:
    name, version = _recipe(args.recipe)
    if args.binding_kind == "judge-profile":
        return f"/api/v1/admin/judge-execution-profiles/{name}/{version}/{args.name}"
    return f"/api/v1/admin/recipe-provider-bindings/{name}/{version}/{args.name}"


def _output(value: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        json.dump(value, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
        return
    sys.stdout.write(
        f"{value.get('profile_name', value.get('logical_name', '-'))} "
        f"v{value.get('version', '-')} {value.get('status', '-')}\n"
        f"  snapshot: {value.get('snapshot_sha256', '-')}\n"
    )


def _show(args: argparse.Namespace) -> int:
    try:
        cfg = require_logged_in()
        with authed_client(cfg) as client:
            value = assert_2xx(client.get(_path(args)), action="read Pipeline control binding")
    except (NotLoggedInError, ValueError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except HttpStatusError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    except httpx.RequestError as exc:
        sys.stderr.write(f"error: Pipeline service is unreachable: {exc}\n")
        return 2
    _output(value, as_json=args.json)
    return 0


def _apply(args: argparse.Namespace) -> int:
    try:
        cfg = require_logged_in()
        raw = json.loads(Path(args.file).read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("apply file must contain one JSON object")
        headers = {"Idempotency-Key": args.idempotency_key}
        if args.create:
            headers["If-None-Match"] = "*"
        else:
            headers["If-Match-Version"] = str(args.if_match_version)
        with authed_client(cfg) as client:
            value = assert_2xx(
                client.put(_path(args), json=raw, headers=headers),
                action="apply Pipeline control binding",
            )
    except (NotLoggedInError, OSError, json.JSONDecodeError, ValueError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except HttpStatusError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    except httpx.RequestError as exc:
        sys.stderr.write(f"error: Pipeline service is unreachable: {exc}\n")
        return 2
    _output(value, as_json=args.json)
    return 0


def add_pipeline_admin_subparser(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    pipeline = sub.add_parser("pipeline", help="Manage official Pipeline control bindings.")
    binding_sub = pipeline.add_subparsers(dest="pipeline_binding_kind", required=True)
    for command, kind, default_name in (
        ("judge-profile", "judge-profile", None),
        ("provider-binding", "provider-binding", "behavior_recovery_primitive"),
    ):
        parser = binding_sub.add_parser(command)
        operation_sub = parser.add_subparsers(dest="pipeline_binding_operation", required=True)
        for operation, handler in (("show", _show), ("apply", _apply)):
            operation_parser = operation_sub.add_parser(operation)
            operation_parser.set_defaults(binding_kind=kind, handler=handler)
            operation_parser.add_argument("--recipe", required=True, help="Official NAME@VERSION.")
            operation_parser.add_argument(
                "--name",
                default=default_name,
                required=default_name is None,
                help="Profile name or immutable logical binding name.",
            )
            operation_parser.add_argument("--json", action="store_true")
            if operation == "apply":
                operation_parser.add_argument("--file", required=True)
                operation_parser.add_argument("--idempotency-key", required=True)
                precondition = operation_parser.add_mutually_exclusive_group(required=True)
                precondition.add_argument("--create", action="store_true")
                precondition.add_argument("--if-match-version", type=int)


__all__ = ["add_pipeline_admin_subparser"]
