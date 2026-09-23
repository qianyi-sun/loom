"""Audited human administrator changes using the current CLI login."""

from __future__ import annotations

import argparse
import json
import sys
from uuid import UUID

import httpx

from loom_cli.server_client import (
    HttpStatusError,
    NotLoggedInError,
    assert_2xx,
    authed_client,
    require_logged_in,
)


def _change(args: argparse.Namespace) -> int:
    try:
        cfg = require_logged_in()
    except NotLoggedInError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    body = (
        {"ensure_admin_team": args.ensure_admin_team}
        if args.operation == "grant"
        else {"credential_policy": args.credential_policy}
    )
    headers = {"X-Loom-Admin-Actor": args.admin_actor} if args.admin_actor else {}
    try:
        with authed_client(cfg) as client:
            response = client.post(
                f"/api/v1/admin/users/{args.user_id}/platform-admin/{args.operation}",
                json=body, headers=headers,
            )
            result = assert_2xx(response, action=f"{args.operation} platform admin")
    except HttpStatusError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    except httpx.RequestError:
        sys.stderr.write("error: could not reach the configured Loom server\n")
        return 2
    json.dump(result, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


def add_platform_admin_subparser(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = sub.add_parser("platform-admin", help="Grant or revoke a human's platform-admin role.")
    operations = parser.add_subparsers(dest="operation", required=True)
    for operation in ("grant", "revoke"):
        command = operations.add_parser(operation)
        command.add_argument("--user-id", type=UUID, required=True, help="Exact target user UUID.")
        command.add_argument(
            "--admin-actor", help="Required only for singleton operator-secret authentication.",
        )
        if operation == "grant":
            command.add_argument(
                "--ensure-admin-team", action=argparse.BooleanOptionalAction, default=True,
                help="Ensure owner membership in the existing enabled reserved admin team.",
            )
        else:
            command.add_argument(
                "--credential-policy", choices=["revoke_all"], required=True,
                help="Revoke all existing sessions and user-owned tokens, including this login if self.",
            )
        command.set_defaults(handler=_change)
