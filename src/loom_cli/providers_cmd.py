"""`loom providers {create,list,show,update,delete}` — manage
per-team LLM provider connections on the deployed Loom server.

Wraps the routes shipped in `src/loom_service/routes/provider_connections.py`.
Requires `loom auth login` to have been run first.

The `test` and `models` subcommands land in a follow-up PR after the
server routes for those exist.

Per cluster-deploy.md §CLI surface argv-hygiene rule, `--api-key`
accepts only the same `env:VAR | file:PATH | -` indirection forms as
`--token` in `loom auth login`.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from typing import Any, cast

import httpx

from loom_cli.secret_source import (
    SecretSourceError,
    resolve_secret_source,
    secret_source_argparse_type,
)
from loom_cli.server_client import (
    HttpStatusError,
    NotLoggedInError,
    assert_2xx,
    authed_client,
    require_logged_in,
)


class _NameNotFoundError(Exception):
    """`loom providers show/update/delete NAME` couldn't find NAME. Same
    raise-not-exit rationale as HttpStatusError — keeps handlers
    testable via `rc = main()`."""


def _pricing_dict_or_none(
    in_per_1m: float | None, out_per_1m: float | None,
) -> dict[str, float] | None:
    """`--input-usd-per-1m` and `--output-usd-per-1m` are interdependent
    (both-or-neither). Return the dict when both are set, None when
    both unset; argparse-level validator rejects half-set combinations
    earlier so this only sees valid pairs."""
    if in_per_1m is None and out_per_1m is None:
        return None
    assert in_per_1m is not None and out_per_1m is not None
    return {
        "input_usd_per_1m": in_per_1m,
        "output_usd_per_1m": out_per_1m,
    }


def _validate_pricing_both_or_neither(
    args: argparse.Namespace,
) -> int | None:
    """argparse can't natively express "if A then B is required"; this
    validator runs in the handler before any HTTP call. Returns None
    if OK, exit code on error so the handler can return it directly
    (preserves `main()`'s return-the-int contract — sys.exit() here
    would propagate SystemExit out of main(), breaking tests + any
    embedding harness that expects an int)."""
    has_in = args.input_usd_per_1m is not None
    has_out = args.output_usd_per_1m is not None
    if has_in != has_out:
        sys.stderr.write(
            "error: --input-usd-per-1m and --output-usd-per-1m are "
            "interdependent (both or neither).\n",
        )
        return 2
    return None


def _print_connection_summary(item: dict[str, Any]) -> None:
    """Stable per-connection output for `show` and (line-per-row)
    `list`. Designed to be greppable for CI use."""
    print(f"id:            {item['id']}")
    print(f"name:          {item['name']}")
    print(f"type:          {item['type']}")
    print(f"base_url:      {item['base_url']}")
    print(f"upstream_host: {item['upstream_host']}")
    print(f"resolved_ips:  {', '.join(item['resolved_egress_ips']) or '(none yet)'}")
    print(f"status:        {item['status']}")
    print(f"pricing:       {item['pricing_source']}", end="")
    if item.get("pricing_data"):
        pd = item["pricing_data"]
        print(
            f" (input={pd.get('input_usd_per_1m')}, "
            f"output={pd.get('output_usd_per_1m')} USD/1M tokens)",
        )
    else:
        print()
    if item.get("allowed_models"):
        print(f"allowed:       {', '.join(item['allowed_models'])}")
    if item.get("last_validation_error"):
        print(f"last_error:    {item['last_validation_error']}")
    print(f"created_at:    {item['created_at']}")


def _resolve_by_name(client: httpx.Client, name: str) -> dict[str, Any]:
    """Look up a connection by display_name (the CLI's surface) and
    return its full row dict. Raises _NameNotFoundError if no/multiple
    matches; caller catches + returns exit code.

    Why server-side filter isn't an option: the list route accepts no
    name filter (matches the spec). For typical team sizes (≤ few
    dozen connections) client-side scan is fine.
    """
    list_resp = client.get("/api/v1/provider-connections")
    items = assert_2xx(list_resp, action="list provider connections")["items"]
    matches = [it for it in items if it["name"] == name]
    if not matches:
        raise _NameNotFoundError(
            f"no provider connection named {name!r}. "
            f"Run `loom providers list` to see what's available.",
        )
    if len(matches) > 1:
        # Soft-delete dedup means UNIQUE on (team_id, name) WHERE
        # deleted_at IS NULL — only ONE active row per name. This is a
        # belt-and-suspenders check.
        raise _NameNotFoundError(
            f"multiple active connections named {name!r} "
            f"({len(matches)} rows). Use `loom providers show NAME` "
            f"+ pick the UUID explicitly.",
        )
    return cast(dict[str, Any], matches[0])


def _run_with_error_handling(fn: Callable[[], int]) -> int:
    """Wrap a handler body so HttpStatusError + _NameNotFoundError +
    NotLoggedInError + SecretSourceError all translate to a printed
    error + exit code. Keeps handler bodies compact."""
    try:
        return fn()
    except (HttpStatusError, _NameNotFoundError) as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except (NotLoggedInError, SecretSourceError) as e:
        sys.stderr.write(f"error: {e}\n")
        return 2


# ──────────────────────────────────────────────────────────────────────
# Handlers
# ──────────────────────────────────────────────────────────────────────


def _create(args: argparse.Namespace) -> int:
    if (rc := _validate_pricing_both_or_neither(args)) is not None:
        return rc

    def _body() -> int:
        cfg = require_logged_in()
        api_key = resolve_secret_source(args.api_key, flag_name="--api-key")

        payload: dict[str, Any] = {
            "name": args.name,
            "type": args.type,
            "base_url": args.base_url,
            "api_key": api_key,
        }
        if args.allowed_models:
            payload["allowed_models"] = args.allowed_models
        pricing_data = _pricing_dict_or_none(
            args.input_usd_per_1m, args.output_usd_per_1m,
        )
        if pricing_data is not None:
            payload["pricing_source"] = "operator-supplied"
            payload["pricing_data"] = pricing_data

        with authed_client(cfg) as c:
            resp = c.post("/api/v1/provider-connections", json=payload)
        body = assert_2xx(
            resp, action=f"create provider connection {args.name!r}",
        )
        print(f"Created provider connection {body['name']!r}:")
        _print_connection_summary(body)
        return 0

    return _run_with_error_handling(_body)


def _list(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            resp = c.get("/api/v1/provider-connections")
        body = assert_2xx(resp, action="list provider connections")
        items = body["items"]

        if args.format == "json":
            print(json.dumps(items, indent=2))
            return 0
        if not items:
            print("(no provider connections — run `loom providers create`)")
            return 0
        for it in items:
            pricing = it["pricing_source"]
            if it.get("pricing_data"):
                pd = it["pricing_data"]
                pricing += (
                    f" ({pd.get('input_usd_per_1m')}/"
                    f"{pd.get('output_usd_per_1m')})"
                )
            print(
                f"{it['name']:<24}  {it['type']:<20}  "
                f"{it['status']:<10}  {pricing}",
            )
        return 0

    return _run_with_error_handling(_body)


def _show(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            row = _resolve_by_name(c, args.name)
        if args.format == "json":
            print(json.dumps(row, indent=2))
        else:
            _print_connection_summary(row)
        return 0

    return _run_with_error_handling(_body)


def _update(args: argparse.Namespace) -> int:
    if (rc := _validate_pricing_both_or_neither(args)) is not None:
        return rc

    def _body() -> int:
        cfg = require_logged_in()
        patch: dict[str, Any] = {}
        if args.base_url is not None:
            patch["base_url"] = args.base_url
        if args.api_key is not None:
            patch["api_key"] = resolve_secret_source(
                args.api_key, flag_name="--api-key",
            )
        if args.allowed_models is not None:
            patch["allowed_models"] = args.allowed_models
        pricing_data = _pricing_dict_or_none(
            args.input_usd_per_1m, args.output_usd_per_1m,
        )
        if pricing_data is not None:
            patch["pricing_source"] = "operator-supplied"
            patch["pricing_data"] = pricing_data

        if not patch:
            sys.stderr.write(
                "error: `update` requires at least one of --base-url / "
                "--api-key / --allowed-models / --input-usd-per-1m + "
                "--output-usd-per-1m.\n",
            )
            return 2

        with authed_client(cfg) as c:
            row = _resolve_by_name(c, args.name)
            resp = c.patch(
                f"/api/v1/provider-connections/{row['id']}", json=patch,
            )
        body = assert_2xx(
            resp, action=f"update provider connection {args.name!r}",
        )
        print(f"Updated provider connection {body['name']!r}:")
        _print_connection_summary(body)
        return 0

    return _run_with_error_handling(_body)


def _delete(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            row = _resolve_by_name(c, args.name)
            resp = c.delete(f"/api/v1/provider-connections/{row['id']}")
        assert_2xx(resp, action=f"delete provider connection {args.name!r}")
        print(
            f"Soft-deleted provider connection {args.name!r}. Re-create with "
            f"`loom providers create --name {args.name}` if needed.",
        )
        return 0

    return _run_with_error_handling(_body)


# ──────────────────────────────────────────────────────────────────────
# Dispatch
# ──────────────────────────────────────────────────────────────────────


def _add_pricing_args(parser: argparse.ArgumentParser) -> None:
    """Shared by `create` and `update`. Both-or-neither validated in
    the handler (argparse can't natively express the constraint)."""
    parser.add_argument(
        "--input-usd-per-1m", dest="input_usd_per_1m",
        type=float, default=None,
        help=(
            "Per-1M-input-tokens cost in USD. Pairs with "
            "--output-usd-per-1m; both required together. Triggers "
            "pricing_source='operator-supplied'."
        ),
    )
    parser.add_argument(
        "--output-usd-per-1m", dest="output_usd_per_1m",
        type=float, default=None,
        help="Per-1M-output-tokens cost in USD. Pairs with --input-usd-per-1m.",
    )


def dispatch(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="loom providers",
        description=(
            "Manage LLM provider connections on the deployed Loom "
            "server. Requires `loom auth login` first."
        ),
    )
    sub = parser.add_subparsers(dest="providers_cmd", required=True)

    # --- create ---
    p_create = sub.add_parser(
        "create",
        help="Register a new provider connection (encrypts the api_key at rest).",
    )
    p_create.add_argument("--name", required=True,
                          help="Display name (unique per team).")
    p_create.add_argument(
        "--type", required=True,
        choices=["openai-compatible", "anthropic", "google", "custom"],
        help="Provider type. Determines default pricing_source.",
    )
    p_create.add_argument("--base-url", required=True,
                          help="Provider HTTPS endpoint (e.g. https://api.openai.com/v1)")
    p_create.add_argument(
        "--api-key", required=True,
        type=secret_source_argparse_type("--api-key"),
        help=(
            "API key source. ONE of: 'env:VAR', 'file:PATH', '-' "
            "(stdin). Literal values rejected — argv leaks via shell "
            "history + ps -ef + CI logs."
        ),
    )
    p_create.add_argument(
        "--allowed-models", nargs="+", default=None,
        help="Restrict the connection to specific upstream model ids.",
    )
    _add_pricing_args(p_create)
    p_create.set_defaults(handler=_create)

    # --- list ---
    p_list = sub.add_parser("list", help="List active provider connections.")
    p_list.add_argument(
        "--format", choices=["table", "json"], default="table",
        help="Output format. JSON for scripting.",
    )
    p_list.set_defaults(handler=_list)

    # --- show ---
    p_show = sub.add_parser("show", help="Show details for one connection.")
    p_show.add_argument("name", help="Display name.")
    p_show.add_argument(
        "--format", choices=["text", "json"], default="text",
    )
    p_show.set_defaults(handler=_show)

    # --- update ---
    p_update = sub.add_parser(
        "update",
        help="Update base_url / api_key / allowed_models / pricing.",
    )
    p_update.add_argument("name", help="Display name to update.")
    p_update.add_argument("--base-url", default=None)
    p_update.add_argument(
        "--api-key", default=None,
        type=secret_source_argparse_type("--api-key"),
        help="See `loom providers create --api-key`.",
    )
    p_update.add_argument(
        "--allowed-models", nargs="+", default=None,
        help="Replace the allowed-model list (NOT append).",
    )
    _add_pricing_args(p_update)
    p_update.set_defaults(handler=_update)

    # --- delete ---
    p_delete = sub.add_parser("delete", help="Soft-delete a connection.")
    p_delete.add_argument("name", help="Display name to delete.")
    p_delete.set_defaults(handler=_delete)

    args = parser.parse_args(argv)
    return cast(int, args.handler(args))
