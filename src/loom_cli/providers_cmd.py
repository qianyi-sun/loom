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
from urllib.parse import quote

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
from loom_cli.time_format import format_local_datetime


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
    pricing_source = getattr(args, "pricing_source", None)
    if pricing_source in {"rate-card", "tokens-only"} and (has_in or has_out):
        sys.stderr.write(
            "error: --input-usd-per-1m/--output-usd-per-1m can only be "
            "used with pricing_source='operator-supplied'.\n",
        )
        return 2
    if pricing_source == "operator-supplied" and not (has_in and has_out):
        sys.stderr.write(
            "error: --pricing-source operator-supplied requires "
            "--input-usd-per-1m and --output-usd-per-1m.\n",
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
    if item.get("rate_card_provider"):
        print(f"rate_card:     {item['rate_card_provider']}")
    if item.get("allowed_models"):
        print(f"allowed:       {', '.join(item['allowed_models'])}")
    if item.get("last_validation_error"):
        print(f"last_error:    {item['last_validation_error']}")
    print(f"created_at:    {format_local_datetime(item['created_at'])}")


def _print_create_next_steps(name: str) -> None:
    print()
    print("Next steps:")
    print(f"  loom providers test {name}")
    print(f"  loom providers models {name} --refresh")
    print(
        "  Then choose this provider connection and a discovered model in "
        "New Batch, or pass the provider/model flags in CLI eval commands.",
    )


def _resolve_by_name(
    client: httpx.Client,
    name: str,
    *,
    team_id: str | None = None,
) -> dict[str, Any]:
    """Look up a connection by display_name (the CLI's surface) and
    return its full row dict. Raises _NameNotFoundError if no/multiple
    matches; caller catches + returns exit code.

    Why server-side filter isn't an option: the list route accepts no
    name filter (matches the spec). For typical team sizes (≤ few
    dozen connections) client-side scan is fine.
    """
    params = {"team_id": team_id} if team_id is not None else None
    list_resp = client.get("/api/v1/provider-connections", params=params)
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
        if args.rate_card_provider is not None:
            payload["rate_card_provider"] = args.rate_card_provider
        if args.pricing_source is not None:
            payload["pricing_source"] = args.pricing_source
        pricing_data = _pricing_dict_or_none(
            args.input_usd_per_1m, args.output_usd_per_1m,
        )
        if pricing_data is not None:
            payload["pricing_source"] = args.pricing_source or "operator-supplied"
            payload["pricing_data"] = pricing_data

        with authed_client(cfg) as c:
            resp = c.post("/api/v1/provider-connections", json=payload)
        body = assert_2xx(
            resp, action=f"create provider connection {args.name!r}",
        )
        print(f"Created provider connection {body['name']!r}:")
        _print_connection_summary(body)
        _print_create_next_steps(body["name"])
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
        if args.rate_card_provider is not None:
            patch["rate_card_provider"] = args.rate_card_provider
        if args.pricing_source is not None:
            patch["pricing_source"] = args.pricing_source
        pricing_data = _pricing_dict_or_none(
            args.input_usd_per_1m, args.output_usd_per_1m,
        )
        if pricing_data is not None:
            patch["pricing_source"] = args.pricing_source or "operator-supplied"
            patch["pricing_data"] = pricing_data

        if not patch:
            sys.stderr.write(
                "error: `update` requires at least one of --base-url / "
                "--api-key / --allowed-models / --rate-card-provider / "
                "--pricing-source / --input-usd-per-1m + --output-usd-per-1m.\n",
            )
            return 2

        admin_actor = args.admin_actor.strip() if args.admin_actor else None
        if args.admin_actor is not None and not admin_actor:
            sys.stderr.write("error: --admin-actor must not be empty\n")
            return 2
        headers = (
            {"X-Loom-Admin-Actor": admin_actor}
            if admin_actor is not None
            else None
        )
        with authed_client(cfg) as c:
            row = _resolve_by_name(c, args.name)
            resp = c.patch(
                f"/api/v1/provider-connections/{row['id']}",
                json=patch,
                headers=headers,
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


def _print_model_row(entry: dict[str, Any]) -> None:
    """Stable one-row format for `models` table output. Greppable."""
    visible = "visible" if entry["visible"] else "hidden"
    flags = [visible]
    if entry.get("hidden_reason"):
        flags.append(entry["hidden_reason"])
    if not entry.get("upstream_present", True):
        flags.append("missing-upstream")
    preflight_status = entry.get("last_preflight_status")
    if preflight_status:
        flags.append(f"preflight={preflight_status}")
        if entry.get("last_preflight_error_code"):
            flags.append(str(entry["last_preflight_error_code"]))
    else:
        flags.append("preflight=untested")
    flag_str = ",".join(flags)
    extras = []
    if entry.get("family"):
        extras.append(f"family={entry['family']}")
    if entry.get("context_length"):
        extras.append(f"ctx={entry['context_length']}")
    extra_str = ("  " + " ".join(extras)) if extras else ""
    print(f"{entry['model_id']:<48}  [{flag_str}]{extra_str}")


def _models(args: argparse.Namespace) -> int:
    """`loom providers models NAME [--refresh] [--preflight M] [--hide M] [--unhide M]
    [--format text|json]` — inspect + manage the per-connection model
    cache.

    Action order: refresh → hide → unhide → preflight → list. All
    actions are additive (no mutual exclusion); the trailing list
    always runs so the operator sees the resulting state."""
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            row = _resolve_by_name(c, args.name)
            conn_id = row["id"]

            if args.refresh:
                resp = c.post(
                    f"/api/v1/provider-connections/{conn_id}/models/refresh",
                    # Generous timeout — the server caps the probe at 5s
                    # but we want headroom for round-trip + decrypt +
                    # batch upserts. 30s is the same default as the
                    # other authed_client calls; spell it for clarity.
                    timeout=30.0,
                )
                refresh_body = assert_2xx(
                    resp, action=f"refresh models for {args.name!r}",
                )
                if args.format != "json":
                    print(
                        f"Refreshed: +{refresh_body['added']} new, "
                        f"{refresh_body['refreshed']} updated, "
                        f"{refresh_body['missing']} missing.",
                    )

            if args.hide is not None:
                resp = c.post(
                    f"/api/v1/provider-connections/{conn_id}/models/"
                    f"{args.hide}/hide",
                )
                assert_2xx(
                    resp,
                    action=f"hide model {args.hide!r} for {args.name!r}",
                )
                if args.format != "json":
                    print(f"Hid model {args.hide!r}.")

            if args.unhide is not None:
                resp = c.post(
                    f"/api/v1/provider-connections/{conn_id}/models/"
                    f"{args.unhide}/unhide",
                )
                assert_2xx(
                    resp,
                    action=f"unhide model {args.unhide!r} for {args.name!r}",
                )
                if args.format != "json":
                    print(f"Unhid model {args.unhide!r}.")

            if args.preflight is not None:
                encoded_model = quote(args.preflight, safe="")
                resp = c.post(
                    f"/api/v1/provider-connections/{conn_id}/models/"
                    f"{encoded_model}/preflight",
                    timeout=30.0,
                )
                preflight_body = assert_2xx(
                    resp,
                    action=(
                        f"preflight model {args.preflight!r} "
                        f"for {args.name!r}"
                    ),
                )
                if args.format != "json":
                    status = preflight_body.get("last_preflight_status", "?")
                    print(f"Preflighted model {args.preflight!r}: {status}")
                    code = preflight_body.get("last_preflight_error_code")
                    message = preflight_body.get("last_preflight_error_message")
                    if code:
                        print(f"  error code: {code}")
                    if message:
                        print(f"  error: {message}")

            list_resp = c.get(
                f"/api/v1/provider-connections/{conn_id}/models",
            )
        body = assert_2xx(
            list_resp, action=f"list models for {args.name!r}",
        )
        items = body["items"]
        if args.format == "json":
            print(json.dumps(items, indent=2))
            return 0
        if not items:
            print(
                f"(no models cached for {args.name!r} — run "
                f"`loom providers models {args.name} --refresh` "
                f"to populate)",
            )
            return 0
        for it in items:
            _print_model_row(it)
        return 0

    return _run_with_error_handling(_body)


def _rotate_key(args: argparse.Namespace) -> int:
    """`loom providers rotate-key NAME --api-key SOURCE` —
    one-shot wrapper around `update --api-key SOURCE` + `test NAME`
    that emphasizes the rotation context for the operator.

    The server-side rotation path:
      1. `_make_secret_store(session).put()` writes new ciphertext
         under a fresh ref.
      2. `provider_connections.encrypted_api_key_ref` is swapped to
         the new ref; the row's `updated_at` advances.
      3. The OLD secret stays decryptable but is no longer pointed
         at by any connection. Phase 5 ships a cleanup job.

    Gateway-side: there is no in-memory cache for provider connection
    rows (the gateway looks up by id per-request — see
    `_facade_common.py:_lookup_provider_connection`), so the new key
    takes effect on the very next gateway call. No cache invalidation
    step is needed.

    --skip-test bypasses the post-rotation probe; useful when the
    upstream provider hasn't propagated the new key yet."""
    def _body() -> int:
        cfg = require_logged_in()
        try:
            new_api_key = resolve_secret_source(
                args.api_key, flag_name="--api-key",
            )
        except SecretSourceError as e:
            sys.stderr.write(f"error: {e}\n")
            return 2

        with authed_client(cfg) as c:
            row = _resolve_by_name(c, args.name)
            patch_resp = c.patch(
                f"/api/v1/provider-connections/{row['id']}",
                json={"api_key": new_api_key},
            )
            body = assert_2xx(
                patch_resp,
                action=f"rotate key for provider connection {args.name!r}",
            )
            print(
                f"Rotated api_key for provider connection "
                f"{body['name']!r}. status: {body.get('status', '?')!r} "
                f"(verifies on next test/call).",
            )
            if args.skip_test:
                print(
                    "Post-rotation test skipped (--skip-test). Run "
                    f"`loom providers test {args.name}` once the "
                    "upstream provider propagates the new key.",
                )
                return 0
            test_resp = c.post(
                f"/api/v1/provider-connections/{row['id']}/test",
                timeout=20.0,
            )
        test_body = assert_2xx(
            test_resp,
            action=f"test rotated key for {args.name!r}",
        )
        status = test_body.get("status", "unknown")
        print(f"Post-rotation test: status={status!r}")
        if status == "valid":
            return 0
        if test_body.get("last_validation_error"):
            print(
                f"  last_validation_error: "
                f"{test_body['last_validation_error']}",
            )
        sys.stderr.write(
            f"\nrotation succeeded but the new key did NOT probe valid. "
            f"The connection now points at the new ciphertext; either "
            f"the new key is wrong or the upstream provider hasn't "
            f"propagated it. Re-run `loom providers test {args.name}` "
            f"after a propagation delay, or rotate again with the "
            f"correct value.\n",
        )
        return 1

    return _run_with_error_handling(_body)


def _test(args: argparse.Namespace) -> int:
    """`loom providers test NAME` — probe the connection's base_url with
    the stored credentials. The server route persists the outcome on
    the row, so subsequent `loom providers show` reflects it."""
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            row = _resolve_by_name(c, args.name)
            resp = c.post(
                f"/api/v1/provider-connections/{row['id']}/test",
                # extend default timeout to cover the upstream probe;
                # the server caps at 5s but +5s for round-trip on top.
                timeout=20.0,
            )
        body = assert_2xx(
            resp, action=f"test provider connection {args.name!r}",
        )
        # status is 'valid' or 'invalid'; rc=0 for valid, rc=1 for
        # invalid so this is greppable from CI. .get() everywhere so
        # a malformed server response surfaces as 'unknown' rather
        # than crashing with KeyError.
        status = body.get("status", "unknown")
        print(f"name:                  {args.name}")
        print(f"status:                {status}")
        if body.get("http_status") is not None:
            print(f"http_status:           {body['http_status']}")
        if body.get("last_validation_error"):
            print(f"last_validation_error: {body['last_validation_error']}")
        if body.get("last_validated_at"):
            print(
                "last_validated_at:     "
                f"{format_local_datetime(body['last_validated_at'])}",
            )
        return 0 if status == "valid" else 1

    return _run_with_error_handling(_body)


# ──────────────────────────────────────────────────────────────────────
# Dispatch
# ──────────────────────────────────────────────────────────────────────


def _add_pricing_args(parser: argparse.ArgumentParser) -> None:
    """Shared by `create` and `update`. Both-or-neither validated in
    the handler (argparse can't natively express the constraint)."""
    parser.add_argument(
        "--pricing-source",
        choices=["rate-card", "tokens-only", "operator-supplied"],
        default=None,
        help=(
            "Cost attribution mode. Use rate-card only when the service "
            "rate-card table has matching provider/model rows; "
            "operator-supplied requires --input-usd-per-1m and "
            "--output-usd-per-1m."
        ),
    )
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
    p_create.add_argument(
        "--rate-card-provider", default=None,
        help=(
            "Rate-card provider namespace for facade cost lookup "
            "(e.g. openai, together, fireworks)."
        ),
    )
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
    p_update.add_argument(
        "--admin-actor",
        default=None,
        help=(
            "Sets X-Loom-Admin-Actor for admin-token provider mutations. "
            "Required by the service when an admin credential updates a "
            "provider on behalf of a team or rollout."
        ),
    )
    p_update.add_argument(
        "--rate-card-provider", default=None,
        help=(
            "Set the rate-card provider namespace used when "
            "pricing_source='rate-card'."
        ),
    )
    p_update.set_defaults(handler=_update)

    # --- delete ---
    p_delete = sub.add_parser("delete", help="Soft-delete a connection.")
    p_delete.add_argument("name", help="Display name to delete.")
    p_delete.set_defaults(handler=_delete)

    # --- rotate-key ---
    p_rotate = sub.add_parser(
        "rotate-key",
        help=(
            "Rotate the provider connection's API key in one step. "
            "Equivalent to `update --api-key SOURCE` + `test NAME`, "
            "but the dedicated verb makes rotation runbooks clearer."
        ),
    )
    p_rotate.add_argument("name", help="Display name of the connection.")
    p_rotate.add_argument(
        "--api-key", required=True,
        type=secret_source_argparse_type("--api-key"),
        help="See `loom providers create --api-key`.",
    )
    p_rotate.add_argument(
        "--skip-test", action="store_true",
        help=(
            "Skip the post-rotation probe. Use when the upstream "
            "provider hasn't propagated the new key yet — the "
            "rotation still completes, you re-run "
            "`loom providers test NAME` separately to verify."
        ),
    )
    p_rotate.set_defaults(handler=_rotate_key)

    # --- test ---
    p_test = sub.add_parser(
        "test",
        help=(
            "Probe the connection's base_url with the stored credentials. "
            "Updates status + last_validated_at on the server."
        ),
    )
    p_test.add_argument("name", help="Display name to test.")
    p_test.set_defaults(handler=_test)

    # --- models ---
    p_models = sub.add_parser(
        "models",
        help=(
            "List, refresh, or hide/unhide models on a provider's "
            "cached model list. With no flags, lists the current cache."
        ),
    )
    p_models.add_argument("name", help="Provider connection display name.")
    p_models.add_argument(
        "--refresh", action="store_true",
        help=(
            "Probe the upstream /models endpoint and upsert the cache "
            "before listing. The probe is server-side; the CLI just "
            "triggers it."
        ),
    )
    p_models.add_argument(
        "--hide", default=None, metavar="MODEL",
        help=(
            "Mark MODEL as operator-hidden (sticky across refreshes). "
            "Must already be in the cache — run --refresh first if not."
        ),
    )
    p_models.add_argument(
        "--unhide", default=None, metavar="MODEL",
        help=(
            "Reverse a previous --hide. Re-visible only if the model "
            "is still upstream-present."
        ),
    )
    p_models.add_argument(
        "--preflight", default=None, metavar="MODEL",
        help=(
            "Run one minimal generation request against MODEL to verify "
            "this connection/key can call it. MODEL must already be cached."
        ),
    )
    p_models.add_argument(
        "--format", choices=["table", "json"], default="table",
        help="Output format. JSON for scripting.",
    )
    p_models.set_defaults(handler=_models)

    args = parser.parse_args(argv)
    return cast(int, args.handler(args))
