"""`loom admin tokens worker {mint, revoke, rotate}` — operator token
rotation for the Control Plane's worker-bearer admin endpoints (#80).

Wraps the existing CP endpoints (`POST /admin/worker-tokens`,
`DELETE /admin/worker-tokens/{prefix}`) so operators have a tested
CLI instead of curl recipes. The CP admin surface is NOT exposed via
Ingress on production deploys — operators reach it through a
port-forward (`kubectl port-forward deploy/loom-control-plane
8080:8080`); `--cp-url` defaults to `http://localhost:8080` to match.

Auth: the CP admin scope (`admin:tokens`) is gated on the singleton
admin token in `loom-admin-secret`. Per the argv-hygiene rule the
raw token isn't a literal flag; pass it via `--admin-token env:VAR`
(default `env:LOOM_ADMIN_TOKEN`).

This first slice ships worker-token rotation. Team-token rotation
(via `loom_service`'s `/api/v1/tokens`) and provider secret rewrap
(`SecretStore.rewrap`) are deferred to follow-up slices of #80.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from typing import cast

import httpx

# Same constraint as the CP route: prefix must be hex, 4-64 chars.
# Catching this client-side avoids a round-trip just to hit the 400.
_HEX_PREFIX_RE = re.compile(r"^[0-9a-f]{4,64}$")

_DEFAULT_CP_URL = "http://localhost:8080"
_DEFAULT_EXPIRES_DAYS = 365
_DEFAULT_ADMIN_TOKEN_SOURCE = "env:LOOM_ADMIN_TOKEN"


def _resolve_admin_token(source: str) -> str:
    """Resolve an `env:VAR` / `file:PATH` / `-` source to a raw token.

    Mirrors `loom_cli.secret_source` but accepts only what makes sense
    for the admin-token use case (token cycles long-lived, not piped
    repeatedly), and reports errors with the `--admin-token` flag
    name.
    """
    if source == "-":
        return sys.stdin.read().strip()
    if source.startswith("env:"):
        var = source[len("env:"):]
        if not var:
            raise ValueError("--admin-token env:VAR — VAR cannot be empty")
        try:
            return os.environ[var]
        except KeyError:
            raise ValueError(
                f"--admin-token env:{var} — environment variable not set",
            ) from None
    if source.startswith("file:"):
        path = source[len("file:"):]
        if not path:
            raise ValueError("--admin-token file:PATH — PATH cannot be empty")
        try:
            with open(path, encoding="utf-8") as f:
                return f.read().strip()
        except OSError as e:
            raise ValueError(f"--admin-token file:{path} — {e}") from None
    raise ValueError(
        f"--admin-token must be one of: env:VAR, file:PATH, '-' "
        f"(stdin). Got {source!r}",
    )


def _mint_worker_token(args: argparse.Namespace) -> int:
    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    url = f"{args.cp_url.rstrip('/')}/admin/worker-tokens"
    body: dict[str, int] = {}
    if args.expires_in_days is not None:
        body["expires_in_days"] = args.expires_in_days

    try:
        resp = httpx.post(
            url, json=body,
            headers={"Authorization": f"Bearer {admin_token}"},
            timeout=10.0,
        )
    except httpx.RequestError as e:
        sys.stderr.write(
            f"error: could not reach CP at {url}: {e}\n"
            f"hint: port-forward the Control Plane "
            f"(kubectl port-forward deploy/loom-control-plane 8080:8080)\n",
        )
        return 2

    if resp.status_code != 201:
        sys.stderr.write(
            f"error: CP returned {resp.status_code}: {resp.text}\n",
        )
        return 1

    data = resp.json()
    if args.format == "json":
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
    else:
        sys.stdout.write(
            f"New worker token minted.\n"
            f"  prefix: {data['token_hash_prefix']}\n"
            f"  token:  {data['token']}\n"
            f"\nNext: update the `worker-token` key in `loom-secrets` "
            f"and restart `deploy/loom-worker`.\n",
        )
    return 0


def _revoke_worker_token(args: argparse.Namespace) -> int:
    if not _HEX_PREFIX_RE.fullmatch(args.prefix):
        sys.stderr.write(
            f"error: prefix must be 4-64 hex characters; got "
            f"{args.prefix!r}\n",
        )
        return 2

    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    url = f"{args.cp_url.rstrip('/')}/admin/worker-tokens/{args.prefix}"
    try:
        resp = httpx.delete(
            url, headers={"Authorization": f"Bearer {admin_token}"},
            timeout=10.0,
        )
    except httpx.RequestError as e:
        sys.stderr.write(
            f"error: could not reach CP at {url}: {e}\n",
        )
        return 2

    if resp.status_code != 200:
        sys.stderr.write(
            f"error: CP returned {resp.status_code}: {resp.text}\n",
        )
        return 1
    sys.stdout.write(
        f"Worker token with prefix {args.prefix!r} revoked.\n",
    )
    return 0


def _rotate_worker_token(args: argparse.Namespace) -> int:
    """Mint a new worker token + print the rollout procedure. Does NOT
    revoke the old token automatically — that's an explicit
    `loom admin tokens worker revoke <prefix>` step, run AFTER the
    operator confirms the new token is live on every worker pod.
    A premature revoke would 401 in-flight worker claims.
    """
    rc = _mint_worker_token(args)
    if rc != 0:
        return rc
    if args.format != "json":
        sys.stdout.write(
            "\nRotation checklist:\n"
            "  1. Update `worker-token` key in `loom-secrets`:\n"
            "       kubectl patch secret loom-secrets \\\n"
            "         -p '{\"stringData\":{\"worker-token\":\"<NEW>\"}}'\n"
            "  2. Restart workers:\n"
            "       kubectl rollout restart deploy/loom-worker\n"
            "  3. Verify workers re-register (no 401s in worker logs).\n"
            "  4. Revoke the OLD token by its hash prefix:\n"
            "       loom admin tokens worker revoke <OLD_PREFIX>\n",
        )
    return 0


def _add_common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--cp-url", default=_DEFAULT_CP_URL,
        help=(
            f"Control Plane base URL (default: {_DEFAULT_CP_URL}). The "
            f"CP admin surface is NOT public; port-forward in another "
            f"shell: kubectl port-forward "
            f"deploy/loom-control-plane 8080:8080"
        ),
    )
    p.add_argument(
        "--admin-token", default=_DEFAULT_ADMIN_TOKEN_SOURCE,
        help=(
            f"Admin token source. ONE of: env:VAR (read os.environ[VAR]), "
            f"file:PATH (read file content), or '-' (read stdin). "
            f"Default: {_DEFAULT_ADMIN_TOKEN_SOURCE!r}."
        ),
    )


def dispatch(argv: list[str]) -> int:
    """Entry point invoked from `loom_cli.__main__` when `argv[0]` is
    `admin`. Returns the process exit code."""
    parser = argparse.ArgumentParser(
        prog="loom admin",
        description=(
            "Operator-only admin operations. Today: worker-token "
            "rotation via the Control Plane's admin surface."
        ),
    )
    sub = parser.add_subparsers(dest="admin_cmd", required=True)

    p_tokens = sub.add_parser(
        "tokens",
        help="Token mint / revoke / rotate.",
    )
    tok_sub = p_tokens.add_subparsers(dest="tokens_target", required=True)

    p_worker = tok_sub.add_parser(
        "worker",
        help="Worker-token operations (Control Plane admin surface).",
    )
    worker_sub = p_worker.add_subparsers(
        dest="worker_op", required=True,
    )

    p_mint = worker_sub.add_parser(
        "mint",
        help="Issue a new worker token. Prints the raw token + prefix.",
    )
    _add_common_args(p_mint)
    p_mint.add_argument(
        "--expires-in-days", type=int, default=_DEFAULT_EXPIRES_DAYS,
        help=(
            f"Token lifetime in days "
            f"(default: {_DEFAULT_EXPIRES_DAYS}). Pass 0 to omit the "
            f"expires_in_days field (server keeps default)."
        ),
    )
    p_mint.add_argument(
        "--format", choices=["text", "json"], default="text",
        help="Output format. JSON for scripting.",
    )
    p_mint.set_defaults(handler=_mint_worker_token)

    p_revoke = worker_sub.add_parser(
        "revoke",
        help="Revoke a worker token by its hash prefix.",
    )
    p_revoke.add_argument(
        "prefix",
        help="4-64 hex chars from token_hash_prefix returned at mint.",
    )
    _add_common_args(p_revoke)
    p_revoke.set_defaults(handler=_revoke_worker_token)

    p_rotate = worker_sub.add_parser(
        "rotate",
        help=(
            "Mint a new worker token + print the rollout procedure. "
            "Does NOT revoke the old token automatically — run "
            "`revoke <OLD_PREFIX>` once new is live."
        ),
    )
    _add_common_args(p_rotate)
    p_rotate.add_argument(
        "--expires-in-days", type=int, default=_DEFAULT_EXPIRES_DAYS,
        help=f"Token lifetime in days (default: {_DEFAULT_EXPIRES_DAYS}).",
    )
    p_rotate.add_argument(
        "--format", choices=["text", "json"], default="text",
        help="Output format.",
    )
    p_rotate.set_defaults(handler=_rotate_worker_token)

    args = parser.parse_args(argv)
    return cast(int, args.handler(args))
