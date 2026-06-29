"""`loom admin` — operator-only admin operations (#80).

Subcommands:

- ``loom admin tokens worker {mint,revoke,rotate}`` — worker-token
  rotation via the Control Plane's admin surface.
- ``loom admin tokens team {mint,revoke,rotate}`` — team-token
  rotation via loom_service's /api/v1/tokens route.
- ``loom admin rate-cards sync-yibuapi`` — sync the official YibuAPI
  pricing catalog into the service rate-card store.
- ``loom admin secret-store rewrap`` — master-key rotation walker;
  re-encrypts all SecretStore rows with the primary key configured
  in ``LOOM_SECRET_STORE_MASTER_KEYS``.

The CP admin surface is NOT exposed via Ingress; reach it through a
port-forward (``kubectl port-forward deploy/loom-control-plane 8080:8080``).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import httpx

from loom_cli.server_client import (
    HttpStatusError,
    NotLoggedInError,
    assert_2xx,
    authed_client,
    require_logged_in,
)

if TYPE_CHECKING:
    from loom_cli.environment_state import EnvironmentStateProfile

# Same constraint as the CP route: prefix must be hex, 4-64 chars.
# Catching this client-side avoids a round-trip just to hit the 400.
_HEX_PREFIX_RE = re.compile(r"^[0-9a-f]{4,64}$")

_DEFAULT_CP_URL = "http://localhost:8080"
_DEFAULT_EXPIRES_DAYS = 365
_DEFAULT_ADMIN_TOKEN_SOURCE = "env:LOOM_ADMIN_TOKEN"


def _gb10_release_target_mismatches(
    data: dict[str, Any],
    *,
    release_image_tag: str | None,
    release_env_config_version: str | None,
) -> list[str]:
    if release_image_tag is None and release_env_config_version is None:
        return []

    mismatches: list[str] = []
    for row in data.get("desired_states", []):
        if not isinstance(row, dict):
            continue
        image = row.get("image_tag")
        env = row.get("env_config_version")
        image_bad = release_image_tag is not None and image != release_image_tag
        env_bad = (
            release_env_config_version is not None
            and env != release_env_config_version
        )
        if image_bad or env_bad:
            mismatches.append(
                "desired "
                f"{row.get('environment', '-')}/{row.get('pool_name', '-')} "
                f"image={image or '-'}/{release_image_tag or '-'} "
                f"env={env or '-'}/{release_env_config_version or '-'}"
            )

    ignored_intents = {"stopped", "draining", "drained", "unavailable"}
    ignored_apply_states = {"stopped", "draining", "unavailable"}
    for node in data.get("nodes", []):
        if not isinstance(node, dict):
            continue
        intent = node.get("desired_intent") or node.get("current_intent")
        apply_state = node.get("apply_state")
        if intent in ignored_intents or apply_state in ignored_apply_states:
            continue
        image = node.get("current_image_tag")
        env = node.get("current_env_config_version")
        image_bad = release_image_tag is not None and image != release_image_tag
        env_bad = (
            release_env_config_version is not None
            and env != release_env_config_version
        )
        if image_bad or env_bad:
            mismatches.append(
                "node "
                f"{node.get('hostname', '-')} "
                f"image={image or '-'}/{release_image_tag or '-'} "
                f"env={env or '-'}/{release_env_config_version or '-'} "
                f"apply_state={apply_state or '-'}"
            )

    return mismatches


def _print_gb10_release_target_mismatches(
    mismatches: list[str],
    *,
    release_image_tag: str | None,
    release_env_config_version: str | None,
) -> None:
    if not mismatches:
        return
    sys.stderr.write(
        "GB10 rollout target mismatch: "
        f"{len(mismatches)} active desired/node state(s) do not match "
        f"release target image={release_image_tag or '-'} "
        f"env={release_env_config_version or '-'}\n",
    )
    for item in mismatches:
        sys.stderr.write(f"  {item}\n")


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
        var = source[len("env:") :]
        if not var:
            raise ValueError("--admin-token env:VAR — VAR cannot be empty")
        try:
            return os.environ[var]
        except KeyError:
            raise ValueError(
                f"--admin-token env:{var} — environment variable not set",
            ) from None
    if source.startswith("file:"):
        path = source[len("file:") :]
        if not path:
            raise ValueError("--admin-token file:PATH — PATH cannot be empty")
        try:
            with open(path, encoding="utf-8") as f:
                return f.read().strip()
        except OSError as e:
            raise ValueError(f"--admin-token file:{path} — {e}") from None
    raise ValueError(
        f"--admin-token must be one of: env:VAR, file:PATH, '-' (stdin). Got {source!r}",
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
            url,
            json=body,
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
            f"error: prefix must be 4-64 hex characters; got {args.prefix!r}\n",
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
            url,
            headers={"Authorization": f"Bearer {admin_token}"},
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
            '         -p \'{"stringData":{"worker-token":"<NEW>"}}\'\n'
            "  2. Restart workers:\n"
            "       kubectl rollout restart deploy/loom-worker\n"
            "  3. Verify workers re-register (no 401s in worker logs).\n"
            "  4. Revoke the OLD token by its hash prefix:\n"
            "       loom admin tokens worker revoke <OLD_PREFIX>\n",
        )
    return 0


def _slurm_workers_status(args: argparse.Namespace) -> int:
    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    url = f"{args.cp_url.rstrip('/')}/admin/slurm-worker-jobs/status"
    try:
        resp = httpx.get(
            url,
            headers={"Authorization": f"Bearer {admin_token}"},
            timeout=10.0,
        )
    except httpx.RequestError as e:
        sys.stderr.write(f"error: could not reach CP at {url}: {e}\n")
        return 2

    if resp.status_code != 200:
        sys.stderr.write(
            f"error: CP returned {resp.status_code}: {resp.text}\n",
        )
        return 1

    data = resp.json()
    if args.format == "json":
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0

    summary = data.get("summary", [])
    jobs = data.get("jobs", [])
    sys.stdout.write("Slurm worker capacity:\n")
    if not summary:
        sys.stdout.write("  no Slurm worker jobs recorded\n")
    for row in summary:
        sys.stdout.write(
            f"  {row['environment']}/{row['pool_name']} "
            f"desired={row['desired_slots']} "
            f"active={row['active_slots']} "
            f"pending={row['pending_slots']} "
            f"stale={row.get('stale_slots', 0)} "
            f"jobs running={row['running_jobs']} "
            f"pending={row['pending_jobs']} "
            f"stale_jobs={row.get('stale_jobs', 0)} "
            f"failed_submissions={row['failed_submissions']} "
            f"cancelled_pending={row['cancelled_pending_jobs']} "
            f"idle_exits={row['idle_exits']}\n",
        )
    if jobs:
        sys.stdout.write("\nJobs:\n")
    for job in jobs:
        env_items = " ".join(
            f"{key}={value}" for key, value in sorted(job.get("redacted_env", {}).items())
        )
        sys.stdout.write(
            f"  {job.get('job_id') or '-'} "
            f"{job['environment']}/{job['pool_name']} "
            f"{job['state']} nodelist={job['nodelist']} "
            f"concurrency={job['requested_concurrency']}",
        )
        if env_items:
            sys.stdout.write(f" env={env_items}")
        pending_reason = job.get("pending_reason")
        if pending_reason:
            sys.stdout.write(f" reason={pending_reason}")
        sys.stdout.write("\n")
    return 0


def _gb10_workers_status(args: argparse.Namespace) -> int:
    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    url = f"{args.cp_url.rstrip('/')}/admin/gb10-worker-pools/status"
    params: dict[str, str] = {}
    if args.environment:
        params["environment"] = args.environment
    if args.pool_name:
        params["pool_name"] = args.pool_name
    try:
        if params:
            resp = httpx.get(
                url,
                headers={"Authorization": f"Bearer {admin_token}"},
                params=params,
                timeout=10.0,
            )
        else:
            resp = httpx.get(
                url,
                headers={"Authorization": f"Bearer {admin_token}"},
                timeout=10.0,
            )
    except httpx.RequestError as e:
        sys.stderr.write(f"error: could not reach CP at {url}: {e}\n")
        return 2

    if resp.status_code != 200:
        sys.stderr.write(
            f"error: CP returned {resp.status_code}: {resp.text}\n",
        )
        return 1

    data = resp.json()
    mismatches = _gb10_release_target_mismatches(
        data,
        release_image_tag=args.release_image_tag,
        release_env_config_version=args.release_env_config_version,
    )
    if args.format == "json":
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
        _print_gb10_release_target_mismatches(
            mismatches,
            release_image_tag=args.release_image_tag,
            release_env_config_version=args.release_env_config_version,
        )
        return 1 if mismatches else 0

    desired_states = data.get("desired_states", [])
    nodes = data.get("nodes", [])
    sys.stdout.write("GB10 worker lifecycle:\n")
    sys.stdout.write("Desired states:\n")
    if not desired_states:
        sys.stdout.write("  no desired states recorded\n")
    for row in desired_states:
        previous = row.get("previous_image_tag") or "-"
        sys.stdout.write(
            f"  {row['environment']}/{row['pool_name']} "
            f"image={row['image_tag']} "
            f"max={row['max_concurrent']} "
            f"env={row['env_config_version']} "
            f"previous={previous}\n",
        )
    sys.stdout.write("Nodes:\n")
    if not nodes:
        sys.stdout.write("  no node reports recorded\n")
    for node in nodes:
        result = node.get("last_apply_result") or "-"
        error = node.get("error_message") or "-"
        sys.stdout.write(
            f"  {node['hostname']} {node['environment']}/{node['pool_name']} "
            f"{node['apply_state']} "
            f"image={node.get('current_image_tag') or '-'}/"
            f"{node.get('desired_image_tag') or '-'} "
            f"max={node.get('current_max_concurrent') or '-'}/"
            f"{node.get('desired_max_concurrent') or '-'} "
            f"env={node.get('current_env_config_version') or '-'}/"
            f"{node.get('desired_env_config_version') or '-'} "
            f"result={result} error={error}\n",
        )
    _print_gb10_release_target_mismatches(
        mismatches,
        release_image_tag=args.release_image_tag,
        release_env_config_version=args.release_env_config_version,
    )
    return 1 if mismatches else 0


def _worker_pool_autoscaler_status(args: argparse.Namespace) -> int:
    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    url = f"{args.cp_url.rstrip('/')}/admin/worker-pool-autoscalers/status"
    try:
        resp = httpx.get(
            url,
            headers={"Authorization": f"Bearer {admin_token}"},
            timeout=10.0,
        )
    except httpx.RequestError as e:
        sys.stderr.write(f"error: could not reach CP at {url}: {e}\n")
        return 2

    if resp.status_code != 200:
        sys.stderr.write(
            f"error: CP returned {resp.status_code}: {resp.text}\n",
        )
        return 1

    data = resp.json()
    if args.format == "json":
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0

    policies = data.get("policies", [])
    sys.stdout.write("Worker-pool autoscalers:\n")
    if not policies:
        sys.stdout.write("  no autoscaler policies recorded\n")
        return 0
    for row in policies:
        error = row.get("last_error") or "-"
        blocked = row.get("last_blocked_reason") or "-"
        sys.stdout.write(
            f"  {row['environment']}/{row['pool_name']} "
            f"{row['actuator']} "
            f"enabled={row['enabled']} "
            f"min={row['min_slots']} max={row['max_slots']} "
            f"desired={row.get('last_desired_slots') or 0} "
            f"actual={row.get('last_actual_slots') or 0} "
            f"pending={row.get('last_pending_slots') or 0} "
            f"draining={row.get('last_draining_slots') or 0} "
            f"occupied={row.get('last_occupied_slots') or 0} "
            f"queued={row.get('last_queued_slots') or 0} "
            f"decision={row.get('last_decision') or '-'} "
            f"reason={row.get('last_decision_reason') or '-'} "
            f"blocked={blocked} error={error}\n",
        )
    return 0


def _parse_environment_state_vars(values: list[str]) -> dict[str, str]:
    variables: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise ValueError(f"--var must be KEY=VALUE, got {item!r}")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"--var key cannot be empty in {item!r}")
        variables[key] = value
    return variables


def _load_environment_state_profile_from_args(
    args: argparse.Namespace,
) -> EnvironmentStateProfile | None:
    from loom_cli.environment_state import (
        EnvironmentStateProfileError,
        load_environment_state_profile,
    )

    try:
        variables = _parse_environment_state_vars(args.var)
        return load_environment_state_profile(
            args.file,
            variables=variables,
            expected_environment=args.environment,
        )
    except (EnvironmentStateProfileError, ValueError) as e:
        sys.stderr.write(f"error: {e}\n")
        return None


def _format_environment_state_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return repr(value)


def _environment_state_label(profile: EnvironmentStateProfile) -> str:
    if profile.control_plane_environment == profile.environment:
        return profile.environment
    return (
        f"{profile.environment} "
        f"(CP environment {profile.control_plane_environment})"
    )


def _fetch_environment_state(
    *,
    cp_url: str,
    admin_token: str,
) -> tuple[int, dict[str, Any] | None]:
    headers = {"Authorization": f"Bearer {admin_token}"}
    base = cp_url.rstrip("/")
    try:
        autoscaler_resp = httpx.get(
            f"{base}/admin/worker-pool-autoscalers/status",
            headers=headers,
            timeout=10.0,
        )
        gb10_resp = httpx.get(
            f"{base}/admin/gb10-worker-pools/status",
            headers=headers,
            timeout=10.0,
        )
    except httpx.RequestError as e:
        sys.stderr.write(f"error: could not reach CP at {base}: {e}\n")
        return 2, None

    for name, resp in (
        ("worker-pool autoscaler status", autoscaler_resp),
        ("GB10 desired state status", gb10_resp),
    ):
        if resp.status_code != 200:
            sys.stderr.write(
                f"error: CP returned {resp.status_code} for {name}: {resp.text}\n",
            )
            return 1, None
    return 0, {
        "autoscaler_status": autoscaler_resp.json(),
        "gb10_status": gb10_resp.json(),
    }


def _environment_state_apply(args: argparse.Namespace) -> int:
    from loom_cli.environment_state import (
        autoscaler_policy_payload,
        gb10_desired_state_payload,
    )

    profile = _load_environment_state_profile_from_args(args)
    if profile is None:
        return 2
    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    headers = {"Authorization": f"Bearer {admin_token}"}
    base = args.cp_url.rstrip("/")
    applied: list[dict[str, str]] = []
    try:
        for policy in profile.autoscaler_policies:
            url = (
                f"{base}/admin/worker-pool-autoscaler-policies/"
                f"{policy['environment']}/{policy['pool_name']}"
            )
            resp = httpx.put(
                url,
                json=autoscaler_policy_payload(policy),
                headers=headers,
                timeout=10.0,
            )
            if resp.status_code != 200:
                sys.stderr.write(
                    f"error: CP returned {resp.status_code} for {url}: {resp.text}\n",
                )
                return 1
            applied.append({"kind": "worker_pool_autoscaler_policy", "url": url})

        for state in profile.gb10_desired_states:
            url = (
                f"{base}/admin/gb10-worker-pools/"
                f"{state['environment']}/{state['pool_name']}/desired-state"
            )
            resp = httpx.put(
                url,
                json=gb10_desired_state_payload(state),
                headers=headers,
                timeout=10.0,
            )
            if resp.status_code != 200:
                sys.stderr.write(
                    f"error: CP returned {resp.status_code} for {url}: {resp.text}\n",
                )
                return 1
            applied.append({"kind": "gb10_worker_pool_desired_state", "url": url})
    except httpx.RequestError as e:
        sys.stderr.write(f"error: could not reach CP at {base}: {e}\n")
        return 2

    if args.format == "json":
        json.dump(
            {
                "environment": profile.environment,
                "control_plane_environment": profile.control_plane_environment,
                "profile": str(args.file),
                "applied": applied,
                "catalog_provisioning": profile.catalog_provisioning,
            },
            sys.stdout,
            indent=2,
        )
        sys.stdout.write("\n")
        return 0

    sys.stdout.write(
        f"Applied environment state {_environment_state_label(profile)}: "
        f"{len(profile.autoscaler_policies)} autoscaler polic"
        f"{'y' if len(profile.autoscaler_policies) == 1 else 'ies'}, "
        f"{len(profile.gb10_desired_states)} GB10 desired state"
        f"{'' if len(profile.gb10_desired_states) == 1 else 's'}.\n",
    )
    if profile.catalog_provisioning.get("required"):
        command = profile.catalog_provisioning.get("command")
        if command:
            sys.stdout.write(f"Catalog provisioning gate: {command}\n")
    return 0


def _environment_state_check(args: argparse.Namespace) -> int:
    from loom_cli.environment_state import diff_environment_state

    profile = _load_environment_state_profile_from_args(args)
    if profile is None:
        return 2
    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    rc, live = _fetch_environment_state(
        cp_url=args.cp_url,
        admin_token=admin_token,
    )
    if rc != 0 or live is None:
        return rc

    drift = diff_environment_state(profile, live)
    if args.format == "json":
        json.dump(
            {
                "environment": profile.environment,
                "control_plane_environment": profile.control_plane_environment,
                "profile": str(args.file),
                "ok": not drift,
                "drift": [
                    {
                        "path": item.path,
                        "desired": item.desired,
                        "live": item.live,
                    }
                    for item in drift
                ],
                "catalog_provisioning": profile.catalog_provisioning,
            },
            sys.stdout,
            indent=2,
        )
        sys.stdout.write("\n")
        return 1 if drift else 0

    if drift:
        sys.stderr.write(
            f"Environment state drift for {_environment_state_label(profile)}: "
            f"{len(drift)} difference(s)\n",
        )
        for item in drift:
            sys.stderr.write(
                f"  {item.path}: desired={_format_environment_state_value(item.desired)} "
                f"live={_format_environment_state_value(item.live)}\n",
            )
        sys.stderr.write(
            "Apply the versioned desired state with "
            "`loom admin environment-state apply --file ...` after confirming "
            "the profile matches this rollout.\n",
        )
        return 1

    sys.stdout.write(
        f"Environment state {_environment_state_label(profile)} "
        "matches desired profile.\n",
    )
    if profile.catalog_provisioning.get("required"):
        command = profile.catalog_provisioning.get("command")
        if command:
            sys.stdout.write(f"Catalog provisioning gate: {command}\n")
    return 0


_KNOWN_TEAM_SCOPES = (
    "read:own",
    "submit",
    "providers:manage",
    "tokens:manage",
)
_KNOWN_TOKEN_TYPES = ("team",)
_HEX_8_PREFIX_RE = re.compile(r"^[0-9a-f]{8}$")
_DEFAULT_TEAM_SCOPES = ("read:own", "submit")


def _mint_team_token(args: argparse.Namespace) -> int:
    """POST to `loom_service` /api/v1/tokens. Uses the bearer from
    `loom auth login` and adds X-Loom-Admin-Actor when supplied."""
    try:
        cfg = require_logged_in()
    except NotLoggedInError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    body: dict[str, object] = {
        "name": args.name,
        "type": args.type,
        "scopes": list(args.scopes),
        "expires_in_days": args.expires_in_days,
    }
    if args.team_id is not None:
        body["team_id"] = args.team_id

    headers: dict[str, str] = {}
    if args.admin_actor is not None:
        headers["X-Loom-Admin-Actor"] = args.admin_actor

    try:
        with authed_client(cfg) as c:
            resp = c.post(
                "/api/v1/tokens",
                json=body,
                headers=headers or None,
            )
            data = assert_2xx(resp, action="mint team token")
    except HttpStatusError as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except httpx.RequestError as e:
        sys.stderr.write(
            f"error: could not reach {cfg.server_url}: {e}\n",
        )
        return 2

    if args.format == "json":
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
    else:
        sys.stdout.write(
            f"New {args.type} token minted.\n"
            f"  name:       {args.name}\n"
            f"  prefix:     {data['token_hash_prefix']}\n"
            f"  token:      {data['token']}\n"
            f"  expires_at: {data['expires_at']}\n",
        )
    return 0


def _revoke_team_token(args: argparse.Namespace) -> int:
    if not _HEX_8_PREFIX_RE.fullmatch(args.prefix):
        sys.stderr.write(
            f"error: prefix must be exactly 8 lowercase hex chars; got {args.prefix!r}\n",
        )
        return 2

    try:
        cfg = require_logged_in()
    except NotLoggedInError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    headers: dict[str, str] = {}
    if args.admin_actor is not None:
        headers["X-Loom-Admin-Actor"] = args.admin_actor

    try:
        with authed_client(cfg) as c:
            resp = c.delete(
                f"/api/v1/tokens/{args.prefix}",
                headers=headers or None,
            )
            assert_2xx(resp, action="revoke team token")
    except HttpStatusError as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except httpx.RequestError as e:
        sys.stderr.write(
            f"error: could not reach {cfg.server_url}: {e}\n",
        )
        return 2

    sys.stdout.write(
        f"Token with prefix {args.prefix!r} revoked.\n",
    )
    return 0


def _rotate_team_token(args: argparse.Namespace) -> int:
    """Mint a new team token + print the rollout checklist. Does NOT
    auto-revoke the old token — premature delete would break clients
    still using the old credential."""
    rc = _mint_team_token(args)
    if rc != 0:
        return rc
    if args.format != "json":
        sys.stdout.write(
            "\nRotation checklist:\n"
            "  1. Distribute the new token to its holders through a "
            "secure channel (1Password, signed email, etc).\n"
            "  2. Confirm clients are using the new token "
            "(check server logs for the new prefix).\n"
            "  3. Revoke the OLD token by its hash prefix:\n"
            "       loom admin tokens team revoke <OLD_PREFIX> "
            "[--admin-actor NAME]\n",
        )
    return 0


def _sync_yibuapi_rate_cards(args: argparse.Namespace) -> int:
    try:
        cfg = require_logged_in()
    except NotLoggedInError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    body: dict[str, object] = {}
    if args.source_url is not None:
        body["source_url"] = args.source_url
    if args.group != "default":
        body["group"] = args.group

    try:
        with authed_client(cfg) as c:
            resp = c.post("/api/v1/rate-cards/sync/yibuapi", json=body)
            data = assert_2xx(resp, action="sync YibuAPI rate card")
    except HttpStatusError as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except httpx.RequestError as e:
        sys.stderr.write(
            f"error: could not reach {cfg.server_url}: {e}\n",
        )
        return 2

    if args.format == "json":
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0

    sys.stdout.write(
        "Synced YibuAPI rate card.\n"
        f"  id:              {data.get('id', '-')}\n"
        f"  source_url:      {data.get('source_url', '-')}\n"
        f"  pricing_version: {data.get('pricing_version', '-')}\n"
        f"  entries:         {data.get('entry_count', 0)}\n"
        f"  skipped:         {data.get('skipped_model_count', 0)}\n",
    )
    return 0


def _scopes_argparse_type(value: str) -> list[str]:
    """Accept a comma-separated list of scopes. Rejects unknown
    scopes client-side to surface typos before the round-trip."""
    items = [s.strip() for s in value.split(",") if s.strip()]
    if not items:
        raise argparse.ArgumentTypeError("--scopes cannot be empty")
    unknown = [s for s in items if s not in _KNOWN_TEAM_SCOPES]
    if unknown:
        raise argparse.ArgumentTypeError(
            f"unknown scope(s) {unknown}; known: {list(_KNOWN_TEAM_SCOPES)}",
        )
    return items


def _add_team_mint_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--name",
        required=True,
        help="Human-readable token name shown in token lists and audit logs.",
    )
    p.add_argument(
        "--type",
        choices=_KNOWN_TOKEN_TYPES,
        default="team",
        help=(
            "Token type. Admin credentials are file-backed singleton secrets; "
            "use `loom service init-admin` or `loom service rotate-admin` "
            "for that lifecycle."
        ),
    )
    p.add_argument(
        "--team-id",
        default=None,
        help=(
            "UUID of the team this token grants access to. Required "
            "for --type team when called as an admin; defaults to "
            "the caller's team for non-admin callers."
        ),
    )
    p.add_argument(
        "--scopes",
        type=_scopes_argparse_type,
        default=list(_DEFAULT_TEAM_SCOPES),
        help=(
            "Comma-separated scope list. Known: "
            f"{', '.join(_KNOWN_TEAM_SCOPES)}. "
            f"Default: {','.join(_DEFAULT_TEAM_SCOPES)}."
        ),
    )
    p.add_argument(
        "--expires-in-days",
        type=int,
        default=90,
        help="Token lifetime in days (default: 90).",
    )
    p.add_argument(
        "--admin-actor",
        default=None,
        help=(
            "Sets `X-Loom-Admin-Actor`. Required when the logged-in "
            "bearer is an admin token (audit trail)."
        ),
    )
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )


def _add_common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--cp-url",
        default=_DEFAULT_CP_URL,
        help=(
            f"Control Plane base URL (default: {_DEFAULT_CP_URL}). The "
            f"CP admin surface is NOT public; port-forward in another "
            f"shell: kubectl port-forward "
            f"deploy/loom-control-plane 8080:8080"
        ),
    )
    p.add_argument(
        "--admin-token",
        default=_DEFAULT_ADMIN_TOKEN_SOURCE,
        help=(
            f"Admin token source. ONE of: env:VAR (read os.environ[VAR]), "
            f"file:PATH (read file content), or '-' (read stdin). "
            f"Default: {_DEFAULT_ADMIN_TOKEN_SOURCE!r}."
        ),
    )


def _generate_new_key() -> str:
    """Return a fresh base64-encoded 32-byte key."""
    import base64

    return base64.b64encode(os.urandom(32)).decode()


def _rewrap_secret_store(args: argparse.Namespace) -> int:
    """POST /api/v1/admin/secret-store/rewrap.

    2-stage online rotation protocol — see operator-runbook.md
    §Secret-store master-key rotation for the full procedure.
    """
    try:
        cfg = require_logged_in()
    except NotLoggedInError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    # Handle --generate-new-key: mint a key, print it + the kubectl
    # commands, but do NOT call the endpoint (the key isn't deployed yet).
    if getattr(args, "generate_new_key", False):
        new_key = _generate_new_key()
        sys.stdout.write(
            f"Generated new master key (keep this secret!):\n"
            f"  {new_key}\n\n"
            f"Next steps:\n"
            f"  1. Deploy new key as FALLBACK (existing rows still readable):\n"
            f"       # Read the current key first:\n"
            f"       OLD_KEY=$(kubectl get secret loom-secrets "
            f"-o jsonpath='{{.data.secret-store-master-key}}' | base64 -d)\n"
            f"       kubectl patch secret loom-secrets \\\n"
            f'         -p \'{{"stringData":{{"secret-store-master-keys":'
            f'"{new_key},${{OLD_KEY}}"}},"data":{{"secret-store-master-key":null}}}}\'\n'
            f"       kubectl rollout restart deploy/loom-service\n\n"
            f"  2. Run the rewrap walk:\n"
            f"       loom admin secret-store rewrap --new-key {new_key!r} "
            f"--admin-actor <your-name>\n\n"
            f"  3. Drop the old key (new key only):\n"
            f"       kubectl patch secret loom-secrets \\\n"
            f'         -p \'{{"stringData":{{"secret-store-master-keys":null,'
            f'"secret-store-master-key":"{new_key}"}}}}\n'
            f"       kubectl rollout restart deploy/loom-service\n",
        )
        return 0

    if getattr(args, "dry_run", False):
        # Dry run: list refs without rewrapping.
        sys.stdout.write(
            "[dry-run] Would call POST /api/v1/admin/secret-store/rewrap\n"
            "[dry-run] Use without --dry-run to execute.\n",
        )
        return 0

    new_key_b64 = getattr(args, "new_key", None)
    admin_actor = getattr(args, "admin_actor", None)

    body: dict[str, object] = {}
    if new_key_b64:
        body["new_master_key"] = new_key_b64

    headers: dict[str, str] = {}
    if admin_actor is not None:
        headers["X-Loom-Admin-Actor"] = admin_actor

    try:
        with authed_client(cfg) as c:
            resp = c.post(
                "/api/v1/admin/secret-store/rewrap",
                json=body,
                headers=headers or None,
                timeout=300.0,  # may take a while for large secrets tables
            )
            # 200 = all rewrapped; 207 = partial (some failed)
            if resp.status_code not in (200, 207):
                try:
                    detail = resp.json().get("detail", resp.text)
                except Exception:
                    detail = resp.text
                sys.stderr.write(
                    f"error: server returned {resp.status_code}: {detail}\n",
                )
                return 1
            data = resp.json()
    except httpx.RequestError as e:
        sys.stderr.write(
            f"error: could not reach {cfg.server_url}: {e}\n",
        )
        return 2

    rewrapped = data.get("rewrapped", 0)
    failed = data.get("failed", [])

    if args.format == "json":
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
    else:
        sys.stdout.write(f"Rewrapped {rewrapped} secret(s).\n")
        if failed:
            sys.stdout.write(
                f"\nFailed ({len(failed)}):\n",
            )
            for ref, err in failed:
                sys.stdout.write(f"  {ref}: {err}\n")
            sys.stdout.write(
                "\nReview failures above. Refs that failed are still encrypted with the OLD key.\n",
            )
        else:
            sys.stdout.write(
                "\nAll secrets now use the primary key.\n"
                "Next step: drop the fallback key from loom-secrets "
                "and restart loom-service:\n"
                "  kubectl patch secret loom-secrets \\\n"
                '    -p \'{"stringData":{"secret-store-master-keys":null,'
                '"secret-store-master-key":"<NEW_KEY>"}}\n'
                "  kubectl rollout restart deploy/loom-service\n",
            )

    return 1 if failed else 0


def dispatch(argv: list[str]) -> int:
    """Entry point invoked from `loom_cli.__main__` when `argv[0]` is
    `admin`. Returns the process exit code."""
    parser = argparse.ArgumentParser(
        prog="loom admin",
        description=(
            "Operator-only admin operations. Subcommands: "
            "`tokens` (worker/team token rotation), "
            "`rate-cards` (pricing catalog sync), and "
            "`secret-store` (master-key rotation walker)."
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
        dest="worker_op",
        required=True,
    )

    p_mint = worker_sub.add_parser(
        "mint",
        help="Issue a new worker token. Prints the raw token + prefix.",
    )
    _add_common_args(p_mint)
    p_mint.add_argument(
        "--expires-in-days",
        type=int,
        default=_DEFAULT_EXPIRES_DAYS,
        help=(
            f"Token lifetime in days "
            f"(default: {_DEFAULT_EXPIRES_DAYS}). Pass 0 to omit the "
            f"expires_in_days field (server keeps default)."
        ),
    )
    p_mint.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
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
        "--expires-in-days",
        type=int,
        default=_DEFAULT_EXPIRES_DAYS,
        help=f"Token lifetime in days (default: {_DEFAULT_EXPIRES_DAYS}).",
    )
    p_rotate.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    p_rotate.set_defaults(handler=_rotate_worker_token)

    p_slurm = sub.add_parser(
        "slurm-workers",
        help="Inspect elastic Slurm worker capacity recorded by the CP.",
    )
    slurm_sub = p_slurm.add_subparsers(dest="slurm_op", required=True)
    p_slurm_status = slurm_sub.add_parser(
        "status",
        help="Show recorded Slurm worker jobs and per-pool capacity.",
    )
    _add_common_args(p_slurm_status)
    p_slurm_status.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    p_slurm_status.set_defaults(handler=_slurm_workers_status)

    p_gb10 = sub.add_parser(
        "gb10-workers",
        help="Inspect GB10 Docker Compose worker lifecycle status.",
    )
    gb10_sub = p_gb10.add_subparsers(dest="gb10_op", required=True)
    p_gb10_status = gb10_sub.add_parser(
        "status",
        help="Show GB10 desired state and per-host rollout status.",
    )
    _add_common_args(p_gb10_status)
    p_gb10_status.add_argument("--environment", default=None)
    p_gb10_status.add_argument("--pool-name", default=None)
    p_gb10_status.add_argument(
        "--release-image-tag",
        default=None,
        help=(
            "Fail if active GB10 nodes or desired state have not converged "
            "to this release image tag."
        ),
    )
    p_gb10_status.add_argument(
        "--release-env-config-version",
        default=None,
        help=(
            "Fail if active GB10 nodes or desired state have not converged "
            "to this env config version."
        ),
    )
    p_gb10_status.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    p_gb10_status.set_defaults(handler=_gb10_workers_status)

    p_worker_pools = sub.add_parser(
        "worker-pools",
        help="Inspect worker-pool autoscaler policy and decision state.",
    )
    worker_pools_sub = p_worker_pools.add_subparsers(
        dest="worker_pools_op",
        required=True,
    )
    p_autoscaler = worker_pools_sub.add_parser(
        "autoscaler",
        help="Worker-pool autoscaler operations.",
    )
    autoscaler_sub = p_autoscaler.add_subparsers(
        dest="autoscaler_op",
        required=True,
    )
    p_autoscaler_status = autoscaler_sub.add_parser(
        "status",
        help="Show worker-pool autoscaler desired/actual/drain state.",
    )
    _add_common_args(p_autoscaler_status)
    p_autoscaler_status.add_argument(
        "--format", choices=["text", "json"], default="text",
        help="Output format.",
    )
    p_autoscaler_status.set_defaults(handler=_worker_pool_autoscaler_status)

    p_env_state = sub.add_parser(
        "environment-state",
        help=(
            "Apply or check versioned environment desired state that lives "
            "outside Kubernetes manifests."
        ),
    )
    env_state_sub = p_env_state.add_subparsers(
        dest="environment_state_op",
        required=True,
    )

    def _add_environment_state_args(p: argparse.ArgumentParser) -> None:
        _add_common_args(p)
        p.add_argument(
            "--file",
            type=Path,
            required=True,
            help=(
                "TOML desired-state profile, for example "
                "deploy/environment-state/public-beta.toml."
            ),
        )
        p.add_argument(
            "--environment",
            required=True,
            help="Expected profile environment; rejects accidental cross-env apply.",
        )
        p.add_argument(
            "--var",
            action="append",
            default=[],
            metavar="KEY=VALUE",
            help=(
                "Template variable for ${KEY} placeholders in the profile. "
                "Repeat for rollout-specific values such as IMAGE_TAG."
            ),
        )
        p.add_argument(
            "--format",
            choices=["text", "json"],
            default="text",
            help="Output format.",
        )

    p_env_state_apply = env_state_sub.add_parser(
        "apply",
        help="Idempotently apply worker-pool and GB10 desired state through CP admin APIs.",
    )
    _add_environment_state_args(p_env_state_apply)
    p_env_state_apply.set_defaults(handler=_environment_state_apply)

    p_env_state_check = env_state_sub.add_parser(
        "check",
        help="Fail if live CP-backed environment state drifts from a desired profile.",
    )
    _add_environment_state_args(p_env_state_check)
    p_env_state_check.set_defaults(handler=_environment_state_check)

    p_team = tok_sub.add_parser(
        "team",
        help=(
            "Team-token operations via `loom_service`'s "
            "`/api/v1/tokens` route. Uses the server + bearer from "
            "`loom auth login`."
        ),
    )
    team_sub = p_team.add_subparsers(dest="team_op", required=True)

    p_team_mint = team_sub.add_parser(
        "mint",
        help="Issue a new team token. Admin caller is recorded in audit.",
    )
    _add_team_mint_args(p_team_mint)
    p_team_mint.set_defaults(handler=_mint_team_token)

    p_team_revoke = team_sub.add_parser(
        "revoke",
        help="Revoke a team token by its 8-hex-char prefix.",
    )
    p_team_revoke.add_argument(
        "prefix",
        help="Exactly 8 lowercase hex chars from token_hash_prefix.",
    )
    p_team_revoke.add_argument(
        "--admin-actor",
        default=None,
        help=(
            "Sets `X-Loom-Admin-Actor`. Required when the logged-in "
            "bearer is an admin token (audit trail). Ignored when "
            "the bearer is a team token."
        ),
    )
    p_team_revoke.set_defaults(handler=_revoke_team_token)

    p_team_rotate = team_sub.add_parser(
        "rotate",
        help=(
            "Mint a new team token + print the rollout procedure. "
            "Does NOT revoke the old token automatically."
        ),
    )
    _add_team_mint_args(p_team_rotate)
    p_team_rotate.set_defaults(handler=_rotate_team_token)

    # ── rate-cards subgroup ───────────────────────────────────────────
    p_rate_cards = sub.add_parser(
        "rate-cards",
        help="Rate-card catalog operations via the public service API.",
    )
    rate_cards_sub = p_rate_cards.add_subparsers(
        dest="rate_cards_op",
        required=True,
    )
    p_rc_sync_yibuapi = rate_cards_sub.add_parser(
        "sync-yibuapi",
        help="Sync YibuAPI official pricing into the service rate-card store.",
    )
    p_rc_sync_yibuapi.add_argument(
        "--source-url",
        default=None,
        help=(
            "Override the YibuAPI pricing endpoint. Defaults to the "
            "server's configured official pricing URL."
        ),
    )
    p_rc_sync_yibuapi.add_argument(
        "--group",
        default="default",
        help="YibuAPI group ratio to use when converting prices.",
    )
    p_rc_sync_yibuapi.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    p_rc_sync_yibuapi.set_defaults(handler=_sync_yibuapi_rate_cards)

    # ── secret-store subgroup ──────────────────────────────────────────
    p_ss = sub.add_parser(
        "secret-store",
        help=("SecretStore master-key rotation operations. See `loom admin secret-store --help`."),
    )
    ss_sub = p_ss.add_subparsers(dest="ss_op", required=True)

    p_ss_rewrap = ss_sub.add_parser(
        "rewrap",
        help=(
            "Walk all SecretStore refs and re-encrypt with the primary "
            "master key. Part of the online master-key rotation protocol. "
            "Run AFTER deploying the new key as a fallback in "
            "LOOM_SECRET_STORE_MASTER_KEYS."
        ),
    )
    p_ss_rewrap.add_argument(
        "--new-key",
        dest="new_key",
        default=None,
        help=(
            "Base64-encoded 32-byte key to rewrap to. Overrides the "
            "PRIMARY key in LOOM_SECRET_STORE_MASTER_KEYS. Normally "
            "omit this — the server uses the deployed primary."
        ),
    )
    p_ss_rewrap.add_argument(
        "--generate-new-key",
        dest="generate_new_key",
        action="store_true",
        default=False,
        help=(
            "Mint a fresh key, print it, and output the kubectl patch "
            "commands. Does NOT call the rewrap endpoint. Use this to "
            "start the rotation workflow."
        ),
    )
    p_ss_rewrap.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=False,
        help="Print what would be done without calling the endpoint.",
    )
    p_ss_rewrap.add_argument(
        "--admin-actor",
        default=None,
        help=(
            "Sets `X-Loom-Admin-Actor`. Required when the logged-in "
            "bearer is an admin token (audit trail)."
        ),
    )
    p_ss_rewrap.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    p_ss_rewrap.set_defaults(handler=_rewrap_secret_store)

    args = parser.parse_args(argv)
    return cast(int, args.handler(args))
