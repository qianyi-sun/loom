"""`loom admin` — operator-only admin operations.

Subcommands:

- ``loom admin tokens worker {mint,revoke,rotate}`` — worker-token
  rotation via the Control Plane's admin surface.
- ``loom admin tokens team {mint,revoke,rotate}`` — legacy team-token
  rotation via loom_service's /api/v1/tokens route.
- ``loom admin env-diagnostics`` — redacted runtime environment inspection
  for deploy/debug evidence without raw secret values.
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
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import httpx

from loom.security.redaction import RedactedEnvironmentEntry, redact_environment_mapping
from loom_cli.cluster_backup_guard import is_protected_environment
from loom_cli.gb10_release_gate import gb10_release_target_mismatches
from loom_cli.rollout_lock import (
    DEFAULT_ROLLOUT_LOCK_TTL_SECONDS,
    RolloutAttribution,
    RolloutLease,
    RolloutLeaseError,
    RolloutLeaseManager,
    default_rollout_lock_dir,
    rollout_owner_id,
)
from loom_cli.rollout_lock_cli import (
    BROKER_LOCK_OPTIONS as _BROKER_LOCK_OPTIONS,
)
from loom_cli.rollout_lock_cli import (
    EXPLICIT_ROLLOUT_LOCK_OPTIONS_ATTR as _EXPLICIT_ROLLOUT_LOCK_OPTIONS_ATTR,
)
from loom_cli.rollout_lock_cli import (
    add_rollout_lock_args as _add_rollout_lock_args,
)
from loom_cli.rollout_lock_cli import (
    fixed_rollout_lock_evidence_path as _fixed_rollout_lock_evidence_path,
)
from loom_cli.rollout_lock_cli import (
    load_broker_rollout_envelope as _load_broker_rollout_envelope,
)
from loom_cli.rollout_lock_cli import (
    require_real_directory as _require_real_directory,
)
from loom_cli.rollout_lock_cli import (
    require_real_file as _require_real_file,
)
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

if TYPE_CHECKING:
    from loom_cli.environment_state import EnvironmentStateProfile

# Same constraint as the CP route: prefix must be hex, 4-64 chars.
# Catching this client-side avoids a round-trip just to hit the 400.
_HEX_PREFIX_RE = re.compile(r"^[0-9a-f]{4,64}$")

_DEFAULT_CP_URL = "http://localhost:8080"
_DEFAULT_EXPIRES_DAYS = 365
_DEFAULT_ADMIN_TOKEN_SOURCE = "env:LOOM_ADMIN_TOKEN"
_DEFAULT_ENV_DIAGNOSTIC_PREFIX = "LOOM_"


@dataclass(frozen=True, slots=True)
class _EnvironmentStateBrokerBinding:
    config: Any
    envelope: Any
    evidence_path: Path
    attribution: RolloutAttribution


def _env_diagnostic_value(entry: RedactedEnvironmentEntry) -> str:
    if not entry.sensitive:
        return entry.value
    if entry.fingerprint is None:
        return "[REDACTED]"
    length = entry.length if entry.length is not None else 0
    return f"[REDACTED {entry.fingerprint} len={length}]"


def _env_diagnostics(args: argparse.Namespace) -> int:
    prefixes = tuple(args.prefix or (_DEFAULT_ENV_DIAGNOSTIC_PREFIX,))
    entries = redact_environment_mapping(os.environ, prefixes=prefixes)
    if args.format == "json":
        json.dump(
            {
                "prefixes": list(prefixes),
                "entries": [entry.to_json() for entry in entries],
            },
            sys.stdout,
            indent=2,
        )
        sys.stdout.write("\n")
        return 0
    if args.format == "markdown":
        sys.stdout.write("| name | kind | value |\n")
        sys.stdout.write("| --- | --- | --- |\n")
        for entry in entries:
            kind = "sensitive" if entry.sensitive else "value"
            sys.stdout.write(f"| {entry.name} | {kind} | {_env_diagnostic_value(entry)} |\n")
        return 0
    for entry in entries:
        sys.stdout.write(f"{entry.name}={_env_diagnostic_value(entry)}\n")
    return 0


def _expected_broker_environment_profile(config: Any, envelope: Any) -> Path:
    from loom_cli.cluster_config import load_cluster_config

    rollout_dir = Path(config.rollout_root) / "rollouts" / str(envelope.rollout_id)
    candidate_root = rollout_dir / "01-worktree" / "src"
    _require_real_directory(candidate_root, label="broker candidate worktree")
    try:
        relative_config = Path(config.cluster_config_path).relative_to(config.runner_repo)
    except ValueError as exc:
        raise ValueError("configured staging cluster profile is outside the runner repo") from exc
    candidate_config = candidate_root / relative_config
    _require_real_file(candidate_config, label="broker candidate cluster config")
    cluster_config = load_cluster_config(candidate_config)
    profile_value = str(cluster_config.env_state_profile or "").strip()
    if not profile_value:
        raise ValueError("broker candidate cluster config has no environment-state profile")
    profile = Path(profile_value)
    if profile.is_absolute():
        expected = profile
    else:
        expected = Path(os.path.normpath(candidate_config.parent / profile))
    try:
        expected.relative_to(candidate_root)
    except ValueError as exc:
        raise ValueError("broker environment-state profile escapes candidate worktree") from exc
    _require_real_file(expected, label="broker environment-state profile")
    return expected


def _validate_broker_environment_state_inputs(
    args: argparse.Namespace,
    config: Any,
    envelope: Any,
    *,
    operation: str,
) -> Path:
    if envelope.environment != "staging" or args.environment != envelope.environment:
        raise ValueError("environment-state target does not match broker envelope")
    if args.cp_url != envelope.cp_url:
        raise ValueError("environment-state CP URL does not match broker envelope")
    if args.admin_token != envelope.admin_token_source:
        raise ValueError("environment-state admin token source does not match broker envelope")
    if args.expect_admin_token_fingerprint != envelope.expect_admin_token_fingerprint:
        raise ValueError("environment-state token fingerprint does not match broker envelope")
    if operation == "check" and args.worker_token != envelope.worker_token_source:
        raise ValueError("environment-state worker token source does not match broker envelope")
    expected_profile = _expected_broker_environment_profile(config, envelope)
    if Path(args.file) != expected_profile:
        raise ValueError("environment-state profile path does not match broker rollout")
    if list(args.var) != [
        f"IMAGE_TAG={envelope.image_tag}",
        f"ENV_CONFIG_VERSION={envelope.image_tag}",
        f"GIT_SHA={envelope.resolved_sha}",
    ]:
        raise ValueError("environment-state release variables do not match broker envelope")
    expected_format = "json" if operation == "check" else "text"
    if args.format != expected_format:
        raise ValueError("environment-state output format does not match broker rollout")
    return _fixed_rollout_lock_evidence_path(
        config,
        envelope,
        step_directory="11-env-state",
    )


def _validate_broker_environment_state_profile(
    profile: EnvironmentStateProfile,
    envelope: Any,
) -> None:
    if (
        profile.environment != envelope.environment
        or profile.control_plane_environment != envelope.environment
    ):
        raise ValueError("environment-state profile targets do not match broker envelope")


def _reject_broker_lock_overrides_before_profile(args: argparse.Namespace) -> None:
    if getattr(args, "rollout_request_envelope", None) is None:
        return
    explicit = set(getattr(args, _EXPLICIT_ROLLOUT_LOCK_OPTIONS_ATTR, ()))
    if explicit & _BROKER_LOCK_OPTIONS:
        raise ValueError("manual rollout lock overrides are forbidden in broker mode")


def _prepare_environment_state_broker_binding(
    args: argparse.Namespace,
    *,
    operation: str,
) -> _EnvironmentStateBrokerBinding | None:
    envelope_path = getattr(args, "rollout_request_envelope", None)
    if envelope_path is None:
        if args.environment == "staging" and is_protected_environment(
            environment=args.environment,
            namespace=args.environment,
        ):
            raise ValueError(
                "broker-created request envelope is required for staging environment-state"
            )
        return None
    _reject_broker_lock_overrides_before_profile(args)
    config, envelope = _load_broker_rollout_envelope(Path(envelope_path))
    evidence_path = _validate_broker_environment_state_inputs(
        args,
        config,
        envelope,
        operation=operation,
    )
    return _EnvironmentStateBrokerBinding(
        config=config,
        envelope=envelope,
        evidence_path=evidence_path,
        attribution=RolloutAttribution(
            request_id=envelope.request_id,
            initiating_operator=envelope.initiating_operator,
            initiating_uid=envelope.initiating_uid,
            attempt_number=envelope.attempt_number,
            attempt_operator=envelope.attempt_operator,
            attempt_uid=envelope.attempt_uid,
        ),
    )


def _protected_environment_state_target(
    args: argparse.Namespace,
    profile: EnvironmentStateProfile,
) -> str | None:
    protected_targets: list[str] = []
    for target in dict.fromkeys(
        (
            str(args.environment),
            profile.environment,
            profile.control_plane_environment,
        )
    ):
        if is_protected_environment(environment=target, namespace=target):
            protected_targets.append(target)
    if "staging" in protected_targets:
        return "staging"
    if "production" in protected_targets:
        return "production"
    return None


def _acquire_environment_state_rollout_lock(
    args: argparse.Namespace,
    *,
    operation: str,
    profile: EnvironmentStateProfile,
    broker_binding: _EnvironmentStateBrokerBinding | None,
) -> RolloutLease | None:
    environment = _protected_environment_state_target(args, profile)
    if environment is None:
        return None
    if environment == "staging":
        if broker_binding is None:
            raise ValueError(
                "broker-created request envelope is required for staging environment-state"
            )
        _validate_broker_environment_state_profile(
            profile,
            broker_binding.envelope,
        )
    manager = RolloutLeaseManager(
        Path(broker_binding.config.runtime_root) / "mutation-locks"
        if broker_binding is not None
        else args.rollout_lock_dir or default_rollout_lock_dir()
    )
    try:
        lease = manager.acquire(
            environment=environment,
            owner_id=(
                broker_binding.envelope.rollout_id
                if broker_binding is not None
                else rollout_owner_id(environment, args.rollout_id)
            ),
            ttl_seconds=(
                DEFAULT_ROLLOUT_LOCK_TTL_SECONDS
                if broker_binding is not None
                else args.rollout_lock_ttl_seconds
            ),
            command=[
                "loom",
                "admin",
                "environment-state",
                operation,
                "--environment",
                args.environment,
                "--file",
                str(args.file),
            ],
            evidence_path=(
                broker_binding.evidence_path
                if broker_binding is not None
                else args.rollout_lock_evidence
            ),
            force=False if broker_binding is not None else args.force_rollout_lock,
            attribution=(broker_binding.attribution if broker_binding is not None else None),
        )
    except (RolloutLeaseError, ValueError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        diagnostic = getattr(exc, "diagnostic", None)
        if isinstance(diagnostic, dict):
            sys.stderr.write(
                "rollout lock diagnostic: "
                + json.dumps(diagnostic, sort_keys=True)
                + "\n",
            )
        raise
    sys.stderr.write(
        f"Acquired rollout mutation lease for {environment}: {lease.owner_id}\n",
    )
    return lease


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


def _secret_fingerprint(value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
    return f"sha256:{digest} len={len(value)}"


def _validate_expected_admin_token_fingerprint(
    args: argparse.Namespace,
    admin_token: str,
) -> bool:
    expected = getattr(args, "expect_admin_token_fingerprint", None)
    if expected is None:
        return True
    live = _secret_fingerprint(admin_token)
    if live == expected:
        return True
    sys.stderr.write(
        "admin_token_fingerprint drift: "
        f"desired={expected!r} live={live!r}. "
        "Resolve the protected-environment admin token source before "
        "running environment-state apply/check.\n",
    )
    return False


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
    elif args.show_secret:
        sys.stdout.write(
            f"New worker token minted.\n"
            f"  prefix: {data['token_hash_prefix']}\n"
            f"  token:  {data['token']}\n"
            f"\nNext: update the `worker-token` key in `loom-secrets` "
            f"and restart `deploy/loom-worker`.\n",
        )
    else:
        sys.stdout.write(
            f"New worker token minted.\n"
            f"  prefix: {data['token_hash_prefix']}\n"
            f"\nThe raw token was NOT printed (terminal scrollback risk).\n"
            f"Pipe it straight into the secret store without exposing it\n"
            f"via shell history or `ps`:\n"
            f"\n"
            f"  loom admin tokens worker mint --format json | jq -r .token \\\n"
            f"    | kubectl create secret generic loom-secrets \\\n"
            f"        --from-file=worker-token=/dev/stdin \\\n"
            f"        --dry-run=client -o yaml | kubectl apply -f -\n"
            f"\n"
            f"Or re-run with --show-secret to print the raw token to stdout.\n",
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
        if args.show_secret:
            install_step = (
                "  1. Install the new token into `loom-secrets` without\n"
                "     exposing it via shell history or `ps`:\n"
                "       kubectl create secret generic loom-secrets \\\n"
                '         --from-literal=worker-token="$NEW_TOKEN" \\\n'
                "         --dry-run=client -o yaml | kubectl apply -f -\n"
                "     (Or use the pipe-stdin form from `--format json`.)\n"
            )
        else:
            install_step = (
                "  1. The new token was NOT captured. To proceed without\n"
                "     orphaning it, re-run rotate piping straight into\n"
                "     the secret store, then revoke the prefix printed\n"
                "     above:\n"
                "       loom admin tokens worker rotate --format json \\\n"
                "         | jq -r .token \\\n"
                "         | kubectl create secret generic loom-secrets \\\n"
                "             --from-file=worker-token=/dev/stdin \\\n"
                "             --dry-run=client -o yaml \\\n"
                "         | kubectl apply -f -\n"
                "     Then revoke the prefix above so it doesn't linger.\n"
            )
        sys.stdout.write(
            "\nRotation checklist:\n"
            + install_step
            + "  2. Distribute the same token to attached remote-worker env files\n"
            + "     (GB10/OLDLAB) without printing it, then restart those pools.\n"
            + "  3. Restart in-cluster workers:\n"
            + "       kubectl rollout restart deploy/loom-worker\n"
            + "  4. Verify in-cluster and remote workers re-register (no 401s),\n"
            + "     then run environment-state check with --worker-token.\n"
            + "  5. Revoke the OLD token by its hash prefix:\n"
            + "       loom admin tokens worker revoke <OLD_PREFIX>\n",
        )
    return 0


def _ensure_smoke_user(args: argparse.Namespace) -> int:
    """Provision the deployment-managed headless smoke-user credential.

    Idempotently ensures a dedicated non-human ``loom-smoke`` User + Team
    and mints a fresh user-owned ``submit`` token so the release-gate /
    operator trajectory smoke can submit ``oracle × gb10-smoke`` without a
    human login (see loom_cli.smoke_credential). Writes directly to the
    target service DB — run it after migrations during a deploy, then pipe
    the token straight into the secret store.
    """
    from loom_cli.smoke_credential import ensure_smoke_user_credential

    db_url = args.db_url
    if not db_url:
        sys.stderr.write(
            "error: no database URL. Pass --db-url or set LOOM_DB_URL "
            "(then LOOM_SVC_DB_URL) in the environment.\n",
        )
        return 2
    try:
        cred = ensure_smoke_user_credential(
            db_url,
            username=args.username,
            team_name=args.team,
            ttl_days=args.expires_in_days,
            revoke_prior=not args.keep_prior,
        )
    except Exception as e:  # surface any provisioning failure to the operator
        sys.stderr.write(f"error: could not provision smoke user: {e}\n")
        return 1

    if args.format == "json":
        json.dump(
            {
                "token": cred.raw_token,
                "token_hash_prefix": cred.token_hash_prefix,
                "username": cred.username,
                "team": cred.team_name,
                "user_id": str(cred.user_id),
                "team_id": str(cred.team_id),
                "expires_at": (
                    cred.expires_at.isoformat() if cred.expires_at else None
                ),
                "rotated_prior": cred.rotated_prior,
            },
            sys.stdout,
            indent=2,
        )
        sys.stdout.write("\n")
    elif args.show_secret:
        sys.stdout.write(
            f"Smoke-user credential provisioned.\n"
            f"  username: {cred.username}\n"
            f"  team:     {cred.team_name}\n"
            f"  prefix:   {cred.token_hash_prefix}\n"
            f"  token:    {cred.raw_token}\n"
            f"  revoked prior tokens: {cred.rotated_prior}\n",
        )
    else:
        sys.stdout.write(
            f"Smoke-user credential provisioned "
            f"(prefix {cred.token_hash_prefix}, "
            f"revoked {cred.rotated_prior} prior).\n"
            f"The raw token was NOT printed. Pipe it straight into the\n"
            f"secret store without exposing it via shell history or `ps`:\n"
            f"\n"
            f"  loom admin ensure-smoke-user --format json | jq -r .token \\\n"
            f"    | kubectl create secret generic loom-secrets \\\n"
            f"        --from-file=smoke-api-token=/dev/stdin \\\n"
            f"        --dry-run=client -o yaml | kubectl apply -f -\n"
            f"\n"
            f"Or re-run with --show-secret to print the raw token to stdout.\n",
        )
    return 0


def _ensure_batch_runner_token(args: argparse.Namespace) -> int:
    """Provision the deployment-managed batch-runner control-plane token.

    Mints the ``submit:batch`` token loom-service uses to fan batches out
    to the control-plane's ``POST /trials`` (see
    loom_cli.smoke_credential.ensure_batch_runner_token). A fresh/cutover
    DB has no valid one, so batches 401 and never dispatch. Writes directly
    to the target DB — run after migrations, then pipe the token into
    loom-secrets/batch-runner-cp-token and restart loom-service.
    """
    from loom_cli.smoke_credential import ensure_batch_runner_token

    db_url = args.db_url
    if not db_url:
        sys.stderr.write(
            "error: no database URL. Pass --db-url or set LOOM_DB_URL "
            "(then LOOM_SVC_DB_URL) in the environment.\n",
        )
        return 2
    try:
        tok = ensure_batch_runner_token(
            db_url,
            ttl_days=args.expires_in_days,
            revoke_prior=not args.keep_prior,
        )
    except Exception as e:  # surface any provisioning failure to the operator
        sys.stderr.write(f"error: could not provision batch-runner token: {e}\n")
        return 1

    if args.format == "json":
        json.dump(
            {
                "token": tok.raw_token,
                "token_hash_prefix": tok.token_hash_prefix,
                "expires_at": (
                    tok.expires_at.isoformat() if tok.expires_at else None
                ),
                "rotated_prior": tok.rotated_prior,
            },
            sys.stdout,
            indent=2,
        )
        sys.stdout.write("\n")
    elif args.show_secret:
        sys.stdout.write(
            f"Batch-runner token provisioned.\n"
            f"  prefix: {tok.token_hash_prefix}\n"
            f"  token:  {tok.raw_token}\n"
            f"  revoked prior tokens: {tok.rotated_prior}\n",
        )
    else:
        sys.stdout.write(
            f"Batch-runner token provisioned "
            f"(prefix {tok.token_hash_prefix}, revoked {tok.rotated_prior} prior).\n"
            f"The raw token was NOT printed. Pipe it into the secret store:\n"
            f"\n"
            f"  loom admin ensure-batch-runner-token --format json | jq -r .token \\\n"
            f"    | kubectl create secret generic loom-secrets \\\n"
            f"        --from-file=batch-runner-cp-token=/dev/stdin \\\n"
            f"        --dry-run=client -o yaml | kubectl apply -f -\n"
            f"  # then: kubectl rollout restart deploy/loom-service\n"
            f"\n"
            f"Or re-run with --show-secret to print the raw token to stdout.\n",
        )
    return 0


def _ensure_dev_worker_token(args: argparse.Namespace) -> int:
    """Seed the fixed dev/smoke worker token so the in-cluster loom-worker
    pods authenticate at `/workers/register` with no mint/patch/restart.

    LOCAL/DEV ONLY — installs a guessable, well-known worker credential
    (the same throwaway value `bootstrap-secrets --smoke-defaults` writes
    into `loom-secrets/worker-token`). Writes directly to the target DB;
    run after migrations. Idempotent (get-or-create by token hash).
    """
    from loom_cli.smoke_credential import ensure_dev_worker_token

    db_url = args.db_url
    if not db_url:
        sys.stderr.write(
            "error: no database URL. Pass --db-url or set LOOM_DB_URL "
            "(then LOOM_CP_DB_URL) in the environment.\n",
        )
        return 2
    try:
        res = ensure_dev_worker_token(db_url)
    except Exception as e:  # surface any provisioning failure to the operator
        sys.stderr.write(f"error: could not seed dev worker token: {e}\n")
        return 1

    if args.format == "json":
        json.dump(
            {"token_hash_prefix": res.token_hash_prefix, "created": res.created},
            sys.stdout,
            indent=2,
        )
        sys.stdout.write("\n")
    else:
        state = "seeded" if res.created else "already present"
        sys.stdout.write(
            f"Dev worker token {state} (prefix {res.token_hash_prefix}).\n"
            f"loom-worker pods carrying the --smoke-defaults worker-token now "
            f"register; crash-looping workers recover on their next retry.\n",
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
    if not _validate_expected_admin_token_fingerprint(args, admin_token):
        return 1

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
    mismatches = gb10_release_target_mismatches(
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
        source = node.get("source_git_commit")
        source_short = source[:12] if isinstance(source, str) else "-"
        source_dirty = node.get("source_git_dirty")
        sys.stdout.write(
            f"  {node['hostname']} {node['environment']}/{node['pool_name']} "
            f"{node['apply_state']} "
            f"image={node.get('current_image_tag') or '-'}/"
            f"{node.get('desired_image_tag') or '-'} "
            f"max={node.get('current_max_concurrent') or '-'}/"
            f"{node.get('desired_max_concurrent') or '-'} "
            f"env={node.get('current_env_config_version') or '-'}/"
            f"{node.get('desired_env_config_version') or '-'} "
            f"source={source_short} "
            f"dirty={source_dirty if source_dirty is not None else '-'} "
            f"dir={node.get('compose_project_dir') or '-'} "
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
        details = _format_autoscaler_blocked_details(
            row.get("last_blocked_details"),
        )
        details_text = f" details={details}" if details else ""
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
            f"blocked={blocked}{details_text} error={error}\n",
        )
    return 0


def _format_autoscaler_blocked_details(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    node_exclusions = value.get("node_exclusions")
    if isinstance(node_exclusions, list):
        parts: list[str] = []
        for item in node_exclusions:
            if not isinstance(item, dict):
                continue
            hostname = str(item.get("hostname") or "").strip()
            reason = str(item.get("reason") or "").strip()
            if hostname and reason:
                parts.append(f"{hostname}:{reason}")
        if parts:
            return ",".join(parts)
    reason = str(value.get("reason") or "").strip()
    return reason


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
        slurm_resp = httpx.get(
            f"{base}/admin/slurm-worker-jobs/status",
            headers=headers,
            timeout=10.0,
        )
    except httpx.RequestError as e:
        sys.stderr.write(f"error: could not reach CP at {base}: {e}\n")
        return 2, None

    for name, resp in (
        ("worker-pool autoscaler status", autoscaler_resp),
        ("GB10 desired state status", gb10_resp),
        ("Slurm worker job status", slurm_resp),
    ):
        if resp.status_code != 200:
            sys.stderr.write(
                f"error: CP returned {resp.status_code} for {name}: {resp.text}\n",
            )
            return 1, None
    return 0, {
        "autoscaler_status": autoscaler_resp.json(),
        "gb10_status": gb10_resp.json(),
        "slurm_status": slurm_resp.json(),
    }


def _environment_state_apply(args: argparse.Namespace) -> int:
    try:
        broker_binding = _prepare_environment_state_broker_binding(
            args,
            operation="apply",
        )
    except ValueError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    profile = _load_environment_state_profile_from_args(args)
    if profile is None:
        return 2
    try:
        lease = _acquire_environment_state_rollout_lock(
            args,
            operation="apply",
            profile=profile,
            broker_binding=broker_binding,
        )
    except RolloutLeaseError:
        return 1
    except ValueError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    rc = 1
    try:
        rc = _environment_state_apply_impl(args, profile)
        return rc
    finally:
        if lease is not None:
            lease.release(status="released" if rc == 0 else "failed")


def _environment_state_apply_impl(
    args: argparse.Namespace,
    profile: EnvironmentStateProfile,
) -> int:
    from loom_cli.environment_state import (
        apply_external_slurm_autoscaler_supervisors,
        autoscaler_policy_payload,
        gb10_desired_state_payload,
    )

    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2
    if not _validate_expected_admin_token_fingerprint(args, admin_token):
        return 1

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

    try:
        applied.extend(apply_external_slurm_autoscaler_supervisors(profile))
    except ValueError as e:
        sys.stderr.write(f"error: {e}\n")
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
        f"{'' if len(profile.gb10_desired_states) == 1 else 's'}, "
        f"{len(profile.external_slurm_autoscaler_supervisors)} external autoscaler "
        "supervisor"
        f"{'' if len(profile.external_slurm_autoscaler_supervisors) == 1 else 's'}.\n",
    )
    if profile.catalog_provisioning.get("required"):
        command = profile.catalog_provisioning.get("command")
        if command:
            sys.stdout.write(f"Catalog provisioning gate: {command}\n")
    return 0


def _environment_state_check(args: argparse.Namespace) -> int:
    try:
        broker_binding = _prepare_environment_state_broker_binding(
            args,
            operation="check",
        )
    except ValueError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    profile = _load_environment_state_profile_from_args(args)
    if profile is None:
        return 2
    try:
        lease = _acquire_environment_state_rollout_lock(
            args,
            operation="check",
            profile=profile,
            broker_binding=broker_binding,
        )
    except RolloutLeaseError:
        return 1
    except ValueError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    rc = 1
    try:
        rc = _environment_state_check_impl(args, profile)
        return rc
    finally:
        if lease is not None:
            lease.release(status="released" if rc == 0 else "failed")


def _environment_state_check_impl(
    args: argparse.Namespace,
    profile: EnvironmentStateProfile,
) -> int:
    from loom_cli.environment_state import (
        autoscaler_blockers,
        diff_environment_state,
        diff_external_slurm_autoscaler_supervisors,
        diff_external_slurm_runner_prerequisites,
    )

    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2
    if not _validate_expected_admin_token_fingerprint(args, admin_token):
        return 1

    rc, live = _fetch_environment_state(
        cp_url=args.cp_url,
        admin_token=admin_token,
    )
    if rc != 0 or live is None:
        return rc

    expected_worker_token: str | None = None
    if getattr(args, "worker_token", None):
        try:
            expected_worker_token = resolve_secret_source(
                args.worker_token,
                flag_name="--worker-token",
            )
        except SecretSourceError as e:
            sys.stderr.write(f"error: {e}\n")
            return 2

    drift = diff_environment_state(
        profile,
        live,
        expected_worker_token=expected_worker_token,
    )
    drift.extend(
        diff_external_slurm_runner_prerequisites(
            profile,
            expected_worker_token=expected_worker_token,
        )
    )
    drift.extend(diff_external_slurm_autoscaler_supervisors(profile))
    blockers = autoscaler_blockers(profile, live)
    if args.format == "json":
        json.dump(
            {
                "environment": profile.environment,
                "control_plane_environment": profile.control_plane_environment,
                "profile": str(args.file),
                "ok": not drift and not blockers,
                "drift": [
                    {
                        "path": item.path,
                        "desired": item.desired,
                        "live": item.live,
                    }
                    for item in drift
                ],
                "autoscaler_blockers": blockers,
                "catalog_provisioning": profile.catalog_provisioning,
            },
            sys.stdout,
            indent=2,
        )
        sys.stdout.write("\n")
        return 1 if drift or blockers else 0

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

    if blockers:
        sys.stderr.write(
            f"Environment state autoscaler blockers for {_environment_state_label(profile)}: "
            f"{len(blockers)} blocker(s)\n",
        )
        for blocker in blockers:
            sys.stderr.write(
                "  "
                f"{blocker.get('environment')}/{blocker.get('pool_name')}: "
                f"blocked={blocker.get('last_blocked_reason')} "
                f"decision={blocker.get('last_decision') or '-'} "
                f"reason={blocker.get('last_decision_reason') or '-'}\n",
            )
        sys.stderr.write(
            "Resolve the autoscaler blocker before accepting this environment as "
            "release-ready.\n",
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
            data = assert_2xx(resp, action="mint legacy team token")
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
            assert_2xx(resp, action="revoke legacy team token")
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
    """Mint a new legacy team token + print the rollout checklist. Does NOT
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


def _admin_submit_batch_on_behalf(args: argparse.Namespace) -> int:
    from loom_cli.eval_cmd import (
        _agent_needs_model,
        _build_agent_model,
        _print_batch_summary,
    )
    from loom_cli.providers_cmd import _resolve_by_name

    admin_actor = args.admin_actor.strip() if args.admin_actor else ""
    if not admin_actor:
        sys.stderr.write("error: --admin-actor is required for admin on-behalf submission\n")
        return 2
    if args.task_filter is not None and args.benchmark is not None:
        sys.stderr.write(
            "error: --benchmark and --task-filter are mutually exclusive "
            "(--benchmark B is sugar for --task-filter "
            '\'{"benchmark_id":"B"}\').\n',
        )
        return 2
    if args.task_filter is None and args.benchmark is None:
        sys.stderr.write("error: one of --benchmark or --task-filter is required.\n")
        return 2

    try:
        cfg = require_logged_in()
    except NotLoggedInError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    try:
        with authed_client(cfg) as c:
            needs_model, agent_err = _agent_needs_model(c, args.agent)
            if agent_err is not None:
                sys.stderr.write(agent_err)
                return 2
            assert needs_model is not None
            if needs_model:
                missing = [
                    flag
                    for flag, value in (
                        ("--provider", args.provider),
                        ("--model", args.model),
                    )
                    if not value
                ]
                if missing:
                    sys.stderr.write(
                        f"error: agent {args.agent!r} requires --provider "
                        "and --model for admin on-behalf batch submission; "
                        "missing " + ", ".join(missing) + ".\n",
                    )
                    return 2
            elif args.provider or args.model or args.agent_provider:
                sys.stderr.write(
                    f"error: agent {args.agent!r} does not take a model; "
                    "omit --provider, --model, and --agent-provider.\n",
                )
                return 2

            task_filter = (
                args.task_filter
                if args.task_filter is not None
                else {"benchmark_id": args.benchmark}
            )
            trial_config: dict[str, Any] = {
                "agent_name": args.agent,
                "agent_model": None,
            }
            payload: dict[str, Any] = {
                "represented_username": args.represented_username,
                "team_id": args.team_id,
                "task_filter": task_filter,
                "trial_config": trial_config,
            }
            if args.name is not None:
                payload["name"] = args.name
            if args.name_suffix is not None:
                payload["name_suffix"] = args.name_suffix
            if args.description is not None:
                payload["description"] = args.description
            if args.n_per_task is not None:
                payload["n_per_task"] = args.n_per_task
            if args.backend is not None:
                payload["backend"] = args.backend
            if args.required_worker_pool:
                payload["required_worker_pools"] = args.required_worker_pool
            if needs_model:
                conn = _resolve_by_name(
                    c,
                    args.provider,
                    team_id=args.team_id,
                )
                trial_config["agent_model"] = _build_agent_model(
                    conn["type"],
                    args.model,
                    agent_provider_override=args.agent_provider,
                )
                payload["provider_connection_id"] = conn["id"]
                payload["provider_model_id"] = args.model

            resp = c.post(
                "/api/v1/admin/batches/on-behalf",
                json=payload,
                headers={"X-Loom-Admin-Actor": admin_actor},
            )
            body = assert_2xx(resp, action="submit admin on-behalf batch")
    except HttpStatusError as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except httpx.RequestError as e:
        sys.stderr.write(
            f"error: could not reach {cfg.server_url}: {e}\n",
        )
        return 2

    if body.get("name") is None:
        body = {**body, "name": args.name or "(server-generated)"}
    sys.stdout.write(
        f"Submitted on-behalf batch for {args.represented_username!r}:\n",
    )
    _print_batch_summary(body)
    return 0


def _load_task_filter_json(raw: str) -> dict[str, Any]:
    from loom_cli.eval_cmd import _load_task_filter_json as load

    return load(raw)


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
            "`env-diagnostics` (redacted runtime environment inspection), "
            "`rate-cards` (pricing catalog sync), and "
            "`secret-store` (master-key rotation walker)."
        ),
    )
    sub = parser.add_subparsers(dest="admin_cmd", required=True)

    from loom_cli.capacity_control_plane_cmd import add_capacity_control_plane_subparser
    from loom_cli.pipeline_admin_cmd import add_pipeline_admin_subparser

    add_capacity_control_plane_subparser(sub)
    add_pipeline_admin_subparser(sub)

    p_env_diagnostics = sub.add_parser(
        "env-diagnostics",
        help="Print redacted runtime environment diagnostics.",
    )
    p_env_diagnostics.add_argument(
        "--prefix",
        action="append",
        default=None,
        help=(
            "Environment variable prefix to include. Repeat to inspect "
            "multiple scoped prefixes. Defaults to LOOM_."
        ),
    )
    p_env_diagnostics.add_argument(
        "--format",
        choices=["text", "json", "markdown"],
        default="text",
        help="Output format for terminal use or evidence artifacts.",
    )
    p_env_diagnostics.set_defaults(handler=_env_diagnostics)

    p_smoke = sub.add_parser(
        "ensure-smoke-user",
        help=(
            "Provision the deployment-managed headless smoke-user "
            "credential (user-owned submit token for the trajectory smoke)."
        ),
    )
    p_smoke.add_argument(
        "--db-url",
        default=os.environ.get("LOOM_DB_URL") or os.environ.get("LOOM_SVC_DB_URL"),
        help=(
            "Target service Postgres URL. Defaults to env LOOM_DB_URL, then "
            "LOOM_SVC_DB_URL, so it isn't exposed via argv."
        ),
    )
    p_smoke.add_argument(
        "--username",
        default="loom-smoke",
        help="Smoke-user username (default: loom-smoke).",
    )
    p_smoke.add_argument(
        "--team",
        default="loom-smoke",
        help="Team the smoke user owns (default: loom-smoke).",
    )
    p_smoke.add_argument(
        "--expires-in-days",
        type=int,
        default=90,
        help="Token lifetime in days (default: 90).",
    )
    p_smoke.add_argument(
        "--keep-prior",
        action="store_true",
        help=(
            "Do NOT revoke the smoke user's existing tokens. Default is to "
            "rotate so exactly one live credential remains."
        ),
    )
    p_smoke.add_argument(
        "--show-secret",
        action="store_true",
        help="Print the raw token to stdout (terminal scrollback risk).",
    )
    p_smoke.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    p_smoke.set_defaults(handler=_ensure_smoke_user)

    p_br = sub.add_parser(
        "ensure-batch-runner-token",
        help=(
            "Provision the deployment-managed batch-runner CP token "
            "(submit:batch token loom-service uses to dispatch batches)."
        ),
    )
    p_br.add_argument(
        "--db-url",
        default=os.environ.get("LOOM_DB_URL") or os.environ.get("LOOM_SVC_DB_URL"),
        help=(
            "Target service Postgres URL. Defaults to env LOOM_DB_URL, then "
            "LOOM_SVC_DB_URL, so it isn't exposed via argv."
        ),
    )
    p_br.add_argument(
        "--expires-in-days",
        type=int,
        default=90,
        help="Token lifetime in days (default: 90).",
    )
    p_br.add_argument(
        "--keep-prior",
        action="store_true",
        help=(
            "Do NOT revoke prior deploy-provisioned batch-runner tokens. "
            "Default rotates so exactly one is live."
        ),
    )
    p_br.add_argument(
        "--show-secret",
        action="store_true",
        help="Print the raw token to stdout (terminal scrollback risk).",
    )
    p_br.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    p_br.set_defaults(handler=_ensure_batch_runner_token)

    p_dwt = sub.add_parser(
        "ensure-dev-worker-token",
        help=(
            "LOCAL/DEV ONLY: seed the fixed --smoke-defaults worker token "
            "so in-cluster workers register with no mint/patch/restart."
        ),
    )
    p_dwt.add_argument(
        "--db-url",
        default=os.environ.get("LOOM_DB_URL") or os.environ.get("LOOM_CP_DB_URL"),
        help=(
            "Target Postgres URL. Defaults to env LOOM_DB_URL, then "
            "LOOM_CP_DB_URL, so it isn't exposed via argv."
        ),
    )
    p_dwt.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    p_dwt.set_defaults(handler=_ensure_dev_worker_token)

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
    p_mint.add_argument(
        "--show-secret",
        action="store_true",
        help=(
            "Print the raw token to stdout in text mode. Default is "
            "prefix-only to keep the raw value out of terminal "
            "scrollback. JSON mode always includes the token."
        ),
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
    p_rotate.add_argument(
        "--show-secret",
        action="store_true",
        help=(
            "Print the raw token to stdout in text mode. Default is "
            "prefix-only; pipe `--format json` into the secret store "
            "to avoid putting the raw value in shell history."
        ),
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
        "--expect-admin-token-fingerprint",
        default=None,
        help=(
            "Redacted fingerprint expected for --admin-token, formatted "
            "as 'sha256:<12-hex> len=<N>'. When set, GB10 status collection "
            "fails before contacting CP if the resolved admin token source "
            "drifts from the protected-environment source."
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
                "deploy/environment-state/staging.toml."
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
        p.add_argument(
            "--expect-admin-token-fingerprint",
            default=None,
            help=(
                "Redacted fingerprint expected for --admin-token, formatted "
                "as 'sha256:<12-hex> len=<N>'. When set, environment-state "
                "apply/check fail before contacting CP if the resolved admin "
                "token source drifts from the protected-environment source."
            ),
        )
        p.add_argument(
            "--rollout-request-envelope",
            type=Path,
            default=None,
            help=argparse.SUPPRESS,
        )
        _add_rollout_lock_args(p)

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
    p_env_state_check.add_argument(
        "--worker-token",
        type=secret_source_argparse_type("--worker-token"),
        default=None,
        help=(
            "Current environment worker token source for remote-worker parity "
            "checks. Must be env:VAR, file:PATH, or -; only redacted "
            "fingerprints are emitted."
        ),
    )
    p_env_state_check.set_defaults(handler=_environment_state_check)

    p_team = tok_sub.add_parser(
        "team",
        help=(
            "Legacy team-token operations via `loom_service`'s "
            "`/api/v1/tokens` route. Uses the server + bearer from "
            "`loom auth login`."
        ),
    )
    team_sub = p_team.add_subparsers(dest="team_op", required=True)

    p_team_mint = team_sub.add_parser(
        "mint",
        help="Issue a legacy team token. Admin caller is recorded in audit.",
    )
    _add_team_mint_args(p_team_mint)
    p_team_mint.set_defaults(handler=_mint_team_token)

    p_team_revoke = team_sub.add_parser(
        "revoke",
        help="Revoke a legacy team token by its 8-hex-char prefix.",
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
            "the bearer is a user-owned API token."
        ),
    )
    p_team_revoke.set_defaults(handler=_revoke_team_token)

    p_team_rotate = team_sub.add_parser(
        "rotate",
        help=(
            "Mint a new legacy team token + print the rollout procedure. "
            "Does NOT revoke the old token automatically."
        ),
    )
    _add_team_mint_args(p_team_rotate)
    p_team_rotate.set_defaults(handler=_rotate_team_token)

    p_batches = sub.add_parser(
        "batches",
        help="Admin batch operations through the public service API.",
    )
    batches_sub = p_batches.add_subparsers(dest="batches_op", required=True)
    p_submit_on_behalf = batches_sub.add_parser(
        "submit-on-behalf",
        help="Submit an audited batch on behalf of an active user/team.",
    )
    p_submit_on_behalf.add_argument(
        "--represented-username",
        required=True,
        help="Active username to record as the represented submitter.",
    )
    p_submit_on_behalf.add_argument(
        "--team-id",
        required=True,
        help="Represented team UUID. The user must be a member of this team.",
    )
    p_submit_on_behalf.add_argument("--agent", required=True)
    p_submit_on_behalf.add_argument(
        "--provider",
        default=None,
        help=(
            "Provider connection name. Required for agents that call a "
            "model; omit for no-model agents such as oracle."
        ),
    )
    p_submit_on_behalf.add_argument(
        "--model",
        default=None,
        help=(
            "Upstream model id. Required for agents that call a model; "
            "omit for no-model agents such as oracle."
        ),
    )
    p_submit_on_behalf.add_argument(
        "--agent-provider",
        dest="agent_provider",
        default=None,
        help="Override the agent model provider field for pricing/adapter compatibility.",
    )
    p_submit_on_behalf.add_argument(
        "--benchmark",
        default=None,
        help=('Benchmark slug — shortcut for --task-filter \'{"benchmark_id":"..."}\'.'),
    )
    p_submit_on_behalf.add_argument(
        "--task-filter",
        dest="task_filter",
        type=_load_task_filter_json,
        default=None,
        help=(
            "Task filter as JSON (object). Pass a literal JSON string "
            "or `@path/to/file.json` to read from disk."
        ),
    )
    p_submit_on_behalf.add_argument(
        "--name",
        default=None,
        help="Optional batch display name. When omitted, the server generates one.",
    )
    p_submit_on_behalf.add_argument(
        "--name-suffix",
        default=None,
        help="Optional suffix appended to the server-generated display name.",
    )
    p_submit_on_behalf.add_argument("--description", default=None)
    p_submit_on_behalf.add_argument(
        "--n-per-task",
        dest="n_per_task",
        type=int,
        default=None,
        help="Number of trials per task (1-100).",
    )
    p_submit_on_behalf.add_argument(
        "--backend",
        default=None,
        help="Worker backend (default: server default).",
    )
    p_submit_on_behalf.add_argument(
        "--required-worker-pool",
        dest="required_worker_pool",
        action="append",
        default=[],
        help=(
            "Operator and release coverage only: add one extra "
            "pool-pinned coverage trial on this worker pool. Repeat for "
            "mixed-pool release canaries. Not for user eval batches — "
            "use a separate on-behalf canary batch instead."
        ),
    )
    p_submit_on_behalf.add_argument(
        "--admin-actor",
        default=None,
        help="Required audit actor for the real operator/admin submitting.",
    )
    p_submit_on_behalf.set_defaults(handler=_admin_submit_batch_on_behalf)

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
