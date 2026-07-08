"""CLI entrypoint for `loom cluster rollout` (#340).

Thin shim that:

* Parses driver-specific args.
* Resolves ``--ref`` to a full SHA (via git).
* Hashes cluster-config.toml.
* Locates or creates the evidence directory.
* Constructs a :class:`RolloutContext` and hands off to
  :func:`loom_cli.rollout.driver.run_rollout`.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

from loom_cli.rollout.context import RolloutContext, sha256_of_file
from loom_cli.rollout.driver import DriverError, run_rollout
from loom_cli.rollout.evidence import EvidenceDirectory, new_rollout_id
from loom_cli.rollout.steps import default_step_sequence
from loom_cli.rollout.steps.s00_resolve_target import resolve_ref_to_sha

_STAGING_CLUSTER_NAME = "loom-staging"
_STAGING_NAMESPACE = "loom-staging"
_STAGING_DATA_ROOT = "/data/loom-staging"
_REPO_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True, slots=True)
class RolloutPreset:
    """Repo-owned stable rollout inputs for one protected environment."""

    name: str
    configured: bool
    cluster_name: str | None = None
    namespace: str | None = None
    environment: str | None = None
    cp_url: str | None = None
    cluster_config_path: Path | None = None
    backup_manifest_path: Path | None = None
    rollout_root: Path | None = None
    admin_token_source: str | None = None
    worker_token_source: str | None = None
    service_token_source: str | None = None
    smoke_submit_mode: str | None = None
    smoke_task_id: str | None = None
    smoke_required_worker_pool: str | None = None
    smoke_agent: str | None = None
    smoke_on_behalf_username: str | None = None
    smoke_on_behalf_team_id: str | None = None
    smoke_admin_actor: str | None = None
    scope: str | None = None


_ROLLOUT_PRESETS: dict[str, RolloutPreset] = {
    "staging": RolloutPreset(
        name="staging",
        configured=True,
        cluster_name=_STAGING_CLUSTER_NAME,
        namespace=_STAGING_NAMESPACE,
        environment="staging",
        cp_url="http://127.0.0.1:18081",
        cluster_config_path=Path("deploy/environments/staging.cluster.toml"),
        backup_manifest_path=Path(
            "/data/loom-staging/backups/latest/backup-manifest.json",
        ),
        rollout_root=Path(_STAGING_DATA_ROOT),
        admin_token_source=(
            "file:/shared_work/qianyi/loom-worker-capacity/staging-admin-token"
        ),
        worker_token_source=(
            "file:/shared_work/qianyi/loom-worker-capacity/staging-worker-token"
        ),
        service_token_source=(
            "file:/shared_work/qianyi/loom-worker-capacity/staging-service-token"
        ),
        smoke_submit_mode="admin-on-behalf",
        smoke_task_id="loom-smoke/gb10-oracle-hello-world",
        smoke_required_worker_pool="gb10-arm64",
        smoke_agent="oracle",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="env:LOOM_SMOKE_ON_BEHALF_TEAM_ID",
        smoke_admin_actor="codex-v1-release-gate",
        scope="current-gb10",
    ),
    # Explicit selector is reserved now, but first-prod values are not ready.
    "prod": RolloutPreset(name="prod", configured=False),
}


def _replayable_secret_source(source: str, *, flag_name: str) -> str:
    """Validate a secret source that rollout steps can safely reuse.

    Direct commands may accept stdin for one-shot operations. The rollout
    driver calls subcommands separately, persists inputs for resume evidence,
    and therefore requires a replayable reference.
    """
    if source.startswith("env:"):
        if source == "env:":
            raise argparse.ArgumentTypeError(
                f"{flag_name}: env: source requires a variable name",
            )
        return source
    if source.startswith("file:"):
        if source == "file:":
            raise argparse.ArgumentTypeError(
                f"{flag_name}: file: source requires a path",
            )
        return source
    if source == "-":
        raise argparse.ArgumentTypeError(
            f"{flag_name}: stdin source '-' is not replayable for rollout; "
            "use env:VAR or file:PATH",
        )
    raise argparse.ArgumentTypeError(
        f"{flag_name}: literal values are rejected; use one of "
        "{env:VAR | file:PATH}",
    )


def _replayable_admin_token_source(source: str) -> str:
    return _replayable_secret_source(source, flag_name="--admin-token")


def _replayable_worker_token_source(source: str) -> str:
    return _replayable_secret_source(source, flag_name="--worker-token")


def _replayable_service_token_source(source: str) -> str:
    return _replayable_secret_source(source, flag_name="--service-token")


def _replayable_smoke_api_token_source(source: str) -> str:
    return _replayable_secret_source(source, flag_name="--smoke-api-token")


def _validate_physical_environment_target(args: argparse.Namespace) -> str | None:
    """Return a rollout-target error when logical env and physical target diverge."""
    environment = str(args.environment).strip().lower()
    cluster_name = str(args.cluster_name).strip()
    namespace = str(args.namespace).strip()
    rollout_root = str(args.rollout_root).strip().rstrip("/")

    if environment != "staging":
        return None
    mismatches: list[str] = []
    if cluster_name != _STAGING_CLUSTER_NAME:
        mismatches.append(
            f"--cluster-name must be {_STAGING_CLUSTER_NAME!r}, got {cluster_name!r}",
        )
    if namespace != _STAGING_NAMESPACE:
        mismatches.append(
            f"--namespace must be {_STAGING_NAMESPACE!r}, got {namespace!r}",
        )
    if rollout_root.startswith("/data/") and not (
        rollout_root == _STAGING_DATA_ROOT
        or rollout_root.startswith(f"{_STAGING_DATA_ROOT}/")
    ):
        mismatches.append(
            f"--rollout-root under /data must be {_STAGING_DATA_ROOT!r} "
            f"or a child path, got {rollout_root!r}",
        )
    if not mismatches:
        return None

    return (
        "staging rollout must use physical staging resources: "
        "cluster 'loom-staging', namespace 'loom-staging', and "
        "rollout root '/data/loom-staging'. "
        + "; ".join(mismatches)
    )


def _selector(args: argparse.Namespace) -> str | None:
    value = getattr(args, "environment_selector", None)
    if isinstance(value, str) and value.strip():
        return value.strip().lower()
    return None


def _derive_image_tag(selector: str | None, resolved_sha: str) -> str:
    prefix = selector or "rollout"
    return f"{prefix}-{resolved_sha[:7]}"


def _resolve_preset_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    return _REPO_ROOT / path


def _resolve_with_preset(
    args: argparse.Namespace,
    *,
    resolved_sha: str | None,
) -> tuple[str | None, str | None]:
    """Merge preset values into omitted CLI args.

    The legacy full-argv mode remains available for explicit one-off operator
    use. Preset mode is selected only by the positional environment selector,
    and it never falls back from one environment to another.
    """
    selector = _selector(args)
    if selector is None:
        required = (
            "image_tag",
            "cluster_name",
            "environment",
            "cp_url",
            "cluster_config",
            "backup_manifest",
            "rollout_root",
        )
        if all(getattr(args, name, None) for name in required):
            return None, None
        return (
            None,
            "rollout requires an explicit environment selector "
            "('staging' or 'prod') for preset mode, or the complete manual "
            "argument set including --environment, --image-tag, "
            "--cluster-name, --cp-url, --cluster-config, --backup-manifest, "
            "and --rollout-root",
        )

    preset = _ROLLOUT_PRESETS.get(selector)
    if preset is None:
        return (
            None,
            f"unknown rollout environment selector {selector!r}; choose staging or prod",
        )
    if not preset.configured:
        return (
            None,
            f"{selector} preset not configured; configure first-prod rollout values before use",
        )

    assert preset.cluster_name is not None
    assert preset.namespace is not None
    assert preset.environment is not None
    assert preset.cp_url is not None
    assert preset.cluster_config_path is not None
    assert preset.backup_manifest_path is not None
    assert preset.rollout_root is not None

    if args.image_tag is None and resolved_sha is not None:
        args.image_tag = _derive_image_tag(selector, resolved_sha)
    if args.cluster_name is None:
        args.cluster_name = preset.cluster_name
    if args.namespace is None:
        args.namespace = preset.namespace
    if args.environment is None:
        args.environment = preset.environment
    if args.cp_url is None:
        args.cp_url = preset.cp_url
    if args.cluster_config is None:
        args.cluster_config = str(_resolve_preset_path(preset.cluster_config_path))
    if args.backup_manifest is None:
        args.backup_manifest = str(preset.backup_manifest_path)
    if args.rollout_root is None:
        args.rollout_root = str(preset.rollout_root)
    if args.admin_token is None and preset.admin_token_source is not None:
        args.admin_token = preset.admin_token_source
    if args.worker_token is None and preset.worker_token_source is not None:
        args.worker_token = preset.worker_token_source
    if args.service_token is None and preset.service_token_source is not None:
        args.service_token = preset.service_token_source
    if args.smoke_submit_mode is None:
        args.smoke_submit_mode = preset.smoke_submit_mode
    if args.smoke_task_id is None:
        args.smoke_task_id = preset.smoke_task_id
    if args.smoke_required_worker_pool is None:
        args.smoke_required_worker_pool = preset.smoke_required_worker_pool
    if args.smoke_agent is None:
        args.smoke_agent = preset.smoke_agent
    if args.smoke_on_behalf_username is None:
        args.smoke_on_behalf_username = preset.smoke_on_behalf_username
    if args.smoke_on_behalf_team_id is None:
        args.smoke_on_behalf_team_id = preset.smoke_on_behalf_team_id
    if args.smoke_admin_actor is None:
        args.smoke_admin_actor = preset.smoke_admin_actor
    if args.scope is None:
        args.scope = preset.scope or "current-gb10"
    return selector, None


def _validate_required_args(args: argparse.Namespace) -> str | None:
    required = {
        "--image-tag": args.image_tag,
        "--cluster-name": args.cluster_name,
        "--environment": args.environment,
        "--cp-url": args.cp_url,
        "--cluster-config": args.cluster_config,
        "--backup-manifest": args.backup_manifest,
        "--rollout-root": args.rollout_root,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        return "missing required rollout inputs: " + ", ".join(missing)
    return None


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(_REPO_ROOT))
    except ValueError:
        return str(path)


def _dry_run_inputs(
    ctx: RolloutContext,
    *,
    preset_name: str | None,
) -> list[tuple[str, object]]:
    return [
        ("preset", preset_name or "manual"),
        ("image_tag", ctx.image_tag),
        ("cluster_name", ctx.cluster_name),
        ("namespace", ctx.namespace),
        ("environment", ctx.environment),
        ("cp_url", ctx.cp_url),
        ("cluster_config_path", _display_path(ctx.cluster_config_path)),
        ("backup_manifest_path", str(ctx.backup_manifest_path)),
        ("rollout_root", str(ctx.rollout_root)),
        ("scope", ctx.scope),
        ("admin_token_source", ctx.admin_token_source),
        ("worker_token_source", ctx.worker_token_source),
        ("service_token_source", ctx.service_token_source),
        ("smoke_submit_mode", ctx.smoke_submit_mode),
        ("smoke_task_id", ctx.smoke_task_id),
        ("smoke_required_worker_pool", ctx.smoke_required_worker_pool),
        ("smoke_agent", ctx.smoke_agent),
        ("smoke_on_behalf_username", ctx.smoke_on_behalf_username),
        ("smoke_on_behalf_team_id", ctx.smoke_on_behalf_team_id),
        ("smoke_admin_actor", ctx.smoke_admin_actor),
        ("gb10_prep_concurrency", ctx.gb10_prep_concurrency),
    ]


def _persisted_resume_sha(
    *,
    evidence: EvidenceDirectory,
    target_ref: str,
    image_tag: str,
) -> str | None:
    """Return the pinned SHA from an existing rollout when it is replayable."""
    if not evidence.inputs_path().is_file():
        return None
    persisted = evidence.read_inputs()
    resolved_sha = persisted.get("resolved_sha")
    if (
        persisted.get("target_ref") == target_ref
        and persisted.get("image_tag") == image_tag
        and isinstance(resolved_sha, str)
        and len(resolved_sha) == 40
    ):
        return resolved_sha
    return None


def build_parser(p: argparse.ArgumentParser) -> None:
    """Populate ``p`` with the rollout subcommand's arguments."""
    p.add_argument(
        "environment_selector",
        nargs="?",
        choices=tuple(_ROLLOUT_PRESETS),
        help=(
            "Explicit rollout environment preset selector. Use 'staging' for "
            "the current staging rollout preset. 'prod' is reserved and fails "
            "closed until first-prod values are configured."
        ),
    )
    p.add_argument(
        "--ref",
        required=True,
        help="Git ref to resolve to a SHA (e.g. origin/dev, or a tag/sha).",
    )
    p.add_argument(
        "--image-tag",
        default=None,
        help=(
            "Target release image tag; the driver validates that the "
            "resolved --ref sha starts with the tag's `sha7` suffix "
            "(convention: `staging-<sha7>`)."
        ),
    )
    p.add_argument(
        "--cluster-name",
        default=None,
        help="Name of the target kind cluster (as in `kind get clusters`).",
    )
    p.add_argument(
        "--namespace",
        default=None,
        help="Kubernetes namespace. Defaults to `loom`.",
    )
    p.add_argument(
        "--environment",
        default=None,
        help=(
            "Protected environment name (e.g. staging). Used by the "
            "backup and release-gate steps to bind evidence to the "
            "operator's declared environment."
        ),
    )
    p.add_argument(
        "--cp-url",
        default=None,
        help=(
            "Operator-reachable Control Plane admin base URL used by rollout "
            "steps that call `loom admin ...`, for example "
            "http://control-node.lan:18081 or http://127.0.0.1:18081."
        ),
    )
    p.add_argument(
        "--admin-token",
        default=None,
        type=_replayable_admin_token_source,
        help=(
            "Admin token source for protected Control Plane admin calls. "
            "Use a replayable env:VAR or file:PATH reference so raw tokens "
            "never enter argv or rollout evidence. Default: "
            "'env:LOOM_CP_ADMIN_TOKEN'."
        ),
    )
    p.add_argument(
        "--expect-admin-token-fingerprint",
        default=None,
        help=(
            "Expected redacted admin-token fingerprint, formatted as "
            "'sha256:<12-hex> len=<N>'. When set, env-state apply/check "
            "fail before contacting CP if --admin-token resolves to a "
            "different token."
        ),
    )
    p.add_argument(
        "--worker-token",
        default=None,
        type=_replayable_worker_token_source,
        help=(
            "Worker token source for protected external runner parity checks. "
            "Use a replayable env:VAR or file:PATH reference so raw tokens "
            "never enter argv or rollout evidence. Passed only to "
            "`loom admin environment-state check`."
        ),
    )
    p.add_argument(
        "--service-token",
        default=None,
        type=_replayable_service_token_source,
        help=(
            "Service API token source for rollout-owned CLI calls that mutate "
            "or verify Service-backed defaults, such as rate-card sync and "
            "hosted provider pricing. Use a replayable env:VAR or file:PATH "
            "reference so raw tokens never enter argv or rollout evidence."
        ),
    )
    p.add_argument(
        "--smoke-submit-mode",
        default=None,
        choices=("user-token", "admin-on-behalf"),
        help=(
            "Step 15 smoke submit mode. When omitted, the smoke step preserves "
            "the legacy LOOM_SMOKE_SUBMIT_MODE fallback. Use admin-on-behalf "
            "for v1.0 represented-user release validation."
        ),
    )
    p.add_argument(
        "--smoke-api-token",
        default=None,
        type=_replayable_smoke_api_token_source,
        help=(
            "User-owned smoke API token source for step 15 user-token mode. "
            "Use a replayable env:VAR or file:PATH reference so raw tokens "
            "never enter argv or rollout evidence. Not used by "
            "admin-on-behalf mode."
        ),
    )
    p.add_argument(
        "--smoke-task-id",
        default=None,
        help=(
            "Optional explicit smoke task id. current-gb10 defaults to "
            "loom-smoke/gb10-oracle-hello-world."
        ),
    )
    p.add_argument(
        "--smoke-required-worker-pool",
        default=None,
        help=(
            "Optional worker-pool requirement for smoke submission. "
            "current-gb10 defaults to gb10-arm64 when the task id is not "
            "overridden."
        ),
    )
    p.add_argument(
        "--smoke-agent",
        default=None,
        help="Optional smoke agent name. Defaults to oracle.",
    )
    p.add_argument(
        "--smoke-on-behalf-username",
        default=None,
        help="Represented username for admin-on-behalf smoke mode.",
    )
    p.add_argument(
        "--smoke-on-behalf-team-id",
        default=None,
        help="Represented team id for admin-on-behalf smoke mode.",
    )
    p.add_argument(
        "--smoke-admin-actor",
        default=None,
        help="Audit actor string for admin-on-behalf smoke submissions.",
    )
    p.add_argument(
        "--cluster-config",
        default=None,
        help="Path to the operator's cluster-config.toml.",
    )
    p.add_argument(
        "--backup-manifest",
        default=None,
        help=(
            "Path to a pre-existing backup manifest for --environment. "
            "The dumps are produced by the operator per the runbook; the "
            "driver verifies via `loom cluster backup check` and refuses "
            "to advance without a fresh manifest."
        ),
    )
    p.add_argument(
        "--backup-manifest-min-remaining-hours",
        type=int,
        default=2,
        help=(
            "Minimum freshness window that must remain on --backup-manifest "
            "when rollout step 05 runs. This fails long protected rollouts "
            "early instead of letting the manifest expire before cluster-up. "
            "Default: 2."
        ),
    )
    p.add_argument(
        "--rollout-root",
        default=None,
        help=(
            "Root of the evidence directory tree "
            "(created by `loom cluster bootstrap-evidence-paths`)."
        ),
    )
    p.add_argument(
        "--scope",
        default=None,
        choices=("current-gb10", "full-cluster"),
        help=(
            "Rollout scope. current-gb10 targets the current GB10 pool; "
            "full-cluster requires evidence across all release-managed "
            "worker pools."
        ),
    )
    p.add_argument(
        "--gb10-prep-concurrency",
        type=int,
        default=None,
        help=(
            "Optional bounded host-level concurrency for rollout step 11 "
            "gb10-prep. Each host still runs its internal command sequence "
            "serially."
        ),
    )
    p.add_argument(
        "--exclude-oldlab",
        action="store_true",
        help=(
            "Exclude the OLDLAB worker pool. Refused when "
            "--scope=full-cluster because you can't claim full-cluster "
            "acceptance while excluding a release-managed pool."
        ),
    )
    p.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Resume an in-progress rollout with matching --image-tag. "
            "Refuses if none is found."
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the planned step sequence + inputs hash and exit.",
    )


def handle(args: argparse.Namespace) -> int:
    """Handler wired up from `loom cluster rollout`."""
    selector_name = _selector(args)
    resolved_sha: str | None = None
    if selector_name and args.image_tag is None:
        preset = _ROLLOUT_PRESETS.get(selector_name)
        if preset is not None and preset.configured:
            try:
                resolved_sha = resolve_ref_to_sha(args.ref)
            except Exception as exc:
                sys.stderr.write(f"error: {exc}\n")
                return 2
    preset_name, preset_error = _resolve_with_preset(
        args,
        resolved_sha=resolved_sha,
    )
    if preset_error is not None:
        sys.stderr.write(f"error: {preset_error}\n")
        return 2
    required_error = _validate_required_args(args)
    if required_error is not None:
        sys.stderr.write(f"error: {required_error}\n")
        return 2
    if args.namespace is None:
        args.namespace = "loom"
    if args.admin_token is None:
        args.admin_token = "env:LOOM_CP_ADMIN_TOKEN"
    if args.scope is None:
        args.scope = "current-gb10"

    physical_target_error = _validate_physical_environment_target(args)
    if physical_target_error is not None:
        sys.stderr.write(f"error: {physical_target_error}\n")
        return 2

    cluster_config_path = Path(args.cluster_config)
    if not cluster_config_path.is_file():
        sys.stderr.write(
            f"error: cluster-config not found: {cluster_config_path}\n"
        )
        return 2
    if args.backup_manifest_min_remaining_hours < 0:
        sys.stderr.write(
            "error: --backup-manifest-min-remaining-hours must be >= 0\n"
        )
        return 2
    if args.gb10_prep_concurrency is not None and args.gb10_prep_concurrency < 1:
        sys.stderr.write("error: --gb10-prep-concurrency must be >= 1\n")
        return 2
    cfg_sha = sha256_of_file(cluster_config_path)
    rollout_root = Path(args.rollout_root)

    # Choose evidence dir: resume finds an existing one; new invocations
    # create one keyed by (image_tag, launch timestamp).
    if args.resume:
        found = EvidenceDirectory.find_in_progress(
            rollout_root, image_tag=args.image_tag,
        )
        if found is None:
            sys.stderr.write(
                "error: --resume requested but no in-progress rollout "
                f"matching --image-tag {args.image_tag!r} found under "
                f"{rollout_root}/rollouts/\n"
            )
            return 2
        evidence = found
        rollout_id = evidence.rollout_id
    else:
        # Auto-detect a still-running rollout for this image tag; the
        # operator likely wants to resume rather than start over.
        found = EvidenceDirectory.find_in_progress(
            rollout_root, image_tag=args.image_tag,
        )
        if found is not None:
            sys.stderr.write(
                f"error: an in-progress rollout for --image-tag "
                f"{args.image_tag!r} already exists at "
                f"{found.path}. Re-run with --resume to continue it, "
                "or remove the state.json to start fresh.\n"
            )
            return 2
        rollout_id = new_rollout_id(image_tag=args.image_tag)
        evidence = EvidenceDirectory(rollout_root, rollout_id)

    persisted_sha = (
        _persisted_resume_sha(
            evidence=evidence,
            target_ref=args.ref,
            image_tag=args.image_tag,
        )
        if args.resume
        else None
    )
    if persisted_sha is not None:
        resolved_sha = persisted_sha
    if resolved_sha is None:
        try:
            resolved_sha = resolve_ref_to_sha(args.ref)
        except Exception as exc:
            sys.stderr.write(f"error: {exc}\n")
            return 2

    ctx = RolloutContext(
        image_tag=args.image_tag,
        target_ref=args.ref,
        resolved_sha=resolved_sha,
        cluster_name=args.cluster_name,
        namespace=args.namespace,
        environment=args.environment,
        cp_url=args.cp_url,
        admin_token_source=args.admin_token,
        expect_admin_token_fingerprint=args.expect_admin_token_fingerprint,
        worker_token_source=args.worker_token,
        service_token_source=args.service_token,
        smoke_submit_mode=args.smoke_submit_mode,
        smoke_api_token_source=args.smoke_api_token,
        smoke_task_id=args.smoke_task_id,
        smoke_required_worker_pool=args.smoke_required_worker_pool,
        smoke_agent=args.smoke_agent,
        smoke_on_behalf_username=args.smoke_on_behalf_username,
        smoke_on_behalf_team_id=args.smoke_on_behalf_team_id,
        smoke_admin_actor=args.smoke_admin_actor,
        cluster_config_path=cluster_config_path,
        cluster_config_sha256=cfg_sha,
        rollout_root=rollout_root,
        backup_manifest_path=Path(args.backup_manifest),
        backup_manifest_min_remaining_hours=(
            args.backup_manifest_min_remaining_hours
        ),
        scope=args.scope,
        exclude_oldlab=args.exclude_oldlab,
        gb10_prep_concurrency=args.gb10_prep_concurrency,
        resume=args.resume,
        metadata={
            key: value
            for key, value in {
                "rollout_id": rollout_id,
            }.items()
            if value is not None
        },
    )

    steps = default_step_sequence()

    if args.dry_run:
        sys.stdout.write(
            f"rollout_id: {rollout_id}\n"
            f"resolved_sha: {resolved_sha}\n"
        )
        for key, value in _dry_run_inputs(ctx, preset_name=preset_name):
            sys.stdout.write(f"{key}: {value}\n")
        sys.stdout.write("steps:\n")
        for step in steps:
            sys.stdout.write(
                f"  {step.number:02d} {step.name}\n"
            )
        return 0

    try:
        return run_rollout(ctx, steps, evidence)
    except DriverError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
