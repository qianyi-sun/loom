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
from pathlib import Path

from loom_cli.rollout.context import RolloutContext, sha256_of_file
from loom_cli.rollout.driver import DriverError, run_rollout
from loom_cli.rollout.evidence import EvidenceDirectory, new_rollout_id
from loom_cli.rollout.steps import default_step_sequence
from loom_cli.rollout.steps.s00_resolve_target import resolve_ref_to_sha

_STAGING_CLUSTER_NAME = "loom-staging"
_STAGING_NAMESPACE = "loom-staging"
_STAGING_DATA_ROOT = "/data/loom-staging"


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


def build_parser(p: argparse.ArgumentParser) -> None:
    """Populate ``p`` with the rollout subcommand's arguments."""
    p.add_argument(
        "--ref",
        required=True,
        help="Git ref to resolve to a SHA (e.g. origin/dev, or a tag/sha).",
    )
    p.add_argument(
        "--image-tag",
        required=True,
        help=(
            "Target release image tag; the driver validates that the "
            "resolved --ref sha starts with the tag's `sha7` suffix "
            "(convention: `staging-<sha7>`)."
        ),
    )
    p.add_argument(
        "--cluster-name",
        required=True,
        help="Name of the target kind cluster (as in `kind get clusters`).",
    )
    p.add_argument(
        "--namespace",
        default="loom",
        help="Kubernetes namespace. Defaults to `loom`.",
    )
    p.add_argument(
        "--environment",
        required=True,
        help=(
            "Protected environment name (e.g. staging). Used by the "
            "backup and release-gate steps to bind evidence to the "
            "operator's declared environment."
        ),
    )
    p.add_argument(
        "--cp-url",
        required=True,
        help=(
            "Operator-reachable Control Plane admin base URL used by rollout "
            "steps that call `loom admin ...`, for example "
            "http://control-node.lan:18081 or http://127.0.0.1:18081."
        ),
    )
    p.add_argument(
        "--admin-token",
        default="env:LOOM_CP_ADMIN_TOKEN",
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
        "--cluster-config",
        required=True,
        help="Path to the operator's cluster-config.toml.",
    )
    p.add_argument(
        "--backup-manifest",
        required=True,
        help=(
            "Path to a pre-existing backup manifest for --environment. "
            "The dumps are produced by the operator per the runbook; the "
            "driver verifies via `loom cluster backup check` and refuses "
            "to advance without a fresh manifest."
        ),
    )
    p.add_argument(
        "--rollout-root",
        required=True,
        help=(
            "Root of the evidence directory tree "
            "(created by `loom cluster bootstrap-evidence-paths`)."
        ),
    )
    p.add_argument(
        "--scope",
        default="current-gb10",
        choices=("current-gb10", "full-cluster"),
        help=(
            "Rollout scope. current-gb10 targets the current GB10 pool; "
            "full-cluster requires evidence across all release-managed "
            "worker pools."
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
    physical_target_error = _validate_physical_environment_target(args)
    if physical_target_error is not None:
        sys.stderr.write(f"error: {physical_target_error}\n")
        return 2

    # Resolve --ref → sha via git.
    try:
        resolved_sha = resolve_ref_to_sha(args.ref)
    except Exception as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2

    cluster_config_path = Path(args.cluster_config)
    if not cluster_config_path.is_file():
        sys.stderr.write(
            f"error: cluster-config not found: {cluster_config_path}\n"
        )
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
        cluster_config_path=cluster_config_path,
        cluster_config_sha256=cfg_sha,
        rollout_root=rollout_root,
        backup_manifest_path=Path(args.backup_manifest),
        scope=args.scope,
        exclude_oldlab=args.exclude_oldlab,
        resume=args.resume,
        metadata={"rollout_id": rollout_id},
    )

    steps = default_step_sequence()

    if args.dry_run:
        sys.stdout.write(
            f"rollout_id: {rollout_id}\n"
            f"resolved_sha: {resolved_sha}\n"
            f"steps:\n"
        )
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
