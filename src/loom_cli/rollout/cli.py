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
import hashlib
import importlib
import os
import stat
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

from loom_cli.rollout.context import RolloutContext, sha256_of_file
from loom_cli.rollout.driver import DriverError, run_rollout
from loom_cli.rollout.evidence import EvidenceDirectory, new_rollout_id
from loom_cli.rollout.operator.backup_limits import operator_backup_traversal_limits
from loom_cli.rollout.operator.config import OperatorConfig
from loom_cli.rollout.operator.envelope import (
    fixed_operator_config_path,
    load_validated_envelope,
)
from loom_cli.rollout.operator.redaction import redact_rollout_text
from loom_cli.rollout.steps import default_step_sequence
from loom_cli.rollout.steps.s00_resolve_target import resolve_ref_to_sha

_STAGING_CLUSTER_NAME = "loom-staging"
_STAGING_NAMESPACE = "loom-staging"
_STAGING_DATA_ROOT = "/data/loom-staging"
_PRODUCTION_CLUSTER_NAME = "loom-production"
_PRODUCTION_NAMESPACE = "loom-production"
_PROTECTED_PHYSICAL_TARGET_ENVIRONMENTS = {
    _STAGING_CLUSTER_NAME: "staging",
    _PRODUCTION_CLUSTER_NAME: "production",
}
_PROTECTED_TARGET_MISMATCH_ERROR = (
    "protected rollout target mismatch: protected physical identity conflicts "
    "with the declared environment"
)
_REPO_ROOT = Path(__file__).resolve().parents[3]
_ROLLOUT_RUNNER_REQUIRED_MODULES = (
    "loom_benchmark_tool.register_cmd",
    "loom_benchmarks.registry",
    "loom_benchmarks.adapters.skilllearnbench",
    "loom_benchmark_terminal_bench_2.adapter",
)
_EXPLICIT_OPTIONS_ATTR = "_rollout_explicit_options"
_MAX_CLASSIFIER_CLUSTER_CONFIG_BYTES = 256 * 1024
_MANUAL_PATH_BINDING_ERROR = (
    "manual non-dry-run path identity is unsafe or changed; "
    "broker-created request envelope is required"
)


def _record_explicit_option(
    namespace: argparse.Namespace,
    option_string: str | None,
) -> None:
    options = set(getattr(namespace, _EXPLICIT_OPTIONS_ATTR, ()))
    if option_string is not None:
        options.add(option_string)
    setattr(namespace, _EXPLICIT_OPTIONS_ATTR, options)


class _ExplicitStoreAction(argparse.Action):
    """Store an option and retain whether it appeared in the original argv."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: object,
        option_string: str | None = None,
    ) -> None:
        del parser
        setattr(namespace, self.dest, values)
        _record_explicit_option(namespace, option_string)


class _ExplicitStoreTrueAction(argparse.Action):
    """Boolean form of :class:`_ExplicitStoreAction`."""

    def __init__(
        self,
        option_strings: list[str],
        dest: str,
        default: object = False,
        required: bool = False,
        help: str | None = None,
    ) -> None:
        super().__init__(
            option_strings=option_strings,
            dest=dest,
            nargs=0,
            default=default,
            required=required,
            help=help,
        )

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: object,
        option_string: str | None = None,
    ) -> None:
        del parser, values
        setattr(namespace, self.dest, True)
        _record_explicit_option(namespace, option_string)


@dataclass(frozen=True, slots=True)
class _PathIdentitySnapshot:
    """No-follow identity for every component of one absolute path."""

    path: Path
    expected_kind: str
    components: tuple[tuple[int, int, int], ...]
    content_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class _ManualPathBindings:
    """Immutable admission record for every manual rollout filesystem input."""

    rollout_root: _PathIdentitySnapshot
    rollouts_directory: _PathIdentitySnapshot | None
    cluster_config: _PathIdentitySnapshot
    backup_manifest: _PathIdentitySnapshot
    storage_root: _PathIdentitySnapshot | None


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
        cluster_config_path=Path("deploy/environments/staging.multinode.cluster.toml"),
        backup_manifest_path=Path(
            "/data/loom-staging/backups/latest/backup-manifest.json",
        ),
        rollout_root=Path(_STAGING_DATA_ROOT),
        admin_token_source=("file:/shared_work/qianyi/loom-worker-capacity/staging-admin-token"),
        worker_token_source=("file:/shared_work/qianyi/loom-worker-capacity/staging-worker-token"),
        service_token_source=(
            "file:/shared_work/qianyi/loom-worker-capacity/staging-service-token"
        ),
        smoke_submit_mode="admin-on-behalf",
        smoke_task_id="loom-smoke/gb10-oracle-hello-world",
        smoke_required_worker_pool="gb10",
        smoke_agent="oracle",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="env:LOOM_SMOKE_ON_BEHALF_TEAM_ID",
        smoke_admin_actor="codex-v1-release-gate",
        scope="current-gb10",
    ),
    # Explicit selector is reserved now, but first-prod values are not ready.
    "prod": RolloutPreset(name="prod", configured=False),
}


def _rollout_runner_dependency_error() -> str | None:
    missing: list[str] = []
    for module in _ROLLOUT_RUNNER_REQUIRED_MODULES:
        try:
            importlib.import_module(module)
        except ModuleNotFoundError as exc:
            missing.append(f"{module} ({exc.name})")
        except Exception as exc:
            missing.append(f"{module} ({type(exc).__name__}: {exc})")
    if not missing:
        return None
    return (
        "rollout runner missing benchmark tooling required by catalog "
        "provisioning: "
        + ", ".join(missing)
        + ". Run `uv sync --locked --all-packages --extra cluster "
        "--extra rollout --python 3.11` "
        "in the rollout driver checkout, then rerun or resume."
    )


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
        f"{flag_name}: literal values are rejected; use one of {{env:VAR | file:PATH}}",
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
    """Reject a logical environment that conflicts with physical target identity."""
    environment = str(args.environment).strip().lower()
    cluster_name = str(args.cluster_name).strip()
    namespace = str(args.namespace).strip()
    rollout_root = str(args.rollout_root).strip().rstrip("/")

    cluster_environment = _PROTECTED_PHYSICAL_TARGET_ENVIRONMENTS.get(
        cluster_name,
    )
    namespace_environment = _PROTECTED_PHYSICAL_TARGET_ENVIRONMENTS.get(
        namespace,
    )
    physical_environments = {
        value for value in (cluster_environment, namespace_environment) if value is not None
    }
    if len(physical_environments) > 1:
        return _PROTECTED_TARGET_MISMATCH_ERROR
    physical_environment = next(iter(physical_environments), None)
    if physical_environment is not None and environment != physical_environment:
        return _PROTECTED_TARGET_MISMATCH_ERROR

    protected_targets = {
        "staging": (_STAGING_CLUSTER_NAME, _STAGING_NAMESPACE),
        "production": (_PRODUCTION_CLUSTER_NAME, _PRODUCTION_NAMESPACE),
    }
    expected_target = protected_targets.get(environment)
    if expected_target is not None:
        if (cluster_name, namespace) != expected_target:
            return _PROTECTED_TARGET_MISMATCH_ERROR
        args.environment = environment

    if (
        environment == "staging"
        and rollout_root.startswith("/data/")
        and not (
            rollout_root == _STAGING_DATA_ROOT or rollout_root.startswith(f"{_STAGING_DATA_ROOT}/")
        )
    ):
        return _PROTECTED_TARGET_MISMATCH_ERROR
    return None


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
    try:
        persisted = evidence.read_inputs()
    except FileNotFoundError:
        return None
    resolved_sha = persisted.get("resolved_sha")
    if (
        persisted.get("target_ref") == target_ref
        and persisted.get("image_tag") == image_tag
        and isinstance(resolved_sha, str)
        and len(resolved_sha) == 40
    ):
        return resolved_sha
    return None


def _is_protected_staging_request(args: argparse.Namespace) -> bool:
    """Recognise staging from every protected physical identity field."""
    if _selector(args) == "staging":
        return True
    if str(getattr(args, "environment", "") or "").strip().lower() == "staging":
        return True
    if str(getattr(args, "cluster_name", "") or "").strip() == _STAGING_CLUSTER_NAME:
        return True
    if str(getattr(args, "namespace", "") or "").strip() == _STAGING_NAMESPACE:
        return True
    root = str(getattr(args, "rollout_root", "") or "").strip()
    if _rollout_root_is_protected_or_unsafe(root):
        return True
    cluster_config = str(getattr(args, "cluster_config", "") or "").strip()
    if cluster_config and _cluster_config_is_protected_or_unsafe(cluster_config):
        return True
    backup_manifest = str(getattr(args, "backup_manifest", "") or "").strip()
    return _rollout_root_is_protected_or_unsafe(backup_manifest)


def _is_staging_data_path(path: str) -> bool:
    return path == _STAGING_DATA_ROOT or path.startswith(f"{_STAGING_DATA_ROOT}/")


def _lexical_absolute_path(path: str) -> str:
    if not os.path.isabs(path):
        path = os.path.join(os.getcwd(), path)
    normalized = os.path.normpath(path)
    if normalized.startswith("//"):
        normalized = "/" + normalized.lstrip("/")
    return normalized


def _rollout_root_is_protected_or_unsafe(root: str) -> bool:
    """Recognise lexical/physical aliases without following untrusted links.

    Symlink targets are read and expanded component-by-component.  A loop or
    unreadable component fails closed into the envelope-only path.
    """
    if not root:
        return False
    try:
        if _is_staging_data_path(_lexical_absolute_path(root)):
            return True
        raw_absolute = root if os.path.isabs(root) else os.path.join(os.getcwd(), root)
    except (OSError, ValueError):
        return True

    try:
        protected_metadata = Path(_STAGING_DATA_ROOT).lstat()
    except OSError:
        protected_identity: tuple[int, int] | None = None
    else:
        protected_identity = None
        if not stat.S_ISLNK(protected_metadata.st_mode):
            protected_identity = (
                protected_metadata.st_dev,
                protected_metadata.st_ino,
            )

    pending = [part for part in raw_absolute.split("/") if part]
    resolved_parts: list[str] = []
    resolved_identities: list[tuple[int, int]] = []
    seen_expansions: set[tuple[tuple[str, ...], tuple[str, ...]]] = set()
    symlink_hops = 0

    while pending:
        part = pending.pop(0)
        if part == ".":
            continue
        if part == "..":
            if resolved_parts:
                resolved_parts.pop()
                resolved_identities.pop()
            continue

        candidate = Path("/").joinpath(*resolved_parts, part)
        try:
            metadata = candidate.lstat()
        except (OSError, ValueError):
            return True

        if metadata is not None and stat.S_ISLNK(metadata.st_mode):
            symlink_hops += 1
            if symlink_hops > 32:
                return True
            try:
                target = os.readlink(candidate)
            except (OSError, ValueError):
                return True
            target_parts = [component for component in target.split("/") if component]
            if os.path.isabs(target):
                resolved_parts.clear()
                resolved_identities.clear()
            pending = target_parts + pending
            expansion = (tuple(resolved_parts), tuple(pending))
            if expansion in seen_expansions:
                return True
            seen_expansions.add(expansion)
            continue

        resolved_parts.append(part)
        resolved_identities.append((metadata.st_dev, metadata.st_ino))

    physical = "/" + "/".join(resolved_parts)
    if _is_staging_data_path(physical):
        return True
    return protected_identity is not None and protected_identity in resolved_identities


def _read_bounded_nofollow_text(
    path: str,
    *,
    limit: int,
) -> tuple[str, tuple[int, int]]:
    """Read one regular file through no-follow dirfds with a hard byte cap."""
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    directory = getattr(os, "O_DIRECTORY", 0)
    if not no_follow or not directory:
        raise OSError("no-follow file inspection is unavailable")
    absolute = Path(_lexical_absolute_path(path))
    parts = absolute.parts[1:]
    if not parts:
        raise OSError("classifier input must be a file")
    base_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | no_follow
    descriptors: list[int] = []
    try:
        directory_fd = os.open("/", base_flags | directory)
        descriptors.append(directory_fd)
        for component in parts[:-1]:
            directory_fd = os.open(
                component,
                base_flags | directory,
                dir_fd=directory_fd,
            )
            descriptors.append(directory_fd)
        file_fd = os.open(
            parts[-1],
            base_flags | getattr(os, "O_NONBLOCK", 0),
            dir_fd=directory_fd,
        )
        descriptors.append(file_fd)
        metadata = os.fstat(file_fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise OSError("classifier input must be a regular file")
        if metadata.st_size > limit:
            raise OSError("classifier input exceeds its size limit")
        chunks: list[bytes] = []
        remaining = limit + 1
        while remaining:
            chunk = os.read(file_fd, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > limit:
            raise OSError("classifier input exceeds its size limit")
        return payload.decode("utf-8"), (metadata.st_dev, metadata.st_ino)
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _cluster_config_is_protected_or_unsafe(path: str) -> bool:
    known_staging_config = (
        _REPO_ROOT / "deploy" / "environments" / "staging.multinode.cluster.toml"
    )
    try:
        if _lexical_absolute_path(path) == str(known_staging_config):
            return True
        text, identity = _read_bounded_nofollow_text(
            path,
            limit=_MAX_CLASSIFIER_CLUSTER_CONFIG_BYTES,
        )
        try:
            known_metadata = known_staging_config.lstat()
        except OSError:
            known_identity: tuple[int, int] | None = None
        else:
            known_identity = None
            if stat.S_ISREG(known_metadata.st_mode):
                known_identity = (known_metadata.st_dev, known_metadata.st_ino)
        if known_identity is not None and identity == known_identity:
            return True
        raw = tomllib.loads(text)
    except (OSError, ValueError):
        return True

    return _cluster_config_mapping_is_protected_or_unsafe(raw)


def _cluster_config_mapping_is_protected_or_unsafe(raw: dict[str, object]) -> bool:
    """Classify the exact config mapping captured by no-follow admission."""

    for key in ("namespace", "runtime_environment", "environment", "cluster_name"):
        value = raw.get(key)
        if value is not None and not isinstance(value, str):
            return True
        normalized = str(value or "").strip()
        if key in {"runtime_environment", "environment"}:
            if normalized.lower() == "staging":
                return True
        elif normalized in {_STAGING_NAMESPACE, _STAGING_CLUSTER_NAME}:
            return True

    storage_root = raw.get("persistent_storage_host_path_root")
    if storage_root is None:
        return False
    if not isinstance(storage_root, str):
        return True
    return _rollout_root_is_protected_or_unsafe(storage_root.strip())


def _snapshot_nofollow_path(
    path: str,
    *,
    expected_kind: str,
    read_limit: int | None = None,
) -> tuple[_PathIdentitySnapshot, bytes | None]:
    """Open every component without following links and record its identity."""
    if expected_kind not in {"directory", "regular"}:
        raise ValueError("unsupported path identity kind")
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    directory = getattr(os, "O_DIRECTORY", 0)
    if not no_follow or not directory:
        raise OSError("no-follow path binding is unavailable")

    absolute = Path(_lexical_absolute_path(path))
    parts = absolute.parts[1:]
    if not parts:
        raise OSError("manual rollout paths may not name the filesystem root")

    base_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | no_follow
    descriptors: list[int] = []
    components: list[tuple[int, int, int]] = []
    payload: bytes | None = None
    try:
        current_fd = os.open("/", base_flags | directory)
        descriptors.append(current_fd)
        root_metadata = os.fstat(current_fd)
        components.append(
            (
                root_metadata.st_dev,
                root_metadata.st_ino,
                stat.S_IFMT(root_metadata.st_mode),
            )
        )
        for index, component in enumerate(parts):
            is_leaf = index == len(parts) - 1
            flags = base_flags
            if not is_leaf or expected_kind == "directory":
                flags |= directory
            else:
                flags |= getattr(os, "O_NONBLOCK", 0)
            current_fd = os.open(component, flags, dir_fd=current_fd)
            descriptors.append(current_fd)
            metadata = os.fstat(current_fd)
            file_type = stat.S_IFMT(metadata.st_mode)
            components.append((metadata.st_dev, metadata.st_ino, file_type))
            if is_leaf:
                if expected_kind == "directory" and not stat.S_ISDIR(metadata.st_mode):
                    raise OSError("manual rollout path must be a directory")
                if expected_kind == "regular" and not stat.S_ISREG(metadata.st_mode):
                    raise OSError("manual rollout path must be a regular file")
                if read_limit is not None:
                    if metadata.st_size > read_limit:
                        raise OSError("manual rollout file exceeds its size limit")
                    chunks: list[bytes] = []
                    remaining = read_limit + 1
                    while remaining:
                        chunk = os.read(current_fd, min(64 * 1024, remaining))
                        if not chunk:
                            break
                        chunks.append(chunk)
                        remaining -= len(chunk)
                    payload = b"".join(chunks)
                    if len(payload) > read_limit:
                        raise OSError("manual rollout file exceeds its size limit")
                    final_metadata = os.fstat(current_fd)
                    if (
                        final_metadata.st_dev,
                        final_metadata.st_ino,
                        final_metadata.st_size,
                        final_metadata.st_mtime_ns,
                    ) != (
                        metadata.st_dev,
                        metadata.st_ino,
                        metadata.st_size,
                        metadata.st_mtime_ns,
                    ):
                        raise OSError("manual rollout file changed while being read")
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)

    digest = hashlib.sha256(payload).hexdigest() if payload is not None else None
    return (
        _PathIdentitySnapshot(
            path=absolute,
            expected_kind=expected_kind,
            components=tuple(components),
            content_sha256=digest,
        ),
        payload,
    )


def _capture_manual_path_bindings(args: argparse.Namespace) -> _ManualPathBindings:
    if _rollout_root_is_protected_or_unsafe(str(args.rollout_root)):
        raise OSError("manual rollout root is protected or unsafe")
    rollout_root, _ = _snapshot_nofollow_path(
        str(args.rollout_root),
        expected_kind="directory",
    )
    try:
        rollouts_directory, _ = _snapshot_nofollow_path(
            str(rollout_root.path / "rollouts"),
            expected_kind="directory",
        )
    except FileNotFoundError:
        rollouts_directory = None
    cluster_config, cluster_config_bytes = _snapshot_nofollow_path(
        str(args.cluster_config),
        expected_kind="regular",
        read_limit=_MAX_CLASSIFIER_CLUSTER_CONFIG_BYTES,
    )
    if cluster_config_bytes is None:  # pragma: no cover - read_limit contract
        raise OSError("cluster config bytes were not captured")
    raw_config = tomllib.loads(cluster_config_bytes.decode("utf-8"))
    if _cluster_config_mapping_is_protected_or_unsafe(raw_config):
        raise OSError("manual cluster config is protected or unsafe")
    storage_root_value = raw_config.get("persistent_storage_host_path_root")
    storage_root: _PathIdentitySnapshot | None = None
    if storage_root_value is not None:
        if not isinstance(storage_root_value, str) or not storage_root_value.strip():
            raise OSError("cluster storage root must be a non-empty path")
        storage_root, _ = _snapshot_nofollow_path(
            storage_root_value.strip(),
            expected_kind="directory",
        )
    if _rollout_root_is_protected_or_unsafe(str(args.backup_manifest)):
        raise OSError("manual backup manifest is protected or unsafe")
    backup_manifest, _ = _snapshot_nofollow_path(
        str(args.backup_manifest),
        expected_kind="regular",
    )
    return _ManualPathBindings(
        rollout_root=rollout_root,
        rollouts_directory=rollouts_directory,
        cluster_config=cluster_config,
        backup_manifest=backup_manifest,
        storage_root=storage_root,
    )


def _manual_path_bindings_unchanged(
    args: argparse.Namespace,
    expected: _ManualPathBindings,
) -> bool:
    try:
        return _capture_manual_path_bindings(args) == expected
    except (OSError, UnicodeError, ValueError):
        return False


def _reject_changed_manual_path_bindings() -> int:
    sys.stderr.write(f"error: {_MANUAL_PATH_BINDING_ERROR}\n")
    return 2


def _has_manual_envelope_override(args: argparse.Namespace) -> bool:
    return bool(getattr(args, _EXPLICIT_OPTIONS_ATTR, ()))


def _handle_envelope_mode(args: argparse.Namespace) -> int:
    if _selector(args) != "staging":
        sys.stderr.write("error: request envelope mode requires the staging selector\n")
        return 2
    if _has_manual_envelope_override(args):
        sys.stderr.write("error: manual rollout overrides are forbidden in envelope mode\n")
        return 2

    try:
        config = OperatorConfig.load(fixed_operator_config_path())
        envelope_path = Path(args.request_envelope)
        envelope = load_validated_envelope(
            envelope_path,
            config,
            effective_uid=os.geteuid(),
        )
    except Exception as exc:
        reason = redact_rollout_text(str(exc), limit=500)
        sys.stderr.write(f"error: request envelope validation failed: {reason}\n")
        return 2

    if bool(args.resume) != envelope.resume:
        sys.stderr.write("error: resume flag does not match request envelope\n")
        return 2

    try:
        cluster_config_sha256 = sha256_of_file(config.cluster_config_path)
    except OSError:
        sys.stderr.write("error: configured cluster config is unavailable\n")
        return 2

    staging_preset = _ROLLOUT_PRESETS["staging"]
    evidence = EvidenceDirectory(config.rollout_root, envelope.rollout_id)
    backup_limits = operator_backup_traversal_limits(config)
    ctx = RolloutContext(
        image_tag=envelope.image_tag,
        target_ref=envelope.target_ref,
        resolved_sha=envelope.resolved_sha,
        cluster_name=envelope.cluster_name,
        namespace=envelope.namespace,
        environment=envelope.environment,
        cp_url=envelope.cp_url,
        admin_token_source=envelope.admin_token_source,
        expect_admin_token_fingerprint=envelope.expect_admin_token_fingerprint,
        worker_token_source=envelope.worker_token_source,
        service_token_source=envelope.service_token_source,
        smoke_submit_mode="admin-on-behalf",
        smoke_api_token_source=None,
        smoke_task_id=staging_preset.smoke_task_id,
        smoke_required_worker_pool=staging_preset.smoke_required_worker_pool,
        smoke_agent=staging_preset.smoke_agent,
        smoke_on_behalf_username=envelope.smoke_on_behalf_username,
        smoke_on_behalf_team_id=envelope.smoke_on_behalf_team_id,
        smoke_admin_actor=staging_preset.smoke_admin_actor,
        cluster_config_path=config.cluster_config_path,
        cluster_config_sha256=cluster_config_sha256,
        rollout_root=config.rollout_root,
        backup_manifest_path=Path(envelope.backup_manifest_path),
        backup_manifest_min_remaining_hours=2,
        backup_manifest_max_files=backup_limits.max_files,
        backup_manifest_max_entries=backup_limits.max_entries,
        backup_manifest_max_total_bytes=backup_limits.max_total_bytes,
        backup_manifest_sha256=envelope.backup_manifest_sha256,
        runner_config_sha256=envelope.runner_config_sha256,
        preflight_attestation_sha256=envelope.preflight_attestation_sha256,
        preflight_registry_sha256=envelope.preflight_registry_sha256,
        preflight_coverage_sha256=envelope.preflight_coverage_sha256,
        request_id=envelope.request_id,
        initiating_operator=envelope.initiating_operator,
        initiating_uid=envelope.initiating_uid,
        attempt_number=envelope.attempt_number,
        attempt_operator=envelope.attempt_operator,
        attempt_uid=envelope.attempt_uid,
        request_envelope_path=envelope_path,
        scope=envelope.scope,
        exclude_oldlab=False,
        gb10_prep_concurrency=envelope.gb10_prep_concurrency,
        resume=envelope.resume,
        source_mode=envelope.source_mode,
        resolved_tree=envelope.resolved_tree,
        approved_base_sha=envelope.approved_base_sha,
        metadata={"rollout_id": envelope.rollout_id},
    )

    dependency_error = _rollout_runner_dependency_error()
    if dependency_error is not None:
        sys.stderr.write(f"error: {redact_rollout_text(dependency_error)}\n")
        return 2
    try:
        return run_rollout(ctx, default_step_sequence(), evidence)
    except DriverError as exc:
        sys.stderr.write(f"error: {redact_rollout_text(str(exc))}\n")
        return 2


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
        "--request-envelope",
        default=None,
        help=argparse.SUPPRESS,
    )
    p.add_argument(
        "--ref",
        default=None,
        action=_ExplicitStoreAction,
        help="Git ref to resolve to a SHA (e.g. origin/dev, or a tag/sha).",
    )
    p.add_argument(
        "--image-tag",
        default=None,
        action=_ExplicitStoreAction,
        help=(
            "Target release image tag; the driver validates that the "
            "resolved --ref sha starts with the tag's `sha7` suffix "
            "(convention: `staging-<sha7>`)."
        ),
    )
    p.add_argument(
        "--cluster-name",
        default=None,
        action=_ExplicitStoreAction,
        help="Name of the target kind cluster (as in `kind get clusters`).",
    )
    p.add_argument(
        "--namespace",
        default=None,
        action=_ExplicitStoreAction,
        help="Kubernetes namespace. Defaults to `loom`.",
    )
    p.add_argument(
        "--environment",
        default=None,
        action=_ExplicitStoreAction,
        help=(
            "Protected environment name (e.g. staging). Used by the "
            "backup and release-gate steps to bind evidence to the "
            "operator's declared environment."
        ),
    )
    p.add_argument(
        "--cp-url",
        default=None,
        action=_ExplicitStoreAction,
        help=(
            "Operator-reachable Control Plane admin base URL used by rollout "
            "steps that call `loom admin ...`, for example "
            "http://control-node.lan:18081 or http://127.0.0.1:18081."
        ),
    )
    p.add_argument(
        "--admin-token",
        default=None,
        action=_ExplicitStoreAction,
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
        action=_ExplicitStoreAction,
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
        action=_ExplicitStoreAction,
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
        action=_ExplicitStoreAction,
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
        action=_ExplicitStoreAction,
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
        action=_ExplicitStoreAction,
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
        action=_ExplicitStoreAction,
        help=(
            "Optional explicit smoke task id. current-gb10 defaults to "
            "loom-smoke/gb10-oracle-hello-world."
        ),
    )
    p.add_argument(
        "--smoke-required-worker-pool",
        default=None,
        action=_ExplicitStoreAction,
        help=(
            "Optional worker-pool requirement for smoke submission. "
            "current-gb10 defaults to gb10 when the task id is not "
            "overridden."
        ),
    )
    p.add_argument(
        "--smoke-agent",
        default=None,
        action=_ExplicitStoreAction,
        help="Optional smoke agent name. Defaults to oracle.",
    )
    p.add_argument(
        "--smoke-on-behalf-username",
        default=None,
        action=_ExplicitStoreAction,
        help="Represented username for admin-on-behalf smoke mode.",
    )
    p.add_argument(
        "--smoke-on-behalf-team-id",
        default=None,
        action=_ExplicitStoreAction,
        help="Represented team id for admin-on-behalf smoke mode.",
    )
    p.add_argument(
        "--smoke-admin-actor",
        default=None,
        action=_ExplicitStoreAction,
        help="Audit actor string for admin-on-behalf smoke submissions.",
    )
    p.add_argument(
        "--cluster-config",
        default=None,
        action=_ExplicitStoreAction,
        help="Path to the operator's cluster-config.toml.",
    )
    p.add_argument(
        "--backup-manifest",
        default=None,
        action=_ExplicitStoreAction,
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
        action=_ExplicitStoreAction,
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
        action=_ExplicitStoreAction,
        help=(
            "Root of the evidence directory tree "
            "(created by `loom cluster bootstrap-evidence-paths`)."
        ),
    )
    p.add_argument(
        "--scope",
        default=None,
        action=_ExplicitStoreAction,
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
        action=_ExplicitStoreAction,
        help=(
            "Optional bounded host-level concurrency for rollout step 12 "
            "gb10-prep. Each host still runs its internal command sequence "
            "serially."
        ),
    )
    p.add_argument(
        "--exclude-oldlab",
        action=_ExplicitStoreTrueAction,
        help=(
            "Exclude the OLDLAB worker pool. Refused when "
            "--scope=full-cluster because you can't claim full-cluster "
            "acceptance while excluding a release-managed pool."
        ),
    )
    p.add_argument(
        "--resume",
        action="store_true",
        help=("Resume an in-progress rollout with matching --image-tag. Refuses if none is found."),
    )
    p.add_argument(
        "--dry-run",
        action=_ExplicitStoreTrueAction,
        help="Print the planned step sequence + inputs hash and exit.",
    )


def handle(args: argparse.Namespace) -> int:
    """Handler wired up from `loom cluster rollout`."""
    if args.request_envelope is not None:
        return _handle_envelope_mode(args)
    if _is_protected_staging_request(args) and not args.dry_run:
        sys.stderr.write(
            "error: broker-created request envelope is required for non-dry-run protected staging\n"
        )
        return 2
    if args.ref is None:
        sys.stderr.write("error: --ref is required in manual rollout mode\n")
        return 2

    manual_path_bindings: _ManualPathBindings | None = None
    manual_paths = (args.rollout_root, args.cluster_config, args.backup_manifest)
    if not args.dry_run and all(manual_paths):
        try:
            manual_path_bindings = _capture_manual_path_bindings(args)
        except (OSError, UnicodeError, ValueError):
            return _reject_changed_manual_path_bindings()

    selector_name = _selector(args)
    resolved_sha: str | None = None
    if selector_name and args.image_tag is None:
        preset = _ROLLOUT_PRESETS.get(selector_name)
        if preset is not None and preset.configured:
            if manual_path_bindings is not None and not _manual_path_bindings_unchanged(
                args,
                manual_path_bindings,
            ):
                return _reject_changed_manual_path_bindings()
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
    if manual_path_bindings is not None and not _manual_path_bindings_unchanged(
        args,
        manual_path_bindings,
    ):
        return _reject_changed_manual_path_bindings()
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

    cluster_config_path = (
        manual_path_bindings.cluster_config.path
        if manual_path_bindings is not None
        else Path(args.cluster_config)
    )
    if manual_path_bindings is None and not cluster_config_path.is_file():
        sys.stderr.write(f"error: cluster-config not found: {cluster_config_path}\n")
        return 2
    if args.backup_manifest_min_remaining_hours < 0:
        sys.stderr.write("error: --backup-manifest-min-remaining-hours must be >= 0\n")
        return 2
    if args.gb10_prep_concurrency is not None and args.gb10_prep_concurrency < 1:
        sys.stderr.write("error: --gb10-prep-concurrency must be >= 1\n")
        return 2
    cfg_sha = (
        manual_path_bindings.cluster_config.content_sha256
        if manual_path_bindings is not None
        else sha256_of_file(cluster_config_path)
    )
    if cfg_sha is None:  # pragma: no cover - manual config snapshot contract
        return _reject_changed_manual_path_bindings()
    rollout_root = (
        manual_path_bindings.rollout_root.path
        if manual_path_bindings is not None
        else Path(args.rollout_root)
    )

    if manual_path_bindings is not None and not _manual_path_bindings_unchanged(
        args,
        manual_path_bindings,
    ):
        return _reject_changed_manual_path_bindings()

    # Choose evidence dir: resume finds an existing one; new invocations
    # create one keyed by (image_tag, launch timestamp).
    if args.resume:
        found = EvidenceDirectory.find_in_progress(
            rollout_root,
            image_tag=args.image_tag,
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
            rollout_root,
            image_tag=args.image_tag,
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

    try:
        persisted_sha = (
            _persisted_resume_sha(
                evidence=evidence,
                target_ref=args.ref,
                image_tag=args.image_tag,
            )
            if args.resume
            else None
        )
    except (OSError, UnicodeError, ValueError):
        sys.stderr.write("error: manual resume inputs are unavailable or unsafe\n")
        return 2
    if persisted_sha is not None:
        resolved_sha = persisted_sha
    if resolved_sha is None:
        if manual_path_bindings is not None and not _manual_path_bindings_unchanged(
            args,
            manual_path_bindings,
        ):
            return _reject_changed_manual_path_bindings()
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
        backup_manifest_path=(
            manual_path_bindings.backup_manifest.path
            if manual_path_bindings is not None
            else Path(args.backup_manifest)
        ),
        backup_manifest_min_remaining_hours=(args.backup_manifest_min_remaining_hours),
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
        sys.stdout.write(f"rollout_id: {rollout_id}\nresolved_sha: {resolved_sha}\n")
        for key, value in _dry_run_inputs(ctx, preset_name=preset_name):
            sys.stdout.write(f"{key}: {value}\n")
        sys.stdout.write("steps:\n")
        for step in steps:
            sys.stdout.write(f"  {step.number:02d} {step.name}\n")
        return 0

    dependency_error = _rollout_runner_dependency_error()
    if dependency_error is not None:
        sys.stderr.write(f"error: {dependency_error}\n")
        return 2

    if manual_path_bindings is not None and not _manual_path_bindings_unchanged(
        args,
        manual_path_bindings,
    ):
        return _reject_changed_manual_path_bindings()

    try:
        return run_rollout(ctx, steps, evidence)
    except DriverError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
