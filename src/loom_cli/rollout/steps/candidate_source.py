"""Candidate worktree invocation helpers for rollout-owned Loom commands."""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path

import tomli_w

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir


class CandidateToolingError(RuntimeError):
    """Raised when the resolved rollout worktree cannot run Loom tooling."""


_CANDIDATE_RUNPY_LAUNCHER = (
    "import runpy,sys;"
    "sys.path.insert(0,'src');"
    "sys.argv=['loom',*sys.argv[1:]];"
    "runpy.run_module('loom_cli',run_name='__main__')"
)
_FIXED_PATH = "/usr/local/bin:/usr/bin:/bin"
_PASSTHROUGH_ENV_KEYS = (
    "XDG_RUNTIME_DIR",
    "DBUS_SESSION_BUS_ADDRESS",
    "KUBECONFIG",
    "LOOM_STAGING_ROLLOUT_CONFIG",
)


def _fixed_candidate_environment() -> dict[str, str]:
    """Build the complete candidate environment without inherited injection."""
    tool_bin = str(Path(sys.executable).parent)
    env = {
        "HOME": "/var/lib/loom-staging-rollout",
        "USER": "loom-rollout",
        "LOGNAME": "loom-rollout",
        "PATH": f"{tool_bin}:{_FIXED_PATH}",
        "LC_ALL": "C.UTF-8",
    }
    for name in _PASSTHROUGH_ENV_KEYS:
        value = os.environ.get(name)
        if value:
            env[name] = value
    return env


_FULL_GIT_SHA_RE = re.compile(r"[0-9a-fA-F]{40}")
_UNTRUSTED_GIT_ENV_KEYS = frozenset(
    {
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_COMMON_DIR",
        "GIT_CONFIG",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_PARAMETERS",
        "GIT_CONFIG_SYSTEM",
        "GIT_DIR",
        "GIT_EXEC_PATH",
        "GIT_GRAFT_FILE",
        "GIT_INDEX_FILE",
        "GIT_NAMESPACE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_REPLACE_REF_BASE",
        "GIT_SHALLOW_FILE",
        "GIT_WORK_TREE",
    },
)


@dataclass(frozen=True, slots=True)
class MaterializedCandidateBlob:
    """Commit-bound bytes and their rollout evidence location."""

    data: bytes
    evidence_path: Path


def _candidate_git_env() -> dict[str, str]:
    """Return a Git environment without caller-controlled object/config hooks."""
    env = _fixed_candidate_environment()
    for key in list(env):
        if (
            key in _UNTRUSTED_GIT_ENV_KEYS
            or key.startswith("GIT_CONFIG_KEY_")
            or key.startswith("GIT_CONFIG_VALUE_")
            or key.startswith("GIT_TRACE")
        ):
            env.pop(key, None)
    env["GIT_NO_REPLACE_OBJECTS"] = "1"
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    env["GIT_CONFIG_GLOBAL"] = os.devnull
    env["GIT_ATTR_NOSYSTEM"] = "1"
    env["GIT_NO_LAZY_FETCH"] = "1"
    env["GIT_TERMINAL_PROMPT"] = "0"
    return env


def _candidate_git_argv(worktree: Path, *args: str) -> list[str]:
    return [
        "git",
        "--no-replace-objects",
        "-c",
        "core.fsmonitor=false",
        "-c",
        f"core.hooksPath={os.devnull}",
        "-C",
        str(worktree),
        *args,
    ]


def candidate_worktree(step_dir: StepDir) -> Path:
    """Return the resolved candidate worktree path from a later step dir."""
    return step_dir.path.parent / "01-worktree" / "src"


def candidate_worktree_from_context(ctx: RolloutContext) -> Path:
    """Return the candidate worktree bound to the context's rollout identity."""
    rollout_id = ctx.metadata.get("rollout_id", "").strip()
    if not rollout_id:
        raise CandidateToolingError(
            "rollout metadata is missing required rollout_id for candidate source",
        )
    worktree = ctx.rollout_root / "rollouts" / rollout_id / "01-worktree" / "src"
    if not worktree.is_dir():
        raise CandidateToolingError(f"candidate worktree does not exist: {worktree}")
    return worktree


def validate_candidate_worktree_identity(ctx: RolloutContext) -> Path:
    """Fail closed unless the rollout worktree is the clean resolved commit."""
    worktree = candidate_worktree_from_context(ctx)
    resolved_sha = ctx.resolved_sha.strip().lower()
    if _FULL_GIT_SHA_RE.fullmatch(resolved_sha) is None:
        raise CandidateToolingError(
            "resolved candidate SHA must be a full 40-character Git commit",
        )
    try:
        head = subprocess.run(
            _candidate_git_argv(
                worktree,
                "rev-parse",
                "--verify",
                "HEAD^{commit}",
            ),
            capture_output=True,
            text=True,
            check=False,
            env=_candidate_git_env(),
        )
    except OSError as exc:
        raise CandidateToolingError(
            f"could not inspect candidate worktree HEAD: {exc}",
        ) from exc
    if head.returncode != 0:
        detail = head.stderr.strip().splitlines()[-1] if head.stderr.strip() else "unknown"
        raise CandidateToolingError(
            f"could not resolve candidate worktree HEAD: {detail[:240]}",
        )
    actual_head = head.stdout.strip().lower()
    if actual_head != resolved_sha:
        raise CandidateToolingError(
            "candidate worktree HEAD does not match resolved rollout SHA: "
            f"expected {resolved_sha}, got {actual_head or 'unknown'}",
        )
    try:
        status = subprocess.run(
            _candidate_git_argv(
                worktree,
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            ),
            capture_output=True,
            text=True,
            check=False,
            env=_candidate_git_env(),
        )
    except OSError as exc:
        raise CandidateToolingError(
            f"could not inspect candidate worktree cleanliness: {exc}",
        ) from exc
    if status.returncode != 0:
        detail = status.stderr.strip().splitlines()[-1] if status.stderr.strip() else "unknown"
        raise CandidateToolingError(
            f"could not inspect candidate worktree cleanliness: {detail[:240]}",
        )
    if status.stdout.strip():
        raise CandidateToolingError(
            "candidate worktree is dirty; refusing to materialize rollout inputs",
        )
    return worktree


def materialize_candidate_blob(
    ctx: RolloutContext,
    repo_path: Path,
    target: Path,
) -> MaterializedCandidateBlob:
    """Materialize one blob from the clean, resolved candidate into evidence.

    The returned path contains bytes read from Git's object database, not from
    the mutable worktree. Repeated calls replace the evidence file atomically
    with the same commit-bound bytes.
    """
    if repo_path.is_absolute() or ".." in repo_path.parts:
        raise CandidateToolingError(
            f"candidate blob path must be repository-relative: {repo_path}",
        )
    worktree = validate_candidate_worktree_identity(ctx)
    object_name = f"{ctx.resolved_sha.lower()}:{repo_path.as_posix()}"
    try:
        blob = subprocess.run(
            _candidate_git_argv(
                worktree,
                "show",
                "--no-ext-diff",
                "--no-textconv",
                object_name,
            ),
            capture_output=True,
            check=False,
            env=_candidate_git_env(),
        )
    except OSError as exc:
        raise CandidateToolingError(
            f"could not read candidate blob {repo_path}: {exc}",
        ) from exc
    if blob.returncode != 0:
        stderr = blob.stderr.decode("utf-8", errors="replace").strip()
        detail = stderr.splitlines()[-1] if stderr else "blob is missing"
        raise CandidateToolingError(
            f"could not read candidate blob {repo_path} at resolved SHA: {detail[:240]}",
        )

    materialized = MaterializedCandidateBlob(
        data=blob.stdout,
        evidence_path=target.absolute(),
    )
    persist_materialized_candidate_blob(materialized)
    return materialized


def persist_materialized_candidate_blob(blob: MaterializedCandidateBlob) -> Path:
    """Atomically restore evidence from already captured commit-bound bytes."""
    target = blob.evidence_path
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=target.parent,
            prefix=f".{target.name}.",
            delete=False,
        ) as handle:
            handle.write(blob.data)
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        os.replace(temporary, target)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return target.resolve()


def validate_candidate_loom_source(step_dir: StepDir) -> Path:
    """Ensure the candidate checkout has importable ``loom_cli`` source."""
    worktree = candidate_worktree(step_dir)
    marker = worktree / "src" / "loom_cli" / "__main__.py"
    if not marker.is_file():
        raise CandidateToolingError(
            "candidate Loom CLI source is not importable from "
            f"{worktree}: expected {marker}. Re-run step 01 or use a "
            "candidate ref that includes compatible operator tooling."
        )
    return worktree


def candidate_loom_argv(*args: str) -> list[str]:
    """Run the candidate checkout's ``loom_cli`` package with this Python."""
    return [sys.executable, "-I", "-c", _CANDIDATE_RUNPY_LAUNCHER, *args]


def candidate_loom_env(step_dir: StepDir) -> dict[str, str]:
    """Return an environment that imports ``loom_cli`` from the worktree."""
    worktree = validate_candidate_loom_source(step_dir)
    env = _fixed_candidate_environment()
    env["LOOM_ROLLOUT_CANDIDATE_WORKTREE"] = str(worktree)
    return env


def candidate_loom_cwd(step_dir: StepDir) -> Path:
    """Return the cwd for candidate Loom command execution."""
    return validate_candidate_loom_source(step_dir)


def candidate_relative_path(path: Path, step_dir: StepDir) -> Path:
    """Map repo-local absolute paths to the candidate worktree when possible.

    Relative paths are intentionally left relative so running with
    ``cwd=<candidate worktree>`` resolves them inside the candidate checkout.
    Absolute paths outside a git worktree, or paths missing from the candidate,
    are preserved because they may be operator-owned evidence/config paths.
    """
    if not path.is_absolute():
        return path
    try:
        proc = subprocess.run(
            ["git", "-C", str(path.parent), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=False,
            env=_fixed_candidate_environment(),
        )
    except OSError:
        return path
    if proc.returncode != 0:
        return path
    repo_root = Path(proc.stdout.strip())
    try:
        relative = path.relative_to(repo_root)
    except ValueError:
        return path
    candidate = candidate_worktree(step_dir) / relative
    return candidate if candidate.exists() else path


def rollout_cluster_config_path(step_dir: StepDir) -> Path:
    """Return the rollout-owned cluster config artifact path."""
    return step_dir.path.parent / "rollout-cluster-config.toml"


def _rollout_local_path(value: object, *, source_config_path: Path, step_dir: StepDir) -> str:
    raw = str(value or "").strip()
    if not raw:
        return raw
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = source_config_path.parent / path
    return str(candidate_relative_path(path.resolve(strict=False), step_dir))


def _rewrite_rollout_local_cluster_config_paths(
    raw: dict[str, object],
    *,
    source_config_path: Path,
    step_dir: StepDir,
) -> bool:
    gb10_pool = raw.get("gb10_pool")
    if not isinstance(gb10_pool, dict):
        return False
    changed = False
    for key in ("ssh_config", "ssh_identity_file", "ssh_certificate_file"):
        value = gb10_pool.get(key)
        if value:
            updated = _rollout_local_path(
                value,
                source_config_path=source_config_path,
                step_dir=step_dir,
            )
            if updated != value:
                gb10_pool[key] = updated
                changed = True
    return changed


def rollout_cluster_config(ctx: RolloutContext, step_dir: StepDir) -> Path:
    """Write and return the per-rollout cluster config artifact.

    The rollout CLI owns ``ctx.image_tag``. The runner checkout may be stale,
    so cluster render/apply/gate steps must synthesize from the resolved
    candidate's repo-local profile when it exists, then pin the target image
    tag. Paths that were relative to that source config must also be made
    rollout-stable so later steps do not resolve them relative to the evidence
    directory.
    """
    target = rollout_cluster_config_path(step_dir)
    source_config_path = candidate_relative_path(ctx.cluster_config_path, step_dir)
    if target.is_file():
        try:
            raw = tomllib.loads(target.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as exc:
            raise CandidateToolingError(
                f"failed to read rollout-local cluster config: {exc}",
            ) from exc
        if _rewrite_rollout_local_cluster_config_paths(
            raw,
            source_config_path=source_config_path,
            step_dir=step_dir,
        ):
            target.write_text(tomli_w.dumps(raw), encoding="utf-8")
        return target
    try:
        raw = tomllib.loads(source_config_path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise CandidateToolingError(
            f"failed to read cluster config for rollout synthesis: {exc}",
        ) from exc
    raw["image_tag"] = ctx.image_tag
    _rewrite_rollout_local_cluster_config_paths(
        raw,
        source_config_path=source_config_path,
        step_dir=step_dir,
    )
    target.write_text(tomli_w.dumps(raw), encoding="utf-8")
    return target
