"""Candidate worktree invocation helpers for rollout-owned Loom commands."""

from __future__ import annotations

import os
import subprocess
import sys
import tomllib
from pathlib import Path

import tomli_w

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir


class CandidateToolingError(RuntimeError):
    """Raised when the resolved rollout worktree cannot run Loom tooling."""


def candidate_worktree(step_dir: StepDir) -> Path:
    """Return the resolved candidate worktree path from a later step dir."""
    return step_dir.path.parent / "01-worktree" / "src"


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
    return [sys.executable, "-m", "loom_cli", *args]


def candidate_loom_env(step_dir: StepDir) -> dict[str, str]:
    """Return an environment that imports ``loom_cli`` from the worktree."""
    worktree = validate_candidate_loom_source(step_dir)
    candidate_src = str(worktree / "src")
    env = os.environ.copy()
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = candidate_src if not existing else f"{candidate_src}{os.pathsep}{existing}"
    tool_bin = str(Path(sys.executable).parent)
    existing_path = env.get("PATH")
    env["PATH"] = tool_bin if not existing_path else f"{tool_bin}{os.pathsep}{existing_path}"
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

    The rollout CLI owns ``ctx.image_tag``. The operator's source config may
    intentionally be long-lived and stale, so cluster render/apply/gate steps
    must consume a rollout-local config that pins the target image tag. Paths
    that were relative to the source config must also be made rollout-stable so
    later steps do not resolve them relative to the evidence directory.
    """
    target = rollout_cluster_config_path(step_dir)
    if target.is_file():
        try:
            raw = tomllib.loads(target.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as exc:
            raise CandidateToolingError(
                f"failed to read rollout-local cluster config: {exc}",
            ) from exc
        if _rewrite_rollout_local_cluster_config_paths(
            raw,
            source_config_path=ctx.cluster_config_path,
            step_dir=step_dir,
        ):
            target.write_text(tomli_w.dumps(raw), encoding="utf-8")
        return target
    try:
        raw = tomllib.loads(ctx.cluster_config_path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise CandidateToolingError(
            f"failed to read cluster config for rollout synthesis: {exc}",
        ) from exc
    raw["image_tag"] = ctx.image_tag
    _rewrite_rollout_local_cluster_config_paths(
        raw,
        source_config_path=ctx.cluster_config_path,
        step_dir=step_dir,
    )
    target.write_text(tomli_w.dumps(raw), encoding="utf-8")
    return target
