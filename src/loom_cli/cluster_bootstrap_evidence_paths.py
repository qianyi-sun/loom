"""Emit `sudo install -d` script for operator-writable rollout evidence paths.

Fixes #174. During #160/#158/#161 public-beta rollout validation after the
#139 storage migration, the operator user could not create rollout evidence
directories under `/data/loom-public-beta` because the top-level directory is
`root:root 755`. Operators worked around it with ad-hoc `sudo install -d`
invocations, which is not repeatable across environments.

This module renders a small idempotent bash script that:

* Creates the requested operator-writable directories with a single owner
  and mode (`install -d -o <user> -g <user> -m 755 <root>/<name>`).
* Refuses to touch service-owned siblings (postgres, minio, backups,
  trajectories, migrations) so the setup step can never widen access to
  data-plane state.
* Is idempotent by construction — `install -d` is a no-op on an existing
  directory that already matches the requested owner/mode. Rerunning after
  a partial creation converges without deleting anything.

The rendered script is meant to be piped through `sudo bash -` or reviewed
first and run by hand.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

# Sibling directories under /data/<env> that hold service data-plane state.
# The setup script must never emit an `install -d` for these because their
# ownership/mode is deliberately locked down (see #174 body for the ls
# evidence from platform-dev). Any attempt to include them is a bug in the
# caller's config, not a case the tool should silently paper over.
RESERVED_SERVICE_DIRS: frozenset[str] = frozenset({
    "backups",
    "migrations",
    "minio",
    "postgres",
    "trajectories",
})

# POSIX portable username: leading [a-z_], remainder [a-z0-9_-]. Length cap
# is generous (accounts of 32 chars are the useradd default upper bound).
# We're not trying to be a full useradd validator; we're stopping shell
# injection at the boundary before we splice into a script.
_USERNAME_RE = re.compile(r"^[a-z_][a-z0-9_-]{0,31}$")

# Default set of operator-writable evidence directories that a public-beta
# or staging rollout expects to exist under the rollout root.
DEFAULT_EVIDENCE_PATHS: tuple[str, ...] = ("rollouts", "evidence", "logs")


class ServiceDirCollisionError(ValueError):
    """Raised when a requested evidence path collides with a service dir."""


@dataclass(frozen=True, slots=True)
class BootstrapPlan:
    """Rendered plan: script text + the paths it would create."""

    script: str
    rollout_root: Path
    operator_user: str
    evidence_paths: tuple[str, ...]


def render_bootstrap_script(
    *,
    rollout_root: Path,
    operator_user: str,
    evidence_paths: Iterable[str],
) -> str:
    """Return the sudo-install bash script text.

    Args:
        rollout_root: Absolute path to the environment's data root.
        operator_user: Non-empty POSIX username to own the created dirs.
        evidence_paths: Leaf names to create under ``rollout_root``.

    Raises:
        ValueError: On empty/invalid inputs (relative root, bad username,
            empty evidence list, non-leaf name).
        ServiceDirCollisionError: If any requested name is in
            :data:`RESERVED_SERVICE_DIRS`.
    """
    if not rollout_root.is_absolute():
        raise ValueError(
            f"rollout_root must be absolute; got {rollout_root!r}. "
            "Pass a full path like /data/loom-public-beta."
        )
    if not operator_user:
        raise ValueError("operator_user must be non-empty")
    if not _USERNAME_RE.match(operator_user):
        raise ValueError(
            f"invalid operator_user {operator_user!r} — expected a POSIX "
            "username matching [a-z_][a-z0-9_-]* (max 32 chars)."
        )
    paths = tuple(evidence_paths)
    if not paths:
        raise ValueError("evidence_paths must not be empty")
    for name in paths:
        if not name:
            raise ValueError("evidence_paths entries must not be empty")
        if "/" in name or name in (".", ".."):
            raise ValueError(
                f"evidence_paths entries must be leaf names (no /); "
                f"got {name!r}"
            )
    collisions = sorted(set(paths) & RESERVED_SERVICE_DIRS)
    if collisions:
        raise ServiceDirCollisionError(
            "refusing to bootstrap evidence dirs that collide with reserved "
            f"service directories: {', '.join(collisions)}. These dirs hold "
            "service data-plane state (postgres, minio, backup contents, "
            "etc.) and must not have their ownership rewritten by an "
            "operator-evidence bootstrap step."
        )

    lines: list[str] = [
        "#!/bin/bash",
        "# loom cluster bootstrap-evidence-paths (#174).",
        "#",
        f"# operator: {operator_user}",
        f"# rollout_root: {rollout_root.as_posix()}",
        "#",
        "# `install -d` is idempotent — rerunning after a partial creation",
        "# converges without deleting anything. Review before piping to",
        "# `sudo bash -`.",
        "set -euo pipefail",
        "",
    ]
    for name in paths:
        target = rollout_root / name
        lines.append(
            f"sudo install -d -o {operator_user} -g {operator_user} -m 755 "
            f"{target.as_posix()}"
        )
    lines.append("")
    return "\n".join(lines)


def render_bootstrap_plan(
    *,
    rollout_root: Path,
    operator_user: str,
    evidence_paths: Iterable[str] | None = None,
) -> BootstrapPlan:
    """Convenience wrapper — returns the plan object (script + inputs)."""
    paths = tuple(evidence_paths) if evidence_paths is not None else DEFAULT_EVIDENCE_PATHS
    script = render_bootstrap_script(
        rollout_root=rollout_root,
        operator_user=operator_user,
        evidence_paths=paths,
    )
    return BootstrapPlan(
        script=script,
        rollout_root=rollout_root,
        operator_user=operator_user,
        evidence_paths=paths,
    )
