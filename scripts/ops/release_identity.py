#!/usr/bin/env python3
"""Verify that a promoted candidate and release commit have the same Git tree."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

SHA_RE = re.compile(r"^[0-9a-f]{40}$")


class ReleaseIdentityError(ValueError):
    """Raised when release source identity cannot be proven."""


@dataclass(frozen=True)
class ReleaseIdentity:
    candidate_sha: str
    candidate_tree: str
    release_sha: str
    release_tree: str


def _run_git(repository: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repository), *args],
        text=True,
        capture_output=True,
        check=False,
    )


def _require_repository(repository: Path) -> None:
    result = _run_git(repository, "rev-parse", "--is-inside-work-tree")
    if result.returncode != 0 or result.stdout.strip() != "true":
        raise ReleaseIdentityError(f"repository is not a Git worktree: {repository}")


def _commit_tree(repository: Path, sha: str, field: str) -> str:
    if SHA_RE.fullmatch(sha) is None:
        raise ReleaseIdentityError(f"{field} must be a 40-character lowercase Git SHA")

    object_type = _run_git(repository, "cat-file", "-t", sha)
    if object_type.returncode != 0:
        raise ReleaseIdentityError(f"{field} does not identify a known Git object")
    actual_type = object_type.stdout.strip()
    if actual_type != "commit":
        raise ReleaseIdentityError(f"{field} must identify a commit, found {actual_type}")

    tree = _run_git(repository, "rev-parse", "--verify", f"{sha}^{{tree}}")
    tree_sha = tree.stdout.strip()
    if tree.returncode != 0 or SHA_RE.fullmatch(tree_sha) is None:
        raise ReleaseIdentityError(f"failed to resolve {field} tree")
    return tree_sha


def verify_release_identity(
    *,
    repository: Path,
    candidate_sha: str,
    release_sha: str,
) -> ReleaseIdentity:
    repository = repository.resolve()
    _require_repository(repository)
    candidate_tree = _commit_tree(repository, candidate_sha, "candidate_sha")
    release_tree = _commit_tree(repository, release_sha, "release_sha")

    tracked_diff = _run_git(repository, "diff", "--quiet", release_sha, "--")
    if tracked_diff.returncode == 1:
        raise ReleaseIdentityError("tracked worktree differs from release commit")
    if tracked_diff.returncode != 0:
        raise ReleaseIdentityError("failed to verify tracked worktree against release commit")

    if candidate_tree != release_tree:
        raise ReleaseIdentityError(
            "candidate tree does not match release tree: "
            f"candidate={candidate_tree} release={release_tree}"
        )

    return ReleaseIdentity(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        release_sha=release_sha,
        release_tree=release_tree,
    )


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    verify = subparsers.add_parser("verify", help="Verify candidate and release tree identity.")
    verify.add_argument("--repository", type=Path, default=Path.cwd())
    verify.add_argument("--candidate-sha", required=True)
    verify.add_argument("--release-sha", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        identity = verify_release_identity(
            repository=args.repository,
            candidate_sha=args.candidate_sha,
            release_sha=args.release_sha,
        )
    except ReleaseIdentityError as exc:
        print(f"Release identity verification: FAIL\n- {exc}", file=sys.stderr)
        return 1

    print(
        "Release identity verification: PASS "
        f"candidate_tree={identity.candidate_tree} release_tree={identity.release_tree}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
