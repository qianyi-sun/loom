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
DEFAULT_TRUSTED_CANDIDATE_REF = "origin/dev"
IN_PROGRESS_GIT_PATHS = (
    "MERGE_HEAD",
    "rebase-merge",
    "rebase-apply",
    "CHERRY_PICK_HEAD",
    "REVERT_HEAD",
    "BISECT_START",
    "sequencer",
)


class ReleaseIdentityError(ValueError):
    """Raised when release source identity cannot be proven."""


@dataclass(frozen=True)
class ReleaseIdentity:
    candidate_sha: str
    candidate_tree: str
    trusted_candidate_ref: str
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


def _resolve_trusted_commit(repository: Path, trusted_candidate_ref: str) -> str:
    if not trusted_candidate_ref.strip():
        raise ReleaseIdentityError("trusted candidate ref must be non-empty")
    resolved = _run_git(
        repository,
        "rev-parse",
        "--verify",
        "--end-of-options",
        f"{trusted_candidate_ref}^{{commit}}",
    )
    trusted_sha = resolved.stdout.strip()
    if resolved.returncode != 0 or SHA_RE.fullmatch(trusted_sha) is None:
        raise ReleaseIdentityError(
            f"trusted candidate ref {trusted_candidate_ref} does not resolve to a commit"
        )
    return trusted_sha


def _require_candidate_reachable(
    repository: Path,
    *,
    candidate_sha: str,
    trusted_candidate_ref: str,
    trusted_sha: str,
) -> None:
    reachable = _run_git(repository, "merge-base", "--is-ancestor", candidate_sha, trusted_sha)
    if reachable.returncode == 1:
        raise ReleaseIdentityError(
            f"candidate_sha is not reachable from trusted candidate ref {trusted_candidate_ref}"
        )
    if reachable.returncode != 0:
        raise ReleaseIdentityError("failed to verify candidate reachability")


def _require_release_is_head(repository: Path, release_sha: str) -> None:
    head = _run_git(repository, "rev-parse", "--verify", "HEAD^{commit}")
    head_sha = head.stdout.strip()
    if head.returncode != 0 or SHA_RE.fullmatch(head_sha) is None:
        raise ReleaseIdentityError("HEAD does not resolve to a commit")
    if release_sha != head_sha:
        raise ReleaseIdentityError(
            f"release_sha must exactly match HEAD: release={release_sha} head={head_sha}"
        )


def _git_path(repository: Path, name: str) -> Path:
    result = _run_git(repository, "rev-parse", "--git-path", name)
    if result.returncode != 0 or not result.stdout.strip():
        raise ReleaseIdentityError(f"failed to resolve Git operation path {name}")
    path = Path(result.stdout.strip())
    return path if path.is_absolute() else repository / path


def _require_clean_release_worktree(repository: Path) -> None:
    active_operations = [
        name for name in IN_PROGRESS_GIT_PATHS if _git_path(repository, name).exists()
    ]
    if active_operations:
        raise ReleaseIdentityError("Git operation is in progress: " + ", ".join(active_operations))

    if (repository / ".env").exists():
        raise ReleaseIdentityError("repository root .env is forbidden during release verification")

    status = _run_git(
        repository,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        "--ignored=no",
    )
    if status.returncode != 0:
        raise ReleaseIdentityError("failed to inspect release worktree status")
    if status.stdout:
        raise ReleaseIdentityError("repository contains staged, unstaged, or untracked changes")


def verify_release_identity(
    *,
    repository: Path,
    candidate_sha: str,
    release_sha: str,
    trusted_candidate_ref: str = DEFAULT_TRUSTED_CANDIDATE_REF,
) -> ReleaseIdentity:
    repository = repository.resolve()
    _require_repository(repository)
    candidate_tree = _commit_tree(repository, candidate_sha, "candidate_sha")
    trusted_sha = _resolve_trusted_commit(repository, trusted_candidate_ref)
    _require_candidate_reachable(
        repository,
        candidate_sha=candidate_sha,
        trusted_candidate_ref=trusted_candidate_ref,
        trusted_sha=trusted_sha,
    )
    release_tree = _commit_tree(repository, release_sha, "release_sha")
    _require_release_is_head(repository, release_sha)
    _require_clean_release_worktree(repository)

    if candidate_tree != release_tree:
        raise ReleaseIdentityError(
            "candidate tree does not match release tree: "
            f"candidate={candidate_tree} release={release_tree}"
        )

    return ReleaseIdentity(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        trusted_candidate_ref=trusted_candidate_ref,
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
    verify.add_argument(
        "--trusted-candidate-ref",
        default=DEFAULT_TRUSTED_CANDIDATE_REF,
        help="Trusted branch/ref that must contain the candidate commit.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        identity = verify_release_identity(
            repository=args.repository,
            candidate_sha=args.candidate_sha,
            release_sha=args.release_sha,
            trusted_candidate_ref=args.trusted_candidate_ref,
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
