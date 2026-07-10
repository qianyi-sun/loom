from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/ops/release_identity.py"
PRODUCTION_VERIFY_SCRIPT = REPO_ROOT / "scripts/ops/verify_production_release_gate.sh"


def _git(repo: Path, *args: str, input_text: str | None = None) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        input=input_text,
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout.strip()


def _squash_equivalent_repo(tmp_path: Path) -> tuple[Path, str, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "--initial-branch=main")
    _git(repo, "config", "user.name", "Release Identity Test")
    _git(repo, "config", "user.email", "release-identity@example.invalid")

    tracked = repo / "tracked.txt"
    tracked.write_text("base\n", encoding="utf-8")
    (repo / ".gitignore").write_text(".env\n.venv/\n", encoding="utf-8")
    _git(repo, "add", "tracked.txt", ".gitignore")
    _git(repo, "commit", "-m", "base")

    _git(repo, "switch", "-c", "dev")
    tracked.write_text("candidate\n", encoding="utf-8")
    _git(repo, "commit", "-am", "candidate")
    candidate_sha = _git(repo, "rev-parse", "HEAD")
    _git(repo, "update-ref", "refs/remotes/origin/dev", candidate_sha)

    _git(repo, "switch", "main")
    _git(repo, "merge", "--squash", "dev")
    _git(repo, "commit", "-m", "squash promotion")
    release_sha = _git(repo, "rev-parse", "HEAD")
    return repo, candidate_sha, release_sha


def _run_identity(
    repo: Path,
    *,
    candidate_sha: str,
    release_sha: str,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "verify",
            "--repository",
            str(repo),
            "--candidate-sha",
            candidate_sha,
            "--release-sha",
            release_sha,
        ],
        text=True,
        capture_output=True,
        check=False,
    )


def test_release_identity_accepts_equal_trees_without_commit_ancestry(tmp_path: Path) -> None:
    repo, candidate_sha, release_sha = _squash_equivalent_repo(tmp_path)
    ancestry = subprocess.run(
        ["git", "-C", str(repo), "merge-base", "--is-ancestor", candidate_sha, release_sha],
        check=False,
    )
    assert ancestry.returncode == 1

    result = _run_identity(
        repo,
        candidate_sha=candidate_sha,
        release_sha=release_sha,
    )

    assert result.returncode == 0, result.stderr
    assert "Release identity verification: PASS" in result.stdout
    assert _git(repo, "rev-parse", f"{candidate_sha}^{{tree}}") in result.stdout


def test_release_identity_rejects_changed_release_tree(tmp_path: Path) -> None:
    repo, candidate_sha, _ = _squash_equivalent_repo(tmp_path)
    (repo / "tracked.txt").write_text("different release tree\n", encoding="utf-8")
    _git(repo, "commit", "-am", "change release tree")
    release_sha = _git(repo, "rev-parse", "HEAD")

    result = _run_identity(
        repo,
        candidate_sha=candidate_sha,
        release_sha=release_sha,
    )

    assert result.returncode == 1
    assert "candidate tree does not match release tree" in result.stderr


@pytest.mark.parametrize("field", ["candidate_sha", "release_sha"])
def test_release_identity_rejects_unknown_commit_sha(tmp_path: Path, field: str) -> None:
    repo, candidate_sha, release_sha = _squash_equivalent_repo(tmp_path)
    values = {
        "candidate_sha": candidate_sha,
        "release_sha": release_sha,
    }
    values[field] = "f" * 40

    result = _run_identity(repo, **values)

    assert result.returncode == 1
    assert f"{field} does not identify a known Git object" in result.stderr


@pytest.mark.parametrize("field", ["candidate_sha", "release_sha"])
def test_release_identity_rejects_non_commit_object(tmp_path: Path, field: str) -> None:
    repo, candidate_sha, release_sha = _squash_equivalent_repo(tmp_path)
    blob_sha = _git(repo, "hash-object", "-w", "--stdin", input_text="not a commit\n")
    values = {
        "candidate_sha": candidate_sha,
        "release_sha": release_sha,
    }
    values[field] = blob_sha

    result = _run_identity(repo, **values)

    assert result.returncode == 1
    assert f"{field} must identify a commit, found blob" in result.stderr


@pytest.mark.parametrize("invalid_sha", ["HEAD", "A" * 40, "0" * 39])
def test_release_identity_rejects_non_canonical_sha(
    tmp_path: Path,
    invalid_sha: str,
) -> None:
    repo, _, release_sha = _squash_equivalent_repo(tmp_path)

    result = _run_identity(
        repo,
        candidate_sha=invalid_sha,
        release_sha=release_sha,
    )

    assert result.returncode == 1
    assert "candidate_sha must be a 40-character lowercase Git SHA" in result.stderr


def test_release_identity_rejects_modified_tracked_worktree(tmp_path: Path) -> None:
    repo, candidate_sha, release_sha = _squash_equivalent_repo(tmp_path)
    (repo / "tracked.txt").write_text("uncommitted modification\n", encoding="utf-8")

    result = _run_identity(
        repo,
        candidate_sha=candidate_sha,
        release_sha=release_sha,
    )

    assert result.returncode == 1
    assert "repository contains staged, unstaged, or untracked changes" in result.stderr


def test_release_identity_rejects_orphan_same_tree_candidate(tmp_path: Path) -> None:
    repo, candidate_sha, release_sha = _squash_equivalent_repo(tmp_path)
    candidate_tree = _git(repo, "rev-parse", f"{candidate_sha}^{{tree}}")
    orphan_sha = _git(repo, "commit-tree", candidate_tree, input_text="orphan candidate\n")

    result = _run_identity(
        repo,
        candidate_sha=orphan_sha,
        release_sha=release_sha,
    )

    assert result.returncode == 1
    assert "candidate_sha is not reachable from trusted candidate ref origin/dev" in result.stderr


def test_release_identity_rejects_missing_trusted_candidate_ref(tmp_path: Path) -> None:
    repo, candidate_sha, release_sha = _squash_equivalent_repo(tmp_path)
    _git(repo, "update-ref", "-d", "refs/remotes/origin/dev")

    result = _run_identity(
        repo,
        candidate_sha=candidate_sha,
        release_sha=release_sha,
    )

    assert result.returncode == 1
    assert "trusted candidate ref origin/dev does not resolve to a commit" in result.stderr


def test_release_identity_rejects_release_sha_that_is_not_head(tmp_path: Path) -> None:
    repo, candidate_sha, release_sha = _squash_equivalent_repo(tmp_path)
    _git(repo, "commit", "--allow-empty", "-m", "different release head")

    result = _run_identity(
        repo,
        candidate_sha=candidate_sha,
        release_sha=release_sha,
    )

    assert result.returncode == 1
    assert "release_sha must exactly match HEAD" in result.stderr


@pytest.mark.parametrize("change", ["staged", "deleted", "untracked"])
def test_release_identity_rejects_any_nonignored_worktree_change(
    tmp_path: Path,
    change: str,
) -> None:
    repo, candidate_sha, release_sha = _squash_equivalent_repo(tmp_path)
    tracked = repo / "tracked.txt"
    if change == "staged":
        tracked.write_text("staged modification\n", encoding="utf-8")
        _git(repo, "add", "tracked.txt")
    elif change == "deleted":
        tracked.unlink()
    else:
        (repo / "untracked.txt").write_text("untracked\n", encoding="utf-8")

    result = _run_identity(
        repo,
        candidate_sha=candidate_sha,
        release_sha=release_sha,
    )

    assert result.returncode == 1
    assert "repository contains staged, unstaged, or untracked changes" in result.stderr


def test_release_identity_rejects_ignored_root_dotenv(tmp_path: Path) -> None:
    repo, candidate_sha, release_sha = _squash_equivalent_repo(tmp_path)
    (repo / ".env").write_text("LOOM_SERVICE_API_TOKEN=not-a-real-token\n", encoding="utf-8")
    assert _git(repo, "check-ignore", ".env") == ".env"

    result = _run_identity(
        repo,
        candidate_sha=candidate_sha,
        release_sha=release_sha,
    )

    assert result.returncode == 1
    assert "repository root .env is forbidden during release verification" in result.stderr


def test_release_identity_allows_ignored_virtual_environment(tmp_path: Path) -> None:
    repo, candidate_sha, release_sha = _squash_equivalent_repo(tmp_path)
    (repo / ".venv").mkdir()
    (repo / ".venv" / "cache.txt").write_text("ignored cache\n", encoding="utf-8")

    result = _run_identity(
        repo,
        candidate_sha=candidate_sha,
        release_sha=release_sha,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    ("git_path", "is_directory"),
    [
        ("MERGE_HEAD", False),
        ("rebase-merge", True),
        ("CHERRY_PICK_HEAD", False),
        ("REVERT_HEAD", False),
        ("BISECT_START", False),
    ],
)
def test_release_identity_rejects_in_progress_git_operation(
    tmp_path: Path,
    git_path: str,
    is_directory: bool,
) -> None:
    repo, candidate_sha, release_sha = _squash_equivalent_repo(tmp_path)
    marker = Path(_git(repo, "rev-parse", "--git-path", git_path))
    if not marker.is_absolute():
        marker = repo / marker
    if is_directory:
        marker.mkdir(parents=True)
    else:
        marker.write_text(candidate_sha + "\n", encoding="utf-8")

    result = _run_identity(
        repo,
        candidate_sha=candidate_sha,
        release_sha=release_sha,
    )

    assert result.returncode == 1
    assert "Git operation is in progress" in result.stderr


def test_production_verifier_uses_squash_safe_tree_identity() -> None:
    verifier = PRODUCTION_VERIFY_SCRIPT.read_text(encoding="utf-8")

    assert "python3 scripts/ops/release_identity.py verify" in verifier
    assert '--release-sha "${release_sha}"' in verifier
    assert '--trusted-candidate-ref "origin/dev"' in verifier
    assert "python3 scripts/ops/release_gate.py verify-production" in verifier
    assert "uv run python" not in verifier
    assert "git merge-base --is-ancestor" not in verifier
