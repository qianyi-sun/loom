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
    _git(repo, "add", "tracked.txt")
    _git(repo, "commit", "-m", "base")

    _git(repo, "switch", "-c", "dev")
    tracked.write_text("candidate\n", encoding="utf-8")
    _git(repo, "commit", "-am", "candidate")
    candidate_sha = _git(repo, "rev-parse", "HEAD")

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
    assert "tracked worktree differs from release commit" in result.stderr


def test_production_verifier_uses_squash_safe_tree_identity() -> None:
    verifier = PRODUCTION_VERIFY_SCRIPT.read_text(encoding="utf-8")

    assert "scripts/ops/release_identity.py verify" in verifier
    assert '--release-sha "${release_sha}"' in verifier
    assert "git merge-base --is-ancestor" not in verifier
