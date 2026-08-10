from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CHECKER = REPO_ROOT / "scripts/check_repository_paths.py"


def _run_checker(
    tmp_path: Path,
    *tracked_paths: str,
    run_from: str = ".",
) -> subprocess.CompletedProcess[str]:
    subprocess.run(["git", "init", "--quiet"], cwd=tmp_path, check=True)
    for relative_path in tracked_paths:
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("test fixture\n", encoding="utf-8")
    subprocess.run(["git", "add", "--", *tracked_paths], cwd=tmp_path, check=True)
    checker_cwd = tmp_path / run_from
    checker_cwd.mkdir(parents=True, exist_ok=True)
    return subprocess.run(
        [sys.executable, str(CHECKER)],
        cwd=checker_cwd,
        check=False,
        capture_output=True,
        text=True,
    )


def test_repository_path_policy_rejects_tracked_superpowers_documents(
    tmp_path: Path,
) -> None:
    result = _run_checker(
        tmp_path,
        "docs/superpowers/specs/feature-design.md",
        "docs/superpowers/plans/feature-plan.md",
    )

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr == (
        "Forbidden tracked repository paths:\n"
        "  docs/superpowers/plans/feature-plan.md\n"
        "  docs/superpowers/specs/feature-design.md\n"
        "Move durable designs to docs/architecture/ and keep execution plans local.\n"
    )


def test_repository_path_policy_allows_similarly_named_paths(tmp_path: Path) -> None:
    result = _run_checker(
        tmp_path,
        "docs/superpowers-guide.md",
        "docs/architecture/feature-design.md",
    )

    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == ""


def test_repository_path_policy_uses_repository_root_from_subdirectory(
    tmp_path: Path,
) -> None:
    result = _run_checker(
        tmp_path,
        "docs/superpowers/specs/feature-design.md",
        run_from="scripts/local",
    )

    assert result.returncode == 1
    assert "docs/superpowers/specs/feature-design.md" in result.stderr
