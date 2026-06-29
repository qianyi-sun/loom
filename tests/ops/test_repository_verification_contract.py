from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_repository_checks_ruff_scope_matches_repo_wide_local_lint() -> None:
    workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/ci.yml").read_text())
    steps = workflow["jobs"]["repository-checks"]["steps"]
    ruff_step = next(step for step in steps if step.get("name") == "Ruff")

    assert ruff_step["run"] == "uv run ruff check src tests packages migrations"


def test_local_python_version_is_pinned_to_ci_interpreter() -> None:
    assert (REPO_ROOT / ".python-version").read_text(encoding="utf-8").strip() == "3.11"


def test_contributor_quickstart_uses_ci_python_for_local_verification() -> None:
    text = (REPO_ROOT / "docs/contributor-quickstart.md").read_text(encoding="utf-8")

    assert "uv python install 3.11" in text
    assert "uv sync --extra dev --python 3.11" in text
