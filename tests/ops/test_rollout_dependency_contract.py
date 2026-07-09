from __future__ import annotations

import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_rollout_extra_installs_benchmark_sibling_packages() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    extras = pyproject["project"]["optional-dependencies"]
    rollout = set(extras["rollout"])
    assert "loom-benchmarks" in rollout
    assert "loom-benchmark-terminal-bench-2" in rollout

    sources = pyproject["tool"]["uv"]["sources"]
    assert sources["loom-benchmarks"] == {
        "path": "packages/loom-benchmarks",
        "editable": True,
    }
    assert sources["loom-benchmark-terminal-bench-2"] == {
        "path": "packages/loom-benchmark-terminal-bench-2",
        "editable": True,
    }


def test_cluster_rollout_workflows_sync_rollout_extra() -> None:
    workflow_paths = [
        ROOT / ".github/workflows/cluster-smoke.yml",
        ROOT / ".github/workflows/staging-smoke.yml",
        ROOT / ".github/workflows/release-promotion-gate.yml",
    ]

    for path in workflow_paths:
        text = path.read_text(encoding="utf-8")
        assert "--extra cluster" in text
        assert "--extra rollout" in text


def test_operator_runbook_bootstraps_rollout_extra() -> None:
    runbook = (ROOT / "docs/runbooks/operator-runbook.md").read_text(encoding="utf-8")

    assert "uv sync --extra cluster --extra rollout" in runbook
    assert "packages/loom-benchmarks" in runbook
    assert "packages/loom-benchmark-terminal-bench-2" in runbook
