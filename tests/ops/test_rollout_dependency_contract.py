from __future__ import annotations

import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_rollout_extra_installs_benchmark_sibling_packages() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    extras = pyproject["project"]["optional-dependencies"]
    rollout = set(extras["rollout"])
    assert "loom-benchmarks" in rollout
    assert "loom-benchmark-terminal-bench-2" in rollout

    sources = pyproject["tool"]["uv"]["sources"]
    assert sources["loom-benchmarks"] == {"workspace": True}
    assert sources["loom-benchmark-terminal-bench-2"] == {"workspace": True}


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


def test_integration_jobs_install_terminal_bench_sibling_independent_of_cache() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))

    for job_name in ("integration", "integration-docker"):
        install_step = next(
            step
            for step in workflow["jobs"][job_name]["steps"]
            if step.get("name") == "Sync locked workspace"
        )
        assert "if" not in install_step
        # workspace:True uv sources mean a locked all-packages sync installs the
        # terminal-bench sibling from source on every run, unconditionally and
        # independent of any restored cache.
        assert "uv sync --locked --all-packages" in install_step["run"]


def test_operator_runbook_bootstraps_rollout_extra() -> None:
    runbook = (ROOT / "docs/runbooks/operator-runbook.md").read_text(encoding="utf-8")

    assert "uv sync --locked --all-packages --extra cluster --extra rollout" in runbook
    assert "packages/loom-benchmarks" in runbook
    assert "packages/loom-benchmark-terminal-bench-2" in runbook
