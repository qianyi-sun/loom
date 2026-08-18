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


def test_protected_staging_operator_runbook_is_merged_only_and_complete() -> None:
    runbook = (ROOT / "docs/runbooks/operator-runbook.md").read_text(encoding="utf-8")

    for command in (
        "loom-staging-rollout --env staging preflight",
        "loom-staging-rollout --env staging start --dry-run",
        "loom-staging-rollout --env staging start",
        "loom-staging-rollout --env staging status REQUEST_ID",
        "loom-staging-rollout --env staging logs REQUEST_ID",
        "loom-staging-rollout --env staging logs REQUEST_ID --follow",
        "loom-staging-rollout --env staging resume REQUEST_ID",
        'loom-staging-rollout --env staging cancel REQUEST_ID --reason "bounded operational reason"',
        "loom-staging-rollout --env staging cleanup-incomplete-backup REQUEST_ID",
    ):
        assert command in runbook

    assert "fresh-fetches" in runbook
    assert "**Shared-staging invariant:**" in runbook
    assert "`refs/heads/dev`" in runbook
    assert "Unmerged pull-request refs" in runbook
    assert "merged revert on `dev`" in runbook
    assert "Broker unavailability\ndoes not grant authority for direct mutation" in runbook
    assert "Never use these direct\nmutation commands against shared staging" in runbook


def test_current_staging_rollout_docs_preserve_acceptance_boundary() -> None:
    architecture = (ROOT / "docs/architecture/staging-rollout.md").read_text(
        encoding="utf-8",
    )
    launch = (ROOT / "docs/runbooks/staging-launch.md").read_text(encoding="utf-8")
    gb10 = (ROOT / "deploy/worker-pools/gb10/README.md").read_text(encoding="utf-8")
    normalized_architecture = " ".join(architecture.split())
    normalized_launch = " ".join(launch.split())
    normalized_gb10 = " ".join(gb10.split())

    assert "freshly fetched allowed branch head" in normalized_architecture
    assert "accepts no ref, SHA, tag, image, checkout" in normalized_architecture
    assert (
        "Only one request may own an environment's full rollout lifecycle"
        in normalized_architecture
    )
    assert "merged revert followed by another normal rollout request" in normalized_architecture
    assert "exact merged `dev` candidate" in normalized_launch
    assert (
        "Do not run `loom cluster up`, `loom cluster rollout`, direct migration Jobs"
        in normalized_launch
    )
    assert "canonical 15 Slurm nodes" in gb10
    assert "`trt-gb10-1` through `trt-gb10-15`" in normalized_gb10
    assert "`trt-gb10-16` remains outside Loom's accepted allocation boundary" in (
        normalized_gb10
    )
    assert "becomes eligible again after resources are released" in normalized_gb10
    assert "loom-autoscaler-gb10-staging.timer" in gb10
    assert "loom-staging-rollout --env staging status REQUEST_ID" in gb10
