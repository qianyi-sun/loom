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


def test_independent_staging_operator_runbook_is_merged_only_and_complete() -> None:
    runbook = (ROOT / "docs/runbooks/operator-runbook.md").read_text(encoding="utf-8")

    for command in (
        "loom-staging-rollout preflight",
        "loom-staging-rollout start --dry-run",
        "loom-staging-rollout start",
        "loom-staging-rollout status REQUEST_ID",
        "loom-staging-rollout logs REQUEST_ID",
        "loom-staging-rollout logs REQUEST_ID --follow",
        "loom-staging-rollout resume REQUEST_ID",
        'loom-staging-rollout cancel REQUEST_ID --reason "bounded operational reason"',
        "loom-staging-rollout cleanup-incomplete-backup REQUEST_ID",
    ):
        assert command in runbook

    assert "fresh-fetches" in runbook
    assert "**Shared-staging invariant:**" in runbook
    assert "`refs/heads/dev`" in runbook
    assert "Unmerged pull" in runbook
    assert "physical and scheduling inventory remains all 15 GB10\n   hosts" in runbook
    assert "healthy\n   busy host remains heartbeat-managed" in runbook
    assert "automatically returns after resource release" in runbook
    assert "#822" in runbook
    assert "merged revert on `dev`" in runbook
    assert (
        "The host installer uses only this\ncommand for its post-install readiness check" in runbook
    )
    assert "staging-gb10-rollout-ed25519" not in runbook
    assert "systemd-run --user" not in runbook
    assert "backup-manifest=/data/loom-staging/backups/latest" not in runbook
    assert "choose a candidate ref" not in runbook
    assert "For a manual retry" not in runbook
    assert "Manual full-argv" not in runbook
    assert "After every rollout, first apply and check" not in runbook
    assert "# On each GB10 host during worker-token rotation:" not in runbook
    assert "disaster restore after the broker path has been declared unavailable" not in runbook
    assert "Broker unavailability does not grant\nauthority" in runbook
    assert "-f environment=staging" not in runbook
    assert "A staging cluster deployed via `loom cluster up`" not in runbook
    assert "Deploy that exact image tag to `staging` using the staging GitHub" not in runbook
    assert "worker_service_tunnels.py install-systemd \\" not in runbook
    assert 'tee "$ROLLOUT_DIR/watchdog-evidence.json"' not in runbook
    assert "staging_validation_capacity_runner.py" not in runbook
    assert "worker_capacity_manifest.py lease-staging" not in runbook
    assert "--image-tag staging-05ab776" not in runbook
    assert "--cluster-name loom-staging \\" not in runbook
    assert "Temporarily rotate the provider key" not in runbook
    assert "export LOOM_HF_ORG=loom-staging" not in runbook
    assert "provision the same new high-entropy capability" not in runbook
    assert 'export LOOM_DB_URL="$STAGING_DB_URL"' not in runbook
    assert "19. **Teardown clean.** `loom cluster down --yes`" not in runbook
    assert "The manual commands in these subsections are limited" not in runbook


def test_independent_staging_adr_and_launch_gate_preserve_acceptance_boundary() -> None:
    adr = (ROOT / "docs/architecture/adr/independent-staging-rollout-runner.md").read_text(
        encoding="utf-8"
    )
    launch = (ROOT / "docs/runbooks/staging-launch.md").read_text(encoding="utf-8")
    gb10 = (ROOT / "deploy/worker-pools/gb10/README.md").read_text(encoding="utf-8")
    design = (
        ROOT / "docs/superpowers/specs/2026-07-13-independent-staging-rollout-design.md"
    ).read_text(encoding="utf-8")

    assert "Status: accepted" in adr
    assert "all 15 GB10 inventory nodes" in adr
    assert "healthy busy node advertises reduced or zero available resources" in adr
    assert "becomes eligible automatically after resource\nrelease" in adr
    assert "#822" in adr
    assert "only after the implementation has merged into\n`dev`" in adr
    assert "freshly fetched `refs/heads/dev`" in launch
    assert "Hongjian's and Devansh's separate `start --dry-run`" in launch
    assert "Operators do not run `loom cluster release-manifest`, `loom cluster up`" in launch
    assert "LIVE_ADMIN_TOKEN_FINGERPRINT" not in launch
    assert "loom cluster up \\" not in launch
    assert "/var/lib/loom-staging-rollout/gb10-deploy-ed25519" in gb10
    assert "staging-gb10-rollout-ed25519" not in gb10
    assert "systemd-run --user" not in gb10
    assert "COMMIT=" not in gb10
    assert "curl -sS -X PUT" not in gb10
    assert "--rollback" not in gb10
    assert "--force" not in gb10
    assert "After a rollout, use the read-only tunnel checks:" not in gb10
    assert "scripts/ops/worker_service_tunnels.py check \\" not in gb10
    assert "loom-staging-rollout status REQUEST_ID" in gb10
    assert "run this private gate after every rollout" not in launch
    assert "worker_service_tunnels.py install-systemd \\" not in launch
    assert "Status: approved for implementation on 2026-07-13." in design
    assert "approved for implementation planning" not in design
    assert "It remains emergency break-glass" not in design
    assert "loom-staging-rollout resume REQUEST_ID" in design
    assert "loom-staging-rollout cleanup-incomplete-backup REQUEST_ID" in design
    assert "loom-staging-rollout cleanup-incomplete-backup REQUEST_ID" in gb10
    assert "`loom-rollout` with `--resume`" not in design
