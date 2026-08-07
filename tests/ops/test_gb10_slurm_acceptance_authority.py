"""Contracts for the root-installed GB10 Slurm acceptance authority."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
AUTHORITY = ROOT / "scripts/ops/gb10_slurm_acceptance_authority.py"
INSTALLER = ROOT / "deploy/slurm/install-loom-gb10-acceptance-authority.sh"


def _authority_source() -> str:
    return AUTHORITY.read_text(encoding="utf-8")


def _installer_source() -> str:
    return INSTALLER.read_text(encoding="utf-8")


def test_authority_compiles_and_installer_parses() -> None:
    compile_result = subprocess.run(
        ["python3", "-m", "py_compile", str(AUTHORITY)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert compile_result.returncode == 0, compile_result.stderr
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    parse_result = subprocess.run(
        [bash, "-n", str(INSTALLER)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert parse_result.returncode == 0, parse_result.stderr


def test_authority_is_fixed_to_service_identity_and_all_fifteen_nodes() -> None:
    source = _authority_source()
    assert 'SERVICE_USER = "loom-rollout"' in source
    assert "SERVICE_UID = 995" in source
    assert "SERVICE_GID = 2007" in source
    assert 'SLURM_ACCOUNT = "loom-staging"' in source
    assert 'SLURM_QOS = "loom-staging"' in source
    assert 'CLUSTER_NAME = "trt-gb10"' in source
    assert 'CONTROLLER_HOST = "gx10-01c7"' in source
    assert re.search(r"range\(1, 16\)", source)


def test_authority_runs_real_service_user_allocations_on_each_exact_node() -> None:
    source = _authority_source()
    assert '"runuser", "-u", SERVICE_USER, "--"' in source
    assert '"srun"' in source
    assert 'f"--nodelist={node}"' in source
    assert 'f"--account={SLURM_ACCOUNT}"' in source
    assert 'f"--qos={SLURM_QOS}"' in source
    assert "loom-slurm-job-cgroup-guard.service" in source
    assert '"docker", "info"' in source


def test_authority_binds_candidate_profile_repo_env_and_short_expiry() -> None:
    source = _authority_source()
    assert "/opt/loom-staging-runner/candidates" in source
    assert "deploy/environment-state/staging.toml" in source
    assert "profile_sha256" in source
    assert "candidate_tree" in source
    assert "timedelta(minutes=30)" in source
    assert '"kind": "loom_gb10_slurm_acceptance"' in source
    assert '"result": "pass"' in source
    assert "/home/qianyi" not in source
    assert "/shared_work2/qianyi" not in source


def test_installer_publishes_only_root_owned_fixed_authority() -> None:
    source = _installer_source()
    assert 'CONTROLLER="gx10-01c7"' in source
    assert "grep -Eq" not in source
    assert 'INSTALL_PATH="/usr/local/libexec/loom-gb10-slurm-acceptance-authority"' in source
    assert 'STATE_ROOT="/var/lib/loom-gb10-slurm-authority"' in source
    assert "install -o root -g root -m 0755" in source
    assert "install -d -o root -g root -m 0755" in source
    assert "/home/qianyi" not in source
