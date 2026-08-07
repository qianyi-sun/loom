"""Contract tests for the fixed GB10 Loom Slurm accounting converger."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CONVERGER = ROOT / "deploy/slurm/converge-loom-gb10-slurm-accounting.sh"


def _source() -> str:
    return CONVERGER.read_text(encoding="utf-8")


def test_converger_exists_and_parses_as_bash() -> None:
    assert CONVERGER.is_file()
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    result = subprocess.run(
        [bash, "-n", str(CONVERGER)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_converger_is_fixed_to_the_gb10_controller_and_service_identity() -> None:
    source = _source()
    assert 'CLUSTER="trt-gb10"' in source
    assert 'CONTROLLER="gx10-01c7"' in source
    assert 'SERVICE_USER="loom-rollout"' in source
    assert 'ACCOUNT="loom-staging"' in source
    assert 'QOS="loom-staging"' in source
    assert "scontrol show config" in source
    assert "hostname -s" in source


def test_converger_adds_only_missing_bounded_accounting_objects() -> None:
    source = _source()
    assert "show account where" in source
    assert 'sacctmgr --immediate add account name="$ACCOUNT"' in source
    assert 'sacctmgr --immediate add qos name="$QOS"' in source
    assert 'MaxJobsPU=15' in source
    assert 'MaxSubmitJobsPU=15' in source
    assert 'sacctmgr --immediate add user name="$SERVICE_USER"' in source
    assert 'sacctmgr --immediate modify user' in source
    assert 'where name="$SERVICE_USER"' in source
    assert "sacctmgr --immediate delete" not in source


def test_converger_verifies_exact_readback_and_never_mentions_personal_paths() -> None:
    source = _source()
    assert "account readback did not converge" in source
    assert "QoS readback did not converge" in source
    assert "association readback did not converge" in source
    assert "/home/qianyi" not in source
    assert "/shared_work2/qianyi" not in source
