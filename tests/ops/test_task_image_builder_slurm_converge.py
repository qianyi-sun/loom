"""Contracts for the bounded native task-image builder Slurm converger."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CONVERGER = ROOT / "deploy/slurm/converge-loom-task-image-builder-capacity.sh"


def test_converger_is_fixed_parseable_and_nonpreemptive() -> None:
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
    source = CONVERGER.read_text(encoding="utf-8")
    assert 'QOS="loom-task-image-builder"' in source
    assert 'RESERVATION="loom-task-image-builder"' in source
    assert 'SERVICE_USER="loom-rollout"' in source
    assert 'ACCOUNT="loom-staging"' in source
    assert 'OLDLAB_NODE="trt-eai-oldlab-6"' in source
    assert 'GB10_NODE="trt-gb10-2"' in source
    assert "MaxJobsPU=1" in source
    assert "MaxSubmitJobsPU=1" in source
    assert "MaxWall=04:00:00" in source
    assert "Duration=INFINITE" in source
    assert "Flags=IGNORE_JOBS" in source
    assert 'grep -E "^ReservationName=$RESERVATION' in source
    assert "scancel" not in source
    assert "delete qos" not in source
    assert "delete reservation" not in source


def test_converger_fails_closed_on_existing_qos_or_reservation_drift() -> None:
    source = CONVERGER.read_text(encoding="utf-8")
    assert "existing task-image builder QoS conflicts" in source
    assert "existing task-image builder reservation conflicts" in source
    assert "task-image builder QoS readback did not converge" in source
    assert "task-image builder reservation readback did not converge" in source
