"""Contracts for adding canonical GB10-11 to the shared Slurm partition."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CONVERGER = ROOT / "deploy/slurm/converge-loom-gb10-slurm-partition.sh"


def test_partition_converger_is_bounded_and_parses() -> None:
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
    assert 'CONTROLLER="gx10-01c7"' in source
    assert 'CLUSTER="trt-gb10"' in source
    assert 'CONFIG="/etc/slurm/slurm.conf"' in source
    assert 'trt-gb10-[1-10,12-16]' in source
    assert 'trt-gb10-[1-16]' in source
    assert "scontrol reconfigure" in source
    assert "grep -Eq" not in source
    assert "trt-gb10-11" in source
    assert "trt-gb10-16" in source
    assert "delete" not in source.lower()
