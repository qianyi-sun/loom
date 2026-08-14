"""Contracts for the dedicated non-preemptive OLDLAB staging partition."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CONVERGER = ROOT / "deploy/slurm/converge-loom-oldlab-slurm-partition.sh"


def test_oldlab_partition_converger_is_bounded_and_parses() -> None:
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
    assert 'CONTROLLER="TRT-EAI-OLDLAB-1"' in source
    assert 'CLUSTER="trt-oldlab"' in source
    assert 'CONFIG="/etc/slurm/slurm.conf"' in source
    assert 'STATE_ROOT="/var/lib/loom-oldlab-slurm-authority"' in source
    assert '"trt:sharedwork:664:regular file"' in source
    assert '"root:root:600:regular file"' in source
    assert 'cmp -s "$BACKUP" "$CONFIG"' in source
    assert (
        'ANCHOR_LINE="PartitionName=all Nodes=ALL Default=YES '
        'MaxTime=INFINITE State=UP OverSubscribe=NO"'
    ) in source
    assert 'PARTITION="loom-staging"' in source
    assert "PartitionName=$PARTITION" in source
    assert "Nodes=trt-eai-oldlab-[3-5]" in source
    assert "Default=NO" in source
    assert "MaxTime=2-00:00:00" in source
    assert "PriorityTier=100" in source
    assert "AllowGroups=loom-rollout" in source
    assert "OverSubscribe=NO" in source
    assert "scontrol reconfigure" in source
    assert "scontrol show partition" in source
    assert "scontrol show node" in source


def test_oldlab_partition_converger_never_preempts_or_mutates_jobs() -> None:
    source = CONVERGER.read_text(encoding="utf-8").lower()

    assert "preempt" not in source
    assert "scancel" not in source
    assert "scontrol update job" not in source
    assert "scontrol hold" not in source
    assert "scontrol release" not in source
