"""The #896 guard installer must stay consistent with the systemd unit it wires.

The installer, the guard script, and the unit are three separate files that must
agree on the on-disk paths; a drift would install the guard where the unit does
not look for it. These checks are pure text/consistency (no root, no systemd).
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_INSTALLER = _REPO_ROOT / "deploy/slurm/install-loom-slurm-job-cgroup-guard.sh"
_UNIT = _REPO_ROOT / "deploy/slurm/loom-slurm-job-cgroup-guard.service"
_GUARD = _REPO_ROOT / "scripts/ops/slurm_job_cgroup_guard.py"


def test_installer_and_artifacts_exist() -> None:
    assert _INSTALLER.is_file()
    assert _UNIT.is_file()
    assert _GUARD.is_file()


def test_installer_parses_as_bash() -> None:
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    result = subprocess.run(
        [bash, "-n", str(_INSTALLER)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def _unit_field(name: str) -> str:
    for line in _UNIT.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].strip()
    raise AssertionError(f"{name}= not found in the unit")


def test_installer_paths_match_the_unit() -> None:
    installer = _INSTALLER.read_text(encoding="utf-8")

    def _var(name: str) -> str:
        match = re.search(rf'^{name}="([^"]+)"', installer, re.MULTILINE)
        assert match is not None, f"{name} not set in the installer"
        return match.group(1)

    guard_dst = _var("GUARD_DST")
    env_dst = _var("ENV_DST")
    unit_dst = _var("UNIT_DST")

    # The unit's ExecStart must invoke exactly where the installer drops the guard.
    assert guard_dst in _unit_field("ExecStart")
    # The unit's EnvironmentFile must be the env file the installer writes.
    assert _unit_field("EnvironmentFile") == env_dst
    # The installer must place the unit where systemd loads system units.
    assert unit_dst == "/etc/systemd/system/loom-slurm-job-cgroup-guard.service"


def test_installer_requires_nodename_and_validates_against_sinfo() -> None:
    installer = _INSTALLER.read_text(encoding="utf-8")
    # Node name is mandatory (fail-closed on missing arg).
    assert 'node="${1:-}"' in installer
    assert "usage:" in installer
    # It validates the name against Slurm so a typo cannot silently no-op.
    assert "sinfo -N -h -o '%N'" in installer
    # And it copies from the repo-owned guard source (not a hand-authored path).
    assert "scripts/ops/slurm_job_cgroup_guard.py" in installer
