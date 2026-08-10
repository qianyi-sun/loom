"""Static contract tests for the GB10 Slurm service-identity installer."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "deploy/slurm/install-loom-gb10-slurm-identity.sh"
GUARD_INSTALLER = ROOT / "deploy/slurm/install-loom-slurm-job-cgroup-guard.sh"


def _source() -> str:
    return INSTALLER.read_text(encoding="utf-8")


def test_installer_exists_and_parses_as_bash() -> None:
    assert INSTALLER.is_file()
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    result = subprocess.run(
        [bash, "-n", str(INSTALLER)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_installer_pins_non_personal_service_identity_and_shared_roots() -> None:
    source = _source()
    assert 'SERVICE_USER="loom-rollout"' in source
    assert 'SERVICE_UID="995"' in source
    assert 'SHARED_GROUP="sharedwork"' in source
    assert 'SHARED_GID="2007"' in source
    assert 'DOCKER_GROUP="docker"' in source
    assert 'SHARED_ROOT="/shared_work2/loom-staging-rollout"' in source
    assert 'WORKER_ENV_ROOT="$SHARED_ROOT/worker-envs"' in source
    assert 'JOB_OUTPUT_ROOT="$SHARED_ROOT/job-output"' in source
    assert "/home/qianyi" not in source
    assert "/shared_work2/qianyi" not in source


def test_installer_admits_replacement_node16_and_rejects_debug_node10() -> None:
    accepted = subprocess.run(
        [str(INSTALLER), "trt-gb10-16"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert accepted.returncode != 2
    assert "node must be one of" not in accepted.stderr

    rejected = subprocess.run(
        [str(INSTALLER), "trt-gb10-10"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert rejected.returncode == 2
    assert "node must be one of" in rejected.stderr

    source = _source()
    assert "scontrol show node" in source
    assert "NodeAddr" in source
    assert "hostname -I" in source
    assert "uname -m" in source
    assert "aarch64" in source


def test_installer_rejects_uid_gid_collisions_before_creating_the_user() -> None:
    source = _source()
    uid_check = source.index('getent passwd "$SERVICE_UID"')
    useradd = source.index("useradd")
    gid_check = source.index('getent group "$SHARED_GROUP"')
    assert uid_check < useradd
    assert gid_check < useradd
    assert "service UID is already owned" in source
    assert "shared group identity is invalid" in source


def test_installer_wires_the_repo_owned_containment_guard() -> None:
    source = _source()
    assert GUARD_INSTALLER.is_file()
    assert 'GUARD_INSTALLER="$SCRIPT_DIR/install-loom-slurm-job-cgroup-guard.sh"' in source
    assert '"$GUARD_INSTALLER" "$node"' in source


def test_controller_mode_enables_linger_for_the_service_identity() -> None:
    source = _source()
    assert 'case "${1:-}" in' in source
    assert "--controller" in source
    assert 'LINGER_PATH="/var/lib/systemd/linger/$SERVICE_USER"' in source
    assert 'loginctl enable-linger "$SERVICE_USER"' in source
    assert "nsenter --target 1 --mount --uts --ipc --net --pid" in source
    assert 'loginctl show-user "$SERVICE_USER" --property=Linger --value' in source
    assert 'systemctl start "user@$SERVICE_UID.service"' in source
