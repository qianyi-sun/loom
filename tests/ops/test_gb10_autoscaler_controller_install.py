"""Contracts for the GB10 autoscaler-controller bootstrap."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "deploy/slurm/install-loom-gb10-autoscaler-controller.sh"


def _source() -> str:
    return INSTALLER.read_text(encoding="utf-8")


def test_installer_parses_as_bash() -> None:
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


def test_installer_is_pinned_to_the_gb10_controller_and_kubectl_digest() -> None:
    source = _source()
    assert 'CONTROLLER="gx10-01c7"' in source
    assert 'CLUSTER="trt-gb10"' in source
    assert 'KUBECTL_VERSION="v1.36.2"' in source
    assert 'KUBECTL_SHA256="c957eb8c4bea27a3bb35b269edd9082e27f027f7b76b20b5bf4afebc726c6d3e"' in source
    assert "linux/arm64/kubectl" in source
    assert "sha256sum --check" in source
    assert "awk -F'\"' '/\"gitVersion\"/" in source


def test_installer_pins_the_arm64_uv_runtime_builder() -> None:
    source = _source()
    assert 'UV_VERSION="0.11.26"' in source
    assert 'UV_SHA256="befa1a59c91e96eb601b0fd9a97c03dd666f17baba644b2b4db9c59a767e387e"' in source
    assert 'UV_ARCHIVE="uv-aarch64-unknown-linux-gnu.tar.gz"' in source
    assert "sha256sum --check" in source
    assert '"$temporary_dir/uv-aarch64-unknown-linux-gnu/uv" /usr/local/bin/uv' in source
    assert 'installed_uv_version="$(/usr/local/bin/uv --version)"' in source


def test_installer_creates_only_non_personal_controller_roots() -> None:
    source = _source()
    assert 'SERVICE_USER="loom-rollout"' in source
    assert 'RUNTIME_ROOT="/opt/loom-staging-runner"' in source
    assert 'STATE_ROOT="/var/lib/loom-staging-rollout"' in source
    assert 'KUBECONFIG_PATH="$STATE_ROOT/kubeconfig"' in source
    assert "/home/qianyi" not in source
    assert "/shared_work2/qianyi" not in source
