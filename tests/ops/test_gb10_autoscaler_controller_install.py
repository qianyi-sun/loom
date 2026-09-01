"""Contracts for the GB10 autoscaler-controller bootstrap."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "deploy/slurm/install-loom-gb10-autoscaler-controller.sh"


def _source() -> str:
    return INSTALLER.read_text(encoding="utf-8")


def _run_before_privilege_gate(
    tmp_path: Path,
    arguments: list[str],
) -> subprocess.CompletedProcess[str]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir(exist_ok=True)
    fake_id = fake_bin / "id"
    fake_id.write_text(
        "#!/usr/bin/env bash\n"
        "if [[ \"$*\" == \"-u\" ]]; then printf '4242\\n'; else exit 2; fi\n",
        encoding="utf-8",
    )
    fake_id.chmod(0o755)
    return subprocess.run(
        [shutil.which("bash") or "bash", str(INSTALLER), *arguments],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PATH": f"{fake_bin}:{os.environ['PATH']}"},
    )


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


@pytest.mark.parametrize(
    "present_option",
    ["--controller-public-key", "--legacy-deploy-public-key"],
)
def test_installer_requires_both_public_key_paths_before_privilege_gate(
    tmp_path: Path,
    present_option: str,
) -> None:
    public_key = tmp_path / "authority.pub"
    public_key.write_text("ssh-ed25519 AAAA authority\n", encoding="utf-8")

    result = _run_before_privilege_gate(
        tmp_path,
        [present_option, str(public_key)],
    )

    assert result.returncode == 2
    assert result.stderr == "error: controller broker installation input is unavailable\n"


def test_installer_accepts_both_public_key_paths_before_privilege_gate(
    tmp_path: Path,
) -> None:
    controller_key = tmp_path / "controller.pub"
    legacy_key = tmp_path / "legacy.pub"
    controller_key.write_text("ssh-ed25519 AAAA controller\n", encoding="utf-8")
    legacy_key.write_text("ssh-ed25519 AAAB legacy\n", encoding="utf-8")

    result = _run_before_privilege_gate(
        tmp_path,
        [
            "--controller-public-key",
            str(controller_key),
            "--legacy-deploy-public-key",
            str(legacy_key),
        ],
    )

    assert result.returncode == 1
    assert result.stderr == "error: GB10 autoscaler-controller installation requires root\n"


def test_installer_is_pinned_to_the_gb10_controller_and_kubectl_digest() -> None:
    source = _source()
    assert 'CONTROLLER="gx10-01c7"' in source
    assert 'CLUSTER="trt-gb10"' in source
    assert 'KUBECTL_VERSION="v1.36.2"' in source
    assert (
        'KUBECTL_SHA256="c957eb8c4bea27a3bb35b269edd9082e27f027f7b76b20b5bf4afebc726c6d3e"'
        in source
    )
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
    assert '"uv $UV_VERSION (aarch64-unknown-linux-gnu)"' in source


def test_installer_creates_only_non_personal_controller_roots() -> None:
    source = _source()
    assert 'SERVICE_USER="loom-rollout"' in source
    assert 'SERVICE_HOME="/var/lib/loom-rollout"' in source
    assert 'RUNTIME_ROOT="/opt/loom-staging-runner"' in source
    assert 'STATE_ROOT="/var/lib/loom-staging-rollout"' in source
    assert 'KUBECONFIG_PATH="$STATE_ROOT/kubeconfig"' in source
    assert '"$SERVICE_HOME/.config/systemd/user"' in source
    assert "/home/qianyi" not in source
    assert "/shared_work2/qianyi" not in source


def test_installer_couples_acceptance_authority_before_broker_publication() -> None:
    source = _source()

    assert "--controller-public-key)" in source
    assert "--legacy-deploy-public-key)" in source
    assert (
        'ACCEPTANCE_AUTHORITY_SOURCE="$REPO_ROOT/scripts/ops/'
        'gb10_slurm_acceptance_authority.py"' in source
    )
    assert (
        'ACCEPTANCE_AUTHORITY_PATH="/usr/local/libexec/'
        'loom-gb10-slurm-acceptance-authority"' in source
    )
    assert (
        'ACCEPTANCE_TMPFILES_SOURCE="$REPO_ROOT/deploy/slurm/'
        'loom-gb10-slurm-authority.tmpfiles"' in source
    )
    assert 'ACCEPTANCE_RUNTIME_ROOT="/run/loom-gb10-slurm-authority"' in source
    assert '/usr/bin/systemd-tmpfiles --create "$ACCEPTANCE_TMPFILES_PATH"' in source
    assert '"root:root:700:directory"' in source
    assert 'BROKER_SOURCE="$REPO_ROOT/scripts/ops/gb10_external_supervisor_broker.py"' in source
    assert 'BROKER_PATH="/usr/local/libexec/loom-gb10-external-supervisor-broker"' in source
    authority_install = source.index(
        "install -o root -g root -m 0755 \\\n"
        '  "$ACCEPTANCE_AUTHORITY_SOURCE" "$ACCEPTANCE_AUTHORITY_PATH"'
    )
    authority_readback = source.index(
        '/usr/bin/python3 "$ACCEPTANCE_AUTHORITY_PATH" --help >/dev/null'
    )
    broker_install = source.index('install -o root -g root -m 0755 "$BROKER_SOURCE" "$BROKER_PATH"')
    assert authority_install < authority_readback < broker_install


def test_installer_keeps_one_forced_ssh_and_sudo_authority_surface() -> None:
    source = _source()

    assert "--controller-public-key)" in source
    assert "--legacy-deploy-public-key)" in source
    assert 'BROKER_PATH="/usr/local/libexec/loom-gb10-external-supervisor-broker"' in source
    assert 'SUDOERS_PATH="/etc/sudoers.d/loom-gb10-external-supervisor"' in source
    assert 'SUDOERS_RULE="qianyi ALL=(root) NOPASSWD:NOSETENV: $BROKER_PATH \\"\\""' in source
    assert (
        '"$BROKER_PATH" --install-authority \\\n'
        '  "$CONTROLLER_PUBLIC_KEY" "$LEGACY_DEPLOY_PUBLIC_KEY"'
    ) in source
    assert source.count("SUDOERS_RULE=") == 1
    assert source.count("qianyi ALL=(root)") == 1
    assert source.count("--install-authority") == 1
