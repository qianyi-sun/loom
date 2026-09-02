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
        '#!/usr/bin/env bash\nif [[ "$*" == "-u" ]]; then printf \'4242\\n\'; else exit 2; fi\n',
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
            "--source-sha",
            "1" * 40,
            "--controller-public-key",
            str(controller_key),
            "--legacy-deploy-public-key",
            str(legacy_key),
        ],
    )

    assert result.returncode == 1
    assert result.stderr == "error: GB10 autoscaler-controller installation requires root\n"


def test_installer_requires_exact_source_sha_before_privilege_gate(tmp_path: Path) -> None:
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

    assert result.returncode == 2
    assert result.stderr == "error: exact source SHA argument is invalid\n"


def test_installer_rejects_source_root_override_before_privilege_gate(tmp_path: Path) -> None:
    controller_key = tmp_path / "controller.pub"
    legacy_key = tmp_path / "legacy.pub"
    controller_key.write_text("ssh-ed25519 AAAA controller\n", encoding="utf-8")
    legacy_key.write_text("ssh-ed25519 AAAB legacy\n", encoding="utf-8")

    result = _run_before_privilege_gate(
        tmp_path,
        [
            "--source-sha",
            "1" * 40,
            "--controller-public-key",
            str(controller_key),
            "--legacy-deploy-public-key",
            str(legacy_key),
            "--trusted-source-root",
            str(tmp_path),
        ],
    )

    assert result.returncode == 2
    assert result.stderr.startswith("usage: sudo ")


def test_launcher_verifies_sealed_host_before_pinned_kubectl_download() -> None:
    source = _source()
    assert 'KUBECTL_VERSION="v1.36.2"' in source
    assert (
        'KUBECTL_SHA256="c957eb8c4bea27a3bb35b269edd9082e27f027f7b76b20b5bf4afebc726c6d3e"'
        in source
    )
    assert "linux/arm64/kubectl" in source
    assert "sha256sum --check" in source
    assert source.index('"$SOURCE_VERIFIER" verify-source') < source.index(
        '"$SOURCE_VERIFIER" verify-host'
    )
    assert source.index('"$SOURCE_VERIFIER" verify-host') < source.index("curl --fail --location")


def test_launcher_invokes_checkout_python_in_isolated_mode() -> None:
    source = _source()

    assert source.count('/usr/bin/python3 -I "$SOURCE_VERIFIER"') == 3


def test_installer_pins_the_arm64_uv_runtime_builder() -> None:
    source = _source()
    assert 'UV_VERSION="0.11.26"' in source
    assert 'UV_SHA256="befa1a59c91e96eb601b0fd9a97c03dd666f17baba644b2b4db9c59a767e387e"' in source
    assert 'UV_ARCHIVE="uv-aarch64-unknown-linux-gnu.tar.gz"' in source
    assert "sha256sum --check" in source
    assert "tar --extract --gzip --to-stdout" in source
    assert '"uv-aarch64-unknown-linux-gnu/uv" >"$temporary_dir/uv"' in source
    assert '--uv-source "$temporary_dir/uv"' in source


def test_launcher_delegates_all_live_mutation_to_transactional_installer() -> None:
    source = _source()

    assert '"$SOURCE_VERIFIER" install' in source
    assert '--controller-public-key "$CONTROLLER_PUBLIC_KEY"' in source
    assert '--legacy-public-key "$LEGACY_DEPLOY_PUBLIC_KEY"' in source
    for forbidden in (
        "/usr/bin/systemd-tmpfiles",
        "/usr/sbin/visudo",
        "install -o",
        "--install-authority",
        "/usr/local/bin/kubectl version",
        "/usr/local/bin/uv --version",
        "/home/qianyi",
        "/shared_work2/qianyi",
    ):
        assert forbidden not in source
