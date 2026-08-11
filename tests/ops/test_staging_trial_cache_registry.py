"""Behavioral contracts for the staging trial-cache TLS registry."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
NODE_PROBE = ROOT / "scripts/ops/staging_trial_cache_registry_node_probe.py"
CA_FILE = ROOT / "deploy/worker-pools/trial-cache/staging-ca.crt"
REGISTRY_UNIT = ROOT / "deploy/slurm/loom-staging-trial-cache-registry.service"
REGISTRY_INSTALLER = ROOT / "deploy/slurm/install-loom-staging-trial-cache-registry.sh"
CA_INSTALLER = ROOT / "deploy/slurm/install-loom-staging-trial-cache-ca.sh"

REGISTRY_REPO = "192.168.50.103:5443/loom-trial-cache"
CA_SHA256 = "539c97669d322f4fe91b91b4b8187a62a6618f5a9ec3f409e1ca5f9d7c56ecc3"
CANARY_DIGEST = "sha256:c64c687cbea9300178b30c95835354e34c4e4febc4badfe27102879de0483b5e"
REGISTRY_IMAGE = "registry@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373"


def _fake_docker(path: Path) -> Path:
    path.write_text(
        f"""#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == "pull" && "$2" == "--quiet" ]]; then
  [[ "$3" == "{REGISTRY_REPO}:transport-canary" ]]
  printf '%s\\n' "$3"
  exit 0
fi
if [[ "$1" == "image" && "$2" == "inspect" ]]; then
  [[ "${{@: -1}}" == "{REGISTRY_REPO}:transport-canary" ]]
  printf '%s\\n' '["{REGISTRY_REPO}@{CANARY_DIGEST}"]'
  exit 0
fi
exit 64
""",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def _run_probe(
    tmp_path: Path,
    *,
    installed_ca: bytes,
    hardlink_installed_ca: bool = False,
) -> subprocess.CompletedProcess[str]:
    env_file = tmp_path / "worker.env"
    env_file.write_text(
        "LOOM_WORKER_TOKEN=must-not-appear-in-output\n"
        f"LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO={REGISTRY_REPO}\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)
    cert_root = tmp_path / "docker-certs"
    installed = cert_root / "192.168.50.103:5443/ca.crt"
    installed.parent.mkdir(parents=True)
    if hardlink_installed_ca:
        installed.hardlink_to(tmp_path / "shared-ca-input")
    else:
        installed.write_bytes(installed_ca)
    docker = _fake_docker(tmp_path / "docker")
    return subprocess.run(
        [
            "python3",
            str(NODE_PROBE),
            "--env-file",
            str(env_file),
            "--ca-file",
            str(CA_FILE),
            "--docker-certs-root",
            str(cert_root),
            "--docker-bin",
            str(docker),
            "--expected-registry-repo",
            REGISTRY_REPO,
            "--expected-ca-sha256",
            CA_SHA256,
            "--canary-digest",
            CANARY_DIGEST,
        ],
        capture_output=True,
        text=True,
        check=False,
    )


def test_node_probe_requires_matching_daemon_trust_and_real_registry_pull(
    tmp_path: Path,
) -> None:
    result = _run_probe(tmp_path, installed_ca=CA_FILE.read_bytes())

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "ca_sha256": CA_SHA256,
        "registry_image": f"{REGISTRY_REPO}:transport-canary",
        "repo_digest": f"{REGISTRY_REPO}@{CANARY_DIGEST}",
    }
    assert "must-not-appear-in-output" not in result.stdout + result.stderr


def test_node_probe_refuses_drifted_docker_ca_before_accepting_pull(
    tmp_path: Path,
) -> None:
    result = _run_probe(tmp_path, installed_ca=b"different public CA\n")

    assert result.returncode == 1
    assert "installed registry CA does not match" in result.stderr
    assert result.stdout == ""
    assert "must-not-appear-in-output" not in result.stderr


def test_node_probe_refuses_hardlinked_trust_input(tmp_path: Path) -> None:
    shared = tmp_path / "shared-ca-input"
    shared.write_bytes(CA_FILE.read_bytes())

    result = _run_probe(
        tmp_path,
        installed_ca=CA_FILE.read_bytes(),
        hardlink_installed_ca=True,
    )

    assert result.returncode == 1
    assert "metadata is unsafe" in result.stderr
    assert result.stdout == ""


def test_registry_assets_are_parseable_and_pin_the_live_public_ca() -> None:
    assert hashlib.sha256(CA_FILE.read_bytes()).hexdigest() == CA_SHA256
    openssl = shutil.which("openssl")
    bash = shutil.which("bash")
    systemd_analyze = shutil.which("systemd-analyze")
    if openssl is None or bash is None or systemd_analyze is None:
        pytest.skip("openssl, bash, and systemd-analyze are required")

    certificate = subprocess.run(
        [openssl, "x509", "-in", str(CA_FILE), "-noout", "-subject", "-enddate"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert certificate.returncode == 0, certificate.stderr
    assert "Loom-Staging-Trial-Cache-CA" in certificate.stdout
    unit = REGISTRY_UNIT.read_text(encoding="utf-8")
    installer_source = REGISTRY_INSTALLER.read_text(encoding="utf-8")
    assert f" {REGISTRY_IMAGE}" in unit
    assert f'REGISTRY_IMAGE="{REGISTRY_IMAGE}"' in installer_source
    assert " registry:2" not in unit

    for installer in (REGISTRY_INSTALLER, CA_INSTALLER):
        parsed = subprocess.run(
            [bash, "-n", str(installer)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert parsed.returncode == 0, parsed.stderr

    verified = subprocess.run(
        [systemd_analyze, "verify", str(REGISTRY_UNIT)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert verified.returncode == 0, verified.stderr
