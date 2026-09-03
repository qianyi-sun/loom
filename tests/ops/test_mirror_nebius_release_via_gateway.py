from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

import pytest
from scripts.ops import mirror_nebius_release_via_gateway as mirror

SHA = "7" * 40
DIGESTS = {
    "gateway": "1" * 64,
    "control_plane": "2" * 64,
    "service": "3" * 64,
    "execution_actuator": "4" * 64,
    "execution_runtime": "5" * 64,
}
COMPONENTS = {
    "gateway": "loom-llm-gateway",
    "control_plane": "loom-control-plane",
    "service": "loom-service",
    "execution_actuator": "loom-execution-actuator",
    "execution_runtime": "loom-execution-runtime",
}
TARGET = "cr.eu-north1.nebius.cloud/e00example"


def _args(tmp_path: Path) -> argparse.Namespace:
    ssh_key = tmp_path / "id_ed25519"
    ssh_key.write_text("private\n", encoding="utf-8")
    ssh_key.chmod(0o600)
    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_text("gateway ssh-ed25519 public\n", encoding="utf-8")
    values: dict[str, object] = {
        "gateway": "192.0.2.20",
        "ssh_key": ssh_key,
        "known_hosts": known_hosts,
        "candidate_sha": SHA,
        "target_registry": TARGET,
        "output": tmp_path / "mirror.json",
    }
    for key, component in COMPONENTS.items():
        values[f"{key}_image"] = f"ghcr.io/qianyi-sun/{component}@sha256:{DIGESTS[key]}"
    return argparse.Namespace(**values)


def test_mirror_uses_gateway_identity_and_preserves_every_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _args(tmp_path)
    stdout = "".join(
        f"{component}\t{getattr(args, f'{key}_image')}\t"
        f"{TARGET}/{component}@sha256:{DIGESTS[key]}\n"
        for key, component in COMPONENTS.items()
    )
    calls: list[tuple[list[str], str]] = []

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((command, str(kwargs["input"])))
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(mirror.subprocess, "run", fake_run)
    payload = mirror.mirror(args)

    assert payload["candidate_sha"] == SHA
    assert payload["images"]["service"]["target_ref"].endswith(f"@sha256:{DIGESTS['service']}")
    command, remote_script = calls[0]
    assert command[:2] == ["ssh", "-o"]
    assert "BatchMode=yes" in command
    assert "StrictHostKeyChecking=yes" in command
    assert f"codex@{args.gateway}" in command
    assert "nebius registry docker-credential" in remote_script
    assert mirror.CRANE_LINUX_X86_64_SHA256 in remote_script
    assert "sudo" not in remote_script


def test_mirror_rejects_mutable_or_mismatched_source_image(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.service_image = "ghcr.io/qianyi-sun/loom-service:latest"
    with pytest.raises(ValueError, match="digest-pinned"):
        mirror.mirror(args)


def test_mirror_rejects_invalid_gateway_before_ssh(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.gateway = "host name"
    with pytest.raises(ValueError, match="gateway"):
        mirror.mirror(args)

    args = _args(tmp_path)
    args.service_image = f"ghcr.io/qianyi-sun/not-service@sha256:{'9' * 64}"
    with pytest.raises(ValueError, match="digest-pinned"):
        mirror.mirror(args)


def test_result_writer_is_atomic_and_owner_only(tmp_path: Path) -> None:
    output = tmp_path / "nested" / "mirror.json"
    payload = {"schema_version": "loom.nebius-release-mirror.v1"}
    mirror._write_json(output, payload)
    assert json.loads(output.read_text(encoding="utf-8")) == payload
    assert os.stat(output).st_mode & 0o777 == 0o600
