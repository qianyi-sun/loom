from __future__ import annotations

import argparse
import io
import json
import os
import subprocess
import tarfile
from pathlib import Path

import pytest
from scripts.ops import collect_nebius_runtime_evidence_via_gateway as collector


def _args(tmp_path: Path) -> argparse.Namespace:
    ssh_key = tmp_path / "id_ed25519"
    ssh_key.write_text("private\n", encoding="utf-8")
    ssh_key.chmod(0o600)
    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_text("gateway ssh-ed25519 public\n", encoding="utf-8")
    return argparse.Namespace(
        gateway="192.0.2.20",
        ssh_key=ssh_key,
        known_hosts=known_hosts,
        service_image=("cr.eu-north1.nebius.cloud/e00example/loom-service@sha256:" + "1" * 64),
        execution_runtime_image=(
            "cr.eu-north1.nebius.cloud/e00example/loom-execution-runtime@sha256:" + "2" * 64
        ),
        output_dir=tmp_path / "evidence",
    )


def _report(severity: str | None) -> bytes:
    vulnerabilities = [] if severity is None else [{"Severity": severity}]
    return json.dumps(
        {
            "Trivy": {"Version": "0.74.0"},
            "Results": [{"Vulnerabilities": vulnerabilities}],
        }
    ).encode()


def _archive(*, service_severity: str = "HIGH") -> bytes:
    sbom = json.dumps({"bomFormat": "CycloneDX", "specVersion": "1.6", "components": []}).encode()
    files = {
        "service.sbom.cdx.json": sbom,
        "service.vulnerability.json": _report(service_severity),
        "execution-runtime.sbom.cdx.json": sbom,
        "execution-runtime.vulnerability.json": _report(None),
        "runtime-binary.sha256": ("sha256:" + "3" * 64 + "\n").encode(),
    }
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, payload in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))
    return buffer.getvalue()


def test_collect_uses_gateway_identity_and_writes_owner_only_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _args(tmp_path)
    calls: list[tuple[list[str], bytes]] = []

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        calls.append((command, bytes(kwargs["input"])))
        return subprocess.CompletedProcess(command, 0, stdout=_archive(), stderr=b"")

    monkeypatch.setattr(collector.subprocess, "run", fake_run)
    summary = collector.collect(args)

    assert summary["runtime_binary_sha256"] == "sha256:" + "3" * 64
    images = summary["images"]
    assert isinstance(images, dict)
    assert images["service"]["highest_vulnerability_severity"] == "high"
    assert images["execution_runtime"]["highest_vulnerability_severity"] == "none"
    command, remote_script = calls[0]
    assert f"codex@{args.gateway}" in command
    assert b"nebius registry docker-credential" in remote_script
    assert collector.TRIVY_RELEASE.archives["amd64"].sha256.encode() in remote_script
    assert collector.CRANE_LINUX_X86_64_SHA256.encode() in remote_script
    assert "sudo" not in remote_script.decode()
    for name in (*collector._FILES, "summary.json"):
        assert os.stat(args.output_dir / name).st_mode & 0o777 == 0o600


def test_collect_rejects_critical_evidence_before_writing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _args(tmp_path)

    def fake_run(command: list[str], **_: object) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(
            command, 0, stdout=_archive(service_severity="CRITICAL"), stderr=b""
        )

    monkeypatch.setattr(collector.subprocess, "run", fake_run)
    with pytest.raises(ValueError, match="critical"):
        collector.collect(args)
    assert not args.output_dir.exists()


def test_collect_rejects_wrong_target_component(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.service_image = "cr.eu-north1.nebius.cloud/e00example/not-service@sha256:" + "1" * 64
    with pytest.raises(ValueError, match="loom-service"):
        collector.collect(args)
