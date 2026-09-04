#!/usr/bin/env python3
"""Collect immutable Nebius runtime image evidence through its gateway."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import stat
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import cast

if __package__ in {None, ""}:  # pragma: no cover - direct script execution
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.install_trivy import TRIVY_RELEASE
from scripts.ops.mirror_nebius_release_via_gateway import (
    CRANE_LINUX_X86_64_SHA256,
    CRANE_VERSION,
)

_GATEWAY = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?$")
_TARGET_IMAGE = re.compile(
    r"^(cr\.eu-north1\.nebius\.cloud/[a-z0-9]+/([a-z0-9-]+))@"
    r"(sha256:[0-9a-f]{64})$"
)
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_FILES = (
    "service.sbom.cdx.json",
    "service.vulnerability.json",
    "execution-runtime.sbom.cdx.json",
    "execution-runtime.vulnerability.json",
    "runtime-binary.sha256",
)
_MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
_MAX_FILE_BYTES = 24 * 1024 * 1024
_SEVERITY_ORDER = {
    "UNKNOWN": 6,
    "CRITICAL": 5,
    "HIGH": 4,
    "MEDIUM": 3,
    "LOW": 2,
    "NEGLIGIBLE": 1,
}

_REMOTE_SCRIPT = r"""
set -euo pipefail

service_ref=$1
runtime_ref=$2
work_dir=$(mktemp -d)
cleanup() {
  find "$work_dir" -type f -delete
  find "$work_dir" -depth -type d -empty -delete
}
trap cleanup EXIT

curl --fail --location --silent --show-error --proto '=https' --tlsv1.2 \
  -o "$work_dir/trivy.tar.gz" \
  "https://github.com/aquasecurity/trivy/releases/download/__TRIVY_VERSION__/__TRIVY_ARCHIVE__"
printf '%s  %s\n' '__TRIVY_SHA256__' "$work_dir/trivy.tar.gz" \
  | sha256sum -c - >/dev/null
tar -xzf "$work_dir/trivy.tar.gz" -C "$work_dir" trivy
chmod 500 "$work_dir/trivy"

curl --fail --location --silent --show-error --proto '=https' --tlsv1.2 \
  -o "$work_dir/crane.tar.gz" \
  "https://github.com/google/go-containerregistry/releases/download/v__CRANE_VERSION__/go-containerregistry_Linux_x86_64.tar.gz"
printf '%s  %s\n' '__CRANE_SHA256__' "$work_dir/crane.tar.gz" \
  | sha256sum -c - >/dev/null
tar -xzf "$work_dir/crane.tar.gz" -C "$work_dir" crane
chmod 500 "$work_dir/crane"

mkdir -m 700 "$work_dir/bin" "$work_dir/docker" "$work_dir/cache"
cat >"$work_dir/bin/docker-credential-nebius" <<'WRAPPER'
#!/bin/sh
exec nebius registry docker-credential "$@"
WRAPPER
chmod 700 "$work_dir/bin/docker-credential-nebius"
printf '%s\n' '{"credHelpers":{"cr.eu-north1.nebius.cloud":"nebius"}}' \
  >"$work_dir/docker/config.json"
chmod 600 "$work_dir/docker/config.json"
export PATH="$work_dir/bin:$PATH"
export DOCKER_CONFIG="$work_dir/docker"

scan_image() {
  component=$1
  image_ref=$2
  "$work_dir/trivy" image --cache-dir "$work_dir/cache" --scanners vuln \
    --timeout 20m --format json \
    --output "$work_dir/$component.vulnerability.json" "$image_ref"
  "$work_dir/trivy" image --cache-dir "$work_dir/cache" --scanners vuln \
    --skip-db-update --timeout 20m --format cyclonedx \
    --output "$work_dir/$component.sbom.cdx.json" "$image_ref"
}

scan_image service "$service_ref"
scan_image execution-runtime "$runtime_ref"
"$work_dir/crane" export "$runtime_ref" - \
  | tar -xOf - loom-execution-runtime \
  | sha256sum \
  | awk '{print "sha256:" $1}' >"$work_dir/runtime-binary.sha256"

tar -C "$work_dir" -czf - \
  service.sbom.cdx.json service.vulnerability.json \
  execution-runtime.sbom.cdx.json execution-runtime.vulnerability.json \
  runtime-binary.sha256
"""
_REMOTE_SCRIPT = (
    _REMOTE_SCRIPT.replace("__TRIVY_VERSION__", TRIVY_RELEASE.version)
    .replace("__TRIVY_ARCHIVE__", TRIVY_RELEASE.archives["amd64"].filename)
    .replace("__TRIVY_SHA256__", TRIVY_RELEASE.archives["amd64"].sha256)
    .replace("__CRANE_VERSION__", CRANE_VERSION)
    .replace("__CRANE_SHA256__", CRANE_LINUX_X86_64_SHA256)
)


def _owner_only(path: Path, *, label: str) -> None:
    if not path.is_file() or stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise ValueError(f"{label} must be an owner-only file")


def _target_image(value: str, *, component: str) -> str:
    match = _TARGET_IMAGE.fullmatch(value)
    if match is None or match.group(2) != component:
        raise ValueError(f"{component} must be its digest-pinned Nebius image")
    return value


def _archive_files(payload: bytes) -> dict[str, bytes]:
    if len(payload) > _MAX_ARCHIVE_BYTES:
        raise ValueError("gateway evidence archive exceeds its size limit")
    result: dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
        for member in archive:
            if member.name not in _FILES or not member.isfile() or member.name in result:
                raise ValueError("gateway evidence archive contains an unexpected member")
            if member.size <= 0 or member.size > _MAX_FILE_BYTES:
                raise ValueError("gateway evidence member has an invalid size")
            source = archive.extractfile(member)
            if source is None:
                raise ValueError("gateway evidence member cannot be read")
            data = source.read(_MAX_FILE_BYTES + 1)
            if len(data) != member.size:
                raise ValueError("gateway evidence member is incomplete")
            result[member.name] = data
    if set(result) != set(_FILES):
        raise ValueError("gateway evidence archive is incomplete")
    return result


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError("image evidence must contain JSON objects")
    return cast(dict[str, object], value)


def _severity(report: bytes) -> str:
    payload = _object(json.loads(report))
    if _object(payload.get("Trivy")).get("Version") != TRIVY_RELEASE.version.removeprefix("v"):
        raise ValueError("vulnerability evidence uses an unexpected scanner")
    results = payload.get("Results")
    if not isinstance(results, list):
        raise ValueError("vulnerability evidence has no result list")
    observed: list[str] = []
    for raw_result in results:
        result = _object(raw_result)
        findings = result.get("Vulnerabilities", [])
        if not isinstance(findings, list):
            raise ValueError("vulnerability evidence has an invalid finding list")
        for raw_finding in findings:
            severity = _object(raw_finding).get("Severity")
            if not isinstance(severity, str) or severity not in _SEVERITY_ORDER:
                raise ValueError("vulnerability evidence has an unknown severity")
            observed.append(severity)
    if "CRITICAL" in observed:
        raise ValueError("image evidence fails runtime policy: critical")
    highest = max(observed, key=_SEVERITY_ORDER.__getitem__) if observed else "NONE"
    return highest.lower()


def _validate_sbom(payload: bytes) -> None:
    document = _object(json.loads(payload))
    if document.get("bomFormat") != "CycloneDX" or not isinstance(document.get("specVersion"), str):
        raise ValueError("SBOM evidence is not CycloneDX")
    if not isinstance(document.get("components"), list):
        raise ValueError("SBOM evidence has no component inventory")


def _write_file(path: Path, payload: bytes) -> None:
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def collect(args: argparse.Namespace) -> dict[str, object]:
    if _GATEWAY.fullmatch(args.gateway) is None:
        raise ValueError("gateway must be an IPv4 address or DNS hostname")
    ssh_key = args.ssh_key.resolve()
    known_hosts = args.known_hosts.resolve()
    _owner_only(ssh_key, label="SSH key")
    if not known_hosts.is_file() or not known_hosts.read_text(encoding="utf-8").strip():
        raise ValueError("known-hosts must be a non-empty file")
    service_ref = _target_image(args.service_image, component="loom-service")
    runtime_ref = _target_image(args.execution_runtime_image, component="loom-execution-runtime")
    command = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={known_hosts}",
        "-o",
        "ConnectTimeout=15",
        "-i",
        str(ssh_key),
        f"codex@{args.gateway}",
        "bash",
        "-s",
        "--",
        service_ref,
        runtime_ref,
    ]
    completed = subprocess.run(
        command,
        input=_REMOTE_SCRIPT.encode("utf-8"),
        check=True,
        capture_output=True,
    )
    files = _archive_files(completed.stdout)
    service_severity = _severity(files["service.vulnerability.json"])
    runtime_severity = _severity(files["execution-runtime.vulnerability.json"])
    _validate_sbom(files["service.sbom.cdx.json"])
    _validate_sbom(files["execution-runtime.sbom.cdx.json"])
    runtime_binary_sha256 = files["runtime-binary.sha256"].decode("ascii").strip()
    if _SHA256.fullmatch(runtime_binary_sha256) is None:
        raise ValueError("runtime binary digest is invalid")

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    output_dir.chmod(0o700)
    for name, payload in files.items():
        _write_file(output_dir / name, payload)
    images = {
        "service": {
            "image_ref": service_ref,
            "sbom_sha256": "sha256:" + hashlib.sha256(files["service.sbom.cdx.json"]).hexdigest(),
            "vulnerability_report_sha256": "sha256:"
            + hashlib.sha256(files["service.vulnerability.json"]).hexdigest(),
            "highest_vulnerability_severity": service_severity,
        },
        "execution_runtime": {
            "image_ref": runtime_ref,
            "sbom_sha256": "sha256:"
            + hashlib.sha256(files["execution-runtime.sbom.cdx.json"]).hexdigest(),
            "vulnerability_report_sha256": "sha256:"
            + hashlib.sha256(files["execution-runtime.vulnerability.json"]).hexdigest(),
            "highest_vulnerability_severity": runtime_severity,
        },
    }
    summary: dict[str, object] = {
        "schema_version": "loom.nebius-runtime-evidence.v1",
        "scanner": {"name": "Trivy", "version": TRIVY_RELEASE.version},
        "images": images,
        "runtime_binary_sha256": runtime_binary_sha256,
    }
    _write_file(
        output_dir / "summary.json",
        (json.dumps(summary, sort_keys=True, separators=(",", ":")) + "\n").encode(),
    )
    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway", required=True)
    parser.add_argument("--ssh-key", required=True, type=Path)
    parser.add_argument("--known-hosts", required=True, type=Path)
    parser.add_argument("--service-image", required=True)
    parser.add_argument("--execution-runtime-image", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser


def main() -> int:
    try:
        summary = collect(_parser().parse_args())
    except (OSError, ValueError, subprocess.CalledProcessError, tarfile.TarError) as exc:
        raise SystemExit(f"error: {exc}") from exc
    print(json.dumps(summary, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
