"""Stdlib-only exact-source management tooling and fixed forced-SSH actions."""
from __future__ import annotations

import fcntl
import hashlib
import io
import json
import os
import re
import stat
import sys
import zipfile
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops.nebius_certificate_gateway import _directory, _read, _write, run_private

SOURCES = (*( "scripts/ops/" + name + ".py" for name in (
    "nebius_certificate_gateway", "nebius_certificates", "nebius_dns_challenge", "nebius_dns_publication",
    "nebius_ingress_bootstrap", "nebius_ingress_gateway", "nebius_ingress_stage", "nebius_ingress_cutover",
    "nebius_ingress_image", "nebius_ingress_operation", "nebius_ingress_probe",
    "nebius_management_gateway", "nebius_management_entry", "nebius_management_transport",
    "nebius_management_bootstrap", "nebius_management_material", "nebius_management_stage",
    "nebius_management_authority_stage", "nebius_management_authority_probe", "nebius_management_supplied",
    "nebius_management_storage", "nebius_management_install", "nebius_management_evidence",
    "nebius_management_proofs", "nebius_management_live", "nebius_management_capacity",
    "nebius_management_cloud_scope", "nebius_management_prerequisites",
)), "deploy/k8s/nebius-execution-actuator.yaml", "deploy/k8s/nebius-capacity-collector.yaml")
LIMITS = {**dict.fromkeys(SOURCES, 262144), "uv": 80 * 1024**2,
          "requirements.txt": 262144, "operation.json": 16384, "manifest.json": 16384}
MAX_BUNDLE, MAX_WHEEL = 100 * 1024**2, 16 * 1024**2
COMMANDS = {"loom-nebius-management-preflight-v1": "preflight", "loom-nebius-management-install-v1": "install"}
_ENTRY = "import sys; sys.path.insert(0, sys.argv[1]); from scripts.ops.nebius_management_entry import main; raise SystemExit(main(sys.argv[2], sys.argv[3]))"


class GatewayError(RuntimeError):
    """Payload-free failure; preserve credentials, installed resources and state."""


def validate_operation(value: dict[str, Any]) -> None:
    try:
        fields = {"schema", "source_sha", "candidate", "installation_id", "namespace",
                  "state_dir", "anchor_dir", "inputs_path", "inputs_sha256"}
        if set(value) != fields or any(not isinstance(item, str) or not 0 < len(item) <= 1024 for item in value.values()):
            raise ValueError()
        if value["schema"] != "loom.nebius-management-operation.v1":
            raise ValueError()
        if any(not re.fullmatch(r"[0-9a-f]{40}", value[key]) for key in ("source_sha", "candidate")):
            raise ValueError()
        if not re.fullmatch(r"[0-9a-f]{64}", value["inputs_sha256"]):
            raise ValueError()
        if str(UUID(value["installation_id"])) != value["installation_id"] or UUID(value["installation_id"]).int == 0:
            raise ValueError()
        if len(value["namespace"]) > 53 or not re.fullmatch(r"loom-nebius-management(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?", value["namespace"]):
            raise ValueError()
        for key in ("state_dir", "anchor_dir", "inputs_path"):
            path = Path(value[key])
            if not path.is_absolute() or path != path.resolve() or not re.fullmatch(r"/[A-Za-z0-9_./-]+", str(path)):
                raise ValueError()
        state = Path(value["state_dir"])
        if (state.name != "state" or state.parent.name != "nebius-management"
                or Path(value["anchor_dir"]) != state.parent / "anchor"
                or Path(value["inputs_path"]) != state.parent / "inputs.json"):
            raise ValueError()
    except Exception:
        raise GatewayError("invalid management operation metadata") from None


def unpack_bundle(content: bytes) -> tuple[dict[str, bytes], dict[str, Any]]:
    try:
        if not 0 < len(content) <= MAX_BUNDLE:
            raise ValueError()
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            entries = archive.infolist()
            names = [entry.filename for entry in entries]
            wheels = {name for name in names if name.startswith("wheels/")}
            if (len(names) != len(set(names)) or set(names) != set(LIMITS) | wheels or len(wheels) != 2
                    or not all(sum(bool(re.fullmatch(r"wheels/" + package + r"-[0-9][0-9.]*-py3-none-any\.whl", name))
                                   for name in wheels) == 1 for package in ("loom", "loom_bundle_checksum"))):
                raise ValueError()
            if any(entry.file_size > LIMITS.get(entry.filename, MAX_WHEEL) or entry.is_dir()
                   or stat.S_ISLNK(entry.external_attr >> 16) for entry in entries):
                raise ValueError()
            files = {entry.filename: archive.read(entry) for entry in entries}
        if json.loads(files["manifest.json"]) != {
            name: hashlib.sha256(value).hexdigest() for name, value in files.items() if name != "manifest.json"
        }:
            raise ValueError()
        operation = json.loads(files["operation.json"])
        validate_operation(operation)
        return files, operation
    except Exception:
        raise GatewayError("invalid management tooling bundle") from None


def command(release: Path, action: str) -> list[str]:
    if action not in {"qualify", "preflight", "install"}:
        raise GatewayError("management action outside fixed authority")
    return [str(release / "venv/bin/python"), "-I", "-c", _ENTRY,
            str(release), str(release / "operation.json"), action]


def prepare_release(content: bytes) -> Path:
    files, operation = unpack_bundle(content)
    root = Path(operation["state_dir"]).parent
    try:
        _directory(root)
        lock_path = root / "tooling.lock"
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        with os.fdopen(descriptor, "rb+") as lock:
            _read(lock_path, 1)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            releases = root / "releases"
            _directory(releases)
            release = releases / hashlib.sha256(content).hexdigest()
            if release.exists() or release.is_symlink():
                _directory(release)
                if _read(release / "complete", 64) != b"complete":
                    raise ValueError()
                for name, expected in files.items():
                    if _read(release / name, LIMITS.get(name, MAX_WHEEL)) != expected:
                        raise ValueError()
                return release
            if sum(1 for _ in releases.iterdir()) >= 8:
                raise ValueError()
            for path in (release, release / "scripts", release / "scripts/ops", release / "wheels",
                         release / "deploy", release / "deploy/k8s"):
                _directory(path)
            for name, value in files.items():
                _write(release / name, value, executable=name == "uv")
            uv, python = str(release / "uv"), str(release / "venv/bin/python")
            run_private([uv, "venv", "--no-config", "--no-cache", "--no-python-downloads",
                         "--python", "/usr/bin/python3", str(release / "venv")], timeout=90)
            run_private([uv, "pip", "sync", "--no-config", "--no-cache", "--python", python,
                         "--require-hashes", "--only-binary", ":all:", "--index-url", "https://pypi.org/simple",
                         str(release / "requirements.txt")], timeout=600)
            run_private([uv, "pip", "install", "--no-config", "--no-cache", "--python", python,
                         "--offline", "--no-deps", *sorted(str(release / name) for name in files if name.startswith("wheels/"))], timeout=90)
            run_private(command(release, "qualify"), timeout=60)
            _write(release / "complete", b"complete")
            return release
    except Exception:
        raise GatewayError("management tooling incomplete; retain private state") from None


def safe_report(raw: bytes, operation: dict[str, Any]) -> dict[str, Any]:
    try:
        validate_operation(operation)
        if len(raw) > 65536:
            raise ValueError()
        value = json.loads(raw)
        status = value["status"]
        if status not in {"preflight_qualified", "pending", "management_installed"}:
            raise ValueError()
        result = {"status": status}
        for key in ("source_sha", "candidate", "installation_id", "namespace"):
            if value[key] != operation[key]:
                raise ValueError()
            result[key] = value[key]
        if status in {"pending", "management_installed"}:
            uid, revision = value["namespace_uid"], value["revision"]
            if str(UUID(uid)) != uid or UUID(uid).int == 0 or not re.fullmatch(r"sha256:[0-9a-f]{64}", revision):
                raise ValueError()
            result.update(namespace_uid=uid, revision=revision)
        if status == "pending":
            if value["phase"] not in {"database", "migration", "backup", "service"}:
                raise ValueError()
            result["phase"] = value["phase"]
        if status == "management_installed":
            backup = value["backup"]
            uid, checksum, size, key = (backup[name] for name in ("job_uid", "sha256", "bytes", "key"))
            if (str(UUID(uid)) != uid or UUID(uid).int == 0 or not re.fullmatch(r"[0-9a-f]{64}", checksum)
                    or type(size) is not int or not 0 < size <= 1024**4
                    or not re.fullmatch(re.escape(operation["namespace"]) + r"/[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{6}-"
                                        + checksum[:12] + r"\.dump", key)):
                raise ValueError()
            result["backup"] = {"job_uid": uid, "sha256": checksum, "bytes": size, "key": key}
        return result
    except Exception:
        raise GatewayError("invalid management operation report") from None


def authorized_main(expected_sha256: str) -> int:
    action = COMMANDS.get(os.environ.get("SSH_ORIGINAL_COMMAND", ""))
    if action is None or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        return 126
    content = sys.stdin.buffer.read(MAX_BUNDLE + 1)
    if not 0 < len(content) <= MAX_BUNDLE or hashlib.sha256(content).hexdigest() != expected_sha256:
        return 126
    try:
        _, operation = unpack_bundle(content)
        release = prepare_release(content)
        report = safe_report(run_private(command(release, action), timeout=1800), operation)
        if (action == "preflight") != (report["status"] == "preflight_qualified"):
            raise ValueError()
        print(json.dumps(report, sort_keys=True))
        return 0
    except Exception:
        print("protected management operation incomplete; preserve private recovery state", file=sys.stderr)
        return 1
