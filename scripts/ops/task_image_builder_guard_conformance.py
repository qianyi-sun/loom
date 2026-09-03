#!/usr/bin/env python3
"""Verify a staged node-guard release while preserving production inertness."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import stat
import subprocess
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

from scripts.ops.task_image_builder_guard_release import (
    Architecture,
    GuardReleaseError,
    VerifiedGuardRelease,
    verify_release_directory,
)

ROOT = Path(__file__).resolve().parents[2]
_SCHEMA = "loom.task-image-builder-guard-conformance/v1"
_BLOCKER = "phase2_guard_provider_release_missing"
_RELEASE_PREFIX = Path("opt/loom-task-image-builder-guard/releases")
_UNIT = Path("deploy/task-image-builder/loom-task-image-builder-node-guard.service")
_SPEC = Path("deploy/task-image-builder/guard-release-v1.json")
_PREREQUISITES = Path("deploy/task-image-builder/prerequisites-v1.toml")
_PROVIDERS = Path("deploy/task-image-builder/rootless-provider-v1.toml")
_AUTHORITY = Path("deploy/task-image-builder/authority-service-v1.yaml")
_PREREQUISITES_SHA256 = "c661d1ee54b392f05f1d4eb2381ee10bc6579381ebdd817cdd440d9bb2ae5046"
_PROVIDERS_SHA256 = "a5286f925d1e78aa08673c36c922425f8f417d6b674fe99dfbef042542d25186"
_AUTHORITY_SHA256 = "b72c82f110c3b8b013fb14782f1dd7cb6faedccb4ae644fcdb783a236dc173fe"
_SELF_CHECK = (
    b'{"schema":"loom.task-image-builder-node-guard-self-check/v1",'
    b'"status":"ok"}\n'
)
_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class GuardConformanceError(ValueError):
    """A staged release or an inertness prerequisite is not exact."""


@dataclass(frozen=True, slots=True)
class ConformanceCheck:
    id: str
    status: Literal["pass"]
    evidence_sha256: str

    def as_dict(self) -> dict[str, object]:
        return {
            "evidence_sha256": self.evidence_sha256,
            "id": self.id,
            "status": self.status,
        }


@dataclass(frozen=True, slots=True)
class ConformanceReport:
    release_sha256: str
    architecture: Architecture
    live: bool
    checks: tuple[ConformanceCheck, ...]
    production_ready: bool = False
    blockers: tuple[str, ...] = (_BLOCKER,)

    def as_dict(self) -> dict[str, object]:
        return {
            "architecture": self.architecture,
            "blockers": list(self.blockers),
            "checks": [item.as_dict() for item in self.checks],
            "live": self.live,
            "production_ready": self.production_ready,
            "release_sha256": self.release_sha256,
            "schema": _SCHEMA,
        }


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode("ascii")


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _read_regular_path(path: Path, *, maximum: int, label: str) -> bytes:
    descriptor = -1
    try:
        initial = path.lstat()
        if (
            stat.S_ISLNK(initial.st_mode)
            or not stat.S_ISREG(initial.st_mode)
            or initial.st_nlink != 1
            or initial.st_size <= 0
            or initial.st_size > maximum
            or initial.st_mode & 0o002
        ):
            raise GuardConformanceError(f"{label} is unsafe")
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        opened = os.fstat(descriptor)
        identity = (
            initial.st_dev,
            initial.st_ino,
            initial.st_size,
            initial.st_mtime_ns,
            initial.st_ctime_ns,
        )
        if identity != (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
            opened.st_ctime_ns,
        ):
            raise GuardConformanceError(f"{label} changed while opening")
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        final = os.fstat(descriptor)
        final_path = path.lstat()
        if len(payload) != initial.st_size or len(payload) > maximum or identity != (
            final.st_dev,
            final.st_ino,
            final.st_size,
            final.st_mtime_ns,
            final.st_ctime_ns,
        ) or identity != (
            final_path.st_dev,
            final_path.st_ino,
            final_path.st_size,
            final_path.st_mtime_ns,
            final_path.st_ctime_ns,
        ):
            raise GuardConformanceError(f"{label} changed while reading")
        return payload
    except GuardConformanceError:
        raise
    except OSError as exc:
        raise GuardConformanceError(f"{label} is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _read_source(root: Path, relative: Path, *, maximum: int = 1024 * 1024) -> bytes:
    if not root.is_absolute() or not relative.parts or ".." in relative.parts:
        raise GuardConformanceError("reviewed source path is invalid")
    return _read_regular_path(
        root / relative,
        maximum=maximum,
        label="reviewed source artifact",
    )


def _unit_values(payload: bytes) -> dict[str, dict[str, tuple[str, ...]]]:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        raise GuardConformanceError("systemd unit encoding is invalid") from None
    sections: dict[str, dict[str, list[str]]] = {}
    section: str | None = None
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1]
            if section in sections:
                raise GuardConformanceError("systemd unit section is duplicate")
            sections[section] = {}
            continue
        if section is None or "=" not in line:
            raise GuardConformanceError("systemd unit syntax is invalid")
        key, value = line.split("=", 1)
        if not key or "\x00" in value:
            raise GuardConformanceError("systemd unit directive is invalid")
        sections[section].setdefault(key, []).append(value)
    return {
        name: {key: tuple(values) for key, values in directives.items()}
        for name, directives in sections.items()
    }


def validate_unit(payload: bytes) -> str:
    """Reject any unit template that could activate or broaden the guard."""

    values = _unit_values(payload)
    if set(values) != {"Unit", "Service"}:
        raise GuardConformanceError("systemd unit activation surface is invalid")
    unit = values["Unit"]
    service = values["Service"]
    required_unit = {
        "After": ("network-online.target",),
        "ConditionPathExists": (
            "/etc/loom/task-image-builder-guard/activation-v1.json",
        ),
        "StartLimitBurst": ("3",),
        "StartLimitIntervalSec": ("300s",),
        "Wants": ("network-online.target",),
    }
    required_service = {
        "AmbientCapabilities": (
            "CAP_BPF CAP_CHOWN CAP_DAC_OVERRIDE CAP_DAC_READ_SEARCH CAP_FOWNER CAP_NET_ADMIN CAP_SYS_ADMIN",
        ),
        "CapabilityBoundingSet": (
            "CAP_BPF CAP_CHOWN CAP_DAC_OVERRIDE CAP_DAC_READ_SEARCH CAP_FOWNER CAP_NET_ADMIN CAP_SYS_ADMIN",
        ),
        "ExecStart": (
            "/usr/bin/python3 -I -B /opt/loom-task-image-builder-guard/releases/"
            "@LOOM_GUARD_RELEASE_SHA256@/loom-task-image-builder-guard.pyz --config "
            "/etc/loom/task-image-builder-guard/config-v1.json",
        ),
        "Group": ("root",),
        "KillMode": ("control-group",),
        "LimitNOFILE": ("4096",),
        "LockPersonality": ("yes",),
        "MemoryDenyWriteExecute": ("yes",),
        "MemoryMax": ("512M",),
        "NoNewPrivileges": ("yes",),
        "NotifyAccess": ("main",),
        "OOMPolicy": ("stop",),
        "PrivateDevices": ("yes",),
        "PrivateTmp": ("yes",),
        "ProcSubset": ("all",),
        "ProtectClock": ("yes",),
        "ProtectControlGroups": ("no",),
        "ProtectHome": ("yes",),
        "ProtectHostname": ("yes",),
        "ProtectKernelLogs": ("yes",),
        "ProtectKernelModules": ("yes",),
        "ProtectKernelTunables": ("yes",),
        "ProtectProc": ("default",),
        "ProtectSystem": ("strict",),
        "ReadOnlyPaths": (
            "/etc/loom/task-image-builder-guard /opt/loom-task-image-builder-guard/"
            "releases/@LOOM_GUARD_RELEASE_SHA256@",
        ),
        "ReadWritePaths": (
            "/sys/fs/cgroup /sys/fs/bpf/loom-task-image-builder "
            "/run/loom-task-image-builder-guard /var/lib/loom-task-image-builder-guard",
        ),
        "RemoveIPC": ("yes",),
        "Restart": ("on-failure",),
        "RestartSec": ("5s",),
        "RestrictAddressFamilies": ("AF_UNIX AF_INET AF_INET6",),
        "RestrictNamespaces": ("yes",),
        "RestrictRealtime": ("yes",),
        "RestrictSUIDSGID": ("yes",),
        "RuntimeDirectory": ("loom-task-image-builder-guard",),
        "RuntimeDirectoryMode": ("0750",),
        "StateDirectory": ("loom-task-image-builder-guard",),
        "StateDirectoryMode": ("0700",),
        "SystemCallArchitectures": ("native",),
        "SystemCallErrorNumber": ("EPERM",),
        "SystemCallFilter": ("@system-service bpf memfd_create pidfd_open",),
        "TasksMax": ("256",),
        "TimeoutStartSec": ("60s",),
        "TimeoutStopSec": ("30s",),
        "Type": ("notify",),
        "UMask": ("0077",),
        "User": ("root",),
        "WatchdogSec": ("30s",),
        "WorkingDirectory": ("/",),
    }
    if any(unit.get(key) != expected for key, expected in required_unit.items()) or any(
        service.get(key) != expected for key, expected in required_service.items()
    ):
        raise GuardConformanceError("systemd unit hardening is invalid")
    environment = service.get("Environment")
    unset = service.get("UnsetEnvironment")
    if environment != ("LANG=C", "LC_ALL=C", "PATH=/usr/bin:/bin") or unset != (
        "ALL_PROXY CURL_CA_BUNDLE HTTP_PROXY HTTPS_PROXY LD_LIBRARY_PATH LD_PRELOAD "
        "NO_PROXY PYTHONHOME PYTHONINSPECT PYTHONPATH PYTHONSTARTUP REQUESTS_CA_BUNDLE "
        "SSL_CERT_DIR SSL_CERT_FILE all_proxy http_proxy https_proxy no_proxy",
    ):
        raise GuardConformanceError("systemd unit environment boundary is invalid")
    if set(unit) != {*required_unit, "Description"} or set(service) != {
        *required_service,
        "Environment",
        "UnsetEnvironment",
    }:
        raise GuardConformanceError("systemd unit directive surface is invalid")
    return _sha(payload)


def _exists(path: Path) -> bool:
    try:
        path.lstat()
        return True
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise GuardConformanceError("inert path could not be inspected") from exc


def _verify_inert_paths(root: Path) -> str:
    forbidden = (
        Path("etc/loom/task-image-builder-guard/config-v1.json"),
        Path("etc/loom/task-image-builder-guard/activation-v1.json"),
        Path("etc/systemd/system/loom-task-image-builder-node-guard.service"),
        Path("opt/loom-task-image-builder-guard/current"),
        Path("run/loom-task-image-builder-guard/guard.sock"),
        Path("sys/fs/bpf/loom-task-image-builder"),
    )
    if any(_exists(root / relative) for relative in forbidden):
        raise GuardConformanceError("guard is not inert")
    releases = root / _RELEASE_PREFIX
    if releases.is_dir():
        entries = list(releases.iterdir())
        if len(entries) > 1024 or any(
            path.name.startswith(".stage-") or ".conflict." in path.name
            for path in entries
        ):
            raise GuardConformanceError("guard release staging is not inert")
    return _sha(_canonical([path.as_posix() for path in forbidden]))


def _verify_provider(source_root: Path) -> tuple[str, str]:
    try:
        prerequisite_payload = _read_source(source_root, _PREREQUISITES)
        provider_payload = _read_source(source_root, _PROVIDERS)
        prerequisites = tomllib.loads(prerequisite_payload.decode("utf-8"))
        providers = tomllib.loads(provider_payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise GuardConformanceError("provider policy is invalid") from exc
    policies = providers.get("policies")
    if (
        _sha(prerequisite_payload) != _PREREQUISITES_SHA256
        or _sha(provider_payload) != _PROVIDERS_SHA256
        or providers.get("schema") != "loom.task-image-rootless-provider-policies/v1"
        or not isinstance(policies, list)
        or len(policies) != 2
        or not all(isinstance(item, dict) for item in policies)
        or {(item.get("slurm_cluster_id"), item.get("cpu_arch")) for item in policies}
        != {("oldlab", "x86_64"), ("gb10", "arm64")}
        or any(item.get("enabled") is not False for item in policies)
        or prerequisites.get("production_certification_allowed") is not False
        or prerequisites.get("certified_nodes") != []
        or prerequisites.get("unconditional_blockers") != [_BLOCKER]
    ):
        raise GuardConformanceError("provider policy inertness is invalid")
    legacy = prerequisites.get("legacy_guard")
    if legacy != {
        "qos": "loom-task-image-builder",
        "reservation": "loom-task-image-builder",
        "account": "loom-staging",
        "user": "loom-rollout",
        "max_jobs_per_user": 1,
        "max_submit_jobs_per_user": 1,
        "max_wall": "04:00:00",
    }:
        raise GuardConformanceError("Phase 1 rollback authority changed")
    return _sha(provider_payload + prerequisite_payload), _sha(_canonical(legacy))


def _verify_authority(source_root: Path) -> str:
    payload = _read_source(source_root, _AUTHORITY)
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        raise GuardConformanceError("authority inertness is invalid") from None
    if (
        _sha(payload) != _AUTHORITY_SHA256
        or text.count("  replicas: 0") != 1
        or re.search(r"^  replicas: (?!0$)", text, flags=re.MULTILINE) is not None
        or text.count("  ingress: []") != 1
        or text.count("  egress: []") != 1
        or "loom.qianyi.dev/activation: disabled-phase2b1" not in text
    ):
        raise GuardConformanceError("authority inertness is invalid")
    return _sha(payload)


def _release_from_path(path: Path, *, live: bool) -> VerifiedGuardRelease:
    manifest_path = path / "release-manifest.json"
    try:
        raw = json.loads(
            _read_regular_path(
                manifest_path,
                maximum=1024 * 1024,
                label="guard release manifest",
            )
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GuardConformanceError("guard release manifest is unavailable") from exc
    architecture = raw.get("architecture") if isinstance(raw, dict) else None
    if architecture not in {"x86_64", "aarch64"}:
        raise GuardConformanceError("guard release architecture is invalid")
    try:
        return verify_release_directory(
            path,
            expected_release_sha256=path.name,
            expected_architecture=cast(Architecture, architecture),
            expected_uid=0 if live else os.geteuid(),
        )
    except GuardReleaseError as exc:
        raise GuardConformanceError("guard release verification failed") from exc


def _verify_receipt(root: Path, release: VerifiedGuardRelease) -> str:
    path = (
        root
        / "var/lib/loom-task-image-builder-guard/staged"
        / f"{release.release_sha256}.json"
    )
    try:
        metadata = path.lstat()
        payload = _read_regular_path(path, maximum=64 * 1024, label="stage receipt")
        value = json.loads(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GuardConformanceError("stage receipt is unavailable") from exc
    expected = {
        "activated": False,
        "architecture": release.architecture,
        "installed_path": (
            f"/opt/loom-task-image-builder-guard/releases/{release.release_sha256}"
        ),
        "manifest_sha256": _sha(release.manifest_payload),
        "production_ready": False,
        "release_sha256": release.release_sha256,
        "schema": "loom.task-image-builder-guard-stage-receipt/v1",
    }
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_uid != (0 if root == Path("/") else os.geteuid())
        or payload != _canonical(value)
        or value != expected
    ):
        raise GuardConformanceError("stage receipt is invalid")
    return _sha(payload)


def _self_check(release: VerifiedGuardRelease) -> str:
    archive = release.directory / "loom-task-image-builder-guard.pyz"
    try:
        completed = subprocess.run(
            ("/usr/bin/python3", "-I", "-B", str(archive), "--self-check"),
            cwd="/",
            env={"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin"},
            stdin=subprocess.DEVNULL,
            capture_output=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise GuardConformanceError("guard zipapp self-check failed") from exc
    if completed.returncode != 0 or completed.stdout != _SELF_CHECK or completed.stderr:
        raise GuardConformanceError("guard zipapp self-check failed")
    return _sha(completed.stdout)


def _live_kernel_checks(release: VerifiedGuardRelease) -> tuple[ConformanceCheck, ...]:
    if os.geteuid() != 0:
        raise GuardConformanceError("live conformance requires root authority")
    controllers = Path("/sys/fs/cgroup/cgroup.controllers")
    bpffs = Path("/sys/fs/bpf")
    if not controllers.is_file() or not bpffs.is_dir():
        raise GuardConformanceError("live cgroup or bpffs prerequisite is unavailable")
    descriptors: list[int] = []
    try:
        descriptors.append(os.pidfd_open(os.getpid(), 0))
        memfd = os.memfd_create("loom-guard-conformance", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
        descriptors.append(memfd)
        os.write(memfd, b"probe")
        seals = fcntl.F_SEAL_SEAL | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_GROW | fcntl.F_SEAL_WRITE
        fcntl.fcntl(memfd, fcntl.F_ADD_SEALS, seals)
        if fcntl.fcntl(memfd, fcntl.F_GET_SEALS) != seals:
            raise GuardConformanceError("live sealed memfd prerequisite failed")
    except OSError as exc:
        raise GuardConformanceError("live kernel primitive prerequisite failed") from exc
    finally:
        for descriptor in descriptors:
            os.close(descriptor)
    bpftool = release.directory / "bpftool"
    try:
        completed = subprocess.run(
            (str(bpftool), "feature", "probe", "kernel"),
            cwd="/",
            env={"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin"},
            stdin=subprocess.DEVNULL,
            capture_output=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise GuardConformanceError("staged bpftool feature probe failed") from exc
    if (
        completed.returncode != 0
        or not completed.stdout
        or len(completed.stdout) > 1024 * 1024
        or len(completed.stderr) > 64 * 1024
    ):
        raise GuardConformanceError("staged bpftool feature probe failed")
    primitive_evidence = controllers.read_bytes() + b"\0" + str(bpffs.stat().st_dev).encode("ascii")
    return (
        ConformanceCheck("live_kernel_primitives", "pass", _sha(primitive_evidence)),
        ConformanceCheck("staged_bpftool_features", "pass", _sha(completed.stdout)),
    )


def conform(
    staged_release: Path,
    *,
    live: bool = False,
    root: Path = Path("/"),
    source_root: Path = ROOT,
) -> ConformanceReport:
    """Return bounded public evidence only after every inert guard check passes."""

    if not root.is_absolute() or not source_root.is_absolute():
        raise GuardConformanceError("conformance roots must be absolute")
    expected_release = root / _RELEASE_PREFIX / staged_release.name
    if staged_release != expected_release:
        raise GuardConformanceError("staged release path is invalid")
    release = _release_from_path(staged_release, live=live)
    spec_payload = _read_source(source_root, _SPEC)
    if release.manifest.get("release_spec_sha256") != _sha(spec_payload):
        raise GuardConformanceError("guard release differs from reviewed specification")
    unit_payload = _read_source(source_root, _UNIT)
    provider_digest, phase1_digest = _verify_provider(source_root)
    checks = [
        ConformanceCheck("authority_inert", "pass", _verify_authority(source_root)),
        ConformanceCheck(
            "bpf_artifacts",
            "pass",
            _sha(
                b"".join(
                    payload
                    for name, _mode, payload in release.members
                    if name.startswith("guard-network-")
                )
            ),
        ),
        ConformanceCheck("guard_release", "pass", _sha(release.manifest_payload)),
        ConformanceCheck("inert_runtime", "pass", _verify_inert_paths(root)),
        ConformanceCheck("phase1_rollback", "pass", phase1_digest),
        ConformanceCheck("provider_policy", "pass", provider_digest),
        ConformanceCheck("stage_receipt", "pass", _verify_receipt(root, release)),
        ConformanceCheck("systemd_unit", "pass", validate_unit(unit_payload)),
        ConformanceCheck("zipapp_self_check", "pass", _self_check(release)),
    ]
    if live:
        if root != Path("/"):
            raise GuardConformanceError("live conformance must inspect the real root")
        checks.extend(_live_kernel_checks(release))
    checks.sort(key=lambda item: item.id)
    return ConformanceReport(
        release_sha256=release.release_sha256,
        architecture=release.architecture,
        live=live,
        checks=tuple(checks),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--staged-release", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, default=ROOT)
    parser.add_argument("--root", type=Path, default=Path("/"))
    parser.add_argument("--live", action="store_true")
    arguments = parser.parse_args(argv)
    try:
        report = conform(
            arguments.staged_release,
            live=arguments.live,
            root=arguments.root,
            source_root=arguments.source_root,
        )
    except GuardConformanceError as exc:
        parser.error(str(exc))
    print(_canonical(report.as_dict()).decode("ascii"), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ConformanceCheck",
    "ConformanceReport",
    "GuardConformanceError",
    "conform",
    "main",
    "validate_unit",
]
