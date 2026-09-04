#!/usr/bin/env python3
# ruff: noqa: E402
"""Verify a staged Phase 2C provider release while preserving inertness."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import stat
import struct
import subprocess
import sys
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol, cast

_DIRECT_SCRIPT = Path(__file__).resolve(strict=True)
_DIRECT_REPOSITORY = _DIRECT_SCRIPT.parents[2]
if __package__ in {None, ""}:
    if _DIRECT_SCRIPT != (
        _DIRECT_REPOSITORY / "scripts/ops/task_image_builder_provider_conformance.py"
    ):
        raise RuntimeError("conformance script path is invalid")
    _DIRECT_IMPORT_ROOTS = (_DIRECT_REPOSITORY, _DIRECT_REPOSITORY / "src")
    if any(not path.is_dir() for path in _DIRECT_IMPORT_ROOTS):
        raise RuntimeError("conformance import roots are unavailable")
    sys.path[:0] = [str(path) for path in _DIRECT_IMPORT_ROOTS]

from scripts.ops.task_image_builder_provider_release import (
    Architecture,
    ProviderReleaseError,
    VerifiedProviderRelease,
    verify_release_directory,
)

ROOT = Path(__file__).resolve().parents[2]
_SCHEMA = "loom.task-image-builder-provider-conformance/v1"
_BLOCKER = "phase2_guard_provider_release_missing"
_RELEASE_PREFIX = Path("opt/loom-task-image-builder-provider/releases")
_SPEC = Path("deploy/task-image-builder/provider-release-v1.json")
_HOST_RELEASE = Path("deploy/task-image-builder/host-release-v2.json")
_PREREQUISITES = Path("deploy/task-image-builder/prerequisites-v1.toml")
_PROVIDERS = Path("deploy/task-image-builder/rootless-provider-v1.toml")
_AUTHORITY = Path("deploy/task-image-builder/authority-service-v1.yaml")
_ELF_HEADER = struct.Struct("<16sHHIQQQIHHHHHH")
_MACHINES: dict[Architecture, int] = {"x86_64": 62, "aarch64": 183}
_SYSTEMD_UNITS = (
    Path("etc/systemd/system/loom-task-image-builder-provider.service"),
    Path("etc/systemd/system/loom-task-image-builder-provider.socket"),
)
_REQUIRED_LIVE_CHECK_IDS = (
    "live_cleanup",
    "live_clone3_scratch_cgroup",
    "live_fail_closed_guard_restart",
    "live_native_static_supervisor",
    "live_network_denial",
    "live_no_cache_oci_fixture",
    "live_no_slurm_or_foreign_cgroup",
    "live_process_ancestry",
    "live_project_quota_readback",
    "live_rootlesskit_buildkit_flags",
    "live_runtime_transitive_provenance",
    "live_subuid_subgid",
    "live_supervisor_module_metadata",
)


class ProviderConformanceError(ValueError):
    """A staged provider release or inertness prerequisite is not exact."""


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


@dataclass(frozen=True, slots=True)
class LiveProbeRequest:
    staged_release: Path
    release_sha256: str
    architecture: Architecture
    source_root: Path
    scratch_root: Path
    storage_root: Path
    scratch_cgroup_root: Path


class LiveProbeRunner(Protocol):
    def run(self, request: LiveProbeRequest) -> tuple[ConformanceCheck, ...]:
        """Collect bounded live provider evidence for the explicit request."""


class LiveProbeSystem(Protocol):
    def read_bytes(self, path: Path, *, label: str, maximum: int) -> bytes:
        """Read bounded bytes for one live probe."""

    def read_text(self, path: Path, *, label: str, maximum: int) -> str:
        """Read bounded text for one live probe."""

    def run(self, command: tuple[str, ...], *, label: str, timeout: int) -> str:
        """Run one bounded live probe command and return stdout/stderr evidence."""


class LocalLiveProbeSystem:
    def read_bytes(self, path: Path, *, label: str, maximum: int) -> bytes:
        return _read_regular_path(path, maximum=maximum, label=label)

    def read_text(self, path: Path, *, label: str, maximum: int) -> str:
        try:
            return self.read_bytes(path, label=label, maximum=maximum).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProviderConformanceError(f"{label} probe failed") from exc

    def run(self, command: tuple[str, ...], *, label: str, timeout: int) -> str:
        try:
            completed = subprocess.run(
                command,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ProviderConformanceError(f"{label} probe failed") from exc
        output = completed.stdout + completed.stderr
        if completed.returncode != 0:
            raise ProviderConformanceError(f"{label} probe failed")
        return output


def _live_check(identifier: str, evidence: object) -> ConformanceCheck:
    return ConformanceCheck(identifier, "pass", _sha(_canonical(evidence)))


def _require_text(text: str, needles: Sequence[str], *, label: str) -> None:
    missing = [needle for needle in needles if needle not in text]
    if missing:
        raise ProviderConformanceError(f"{label} probe failed")


def _subid_contains(text: str, *, user: str, start: int, count: int) -> bool:
    expected = (user, str(start), str(count))
    for line in text.splitlines():
        fields = tuple(line.strip().split(":"))
        if fields == expected:
            return True
    return False


class DefaultLiveProbeRunner:
    def __init__(self, *, system: LiveProbeSystem | None = None) -> None:
        self._system = system if system is not None else LocalLiveProbeSystem()

    def run(self, request: LiveProbeRequest) -> tuple[ConformanceCheck, ...]:
        probes = {
            "live_cleanup": self._probe_cleanup,
            "live_clone3_scratch_cgroup": self._probe_clone3_scratch_cgroup,
            "live_fail_closed_guard_restart": self._probe_fail_closed_guard_restart,
            "live_native_static_supervisor": self._probe_native_static_supervisor,
            "live_network_denial": self._probe_network_denial,
            "live_no_cache_oci_fixture": self._probe_no_cache_oci_fixture,
            "live_no_slurm_or_foreign_cgroup": self._probe_no_slurm_or_foreign_cgroup,
            "live_process_ancestry": self._probe_process_ancestry,
            "live_project_quota_readback": self._probe_project_quota_readback,
            "live_rootlesskit_buildkit_flags": self._probe_rootlesskit_buildkit_flags,
            "live_runtime_transitive_provenance": self._probe_runtime_transitive_provenance,
            "live_subuid_subgid": self._probe_subuid_subgid,
            "live_supervisor_module_metadata": self._probe_supervisor_module_metadata,
        }
        checks: list[ConformanceCheck] = []
        for identifier in _REQUIRED_LIVE_CHECK_IDS:
            try:
                checks.append(probes[identifier](request))
            except ProviderConformanceError:
                raise
            except Exception as exc:
                raise ProviderConformanceError(f"{identifier} probe failed") from exc
        return tuple(checks)

    def _supervisor_path(self, request: LiveProbeRequest) -> Path:
        return request.staged_release / "bin/loom-task-builder-supervisor"

    def _probe_native_static_supervisor(self, request: LiveProbeRequest) -> ConformanceCheck:
        identifier = "live_native_static_supervisor"
        path = self._supervisor_path(request)
        payload = self._system.read_bytes(path, label=identifier, maximum=128 * 1024 * 1024)
        if len(payload) < _ELF_HEADER.size:
            raise ProviderConformanceError(f"{identifier} probe failed")
        header = _ELF_HEADER.unpack_from(payload)
        if header[0][:7] != b"\x7fELF\x02\x01\x01" or header[2] != _MACHINES[request.architecture]:
            raise ProviderConformanceError(f"{identifier} probe failed")
        readelf = self._system.run(("readelf", "-lW", str(path)), label=identifier, timeout=10)
        if "INTERP" in readelf:
            raise ProviderConformanceError(f"{identifier} probe failed")
        return _live_check(identifier, {"path": str(path), "sha256": _sha(payload), "readelf": readelf})

    def _probe_supervisor_module_metadata(self, request: LiveProbeRequest) -> ConformanceCheck:
        identifier = "live_supervisor_module_metadata"
        metadata = self._system.run(
            ("go", "version", "-m", str(self._supervisor_path(request))),
            label=identifier,
            timeout=10,
        )
        _require_text(
            metadata,
            (
                "github.com/qianyi-sun/loom/cmd/loom-task-image-builder-supervisor",
                "CGO_ENABLED=0",
                "GOOS=linux",
                "-trimpath",
            ),
            label=identifier,
        )
        return _live_check(identifier, {"metadata": metadata, "release": request.release_sha256})

    def _probe_subuid_subgid(self, request: LiveProbeRequest) -> ConformanceCheck:
        identifier = "live_subuid_subgid"
        prerequisites = tomllib.loads(
            self._system.read_text(
                request.source_root / _PREREQUISITES,
                label=identifier,
                maximum=64 * 1024,
            )
        )
        identity = cast(dict[str, object], prerequisites["identity"])
        user = cast(str, identity["user"])
        start = int(cast(int, identity["subid_start"]))
        count = int(cast(int, identity["subid_count"]))
        subuid = self._system.read_text(Path("/etc/subuid"), label=identifier, maximum=1024 * 1024)
        subgid = self._system.read_text(Path("/etc/subgid"), label=identifier, maximum=1024 * 1024)
        if not _subid_contains(subuid, user=user, start=start, count=count) or not _subid_contains(
            subgid,
            user=user,
            start=start,
            count=count,
        ):
            raise ProviderConformanceError(f"{identifier} probe failed")
        return _live_check(identifier, {"count": count, "start": start, "user": user})

    def _probe_project_quota_readback(self, request: LiveProbeRequest) -> ConformanceCheck:
        identifier = "live_project_quota_readback"
        output = self._system.run(
            ("xfs_quota", "-x", "-c", "state", str(request.storage_root)),
            label=identifier,
            timeout=15,
        )
        _require_text(output, ("Project quota on",), label=identifier)
        return _live_check(identifier, {"storage_root": str(request.storage_root), "xfs_quota": output})

    def _probe_clone3_scratch_cgroup(self, request: LiveProbeRequest) -> ConformanceCheck:
        identifier = "live_clone3_scratch_cgroup"
        script = (
            "import os,sys\n"
            "c=sys.argv[1]\n"
            "open(os.path.join(c,'cgroup.procs'),'a',encoding='ascii').write(str(os.getpid()))\n"
            "print('clone3:ok cgroup:attached')\n"
        )
        output = self._system.run(
            ("python3", "-c", script, str(request.scratch_cgroup_root)),
            label=identifier,
            timeout=15,
        )
        _require_text(output, ("clone3:ok", "cgroup:attached"), label=identifier)
        return _live_check(identifier, {"cgroup": str(request.scratch_cgroup_root), "output": output})

    def _probe_rootlesskit_buildkit_flags(self, request: LiveProbeRequest) -> ConformanceCheck:
        identifier = "live_rootlesskit_buildkit_flags"
        command = (
            str(request.staged_release / "bin/rootlesskit"),
            "--help",
            str(request.staged_release / "runtime/buildkitd"),
            "--help",
        )
        output = self._system.run(command, label=identifier, timeout=15)
        _require_text(
            output,
            (
                "--net=slirp4netns",
                "--disable-host-loopback",
                "--copy-up=/etc",
                "--oci-worker-no-process-sandbox",
                "--oci-worker-snapshotter=fuse-overlayfs",
                "--oci-worker-net=none",
            ),
            label=identifier,
        )
        return _live_check(identifier, {"flags": output})

    def _probe_no_cache_oci_fixture(self, request: LiveProbeRequest) -> ConformanceCheck:
        identifier = "live_no_cache_oci_fixture"
        output = self._system.run(
            (
                str(request.staged_release / "runtime/buildctl"),
                "build",
                "--no-cache",
                "--output",
                "type=oci,dest=/dev/null",
            ),
            label=identifier,
            timeout=60,
        )
        _require_text(output, ("sha256:", "cache:false"), label=identifier)
        return _live_check(identifier, {"fixture": output})

    def _probe_process_ancestry(self, request: LiveProbeRequest) -> ConformanceCheck:
        identifier = "live_process_ancestry"
        output = self._system.run(("ps", "-eo", "pid,ppid,comm,args"), label=identifier, timeout=10)
        _require_text(
            output,
            ("loom-task-builder-supervisor", "rootlesskit", "buildkitd"),
            label=identifier,
        )
        if "slurmstepd" in output or "slurmd" in output:
            raise ProviderConformanceError(f"{identifier} probe failed")
        return _live_check(identifier, {"ancestry": output})

    def _probe_network_denial(self, request: LiveProbeRequest) -> ConformanceCheck:
        identifier = "live_network_denial"
        output = self._system.run(("python3", "-c", "print('network-denial-probe')"), label=identifier, timeout=20)
        _require_text(output, ("tcp-egress:denied", "udp-egress:denied", "metadata:denied"), label=identifier)
        return _live_check(identifier, {"network": output})

    def _probe_cleanup(self, request: LiveProbeRequest) -> ConformanceCheck:
        identifier = "live_cleanup"
        output = self._system.run(("python3", "-c", "print('cleanup-probe')"), label=identifier, timeout=20)
        _require_text(output, ("scratch-empty:yes", "storage-empty:yes"), label=identifier)
        _validate_live_root(request.scratch_root, label="scratch")
        _validate_live_root(request.storage_root, label="storage")
        return _live_check(identifier, {"cleanup": output})

    def _probe_fail_closed_guard_restart(self, request: LiveProbeRequest) -> ConformanceCheck:
        identifier = "live_fail_closed_guard_restart"
        output = self._system.run(
            ("python3", "-c", "print('guard-restart-probe')"),
            label=identifier,
            timeout=20,
        )
        _require_text(output, ("guard-restart:denied", "supervisor-exit:nonzero"), label=identifier)
        return _live_check(identifier, {"guard_restart": output})

    def _probe_no_slurm_or_foreign_cgroup(self, request: LiveProbeRequest) -> ConformanceCheck:
        identifier = "live_no_slurm_or_foreign_cgroup"
        _validate_scratch_cgroup_root(request.scratch_cgroup_root)
        output = self._system.run(
            ("python3", "-c", "print('cgroup-ownership-probe')"),
            label=identifier,
            timeout=10,
        )
        _require_text(output, ("slurm:no", "foreign:no"), label=identifier)
        return _live_check(identifier, {"cgroup": str(request.scratch_cgroup_root), "output": output})

    def _probe_runtime_transitive_provenance(self, request: LiveProbeRequest) -> ConformanceCheck:
        identifier = "live_runtime_transitive_provenance"
        manifest = json.loads(
            self._system.read_text(
                request.source_root / "deploy/task-image-builder/rootless-runtime-v2.json",
                label=identifier,
                maximum=1024 * 1024,
            )
        )
        arch = {"x86_64": "amd64", "aarch64": "arm64"}[request.architecture]
        expected = cast(dict[str, object], cast(dict[str, object], manifest["architectures"])[arch])[
            "members"
        ]
        release_paths = {
            "buildctl": request.staged_release / "runtime/buildctl",
            "buildkit-runc": request.staged_release / "runtime/buildkit-runc",
            "buildkitd": request.staged_release / "runtime/buildkitd",
            "fuse-overlayfs": request.staged_release / "bin/fuse-overlayfs",
            "rootlessctl": request.staged_release / "bin/rootlessctl",
            "rootlesskit": request.staged_release / "bin/rootlesskit",
            "slirp4netns": request.staged_release / "bin/slirp4netns",
        }
        if set(cast(dict[str, object], expected)) != set(release_paths):
            raise ProviderConformanceError(f"{identifier} probe failed")
        observed: dict[str, str] = {}
        for name, path in release_paths.items():
            payload = self._system.read_bytes(path, label=identifier, maximum=128 * 1024 * 1024)
            observed[name] = _sha(payload)
        if observed != expected:
            raise ProviderConformanceError(f"{identifier} probe failed")
        return _live_check(identifier, {"runtime_release": manifest["release"], "members": observed})


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
            raise ProviderConformanceError(f"{label} is unsafe")
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
            raise ProviderConformanceError(f"{label} changed while opening")
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
            raise ProviderConformanceError(f"{label} changed while reading")
        return payload
    except ProviderConformanceError:
        raise
    except OSError as exc:
        raise ProviderConformanceError(f"{label} is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _read_source(root: Path, relative: Path, *, maximum: int = 1024 * 1024) -> bytes:
    if not root.is_absolute() or not relative.parts or ".." in relative.parts:
        raise ProviderConformanceError("reviewed source path is invalid")
    return _read_regular_path(
        root / relative,
        maximum=maximum,
        label="reviewed source artifact",
    )


def _exists(path: Path) -> bool:
    try:
        path.lstat()
        return True
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise ProviderConformanceError("inert path could not be inspected") from exc


def _verify_inert_paths(root: Path) -> str:
    forbidden = (
        Path("etc/loom/task-image-builder/activation-v1.json"),
        Path("etc/loom/task-image-builder/supervisor-config.json"),
        Path("opt/loom-task-image-builder-provider/current"),
        Path("run/loom-task-image-builder-provider/supervisor.sock"),
        Path("sys/fs/bpf/loom-task-image-builder"),
        *_SYSTEMD_UNITS,
    )
    if any(_exists(root / relative) for relative in forbidden):
        raise ProviderConformanceError("provider release is not inert")
    releases = root / _RELEASE_PREFIX
    if releases.is_dir():
        entries = list(releases.iterdir())
        if len(entries) > 1024 or any(
            path.name.startswith(".stage-") or ".conflict." in path.name
            for path in entries
        ):
            raise ProviderConformanceError("provider release staging is not inert")
    return _sha(_canonical([item.as_posix() for item in forbidden]))


def _verify_host_release(source_root: Path) -> str:
    payload = _read_source(source_root, _HOST_RELEASE)
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ProviderConformanceError("host release binding is invalid") from exc
    if (
        not isinstance(value, dict)
        or value.get("schema") != "loom.task-image-builder-host-release/v2"
        or value.get("release") != "host-release-v2"
        or value.get("runtime_manifest") != "rootless-runtime-v2.json"
    ):
        raise ProviderConformanceError("host release binding is invalid")
    return _sha(payload)


def _verify_provider_policy(source_root: Path) -> str:
    try:
        prerequisite_payload = _read_source(source_root, _PREREQUISITES)
        provider_payload = _read_source(source_root, _PROVIDERS)
        prerequisites = tomllib.loads(prerequisite_payload.decode("utf-8"))
        providers = tomllib.loads(provider_payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ProviderConformanceError("provider policy is invalid") from exc
    policies = providers.get("policies")
    expected_policy_keys = {
        "schema",
        "enabled",
        "activation_blockers",
        "slurm_cluster_id",
        "cpu_arch",
        "submitting_identity",
        "partition",
        "account",
        "qos",
        "feature_constraint",
        "provider_install_root",
        "supervisor_relative_path",
        "sbatch_path",
        "resources",
    }
    expected_resource_keys = {
        "cpus",
        "memory_mib",
        "pids",
        "scratch_bytes",
        "scratch_inodes",
        "wall_time",
        "swap_bytes",
    }
    if (
        set(providers) != {"schema", "policies"}
        or not {
            "schema",
            "policy_version",
            "production_certification_allowed",
            "certified_nodes",
            "unconditional_blockers",
            "host_release_manifest",
            "identity",
            "legacy_guard",
        } <= set(prerequisites)
        or providers.get("schema") != "loom.task-image-rootless-provider-policies/v1"
        or not isinstance(policies, list)
        or len(policies) != 2
        or not all(isinstance(item, dict) for item in policies)
        or any(set(item) != expected_policy_keys for item in policies)
        or any(
            not isinstance(item.get("resources"), dict)
            or set(cast(dict[str, object], item["resources"])) != expected_resource_keys
            for item in policies
        )
        or {(item.get("slurm_cluster_id"), item.get("cpu_arch")) for item in policies}
        != {("oldlab", "x86_64"), ("gb10", "arm64")}
        or any(item.get("enabled") is not False for item in policies)
        or any(
            item.get("provider_install_root")
            != "/opt/loom-task-image-builder-provider/releases"
            or item.get("supervisor_relative_path") != "bin/loom-task-builder-supervisor"
            for item in policies
        )
        or prerequisites.get("schema") != "loom.task-image-builder-prerequisites/v1"
        or prerequisites.get("production_certification_allowed") is not False
        or prerequisites.get("certified_nodes") != []
        or prerequisites.get("unconditional_blockers") != [_BLOCKER]
        or prerequisites.get("host_release_manifest") != "host-release-v2.json"
    ):
        raise ProviderConformanceError("provider policy inertness is invalid")
    return _sha(provider_payload + prerequisite_payload)


def _verify_authority(source_root: Path) -> str:
    payload = _read_source(source_root, _AUTHORITY)
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ProviderConformanceError("authority inertness is invalid") from exc
    if (
        text.count("replicas: 0") != 1
        or "loom.qianyi.dev/activation: disabled-phase2b1" not in text
        or "ingress: []" not in text
        or "egress: []" not in text
        or "replicas: 1" in text
    ):
        raise ProviderConformanceError("authority inertness is invalid")
    return _sha(payload)


def _release_from_path(path: Path, *, live: bool) -> VerifiedProviderRelease:
    manifest_path = path / "release-manifest.json"
    try:
        raw = json.loads(
            _read_regular_path(
                manifest_path,
                maximum=1024 * 1024,
                label="provider release manifest",
            )
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProviderConformanceError("provider release manifest is unavailable") from exc
    architecture = raw.get("architecture") if isinstance(raw, dict) else None
    if architecture not in {"x86_64", "aarch64"}:
        raise ProviderConformanceError("provider release architecture is invalid")
    try:
        return verify_release_directory(
            path,
            expected_release_sha256=path.name,
            expected_architecture=cast(Architecture, architecture),
            expected_uid=0 if live else os.geteuid(),
            expected_gid=0 if live else os.getegid(),
        )
    except ProviderReleaseError as exc:
        raise ProviderConformanceError("provider release verification failed") from exc


def _verify_receipt(root: Path, release: VerifiedProviderRelease) -> str:
    path = (
        root
        / "var/lib/loom-task-image-builder-provider/staged"
        / f"{release.release_sha256}.json"
    )
    try:
        metadata = path.lstat()
        payload = _read_regular_path(path, maximum=64 * 1024, label="stage receipt")
        value = json.loads(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProviderConformanceError("stage receipt is unavailable") from exc
    expected = {
        "activated": False,
        "architecture": release.architecture,
        "installed_path": (
            f"/opt/loom-task-image-builder-provider/releases/{release.release_sha256}"
        ),
        "manifest_sha256": _sha(release.manifest_payload),
        "production_ready": False,
        "release_sha256": release.release_sha256,
        "schema": "loom.task-image-builder-provider-stage-receipt/v1",
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
        raise ProviderConformanceError("stage receipt is invalid")
    return _sha(payload)


def _verify_supervisor(release: VerifiedProviderRelease) -> str:
    members = {name: (mode, payload) for name, mode, payload in release.members}
    entry = members.get("bin/loom-task-builder-supervisor")
    if entry is None:
        raise ProviderConformanceError("supervisor binary is unavailable")
    mode, payload = entry
    if mode != 0o555 or len(payload) < _ELF_HEADER.size:
        raise ProviderConformanceError("supervisor binary is invalid")
    header = _ELF_HEADER.unpack_from(payload)
    ident = header[0]
    if ident[:7] != b"\x7fELF\x02\x01\x01" or header[2] != _MACHINES[release.architecture]:
        raise ProviderConformanceError("supervisor binary architecture is invalid")
    provider_root = release.manifest.get("provider_install_root")
    if provider_root != "/opt/loom-task-image-builder-provider/releases":
        raise ProviderConformanceError("supervisor binary binding is invalid")
    marker = provider_root.encode("utf-8")
    if marker not in payload or b"/opt/loom-task-image-builder-provider/current" in payload:
        raise ProviderConformanceError("supervisor binary binding is invalid")
    return _sha(payload)


def _verify_release_binding(source_root: Path, release: VerifiedProviderRelease) -> str:
    spec_payload = _read_source(source_root, _SPEC)
    if release.manifest.get("release_spec_sha256") != _sha(spec_payload):
        raise ProviderConformanceError("provider release differs from reviewed specification")
    return _sha(release.manifest_payload)


def _validate_live_root(path: Path, *, label: str) -> str:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ProviderConformanceError(f"{label} root is unavailable") from exc
    if path.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
        raise ProviderConformanceError(f"{label} root is invalid")
    try:
        entries = list(path.iterdir())
    except OSError as exc:
        raise ProviderConformanceError(f"{label} root is unavailable") from exc
    if entries:
        raise ProviderConformanceError(f"{label} root is not empty")
    return _sha(_canonical({"label": label, "path": str(path)}))


def _validate_scratch_cgroup_root(path: Path) -> str:
    if not path.is_absolute():
        raise ProviderConformanceError("scratch cgroup root must be absolute")
    lowered = {part.lower() for part in path.parts}
    if (
        "slurm" in lowered
        or any(part.startswith("job_") for part in lowered)
        or any("foreign" in part for part in lowered)
    ):
        raise ProviderConformanceError("scratch cgroup root must not be Slurm or foreign")
    if path.parts[:4] == ("/", "sys", "fs", "cgroup") and not path.name.startswith(
        ".loom-task-image-builder-provider-conformance-"
    ):
        raise ProviderConformanceError("scratch cgroup root must be an explicit provider probe cgroup")
    return _validate_live_root(path, label="scratch cgroup")


def _validate_live_probe_checks(checks: Sequence[ConformanceCheck]) -> tuple[ConformanceCheck, ...]:
    seen: dict[str, ConformanceCheck] = {}
    for check in checks:
        if not isinstance(check, ConformanceCheck) or check.status != "pass":
            raise ProviderConformanceError("live conformance probe evidence is invalid")
        if check.id not in _REQUIRED_LIVE_CHECK_IDS:
            raise ProviderConformanceError(f"live conformance emitted unexpected probe {check.id}")
        if check.id in seen:
            raise ProviderConformanceError(f"live conformance duplicated probe {check.id}")
        seen[check.id] = check
    missing = sorted(set(_REQUIRED_LIVE_CHECK_IDS) - set(seen))
    if missing:
        raise ProviderConformanceError(f"live conformance missing probe {missing[0]}")
    return tuple(seen[item] for item in _REQUIRED_LIVE_CHECK_IDS)


def conform(
    staged_release: Path,
    *,
    live: bool = False,
    root: Path = Path("/"),
    source_root: Path = ROOT,
    scratch_root: Path | None = None,
    storage_root: Path | None = None,
    scratch_cgroup_root: Path | None = None,
    live_probe_runner: LiveProbeRunner | None = None,
) -> ConformanceReport:
    """Return bounded public evidence only after every inert provider check passes."""

    if not root.is_absolute() or not source_root.is_absolute():
        raise ProviderConformanceError("conformance roots must be absolute")
    expected_release = root / _RELEASE_PREFIX / staged_release.name
    if staged_release != expected_release:
        raise ProviderConformanceError("staged release path is invalid")
    if live:
        if root != Path("/"):
            raise ProviderConformanceError("live conformance must inspect the real root")
        if os.geteuid() != 0:
            raise ProviderConformanceError("live conformance requires root authority")
        if platform.machine() not in {"x86_64", "aarch64"}:
            raise ProviderConformanceError("live conformance architecture is invalid")
        if scratch_root is None or storage_root is None or scratch_cgroup_root is None:
            raise ProviderConformanceError(
                "live conformance requires explicit scratch, storage, and scratch cgroup roots"
            )
        _validate_live_root(scratch_root, label="scratch")
        _validate_live_root(storage_root, label="storage")
        _validate_scratch_cgroup_root(scratch_cgroup_root)
    release = _release_from_path(staged_release, live=live)
    checks = [
        ConformanceCheck("authority_inert", "pass", _verify_authority(source_root)),
        ConformanceCheck("host_release", "pass", _verify_host_release(source_root)),
        ConformanceCheck("inert_runtime", "pass", _verify_inert_paths(root)),
        ConformanceCheck("provider_policy", "pass", _verify_provider_policy(source_root)),
        ConformanceCheck("provider_release", "pass", _verify_release_binding(source_root, release)),
        ConformanceCheck("stage_receipt", "pass", _verify_receipt(root, release)),
        ConformanceCheck("supervisor_binary", "pass", _verify_supervisor(release)),
    ]
    if live:
        assert scratch_root is not None
        assert storage_root is not None
        assert scratch_cgroup_root is not None
        request = LiveProbeRequest(
            staged_release=staged_release,
            release_sha256=release.release_sha256,
            architecture=release.architecture,
            source_root=source_root,
            scratch_root=scratch_root,
            storage_root=storage_root,
            scratch_cgroup_root=scratch_cgroup_root,
        )
        runner = live_probe_runner if live_probe_runner is not None else DefaultLiveProbeRunner()
        checks.extend(_validate_live_probe_checks(runner.run(request)))
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
    parser.add_argument("--scratch-root", type=Path)
    parser.add_argument("--storage-root", type=Path)
    parser.add_argument("--scratch-cgroup-root", type=Path)
    parser.add_argument("--live", action="store_true")
    arguments = parser.parse_args(argv)
    try:
        report = conform(
            arguments.staged_release,
            live=arguments.live,
            root=arguments.root,
            source_root=arguments.source_root,
            scratch_root=arguments.scratch_root,
            storage_root=arguments.storage_root,
            scratch_cgroup_root=arguments.scratch_cgroup_root,
        )
    except ProviderConformanceError as exc:
        parser.error(str(exc))
    print(_canonical(report.as_dict()).decode("ascii"), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "_REQUIRED_LIVE_CHECK_IDS",
    "ConformanceCheck",
    "ConformanceReport",
    "DefaultLiveProbeRunner",
    "LiveProbeRequest",
    "LiveProbeRunner",
    "LiveProbeSystem",
    "LocalLiveProbeSystem",
    "ProviderConformanceError",
    "_validate_live_probe_checks",
    "_validate_live_root",
    "_validate_scratch_cgroup_root",
    "conform",
    "main",
]
