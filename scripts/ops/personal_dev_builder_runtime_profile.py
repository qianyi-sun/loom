"""Exact public profile for the personal-development gVisor builder runtime."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

_MAX_PROFILE_BYTES = 64 * 1024
_DIGEST = "0123456789abcdef"
_EXPECTED_MEMBERS: dict[str, dict[str, object]] = {
    "containerd-shim-runsc-v1": {
        "archive_mode": 0o755,
        "install_mode": 0o555,
        "sha256": "71b9e90897f39ee51fee8e0345cf675956d95bd1d6458c92f49d984097ffa327",
        "size": 43_208_193,
    },
    "gvisor-bin/checkpointgofer": {
        "archive_mode": 0o755,
        "install_mode": 0o555,
        "sha256": "a4f6837a9837a8c3499c7e2d1d58931babb140bf228762f1c2b13469256b2bda",
        "size": 68_743_833,
    },
    "gvisor-bin/gvisor_sentry": {
        "archive_mode": 0o755,
        "install_mode": 0o555,
        "sha256": "871a4b5ca197d37fae7d30ab0aa356fe3156c1f9836e8a40122f7f08c6b46f62",
        "size": 47_910_193,
    },
    "gvisor-bin/runsc-metric-server": {
        "archive_mode": 0o755,
        "install_mode": 0o555,
        "sha256": "ff3476a1f28cb684bd7340e183e80f8af7a5be5b0b3ca4bdb79bc2a6d92b6cb4",
        "size": 52_294_519,
    },
    "runsc": {
        "archive_mode": 0o755,
        "install_mode": 0o555,
        "sha256": "670bcd3cbc103f00d8bb5098edc370f32397ee4c134231436bafa659bb3c068e",
        "size": 104_854_508,
    },
}
_EXPECTED_FLAGS = {
    "allow-flag-override": "false",
    "allow-packet-socket-write": "false",
    "allow-suid": "false",
    "debug": "false",
    "directfs": "false",
    "file-access": "exclusive",
    "file-access-mounts": "shared",
    "gvisor-marker-file": "true",
    "host-fifo": "none",
    "host-settings": "check",
    "host-uds": "none",
    "net-raw": "false",
    "network": "sandbox",
    "oci-seccomp": "true",
    "platform": "kvm",
    "platform_device_path": "/dev/kvm",
    "profile": "false",
    "restore-spec-validation": "enforce",
    "sidecar-release-enforcement-policy": "ALWAYS",
    "strace": "false",
    "watchdog-action": "panic",
}
_EXPECTED_PROFILE: dict[str, object] = {
    "archive": {
        "members": _EXPECTED_MEMBERS,
        "sha512": (
            "3de91138cda15682c11807387f6ecad9e7c8932262018a2813277e1b4efa03efe"
            "33b0a948e148c6b1ccfe7345bfab5d5e0d072519505465751273898bae19c62"
        ),
        "url": (
            "https://storage.googleapis.com/gvisor/releases/"
            "release/20260810/x86_64/gvisor.tar.bz2"
        ),
    },
    "host": {
        "architecture": "amd64",
        "containerd_version": "v2.3.2-k3s2",
        "device": "/dev/kvm",
        "k3s_service": "k3s-agent",
        "k3s_version": "v1.36.2+k3s1",
        "modules": ["kvm", "kvm_intel"],
    },
    "installation": {
        "k3s_template": (
            "/var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.tmpl"
        ),
        "profile": "/etc/loom/personal-dev-builder-runtime-profile.json",
        "release_root": "/opt/loom/gvisor/release-20260810.0",
        "runsc_config": "/etc/containerd/runsc-personal-dev.toml",
        "shim_link": "/usr/local/bin/containerd-shim-runsc-v1",
    },
    "release": {
        "tag_commit": "5ceb9a5fd5750d6c73dd166441f28306039300d0",
        "version": "release-20260810.0",
    },
    "runtime": {
        "flags": _EXPECTED_FLAGS,
        "handler": "runsc-personal-dev",
        "runtime_type": "io.containerd.runsc.v1",
    },
    "runtime_class": {
        "name": "loom-personal-dev-builder",
        "profile_label_encoding": "sha256-halves-v1",
    },
    "schema": "loom.personal-dev-builder-runtime-profile.v1",
}


class RuntimeProfileError(RuntimeError):
    """The runtime profile is malformed, unreviewed, or unsafe to consume."""


@dataclass(frozen=True, slots=True)
class RuntimeArchiveMember:
    size: int
    archive_mode: int
    install_mode: int
    sha256: str


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise RuntimeProfileError("runtime profile contains a duplicate field")
        result[key] = value
    return result


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True, allow_nan=False)
        + "\n"
    ).encode("ascii")


def _file_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_profile(path: Path) -> bytes:
    if not isinstance(path, Path) or not path.is_absolute():
        raise RuntimeProfileError("runtime profile path must be absolute")
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or not 0 < before.st_size <= _MAX_PROFILE_BYTES
        ):
            raise RuntimeProfileError("runtime profile file is unsafe")
        payload = bytearray()
        while chunk := os.read(descriptor, min(64 * 1024, before.st_size + 1 - len(payload))):
            payload.extend(chunk)
        after = os.fstat(descriptor)
        if len(payload) != before.st_size or _file_identity(before) != _file_identity(after):
            raise RuntimeProfileError("runtime profile changed while being read")
        return bytes(payload)
    except OSError as exc:
        raise RuntimeProfileError("runtime profile file is unavailable") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


@dataclass(frozen=True, slots=True)
class RuntimeProfile:
    """Validated exact bytes and derived runtime installation values."""

    payload: bytes
    sha256: str
    members: Mapping[str, RuntimeArchiveMember]
    flags: Mapping[str, str]

    archive_sha512: str
    archive_url: str
    version: str
    tag_commit: str
    architecture: str
    k3s_version: str
    containerd_version: str
    k3s_service: str
    modules: tuple[str, ...]
    device_path: Path

    release_root: Path
    profile_path: Path
    runsc_config_path: Path
    k3s_template_path: Path
    shim_link_path: Path

    handler: str
    runtime_type: str
    runtime_class_name: str

    @classmethod
    def load(cls, path: Path) -> RuntimeProfile:
        return load_runtime_profile(path)

    @property
    def selector(self) -> dict[str, str]:
        return {
            "kubernetes.io/arch": self.architecture,
            "kubernetes.io/os": "linux",
            "loom.dev/personal-dev-runtime-profile-a": self.sha256[:32],
            "loom.dev/personal-dev-runtime-profile-b": self.sha256[32:],
        }

    @property
    def runsc_path(self) -> Path:
        return self.release_root / "runsc"

    @property
    def shim_path(self) -> Path:
        return self.release_root / "containerd-shim-runsc-v1"

    @property
    def runsc_toml(self) -> bytes:
        lines = [f'binary_name = "{self.runsc_path}"', "", "[runsc_config]"]
        lines.extend(f'  {key} = "{self.flags[key]}"' for key in sorted(self.flags))
        return ("\n".join(lines) + "\n").encode("ascii")

    @property
    def k3s_template(self) -> bytes:
        return (
            '{{ template "base" . }}\n\n'
            "[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes."
            f"'{self.handler}']\n"
            f'  runtime_type = "{self.runtime_type}"\n'
            "[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes."
            f"'{self.handler}'.options]\n"
            '  TypeUrl = "io.containerd.runsc.v1.options"\n'
            f'  ConfigPath = "{self.runsc_config_path}"\n'
        ).encode("ascii")


def load_runtime_profile(path: Path) -> RuntimeProfile:
    payload = _read_profile(path)
    try:
        value = json.loads(payload.decode("ascii"), object_pairs_hook=_unique_object)
    except (
        RecursionError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
    ) as exc:
        raise RuntimeProfileError("runtime profile is not valid JSON") from exc
    if not isinstance(value, dict) or payload != _canonical(value) or value != _EXPECTED_PROFILE:
        raise RuntimeProfileError("runtime profile differs from the reviewed contract")

    archive = value["archive"]
    host = value["host"]
    installation = value["installation"]
    release = value["release"]
    runtime = value["runtime"]
    runtime_class = value["runtime_class"]
    assert isinstance(archive, dict)
    assert isinstance(host, dict)
    assert isinstance(installation, dict)
    assert isinstance(release, dict)
    assert isinstance(runtime, dict)
    assert isinstance(runtime_class, dict)
    raw_members = archive["members"]
    raw_flags = runtime["flags"]
    assert isinstance(raw_members, dict)
    assert isinstance(raw_flags, dict)
    members = MappingProxyType(
        {
            name: RuntimeArchiveMember(
                size=int(member["size"]),
                archive_mode=int(member["archive_mode"]),
                install_mode=int(member["install_mode"]),
                sha256=str(member["sha256"]),
            )
            for name, member in raw_members.items()
            if isinstance(member, dict)
        }
    )
    if set(members) != set(raw_members):  # pragma: no cover - exact equality above
        raise RuntimeProfileError("runtime profile member records are invalid")
    digest = hashlib.sha256(payload).hexdigest()
    if len(digest) != 64 or any(character not in _DIGEST for character in digest):
        raise RuntimeProfileError("runtime profile digest is invalid")  # pragma: no cover
    return RuntimeProfile(
        payload=payload,
        sha256=digest,
        members=members,
        flags=MappingProxyType({str(key): str(item) for key, item in raw_flags.items()}),
        archive_sha512=str(archive["sha512"]),
        archive_url=str(archive["url"]),
        version=str(release["version"]),
        tag_commit=str(release["tag_commit"]),
        architecture=str(host["architecture"]),
        k3s_version=str(host["k3s_version"]),
        containerd_version=str(host["containerd_version"]),
        k3s_service=str(host["k3s_service"]),
        modules=tuple(str(item) for item in host["modules"]),
        device_path=Path(str(host["device"])),
        release_root=Path(str(installation["release_root"])),
        profile_path=Path(str(installation["profile"])),
        runsc_config_path=Path(str(installation["runsc_config"])),
        k3s_template_path=Path(str(installation["k3s_template"])),
        shim_link_path=Path(str(installation["shim_link"])),
        handler=str(runtime["handler"]),
        runtime_type=str(runtime["runtime_type"]),
        runtime_class_name=str(runtime_class["name"]),
    )


def render_runtime_class(profile: RuntimeProfile) -> dict[str, Any]:
    if not isinstance(profile, RuntimeProfile):
        raise TypeError("runtime profile is invalid")
    return {
        "apiVersion": "node.k8s.io/v1",
        "kind": "RuntimeClass",
        "metadata": {
            "name": profile.runtime_class_name,
            "annotations": {
                "loom.dev/runtime-profile-sha256": profile.sha256,
            },
        },
        "handler": profile.handler,
        "scheduling": {"nodeSelector": profile.selector},
    }


__all__ = [
    "RuntimeArchiveMember",
    "RuntimeProfile",
    "RuntimeProfileError",
    "load_runtime_profile",
    "render_runtime_class",
]
