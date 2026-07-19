"""Exact-candidate static systemd unit rendering and verification."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Protocol

UNIT_PATHS = (
    "deploy/worker-pools/gb10/loom-gb10-node-agent.service",
    "deploy/worker-pools/gb10/loom-gb10-node-agent.timer",
    "deploy/worker-pools/gb10/loom-gb10-worker.service",
)


class CommandResult(Protocol):
    @property
    def returncode(self) -> int: ...


CommandRunner = Callable[[Sequence[str]], CommandResult]


@dataclass(frozen=True, slots=True)
class SystemdUnitEvidence:
    ready: bool
    unit_sha256: Mapping[str, str]
    failed_units: Mapping[str, str]
    unit_set_digest: str

    def __post_init__(self) -> None:
        units = dict(self.unit_sha256)
        failures = dict(self.failed_units)
        if (
            set(units) | set(failures) != set(UNIT_PATHS)
            or set(units) & set(failures)
            or any(len(value) != 64 for value in units.values())
        ):
            raise ValueError("systemd unit evidence is inconsistent")
        object.__setattr__(self, "unit_sha256", MappingProxyType(units))
        object.__setattr__(self, "failed_units", MappingProxyType(failures))


def _read_exact_unit(root: Path, relative: str) -> bytes:
    root_path = root.resolve(strict=True)
    path = root / relative
    if not path.resolve(strict=True).is_relative_to(root_path):
        raise ValueError("systemd unit escapes candidate root")
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        raise ValueError("systemd unit source authority is unsafe")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        first = os.fstat(descriptor)
        payload = os.read(descriptor, 128 * 1024 + 1)
        second = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (
        len(payload) > 128 * 1024
        or (first.st_dev, first.st_ino, first.st_size, first.st_mtime_ns)
        != (second.st_dev, second.st_ino, second.st_size, second.st_mtime_ns)
        or first.st_size != len(payload)
    ):
        raise ValueError("systemd unit source is unstable")
    return payload


def _has_exact_line(payload: bytes, line: bytes) -> bool:
    return line in payload.splitlines()


def inspect_systemd_units(root: Path, *, run: CommandRunner) -> SystemdUnitEvidence:
    """Verify fixed unit files without installing, enabling, or starting them."""
    hashes: dict[str, str] = {}
    failures: dict[str, str] = {}
    payloads: dict[str, bytes] = {}
    for relative in UNIT_PATHS:
        try:
            payload = _read_exact_unit(root, relative)
        except (OSError, RuntimeError, ValueError):
            failures[relative] = "source-authority"
            continue
        payloads[relative] = payload
        hashes[relative] = hashlib.sha256(payload).hexdigest()

    node_service = payloads.get(UNIT_PATHS[0], b"")
    node_timer = payloads.get(UNIT_PATHS[1], b"")
    worker_service = payloads.get(UNIT_PATHS[2], b"")
    semantics = {
        UNIT_PATHS[0]: (
            _has_exact_line(node_service, b"Type=oneshot")
            and _has_exact_line(node_service, b"TimeoutStartSec=0")
        ),
        UNIT_PATHS[1]: (
            _has_exact_line(node_timer, b"Unit=loom-gb10-node-agent.service")
            and _has_exact_line(node_timer, b"OnUnitActiveSec=60s")
            and _has_exact_line(node_timer, b"WantedBy=timers.target")
        ),
        UNIT_PATHS[2]: (
            _has_exact_line(worker_service, b"Type=oneshot")
            and _has_exact_line(worker_service, b"RemainAfterExit=yes")
            and _has_exact_line(worker_service, b"TimeoutStartSec=0")
        ),
    }
    for relative, passed in semantics.items():
        if relative in payloads and not passed:
            failures[relative] = "loom-contract"
            hashes.pop(relative, None)

    if not failures:
        try:
            result = run(("systemd-analyze", "verify", *(str(root / path) for path in UNIT_PATHS)))
        except Exception:
            result = None
        if result is None or result.returncode != 0:
            failures = {relative: "systemd-analyze" for relative in UNIT_PATHS}
            hashes = {}
    evidence_payload = {
        "failed": failures,
        "units": hashes,
    }
    return SystemdUnitEvidence(
        ready=not failures and len(hashes) == len(UNIT_PATHS),
        unit_sha256=hashes,
        failed_units=failures,
        unit_set_digest=hashlib.sha256(
            json.dumps(evidence_payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    )


__all__ = [
    "UNIT_PATHS",
    "CommandRunner",
    "SystemdUnitEvidence",
    "inspect_systemd_units",
]
