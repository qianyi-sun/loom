"""Exact, isolated transient-unit rehearsal across the fixed GB10 fleet."""

from __future__ import annotations

import hashlib
import json
import re
import shlex
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Protocol

from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.gb10_readiness import ACTIVE_GB10_HOSTS
from loom_cli.rollout.systemd_readiness import RehearsalSystemdActivation

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_BOOT_ID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z"
)
_KNOWN_HOSTS = Path("/etc/loom/staging-rollout-gb10-known-hosts")
_MAX_OUTPUT_BYTES = 16 * 1024


class CommandResult(Protocol):
    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> str: ...

    @property
    def stderr(self) -> str: ...


CommandRunner = Callable[[Sequence[str], int], CommandResult]


@dataclass(frozen=True, slots=True)
class GB10RehearsalAuthority:
    """Secret-free exact input identity for the fleet rehearsal transport."""

    hosts: tuple[str, ...]
    ssh_config: Path
    identity: Path
    ssh_config_sha256: str
    identity_metadata_fingerprint: str
    max_concurrency: int = 8

    def __post_init__(self) -> None:
        if (
            self.hosts != ACTIVE_GB10_HOSTS
            or not self.ssh_config.is_absolute()
            or not self.identity.is_absolute()
            or ".." in self.ssh_config.parts
            or ".." in self.identity.parts
            or _SHA256_RE.fullmatch(self.ssh_config_sha256) is None
            or _SHA256_RE.fullmatch(self.identity_metadata_fingerprint) is None
            or not 1 <= self.max_concurrency <= 16
        ):
            raise ValueError("GB10 rehearsal authority is invalid")

    def to_record(self) -> dict[str, object]:
        return {
            "hosts": list(self.hosts),
            "identity": str(self.identity),
            "identity_metadata_fingerprint": self.identity_metadata_fingerprint,
            "max_concurrency": self.max_concurrency,
            "ssh_config": str(self.ssh_config),
            "ssh_config_sha256": self.ssh_config_sha256,
        }

    @classmethod
    def from_record(cls, value: Mapping[str, object]) -> GB10RehearsalAuthority:
        expected = {
            "hosts",
            "identity",
            "identity_metadata_fingerprint",
            "max_concurrency",
            "ssh_config",
            "ssh_config_sha256",
        }
        hosts = value.get("hosts")
        max_concurrency = value.get("max_concurrency")
        if (
            set(value) != expected
            or not isinstance(hosts, list)
            or any(not isinstance(host, str) for host in hosts)
            or any(
                not isinstance(value.get(field), str)
                for field in (
                    "identity",
                    "identity_metadata_fingerprint",
                    "ssh_config",
                    "ssh_config_sha256",
                )
            )
            or type(max_concurrency) is not int
        ):
            raise ValueError("GB10 rehearsal authority schema is invalid")
        return cls(
            hosts=tuple(hosts),
            ssh_config=Path(str(value["ssh_config"])),
            identity=Path(str(value["identity"])),
            ssh_config_sha256=str(value["ssh_config_sha256"]),
            identity_metadata_fingerprint=str(value["identity_metadata_fingerprint"]),
            max_concurrency=max_concurrency,
        )


@dataclass(frozen=True, slots=True)
class GB10RehearsalEvidence:
    host_boot_ids: Mapping[str, str]
    host_evidence_digests: Mapping[str, str]
    blockers: Mapping[str, str]
    cleanup_verified: bool

    def __post_init__(self) -> None:
        boot_ids = dict(self.host_boot_ids)
        digests = dict(self.host_evidence_digests)
        blockers = dict(self.blockers)
        if (
            set(boot_ids) != set(digests)
            or set(boot_ids) | set(blockers) != set(ACTIVE_GB10_HOSTS)
            or set(boot_ids) & set(blockers)
            or any(_BOOT_ID_RE.fullmatch(value) is None for value in boot_ids.values())
            or any(_SHA256_RE.fullmatch(value) is None for value in digests.values())
            or any(host not in ACTIVE_GB10_HOSTS or not reason for host, reason in blockers.items())
            or self.cleanup_verified != (not blockers)
        ):
            raise ValueError("GB10 rehearsal evidence is invalid")
        object.__setattr__(self, "host_boot_ids", MappingProxyType(boot_ids))
        object.__setattr__(self, "host_evidence_digests", MappingProxyType(digests))
        object.__setattr__(self, "blockers", MappingProxyType(blockers))

    @property
    def evidence_digest(self) -> str:
        return _hash_json(
            {
                "blockers": dict(self.blockers),
                "boot_ids": dict(self.host_boot_ids),
                "cleanup_verified": self.cleanup_verified,
                "host_evidence": dict(self.host_evidence_digests),
            }
        )


@dataclass(frozen=True, slots=True)
class FixedGB10RehearsalTransport:
    """Exercise only a plan-derived transient unit on the fixed GB10 hosts."""

    authority: GB10RehearsalAuthority
    service_uid: int
    run: CommandRunner

    def __post_init__(self) -> None:
        if self.service_uid < 0:
            raise ValueError("GB10 rehearsal service identity is invalid")

    def execute(self, contract: RehearsalSystemdActivation) -> GB10RehearsalEvidence:
        self._verify_local_authority()
        return self._fleet(contract, mode="execute")

    def cleanup(self, contract: RehearsalSystemdActivation) -> GB10RehearsalEvidence:
        self._verify_local_authority()
        return self._fleet(contract, mode="cleanup")

    def _verify_local_authority(self) -> None:
        config = read_trusted_file(
            self.authority.ssh_config,
            service_uid=self.service_uid,
            private=False,
            require_nonempty=True,
        )
        identity = read_trusted_file(
            self.authority.identity,
            service_uid=self.service_uid,
            private=True,
            require_nonempty=True,
        )
        if (
            hashlib.sha256(config.payload).hexdigest() != self.authority.ssh_config_sha256
            or identity.metadata_fingerprint != self.authority.identity_metadata_fingerprint
        ):
            raise ValueError("GB10 rehearsal local authority drifted")

    def _fleet(
        self,
        contract: RehearsalSystemdActivation,
        *,
        mode: str,
    ) -> GB10RehearsalEvidence:
        if mode not in {"execute", "cleanup"}:
            raise ValueError("GB10 rehearsal mode is invalid")
        successes: dict[str, tuple[str, str]] = {}
        blockers: dict[str, str] = {}
        with ThreadPoolExecutor(
            max_workers=min(self.authority.max_concurrency, len(self.authority.hosts)),
            thread_name_prefix="loom-gb10-rehearsal",
        ) as executor:
            futures = {
                executor.submit(self._one, host, contract, mode=mode): host
                for host in self.authority.hosts
            }
            for future in as_completed(futures):
                host = futures[future]
                try:
                    record = future.result()
                except Exception:
                    blockers[host] = "transport-unavailable"
                    continue
                reason = self._validate_record(record, contract, mode=mode)
                if reason is not None:
                    blockers[host] = reason
                    continue
                successes[host] = (str(record["boot_id"]), _hash_json(record))
        return GB10RehearsalEvidence(
            host_boot_ids={host: value[0] for host, value in successes.items()},
            host_evidence_digests={host: value[1] for host, value in successes.items()},
            blockers=blockers,
            cleanup_verified=not blockers,
        )

    def _one(
        self,
        host: str,
        contract: RehearsalSystemdActivation,
        *,
        mode: str,
    ) -> dict[str, object]:
        command = "python3 -c " + shlex.quote(_remote_source(contract, mode=mode))
        result = self.run(self._ssh_argv(host, command), 60)
        if (
            result.returncode != 0
            or result.stderr
            or len(result.stdout.encode()) > _MAX_OUTPUT_BYTES
        ):
            raise RuntimeError("GB10 rehearsal command failed")
        value = json.loads(result.stdout)
        if not isinstance(value, dict):
            raise ValueError("GB10 rehearsal output is invalid")
        return value

    def _ssh_argv(self, host: str, command: str) -> tuple[str, ...]:
        if host not in self.authority.hosts:
            raise ValueError("GB10 rehearsal host escaped fixed inventory")
        return (
            "ssh",
            "-F",
            str(self.authority.ssh_config),
            "-i",
            str(self.authority.identity),
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            f"UserKnownHostsFile={_KNOWN_HOSTS}",
            "-o",
            "GlobalKnownHostsFile=/dev/null",
            "-o",
            "UpdateHostKeys=no",
            host,
            command,
        )

    @staticmethod
    def _validate_record(
        value: Mapping[str, object],
        contract: RehearsalSystemdActivation,
        *,
        mode: str,
    ) -> str | None:
        expected = {
            "boot_id",
            "cleanup_verified",
            "latency_ms",
            "mode",
            "properties",
            "reason",
            "unit",
        }
        properties = value.get("properties")
        latency_ms = value.get("latency_ms")
        if (
            set(value) != expected
            or value.get("mode") != mode
            or value.get("unit") != contract.unit
            or not isinstance(value.get("boot_id"), str)
            or _BOOT_ID_RE.fullmatch(str(value["boot_id"])) is None
            or type(value.get("cleanup_verified")) is not bool
            or type(latency_ms) is not int
            or not isinstance(value.get("reason"), str)
            or not isinstance(properties, dict)
            or any(
                not isinstance(key, str) or not isinstance(item, str)
                for key, item in properties.items()
            )
        ):
            return "output-invalid"
        if value["cleanup_verified"] is not True:
            return "cleanup-not-verified"
        if value["reason"]:
            return str(value["reason"])
        if mode == "execute" and not contract.ready(
            properties,
            latency_ms=latency_ms,
        ):
            return "activation-readback-drift"
        if mode == "cleanup" and properties and not contract.ready(properties, latency_ms=0):
            return "cleanup-readback-drift"
        return None


def _remote_source(contract: RehearsalSystemdActivation, *, mode: str) -> str:
    if mode not in {"execute", "cleanup"}:
        raise ValueError("GB10 rehearsal remote mode is invalid")
    names = (
        "LoadState",
        "ActiveState",
        "SubState",
        "Type",
        "Result",
        "ExecMainStatus",
        "NeedDaemonReload",
        "Transient",
        "Description",
    )
    start_argv = list(contract.start_argv)
    expected_properties = contract.expected_properties
    return f"""import json
import os
import pathlib
import subprocess
import time

unit = {contract.unit!r}
description = {contract.description!r}
mode = {mode!r}
names = {names!r}
expected_properties = {expected_properties!r}
environment = {{
    "DBUS_SESSION_BUS_ADDRESS": f"unix:path=/run/user/{{os.getuid()}}/bus",
    "HOME": str(pathlib.Path.home()),
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "PATH": "/usr/local/bin:/usr/bin:/bin",
    "XDG_RUNTIME_DIR": f"/run/user/{{os.getuid()}}",
}}

def run(argv, timeout=15):
    return subprocess.run(argv, capture_output=True, check=False, text=True,
                          timeout=timeout, env=environment)

def show():
    result = run(["systemctl", "--user", "show", unit,
                  *[f"--property={{name}}" for name in names]])
    if result.returncode not in (0, 4):
        return None
    parsed = {{}}
    for line in result.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            parsed[key] = value
    return parsed or None

def load_state():
    result = run(["systemctl", "--user", "show", unit,
                  "--property=LoadState", "--value"])
    return result.stdout.strip() if result.returncode in (0, 4) else "unavailable"

boot_id = "unavailable"
try:
    boot_id = pathlib.Path("/proc/sys/kernel/random/boot_id").read_text(
        encoding="ascii"
    ).strip()
except OSError:
    pass
properties = {{}}
reason = ""
latency_ms = 0
cleanup_verified = False
try:
    initial_state = load_state()
    if initial_state == "unavailable":
        reason = "initial-readback-unavailable"
    elif initial_state != "not-found":
        existing = show()
        if existing != expected_properties:
            reason = "existing-unit-drift"
        elif mode == "execute":
            reason = "existing-unit-present"
    if not reason and mode == "execute":
        started = time.monotonic()
        result = run({start_argv!r}, 30)
        latency_ms = max(0, round((time.monotonic() - started) * 1000))
        if result.returncode != 0:
            reason = "activation-failed"
        else:
            properties = show() or {{}}
finally:
    observed_state = load_state()
    if observed_state == "unavailable":
        reason = reason or "cleanup-readback-unavailable"
    elif observed_state != "not-found":
        observed = show()
        if observed != expected_properties:
            reason = reason or "cleanup-identity-drift"
        else:
            if mode == "cleanup":
                properties = observed
            stopped = run(["systemctl", "--user", "stop", unit], 30)
            reset = run(["systemctl", "--user", "reset-failed", unit], 30)
            if stopped.returncode != 0 or reset.returncode != 0:
                reason = reason or "cleanup-command-failed"
    cleanup_verified = load_state() == "not-found"
    if not cleanup_verified:
        reason = reason or "cleanup-not-verified"

print(json.dumps({{
    "boot_id": boot_id,
    "cleanup_verified": cleanup_verified,
    "latency_ms": latency_ms,
    "mode": mode,
    "properties": properties,
    "reason": reason,
    "unit": unit,
}}, sort_keys=True, separators=(",", ":")))
"""


def _hash_json(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = [
    "FixedGB10RehearsalTransport",
    "GB10RehearsalAuthority",
    "GB10RehearsalEvidence",
]
