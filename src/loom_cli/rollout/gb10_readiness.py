"""Bounded read-only GB10 fleet readiness probe for staged preflight."""

from __future__ import annotations

import hashlib
import json
import re
import shlex
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from loom_cli.rollout.systemd_readiness import (
    CommandResult,
    CommandRunner,
    GB10HostReadiness,
    parse_gb10_host_readiness,
)

_HOST_RE = re.compile(r"trt-gb10-(?:[1-9]|1[0-5])\Z")
_SERVICE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.@-]*[.]service\Z")
_KNOWN_HOSTS = Path("/etc/loom/staging-rollout-gb10-known-hosts")
_SHARED_WORKER_REPOSITORY_ROOT = Path("/shared_work2/qianyi/.loom-staging-rollout/worker-repos")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_TAG_RE = re.compile(r"^staging-[0-9a-f]{7}$")
_GB10_RELEASE_UNIT_PATHS = frozenset(
    {
        "deploy/worker-pools/gb10/loom-gb10-node-agent.service",
        "deploy/worker-pools/gb10/loom-gb10-node-agent.timer",
        "deploy/worker-pools/gb10/loom-gb10-worker.service",
    }
)
DEFAULT_SETTLE_ATTEMPTS = 16
DEFAULT_SETTLE_INTERVAL_SECONDS = 2.0
FULL_GB10_HOSTS = tuple(f"trt-gb10-{number}" for number in range(1, 16))
TEMPORARILY_EXCLUDED_GB10_HOSTS = frozenset({"trt-gb10-7"})
ACTIVE_GB10_HOSTS = tuple(
    host for host in FULL_GB10_HOSTS if host not in TEMPORARILY_EXCLUDED_GB10_HOSTS
)


@dataclass(frozen=True, slots=True)
class GB10ProbeTarget:
    ssh_target: str
    node_agent_service: str

    def __post_init__(self) -> None:
        if _HOST_RE.fullmatch(self.ssh_target) is None:
            raise ValueError("GB10 readiness target is outside the fixed inventory")
        if _SERVICE_RE.fullmatch(self.node_agent_service) is None:
            raise ValueError("GB10 readiness service is invalid")


@dataclass(frozen=True, slots=True)
class GB10FleetReadiness:
    host_boot_ids: Mapping[str, str]
    host_evidence_digests: Mapping[str, str]
    failed_hosts: tuple[str, ...]
    transient_hosts: tuple[str, ...]

    def __post_init__(self) -> None:
        boot_ids = dict(self.host_boot_ids)
        digests = dict(self.host_evidence_digests)
        if (
            set(boot_ids) != set(digests)
            or any(
                _HOST_RE.fullmatch(host) is None for host in set(boot_ids) | set(self.failed_hosts)
            )
            or len(set(self.failed_hosts)) != len(self.failed_hosts)
            or len(set(self.transient_hosts)) != len(self.transient_hosts)
            or not set(self.transient_hosts) <= set(boot_ids) | set(self.failed_hosts)
        ):
            raise ValueError("GB10 fleet evidence is inconsistent")
        object.__setattr__(self, "host_boot_ids", MappingProxyType(boot_ids))
        object.__setattr__(self, "host_evidence_digests", MappingProxyType(digests))

    @property
    def ready(self) -> bool:
        return bool(self.host_boot_ids) and not self.failed_hosts

    @property
    def inventory_digest(self) -> str:
        payload = json.dumps(
            {
                "boot_ids": dict(self.host_boot_ids),
                "evidence": dict(self.host_evidence_digests),
                "failed": self.failed_hosts,
                "transient": self.transient_hosts,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class GB10SshTopology:
    reachable_hosts: tuple[str, ...]
    failed_hosts: tuple[str, ...]

    def __post_init__(self) -> None:
        hosts = (*self.reachable_hosts, *self.failed_hosts)
        if (
            not hosts
            or len(set(hosts)) != len(hosts)
            or any(_HOST_RE.fullmatch(host) is None for host in hosts)
        ):
            raise ValueError("GB10 SSH topology evidence is inconsistent")

    @property
    def ready(self) -> bool:
        return bool(self.reachable_hosts) and not self.failed_hosts

    @property
    def evidence_digest(self) -> str:
        payload = {
            "failed": self.failed_hosts,
            "reachable": self.reachable_hosts,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class GB10SharedMountReadiness:
    host_digests: Mapping[str, str]
    failed_hosts: tuple[str, ...]

    def __post_init__(self) -> None:
        digests = dict(self.host_digests)
        hosts = (*digests, *self.failed_hosts)
        if (
            not hosts
            or len(set(hosts)) != len(hosts)
            or any(_HOST_RE.fullmatch(host) is None for host in hosts)
            or any(re.fullmatch(r"[0-9a-f]{64}", digest) is None for digest in digests.values())
        ):
            raise ValueError("GB10 shared mount evidence is inconsistent")
        object.__setattr__(self, "host_digests", MappingProxyType(digests))

    @property
    def ready(self) -> bool:
        return bool(self.host_digests) and not self.failed_hosts

    @property
    def evidence_digest(self) -> str:
        payload = {
            "failed": self.failed_hosts,
            "hosts": dict(self.host_digests),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class GB10CandidateSourceReadiness:
    """Exact immutable shared checkout evidence observed by every GB10 host."""

    host_digests: Mapping[str, str]
    failed_hosts: tuple[str, ...]
    candidate_sha: str
    candidate_tree: str
    unit_set_digest: str

    def __post_init__(self) -> None:
        digests = dict(self.host_digests)
        hosts = (*digests, *self.failed_hosts)
        if (
            not hosts
            or len(set(hosts)) != len(hosts)
            or any(_HOST_RE.fullmatch(host) is None for host in hosts)
            or any(_SHA256_RE.fullmatch(digest) is None for digest in digests.values())
            or _SHA_RE.fullmatch(self.candidate_sha) is None
            or _SHA_RE.fullmatch(self.candidate_tree) is None
            or _SHA256_RE.fullmatch(self.unit_set_digest) is None
        ):
            raise ValueError("GB10 candidate source evidence is inconsistent")
        object.__setattr__(self, "host_digests", MappingProxyType(digests))

    @property
    def ready(self) -> bool:
        return bool(self.host_digests) and not self.failed_hosts

    @property
    def evidence_digest(self) -> str:
        payload = {
            "candidate_sha": self.candidate_sha,
            "candidate_tree": self.candidate_tree,
            "failed": self.failed_hosts,
            "hosts": dict(self.host_digests),
            "unit_set_digest": self.unit_set_digest,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


def _candidate_source_remote_probe(
    *,
    candidate_sha: str,
    candidate_tree: str,
    image_tag: str,
    unit_sha256: Mapping[str, str],
) -> str:
    """Render one fixed read-only source probe with no caller-controlled paths."""
    units = dict(unit_sha256)
    if (
        _SHA_RE.fullmatch(candidate_sha) is None
        or _SHA_RE.fullmatch(candidate_tree) is None
        or _IMAGE_TAG_RE.fullmatch(image_tag) is None
        or set(units) != _GB10_RELEASE_UNIT_PATHS
        or any(_SHA256_RE.fullmatch(digest) is None for digest in units.values())
    ):
        raise ValueError("GB10 candidate source binding is invalid")
    repository = _SHARED_WORKER_REPOSITORY_ROOT / f"loom-remote-worker-{image_tag}"
    return f"""
import hashlib
import json
import os
import pathlib
import stat
import subprocess

repository = pathlib.Path({str(repository)!r})
candidate_sha = {candidate_sha!r}
candidate_tree = {candidate_tree!r}
units = {units!r}

def git(*args):
    result = subprocess.run(
        ["git", "-c", f"safe.directory={{repository}}", "-C", str(repository), *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
        env={{
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_TERMINAL_PROMPT": "0",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/bin:/bin",
        }},
    )
    if result.returncode != 0 or result.stderr or "\\x00" in result.stdout:
        raise SystemExit(1)
    return result.stdout.strip()

metadata = repository.lstat()
if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
    raise SystemExit(1)
if git("rev-parse", "HEAD") != candidate_sha:
    raise SystemExit(1)
if git("rev-parse", "HEAD^{{tree}}") != candidate_tree:
    raise SystemExit(1)
if git("status", "--porcelain=v1", "--untracked-files=all"):
    raise SystemExit(1)

observed = {{}}
for relative, expected in sorted(units.items()):
    path = repository / relative
    item = path.lstat()
    if (
        not stat.S_ISREG(item.st_mode)
        or stat.S_ISLNK(item.st_mode)
        or item.st_nlink != 1
        or stat.S_IMODE(item.st_mode) & 0o022
    ):
        raise SystemExit(1)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        first = os.fstat(descriptor)
        payload = os.read(descriptor, 131073)
        second = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    digest = hashlib.sha256(payload).hexdigest()
    if (
        len(payload) > 131072
        or first.st_size != len(payload)
        or (first.st_dev, first.st_ino, first.st_size, first.st_mtime_ns)
        != (second.st_dev, second.st_ino, second.st_size, second.st_mtime_ns)
        or digest != expected
    ):
        raise SystemExit(1)
    observed[relative] = digest

print(json.dumps({{
    "candidate_sha": candidate_sha,
    "candidate_tree": candidate_tree,
    "unit_sha256": observed,
}}, sort_keys=True, separators=(",", ":")))
""".strip()


def candidate_source_remote_command(
    *,
    candidate_sha: str,
    candidate_tree: str,
    image_tag: str,
    unit_sha256: Mapping[str, str],
) -> str:
    """Return the fixed read-only remote command for one exact candidate source."""
    return "python3 -c " + shlex.quote(
        _candidate_source_remote_probe(
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            image_tag=image_tag,
            unit_sha256=unit_sha256,
        )
    )


def probe_gb10_candidate_source_readonly(
    run: CommandRunner,
    targets: Sequence[GB10ProbeTarget],
    *,
    ssh_config: Path,
    identity: Path,
    candidate_sha: str,
    candidate_tree: str,
    image_tag: str,
    unit_sha256: Mapping[str, str],
    unit_set_digest: str,
    max_concurrency: int = 8,
    settle_attempts: int = 6,
    settle_interval_seconds: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
) -> GB10CandidateSourceReadiness:
    """Verify the same exact shared source on the complete fixed GB10 fleet."""
    if (
        not targets
        or len({target.ssh_target for target in targets}) != len(targets)
        or not 1 <= max_concurrency <= 16
        or not 1 <= settle_attempts <= 6
        or not 0 <= settle_interval_seconds <= 5
        or _SHA256_RE.fullmatch(unit_set_digest) is None
    ):
        raise ValueError("GB10 candidate source probe bounds are invalid")
    command = candidate_source_remote_command(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        image_tag=image_tag,
        unit_sha256=unit_sha256,
    )
    expected_payload = {
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "unit_sha256": dict(sorted(unit_sha256.items())),
    }

    def probe(target: GB10ProbeTarget) -> str | None:
        for attempt in range(settle_attempts):
            try:
                result = run(
                    (*_ssh_argv(target, ssh_config=ssh_config, identity=identity)[:-1], command)
                )
            except Exception:
                result = None
            if result is not None and result.returncode == 0:
                try:
                    payload = json.loads(result.stdout)
                except (TypeError, ValueError):
                    return None
                if payload != expected_payload:
                    return None
                return hashlib.sha256(
                    json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest()
            if attempt + 1 < settle_attempts:
                sleep(min(settle_interval_seconds * (2**attempt), 30.0))
        return None

    outcomes: dict[str, str | None] = {}
    with ThreadPoolExecutor(
        max_workers=min(max_concurrency, len(targets)),
        thread_name_prefix="loom-gb10-candidate-source",
    ) as executor:
        futures = {executor.submit(probe, target): target for target in targets}
        for future in as_completed(futures):
            target = futures[future]
            try:
                outcomes[target.ssh_target] = future.result()
            except Exception:
                outcomes[target.ssh_target] = None
    return GB10CandidateSourceReadiness(
        host_digests={host: digest for host, digest in outcomes.items() if digest is not None},
        failed_hosts=tuple(sorted(host for host, digest in outcomes.items() if digest is None)),
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        unit_set_digest=unit_set_digest,
    )


def _remote_probe_source(service: str) -> str:
    """Return a fixed Python probe that emits only an allowlisted JSON object."""
    if _SERVICE_RE.fullmatch(service) is None:
        raise ValueError("GB10 readiness service is invalid")
    timer = f"{service.removesuffix('.service')}.timer"
    return f"""
import json
import os
import pathlib
import subprocess

def run(argv):
    result = subprocess.run(argv, check=False, capture_output=True, text=True, timeout=5)
    if result.returncode != 0:
        raise SystemExit(1)
    value = result.stdout.strip()
    if not value or "\\n\\n" in value:
        raise SystemExit(1)
    return value

def properties(unit, names):
    output = run(["systemctl", "--user", "show", unit, *[f"--property={{name}}" for name in names]])
    parsed = {{}}
    for line in output.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            parsed[key] = value
    if set(parsed) != set(names):
        raise SystemExit(1)
    return parsed

service = {service!r}
timer = {timer!r}
payload = {{
    "schema_version": 1,
    "boot_id": pathlib.Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip(),
    "manager_version": run(["systemctl", "--user", "show", "--property=Version", "--value"]),
    "linger_enabled": run(["loginctl", "show-user", str(os.getuid()), "--property=Linger", "--value"]) == "yes",
    "service": properties(service, ["LoadState", "Type", "Result", "ExecMainStatus", "ActiveState", "SubState", "NeedDaemonReload"]),
    "timer": properties(timer, ["LoadState", "ActiveState", "SubState", "Unit", "NeedDaemonReload"]),
    "timer_enabled": run(["systemctl", "--user", "is-enabled", timer]) == "enabled",
}}
print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
""".strip()


def remote_probe_command(service: str) -> str:
    """Render the fixed remote invocation without interpolated shell syntax."""
    return f"python3 -c {shlex.quote(_remote_probe_source(service))}"


def _ssh_argv(
    target: GB10ProbeTarget,
    *,
    ssh_config: Path,
    identity: Path,
) -> tuple[str, ...]:
    return (
        "ssh",
        "-F",
        str(ssh_config),
        "-i",
        str(identity),
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
        target.ssh_target,
        remote_probe_command(target.node_agent_service),
    )


def _ssh_topology_argv(
    target: GB10ProbeTarget,
    *,
    ssh_config: Path,
    identity: Path,
) -> tuple[str, ...]:
    return (
        "ssh",
        "-F",
        str(ssh_config),
        "-i",
        str(identity),
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
        target.ssh_target,
        "true",
    )


def probe_gb10_ssh_topology(
    run: CommandRunner,
    targets: Sequence[GB10ProbeTarget],
    *,
    ssh_config: Path,
    identity: Path,
    max_concurrency: int = 8,
) -> GB10SshTopology:
    """Test fixed batch-mode trust concurrently without remote diagnostics."""
    if (
        not targets
        or len({target.ssh_target for target in targets}) != len(targets)
        or not 1 <= max_concurrency <= 16
    ):
        raise ValueError("GB10 SSH topology bounds are invalid")

    def probe(target: GB10ProbeTarget) -> bool:
        try:
            result = run(_ssh_topology_argv(target, ssh_config=ssh_config, identity=identity))
        except Exception:
            return False
        return result.returncode == 0 and result.stdout == ""

    outcomes: dict[str, bool] = {}
    with ThreadPoolExecutor(
        max_workers=min(max_concurrency, len(targets)),
        thread_name_prefix="loom-gb10-ssh-topology",
    ) as executor:
        futures = {executor.submit(probe, target): target for target in targets}
        for future in as_completed(futures):
            target = futures[future]
            try:
                outcomes[target.ssh_target] = future.result()
            except Exception:
                outcomes[target.ssh_target] = False
    return GB10SshTopology(
        reachable_hosts=tuple(sorted(host for host, ready in outcomes.items() if ready)),
        failed_hosts=tuple(sorted(host for host, ready in outcomes.items() if not ready)),
    )


def _probe_target(
    run: CommandRunner,
    target: GB10ProbeTarget,
    *,
    ssh_config: Path,
    identity: Path,
    settle_attempts: int,
    settle_interval_seconds: float,
    sleep: Callable[[float], None],
) -> tuple[GB10HostReadiness | None, bool]:
    observed_transient = False
    evidence: GB10HostReadiness | None = None
    for attempt in range(settle_attempts):
        # A transient SSH/transport failure (an overloaded single bastion dropping
        # the connection, a non-zero exit, or unparseable output) is retried within
        # the settle budget rather than instantly failing the host -- mirroring the
        # candidate-source probe. Only a fully parsed observation short-circuits:
        # ready -> success; a non-transient not-ready state -> genuine drift returned
        # immediately. After the budget is exhausted the host still fails closed.
        evidence = None
        try:
            result: CommandResult | None = run(
                _ssh_argv(target, ssh_config=ssh_config, identity=identity)
            )
        except Exception:
            result = None
        if result is not None and result.returncode == 0 and isinstance(result.stdout, str):
            evidence = parse_gb10_host_readiness(
                result.stdout,
                service=target.node_agent_service,
            )
            if evidence is not None:
                if evidence.ready:
                    return evidence, observed_transient
                if not evidence.transient_timer:
                    return evidence, observed_transient
                observed_transient = True
        if attempt + 1 < settle_attempts:
            sleep(settle_interval_seconds)
    return evidence, observed_transient


def probe_gb10_fleet_readonly(
    run: CommandRunner,
    targets: Sequence[GB10ProbeTarget],
    *,
    ssh_config: Path,
    identity: Path,
    max_concurrency: int = 8,
    settle_attempts: int = DEFAULT_SETTLE_ATTEMPTS,
    settle_interval_seconds: float = DEFAULT_SETTLE_INTERVAL_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> GB10FleetReadiness:
    """Probe every host concurrently and return one complete blocker report."""
    if (
        not targets
        or len({target.ssh_target for target in targets}) != len(targets)
        or not 1 <= max_concurrency <= 16
        or not 1 <= settle_attempts <= 32
        or not 0 <= settle_interval_seconds <= 10
    ):
        raise ValueError("GB10 readiness probe bounds are invalid")
    results: dict[str, GB10HostReadiness | None] = {}
    transient: set[str] = set()
    with ThreadPoolExecutor(
        max_workers=min(max_concurrency, len(targets)),
        thread_name_prefix="loom-gb10-readiness",
    ) as executor:
        futures = {
            executor.submit(
                _probe_target,
                run,
                target,
                ssh_config=ssh_config,
                identity=identity,
                settle_attempts=settle_attempts,
                settle_interval_seconds=settle_interval_seconds,
                sleep=sleep,
            ): target
            for target in targets
        }
        for future in as_completed(futures):
            target = futures[future]
            try:
                evidence, observed_transient = future.result()
            except Exception:
                evidence, observed_transient = None, False
            results[target.ssh_target] = evidence
            if observed_transient:
                transient.add(target.ssh_target)
    failed = tuple(
        sorted(host for host, evidence in results.items() if evidence is None or not evidence.ready)
    )
    boot_ids = {
        host: evidence.boot_id for host, evidence in sorted(results.items()) if evidence is not None
    }
    digests = {
        host: evidence.evidence_digest
        for host, evidence in sorted(results.items())
        if evidence is not None
    }
    return GB10FleetReadiness(
        host_boot_ids=boot_ids,
        host_evidence_digests=digests,
        failed_hosts=failed,
        transient_hosts=tuple(sorted(transient)),
    )


__all__ = [
    "ACTIVE_GB10_HOSTS",
    "DEFAULT_SETTLE_ATTEMPTS",
    "DEFAULT_SETTLE_INTERVAL_SECONDS",
    "FULL_GB10_HOSTS",
    "TEMPORARILY_EXCLUDED_GB10_HOSTS",
    "GB10CandidateSourceReadiness",
    "GB10FleetReadiness",
    "GB10ProbeTarget",
    "GB10SharedMountReadiness",
    "GB10SshTopology",
    "candidate_source_remote_command",
    "probe_gb10_candidate_source_readonly",
    "probe_gb10_fleet_readonly",
    "probe_gb10_ssh_topology",
    "remote_probe_command",
]
