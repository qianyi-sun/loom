"""Safe, named preflight checks for the protected staging rollout broker."""

from __future__ import annotations

import grp
import hashlib
import importlib
import io
import json
import os
import pwd
import re
import shlex
import shutil
import stat
import subprocess
import tomllib
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from dotenv import dotenv_values

from loom_cli.rollout import gb10_readiness as gb10_readiness_module
from loom_cli.rollout.credential_authority import (
    read_trusted_file,
    safe_content_fingerprint,
)
from loom_cli.rollout.docker_readiness import probe_docker_runtime
from loom_cli.rollout.gb10_readiness import (
    GB10ProbeTarget,
    GB10SharedMountReadiness,
)
from loom_cli.rollout.install_attestation import verify_runner_install
from loom_cli.rollout.kubernetes_readiness import probe_kubernetes_client
from loom_cli.rollout.runtime_readiness import ModuleImporter, probe_runtime_readiness

from .candidate import CandidateBindingError, bind_configured_candidate
from .config import OperatorConfig
from .policy import sanitized_child_environment
from .redaction import redact_rollout_text

_CHECK_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,79}$")
_MAX_REMEDIATION_LENGTH = 240
_MAX_EVIDENCE_LENGTH = 120
_REQUIRED_ROLLOUT_SUBDIRECTORIES = (
    "rollouts",
    "postgres",
    "minio",
    "backups",
    "environment-state",
)
FULL_GB10_HOSTS = gb10_readiness_module.FULL_GB10_HOSTS
TEMPORARILY_EXCLUDED_GB10_HOSTS = gb10_readiness_module.TEMPORARILY_EXCLUDED_GB10_HOSTS
ACTIVE_GB10_HOSTS = gb10_readiness_module.ACTIVE_GB10_HOSTS
EXPECTED_GB10_SSH_CONFIG_SHA256 = "7ac3cbe20670762590b9efe4daea46126caa823f192e060be109b96350e82b4e"
_SHARED_REPOSITORY_ROOT = Path("/shared_work2/qianyi/.loom-staging-rollout/worker-repos")
_SHARED_REPOSITORY_SOURCE = "192.168.20.12:/shared_work2"
_MOUNTINFO = Path("/proc/self/mountinfo")
_GB10_KNOWN_HOSTS = Path("/etc/loom/staging-rollout-gb10-known-hosts")


def _render_remote_shared_repository_probe(
    *,
    shared_root: Path = Path("/shared_work2"),
    mountinfo: Path = Path("/proc/self/mountinfo"),
) -> str:
    parent = shared_root / "qianyi"
    authority = parent / ".loom-staging-rollout"
    repository = authority / "worker-repos"
    return f"""
import os
import stat

paths = (
    {str(parent)!r},
    {str(authority)!r},
    {str(repository)!r},
)
entries = [os.lstat(path) for path in paths]
safe = all(stat.S_ISDIR(item.st_mode) and not stat.S_ISLNK(item.st_mode) for item in entries)
safe = safe and os.access(paths[-1], os.R_OK | os.X_OK) and not os.access(paths[-1], os.W_OK)
mounts = []
target = {str(shared_root)!r}
with open({str(mountinfo)!r}, encoding="utf-8") as stream:
    for line in stream:
        left, separator, right = line.partition(" - ")
        if not separator:
            continue
        left_fields = left.split()
        right_fields = right.split()
        if len(left_fields) >= 6 and len(right_fields) == 3:
            mount_point = left_fields[4]
            if not mount_point.startswith("/") or "\\\\" in mount_point:
                continue
            contains_target = mount_point == "/" or target == mount_point or target.startswith(
                mount_point.rstrip("/") + "/"
            )
            if not contains_target:
                continue
            device = left_fields[2].split(":", 1)
            if len(device) == 2:
                mounts.append(
                    (
                        len(mount_point),
                        right_fields[0],
                        right_fields[1],
                        int(device[0]),
                        int(device[1]),
                    )
                )
mount = None
if mounts:
    specificity = max(item[0] for item in mounts)
    selected = [item for item in mounts if item[0] == specificity]
    if len(selected) == 1:
        mount = selected[0][1:]
mount_stat = os.lstat(target)
safe = safe and mount is not None and (os.major(mount_stat.st_dev), os.minor(mount_stat.st_dev)) == mount[2:]
groups = set(os.getgroups())
groups.add(os.getegid())
fields = [str(os.getuid()), ",".join(str(value) for value in sorted(groups))]
for item in entries:
    fields.extend(
        (
            str(item.st_uid),
            str(item.st_gid),
            format(stat.S_IMODE(item.st_mode), "o"),
            str(item.st_dev),
            str(item.st_ino),
        )
    )
if mount is not None:
    fields.extend((mount[0], mount[1], str(mount[2]), str(mount[3])))
print(";".join(fields))
raise SystemExit(0 if safe else 1)
""".strip()


_REMOTE_SHARED_REPOSITORY_PROBE = _render_remote_shared_repository_probe()


class CommandResult(Protocol):
    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> str: ...

    @property
    def stderr(self) -> str: ...


CommandRunner = Callable[[Sequence[str]], CommandResult]


@dataclass(frozen=True, slots=True)
class PreflightCheck:
    """One allowlisted check outcome with no captured diagnostic values."""

    name: str
    passed: bool
    remediation: str | None
    evidence: str | None = None

    def __post_init__(self) -> None:
        if _CHECK_NAME_RE.fullmatch(self.name) is None:
            raise ValueError("preflight check name is invalid")
        if type(self.passed) is not bool:
            raise ValueError("preflight check passed must be a boolean")
        if self.remediation is None:
            pass
        elif (
            not self.remediation.strip()
            or len(self.remediation) > _MAX_REMEDIATION_LENGTH
            or any(ord(char) < 32 for char in self.remediation)
            or redact_rollout_text(self.remediation) != self.remediation
        ):
            raise ValueError("preflight remediation must be short and safe")
        if self.evidence is not None and (
            not self.evidence.strip()
            or len(self.evidence) > _MAX_EVIDENCE_LENGTH
            or any(ord(char) < 32 for char in self.evidence)
            or redact_rollout_text(self.evidence) != self.evidence
        ):
            raise ValueError("preflight evidence must be short and safe")

    def to_dict(self) -> dict[str, object]:
        rendered: dict[str, object] = {
            "name": self.name,
            "passed": self.passed,
            "remediation": self.remediation,
        }
        if self.evidence is not None:
            rendered["evidence"] = self.evidence
        return rendered


@dataclass(frozen=True, slots=True)
class PreflightReport:
    """A bounded report containing only named pass/fail checks."""

    checks: tuple[PreflightCheck, ...]

    def __post_init__(self) -> None:
        if not self.checks or len(self.checks) > 64:
            raise ValueError("preflight report must contain between 1 and 64 checks")
        if len({check.name for check in self.checks}) != len(self.checks):
            raise ValueError("preflight report contains duplicate check names")

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    def to_dict(self) -> dict[str, object]:
        return {
            "checks": [check.to_dict() for check in self.checks],
            "passed": self.passed,
        }


def safe_fingerprint(payload: bytes) -> str:
    """Render the only credential detail permitted in preflight output."""
    return safe_content_fingerprint(payload)


def _default_run(
    argv: Sequence[str],
    *,
    environment: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(argv),
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
        env=environment,
    )


def _command_passes(run: CommandRunner, argv: Sequence[str]) -> bool:
    try:
        result = run(argv)
    except Exception:
        return False
    return result.returncode == 0


def _command_stdout(run: CommandRunner, argv: Sequence[str]) -> str | None:
    try:
        result = run(argv)
    except Exception:
        return None
    if result.returncode != 0 or not isinstance(result.stdout, str):
        return None
    return result.stdout


def _trusted_file_bytes(
    path: Path,
    *,
    service_uid: int,
    private: bool,
    allow_qianyi_owner: bool = False,
) -> bytes | None:
    try:
        return read_trusted_file(
            path,
            service_uid=service_uid,
            private=private,
            allow_qianyi_owner=allow_qianyi_owner,
        ).payload
    except ValueError:
        return None


def _private_file_bytes(
    path: Path,
    *,
    service_uid: int,
    allow_qianyi_owner: bool = False,
) -> bytes | None:
    return _trusted_file_bytes(
        path,
        service_uid=service_uid,
        private=True,
        allow_qianyi_owner=allow_qianyi_owner,
    )


def _readable_config_bytes(path: Path, *, service_uid: int) -> bytes | None:
    return _trusted_file_bytes(path, service_uid=service_uid, private=False)


def _source_path(source: str) -> Path | None:
    if not source.startswith("file:"):
        return None
    path = Path(source.removeprefix("file:"))
    return path if path.is_absolute() and ".." not in path.parts else None


def _directory_has_access(path: Path, modes: tuple[int, ...]) -> bool:
    try:
        metadata = path.lstat()
    except OSError:
        return False
    return bool(
        stat.S_ISDIR(metadata.st_mode)
        and not stat.S_ISLNK(metadata.st_mode)
        and all(os.access(path, mode) for mode in modes)
    )


def _checkout_is_trusted(
    config: OperatorConfig,
    *,
    service_uid: int,
    run: CommandRunner,
) -> bool:
    """Reuse fixed-size install metadata and the shared exact candidate proof.

    The root installer recursively validates and hardens the candidate once.
    Runtime Tier 0 therefore re-hashes the fixed install-asset allowlist and
    delegates checkout identity to ``bind_configured_candidate``.  That shared
    predicate checks fixed root metadata plus detached SHA/tree/remote and, for
    a sealed cumulative source, the configured base and bounded linear history.
    It intentionally performs neither a checkout walk nor a Git status scan.
    """
    if service_uid < 0:
        return False
    try:
        installed = verify_runner_install(service_uid=service_uid)
        if not installed.ready:
            return False
        binding = bind_configured_candidate(
            config,
            run=lambda argv: run(argv),
            now=lambda: datetime.now(UTC),
        )
    except (CandidateBindingError, OSError, RuntimeError, ValueError):
        return False

    statement = installed.attestation
    expected_tree = binding.resolved_tree or "none"
    expected_base = binding.approved_base_sha or "none"
    return bool(
        statement.source_mode == config.source_mode == binding.source_mode
        and statement.source_sha == binding.resolved_sha
        and statement.source_tree_sha == expected_tree
        and statement.source_base_sha == expected_base
    )


def _load_toml(path: Path, *, service_uid: int) -> dict[str, object] | None:
    payload = _readable_config_bytes(path, service_uid=service_uid)
    if payload is None:
        return None
    try:
        loaded = tomllib.loads(payload.decode("utf-8"))
    except (UnicodeError, tomllib.TOMLDecodeError):
        return None
    return loaded if isinstance(loaded, dict) else None


def _catalog_environment_path(config: OperatorConfig, *, service_uid: int) -> Path | None:
    cluster = _load_toml(config.cluster_config_path, service_uid=service_uid)
    if cluster is None:
        return None
    profile_value = cluster.get("env_state_profile")
    if not isinstance(profile_value, str) or not profile_value.strip():
        return None
    profile_path = Path(profile_value)
    if not profile_path.is_absolute():
        profile_path = config.cluster_config_path.parent / profile_path
    profile_path = Path(os.path.normpath(profile_path))
    profile = _load_toml(profile_path, service_uid=service_uid)
    if profile is None:
        return None
    catalog = profile.get("catalog_provisioning")
    if not isinstance(catalog, dict):
        return None
    value = catalog.get("env_file")
    if not isinstance(value, str):
        return None
    path = Path(value)
    return Path(os.path.normpath(path)) if path.is_absolute() else None


def _gb10_inputs(
    config: OperatorConfig,
    *,
    service_uid: int,
) -> tuple[Path, Path, tuple[str, ...]] | None:
    cluster = _load_toml(config.cluster_config_path, service_uid=service_uid)
    if cluster is None:
        return None
    pool = cluster.get("gb10_pool")
    if not isinstance(pool, dict):
        return None
    ssh_config_value = pool.get("ssh_config")
    identity_value = pool.get("ssh_identity_file")
    host_values = pool.get("hosts")
    if not isinstance(ssh_config_value, str) or not isinstance(identity_value, str):
        return None
    if not isinstance(host_values, list):
        return None
    hosts: list[str] = []
    for item in host_values:
        if not isinstance(item, dict) or not isinstance(item.get("ssh_target"), str):
            return None
        hosts.append(item["ssh_target"])
    # The checked-in SSH/trust topology remains the fixed 15-host authority.
    # Cluster hosts are the separately declared rollout target and must match
    # the merged #822 active set exactly; runtime skips and reordering fail
    # closed here before any SSH probe.
    if tuple(hosts) != ACTIVE_GB10_HOSTS:
        return None
    ssh_config = Path(ssh_config_value)
    if not ssh_config.is_absolute():
        ssh_config = config.cluster_config_path.parent / ssh_config
    identity = Path(identity_value)
    if not identity.is_absolute() or ".." in identity.parts:
        return None
    return Path(os.path.normpath(ssh_config)), identity, ACTIVE_GB10_HOSTS


@dataclass(frozen=True, slots=True)
class GB10PreflightInputs:
    """Exact checked-in GB10 topology consumed by legacy and DAG preflight."""

    ssh_config: Path
    identity: Path
    targets: tuple[GB10ProbeTarget, ...]


def load_catalog_environment_path(
    config: OperatorConfig,
    *,
    service_uid: int,
) -> Path | None:
    """Return the protected catalog environment path through the canonical parser."""
    return _catalog_environment_path(config, service_uid=service_uid)


def load_gb10_preflight_inputs(
    config: OperatorConfig,
    *,
    service_uid: int,
) -> GB10PreflightInputs | None:
    """Return one typed GB10 authority without duplicating cluster parsing rules."""
    legacy = _gb10_inputs(config, service_uid=service_uid)
    cluster = _load_toml(config.cluster_config_path, service_uid=service_uid)
    if legacy is None or cluster is None:
        return None
    pool = cluster.get("gb10_pool")
    hosts = pool.get("hosts") if isinstance(pool, dict) else None
    if not isinstance(hosts, list):
        return None
    targets: list[GB10ProbeTarget] = []
    try:
        for item in hosts:
            if not isinstance(item, dict):
                return None
            target = item.get("ssh_target")
            service = item.get("node_agent_service", "loom-gb10-node-agent.service")
            if not isinstance(target, str) or not isinstance(service, str):
                return None
            targets.append(GB10ProbeTarget(target, service))
    except ValueError:
        return None
    ssh_config, identity, expected_hosts = legacy
    if tuple(target.ssh_target for target in targets) != expected_hosts:
        return None
    return GB10PreflightInputs(
        ssh_config=ssh_config,
        identity=identity,
        targets=tuple(targets),
    )


def _shared_repository_binding(
    *,
    service_uid: int,
    root: Path = _SHARED_REPOSITORY_ROOT,
    mountinfo: Path = _MOUNTINFO,
) -> dict[str, int] | None:
    try:
        service = pwd.getpwnam("loom-rollout")
        service_group = grp.getgrnam("loom-rollout")
        consumer = pwd.getpwnam("qianyi")
        shared_group = grp.getgrnam("sharedwork")
        service_groups = set(os.getgrouplist(service.pw_name, service.pw_gid))
        consumer_groups = set(os.getgrouplist(consumer.pw_name, consumer.pw_gid))
    except (KeyError, OSError):
        return None
    if (
        service_uid < 0
        or service.pw_uid != service_uid
        or service.pw_gid != service_group.gr_gid
        or shared_group.gr_gid in service_groups
        or shared_group.gr_gid not in consumer_groups
        or consumer.pw_uid <= 0
        or shared_group.gr_gid <= 0
        or not root.is_absolute()
    ):
        return None

    mount_point = root.parents[2]
    try:
        mount_payload = mountinfo.read_text(encoding="utf-8")
        mount_metadata = os.lstat(mount_point)
    except OSError:
        return None
    mount_matches: list[tuple[int, int]] = []
    for raw_line in mount_payload.splitlines():
        left, separator, right = raw_line.partition(" - ")
        left_fields = left.split()
        right_fields = right.split()
        if not separator or len(left_fields) < 6 or len(right_fields) != 3:
            continue
        if left_fields[4] != str(mount_point):
            continue
        device = left_fields[2].split(":", 1)
        try:
            major, minor = int(device[0]), int(device[1])
        except (IndexError, ValueError):
            return None
        mount_options = set(left_fields[5].split(","))
        super_options = set(right_fields[2].split(","))
        if (
            right_fields[0] != "nfs4"
            or right_fields[1] != _SHARED_REPOSITORY_SOURCE
            or not {"rw", "nosuid", "nodev", "noexec"}.issubset(mount_options)
            or not {
                "rw",
                "hard",
                "vers=4.2",
                "proto=tcp",
                "sec=sys",
                "timeo=600",
                "retrans=2",
            }.issubset(super_options)
        ):
            return None
        mount_matches.append((major, minor))
    if (
        len(mount_matches) != 1
        or not stat.S_ISDIR(mount_metadata.st_mode)
        or stat.S_ISLNK(mount_metadata.st_mode)
        or mount_matches[0] != (os.major(mount_metadata.st_dev), os.minor(mount_metadata.st_dev))
    ):
        return None

    directory_flags = (
        getattr(os, "O_PATH", os.O_RDONLY)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptors: list[int] = []
    try:
        current_fd = os.open("/", directory_flags)
        descriptors.append(current_fd)
        selected: dict[Path, int] = {}
        current_path = Path("/")
        for component in root.parts[1:]:
            next_fd = os.open(component, directory_flags, dir_fd=current_fd)
            descriptors.append(next_fd)
            current_fd = next_fd
            current_path /= component
            if current_path in {root.parent.parent, root.parent, root}:
                selected[current_path] = current_fd
        if set(selected) != {root.parent.parent, root.parent, root}:
            return None

        parent = os.fstat(selected[root.parent.parent])
        authority = os.fstat(selected[root.parent])
        repository = os.fstat(selected[root])
        lexical_authority = os.stat(
            root.parent.name,
            dir_fd=selected[root.parent.parent],
            follow_symlinks=False,
        )
        lexical_repository = os.stat(
            root.name,
            dir_fd=selected[root.parent],
            follow_symlinks=False,
        )
        if (
            not all(stat.S_ISDIR(item.st_mode) for item in (parent, authority, repository))
            or (parent.st_uid, parent.st_gid, stat.S_IMODE(parent.st_mode))
            != (consumer.pw_uid, shared_group.gr_gid, 0o2775)
            or (authority.st_uid, authority.st_gid, stat.S_IMODE(authority.st_mode))
            != (service_uid, shared_group.gr_gid, 0o2750)
            or (repository.st_uid, repository.st_gid, stat.S_IMODE(repository.st_mode))
            != (service_uid, shared_group.gr_gid, 0o2750)
            or (lexical_authority.st_dev, lexical_authority.st_ino)
            != (authority.st_dev, authority.st_ino)
            or (lexical_repository.st_dev, lexical_repository.st_ino)
            != (repository.st_dev, repository.st_ino)
            or not os.access(
                ".",
                os.W_OK | os.X_OK,
                dir_fd=selected[root],
                effective_ids=True,
            )
            or os.access(
                ".",
                os.W_OK,
                dir_fd=selected[root.parent.parent],
                effective_ids=True,
            )
        ):
            return None
    except OSError:
        return None
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
    return {
        "service_uid": service_uid,
        "service_primary_gid": service_group.gr_gid,
        "consumer_uid": consumer.pw_uid,
        "consumer_primary_gid": consumer.pw_gid,
        "shared_gid": shared_group.gr_gid,
        "parent_device": parent.st_dev,
        "parent_inode": parent.st_ino,
        "authority_device": authority.st_dev,
        "authority_inode": authority.st_ino,
        "repository_device": repository.st_dev,
        "repository_inode": repository.st_ino,
    }


def load_shared_repository_binding(*, service_uid: int) -> dict[str, int] | None:
    """Return the held-descriptor mount authority used by every GB10 predicate."""
    return _shared_repository_binding(service_uid=service_uid)


def shared_repository_binding_digest(binding: dict[str, int]) -> str:
    """Hash the complete non-secret local mount/UID/GID binding."""
    expected = {
        "authority_device",
        "authority_inode",
        "consumer_primary_gid",
        "consumer_uid",
        "parent_device",
        "parent_inode",
        "repository_device",
        "repository_inode",
        "service_primary_gid",
        "service_uid",
        "shared_gid",
    }
    if set(binding) != expected or any(
        type(value) is not int or value < 0 for value in binding.values()
    ):
        raise ValueError("shared repository binding is invalid")
    return hashlib.sha256(
        json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _validate_gb10_shared_mount_output(
    output: str,
    *,
    host: str,
    binding: dict[str, int],
) -> str | None:
    fields = output.strip().split(";")
    if len(fields) != 21 or any(not field for field in fields):
        return None
    try:
        remote_uid = int(fields[0])
        remote_groups = {int(value) for value in fields[1].split(",")}
        values: list[int] = []
        for offset in range(2, 17, 5):
            values.extend(
                (
                    int(fields[offset]),
                    int(fields[offset + 1]),
                    int(fields[offset + 2], 8),
                    int(fields[offset + 3]),
                    int(fields[offset + 4]),
                )
            )
        mount_type = fields[17]
        mount_source = fields[18]
        mount_device = (int(fields[19]), int(fields[20]))
        parent = values[0:5]
        authority = values[5:10]
        repository = values[10:15]
        repository_device = (os.major(repository[3]), os.minor(repository[3]))
    except (OverflowError, ValueError):
        return None
    if (
        remote_uid != binding["consumer_uid"]
        or binding["shared_gid"] not in remote_groups
        or parent[:3] != [binding["consumer_uid"], binding["shared_gid"], int("2775", 8)]
        or authority[:3] != [binding["service_uid"], binding["shared_gid"], int("2750", 8)]
        or repository[:3] != [binding["service_uid"], binding["shared_gid"], int("2750", 8)]
        or any(value <= 0 for value in (*parent[3:], *authority[3:], *repository[3:]))
        or mount_device != repository_device
        or (
            host == "trt-gb10-2"
            and (mount_type != "ext4" or mount_source == _SHARED_REPOSITORY_SOURCE)
        )
        or (
            host != "trt-gb10-2"
            and (mount_type != "nfs4" or mount_source != _SHARED_REPOSITORY_SOURCE)
        )
    ):
        return None
    return hashlib.sha256(output.strip().encode("ascii")).hexdigest()


def probe_gb10_shared_mount_readonly(
    run: CommandRunner,
    *,
    ssh_config: Path,
    identity: Path,
    hosts: tuple[str, ...],
    binding: dict[str, int],
    max_concurrency: int = 8,
) -> GB10SharedMountReadiness:
    """Collect exact mount, directory, UID/GID, and checkout-root evidence."""
    if hosts != ACTIVE_GB10_HOSTS or not 1 <= max_concurrency <= 16:
        raise ValueError("GB10 shared mount inventory is invalid")
    remote_command = f"python3 -c {shlex.quote(_REMOTE_SHARED_REPOSITORY_PROBE)}"

    def probe(host: str) -> str | None:
        output = _command_stdout(
            run,
            [
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
                f"UserKnownHostsFile={_GB10_KNOWN_HOSTS}",
                "-o",
                "GlobalKnownHostsFile=/dev/null",
                "-o",
                "UpdateHostKeys=no",
                host,
                remote_command,
            ],
        )
        return (
            None
            if output is None
            else _validate_gb10_shared_mount_output(output, host=host, binding=binding)
        )

    outcomes: dict[str, str | None] = {}
    with ThreadPoolExecutor(
        max_workers=min(max_concurrency, len(hosts)),
        thread_name_prefix="loom-gb10-shared-mount",
    ) as executor:
        futures = {executor.submit(probe, host): host for host in hosts}
        for future in as_completed(futures):
            host = futures[future]
            try:
                outcomes[host] = future.result()
            except Exception:
                outcomes[host] = None
    return GB10SharedMountReadiness(
        host_digests={host: digest for host, digest in outcomes.items() if digest is not None},
        failed_hosts=tuple(sorted(host for host, digest in outcomes.items() if digest is None)),
    )


def _gb10_shared_repository_probe(
    run: CommandRunner,
    *,
    ssh_config: Path,
    identity: Path,
    hosts: tuple[str, ...],
    binding: dict[str, int],
) -> str | None:
    try:
        evidence = probe_gb10_shared_mount_readonly(
            run,
            ssh_config=ssh_config,
            identity=identity,
            hosts=hosts,
            binding=binding,
        )
    except ValueError:
        return None
    return (
        f"sha256:{evidence.evidence_digest[:12]} hosts={len(evidence.host_digests)}"
        if evidence.ready
        else None
    )


def catalog_secret_values(
    config: OperatorConfig,
    *,
    service_uid: int,
) -> tuple[str, ...]:
    """Read individual catalog dotenv values through the protected no-follow path."""
    path = _catalog_environment_path(config, service_uid=service_uid)
    payload = (
        None
        if path is None
        else _private_file_bytes(
            path,
            service_uid=service_uid,
            allow_qianyi_owner=True,
        )
    )
    if payload is None:
        return ()
    try:
        decoded = payload.decode("utf-8")
        loaded = dotenv_values(stream=io.StringIO(decoded), interpolate=False)
    except (UnicodeError, ValueError):
        return ()
    return tuple(value for value in loaded.values() if isinstance(value, str) and value)


def collect_preflight(
    config: OperatorConfig,
    *,
    service_uid: int | None = None,
    run: CommandRunner | None = None,
    which: Callable[[str], str | None] | None = None,
    importer: ModuleImporter = importlib.import_module,
) -> PreflightReport:
    """Collect every fixed preflight without exposing subprocess diagnostics."""
    if service_uid is None:
        try:
            service_uid = pwd.getpwnam(config.service_user).pw_uid
        except (KeyError, OSError):
            service_uid = -1
    environment = sanitized_child_environment(config, service_uid=service_uid)
    child_run: CommandRunner
    if run is None:

        def child_run(argv: Sequence[str]) -> CommandResult:
            return _default_run(argv, environment=environment)

    else:
        child_run = run

    checks: list[PreflightCheck] = []

    def add(name: str, passed: bool, remediation: str) -> None:
        checks.append(PreflightCheck(name, bool(passed), None if passed else remediation))

    trusted_checkout = _checkout_is_trusted(
        config,
        service_uid=service_uid,
        run=child_run,
    )
    add(
        "checkout",
        trusted_checkout,
        "restore the exact protected runner candidate and install attestation",
    )

    executable_lookup = which or (lambda name: shutil.which(name, path=environment["PATH"]))
    runtime = probe_runtime_readiness(
        executable_lookup=executable_lookup,
        importer=importer,
    )
    add(
        "executables",
        runtime.executables_ready,
        "install the fixed rollout executable set",
    )
    add(
        "python-imports",
        runtime.imports_ready,
        "synchronize the locked rollout Python environment",
    )

    docker = probe_docker_runtime(child_run) if trusted_checkout else None
    add(
        "docker",
        docker is not None and docker.daemon_ready,
        "restore service Docker access",
    )
    add(
        "docker-buildx",
        docker is not None and docker.buildx_ready,
        "install and verify the Docker buildx plugin",
    )
    add(
        "docker-inotify",
        docker is not None and docker.inotify_capacity_ready,
        "restore the managed host inotify instance headroom",
    )

    kubernetes = (
        probe_kubernetes_client(
            child_run,
            kubeconfig=config.kubeconfig_path,
            cluster_name=config.cluster_name,
            namespace=config.namespace,
        )
        if trusted_checkout
        else None
    )
    add(
        "kube-context",
        kubernetes is not None and kubernetes.context_ready,
        "restore the fixed staging kube context",
    )
    add(
        "kube-namespace",
        kubernetes is not None and kubernetes.namespace_ready,
        "restore access to the staging namespace",
    )

    data_ok = _directory_has_access(config.rollout_root, (os.R_OK, os.X_OK)) and all(
        _directory_has_access(
            config.rollout_root / name,
            (os.R_OK, os.W_OK, os.X_OK),
        )
        for name in _REQUIRED_ROLLOUT_SUBDIRECTORIES
    )
    add(
        "data-root",
        data_ok,
        "restore root read traverse and declared staging subdirectory write access",
    )

    credentials_ok = service_uid >= 0
    admin_fingerprint_ok = False
    for source in (
        config.admin_token_source,
        config.worker_token_source,
        config.service_token_source,
    ):
        path = _source_path(source)
        payload = (
            None
            if path is None
            else _private_file_bytes(
                path,
                service_uid=service_uid,
                allow_qianyi_owner=True,
            )
        )
        credentials_ok = credentials_ok and payload is not None and bool(payload.strip())
        if source == config.admin_token_source and payload is not None:
            admin_fingerprint_ok = safe_fingerprint(payload.strip()) == (
                config.expect_admin_token_fingerprint
            )
    add("credentials", credentials_ok, "restore private service credential readability")
    add(
        "admin-fingerprint",
        admin_fingerprint_ok,
        "install the configured staging admin credential generation",
    )

    catalog_path = (
        _catalog_environment_path(config, service_uid=service_uid) if service_uid >= 0 else None
    )
    catalog_ok = catalog_path is not None and (
        _private_file_bytes(
            catalog_path,
            service_uid=service_uid,
            allow_qianyi_owner=True,
        )
        is not None
    )
    add("catalog-environment", catalog_ok, "restore private catalog environment readability")

    backup_ok = (
        trusted_checkout
        and _command_passes(
            child_run,
            [
                "kubectl",
                "--kubeconfig",
                str(config.kubeconfig_path),
                "-n",
                config.namespace,
                "get",
                "statefulset/loom-postgres",
                "--request-timeout=10s",
            ],
        )
        and _command_passes(
            child_run,
            [
                "kubectl",
                "--kubeconfig",
                str(config.kubeconfig_path),
                "-n",
                config.namespace,
                "get",
                "service/loom-minio",
                "--request-timeout=10s",
            ],
        )
    )
    add("backup-commands", backup_ok, "restore staging backup command readiness")

    gb10 = _gb10_inputs(config, service_uid=service_uid) if service_uid >= 0 else None
    gb10_topology_ok = False
    gb10_ok = trusted_checkout and gb10 is not None
    if gb10 is not None and trusted_checkout:
        ssh_config, identity, hosts = gb10
        ssh_config_payload = _readable_config_bytes(ssh_config, service_uid=service_uid)
        gb10_topology_ok = bool(
            ssh_config_payload is not None
            and hashlib.sha256(ssh_config_payload).hexdigest() == EXPECTED_GB10_SSH_CONFIG_SHA256
        )
        gb10_ok = (
            gb10_topology_ok and _private_file_bytes(identity, service_uid=service_uid) is not None
        )
        if gb10_ok:
            for host in hosts:
                if not _command_passes(
                    child_run,
                    [
                        "ssh",
                        "-F",
                        str(ssh_config),
                        "-i",
                        str(identity),
                        "-o",
                        "BatchMode=yes",
                        "-o",
                        "ConnectTimeout=10",
                        host,
                        "true",
                    ],
                ):
                    gb10_ok = False
                    break
    add(
        "gb10-topology",
        gb10_topology_ok,
        "restore the exact merged full GB10 SSH topology",
    )
    add(
        "gb10-batch-mode",
        gb10_ok,
        "restore service SSH trust for every configured GB10 host",
    )
    shared_binding = (
        _shared_repository_binding(service_uid=service_uid) if service_uid >= 0 else None
    )
    shared_evidence = None
    if gb10_ok and gb10 is not None and shared_binding is not None:
        ssh_config, identity, hosts = gb10
        shared_evidence = _gb10_shared_repository_probe(
            child_run,
            ssh_config=ssh_config,
            identity=identity,
            hosts=hosts,
            binding=shared_binding,
        )
    shared_ready = shared_evidence is not None
    checks.append(
        PreflightCheck(
            "gb10-shared-repository",
            shared_ready,
            None
            if shared_ready
            else "restore the fixed shared GB10 repository identity and access contract",
            evidence=shared_evidence,
        )
    )
    return PreflightReport(tuple(checks))


__all__ = [
    "CommandResult",
    "CommandRunner",
    "GB10PreflightInputs",
    "PreflightCheck",
    "PreflightReport",
    "catalog_secret_values",
    "collect_preflight",
    "load_catalog_environment_path",
    "load_gb10_preflight_inputs",
    "load_shared_repository_binding",
    "probe_gb10_shared_mount_readonly",
    "safe_fingerprint",
    "shared_repository_binding_digest",
]
