"""Safe, named preflight checks for the protected staging rollout broker."""

from __future__ import annotations

import grp
import hashlib
import importlib
import io
import os
import pwd
import re
import shlex
import shutil
import stat
import subprocess
import tomllib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from dotenv import dotenv_values

from .config import OperatorConfig
from .policy import sanitized_child_environment
from .redaction import redact_rollout_text

_CHECK_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,79}$")
_MAX_REMEDIATION_LENGTH = 240
_MAX_EVIDENCE_LENGTH = 120
_REQUIRED_EXECUTABLES = (
    "git",
    "docker",
    "kind",
    "kubectl",
    "ssh",
    "systemd-run",
    "systemctl",
    "journalctl",
)
_REQUIRED_IMPORTS = (
    "boto3",
    "yaml",
    "loom_benchmark_tool.register_cmd",
    "loom_benchmarks.registry",
    "loom_benchmarks.adapters.skilllearnbench",
    "loom_benchmark_terminal_bench_2.adapter",
)
_REQUIRED_ROLLOUT_SUBDIRECTORIES = (
    "rollouts",
    "postgres",
    "minio",
    "backups",
    "environment-state",
)
FULL_GB10_HOSTS = tuple(f"trt-gb10-{number}" for number in range(1, 16))
TEMPORARILY_EXCLUDED_GB10_HOSTS = frozenset({"trt-gb10-7"})
ACTIVE_GB10_HOSTS = tuple(
    host for host in FULL_GB10_HOSTS if host not in TEMPORARILY_EXCLUDED_GB10_HOSTS
)
EXPECTED_GB10_SSH_CONFIG_SHA256 = "7ac3cbe20670762590b9efe4daea46126caa823f192e060be109b96350e82b4e"
_SHARED_REPOSITORY_ROOT = Path("/shared_work/qianyi/.loom-staging-rollout/worker-repos")
_GB10_KNOWN_HOSTS = Path("/etc/loom/staging-rollout-gb10-known-hosts")
_REMOTE_SHARED_REPOSITORY_PROBE = """
import os
import stat

paths = (
    "/shared_work/qianyi",
    "/shared_work/qianyi/.loom-staging-rollout",
    "/shared_work/qianyi/.loom-staging-rollout/worker-repos",
)
entries = [os.lstat(path) for path in paths]
safe = all(stat.S_ISDIR(item.st_mode) and not stat.S_ISLNK(item.st_mode) for item in entries)
safe = safe and os.access(paths[-1], os.R_OK | os.X_OK) and not os.access(paths[-1], os.W_OK)
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
print(";".join(fields))
raise SystemExit(0 if safe else 1)
""".strip()


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
    return f"sha256:{hashlib.sha256(payload).hexdigest()[:12]} len={len(payload)}"


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
    normalized = Path(os.path.normpath(path))
    if not normalized.is_absolute():
        return None
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    # Linux O_PATH preserves the installer contract of traverse-only parent
    # ACLs; O_RDONLY would also require directory listing permission.
    directory_flags = (
        getattr(os, "O_PATH", os.O_RDONLY)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    directory_fd: int | None = None
    try:
        directory_fd = os.open("/", directory_flags)
        for component in normalized.parts[1:-1]:
            next_fd = os.open(component, directory_flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = next_fd
        fd = os.open(normalized.name, flags, dir_fd=directory_fd)
    except OSError:
        if directory_fd is not None:
            os.close(directory_fd)
        return None
    os.close(directory_fd)
    try:
        metadata = os.fstat(fd)
        allowed_owners = {0, service_uid}
        if allow_qianyi_owner:
            try:
                allowed_owners.add(pwd.getpwnam("qianyi").pw_uid)
            except (KeyError, OSError):
                pass
        unsafe_mode = 0o137 if private else 0o022
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid not in allowed_owners
            or stat.S_IMODE(metadata.st_mode) & unsafe_mode
            or metadata.st_nlink != 1
            or metadata.st_size > 1024 * 1024
        ):
            return None
        payload = os.read(fd, metadata.st_size + 1)
        if len(payload) != metadata.st_size:
            return None
        after = os.fstat(fd)
        if (metadata.st_dev, metadata.st_ino, metadata.st_mtime_ns, metadata.st_size) != (
            after.st_dev,
            after.st_ino,
            after.st_mtime_ns,
            after.st_size,
        ):
            return None
        return payload
    finally:
        os.close(fd)


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


def _checkout_tree_is_trusted(root: Path, *, service_uid: int) -> bool:
    allowed_owners = {0, service_uid}
    try:
        root_metadata = root.lstat()
        root_resolved = root.resolve(strict=True)
    except (OSError, RuntimeError):
        return False
    if (
        not stat.S_ISDIR(root_metadata.st_mode)
        or stat.S_ISLNK(root_metadata.st_mode)
        or root_metadata.st_uid not in allowed_owners
        or stat.S_IMODE(root_metadata.st_mode) & 0o022
    ):
        return False
    for directory, directories, filenames in os.walk(root, followlinks=False):
        for name in [".", *directories, *filenames]:
            path = Path(directory) if name == "." else Path(directory) / name
            try:
                metadata = path.lstat()
            except OSError:
                return False
            if metadata.st_uid not in allowed_owners:
                return False
            if stat.S_ISLNK(metadata.st_mode):
                try:
                    target = (path.parent / os.readlink(path)).resolve(strict=False)
                except (OSError, RuntimeError):
                    return False
                if not target.is_relative_to(root_resolved):
                    return False
                continue
            if not (stat.S_ISDIR(metadata.st_mode) or stat.S_ISREG(metadata.st_mode)):
                return False
            if stat.S_IMODE(metadata.st_mode) & 0o022:
                return False
    try:
        git_dir = (root / ".git").lstat()
    except OSError:
        return False
    return stat.S_ISDIR(git_dir.st_mode) and not stat.S_ISLNK(git_dir.st_mode)


def _checkout_is_trusted(config: OperatorConfig, *, service_uid: int) -> bool:
    if not _checkout_tree_is_trusted(config.runner_repo, service_uid=service_uid):
        return False
    return (
        _readable_config_bytes(
            config.runner_repo / ".git" / "config",
            service_uid=service_uid,
        )
        is not None
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


def _shared_repository_binding(
    *,
    service_uid: int,
    root: Path = _SHARED_REPOSITORY_ROOT,
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


def _gb10_shared_repository_probe(
    run: CommandRunner,
    *,
    ssh_config: Path,
    identity: Path,
    hosts: tuple[str, ...],
    binding: dict[str, int],
) -> str | None:
    if hosts != ACTIVE_GB10_HOSTS:
        return None
    evidence = hashlib.sha256()
    remote_command = f"python3 -c {shlex.quote(_REMOTE_SHARED_REPOSITORY_PROBE)}"
    for host in hosts:
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
        if output is None:
            return None
        fields = output.strip().split(";")
        if len(fields) != 17 or any(not field for field in fields):
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
        except ValueError:
            return None
        parent = values[0:5]
        authority = values[5:10]
        repository = values[10:15]
        if (
            remote_uid != binding["consumer_uid"]
            or binding["shared_gid"] not in remote_groups
            or parent[:3] != [binding["consumer_uid"], binding["shared_gid"], int("2775", 8)]
            or authority[:3] != [binding["service_uid"], binding["shared_gid"], int("2750", 8)]
            or repository[:3] != [binding["service_uid"], binding["shared_gid"], int("2750", 8)]
            or any(value <= 0 for value in (*parent[3:], *authority[3:], *repository[3:]))
        ):
            return None
        evidence.update(host.encode("ascii"))
        evidence.update(b"\0")
        evidence.update(output.strip().encode("ascii"))
        evidence.update(b"\0")
    return f"sha256:{evidence.hexdigest()[:12]} hosts={len(hosts)}"


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
    importer: Callable[[str], object] = importlib.import_module,
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

    trusted_checkout = service_uid >= 0 and _checkout_is_trusted(config, service_uid=service_uid)
    remotes: str | None = None
    origin_url: str | None = None
    status: str | None = None
    pushurl_absent = False
    if trusted_checkout:
        remotes = _command_stdout(
            child_run,
            ["git", "-C", str(config.runner_repo), "remote"],
        )
        origin_url = _command_stdout(
            child_run,
            [
                "git",
                "-C",
                str(config.runner_repo),
                "remote",
                "get-url",
                "--all",
                "origin",
            ],
        )
        try:
            pushurl = child_run(
                [
                    "git",
                    "-C",
                    str(config.runner_repo),
                    "config",
                    "--get-all",
                    "remote.origin.pushurl",
                ]
            )
            pushurl_absent = (
                pushurl.returncode == 1 and pushurl.stdout == "" and pushurl.stderr == ""
            )
        except Exception:
            pushurl_absent = False
        status = _command_stdout(
            child_run,
            [
                "git",
                "-C",
                str(config.runner_repo),
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            ],
        )
    clean_checkout = (
        trusted_checkout
        and remotes is not None
        and remotes.splitlines() == ["origin"]
        and origin_url is not None
        and origin_url.splitlines() == [config.remote_url]
        and pushurl_absent
        and status == ""
    )
    add("checkout", clean_checkout, "restore the protected clean runner checkout")

    executable_lookup = which or (lambda name: shutil.which(name, path=environment["PATH"]))
    executables_ok = all(executable_lookup(name) is not None for name in _REQUIRED_EXECUTABLES)
    add("executables", executables_ok, "install the fixed rollout executable set")

    imports_ok = True
    for module in _REQUIRED_IMPORTS:
        try:
            importer(module)
        except Exception:
            imports_ok = False
    add("python-imports", imports_ok, "synchronize the locked rollout Python environment")

    add(
        "docker",
        trusted_checkout and _command_passes(child_run, ["docker", "info"]),
        "restore service Docker access",
    )
    add(
        "docker-buildx",
        trusted_checkout and _command_passes(child_run, ["docker", "buildx", "version"]),
        "install and verify the Docker buildx plugin",
    )

    current_context = None
    if trusted_checkout:
        current_context = _command_stdout(
            child_run,
            [
                "kubectl",
                "--kubeconfig",
                str(config.kubeconfig_path),
                "config",
                "current-context",
            ],
        )
    kube_context_ok = current_context is not None and current_context.strip() in {
        config.cluster_name,
        f"kind-{config.cluster_name}",
    }
    add("kube-context", kube_context_ok, "restore the fixed staging kube context")
    namespace_ok = trusted_checkout and _command_passes(
        child_run,
        [
            "kubectl",
            "--kubeconfig",
            str(config.kubeconfig_path),
            "get",
            "namespace",
            config.namespace,
            "--request-timeout=10s",
        ],
    )
    add(
        "kube-namespace",
        trusted_checkout and namespace_ok,
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
    "PreflightCheck",
    "PreflightReport",
    "catalog_secret_values",
    "collect_preflight",
    "safe_fingerprint",
]
