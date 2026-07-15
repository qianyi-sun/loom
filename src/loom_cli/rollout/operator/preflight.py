"""Safe, named preflight checks for the protected staging rollout broker."""

from __future__ import annotations

import hashlib
import importlib
import io
import os
import pwd
import re
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

    def __post_init__(self) -> None:
        if _CHECK_NAME_RE.fullmatch(self.name) is None:
            raise ValueError("preflight check name is invalid")
        if type(self.passed) is not bool:
            raise ValueError("preflight check passed must be a boolean")
        if self.remediation is None:
            return
        if (
            not self.remediation.strip()
            or len(self.remediation) > _MAX_REMEDIATION_LENGTH
            or any(ord(char) < 32 for char in self.remediation)
            or redact_rollout_text(self.remediation) != self.remediation
        ):
            raise ValueError("preflight remediation must be short and safe")

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "passed": self.passed,
            "remediation": self.remediation,
        }


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
