#!/usr/bin/python3 -I
"""Install the developer-environment authority transactionally on oldlab-2."""

from __future__ import annotations

import argparse
import fcntl
import grp
import hashlib
import json
import os
import pwd
import re
import socket
import sqlite3
import stat
import subprocess
import sys
import tempfile
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

SCHEMA_VERSION: Final = 1
NODE: Final = "oldlab-2"
CANONICAL_HOSTNAME: Final = "trt-eai-oldlab-2"
REPO_ROOT: Final = Path(__file__).absolute().parents[2]
MANIFEST_RELATIVE: Final = Path(
    "deploy/developer-sandboxes/developer-environment-authority.install.toml"
)
INSTALLER_RELATIVE: Final = Path("scripts/ops/developer_environment_authority_installer.py")
STATE_ROOT: Final = Path("/var/lib/loom-developer-environment-authority-installer")
LOCK_PATH: Final = STATE_ROOT / "installer.lock"
ACTIVE_PATH: Final = STATE_ROOT / "active-transaction.json"
JOURNAL_PATH: Final = STATE_ROOT / "journal.jsonl"
INSTALLED_PATH: Final = STATE_ROOT / "installed.json"
TRANSACTION_ROOT: Final = STATE_ROOT / "transactions"
REGISTRY_DATABASE: Final = Path("/var/lib/loom-developer-environment-registry/registry.sqlite3")
REGISTRY_SNAPSHOT: Final = Path(
    "/var/lib/loom-developer-environment-registry/current-snapshot.json"
)
AUTHORITY_RUNTIME_ROOT: Final = Path("/run/loom-developer-environment-authority")
AUTHORITY_SOCKET: Final = AUTHORITY_RUNTIME_ROOT / "authority.sock"
REGISTRY_ADMIN: Final = Path("/usr/local/libexec/scripts/ops/developer_environment_registry.py")
SOCKET_UNIT: Final = "loom-developer-environment-authority.socket"
RENEWAL_TIMER_UNIT: Final = "loom-developer-sandbox-attestation-renewal.timer"
NODE_TRANSPORT: Final = Path("/usr/local/libexec/loom-developer-sandbox-node-transport")
CAPACITY_RUNTIME_STATE: Final = Path(
    "/var/lib/loom-shared-capacity/runtime-host-installer/state.json"
)
CAPACITY_RUNTIME_STATE_ROOT: Final = CAPACITY_RUNTIME_STATE.parent
INITIAL_CAPACITY_RUNTIME_STATE: Final = (
    b'{"activation_status":"installed","admission_state":"closed",'
    b'"installation_mode":"fixed-registry-runtime","schema_version":1}\n'
)
GROUP_NAME: Final = "loom-developers"
LEGACY_OWNERS: Final = ("qianyi", "hongjian", "devansh")
SHA_RE: Final = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
TRANSACTION_RE: Final = re.compile(r"^[0-9a-f]{64}$")
MAX_ASSET_BYTES: Final = 4 * 1024 * 1024
MAX_STATE_BYTES: Final = 4 * 1024 * 1024
MAX_COMMAND_OUTPUT: Final = 4 * 1024 * 1024

EXPECTED_ASSETS: Final = {
    (
        "scripts/ops/developer_environment_registry.py",
        "/usr/local/libexec/scripts/ops/developer_environment_registry.py",
        "0555",
    ),
    (
        "scripts/ops/developer_environment_authority.py",
        "/usr/local/libexec/loom-developer-environment/developer_environment_authority.py",
        "0555",
    ),
    (
        "scripts/ops/developer_environment_authority_installer.py",
        "/usr/local/libexec/loom-developer-environment/"
        "developer_environment_authority_installer.py",
        "0555",
    ),
    (
        "scripts/ops/developer_environment_cli.py",
        "/usr/local/bin/loom-developer-environment",
        "0555",
    ),
    (
        "scripts/ops/developer_environment_deploy.py",
        "/usr/local/libexec/loom-developer-environment-deploy",
        "0555",
    ),
    (
        "scripts/ops/developer_environment_runtime_authority.py",
        "/usr/local/libexec/scripts/ops/developer_environment_runtime_authority.py",
        "0444",
    ),
    (
        "deploy/developer-sandboxes/loom-developer-environment-runtime-authority",
        "/usr/local/libexec/loom-developer-environment-runtime-authority",
        "0555",
    ),
    (
        "deploy/developer-sandboxes/loom-installed-python-package-init.py",
        "/usr/local/libexec/scripts/__init__.py",
        "0444",
    ),
    (
        "deploy/developer-sandboxes/loom-installed-python-package-init.py",
        "/usr/local/libexec/scripts/ops/__init__.py",
        "0444",
    ),
    (
        "scripts/ops/developer_environment_acceptance_probe.py",
        "/usr/local/libexec/scripts/ops/developer_environment_acceptance_probe.py",
        "0444",
    ),
    (
        "scripts/ops/developer_environment_runtime_retire.py",
        "/usr/local/libexec/scripts/ops/developer_environment_runtime_retire.py",
        "0444",
    ),
    (
        "scripts/ops/developer_environment_runtime_retire.py",
        "/usr/local/libexec/loom-developer-environment-runtime-retire",
        "0555",
    ),
    (
        "scripts/ops/developer_sandbox_host.py",
        "/usr/local/libexec/scripts/ops/developer_sandbox_host.py",
        "0444",
    ),
    (
        "scripts/ops/developer_sandbox_slurm_policy.py",
        "/usr/local/libexec/scripts/ops/developer_sandbox_slurm_policy.py",
        "0555",
    ),
    (
        "scripts/ops/developer_sandbox_remote_link_host.py",
        "/usr/local/libexec/loom-developer-sandbox-remote-link-host",
        "0555",
    ),
    (
        "scripts/ops/developer_sandbox_remote_link.py",
        "/usr/local/libexec/loom-developer-sandbox-remote-link",
        "0555",
    ),
    (
        "scripts/ops/developer_sandbox_domain_runtime.py",
        "/usr/local/libexec/loom-developer-domain-runtime",
        "0555",
    ),
    (
        "scripts/ops/shared_capacity_runtime_host.py",
        "/usr/local/libexec/scripts/ops/shared_capacity_runtime_host.py",
        "0444",
    ),
    (
        "scripts/ops/developer_sandbox_platform_health_authority.py",
        "/usr/local/libexec/scripts/ops/developer_sandbox_platform_health_authority.py",
        "0444",
    ),
    (
        "scripts/ops/developer_sandbox_live_acceptance.py",
        "/usr/local/libexec/scripts/ops/developer_sandbox_live_acceptance.py",
        "0444",
    ),
    (
        "scripts/ops/shared_capacity_adapter.py",
        "/usr/local/libexec/scripts/ops/shared_capacity_adapter.py",
        "0555",
    ),
    (
        "scripts/ops/shared_capacity_supervisor.py",
        "/usr/local/libexec/scripts/ops/shared_capacity_supervisor.py",
        "0444",
    ),
    (
        "src/loom_control_plane/__init__.py",
        "/usr/local/libexec/loom-runtime-python/loom_control_plane/__init__.py",
        "0444",
    ),
    (
        "src/loom_control_plane/shared_capacity_broker.py",
        "/usr/local/libexec/loom-runtime-python/loom_control_plane/shared_capacity_broker.py",
        "0444",
    ),
    (
        "scripts/ops/developer_sandbox_capacity_contract.py",
        "/usr/local/libexec/scripts/ops/developer_sandbox_capacity_contract.py",
        "0444",
    ),
    (
        "deploy/developer-sandboxes/loom-developer-environment-capacity-authority",
        "/usr/local/libexec/loom-developer-environment-capacity-authority",
        "0555",
    ),
    (
        "deploy/developer-sandboxes/developer-environment-registry-seed.toml",
        "/usr/local/share/loom/developer-environment-registry-seed.toml",
        "0444",
    ),
    (
        "deploy/developer-sandboxes/loom-developer-environment-authority.service",
        "/etc/systemd/system/loom-developer-environment-authority.service",
        "0644",
    ),
    (
        "deploy/developer-sandboxes/loom-developer-environment@.service",
        "/etc/systemd/system/loom-developer-environment@.service",
        "0644",
    ),
    (
        "deploy/developer-sandboxes/loom-developer-sandbox-attestation-renewal.service",
        "/etc/systemd/system/loom-developer-sandbox-attestation-renewal.service",
        "0644",
    ),
    (
        "deploy/developer-sandboxes/loom-developer-sandbox-attestation-renewal.timer",
        "/etc/systemd/system/loom-developer-sandbox-attestation-renewal.timer",
        "0644",
    ),
    (
        "deploy/developer-sandboxes/loom-developer-sandbox-links.target",
        "/etc/systemd/system/loom-developer-sandbox-links.target",
        "0644",
    ),
    (
        "deploy/developer-sandboxes/loom-developer-sandbox-link@.service",
        "/etc/systemd/system/loom-developer-sandbox-link@.service",
        "0644",
    ),
    (
        "deploy/developer-sandboxes/runtime-domains.toml",
        "/etc/loom/developer-runtime-domains.toml",
        "0644",
    ),
    (
        "deploy/developer-sandboxes/shared-capacity-policies/oldlab.toml",
        "/etc/loom/developer-shared-capacity-policies/oldlab.toml",
        "0644",
    ),
    (
        "deploy/developer-sandboxes/shared-capacity-policies/gb10.toml",
        "/etc/loom/developer-shared-capacity-policies/gb10.toml",
        "0644",
    ),
    (
        "deploy/developer-sandboxes/shared-capacity-supervisor/config.toml",
        "/etc/loom/shared-capacity-supervisor.base.toml",
        "0644",
    ),
    (
        "deploy/developer-sandboxes/loom-developer-shared-capacity-adapter@.service",
        "/etc/systemd/system/loom-shared-capacity-adapter@.service",
        "0644",
    ),
    (
        "deploy/developer-sandboxes/loom-shared-capacity-adapter@.timer",
        "/etc/systemd/system/loom-shared-capacity-adapter@.timer",
        "0644",
    ),
    (
        "deploy/developer-sandboxes/loom-developer-shared-capacity-supervisor.service",
        "/etc/systemd/system/loom-shared-capacity-supervisor.service",
        "0644",
    ),
    (
        "deploy/developer-sandboxes/loom-shared-capacity-supervisor.timer",
        "/etc/systemd/system/loom-shared-capacity-supervisor.timer",
        "0644",
    ),
    (
        "deploy/developer-sandboxes/loom-developer-environment-authority.socket",
        "/etc/systemd/system/loom-developer-environment-authority.socket",
        "0644",
    ),
    (
        "deploy/developer-sandboxes/loom-developer-environment-authority.tmpfiles.conf",
        "/etc/tmpfiles.d/loom-developer-environment-authority.conf",
        "0644",
    ),
    (
        "deploy/developer-sandboxes/loom-developer-environment-authority.sysusers.conf",
        "/etc/sysusers.d/loom-developer-environment-authority.conf",
        "0644",
    ),
}
FIXED_DESTINATIONS: Final = frozenset(
    destination for _source, destination, _mode in EXPECTED_ASSETS
)
MANAGED_DESTINATIONS: Final = FIXED_DESTINATIONS | {str(INSTALLED_PATH)}
LIBEXEC_DESTINATIONS: Final = frozenset(
    Path(destination).name
    for _source, destination, _mode in EXPECTED_ASSETS
    if destination.startswith("/usr/local/libexec/loom-developer-environment/")
)


class AuthorityInstallerError(RuntimeError):
    """A bounded, secret-safe authority installation failure."""


@dataclass(frozen=True, slots=True)
class Asset:
    source: str
    destination: str
    mode: int
    digest: str
    payload: bytes


@dataclass(frozen=True, slots=True)
class Backup:
    destination: str
    existed: bool
    mode: int | None
    digest: str | None
    backup_name: str | None


CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[bytes]]


def _canonical(payload: Mapping[str, Any]) -> bytes:
    try:
        return (
            json.dumps(
                dict(payload),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("ascii")
            + b"\n"
        )
    except (TypeError, UnicodeEncodeError) as exc:
        raise AuthorityInstallerError("installer evidence is not canonical") from exc


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _lexists(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise AuthorityInstallerError("installer path is unavailable") from exc
    return True


def _clean_env() -> dict[str, str]:
    return {
        "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "XDG_CONFIG_HOME": ("/var/lib/loom-developer-environment-authority-installer/xdg-config"),
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_OPTIONAL_LOCKS": "0",
    }


def _default_runner(argv: Sequence[str]) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        list(argv),
        stdin=subprocess.DEVNULL,
        env=_clean_env(),
        check=False,
        capture_output=True,
        timeout=120,
    )


def _metadata_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _read_regular(
    path: Path,
    *,
    limit: int,
    expected_uid: int | None = None,
    expected_gid: int | None = None,
    expected_mode: int | None = None,
) -> bytes:
    descriptor = -1
    try:
        lexical = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
        )
        before = os.fstat(descriptor)
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(65_536, limit + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                raise AuthorityInstallerError("installer file exceeds its size bound")
            chunks.append(chunk)
        after = os.fstat(descriptor)
        current = path.lstat()
    except AuthorityInstallerError:
        raise
    except OSError as exc:
        raise AuthorityInstallerError("installer file is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_ISLNK(lexical.st_mode)
        or before.st_nlink != 1
        or len(
            {
                _metadata_identity(lexical),
                _metadata_identity(before),
                _metadata_identity(after),
                _metadata_identity(current),
            }
        )
        != 1
        or (expected_uid is not None and before.st_uid != expected_uid)
        or (expected_gid is not None and before.st_gid != expected_gid)
        or (expected_mode is not None and stat.S_IMODE(before.st_mode) != expected_mode)
    ):
        raise AuthorityInstallerError("installer file metadata is unsafe")
    return b"".join(chunks)


class AuthorityInstaller:
    """Transactional fixed-path installer with a persistent recovery journal."""

    def __init__(
        self,
        *,
        filesystem_root: Path,
        source_root: Path,
        runner: CommandRunner = _default_runner,
        expected_uid: int = 0,
        expected_gid: int = 0,
        group_resolver: Callable[[str], Any] = grp.getgrnam,
        account_resolver: Callable[[str], Any] = pwd.getpwnam,
    ) -> None:
        self.filesystem_root = filesystem_root
        self.source_root = source_root
        self.runner = runner
        self.expected_uid = expected_uid
        self.expected_gid = expected_gid
        self.group_resolver = group_resolver
        self.account_resolver = account_resolver

    def _host(self, path: Path) -> Path:
        if (
            not path.is_absolute()
            or ".." in path.parts
            or "." in path.parts
            or str(Path(str(path))) != str(path)
        ):
            raise AuthorityInstallerError("installer path binding is invalid")
        return self.filesystem_root / path.relative_to("/")

    def _checked(self, *argv: str) -> bytes:
        try:
            completed = self.runner(argv)
        except (OSError, subprocess.SubprocessError) as exc:
            raise AuthorityInstallerError("installer fixed command failed") from exc
        if (
            completed.returncode != 0
            or len(completed.stdout) > MAX_COMMAND_OUTPUT
            or len(completed.stderr) > MAX_COMMAND_OUTPUT
        ):
            raise AuthorityInstallerError("installer fixed command failed")
        return completed.stdout

    def _ensure_directory(self, path: Path, mode: int) -> None:
        self._host(path)
        current = self.filesystem_root
        try:
            root_metadata = current.lstat()
        except OSError as exc:
            raise AuthorityInstallerError("installer filesystem root is unavailable") from exc
        if (
            not stat.S_ISDIR(root_metadata.st_mode)
            or stat.S_ISLNK(root_metadata.st_mode)
            or root_metadata.st_uid != self.expected_uid
            or root_metadata.st_gid != self.expected_gid
            or root_metadata.st_mode & 0o022
        ):
            raise AuthorityInstallerError("installer filesystem root is unsafe")
        for index, part in enumerate(path.relative_to("/").parts):
            current = current / part
            final = index == len(path.relative_to("/").parts) - 1
            created = False
            try:
                current.mkdir(mode=mode if final else 0o755)
                os.chown(current, self.expected_uid, self.expected_gid)
                os.chmod(current, mode if final else 0o755)
                created = True
            except FileExistsError:
                pass
            except OSError as exc:
                raise AuthorityInstallerError("installer directory is unavailable") from exc
            try:
                metadata = current.lstat()
            except OSError as exc:
                raise AuthorityInstallerError("installer directory is unavailable") from exc
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or metadata.st_uid != self.expected_uid
                or metadata.st_gid != self.expected_gid
                or metadata.st_mode & 0o022
            ):
                raise AuthorityInstallerError("installer directory is unsafe")
            if created and metadata.st_gid != self.expected_gid:
                raise AuthorityInstallerError("installer directory owner is unsafe")
            if (
                final
                and (
                    path == STATE_ROOT
                    or path == TRANSACTION_ROOT
                    or path == STATE_ROOT / "xdg-config"
                    or str(path).startswith(str(TRANSACTION_ROOT) + "/")
                )
                and stat.S_IMODE(metadata.st_mode) != mode
            ):
                raise AuthorityInstallerError("installer state directory is unsafe")

    def _fsync_directory(self, path: Path) -> None:
        descriptor = -1
        try:
            descriptor = os.open(
                self._host(path),
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
            )
            os.fsync(descriptor)
        except OSError as exc:
            raise AuthorityInstallerError("installer durability boundary failed") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    def _atomic_write(
        self,
        destination: Path,
        payload: bytes,
        mode: int,
    ) -> None:
        if (
            str(destination) not in FIXED_DESTINATIONS
            and destination != CAPACITY_RUNTIME_STATE
            and not str(destination).startswith(str(STATE_ROOT) + "/")
        ):
            raise AuthorityInstallerError("installer destination is outside authority")
        parent = destination.parent
        state_parent = parent == STATE_ROOT or str(parent).startswith(str(TRANSACTION_ROOT) + "/")
        self._ensure_directory(parent, 0o700 if state_parent else 0o755)
        host_parent = self._host(parent)
        descriptor = -1
        temporary: Path | None = None
        try:
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{destination.name}.",
                suffix=".tmp",
                dir=host_parent,
            )
            temporary = Path(temporary_name)
            os.fchmod(descriptor, mode)
            os.fchown(descriptor, self.expected_uid, self.expected_gid)
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                if written < 1:
                    raise AuthorityInstallerError("installer asset write failed")
                view = view[written:]
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            os.replace(temporary, self._host(destination))
            temporary = None
            self._fsync_directory(parent)
        except AuthorityInstallerError:
            raise
        except OSError as exc:
            raise AuthorityInstallerError("installer asset publication failed") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def _unlink_fixed(self, destination: Path) -> None:
        if str(destination) not in MANAGED_DESTINATIONS:
            raise AuthorityInstallerError("installer rollback target is outside authority")
        host = self._host(destination)
        try:
            metadata = host.lstat()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise AuthorityInstallerError("installer rollback target is unavailable") from exc
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != self.expected_uid
        ):
            raise AuthorityInstallerError("installer rollback target is unsafe")
        try:
            host.unlink()
        except OSError as exc:
            raise AuthorityInstallerError("installer rollback failed") from exc
        self._fsync_directory(destination.parent)

    def _verify_candidate(self, candidate_sha: str, candidate_tree: str) -> None:
        try:
            metadata = self.source_root.lstat()
        except OSError as exc:
            raise AuthorityInstallerError("installer candidate is unavailable") from exc
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or SHA_RE.fullmatch(candidate_sha) is None
            or SHA_RE.fullmatch(candidate_tree) is None
        ):
            raise AuthorityInstallerError("installer candidate binding is invalid")
        sha = (
            self._checked(
                "/usr/bin/git",
                "-C",
                str(self.source_root),
                "rev-parse",
                "HEAD",
            )
            .decode("ascii")
            .strip()
        )
        tree = (
            self._checked(
                "/usr/bin/git",
                "-C",
                str(self.source_root),
                "rev-parse",
                "HEAD^{tree}",
            )
            .decode("ascii")
            .strip()
        )
        tracked = sorted(
            {source for source, _destination, _mode in EXPECTED_ASSETS}
            | {MANIFEST_RELATIVE.as_posix(), INSTALLER_RELATIVE.as_posix()}
        )
        status = self._checked(
            "/usr/bin/git",
            "-C",
            str(self.source_root),
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--",
            *tracked,
        )
        if sha != candidate_sha or tree != candidate_tree or status:
            raise AuthorityInstallerError("installer candidate checkout is not exact")
        for relative in tracked:
            payload = _read_regular(
                self.source_root / relative,
                limit=MAX_ASSET_BYTES,
            )
            committed = self._checked(
                "/usr/bin/git",
                "-C",
                str(self.source_root),
                "cat-file",
                "blob",
                f"{candidate_sha}:{relative}",
            )
            if payload != committed:
                raise AuthorityInstallerError("installer candidate tracked asset drifted")

    def _load_assets(self) -> tuple[bytes, tuple[Asset, ...]]:
        manifest_raw = _read_regular(
            self.source_root / MANIFEST_RELATIVE,
            limit=MAX_ASSET_BYTES,
        )
        try:
            manifest = tomllib.loads(manifest_raw.decode("utf-8"))
        except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
            raise AuthorityInstallerError("installer manifest is invalid") from exc
        if (
            set(manifest)
            != {
                "schema_version",
                "kind",
                "socket_group",
                "accepted_membership_groups",
                "membership_migration_from",
                "asset",
            }
            or manifest.get("schema_version") != SCHEMA_VERSION
            or manifest.get("kind") != "loom.developer-environment.authority-install"
            or manifest.get("socket_group") != GROUP_NAME
            or manifest.get("accepted_membership_groups") != [GROUP_NAME]
            or manifest.get("membership_migration_from") != ["sharedwork"]
            or not isinstance(manifest.get("asset"), list)
        ):
            raise AuthorityInstallerError("installer manifest binding is invalid")
        declared: set[tuple[str, str, str]] = set()
        for value in manifest["asset"]:
            if (
                not isinstance(value, dict)
                or set(value) != {"source", "destination", "mode", "owner", "group"}
                or value.get("owner") != "root"
                or value.get("group") != "root"
                or not isinstance(value.get("source"), str)
                or not isinstance(value.get("destination"), str)
                or not isinstance(value.get("mode"), str)
            ):
                raise AuthorityInstallerError("installer manifest asset is invalid")
            declared.add(
                (
                    value["source"],
                    value["destination"],
                    value["mode"],
                )
            )
        if declared != EXPECTED_ASSETS or len(declared) != len(manifest["asset"]):
            raise AuthorityInstallerError("installer manifest asset set drifted")
        assets: list[Asset] = []
        for source, destination, rendered_mode in sorted(declared):
            payload = _read_regular(
                self.source_root / source,
                limit=MAX_ASSET_BYTES,
            )
            assets.append(
                Asset(
                    source=source,
                    destination=destination,
                    mode=int(rendered_mode, 8),
                    digest=_digest(payload),
                    payload=payload,
                )
            )
        return manifest_raw, tuple(assets)

    def _append_journal(self, payload: Mapping[str, Any]) -> None:
        event = {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-environment.authority-install-event",
            **payload,
            "recorded_at": _timestamp(),
        }
        raw = _canonical(event)
        path = self._host(JOURNAL_PATH)
        descriptor = -1
        try:
            descriptor = os.open(
                path,
                os.O_WRONLY
                | os.O_APPEND
                | os.O_CREAT
                | os.O_NOFOLLOW
                | getattr(os, "O_CLOEXEC", 0),
                0o600,
            )
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_nlink != 1
                or metadata.st_uid != self.expected_uid
                or stat.S_IMODE(metadata.st_mode) != 0o600
            ):
                raise AuthorityInstallerError("installer journal is unsafe")
            os.write(descriptor, raw)
            os.fsync(descriptor)
        except AuthorityInstallerError:
            raise
        except OSError as exc:
            raise AuthorityInstallerError("installer journal write failed") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    def _active_payload(
        self,
        *,
        transaction_id: str,
        action: str,
        candidate_sha: str,
        candidate_tree: str,
        manifest_sha256: str,
        backups: Sequence[Backup],
        phase: str,
    ) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-environment.authority-active-install",
            "transaction_id": transaction_id,
            "action": action,
            "candidate_sha": candidate_sha,
            "candidate_tree": candidate_tree,
            "manifest_sha256": manifest_sha256,
            "backups": [asdict(item) for item in backups],
            "phase": phase,
        }

    def _write_active(self, payload: Mapping[str, Any]) -> None:
        self._atomic_write(ACTIVE_PATH, _canonical(payload), 0o600)

    def _read_active(self) -> dict[str, Any] | None:
        path = self._host(ACTIVE_PATH)
        if not _lexists(path):
            return None
        raw = _read_regular(
            path,
            limit=MAX_STATE_BYTES,
            expected_uid=self.expected_uid,
            expected_gid=self.expected_gid,
            expected_mode=0o600,
        )
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AuthorityInstallerError("installer active journal is invalid") from exc
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "schema_version",
                "kind",
                "transaction_id",
                "action",
                "candidate_sha",
                "candidate_tree",
                "manifest_sha256",
                "backups",
                "phase",
            }
            or value.get("schema_version") != SCHEMA_VERSION
            or value.get("kind") != "loom.developer-environment.authority-active-install"
            or TRANSACTION_RE.fullmatch(str(value.get("transaction_id"))) is None
            or value.get("action")
            not in {
                "environment-authority-bootstrap",
                "environment-authority-upgrade",
            }
            or SHA_RE.fullmatch(str(value.get("candidate_sha"))) is None
            or SHA_RE.fullmatch(str(value.get("candidate_tree"))) is None
            or SHA256_RE.fullmatch(str(value.get("manifest_sha256"))) is None
            or value.get("phase") not in {"prepared", "assets-installed"}
            or not isinstance(value.get("backups"), list)
            or raw != _canonical(value)
        ):
            raise AuthorityInstallerError("installer active journal binding is invalid")
        destinations: set[str] = set()
        for item in value["backups"]:
            if (
                not isinstance(item, dict)
                or set(item)
                != {
                    "destination",
                    "existed",
                    "mode",
                    "digest",
                    "backup_name",
                }
                or item.get("destination") not in MANAGED_DESTINATIONS
                or not isinstance(item.get("existed"), bool)
                or (
                    item["existed"]
                    and (
                        not isinstance(item.get("mode"), int)
                        or not isinstance(item.get("digest"), str)
                        or SHA256_RE.fullmatch(item["digest"]) is None
                        or not isinstance(item.get("backup_name"), str)
                        or re.fullmatch(
                            r"[0-9]{2}[.](?:asset|installed)",
                            item["backup_name"],
                        )
                        is None
                    )
                )
                or (
                    not item["existed"]
                    and any(
                        item.get(field) is not None for field in ("mode", "digest", "backup_name")
                    )
                )
            ):
                raise AuthorityInstallerError("installer active backup is invalid")
            destinations.add(item["destination"])
        if destinations != MANAGED_DESTINATIONS:
            raise AuthorityInstallerError("installer active backup set is incomplete")
        return value

    def _remove_active(self) -> None:
        path = self._host(ACTIVE_PATH)
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            raise AuthorityInstallerError("installer active cleanup failed") from exc
        self._fsync_directory(STATE_ROOT)

    def _prepare_backups(
        self,
        transaction_id: str,
        assets: Sequence[Asset],
    ) -> tuple[Backup, ...]:
        transaction = TRANSACTION_ROOT / transaction_id
        self._ensure_directory(transaction, 0o700)
        backups: list[Backup] = []
        for index, asset in enumerate(assets):
            destination = Path(asset.destination)
            host = self._host(destination)
            try:
                metadata = host.lstat()
            except FileNotFoundError:
                backups.append(Backup(asset.destination, False, None, None, None))
                continue
            except OSError as exc:
                raise AuthorityInstallerError("installer prior asset is unavailable") from exc
            if (
                not stat.S_ISREG(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or metadata.st_uid != self.expected_uid
                or metadata.st_gid != self.expected_gid
                or metadata.st_nlink != 1
            ):
                raise AuthorityInstallerError("installer prior asset is unsafe")
            payload = _read_regular(
                host,
                limit=MAX_ASSET_BYTES,
                expected_uid=self.expected_uid,
                expected_gid=self.expected_gid,
                expected_mode=stat.S_IMODE(metadata.st_mode),
            )
            backup_name = f"{index:02d}.asset"
            self._atomic_write(
                transaction / backup_name,
                payload,
                0o600,
            )
            backups.append(
                Backup(
                    destination=asset.destination,
                    existed=True,
                    mode=stat.S_IMODE(metadata.st_mode),
                    digest=_digest(payload),
                    backup_name=backup_name,
                )
            )
        installed_host = self._host(INSTALLED_PATH)
        try:
            installed_metadata = installed_host.lstat()
        except FileNotFoundError:
            backups.append(Backup(str(INSTALLED_PATH), False, None, None, None))
        except OSError as exc:
            raise AuthorityInstallerError("installer prior record is unavailable") from exc
        else:
            installed_payload = _read_regular(
                installed_host,
                limit=MAX_STATE_BYTES,
                expected_uid=self.expected_uid,
                expected_gid=self.expected_gid,
                expected_mode=0o600,
            )
            backup_name = f"{len(assets):02d}.installed"
            self._atomic_write(
                transaction / backup_name,
                installed_payload,
                0o600,
            )
            backups.append(
                Backup(
                    destination=str(INSTALLED_PATH),
                    existed=True,
                    mode=stat.S_IMODE(installed_metadata.st_mode),
                    digest=_digest(installed_payload),
                    backup_name=backup_name,
                )
            )
        return tuple(backups)

    def _rollback(self, active: Mapping[str, Any]) -> None:
        transaction = TRANSACTION_ROOT / str(active["transaction_id"])
        if active["action"] == "environment-authority-bootstrap":
            self._checked(
                "/usr/bin/systemctl",
                "disable",
                "--now",
                RENEWAL_TIMER_UNIT,
                SOCKET_UNIT,
            )
        for value in active["backups"]:
            destination = Path(value["destination"])
            if value["existed"]:
                backup = transaction / str(value["backup_name"])
                payload = _read_regular(
                    self._host(backup),
                    limit=MAX_ASSET_BYTES,
                    expected_uid=self.expected_uid,
                    expected_gid=self.expected_gid,
                    expected_mode=0o600,
                )
                if _digest(payload) != value["digest"]:
                    raise AuthorityInstallerError("installer rollback backup drifted")
                self._atomic_write(destination, payload, int(value["mode"]))
            else:
                self._unlink_fixed(destination)
        self._checked("/usr/bin/systemctl", "daemon-reload")
        self._append_journal(
            {
                "transaction_id": active["transaction_id"],
                "action": active["action"],
                "candidate_sha": active["candidate_sha"],
                "candidate_tree": active["candidate_tree"],
                "phase": "rolled-back",
            }
        )
        self._remove_active()

    def recover(self) -> None:
        active = self._read_active()
        if active is not None:
            self._rollback(active)

    def _validate_dedicated_inventory(self) -> None:
        root = self._host(Path("/usr/local/libexec/loom-developer-environment"))
        try:
            entries = tuple(root.iterdir())
        except FileNotFoundError:
            return
        except OSError as exc:
            raise AuthorityInstallerError("installer dedicated inventory is unavailable") from exc
        if (
            any(entry.is_symlink() for entry in entries)
            or {entry.name for entry in entries} - LIBEXEC_DESTINATIONS
        ):
            raise AuthorityInstallerError("installer dedicated inventory is not closed")

    def _read_installed(self) -> dict[str, Any] | None:
        path = self._host(INSTALLED_PATH)
        if not _lexists(path):
            return None
        raw = _read_regular(
            path,
            limit=MAX_STATE_BYTES,
            expected_uid=self.expected_uid,
            expected_gid=self.expected_gid,
            expected_mode=0o600,
        )
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AuthorityInstallerError("installed authority record is invalid") from exc
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "schema_version",
                "kind",
                "source_sha",
                "source_tree",
                "manifest_sha256",
                "asset_digests",
                "installed_at",
                "status",
            }
            or value.get("schema_version") != SCHEMA_VERSION
            or value.get("kind") != "loom.developer-environment.authority-installed"
            or SHA_RE.fullmatch(str(value.get("source_sha"))) is None
            or SHA_RE.fullmatch(str(value.get("source_tree"))) is None
            or SHA256_RE.fullmatch(str(value.get("manifest_sha256"))) is None
            or not isinstance(value.get("asset_digests"), dict)
            or set(value["asset_digests"]) != FIXED_DESTINATIONS
            or any(
                SHA256_RE.fullmatch(str(digest)) is None
                for digest in value["asset_digests"].values()
            )
            or not isinstance(value.get("installed_at"), str)
            or value.get("status") != "installed"
            or raw != _canonical(value)
        ):
            raise AuthorityInstallerError("installed authority record is invalid")
        return value

    def _verify_assets(
        self,
        assets: Sequence[Asset],
        expected_digests: Mapping[str, Any],
    ) -> None:
        for asset in assets:
            if expected_digests.get(asset.destination) != asset.digest:
                raise AuthorityInstallerError("installed manifest drifted")
            self._validate_installed_ancestry(Path(asset.destination))
            payload = _read_regular(
                self._host(Path(asset.destination)),
                limit=MAX_ASSET_BYTES,
                expected_uid=self.expected_uid,
                expected_gid=self.expected_gid,
                expected_mode=asset.mode,
            )
            if payload != asset.payload:
                raise AuthorityInstallerError("installed authority asset drifted")
        self._validate_import_inventory()
        self._validate_dedicated_inventory()

    def _verify_installed_assets(self, installed: Mapping[str, Any]) -> None:
        modes = {destination: int(mode, 8) for _source, destination, mode in EXPECTED_ASSETS}
        digests = installed["asset_digests"]
        for destination in sorted(FIXED_DESTINATIONS):
            self._validate_installed_ancestry(Path(destination))
            payload = _read_regular(
                self._host(Path(destination)),
                limit=MAX_ASSET_BYTES,
                expected_uid=self.expected_uid,
                expected_gid=self.expected_gid,
                expected_mode=modes[destination],
            )
            if _digest(payload) != digests[destination]:
                raise AuthorityInstallerError("installed authority asset drifted")
        self._validate_import_inventory()
        self._validate_dedicated_inventory()

    def _validate_import_inventory(self) -> None:
        python_destinations = {
            Path(destination)
            for _source, destination, _mode in EXPECTED_ASSETS
            if Path(destination).parent
            in {
                Path("/usr/local/libexec/scripts"),
                Path("/usr/local/libexec/scripts/ops"),
                Path("/usr/local/libexec/loom-runtime-python/loom_control_plane"),
            }
        }
        expected = {
            Path("/usr/local/libexec/scripts"): {"__init__.py", "ops"},
            Path("/usr/local/libexec/scripts/ops"): {
                path.name
                for path in python_destinations
                if path.parent == Path("/usr/local/libexec/scripts/ops")
            },
            Path("/usr/local/libexec/loom-runtime-python"): {"loom_control_plane"},
            Path("/usr/local/libexec/loom-runtime-python/loom_control_plane"): {
                path.name
                for path in python_destinations
                if path.parent == Path("/usr/local/libexec/loom-runtime-python/loom_control_plane")
            },
        }
        for directory, allowed in expected.items():
            self._validate_installed_ancestry(directory / ".inventory")
            host = self._host(directory)
            try:
                entries = tuple(host.iterdir())
            except OSError as exc:
                raise AuthorityInstallerError(
                    "installed authority import inventory is unavailable"
                ) from exc
            if {entry.name for entry in entries} != allowed or any(
                entry.is_symlink() for entry in entries
            ):
                raise AuthorityInstallerError("installed authority import inventory is not closed")
            for entry in entries:
                metadata = entry.lstat()
                expected_directory = entry.name in {"ops", "loom_control_plane"}
                if (
                    metadata.st_uid != self.expected_uid
                    or metadata.st_gid != self.expected_gid
                    or metadata.st_mode & 0o022
                    or (expected_directory and not stat.S_ISDIR(metadata.st_mode))
                    or (
                        not expected_directory
                        and (not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1)
                    )
                ):
                    raise AuthorityInstallerError(
                        "installed authority import inventory metadata is unsafe"
                    )

    def _validate_installed_ancestry(self, destination: Path) -> None:
        self._host(destination)
        current = self.filesystem_root
        paths = [current]
        for part in destination.parent.relative_to("/").parts:
            current /= part
            paths.append(current)
        for path in paths:
            try:
                metadata = path.lstat()
            except OSError as exc:
                raise AuthorityInstallerError(
                    "installed authority import ancestry is unavailable"
                ) from exc
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or metadata.st_uid != self.expected_uid
                or metadata.st_gid != self.expected_gid
                or metadata.st_mode & 0o022
            ):
                raise AuthorityInstallerError("installed authority import ancestry is unsafe")

    def _validate_transport_prerequisite(self) -> None:
        self._validate_installed_ancestry(NODE_TRANSPORT)
        _read_regular(
            self._host(NODE_TRANSPORT),
            limit=MAX_ASSET_BYTES,
            expected_uid=self.expected_uid,
            expected_gid=self.expected_gid,
            expected_mode=0o755,
        )
        raw = self._checked(str(NODE_TRANSPORT), "check-client")
        try:
            report = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AuthorityInstallerError(
                "node authority transport prerequisite is invalid"
            ) from exc
        if (
            not isinstance(report, dict)
            or report.get("schema_version") != 1
            or report.get("action") != "check-client"
            or report.get("initiator") != NODE
            or report.get("status") != "succeeded"
            or not isinstance(report.get("roles"), list)
            or not report["roles"]
            or not isinstance(report.get("public_key_fingerprints"), dict)
        ):
            raise AuthorityInstallerError("node authority transport prerequisite is invalid")

    def _validate_capacity_runtime_state(self) -> bytes:
        raw = _read_regular(
            self._host(CAPACITY_RUNTIME_STATE),
            limit=MAX_STATE_BYTES,
            expected_uid=self.expected_uid,
            expected_gid=self.expected_gid,
            expected_mode=0o600,
        )
        try:
            state = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AuthorityInstallerError("shared capacity runtime state is invalid") from exc
        activation_status = state.get("activation_status") if isinstance(state, dict) else None
        if (
            not isinstance(state, dict)
            or raw != _canonical(state)
            or state.get("schema_version") != 1
            or activation_status
            not in {"installed", "bootstrap-active", "acceptance-active", "activated"}
        ):
            raise AuthorityInstallerError("shared capacity runtime state is invalid")
        if activation_status == "installed":
            if state != json.loads(INITIAL_CAPACITY_RUNTIME_STATE):
                required = {
                    "candidate_sha",
                    "candidate_tree",
                    "transaction_id",
                    "runtime_manifest",
                }
                if not required.issubset(state):
                    raise AuthorityInstallerError("shared capacity runtime state is invalid")
        elif (
            SHA_RE.fullmatch(str(state.get("candidate_sha"))) is None
            or SHA_RE.fullmatch(str(state.get("candidate_tree"))) is None
            or not isinstance(state.get("runtime_manifest"), dict)
        ):
            raise AuthorityInstallerError("shared capacity runtime state is invalid")
        return raw

    def _ensure_capacity_runtime_state(
        self,
        *,
        transaction_id: str,
        action: str,
        candidate_sha: str,
        candidate_tree: str,
    ) -> bytes:
        state_path = self._host(CAPACITY_RUNTIME_STATE)
        if _lexists(state_path):
            return self._validate_capacity_runtime_state()
        if action != "environment-authority-bootstrap":
            raise AuthorityInstallerError("shared capacity runtime state is unavailable")
        self._ensure_directory(CAPACITY_RUNTIME_STATE_ROOT, 0o700)
        self._atomic_write(
            CAPACITY_RUNTIME_STATE,
            INITIAL_CAPACITY_RUNTIME_STATE,
            0o600,
        )
        raw = self._validate_capacity_runtime_state()
        self._append_journal(
            {
                "transaction_id": transaction_id,
                "action": action,
                "candidate_sha": candidate_sha,
                "candidate_tree": candidate_tree,
                "phase": "capacity-runtime-state-created",
                "capacity_runtime_state_sha256": _digest(raw),
            }
        )
        return raw

    def _ensure_group(self) -> int:
        self._checked(
            "/usr/bin/systemd-sysusers",
            "/etc/sysusers.d/loom-developer-environment-authority.conf",
        )
        try:
            group = self.group_resolver(GROUP_NAME)
        except KeyError as exc:
            raise AuthorityInstallerError("authority group is unavailable") from exc
        if group.gr_name != GROUP_NAME or not isinstance(group.gr_gid, int):
            raise AuthorityInstallerError("authority group binding is invalid")
        for username in LEGACY_OWNERS:
            try:
                account = self.account_resolver(username)
            except KeyError as exc:
                raise AuthorityInstallerError("legacy authority owner is unavailable") from exc
            if account.pw_name != username:
                raise AuthorityInstallerError("legacy authority owner binding is invalid")
            self._checked(
                "/usr/sbin/usermod",
                "-a",
                "-G",
                GROUP_NAME,
                username,
            )
            memberships = self._checked("/usr/bin/id", "-nG", username).decode("utf-8").split()
            if GROUP_NAME not in memberships:
                raise AuthorityInstallerError("legacy authority membership migration failed")
        return int(group.gr_gid)

    def _validate_registry_snapshot(self) -> str:
        raw = _read_regular(
            self._host(REGISTRY_SNAPSHOT),
            limit=MAX_STATE_BYTES,
            expected_uid=self.expected_uid,
            expected_mode=0o600,
        )
        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AuthorityInstallerError("registry snapshot is invalid") from exc
        if (
            not isinstance(payload, dict)
            or raw != _canonical(payload)
            or SHA256_RE.fullmatch(str(payload.get("payload_sha256"))) is None
        ):
            raise AuthorityInstallerError("registry snapshot binding is invalid")
        unsigned = {key: value for key, value in payload.items() if key != "payload_sha256"}
        if payload["payload_sha256"] != _digest(_canonical(unsigned)):
            raise AuthorityInstallerError("registry snapshot digest is invalid")
        return str(payload["payload_sha256"])

    def _validate_socket(self, group_gid: int) -> None:
        parent = self._host(AUTHORITY_RUNTIME_ROOT)
        endpoint = self._host(AUTHORITY_SOCKET)
        try:
            parent_metadata = parent.lstat()
            socket_metadata = endpoint.lstat()
        except OSError as exc:
            raise AuthorityInstallerError("authority socket is unavailable") from exc
        if (
            not stat.S_ISDIR(parent_metadata.st_mode)
            or stat.S_ISLNK(parent_metadata.st_mode)
            or parent_metadata.st_uid != self.expected_uid
            or stat.S_IMODE(parent_metadata.st_mode) != 0o755
            or not stat.S_ISSOCK(socket_metadata.st_mode)
            or stat.S_ISLNK(socket_metadata.st_mode)
            or socket_metadata.st_uid != self.expected_uid
            or socket_metadata.st_gid != group_gid
            or stat.S_IMODE(socket_metadata.st_mode) != 0o660
        ):
            raise AuthorityInstallerError("authority socket permission model is invalid")

    def _converge_services(self) -> tuple[int, str]:
        self._preflight_registry_finalization_schema()
        group_gid = self._ensure_group()
        self._checked(
            "/usr/bin/systemd-tmpfiles",
            "--create",
            "/etc/tmpfiles.d/loom-developer-environment-authority.conf",
        )
        self._checked("/usr/bin/systemctl", "daemon-reload")
        self._checked(
            "/usr/bin/systemctl",
            "enable",
            "--now",
            SOCKET_UNIT,
        )
        self._checked(
            "/usr/bin/python3",
            "-I",
            "-B",
            str(REGISTRY_ADMIN),
            "init",
        )
        self._checked(
            "/usr/bin/python3",
            "-I",
            "-B",
            str(REGISTRY_ADMIN),
            "import-seed",
        )
        self._checked(
            "/usr/bin/systemctl",
            "enable",
            "--now",
            RENEWAL_TIMER_UNIT,
        )
        self._checked("/usr/bin/systemctl", "restart", SOCKET_UNIT)
        self._checked("/usr/bin/systemctl", "is-active", "--quiet", SOCKET_UNIT)
        self._checked("/usr/bin/systemctl", "is-active", "--quiet", RENEWAL_TIMER_UNIT)
        self._checked("/usr/bin/systemctl", "is-enabled", "--quiet", SOCKET_UNIT)
        self._checked("/usr/bin/systemctl", "is-enabled", "--quiet", RENEWAL_TIMER_UNIT)
        status_raw = self._checked(
            "/usr/bin/python3",
            "-I",
            "-B",
            str(REGISTRY_ADMIN),
            "status",
        )
        try:
            status = json.loads(status_raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AuthorityInstallerError("registry status evidence is invalid") from exc
        if (
            not isinstance(status, dict)
            or status_raw != _canonical(status)
            or status.get("status") != "succeeded"
            or status.get("action") != "status"
        ):
            raise AuthorityInstallerError("registry status evidence is invalid")
        database = self._host(REGISTRY_DATABASE)
        _read_regular(
            database,
            limit=MAX_STATE_BYTES,
            expected_uid=self.expected_uid,
            expected_mode=0o600,
        )
        registry_digest = self._validate_registry_snapshot()
        self._validate_socket(group_gid)
        return group_gid, registry_digest

    def _preflight_registry_finalization_schema(self) -> None:
        """Refuse legacy committed rows using a strictly read-only connection."""

        database = self._host(REGISTRY_DATABASE)
        if not database.exists() and not database.is_symlink():
            return
        try:
            metadata = database.lstat()
        except OSError as exc:
            raise AuthorityInstallerError("registry database is unavailable") from exc
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != self.expected_uid
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise AuthorityInstallerError("registry database metadata is unsafe")
        try:
            connection = sqlite3.connect(
                f"{database.as_uri()}?mode=ro&immutable=1",
                uri=True,
            )
            connection.row_factory = sqlite3.Row
            deployments = connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = 'deployments'
                """
            ).fetchone()
            if deployments is None:
                return
            committed = connection.execute(
                "SELECT 1 FROM deployments WHERE phase = 'committed' LIMIT 1"
            ).fetchone()
            if committed is None:
                return
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(deployments)").fetchall()
            }
            finalizations = connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = 'deployment_finalizations'
                """
            ).fetchone()
            required = {
                "applied_resource_generation",
                "applied_registry_generation",
                "applied_registry_payload_sha256",
                "finalization_payload_sha256",
            }
            incomplete = not required.issubset(columns) or finalizations is None
            if not incomplete:
                incomplete = (
                    connection.execute(
                        """
                        SELECT 1
                        FROM deployments AS d
                        LEFT JOIN deployment_finalizations AS f
                          ON f.deployment_id = d.deployment_id
                        WHERE d.phase = 'committed'
                          AND (
                            d.finalization_payload_sha256 IS NULL
                            OR f.payload_sha256 IS NULL
                            OR f.payload_sha256 != d.finalization_payload_sha256
                          )
                        LIMIT 1
                        """
                    ).fetchone()
                    is not None
                )
            if incomplete:
                raise AuthorityInstallerError("legacy committed finalization migration required")
        except AuthorityInstallerError:
            raise
        except sqlite3.Error as exc:
            raise AuthorityInstallerError("registry finalization preflight failed safely") from exc
        finally:
            if "connection" in locals():
                connection.close()

    def readback(
        self,
        *,
        candidate_sha: str,
        candidate_tree: str,
        assets: Sequence[Asset],
        manifest_sha256: str,
    ) -> dict[str, Any]:
        installed = self._read_installed()
        if (
            installed is None
            or installed["source_sha"] != candidate_sha
            or installed["source_tree"] != candidate_tree
            or installed["manifest_sha256"] != manifest_sha256
        ):
            raise AuthorityInstallerError("installed authority candidate drifted")
        self._verify_assets(assets, installed["asset_digests"])
        self._validate_capacity_runtime_state()
        self._validate_transport_prerequisite()
        group_gid = self._ensure_group()
        self._checked("/usr/bin/systemctl", "is-active", "--quiet", SOCKET_UNIT)
        self._checked("/usr/bin/systemctl", "is-active", "--quiet", RENEWAL_TIMER_UNIT)
        self._checked("/usr/bin/systemctl", "is-enabled", "--quiet", SOCKET_UNIT)
        self._checked("/usr/bin/systemctl", "is-enabled", "--quiet", RENEWAL_TIMER_UNIT)
        status_raw = self._checked(
            "/usr/bin/python3",
            "-I",
            "-B",
            str(REGISTRY_ADMIN),
            "status",
        )
        try:
            status = json.loads(status_raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AuthorityInstallerError("registry status evidence is invalid") from exc
        if (
            not isinstance(status, dict)
            or status_raw != _canonical(status)
            or status.get("status") != "succeeded"
        ):
            raise AuthorityInstallerError("registry status evidence is invalid")
        registry_digest = self._validate_registry_snapshot()
        self._validate_socket(group_gid)
        return {
            "schema_version": SCHEMA_VERSION,
            "action": "environment-authority-readback",
            "node": NODE,
            "source_sha": candidate_sha,
            "source_tree": candidate_tree,
            "manifest_sha256": manifest_sha256,
            "installed_asset_digests": dict(installed["asset_digests"]),
            "registry_snapshot_sha256": registry_digest,
            "status": "succeeded",
        }

    def install(
        self,
        *,
        action: str,
        candidate_sha: str,
        candidate_tree: str,
    ) -> dict[str, Any]:
        if action not in {
            "environment-authority-bootstrap",
            "environment-authority-upgrade",
            "environment-authority-readback",
        }:
            raise AuthorityInstallerError("installer action is invalid")
        self._ensure_directory(STATE_ROOT, 0o700)
        self._ensure_directory(TRANSACTION_ROOT, 0o700)
        self._ensure_directory(STATE_ROOT / "xdg-config", 0o700)
        self.recover()
        self._validate_transport_prerequisite()
        self._verify_candidate(candidate_sha, candidate_tree)
        manifest_raw, assets = self._load_assets()
        manifest_sha256 = _digest(manifest_raw)
        if action == "environment-authority-readback":
            return self.readback(
                candidate_sha=candidate_sha,
                candidate_tree=candidate_tree,
                assets=assets,
                manifest_sha256=manifest_sha256,
            )
        self._validate_dedicated_inventory()
        installed = self._read_installed()
        if installed is not None:
            self._verify_installed_assets(installed)
            if (
                installed["source_sha"] == candidate_sha
                and installed["source_tree"] == candidate_tree
                and installed["manifest_sha256"] == manifest_sha256
            ):
                self._verify_assets(assets, installed["asset_digests"])
                report = self.readback(
                    candidate_sha=candidate_sha,
                    candidate_tree=candidate_tree,
                    assets=assets,
                    manifest_sha256=manifest_sha256,
                )
                return {**report, "action": action, "idempotent": True}
            if action != "environment-authority-upgrade":
                raise AuthorityInstallerError("bootstrap cannot replace an installed authority")
        elif action == "environment-authority-upgrade":
            raise AuthorityInstallerError("upgrade requires an installed authority")
        elif any(_lexists(self._host(Path(value))) for value in FIXED_DESTINATIONS):
            raise AuthorityInstallerError("partial authority installation is present")

        transaction_id = _digest(
            _canonical(
                {
                    "action": action,
                    "candidate_sha": candidate_sha,
                    "candidate_tree": candidate_tree,
                    "manifest_sha256": manifest_sha256,
                }
            )
        )
        backups = self._prepare_backups(transaction_id, assets)
        active = self._active_payload(
            transaction_id=transaction_id,
            action=action,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            manifest_sha256=manifest_sha256,
            backups=backups,
            phase="prepared",
        )
        self._write_active(active)
        self._append_journal(
            {
                "transaction_id": transaction_id,
                "action": action,
                "candidate_sha": candidate_sha,
                "candidate_tree": candidate_tree,
                "phase": "prepared",
            }
        )
        try:
            self._ensure_capacity_runtime_state(
                transaction_id=transaction_id,
                action=action,
                candidate_sha=candidate_sha,
                candidate_tree=candidate_tree,
            )
            for asset in assets:
                self._atomic_write(
                    Path(asset.destination),
                    asset.payload,
                    asset.mode,
                )
            active = {**active, "phase": "assets-installed"}
            self._write_active(active)
            self._append_journal(
                {
                    "transaction_id": transaction_id,
                    "action": action,
                    "candidate_sha": candidate_sha,
                    "candidate_tree": candidate_tree,
                    "phase": "assets-installed",
                }
            )
            _group_gid, registry_digest = self._converge_services()
            asset_digests = {asset.destination: asset.digest for asset in assets}
            installed_record = {
                "schema_version": SCHEMA_VERSION,
                "kind": "loom.developer-environment.authority-installed",
                "source_sha": candidate_sha,
                "source_tree": candidate_tree,
                "manifest_sha256": manifest_sha256,
                "asset_digests": asset_digests,
                "installed_at": _timestamp(),
                "status": "installed",
            }
            self._atomic_write(INSTALLED_PATH, _canonical(installed_record), 0o600)
            self._verify_assets(assets, asset_digests)
            self._append_journal(
                {
                    "transaction_id": transaction_id,
                    "action": action,
                    "candidate_sha": candidate_sha,
                    "candidate_tree": candidate_tree,
                    "phase": "committed",
                }
            )
            self._remove_active()
        except Exception:
            recovery = self._read_active()
            if recovery is not None:
                self._rollback(recovery)
            raise
        return {
            "schema_version": SCHEMA_VERSION,
            "action": action,
            "node": NODE,
            "source_sha": candidate_sha,
            "source_tree": candidate_tree,
            "manifest_sha256": manifest_sha256,
            "installed_asset_digests": asset_digests,
            "registry_snapshot_sha256": registry_digest,
            "idempotent": False,
            "status": "succeeded",
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument(
        "action",
        choices=(
            "environment-authority-bootstrap",
            "environment-authority-upgrade",
            "environment-authority-readback",
        ),
    )
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--candidate-tree", required=True)
    return parser


def _require_canonical_root() -> None:
    hostname = socket.gethostname().split(".", 1)[0].casefold()
    if os.getuid() != 0 or os.geteuid() != 0 or hostname != CANONICAL_HOSTNAME:
        raise AuthorityInstallerError(
            f"installer requires canonical {CANONICAL_HOSTNAME} root",
        )


def execute(argv: Sequence[str] | None = None) -> dict[str, Any]:
    arguments = _parser().parse_args(list(argv) if argv is not None else None)
    _require_canonical_root()
    installer = AuthorityInstaller(
        filesystem_root=Path("/"),
        source_root=REPO_ROOT,
    )
    installer._ensure_directory(STATE_ROOT, 0o700)
    lock = os.open(
        LOCK_PATH,
        os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    try:
        metadata = os.fstat(lock)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise AuthorityInstallerError("installer lock is unsafe")
        fcntl.flock(lock, fcntl.LOCK_EX)
        return installer.install(
            action=arguments.action,
            candidate_sha=arguments.candidate_sha,
            candidate_tree=arguments.candidate_tree,
        )
    finally:
        os.close(lock)


def main() -> int:
    try:
        report = execute()
    except AuthorityInstallerError:
        sys.stderr.write("error: authority installer failed safely\n")
        return 1
    sys.stdout.buffer.write(_canonical(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
