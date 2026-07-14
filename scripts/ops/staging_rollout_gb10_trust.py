#!/usr/bin/env python3
"""Converge the fixed Loom staging rollout public key on every GB10 host."""

from __future__ import annotations

import argparse
import base64
import binascii
import contextlib
import errno
import fcntl
import fnmatch
import hashlib
import ipaddress
import json
import os
import re
import shlex
import stat
import subprocess
import sys
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

SSH_CONFIG_PATH = Path("/opt/loom-staging-runner/repo/deploy/worker-pools/gb10/ssh_config")
KNOWN_HOSTS_PATH = Path("/etc/loom/staging-rollout-gb10-known-hosts")
SSH_BINARY = Path("/usr/bin/ssh")
SERVICE_PRIVATE_KEY_PATH = Path("/var/lib/loom-staging-rollout/gb10-deploy-ed25519")
SERVICE_PUBLIC_KEY_PATH = Path("/var/lib/loom-staging-rollout/gb10-deploy-ed25519.pub")
REVOCATION_LEDGER_PATH = Path("/etc/loom/staging-rollout-gb10-trust-revocation.json")
LIFECYCLE_LOCK_PATH = Path("/etc/loom/staging-rollout-gb10-trust.lock")
_INHERITED_LOCK_FD_ENV = "LOOM_GB10_TRUST_LOCK_FD"
_EXPECTED_HOSTS = tuple(f"trt-gb10-{number}" for number in range(1, 16))
_EXCLUDED_HOSTS = frozenset({"trt-gb10-7"})
_ACTIVE_HOSTS = tuple(host for host in _EXPECTED_HOSTS if host not in _EXCLUDED_HOSTS)
_EXPECTED_HOSTNAMES = (
    "207.35.188.227",
    *(f"192.168.20.{number}" for number in range(12, 26)),
)
_EXPECTED_PORTS = (2221,) + (22,) * 14
_EXPECTED_REMOTE_USER = "qianyi"
_SAFE_USER_RE = re.compile(r"^[a-z_][a-z0-9_-]{0,63}$")
_PUBLIC_KEY_MAX_BYTES = 16 * 1024
_KNOWN_HOSTS_MAX_BYTES = 64 * 1024
_LEDGER_MAX_BYTES = 64 * 1024
_LEDGER_SCHEMA_VERSION = 2
_SSH_TIMEOUT_SECONDS = 30


class CommandResult(Protocol):
    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> str | bytes | None: ...

    @property
    def stderr(self) -> str | bytes | None: ...


class Runner(Protocol):
    def __call__(self, argv: Sequence[str], **kwargs: Any) -> CommandResult: ...


class TrustConfigurationError(ValueError):
    """The fixed local trust inputs are absent or malformed."""


@dataclass(frozen=True, slots=True)
class SshInventory:
    hosts: tuple[str, ...]
    active_hosts: tuple[str, ...]
    remote_user: str
    hostnames: tuple[str, ...]
    ports: tuple[int, ...]
    proxy_jumps: tuple[str | None, ...]
    identity_file: Path
    known_hosts_file: Path


@dataclass(frozen=True, slots=True)
class HostResult:
    host: str
    ok: bool
    status: str

    def to_dict(self) -> dict[str, object]:
        return {"host": self.host, "ok": self.ok, "status": self.status}


@dataclass(frozen=True, slots=True)
class _HostBlock:
    patterns: tuple[str, ...]
    options: Mapping[str, tuple[str, ...]]


@dataclass(frozen=True, slots=True)
class RevocationLedger:
    key_fingerprint: str
    topology_sha256: str
    active_policy_sha256: str
    revocation_hosts: tuple[str, ...]

    def to_bytes(self) -> bytes:
        return (
            json.dumps(
                {
                    "active_policy_sha256": self.active_policy_sha256,
                    "key_fingerprint": self.key_fingerprint,
                    "revocation_hosts": list(self.revocation_hosts),
                    "schema_version": _LEDGER_SCHEMA_VERSION,
                    "topology_sha256": self.topology_sha256,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")

    @classmethod
    def from_bytes(cls, payload: bytes) -> RevocationLedger:
        try:
            raw = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TrustConfigurationError("GB10 trust revocation ledger is invalid") from exc
        required = {
            "active_policy_sha256",
            "key_fingerprint",
            "revocation_hosts",
            "schema_version",
            "topology_sha256",
        }
        if not isinstance(raw, dict) or set(raw) != required:
            raise TrustConfigurationError("GB10 trust revocation ledger schema is invalid")
        fingerprint = raw["key_fingerprint"]
        topology_sha256 = raw["topology_sha256"]
        active_policy_sha256 = raw["active_policy_sha256"]
        hosts = raw["revocation_hosts"]
        if (
            type(raw["schema_version"]) is not int
            or raw["schema_version"] != _LEDGER_SCHEMA_VERSION
            or not isinstance(fingerprint, str)
            or re.fullmatch(r"SHA256:[A-Za-z0-9+/]{43}", fingerprint) is None
            or not isinstance(topology_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", topology_sha256) is None
            or not isinstance(active_policy_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", active_policy_sha256) is None
            or not isinstance(hosts, list)
            or any(not isinstance(host, str) for host in hosts)
            or len(hosts) != len(set(hosts))
            or any(host not in _EXPECTED_HOSTS for host in hosts)
        ):
            raise TrustConfigurationError("GB10 trust revocation ledger values are invalid")
        ordered = tuple(host for host in _EXPECTED_HOSTS if host in hosts)
        if tuple(hosts) != ordered:
            raise TrustConfigurationError("GB10 trust revocation ledger host order is invalid")
        return cls(
            key_fingerprint=fingerprint,
            topology_sha256=topology_sha256,
            active_policy_sha256=active_policy_sha256,
            revocation_hosts=ordered,
        )


@dataclass(slots=True)
class RevocationLedgerStore:
    path: Path = REVOCATION_LEDGER_PATH
    expected_uid: int = 0
    expected_gid: int = 0

    def _validate_path(self) -> Path:
        if not self.path.is_absolute() or ".." in self.path.parts:
            raise TrustConfigurationError("GB10 trust revocation ledger path is unsafe")
        parent = self.path.parent
        try:
            metadata = os.lstat(parent)
        except OSError as exc:
            raise TrustConfigurationError(
                "GB10 trust revocation ledger directory is unavailable"
            ) from exc
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != self.expected_uid
            or metadata.st_gid != self.expected_gid
            or stat.S_IMODE(metadata.st_mode) != 0o755
        ):
            raise TrustConfigurationError("GB10 trust revocation ledger directory is unsafe")
        return parent

    def load(self, *, allow_absent: bool) -> RevocationLedger | None:
        self._validate_path()
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(self.path, flags)
        except OSError as exc:
            if allow_absent and exc.errno == errno.ENOENT:
                return None
            raise TrustConfigurationError("GB10 trust revocation ledger is unavailable") from exc
        try:
            metadata = os.fstat(fd)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != self.expected_uid
                or metadata.st_gid != self.expected_gid
                or stat.S_IMODE(metadata.st_mode) != 0o600
                or metadata.st_size <= 0
                or metadata.st_size > _LEDGER_MAX_BYTES
            ):
                raise TrustConfigurationError("GB10 trust revocation ledger metadata is unsafe")
            payload = os.read(fd, _LEDGER_MAX_BYTES + 1)
            if len(payload) != metadata.st_size:
                raise TrustConfigurationError(
                    "GB10 trust revocation ledger changed while it was read"
                )
            return RevocationLedger.from_bytes(payload)
        finally:
            os.close(fd)

    def write(self, ledger: RevocationLedger) -> None:
        parent = self._validate_path()
        self.load(allow_absent=True)
        payload = ledger.to_bytes()
        if len(payload) > _LEDGER_MAX_BYTES:
            raise TrustConfigurationError("GB10 trust revocation ledger is too large")
        fd, temporary = tempfile.mkstemp(prefix=f".{self.path.name}.", dir=parent)
        try:
            os.fchmod(fd, 0o600)
            metadata = os.fstat(fd)
            if metadata.st_uid != self.expected_uid or metadata.st_gid != self.expected_gid:
                raise TrustConfigurationError(
                    "GB10 trust revocation ledger temporary owner is unsafe"
                )
            with os.fdopen(fd, "wb", closefd=True) as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            directory_fd = os.open(parent, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            if self.load(allow_absent=False) != ledger:
                raise TrustConfigurationError("GB10 trust revocation ledger write did not converge")
        except BaseException:
            try:
                os.close(fd)
            except OSError:
                pass
            Path(temporary).unlink(missing_ok=True)
            raise


@contextlib.contextmanager
def _trust_lifecycle_lock(
    path: Path,
    *,
    expected_uid: int,
    expected_gid: int,
    inherited_fd: int | None = None,
) -> Iterator[int]:
    if not path.is_absolute() or ".." in path.parts:
        raise TrustConfigurationError("GB10 trust lifecycle lock path is unsafe")
    try:
        parent_metadata = os.lstat(path.parent)
    except OSError as exc:
        raise TrustConfigurationError("GB10 trust lifecycle lock directory is unavailable") from exc
    if (
        not stat.S_ISDIR(parent_metadata.st_mode)
        or stat.S_ISLNK(parent_metadata.st_mode)
        or parent_metadata.st_uid != expected_uid
        or parent_metadata.st_gid != expected_gid
        or stat.S_IMODE(parent_metadata.st_mode) != 0o755
    ):
        raise TrustConfigurationError("GB10 trust lifecycle lock directory is unsafe")
    owned_fd = inherited_fd is None
    if inherited_fd is None:
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(path, flags, 0o600)
        except OSError as exc:
            raise TrustConfigurationError("GB10 trust lifecycle lock is unavailable") from exc
    else:
        fd = inherited_fd
    try:
        try:
            metadata = os.fstat(fd)
            current = os.lstat(path)
        except OSError as exc:
            raise TrustConfigurationError("GB10 trust lifecycle lock changed") from exc
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != expected_uid
            or metadata.st_gid != expected_gid
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or (current.st_dev, current.st_ino) != (metadata.st_dev, metadata.st_ino)
        ):
            raise TrustConfigurationError("GB10 trust lifecycle lock metadata is unsafe")
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield fd
    except OSError as exc:
        raise TrustConfigurationError("GB10 trust lifecycle lock failed") from exc
    finally:
        if owned_fd:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)


def _tokens(raw_line: str) -> list[str]:
    try:
        return shlex.split(raw_line, comments=True, posix=True)
    except ValueError as exc:
        raise TrustConfigurationError("GB10 SSH config contains invalid quoting") from exc


def _block_matches(patterns: Sequence[str], host: str) -> bool:
    positive = False
    for pattern in patterns:
        if pattern.startswith("!"):
            if fnmatch.fnmatchcase(host, pattern[1:]):
                return False
        elif fnmatch.fnmatchcase(host, pattern):
            positive = True
    return positive


def parse_ssh_inventory(
    text: str,
    *,
    require_strict_host_key_policy: bool = True,
) -> SshInventory:
    """Resolve and validate the fixed concrete GB10 SSH topology."""
    blocks: list[_HostBlock] = []
    current_patterns: tuple[str, ...] = ("*",)
    current_options: dict[str, list[str]] = {}

    def flush() -> None:
        nonlocal current_options
        blocks.append(
            _HostBlock(
                patterns=current_patterns,
                options={key: tuple(values) for key, values in current_options.items()},
            )
        )
        current_options = {}

    concrete_hosts: set[str] = set()
    saw_host = False
    for raw_line in text.splitlines():
        parts = _tokens(raw_line)
        if not parts:
            continue
        keyword = parts[0].lower()
        if keyword in {"include", "match"}:
            raise TrustConfigurationError(
                "GB10 SSH config must not delegate host or user selection"
            )
        if keyword == "host":
            if len(parts) < 2:
                raise TrustConfigurationError("GB10 SSH config has an empty Host stanza")
            flush()
            current_patterns = tuple(parts[1:])
            saw_host = True
            for pattern in current_patterns:
                if not any(character in pattern for character in "*!?[") and re.fullmatch(
                    r"trt-gb10-[0-9]+", pattern
                ):
                    concrete_hosts.add(pattern)
            continue
        if len(parts) < 2:
            raise TrustConfigurationError("GB10 SSH config contains an empty option")
        current_options.setdefault(keyword, []).append(" ".join(parts[1:]))
    flush()

    if not saw_host or concrete_hosts != set(_EXPECTED_HOSTS):
        raise TrustConfigurationError(
            "GB10 SSH config must declare exactly trt-gb10-1 through trt-gb10-15"
        )

    users: list[str] = []
    hostnames: list[str] = []
    ports: list[int] = []
    proxy_jumps: list[str | None] = []
    identity_files: list[str] = []
    for index, host in enumerate(_EXPECTED_HOSTS):
        effective: dict[str, str] = {}
        all_identity_files: list[str] = []
        for block in blocks:
            if not _block_matches(block.patterns, host):
                continue
            if block.options.get("proxycommand"):
                raise TrustConfigurationError("GB10 SSH ProxyCommand is not approved")
            for key in (
                "user",
                "hostname",
                "port",
                "proxyjump",
                "identitiesonly",
                "pubkeyauthentication",
                "passwordauthentication",
                "stricthostkeychecking",
                "userknownhostsfile",
                "globalknownhostsfile",
                "updatehostkeys",
            ):
                values = block.options.get(key, ())
                if values and key not in effective:
                    if len(values) != 1:
                        raise TrustConfigurationError(
                            f"GB10 SSH config has an ambiguous {key} selection"
                        )
                    effective[key] = values[0]
            all_identity_files.extend(block.options.get("identityfile", ()))
        effective_user = effective.get("user")
        if effective_user is None or _SAFE_USER_RE.fullmatch(effective_user) is None:
            raise TrustConfigurationError(
                "every GB10 SSH target must resolve one safe User from the checked-in config"
            )
        if effective_user != _EXPECTED_REMOTE_USER:
            raise TrustConfigurationError("GB10 SSH User policy is not approved")
        hostname = effective.get("hostname")
        try:
            if hostname is None:
                raise ValueError
            ipaddress.ip_address(hostname)
        except ValueError as exc:
            raise TrustConfigurationError(
                "every GB10 SSH target must resolve one literal IP HostName"
            ) from exc
        if hostname != _EXPECTED_HOSTNAMES[index]:
            raise TrustConfigurationError("GB10 SSH HostName policy is not approved")
        expected_port = _EXPECTED_PORTS[index]
        port_value = effective.get("port")
        if port_value != str(expected_port):
            raise TrustConfigurationError("GB10 SSH Port policy is not approved")
        if len(all_identity_files) != 1:
            raise TrustConfigurationError(
                "every GB10 SSH target must resolve exactly one IdentityFile"
            )
        if Path(all_identity_files[0]) != SERVICE_PRIVATE_KEY_PATH:
            raise TrustConfigurationError("GB10 SSH config IdentityFile is not approved")
        if (
            effective.get("identitiesonly", "").lower() != "yes"
            or effective.get("pubkeyauthentication", "").lower() != "yes"
            or effective.get("passwordauthentication", "").lower() != "no"
        ):
            raise TrustConfigurationError("GB10 SSH authentication policy is not approved")
        if require_strict_host_key_policy and (
            effective.get("stricthostkeychecking", "").lower() != "yes"
            or effective.get("userknownhostsfile") != str(KNOWN_HOSTS_PATH)
            or effective.get("globalknownhostsfile") != "/dev/null"
            or effective.get("updatehostkeys", "").lower() != "no"
        ):
            raise TrustConfigurationError("GB10 SSH host-key policy is not approved")
        expected_jump = None if host == _EXPECTED_HOSTS[0] else "trt-gb10-1"
        proxy_jump = effective.get("proxyjump")
        if proxy_jump != expected_jump:
            raise TrustConfigurationError("GB10 SSH ProxyJump policy is not approved")
        users.append(effective_user)
        hostnames.append(hostname)
        ports.append(expected_port)
        proxy_jumps.append(proxy_jump)
        identity_files.append(all_identity_files[0])
    if len(set(users)) != 1:
        raise TrustConfigurationError("all GB10 SSH targets must use the same remote User")
    if len(set(hostnames)) != len(_EXPECTED_HOSTS) or len(set(identity_files)) != 1:
        raise TrustConfigurationError("GB10 SSH target resolution is ambiguous")
    return SshInventory(
        hosts=_EXPECTED_HOSTS,
        active_hosts=_ACTIVE_HOSTS,
        remote_user=users[0],
        hostnames=tuple(hostnames),
        ports=tuple(ports),
        proxy_jumps=tuple(proxy_jumps),
        identity_file=SERVICE_PRIVATE_KEY_PATH,
        known_hosts_file=KNOWN_HOSTS_PATH,
    )


def _topology_sha256(inventory: SshInventory) -> str:
    payload = json.dumps(
        [
            {
                "host": host,
                "hostname": inventory.hostnames[index],
                "identity_file": str(inventory.identity_file),
                "known_hosts_file": str(inventory.known_hosts_file),
                "port": inventory.ports[index],
                "proxy_jump": inventory.proxy_jumps[index],
                "remote_user": inventory.remote_user,
            }
            for index, host in enumerate(inventory.hosts)
        ],
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _active_policy_sha256(inventory: SshInventory) -> str:
    payload = json.dumps(
        {"active_hosts": list(inventory.active_hosts)},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _validate_legacy_topology(
    current: SshInventory,
    previous: SshInventory,
) -> None:
    if _topology_sha256(current) != _topology_sha256(previous):
        raise TrustConfigurationError(
            "legacy GB10 trust topology drifted from the previous installed source"
        )


def _read_bounded_regular_file(path: Path) -> bytes:
    if not path.is_absolute() or ".." in path.parts:
        raise TrustConfigurationError("fixed trust input must be an absolute path")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise TrustConfigurationError("fixed trust input is not a readable regular file") from exc
    try:
        metadata = os.fstat(fd)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size <= 0
            or metadata.st_size > _PUBLIC_KEY_MAX_BYTES
            or stat.S_IMODE(metadata.st_mode) & 0o022
        ):
            raise TrustConfigurationError("fixed trust input metadata is not approved")
        payload = os.read(fd, _PUBLIC_KEY_MAX_BYTES + 1)
        if len(payload) != metadata.st_size:
            raise TrustConfigurationError("fixed trust input changed while it was read")
        return payload
    finally:
        os.close(fd)


def _read_known_hosts_authority(
    path: Path,
    *,
    expected_uid: int,
    expected_gid: int,
) -> bytes:
    if not path.is_absolute() or ".." in path.parts:
        raise TrustConfigurationError("GB10 known-hosts path is unsafe")
    try:
        parent = os.lstat(path.parent)
    except OSError as exc:
        raise TrustConfigurationError("GB10 known-hosts directory is unavailable") from exc
    if (
        not stat.S_ISDIR(parent.st_mode)
        or stat.S_ISLNK(parent.st_mode)
        or parent.st_uid != expected_uid
        or parent.st_gid != expected_gid
        or stat.S_IMODE(parent.st_mode) != 0o755
    ):
        raise TrustConfigurationError("GB10 known-hosts directory is unsafe")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise TrustConfigurationError("GB10 known-hosts authority is unavailable") from exc
    try:
        metadata = os.fstat(fd)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != expected_uid
            or metadata.st_gid != expected_gid
            or stat.S_IMODE(metadata.st_mode) != 0o644
            or metadata.st_size <= 0
            or metadata.st_size > _KNOWN_HOSTS_MAX_BYTES
        ):
            raise TrustConfigurationError("GB10 known-hosts authority metadata is unsafe")
        payload = os.read(fd, _KNOWN_HOSTS_MAX_BYTES + 1)
        if len(payload) != metadata.st_size:
            raise TrustConfigurationError("GB10 known-hosts authority changed while read")
        try:
            current = os.lstat(path)
        except OSError as exc:
            raise TrustConfigurationError("GB10 known-hosts authority changed while read") from exc
        if (current.st_dev, current.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise TrustConfigurationError("GB10 known-hosts authority changed while read")
    finally:
        os.close(fd)
    _validate_known_hosts_authority(payload)
    return payload


def _validate_known_hosts_authority(payload: bytes) -> None:
    try:
        lines = payload.decode("ascii").splitlines()
    except UnicodeDecodeError as exc:
        raise TrustConfigurationError("GB10 known-hosts authority must be ASCII") from exc
    entries = [line for line in lines if line and not line.startswith("#")]
    expected_hosts = (
        "[207.35.188.227]:2221,trt-gb10-1",
        *(f"192.168.20.{number + 10},trt-gb10-{number}" for number in range(2, 16)),
    )
    if len(entries) != len(expected_hosts):
        raise TrustConfigurationError("GB10 known-hosts authority must contain exactly 15 hosts")
    observed: list[str] = []
    for line in entries:
        fields = line.split()
        if len(fields) != 3 or fields[1] != "ssh-ed25519":
            raise TrustConfigurationError("GB10 known-hosts authority contains an invalid entry")
        _decoded_ed25519_blob((fields[1] + " " + fields[2] + "\n").encode("ascii"))
        observed.append(fields[0])
    if tuple(observed) != expected_hosts:
        raise TrustConfigurationError("GB10 known-hosts authority host coverage is invalid")


def _decoded_ed25519_blob(payload: bytes) -> bytes:
    try:
        line = payload.decode("ascii").strip()
    except UnicodeDecodeError as exc:
        raise TrustConfigurationError("service public key must be ASCII") from exc
    if "\n" in line or "\r" in line:
        raise TrustConfigurationError("service public key must contain exactly one line")
    parts = line.split()
    if len(parts) < 2 or parts[0] != "ssh-ed25519":
        raise TrustConfigurationError("service public key must be an Ed25519 public key")
    try:
        decoded = base64.b64decode(parts[1], validate=True)
    except (ValueError, binascii.Error) as exc:
        raise TrustConfigurationError("service public key encoding is invalid") from exc
    algorithm = b"ssh-ed25519"
    expected_prefix = len(algorithm).to_bytes(4, "big") + algorithm
    if not decoded.startswith(expected_prefix):
        raise TrustConfigurationError("service public key blob has the wrong algorithm")
    offset = len(expected_prefix)
    if len(decoded) < offset + 4:
        raise TrustConfigurationError("service public key blob is truncated")
    key_length = int.from_bytes(decoded[offset : offset + 4], "big")
    if key_length != 32 or len(decoded) != offset + 4 + key_length:
        raise TrustConfigurationError("service public key blob is not a valid Ed25519 key")
    return decoded


def _key_fingerprint(public_key: bytes) -> str:
    digest = hashlib.sha256(_decoded_ed25519_blob(public_key)).digest()
    encoded = base64.b64encode(digest).decode("ascii").rstrip("=")
    return f"SHA256:{encoded}"


def _validate_bootstrap_identity(path: Path) -> None:
    if not path.is_absolute() or ".." in path.parts:
        raise TrustConfigurationError("bootstrap identity must be an absolute path")
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise TrustConfigurationError("bootstrap identity is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_size <= 0
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise TrustConfigurationError("bootstrap identity metadata is not approved")


_REMOTE_SCRIPT = r"""
import base64
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
from pathlib import Path

COMMENT = "loom-staging-rollout"
TOMBSTONE_COMMENT = "loom-staging-rollout-revoked"
REVOKED_RECEIPT = '{"status":"revoked"}'

def fail(message, code=2):
    print(json.dumps({"status": message}, sort_keys=True, separators=(",", ":")))
    raise SystemExit(code)

def decoded_blob(payload):
    try:
        line = payload.decode("ascii").strip()
    except UnicodeDecodeError:
        fail("invalid-public-key")
    if "\n" in line or "\r" in line:
        fail("invalid-public-key")
    parts = line.split()
    if len(parts) < 2 or parts[0] != "ssh-ed25519":
        fail("invalid-public-key")
    try:
        blob = base64.b64decode(parts[1], validate=True)
    except Exception:
        fail("invalid-public-key")
    algorithm = b"ssh-ed25519"
    prefix = len(algorithm).to_bytes(4, "big") + algorithm
    if not blob.startswith(prefix):
        fail("invalid-public-key")
    offset = len(prefix)
    if len(blob) < offset + 4:
        fail("invalid-public-key")
    key_length = int.from_bytes(blob[offset:offset + 4], "big")
    if key_length != 32 or len(blob) != offset + 4 + key_length:
        fail("invalid-public-key")
    return blob, parts[1]

def fingerprint(blob):
    digest = base64.b64encode(hashlib.sha256(blob).digest()).decode("ascii").rstrip("=")
    return "SHA256:" + digest

def quote_authorized_option(value):
    return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'

def normal_line(key_base64):
    return "ssh-ed25519 " + key_base64 + " " + COMMENT

def tombstone_line(key_base64):
    command = "/usr/bin/printf '%s\\n' '" + REVOKED_RECEIPT + "'"
    options = "restrict,command=" + quote_authorized_option(command)
    return options + " ssh-ed25519 " + key_base64 + " " + TOMBSTONE_COMMENT

def authorized_key_fields(line):
    fields = []
    start = None
    quoted = False
    escaped = False
    for index, character in enumerate(line):
        if start is None:
            if character.isspace():
                continue
            start = index
        if escaped:
            escaped = False
            continue
        if character == "\\":
            escaped = True
            continue
        if character == '"':
            quoted = not quoted
            continue
        if character.isspace() and not quoted:
            fields.append((line[start:index], start, index))
            start = None
    if quoted or escaped:
        return []
    if start is not None:
        fields.append((line[start:], start, len(line)))
    return fields

def line_identity(line):
    fields = authorized_key_fields(line)
    key_index = next(
        (
            index
            for index, field in enumerate(fields)
            if field[0].startswith(("ssh-", "ecdsa-", "sk-"))
        ),
        None,
    )
    if key_index is None or key_index + 1 >= len(fields):
        return None
    key_type = fields[key_index][0]
    key_end = fields[key_index + 1][2]
    try:
        blob = base64.b64decode(fields[key_index + 1][0], validate=True)
    except Exception:
        return None, key_end
    algorithm = key_type.encode("ascii", errors="ignore")
    prefix = len(algorithm).to_bytes(4, "big") + algorithm
    if not algorithm or not blob.startswith(prefix):
        return None, key_end
    return fingerprint(blob), key_end

def load_authorized_keys(path):
    if not os.path.lexists(path):
        return "", []
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        fail("unsafe-authorized-keys")
    if metadata.st_size > 4 * 1024 * 1024:
        fail("oversized-authorized-keys")
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        fail("unreadable-authorized-keys")
    return text, text.splitlines()

def ensure_ssh_directory(path):
    if os.path.lexists(path):
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            fail("unsafe-ssh-directory")
    else:
        path.mkdir(mode=0o700)
    path.chmod(0o700)

def atomic_write(path, text):
    fd, temp_name = tempfile.mkstemp(prefix=".authorized_keys.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", closefd=True) as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass

operation = sys.argv[1] if len(sys.argv) == 2 else ""
if operation not in {"bootstrap", "check", "revoke"}:
    fail("invalid-operation")
payload = sys.stdin.buffer.read(16385)
if not payload or len(payload) > 16384:
    fail("invalid-public-key")
target_blob, target_base64 = decoded_blob(payload)
target_fingerprint = fingerprint(target_blob)
ssh_dir = Path.home() / ".ssh"
authorized_keys = ssh_dir / "authorized_keys"

if os.path.lexists(ssh_dir):
    metadata = ssh_dir.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        fail("unsafe-ssh-directory")
text, lines = load_authorized_keys(authorized_keys)
matches = []
marked = []
for index, line in enumerate(lines):
    identity = line_identity(line)
    if identity is None:
        continue
    line_fingerprint, key_end = identity
    if line_fingerprint == target_fingerprint:
        matches.append((index, key_end))
    if line[key_end:].strip() in {COMMENT, TOMBSTONE_COMMENT}:
        marked.append((index, line_fingerprint))
if len(matches) > 1:
    fail("ambiguous-fingerprint", 3)
if any(line_fingerprint != target_fingerprint for _, line_fingerprint in marked):
    fail("ambiguous-marker", 3)
if len(marked) > 1:
    fail("ambiguous-marker", 3)
expected_normal = normal_line(target_base64)
expected_tombstone = tombstone_line(target_base64)

if operation == "check":
    if not os.path.lexists(ssh_dir) or stat.S_IMODE(ssh_dir.stat().st_mode) != 0o700:
        fail("incorrect-ssh-directory-mode", 4)
    if not os.path.lexists(authorized_keys) or stat.S_IMODE(authorized_keys.stat().st_mode) != 0o600:
        fail("incorrect-authorized-keys-mode", 4)
    if not matches:
        fail("absent", 4)
    index, _ = matches[0]
    if lines[index].strip() == expected_tombstone:
        fail("revoked", 4)
    if lines[index].strip() != expected_normal:
        fail("incorrect-comment", 4)
    print(json.dumps({"status": "present"}, sort_keys=True, separators=(",", ":")))
    raise SystemExit(0)

if operation == "bootstrap":
    ensure_ssh_directory(ssh_dir)
    if matches:
        index, _ = matches[0]
        was_tombstoned = lines[index].strip() == expected_tombstone
        replacement = expected_normal
        changed = replacement != lines[index]
        lines[index] = replacement
        status_value = "restored" if was_tombstoned else ("updated" if changed else "already-present")
    else:
        lines.append(expected_normal)
        changed = True
        status_value = "installed"
    rendered = "\n".join(lines) + "\n"
    if changed or not os.path.lexists(authorized_keys):
        atomic_write(authorized_keys, rendered)
    authorized_keys.chmod(0o600)
    print(json.dumps({"status": status_value}, sort_keys=True, separators=(",", ":")))
    raise SystemExit(0)

if not matches:
    print(json.dumps({"status": "absent"}, sort_keys=True, separators=(",", ":")))
    raise SystemExit(0)
index, _ = matches[0]
if lines[index].strip() == expected_tombstone:
    print(json.dumps({"status": "revoked"}, sort_keys=True, separators=(",", ":")))
    raise SystemExit(0)
if lines[index].strip() != expected_normal:
    fail("incorrect-managed-key", 4)
lines[index] = expected_tombstone
ensure_ssh_directory(ssh_dir)
atomic_write(authorized_keys, "\n".join(lines) + "\n")
authorized_keys.chmod(0o600)
print(json.dumps({"status": "revoked"}, sort_keys=True, separators=(",", ":")))
"""


_SUCCESS_STATUSES = {
    "bootstrap": frozenset({"installed", "updated", "restored", "already-present"}),
    "check": frozenset({"present"}),
    "revoke": frozenset({"revoked", "absent"}),
}


def _as_bytes(value: str | bytes | None) -> bytes:
    if value is None:
        return b""
    return value if isinstance(value, bytes) else value.encode("utf-8", errors="replace")


def _safe_remote_status(stdout: str | bytes | None, operation: str) -> str | None:
    payload = _as_bytes(stdout)
    if len(payload) > 1024:
        return None
    try:
        parsed = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(parsed, dict) or set(parsed) != {"status"}:
        return None
    status_value = parsed.get("status")
    if not isinstance(status_value, str) or status_value not in _SUCCESS_STATUSES[operation]:
        return None
    return status_value


def _ssh_argv(
    *,
    host: str,
    operation: str,
    ssh_config: Path,
    bootstrap_identity: Path | None,
) -> list[str]:
    remote_command = shlex.join(["python3", "-c", _REMOTE_SCRIPT, operation])
    argv = [
        str(SSH_BINARY),
        "-F",
        str(ssh_config),
        "-o",
        "BatchMode=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "ControlMaster=no",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={KNOWN_HOSTS_PATH}",
        "-o",
        "GlobalKnownHostsFile=/dev/null",
        "-o",
        "UpdateHostKeys=no",
    ]
    if bootstrap_identity is not None:
        argv.extend(["-i", str(bootstrap_identity)])
    return [*argv, host, remote_command]


def converge_trust(
    operation: str,
    *,
    hosts: Sequence[str],
    ssh_config: Path,
    public_key: bytes,
    run: Runner,
    bootstrap_identity: Path | None = None,
) -> tuple[HostResult, ...]:
    """Run one fixed trust operation on the supplied fixed hosts."""
    if operation not in _SUCCESS_STATUSES:
        raise ValueError("unsupported trust operation")
    results: list[HostResult] = []
    for host in hosts:
        argv = _ssh_argv(
            host=host,
            operation=operation,
            ssh_config=ssh_config,
            bootstrap_identity=bootstrap_identity,
        )
        try:
            completed = run(
                argv,
                input=public_key,
                capture_output=True,
                text=False,
                check=False,
                timeout=_SSH_TIMEOUT_SECONDS,
            )
        except (OSError, subprocess.SubprocessError):
            results.append(HostResult(host=host, ok=False, status="transport-failed"))
            continue
        status_value = (
            _safe_remote_status(completed.stdout, operation) if completed.returncode == 0 else None
        )
        if status_value is None:
            results.append(HostResult(host=host, ok=False, status="remote-failed"))
        else:
            results.append(HostResult(host=host, ok=True, status=status_value))
    return tuple(results)


def _validate_ledger_binding(
    ledger: RevocationLedger,
    *,
    inventory: SshInventory,
    key_fingerprint: str,
    require_active_policy: bool = True,
) -> None:
    if ledger.key_fingerprint != key_fingerprint:
        raise TrustConfigurationError("GB10 trust revocation ledger key binding is invalid")
    if ledger.topology_sha256 != _topology_sha256(inventory):
        raise TrustConfigurationError("GB10 trust revocation ledger topology binding is invalid")
    if require_active_policy and ledger.active_policy_sha256 != _active_policy_sha256(inventory):
        raise TrustConfigurationError(
            "GB10 trust revocation ledger active-policy binding is invalid"
        )


def _initialize_ledger(
    store: RevocationLedgerStore,
    *,
    inventory: SshInventory,
    key_fingerprint: str,
) -> RevocationLedger:
    ledger = store.load(allow_absent=True)
    if ledger is None:
        ledger = RevocationLedger(
            key_fingerprint=key_fingerprint,
            topology_sha256=_topology_sha256(inventory),
            active_policy_sha256=_active_policy_sha256(inventory),
            revocation_hosts=(),
        )
        store.write(ledger)
    _validate_ledger_binding(
        ledger,
        inventory=inventory,
        key_fingerprint=key_fingerprint,
    )
    return ledger


def _migrate_active_policy(
    store: RevocationLedgerStore,
    *,
    ledger: RevocationLedger,
    inventory: SshInventory,
    key_fingerprint: str,
) -> RevocationLedger:
    _validate_ledger_binding(
        ledger,
        inventory=inventory,
        key_fingerprint=key_fingerprint,
        require_active_policy=False,
    )
    updated = RevocationLedger(
        key_fingerprint=ledger.key_fingerprint,
        topology_sha256=ledger.topology_sha256,
        active_policy_sha256=_active_policy_sha256(inventory),
        revocation_hosts=ledger.revocation_hosts,
    )
    if updated != ledger:
        store.write(updated)
    return updated


def _load_bound_ledger(
    store: RevocationLedgerStore,
    *,
    inventory: SshInventory,
    key_fingerprint: str,
) -> RevocationLedger:
    ledger = store.load(allow_absent=False)
    if ledger is None:  # pragma: no cover - allow_absent=False owns this invariant
        raise TrustConfigurationError("GB10 trust revocation ledger is unavailable")
    _validate_ledger_binding(
        ledger,
        inventory=inventory,
        key_fingerprint=key_fingerprint,
    )
    return ledger


def _register_revocation_hosts(
    store: RevocationLedgerStore,
    *,
    ledger: RevocationLedger,
    hosts: Sequence[str],
) -> RevocationLedger:
    if not set(hosts).issubset(_EXPECTED_HOSTS):
        raise TrustConfigurationError("GB10 trust revocation host registration is invalid")
    requested = set(ledger.revocation_hosts)
    requested.update(hosts)
    updated = RevocationLedger(
        key_fingerprint=ledger.key_fingerprint,
        topology_sha256=ledger.topology_sha256,
        active_policy_sha256=ledger.active_policy_sha256,
        revocation_hosts=tuple(host for host in _EXPECTED_HOSTS if host in requested),
    )
    if updated != ledger:
        store.write(updated)
    return updated


def revoke_trust(
    *,
    inventory: SshInventory,
    ssh_config: Path,
    public_key: bytes,
    run: Runner,
    store: RevocationLedgerStore,
    ledger: RevocationLedger,
) -> tuple[HostResult, ...]:
    """Revoke private hosts durably before removing the jump-host trust."""
    results: list[HostResult] = []
    jump_host = inventory.hosts[0]
    private_hosts = tuple(host for host in ledger.revocation_hosts if host != jump_host)
    for host in private_hosts:
        result = converge_trust(
            "revoke",
            hosts=(host,),
            ssh_config=ssh_config,
            public_key=public_key,
            run=run,
        )[0]
        results.append(result)
        if not result.ok:
            continue
        ledger = RevocationLedger(
            key_fingerprint=ledger.key_fingerprint,
            topology_sha256=ledger.topology_sha256,
            active_policy_sha256=ledger.active_policy_sha256,
            revocation_hosts=tuple(
                candidate for candidate in ledger.revocation_hosts if candidate != host
            ),
        )
        store.write(ledger)

    remaining_private = tuple(host for host in ledger.revocation_hosts if host != jump_host)
    if jump_host in ledger.revocation_hosts and remaining_private:
        results.append(HostResult(host=jump_host, ok=False, status="dependency-failed"))
    elif jump_host in ledger.revocation_hosts:
        result = converge_trust(
            "revoke",
            hosts=(jump_host,),
            ssh_config=ssh_config,
            public_key=public_key,
            run=run,
        )[0]
        results.append(result)
        if result.ok:
            store.write(
                RevocationLedger(
                    key_fingerprint=ledger.key_fingerprint,
                    topology_sha256=ledger.topology_sha256,
                    active_policy_sha256=ledger.active_policy_sha256,
                    revocation_hosts=(),
                )
            )
    return tuple(results)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="operation", required=True)
    bootstrap = subparsers.add_parser("bootstrap", allow_abbrev=False)
    bootstrap.add_argument("--bootstrap-identity", type=Path, required=True)
    subparsers.add_parser("check", allow_abbrev=False)
    subparsers.add_parser("revoke", allow_abbrev=False)
    subparsers.add_parser("initialize-ledger", allow_abbrev=False)
    subparsers.add_parser("register-legacy-ledger", allow_abbrev=False)
    subparsers.add_parser("validate-ledger", allow_abbrev=False)
    subparsers.add_parser("migrate-active-policy", allow_abbrev=False)
    subparsers.add_parser("finalize-check", allow_abbrev=False)
    legacy_topology = subparsers.add_parser(
        "validate-legacy-topology",
        allow_abbrev=False,
    )
    legacy_topology.add_argument("--previous-config", type=Path, required=True)
    return parser


def _subprocess_runner(argv: Sequence[str], **kwargs: Any) -> CommandResult:
    return subprocess.run(list(argv), **kwargs)


def main(
    argv: Sequence[str] | None = None,
    *,
    run: Runner = _subprocess_runner,
    ssh_config_path: Path = SSH_CONFIG_PATH,
    known_hosts_path: Path = KNOWN_HOSTS_PATH,
    public_key_path: Path = SERVICE_PUBLIC_KEY_PATH,
    ledger_path: Path = REVOCATION_LEDGER_PATH,
    lock_path: Path = LIFECYCLE_LOCK_PATH,
    expected_uid: int = 0,
    expected_gid: int = 0,
    _lock_held: bool = False,
    _installer_lock_authority: bool = False,
) -> int:
    args = _parser().parse_args(argv)
    if not _lock_held:
        raw_inherited_fd = os.environ.get(_INHERITED_LOCK_FD_ENV)
        inherited_fd: int | None = None
        if raw_inherited_fd is not None:
            if not raw_inherited_fd.isdigit():
                print("error: inherited GB10 trust lifecycle lock is invalid", file=sys.stderr)
                return 2
            inherited_fd = int(raw_inherited_fd)
        try:
            with _trust_lifecycle_lock(
                lock_path,
                expected_uid=expected_uid,
                expected_gid=expected_gid,
                inherited_fd=inherited_fd,
            ):
                return main(
                    argv,
                    run=run,
                    ssh_config_path=ssh_config_path,
                    known_hosts_path=known_hosts_path,
                    public_key_path=public_key_path,
                    ledger_path=ledger_path,
                    lock_path=lock_path,
                    expected_uid=expected_uid,
                    expected_gid=expected_gid,
                    _lock_held=True,
                    _installer_lock_authority=inherited_fd is not None,
                )
        except TrustConfigurationError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
    try:
        config_payload = _read_bounded_regular_file(ssh_config_path)
        inventory = parse_ssh_inventory(config_payload.decode("utf-8"))
        if args.operation in {"bootstrap", "check", "revoke"}:
            _read_known_hosts_authority(
                known_hosts_path,
                expected_uid=expected_uid,
                expected_gid=expected_gid,
            )
        if args.operation == "validate-legacy-topology":
            previous_payload = _read_bounded_regular_file(args.previous_config)
            previous_inventory = parse_ssh_inventory(
                previous_payload.decode("utf-8"),
                require_strict_host_key_policy=False,
            )
            _validate_legacy_topology(inventory, previous_inventory)
            print(
                json.dumps(
                    {
                        "action": args.operation,
                        "ok": True,
                        "topology_sha256": _topology_sha256(inventory),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
            return 0
        public_key = _read_bounded_regular_file(public_key_path)
        key_fingerprint = _key_fingerprint(public_key)
        store = RevocationLedgerStore(
            path=ledger_path,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
        )
        bootstrap_identity = getattr(args, "bootstrap_identity", None)
        if bootstrap_identity is not None:
            _validate_bootstrap_identity(bootstrap_identity)
        if args.operation == "initialize-ledger":
            ledger = _initialize_ledger(
                store,
                inventory=inventory,
                key_fingerprint=key_fingerprint,
            )
            results: tuple[HostResult, ...] = ()
        elif args.operation == "register-legacy-ledger":
            ledger = _initialize_ledger(
                store,
                inventory=inventory,
                key_fingerprint=key_fingerprint,
            )
            ledger = _register_revocation_hosts(
                store,
                ledger=ledger,
                hosts=inventory.hosts,
            )
            results = ()
        elif args.operation == "validate-ledger":
            ledger = _load_bound_ledger(
                store,
                inventory=inventory,
                key_fingerprint=key_fingerprint,
            )
            results = ()
        elif args.operation == "migrate-active-policy":
            if not _installer_lock_authority:
                raise TrustConfigurationError(
                    "active-policy migration requires merged installer authority"
                )
            existing_ledger = store.load(allow_absent=False)
            if existing_ledger is None:  # pragma: no cover - allow_absent=False owns this invariant
                raise TrustConfigurationError("GB10 trust revocation ledger is unavailable")
            ledger = _migrate_active_policy(
                store,
                ledger=existing_ledger,
                inventory=inventory,
                key_fingerprint=key_fingerprint,
            )
            results = ()
        else:
            ledger = _load_bound_ledger(
                store,
                inventory=inventory,
                key_fingerprint=key_fingerprint,
            )
            if args.operation == "bootstrap":
                ledger = _register_revocation_hosts(
                    store,
                    ledger=ledger,
                    hosts=inventory.active_hosts,
                )
                results = converge_trust(
                    "bootstrap",
                    hosts=inventory.active_hosts,
                    ssh_config=ssh_config_path,
                    public_key=public_key,
                    run=run,
                    bootstrap_identity=bootstrap_identity,
                )
            elif args.operation == "check":
                if not set(inventory.active_hosts).issubset(ledger.revocation_hosts):
                    raise TrustConfigurationError(
                        "GB10 trust revocation ledger is missing active hosts"
                    )
                results = converge_trust(
                    "check",
                    hosts=inventory.active_hosts,
                    ssh_config=ssh_config_path,
                    public_key=public_key,
                    run=run,
                )
            elif args.operation == "revoke":
                results = revoke_trust(
                    inventory=inventory,
                    ssh_config=ssh_config_path,
                    public_key=public_key,
                    run=run,
                    store=store,
                    ledger=ledger,
                )
                ledger = _load_bound_ledger(
                    store,
                    inventory=inventory,
                    key_fingerprint=key_fingerprint,
                )
            else:
                if ledger.revocation_hosts:
                    raise TrustConfigurationError(
                        "GB10 trust revocation ledger still has managed hosts"
                    )
                results = ()
    except (TrustConfigurationError, UnicodeDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    ok = all(result.ok for result in results) and (
        args.operation != "revoke" or not ledger.revocation_hosts
    )
    print(
        json.dumps(
            {
                "action": args.operation,
                "hosts": [result.to_dict() for result in results],
                "ledger_hosts_remaining": len(ledger.revocation_hosts),
                "ok": ok,
                "remote_user": inventory.remote_user,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
