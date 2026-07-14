#!/usr/bin/env python3
"""Converge the fixed Loom staging rollout public key on every GB10 host."""

from __future__ import annotations

import argparse
import base64
import binascii
import fnmatch
import ipaddress
import json
import os
import re
import shlex
import stat
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

SSH_CONFIG_PATH = Path("/opt/loom-staging-runner/repo/deploy/worker-pools/gb10/ssh_config")
SERVICE_PRIVATE_KEY_PATH = Path("/var/lib/loom-staging-rollout/gb10-deploy-ed25519")
SERVICE_PUBLIC_KEY_PATH = Path("/var/lib/loom-staging-rollout/gb10-deploy-ed25519.pub")
_EXPECTED_HOSTS = tuple(f"trt-gb10-{number}" for number in range(1, 16))
_SAFE_USER_RE = re.compile(r"^[a-z_][a-z0-9_-]{0,63}$")
_PUBLIC_KEY_MAX_BYTES = 16 * 1024
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
    remote_user: str
    hostnames: tuple[str, ...]
    proxy_jumps: tuple[str | None, ...]
    identity_file: Path


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


def parse_ssh_inventory(text: str) -> SshInventory:
    """Resolve the exact concrete GB10 aliases and their effective SSH User."""
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
    proxy_jumps: list[str | None] = []
    identity_files: list[str] = []
    for host in _EXPECTED_HOSTS:
        effective: dict[str, str] = {}
        all_identity_files: list[str] = []
        for block in blocks:
            if not _block_matches(block.patterns, host):
                continue
            for key in (
                "user",
                "hostname",
                "proxyjump",
                "identitiesonly",
                "pubkeyauthentication",
                "passwordauthentication",
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
        hostname = effective.get("hostname")
        try:
            if hostname is None:
                raise ValueError
            ipaddress.ip_address(hostname)
        except ValueError as exc:
            raise TrustConfigurationError(
                "every GB10 SSH target must resolve one literal IP HostName"
            ) from exc
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
        expected_jump = None if host in _EXPECTED_HOSTS[:10] else "trt-gb10-1"
        proxy_jump = effective.get("proxyjump")
        if proxy_jump != expected_jump:
            raise TrustConfigurationError("GB10 SSH ProxyJump policy is not approved")
        users.append(effective_user)
        hostnames.append(hostname)
        proxy_jumps.append(proxy_jump)
        identity_files.append(all_identity_files[0])
    if len(set(users)) != 1:
        raise TrustConfigurationError("all GB10 SSH targets must use the same remote User")
    if len(set(hostnames)) != len(_EXPECTED_HOSTS) or len(set(identity_files)) != 1:
        raise TrustConfigurationError("GB10 SSH target resolution is ambiguous")
    return SshInventory(
        hosts=_EXPECTED_HOSTS,
        remote_user=users[0],
        hostnames=tuple(hostnames),
        proxy_jumps=tuple(proxy_jumps),
        identity_file=SERVICE_PRIVATE_KEY_PATH,
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
    if line[key_end:].strip() == COMMENT:
        marked.append((index, line_fingerprint))
if len(matches) > 1:
    fail("ambiguous-fingerprint", 3)
if any(line_fingerprint != target_fingerprint for _, line_fingerprint in marked):
    fail("ambiguous-marker", 3)
if len(marked) > 1:
    fail("ambiguous-marker", 3)

if operation == "check":
    if not os.path.lexists(ssh_dir) or stat.S_IMODE(ssh_dir.stat().st_mode) != 0o700:
        fail("incorrect-ssh-directory-mode", 4)
    if not os.path.lexists(authorized_keys) or stat.S_IMODE(authorized_keys.stat().st_mode) != 0o600:
        fail("incorrect-authorized-keys-mode", 4)
    if not matches:
        fail("absent", 4)
    index, key_end = matches[0]
    if lines[index][key_end:].strip() != COMMENT:
        fail("incorrect-comment", 4)
    print(json.dumps({"status": "present"}, sort_keys=True, separators=(",", ":")))
    raise SystemExit(0)

if operation == "bootstrap":
    ensure_ssh_directory(ssh_dir)
    if matches:
        index, key_end = matches[0]
        replacement = lines[index][:key_end].rstrip() + " " + COMMENT
        changed = replacement != lines[index]
        lines[index] = replacement
        status_value = "updated" if changed else "already-present"
    else:
        lines.append("ssh-ed25519 " + target_base64 + " " + COMMENT)
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
del lines[index]
ensure_ssh_directory(ssh_dir)
atomic_write(authorized_keys, ("\n".join(lines) + "\n") if lines else "")
authorized_keys.chmod(0o600)
print(json.dumps({"status": "revoked"}, sort_keys=True, separators=(",", ":")))
"""


_SUCCESS_STATUSES = {
    "bootstrap": frozenset({"installed", "updated", "already-present"}),
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
        "ssh",
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
    ]
    if bootstrap_identity is not None:
        argv.extend(["-i", str(bootstrap_identity)])
    return [*argv, host, remote_command]


def converge_trust(
    operation: str,
    *,
    inventory: SshInventory,
    ssh_config: Path,
    public_key: bytes,
    run: Runner,
    bootstrap_identity: Path | None = None,
) -> tuple[HostResult, ...]:
    """Run one fixed trust operation on all hosts and retain only safe status."""
    if operation not in _SUCCESS_STATUSES:
        raise ValueError("unsupported trust operation")
    results: list[HostResult] = []
    for host in inventory.hosts:
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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="operation", required=True)
    bootstrap = subparsers.add_parser("bootstrap", allow_abbrev=False)
    bootstrap.add_argument("--bootstrap-identity", type=Path, required=True)
    subparsers.add_parser("check", allow_abbrev=False)
    subparsers.add_parser("revoke", allow_abbrev=False)
    return parser


def _subprocess_runner(argv: Sequence[str], **kwargs: Any) -> CommandResult:
    return subprocess.run(list(argv), **kwargs)


def main(
    argv: Sequence[str] | None = None,
    *,
    run: Runner = _subprocess_runner,
    ssh_config_path: Path = SSH_CONFIG_PATH,
    public_key_path: Path = SERVICE_PUBLIC_KEY_PATH,
) -> int:
    args = _parser().parse_args(argv)
    try:
        config_payload = _read_bounded_regular_file(ssh_config_path)
        inventory = parse_ssh_inventory(config_payload.decode("utf-8"))
        public_key = _read_bounded_regular_file(public_key_path)
        _decoded_ed25519_blob(public_key)
        bootstrap_identity = getattr(args, "bootstrap_identity", None)
        if bootstrap_identity is not None:
            _validate_bootstrap_identity(bootstrap_identity)
    except (TrustConfigurationError, UnicodeDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    results = converge_trust(
        args.operation,
        inventory=inventory,
        ssh_config=ssh_config_path,
        public_key=public_key,
        run=run,
        bootstrap_identity=bootstrap_identity,
    )
    ok = all(result.ok for result in results)
    print(
        json.dumps(
            {
                "action": args.operation,
                "hosts": [result.to_dict() for result in results],
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
