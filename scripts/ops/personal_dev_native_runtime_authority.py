#!/usr/bin/python3
"""Install and enforce the fixed personal-dev native-runtime root authority.

The installed runtime command accepts no arguments.  It reads one bounded,
typed request from stdin and can operate only the dedicated personal-builder
runtime.  The separate ``bootstrap`` command must be run directly by an
external root administrator from the fixed root-owned sealed checkout; it is
intentionally absent from the installed sudoers rule.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import importlib.util
import ipaddress
import json
import os
import pwd
import re
import stat
import subprocess
import sys
import tempfile
import time
import urllib.parse
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, cast

APPROVED_BASE_SHA: Final = "5367958b2f32fd440771d4a820c7323140892284"
OPERATOR: Final = "qianyi"
SOURCE_ROOT: Final = Path("/opt/loom-personal-dev-native-runtime-authority/source")
LIBEXEC: Final = Path("/usr/local/libexec/loom-personal-dev-native-runtime-authority")
VALIDATOR: Final = Path("/usr/local/libexec/loom-personal-dev-native-runtime-sealed-source.py")
POLICY: Final = Path("/etc/loom/personal-dev-native-runtime-authority.json")
SUDOERS: Final = Path("/etc/sudoers.d/loom-personal-dev-native-runtime-authority")
STATE_ROOT: Final = Path("/var/lib/loom-personal-dev-native-runtime-authority")
LOCK: Final = STATE_ROOT / "authority.lock"
JOURNAL: Final = STATE_ROOT / "journal.jsonl"

INSTALLER_RELATIVE: Final = Path("scripts/ops/install_personal_dev_native_builder_runtime.py")
CONVERGER_RELATIVE: Final = Path("scripts/ops/converge_personal_dev_native_builder_release.py")
CONFORMANCE_RELATIVE: Final = Path("scripts/ops/personal_dev_native_builder_conformance.sh")
PROFILE_RELATIVE: Final = Path("deploy/personal-dev-native-builder/runtime-profile-v1.json")
VALIDATOR_RELATIVE: Final = Path("scripts/ops/staging_rollout_sealed_source.py")
SUDOERS_RELATIVE: Final = Path(
    "deploy/personal-dev-native-builder/loom-personal-dev-native-runtime-authority.sudoers"
)

SCHEMA: Final = "loom.personal-dev-native-runtime-authority.request.v1"
POLICY_SCHEMA_VERSION: Final = 1
RUNTIME_PROFILE_SHA256: Final = "c193873a276ace659a27ff9318d4b8322b487f83a68f5d100d18bc6935eb477d"
GVISOR_ARCHIVE_SHA512: Final = (
    "dc21bdc7a4f52d049f4da74a337fc7437b2ac1465c7479816a852120a8cff5292"
    "d72ae78bc4c581f857836bc9a56a1ba18ad687e6bef13d03fdd670d6f2071f7"
)
MANAGEMENT_ORIGIN: Final = "https://loom-service.dev.yylx.world"
NFT_CONFIG: Final = Path("/etc/loom/personal-dev-native-builder/provider-network.nft")
NFT_FAMILY: Final = "inet"
NFT_TABLE: Final = "loom_personal_dev_builder"
DAEMON_UNIT: Final = "loom-personal-dev-builder-dockerd.service"
AGENT_UNIT: Final = "loom-personal-dev-native-builder-agent.service"
DOCKER_ENDPOINT: Final = "unix:///run/loom-personal-dev-builder/docker.sock"

_MAX_HEADER_BYTES: Final = 65_536
_MAX_ARCHIVE_BYTES: Final = 1_073_741_824
_MAX_CA_BYTES: Final = 1_048_576
_MAX_RECEIPT_BYTES: Final = 65_536
_MAX_ASSET_BYTES: Final = 4_194_304
_SHA40_RE: Final = re.compile(r"^[0-9a-f]{40}$")
_SHA64_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_REQUEST_ID_RE: Final = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}[a-z0-9]$")
_KEY_ID_RE: Final = re.compile(r"^[a-z][a-z0-9._-]{0,63}$")
_AGENT_IMAGE_RE: Final = re.compile(
    r"^ghcr\.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:"
    r"[0-9a-f]{64}$"
)
_BUILDER_IMAGE_RE: Final = re.compile(
    r"^ghcr\.io/qianyi-sun/loom-personal-dev-builder@sha256:[0-9a-f]{64}$"
)
_CONTAINER_ID_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_ID_RE: Final = re.compile(r"^sha256:[0-9a-f]{64}$")


class AuthorityError(RuntimeError):
    """A bounded authority failure safe to collapse into a generic receipt."""


Runner = Callable[[Sequence[str], Mapping[str, str]], subprocess.CompletedProcess[str]]


@dataclass(frozen=True, slots=True)
class AuthorityPolicy:
    source_sha: str
    source_tree_sha: str
    source_base_sha: str
    wrapper_sha256: str
    validator_sha256: str
    sudoers_sha256: str
    installer_sha256: str
    converger_sha256: str
    conformance_sha256: str
    profile_sha256: str

    def __post_init__(self) -> None:
        for value in (self.source_sha, self.source_tree_sha, self.source_base_sha):
            if _SHA40_RE.fullmatch(value) is None:
                raise AuthorityError("native runtime authority source identity is invalid")
        if self.source_base_sha != APPROVED_BASE_SHA:
            raise AuthorityError("native runtime authority approved base is invalid")
        for value in (
            self.wrapper_sha256,
            self.validator_sha256,
            self.sudoers_sha256,
            self.installer_sha256,
            self.converger_sha256,
            self.conformance_sha256,
            self.profile_sha256,
        ):
            if _SHA64_RE.fullmatch(value) is None:
                raise AuthorityError("native runtime authority asset identity is invalid")
        if self.profile_sha256 != RUNTIME_PROFILE_SHA256:
            raise AuthorityError("native runtime authority profile identity is invalid")

    def payload(self) -> bytes:
        value = {
            "conformance_sha256": self.conformance_sha256,
            "converger_sha256": self.converger_sha256,
            "installer_sha256": self.installer_sha256,
            "profile_sha256": self.profile_sha256,
            "schema_version": POLICY_SCHEMA_VERSION,
            "source_base_sha": self.source_base_sha,
            "source_mode": "sealed-cumulative",
            "source_sha": self.source_sha,
            "source_tree_sha": self.source_tree_sha,
            "sudoers_sha256": self.sudoers_sha256,
            "validator_sha256": self.validator_sha256,
            "wrapper_sha256": self.wrapper_sha256,
        }
        return _canonical_json(value)


def _canonical_json(value: object) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("ascii")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _run(argv: Sequence[str], env: Mapping[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        check=False,
        capture_output=True,
        text=True,
        env=dict(env),
        timeout=1_800,
    )


def _clean_env() -> dict[str, str]:
    return {
        "HOME": "/root",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "PYTHONPATH": str(SOURCE_ROOT),
        "PYTHONSAFEPATH": "1",
    }


def _read_bounded(descriptor: int, maximum: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = os.read(descriptor, min(65_536, maximum + 1 - total))
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)
        total += len(chunk)
        if total > maximum:
            raise AuthorityError("native runtime authority input exceeds its bound")


def _require_eof(descriptor: int) -> None:
    if os.read(descriptor, 1):
        raise AuthorityError("native runtime authority payload has trailing bytes")


def _read_header(descriptor: int) -> dict[str, object]:
    payload = bytearray()
    while True:
        chunk = os.read(descriptor, 1)
        if not chunk:
            raise AuthorityError("native runtime authority header is truncated")
        if chunk == b"\n":
            break
        payload.extend(chunk)
        if len(payload) > _MAX_HEADER_BYTES:
            raise AuthorityError("native runtime authority header exceeds its bound")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AuthorityError("native runtime authority header is invalid") from exc
    if not isinstance(value, dict):
        raise AuthorityError("native runtime authority header is invalid")
    return value


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:  # pragma: no cover - operating-system invariant
            raise AuthorityError("native runtime authority write failed safely")
        view = view[written:]


def _open_regular_root_file(path: Path, *, mode: int) -> int:
    try:
        lexical = os.lstat(path)
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        )
    except OSError as exc:
        raise AuthorityError("native runtime authority asset is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        safe = (
            stat.S_ISREG(metadata.st_mode)
            and not stat.S_ISLNK(lexical.st_mode)
            and metadata.st_uid == 0
            and metadata.st_gid == 0
            and stat.S_IMODE(metadata.st_mode) == mode
            and metadata.st_nlink == 1
            and (metadata.st_dev, metadata.st_ino) == (lexical.st_dev, lexical.st_ino)
        )
    except OSError:
        os.close(descriptor)
        raise
    if not safe:
        os.close(descriptor)
        raise AuthorityError("native runtime authority asset metadata is unsafe")
    return descriptor


def _regular_root_file(path: Path, *, mode: int, maximum: int = _MAX_ASSET_BYTES) -> bytes:
    descriptor = _open_regular_root_file(path, mode=mode)
    try:
        return _read_bounded(descriptor, maximum)
    finally:
        os.close(descriptor)


def _validate_root_file_metadata(path: Path, *, mode: int) -> None:
    descriptor = _open_regular_root_file(path, mode=mode)
    os.close(descriptor)


def _safe_root_directory(path: Path, *, mode: int) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise AuthorityError("native runtime authority directory is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != mode
    ):
        raise AuthorityError("native runtime authority directory is unsafe")


def _ensure_root_directory(path: Path, *, mode: int) -> bool:
    try:
        _safe_root_directory(path, mode=mode)
        return False
    except AuthorityError:
        try:
            os.lstat(path)
        except FileNotFoundError:
            pass
        else:
            raise
    _safe_root_directory(path.parent, mode=0o755)
    os.mkdir(path, mode)
    os.chown(path, 0, 0)
    os.chmod(path, mode)
    _safe_root_directory(path, mode=mode)
    return True


def _optional_regular_root_file(path: Path, *, mode: int) -> bytes | None:
    try:
        os.lstat(path)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise AuthorityError("native runtime authority asset is unavailable") from exc
    return _regular_root_file(path, mode=mode)


def _atomic_set(
    path: Path,
    payload: bytes,
    mode: int,
    *,
    expected: bytes | None,
    parent_mode: int = 0o755,
) -> bool:
    _safe_root_directory(path.parent, mode=parent_mode)
    existing = _optional_regular_root_file(path, mode=mode)
    if existing == payload:
        return False
    if existing != expected:
        raise AuthorityError("native runtime authority installed asset drifted")
    descriptor = os.open(
        path.parent,
        os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
    )
    temporary = f".{path.name}.new-{os.getpid()}"
    try:
        target = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=descriptor,
        )
        try:
            _write_all(target, payload)
            os.fchown(target, 0, 0)
            os.fchmod(target, mode)
            os.fsync(target)
        finally:
            os.close(target)
        os.rename(temporary, path.name, src_dir_fd=descriptor, dst_dir_fd=descriptor)
        os.fsync(descriptor)
    except Exception:
        try:
            os.unlink(temporary, dir_fd=descriptor)
        except OSError:
            pass
        raise
    finally:
        os.close(descriptor)
    return True


def _unlink_exact(path: Path, *, payload: bytes, mode: int) -> None:
    if _regular_root_file(path, mode=mode) != payload:
        raise AuthorityError("native runtime authority rollback asset drifted")
    path.unlink()


def _load_validator(path: Path) -> Any:
    module_name = "_loom_personal_dev_native_runtime_sealed_source"
    specification = importlib.util.spec_from_file_location(module_name, path)
    if specification is None or specification.loader is None:
        raise AuthorityError("native runtime authority source validator is unavailable")
    module = importlib.util.module_from_spec(specification)
    previous = sys.modules.get(module_name)
    sys.modules[module_name] = module
    try:
        specification.loader.exec_module(module)
    except Exception as exc:
        raise AuthorityError("native runtime authority source validator failed safely") from exc
    finally:
        if previous is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = previous
    if not hasattr(module, "SealedSource") or not hasattr(module, "validate_sealed_source"):
        raise AuthorityError("native runtime authority source validator is invalid")
    return module


def _read_policy() -> AuthorityPolicy:
    payload = _regular_root_file(POLICY, mode=0o600)
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AuthorityError("native runtime authority policy is invalid") from exc
    expected = {
        "conformance_sha256",
        "converger_sha256",
        "installer_sha256",
        "profile_sha256",
        "schema_version",
        "source_base_sha",
        "source_mode",
        "source_sha",
        "source_tree_sha",
        "sudoers_sha256",
        "validator_sha256",
        "wrapper_sha256",
    }
    if (
        not isinstance(value, dict)
        or set(value) != expected
        or value["schema_version"] != POLICY_SCHEMA_VERSION
        or value["source_mode"] != "sealed-cumulative"
        or any(not isinstance(value[key], str) for key in expected - {"schema_version"})
    ):
        raise AuthorityError("native runtime authority policy is invalid")
    return AuthorityPolicy(
        source_sha=value["source_sha"],
        source_tree_sha=value["source_tree_sha"],
        source_base_sha=value["source_base_sha"],
        wrapper_sha256=value["wrapper_sha256"],
        validator_sha256=value["validator_sha256"],
        sudoers_sha256=value["sudoers_sha256"],
        installer_sha256=value["installer_sha256"],
        converger_sha256=value["converger_sha256"],
        conformance_sha256=value["conformance_sha256"],
        profile_sha256=value["profile_sha256"],
    )


def _validate_source(policy: AuthorityPolicy) -> None:
    validator = _load_validator(VALIDATOR)
    source = validator.SealedSource(
        SOURCE_ROOT,
        policy.source_sha,
        policy.source_tree_sha,
        policy.source_base_sha,
    )
    try:
        validator.validate_sealed_source(source)
    except Exception as exc:
        raise AuthorityError("native runtime authority sealed source failed validation") from exc
    assets = {
        INSTALLER_RELATIVE: policy.installer_sha256,
        CONVERGER_RELATIVE: policy.converger_sha256,
        CONFORMANCE_RELATIVE: policy.conformance_sha256,
        PROFILE_RELATIVE: policy.profile_sha256,
    }
    for relative, expected in assets.items():
        payload = _regular_root_file(SOURCE_ROOT / relative, mode=0o644)
        if _sha256(payload) != expected:
            raise AuthorityError("native runtime authority source asset drifted")


def _validate_runtime_assets(policy: AuthorityPolicy) -> None:
    checks = (
        (_regular_root_file(LIBEXEC, mode=0o755), policy.wrapper_sha256),
        (_regular_root_file(VALIDATOR, mode=0o644), policy.validator_sha256),
        (_regular_root_file(SUDOERS, mode=0o440), policy.sudoers_sha256),
    )
    if any(_sha256(payload) != expected for payload, expected in checks):
        raise AuthorityError("native runtime authority installed asset drifted")


def _validate_invoker(environ: Mapping[str, str]) -> None:
    try:
        account = pwd.getpwnam(OPERATOR)
    except KeyError as exc:
        raise AuthorityError("native runtime authority operator is unavailable") from exc
    if (
        os.geteuid() != 0
        or os.getegid() != 0
        or environ.get("SUDO_USER") != OPERATOR
        or environ.get("SUDO_UID") != str(account.pw_uid)
        or environ.get("SUDO_GID") != str(account.pw_gid)
        or environ.get("SUDO_COMMAND") != str(LIBEXEC)
    ):
        raise AuthorityError("native runtime authority invocation is not approved")


def _open_lock() -> int:
    _safe_root_directory(STATE_ROOT, mode=0o700)
    try:
        descriptor = os.open(
            LOCK,
            os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        )
        metadata = os.fstat(descriptor)
    except OSError as exc:
        raise AuthorityError("native runtime authority lock is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_nlink != 1
    ):
        os.close(descriptor)
        raise AuthorityError("native runtime authority lock is unsafe")
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        os.close(descriptor)
        raise AuthorityError("native runtime authority is busy") from exc
    return descriptor


def _validate_request(value: dict[str, object], policy: AuthorityPolicy) -> str:
    action = value.get("action")
    if action not in {
        "prepare",
        "stage-agent",
        "activate",
        "check",
        "observe-agent",
        "observe-containers",
        "remove",
    }:
        raise AuthorityError("native runtime authority action is invalid")
    common = {"action", "request_id", "schema", "source_sha", "source_tree_sha"}
    extras = {
        "prepare": {
            "archive_sha512",
            "archive_size",
            "current_agent",
            "current_builder",
            "current_revision",
            "previous_agent",
            "previous_builder",
            "previous_revision",
            "public_store_origin",
        },
        "stage-agent": {
            "agent_instance_id",
            "ca_size",
            "current_agent",
            "current_builder",
            "key_id",
            "private_key_size",
            "service_url",
        },
        "activate": set(),
        "check": {"expected_state"},
        "observe-agent": set(),
        "observe-containers": {"grant_ids"},
        "remove": set(),
    }[action]
    if set(value) != common | extras:
        raise AuthorityError("native runtime authority request fields are invalid")
    if (
        value["schema"] != SCHEMA
        or value["source_sha"] != policy.source_sha
        or value["source_tree_sha"] != policy.source_tree_sha
        or not isinstance(value["request_id"], str)
        or _REQUEST_ID_RE.fullmatch(value["request_id"]) is None
    ):
        raise AuthorityError("native runtime authority request identity is invalid")
    if action == "prepare":
        _validate_prepare(value)
    elif action == "stage-agent":
        _validate_stage_agent(value)
    elif action == "check" and value["expected_state"] not in {"staged", "active"}:
        raise AuthorityError("native runtime authority expected state is invalid")
    elif action == "observe-containers":
        _validate_grant_ids(value["grant_ids"])
    return action


def _validate_grant_ids(value: object) -> None:
    if not isinstance(value, list) or len(value) != 2:
        raise AuthorityError("native runtime authority grant identities are invalid")
    canonical: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise AuthorityError("native runtime authority grant identities are invalid")
        try:
            observed = str(uuid.UUID(item))
        except ValueError as exc:
            raise AuthorityError("native runtime authority grant identities are invalid") from exc
        if observed != item:
            raise AuthorityError("native runtime authority grant identities are invalid")
        canonical.append(item)
    if canonical != sorted(set(canonical)):
        raise AuthorityError("native runtime authority grant identities are invalid")


def _validate_prepare(value: Mapping[str, object]) -> None:
    if (
        not isinstance(value["archive_size"], int)
        or isinstance(value["archive_size"], bool)
        or not 0 < value["archive_size"] <= _MAX_ARCHIVE_BYTES
        or value["archive_sha512"] != GVISOR_ARCHIVE_SHA512
        or not isinstance(value["current_agent"], str)
        or _AGENT_IMAGE_RE.fullmatch(value["current_agent"]) is None
        or not isinstance(value["current_builder"], str)
        or _BUILDER_IMAGE_RE.fullmatch(value["current_builder"]) is None
        or not isinstance(value["current_revision"], str)
        or _SHA40_RE.fullmatch(value["current_revision"]) is None
    ):
        raise AuthorityError("native runtime authority prepare request is invalid")
    public_store_origin = value["public_store_origin"]
    if not isinstance(public_store_origin, str):
        raise AuthorityError("native runtime authority public store is invalid")
    parsed = urllib.parse.urlsplit(public_store_origin)
    hostname = parsed.hostname or ""
    try:
        port = parsed.port
    except ValueError as exc:
        raise AuthorityError("native runtime authority public store is invalid") from exc
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        hostname_is_ip = False
    else:
        hostname_is_ip = True
    if (
        parsed.scheme != "https"
        or not hostname
        or hostname_is_ip
        or hostname == urllib.parse.urlsplit(MANAGEMENT_ORIGIN).hostname
        or re.fullmatch(
            r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
            r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+",
            hostname,
        )
        is None
        or port not in {None, 443}
        or parsed.path not in {"", "/"}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or public_store_origin
        not in {
            f"https://{hostname}",
            f"https://{hostname}:443",
            f"https://{hostname}/",
            f"https://{hostname}:443/",
        }
    ):
        raise AuthorityError("native runtime authority public store is invalid")
    previous = (
        value["previous_agent"],
        value["previous_builder"],
        value["previous_revision"],
    )
    if previous == ("", "", ""):
        return
    if (
        not isinstance(previous[0], str)
        or _AGENT_IMAGE_RE.fullmatch(previous[0]) is None
        or not isinstance(previous[1], str)
        or _BUILDER_IMAGE_RE.fullmatch(previous[1]) is None
        or not isinstance(previous[2], str)
        or _SHA40_RE.fullmatch(previous[2]) is None
        or previous[2] == value["current_revision"]
    ):
        raise AuthorityError("native runtime authority previous release is invalid")


def _validate_stage_agent(value: Mapping[str, object]) -> None:
    agent_instance_id = value["agent_instance_id"]
    try:
        canonical_instance_id = str(uuid.UUID(cast(str, agent_instance_id)))
    except (ValueError, AttributeError, TypeError):
        canonical_instance_id = ""
    if (
        not isinstance(value["private_key_size"], int)
        or isinstance(value["private_key_size"], bool)
        or value["private_key_size"] != 32
        or not isinstance(value["ca_size"], int)
        or isinstance(value["ca_size"], bool)
        or not 0 < value["ca_size"] <= _MAX_CA_BYTES
        or not isinstance(value["current_agent"], str)
        or _AGENT_IMAGE_RE.fullmatch(value["current_agent"]) is None
        or not isinstance(value["current_builder"], str)
        or _BUILDER_IMAGE_RE.fullmatch(value["current_builder"]) is None
        or value["service_url"] != MANAGEMENT_ORIGIN
        or not isinstance(agent_instance_id, str)
        or canonical_instance_id != agent_instance_id
        or not isinstance(value["key_id"], str)
        or _KEY_ID_RE.fullmatch(value["key_id"]) is None
    ):
        raise AuthorityError("native runtime authority agent request is invalid")


def _invoke_json(
    argv: Sequence[str],
    run: Runner,
) -> tuple[dict[str, object], bytes]:
    result = run(argv, _clean_env())
    encoded = result.stdout.encode("utf-8", errors="strict")
    if result.returncode != 0 or result.stderr or len(encoded) > _MAX_RECEIPT_BYTES:
        raise AuthorityError("native runtime authority helper failed safely")
    try:
        value = json.loads(encoded)
    except json.JSONDecodeError as exc:
        raise AuthorityError("native runtime authority helper receipt is invalid") from exc
    if not isinstance(value, dict):
        raise AuthorityError("native runtime authority helper receipt is invalid")
    return value, _canonical_json(value)


def _installer_argv(operation: str, *extra: str) -> tuple[str, ...]:
    return (
        "/usr/bin/python3",
        str(SOURCE_ROOT / INSTALLER_RELATIVE),
        operation,
        "--profile",
        str(SOURCE_ROOT / PROFILE_RELATIVE),
        *extra,
    )


def _converger_argv(operation: str, value: Mapping[str, object]) -> tuple[str, ...]:
    argv = [
        "/usr/bin/python3",
        str(SOURCE_ROOT / CONVERGER_RELATIVE),
        operation,
        "--current-agent",
        str(value["current_agent"]),
        "--current-builder",
        str(value["current_builder"]),
        "--current-revision",
        str(value["current_revision"]),
    ]
    if value["previous_agent"]:
        argv.extend(
            (
                "--previous-agent",
                str(value["previous_agent"]),
                "--previous-builder",
                str(value["previous_builder"]),
                "--previous-revision",
                str(value["previous_revision"]),
            )
        )
    return tuple(argv)


def _run_fixed(argv: Sequence[str], run: Runner, *, allow_stdout: bool = False) -> str:
    result = run(argv, _clean_env())
    if result.returncode != 0 or result.stderr or (result.stdout and not allow_stdout):
        raise AuthorityError("native runtime authority fixed command failed safely")
    return result.stdout


def _unit_inactive_disabled(unit: str, run: Runner) -> None:
    active = run(("/usr/bin/systemctl", "is-active", "--quiet", unit), _clean_env())
    enabled = run(("/usr/bin/systemctl", "is-enabled", unit), _clean_env())
    if active.returncode == 0 or enabled.stdout.strip() != "disabled":
        raise AuthorityError("native runtime authority unit state is unsafe")


def _delete_nft_table(run: Runner) -> None:
    listed = run(
        ("/usr/sbin/nft", "list", "table", NFT_FAMILY, NFT_TABLE),
        _clean_env(),
    )
    if listed.returncode == 0:
        _run_fixed(
            ("/usr/sbin/nft", "delete", "table", NFT_FAMILY, NFT_TABLE),
            run,
        )
    elif "No such file" not in listed.stderr:
        raise AuthorityError("native runtime authority nft state is unavailable")


def _deactivate(run: Runner, *, include_agent: bool) -> None:
    if include_agent:
        _run_fixed(("/usr/bin/systemctl", "stop", AGENT_UNIT), run)
    _run_fixed(("/usr/bin/systemctl", "stop", DAEMON_UNIT), run)
    _delete_nft_table(run)


def _stage_payload_file(
    descriptor: int,
    *,
    size: int,
    algorithm: str,
    expected_digest: str | None,
    directory: Path,
    name: str,
    mode: int,
) -> tuple[Path, str]:
    path = directory / name
    output = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    digest = hashlib.new(algorithm)
    remaining = size
    try:
        while remaining:
            chunk = os.read(descriptor, min(65_536, remaining))
            if not chunk:
                raise AuthorityError("native runtime authority payload is truncated")
            _write_all(output, chunk)
            digest.update(chunk)
            remaining -= len(chunk)
        os.fchown(output, 0, 0)
        os.fchmod(output, mode)
        os.fsync(output)
    finally:
        os.close(output)
    observed = digest.hexdigest()
    if expected_digest is not None and observed != expected_digest:
        raise AuthorityError("native runtime authority payload digest is invalid")
    return path, observed


def _prepare(
    value: Mapping[str, object],
    descriptor: int,
    run: Runner,
) -> dict[str, str]:
    receipts: dict[str, str] = {}
    with tempfile.TemporaryDirectory(prefix="request-", dir=STATE_ROOT) as temporary:
        directory = Path(temporary)
        archive, _ = _stage_payload_file(
            descriptor,
            size=cast(int, value["archive_size"]),
            algorithm="sha512",
            expected_digest=GVISOR_ARCHIVE_SHA512,
            directory=directory,
            name="gvisor.tar.bz2",
            mode=0o600,
        )
        _require_eof(descriptor)
        _unit_inactive_disabled(DAEMON_UNIT, run)
        _unit_inactive_disabled(AGENT_UNIT, run)
        for operation in ("preflight", "install"):
            _, payload = _invoke_json(
                _installer_argv(operation, "--archive", str(archive)),
                run,
            )
            receipts[f"runtime-{operation}"] = _sha256(payload)
        _, payload = _invoke_json(_installer_argv("verify-staged"), run)
        receipts["runtime-verify-staged"] = _sha256(payload)
        try:
            _run_fixed(("/usr/sbin/nft", "--file", str(NFT_CONFIG)), run)
            _run_fixed(("/usr/bin/systemctl", "start", DAEMON_UNIT), run)
            plan_one, plan_payload_one = _invoke_json(_converger_argv("plan", value), run)
            plan_two, plan_payload_two = _invoke_json(_converger_argv("plan", value), run)
            if plan_one != plan_two or plan_payload_one != plan_payload_two:
                raise AuthorityError("native runtime authority release plan drifted")
            receipts["release-plan"] = _sha256(plan_payload_one)
            for operation in ("apply", "verify"):
                _, payload = _invoke_json(_converger_argv(operation, value), run)
                receipts[f"release-{operation}"] = _sha256(payload)
            conformance = _run_fixed(
                (
                    "/bin/bash",
                    str(SOURCE_ROOT / CONFORMANCE_RELATIVE),
                    str(value["current_builder"]),
                    str(value["current_agent"]),
                    str(value["public_store_origin"]),
                ),
                run,
                allow_stdout=True,
            )
            expected = (
                "Runtime=runsc-personal-dev-native architecture=arm64 "
                "platform=linux/arm64 kvm=/dev/kvm public_https=allowed "
                "private=denied host_to_provider=denied "
                "foreign_to_provider=denied cross_network=denied\n"
            )
            if conformance != expected:
                raise AuthorityError("native runtime authority conformance is invalid")
            receipts["two-container-conformance"] = _sha256(conformance.encode())
            managed_containers = _run_fixed(
                (
                    "/usr/bin/docker",
                    "-H",
                    DOCKER_ENDPOINT,
                    "ps",
                    "-aq",
                    "--filter",
                    "label=loom.personal-dev-native-builder.managed=true",
                ),
                run,
                allow_stdout=True,
            )
            managed_networks = _run_fixed(
                (
                    "/usr/bin/docker",
                    "-H",
                    DOCKER_ENDPOINT,
                    "network",
                    "ls",
                    "-q",
                    "--filter",
                    "label=loom.personal-dev-native-builder.managed=true",
                ),
                run,
                allow_stdout=True,
            )
            if managed_containers or managed_networks:
                raise AuthorityError("native runtime authority conformance cleanup failed")
        finally:
            _deactivate(run, include_agent=False)
        _unit_inactive_disabled(DAEMON_UNIT, run)
        _unit_inactive_disabled(AGENT_UNIT, run)
    return receipts


def _stage_agent(
    value: Mapping[str, object],
    descriptor: int,
    run: Runner,
) -> dict[str, str]:
    _unit_inactive_disabled(DAEMON_UNIT, run)
    _unit_inactive_disabled(AGENT_UNIT, run)
    with tempfile.TemporaryDirectory(prefix="request-", dir=STATE_ROOT) as temporary:
        directory = Path(temporary)
        private_key, _ = _stage_payload_file(
            descriptor,
            size=32,
            algorithm="sha256",
            expected_digest=None,
            directory=directory,
            name="agent-ed25519",
            mode=0o400,
        )
        ca_file, _ = _stage_payload_file(
            descriptor,
            size=cast(int, value["ca_size"]),
            algorithm="sha256",
            expected_digest=None,
            directory=directory,
            name="service-ca.pem",
            mode=0o444,
        )
        _require_eof(descriptor)
        _, stage_payload = _invoke_json(
            _installer_argv(
                "stage-agent",
                "--agent-image",
                str(value["current_agent"]),
                "--builder-image",
                str(value["current_builder"]),
                "--service-url",
                MANAGEMENT_ORIGIN,
                "--agent-instance-id",
                str(value["agent_instance_id"]),
                "--key-id",
                str(value["key_id"]),
                "--private-key",
                str(private_key),
                "--ca-file",
                str(ca_file),
            ),
            run,
        )
        _, verify_payload = _invoke_json(_installer_argv("verify-staged"), run)
    return {
        "agent-stage": _sha256(stage_payload),
        "runtime-verify-staged": _sha256(verify_payload),
    }


def _activate(descriptor: int, run: Runner) -> dict[str, str]:
    _require_eof(descriptor)
    _, staged_payload = _invoke_json(_installer_argv("verify-staged"), run)
    try:
        _run_fixed(("/usr/sbin/nft", "--file", str(NFT_CONFIG)), run)
        _run_fixed(("/usr/bin/systemctl", "start", DAEMON_UNIT), run)
        _run_fixed(("/usr/bin/systemctl", "start", AGENT_UNIT), run)
        _run_fixed(
            ("/usr/bin/systemctl", "is-active", "--quiet", AGENT_UNIT),
            run,
        )
        _, active_payload = _invoke_json(_installer_argv("verify-active"), run)
    except Exception:
        _deactivate(run, include_agent=True)
        raise
    return {
        "runtime-verify-active": _sha256(active_payload),
        "runtime-verify-staged": _sha256(staged_payload),
    }


def _check(value: Mapping[str, object], descriptor: int, run: Runner) -> dict[str, str]:
    _require_eof(descriptor)
    operation = "verify-active" if value["expected_state"] == "active" else "verify-staged"
    _, payload = _invoke_json(_installer_argv(operation), run)
    if value["expected_state"] == "staged":
        _unit_inactive_disabled(DAEMON_UNIT, run)
        _unit_inactive_disabled(AGENT_UNIT, run)
    return {f"runtime-{operation}": _sha256(payload)}


def _observe_agent(
    descriptor: int,
    run: Runner,
) -> tuple[dict[str, str], dict[str, str]]:
    _require_eof(descriptor)
    evidence: dict[str, str] = {}
    for key, property_name, expected in (
        ("active_state", "ActiveState", "active"),
        (
            "fragment_path",
            "FragmentPath",
            "/etc/systemd/system/loom-personal-dev-native-builder-agent.service",
        ),
        ("sub_state", "SubState", "running"),
    ):
        observed = _run_fixed(
            (
                "/usr/bin/systemctl",
                "show",
                AGENT_UNIT,
                f"--property={property_name}",
                "--value",
            ),
            run,
            allow_stdout=True,
        ).strip()
        if observed != expected:
            raise AuthorityError("native runtime authority agent evidence is invalid")
        evidence[key] = observed
    payload = _canonical_json(evidence)
    return {"agent-active-evidence": _sha256(payload)}, evidence


def _observe_containers(
    value: Mapping[str, object],
    descriptor: int,
    run: Runner,
) -> tuple[dict[str, str], list[dict[str, str]]]:
    _require_eof(descriptor)
    expected_grants = cast(list[str], value["grant_ids"])
    observed_ids = sorted(
        item
        for item in _run_fixed(
            (
                "/usr/bin/docker",
                "-H",
                DOCKER_ENDPOINT,
                "ps",
                "--no-trunc",
                "-q",
                "--filter",
                "label=loom.personal-dev-native-builder.managed=true",
            ),
            run,
            allow_stdout=True,
        ).splitlines()
        if item
    )
    if (
        len(observed_ids) != 4
        or len(set(observed_ids)) != 4
        or any(_CONTAINER_ID_RE.fullmatch(item) is None for item in observed_ids)
    ):
        raise AuthorityError("native runtime authority container evidence is invalid")
    raw = _run_fixed(
        (
            "/usr/bin/docker",
            "-H",
            DOCKER_ENDPOINT,
            "inspect",
            *observed_ids,
        ),
        run,
        allow_stdout=True,
    )
    try:
        inspected = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AuthorityError("native runtime authority container evidence is invalid") from exc
    if not isinstance(inspected, list) or len(inspected) != 4:
        raise AuthorityError("native runtime authority container evidence is invalid")
    evidence: list[dict[str, str]] = []
    for item in inspected:
        if not isinstance(item, dict):
            raise AuthorityError("native runtime authority container evidence is invalid")
        config = item.get("Config")
        host_config = item.get("HostConfig")
        if not isinstance(config, dict) or not isinstance(host_config, dict):
            raise AuthorityError("native runtime authority container evidence is invalid")
        labels = config.get("Labels")
        if not isinstance(labels, dict):
            raise AuthorityError("native runtime authority container evidence is invalid")
        row = {
            "grant_id": labels.get("loom.personal-dev-native-builder.grant-id"),
            "id": item.get("Id"),
            "image": item.get("Image"),
            "platform": labels.get("loom.personal-dev-native-builder.platform"),
            "role": labels.get("loom.personal-dev-native-builder.role"),
            "runtime": host_config.get("Runtime"),
        }
        if (
            not all(isinstance(entry, str) for entry in row.values())
            or row["grant_id"] not in expected_grants
            or _CONTAINER_ID_RE.fullmatch(cast(str, row["id"])) is None
            or row["id"] not in observed_ids
            or _IMAGE_ID_RE.fullmatch(cast(str, row["image"])) is None
            or row["platform"] != "linux/arm64"
            or row["role"] not in {"buildkit", "client"}
            or row["runtime"] != "runsc-personal-dev-native"
        ):
            raise AuthorityError("native runtime authority container evidence is invalid")
        evidence.append(cast(dict[str, str], row))
    evidence.sort(key=lambda item: (item["grant_id"], item["role"]))
    expected_pairs = {
        (grant_id, role) for grant_id in expected_grants for role in ("buildkit", "client")
    }
    if {(item["grant_id"], item["role"]) for item in evidence} != expected_pairs:
        raise AuthorityError("native runtime authority container evidence is invalid")
    payload = _canonical_json(evidence)
    return {"container-evidence": _sha256(payload)}, evidence


def _remove(descriptor: int, run: Runner) -> dict[str, str]:
    _require_eof(descriptor)
    _deactivate(run, include_agent=True)
    _, payload = _invoke_json(_installer_argv("remove"), run)
    return {"runtime-remove": _sha256(payload)}


def _journal(value: Mapping[str, object], policy: AuthorityPolicy) -> None:
    record = {
        "action": value["action"],
        "operator": OPERATOR,
        "request_id": value["request_id"],
        "source_sha": policy.source_sha,
        "source_tree_sha": policy.source_tree_sha,
        "timestamp_ns": time.time_ns(),
    }
    payload = _canonical_json(record)
    descriptor = os.open(
        JOURNAL,
        os.O_WRONLY | os.O_APPEND | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
    )
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_nlink != 1
        ):
            raise AuthorityError("native runtime authority journal is unsafe")
        _write_all(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def dispatch(
    *,
    descriptor: int,
    environ: Mapping[str, str],
    run: Runner = _run,
) -> dict[str, object]:
    _validate_invoker(environ)
    lock = _open_lock()
    try:
        policy = _read_policy()
        _validate_runtime_assets(policy)
        _validate_source(policy)
        value = _read_header(descriptor)
        action = _validate_request(value, policy)
        evidence: dict[str, str] | list[dict[str, str]] | None = None
        if action == "prepare":
            receipts = _prepare(value, descriptor, run)
        elif action == "stage-agent":
            receipts = _stage_agent(value, descriptor, run)
        elif action == "activate":
            receipts = _activate(descriptor, run)
        elif action == "check":
            receipts = _check(value, descriptor, run)
        elif action == "observe-agent":
            receipts, evidence = _observe_agent(descriptor, run)
        elif action == "observe-containers":
            receipts, evidence = _observe_containers(value, descriptor, run)
        else:
            receipts = _remove(descriptor, run)
        _journal(value, policy)
        report: dict[str, object] = {
            "action": action,
            "receipts": receipts,
            "request_id": value["request_id"],
            "source_sha": policy.source_sha,
            "source_tree_sha": policy.source_tree_sha,
            "status": "ok",
        }
        if evidence is not None:
            report["evidence"] = evidence
        return report
    finally:
        os.close(lock)


def bootstrap(source_sha: str, source_tree_sha: str, *, run: Runner = _run) -> dict[str, object]:
    if os.geteuid() != 0 or os.getegid() != 0 or any(key.startswith("SUDO_") for key in os.environ):
        raise AuthorityError(
            "native runtime authority bootstrap requires a direct root administrator"
        )
    expected = SOURCE_ROOT / Path("scripts/ops") / Path(__file__).name
    if Path(__file__).resolve() != expected:
        raise AuthorityError("native runtime authority bootstrap source is not fixed")
    _safe_root_directory(SOURCE_ROOT, mode=0o700)
    validator_path = SOURCE_ROOT / VALIDATOR_RELATIVE
    _regular_root_file(validator_path, mode=0o644)
    validator = _load_validator(validator_path)
    source = validator.SealedSource(
        SOURCE_ROOT,
        source_sha,
        source_tree_sha,
        APPROVED_BASE_SHA,
    )
    try:
        validator.validate_sealed_source(source)
    except Exception as exc:
        raise AuthorityError("native runtime authority sealed source failed validation") from exc
    wrapper = _regular_root_file(
        SOURCE_ROOT / Path(__file__).resolve().relative_to(SOURCE_ROOT), mode=0o644
    )
    validator_payload = _regular_root_file(validator_path, mode=0o644)
    sudoers_payload = _regular_root_file(SOURCE_ROOT / SUDOERS_RELATIVE, mode=0o644)
    installer = _regular_root_file(SOURCE_ROOT / INSTALLER_RELATIVE, mode=0o644)
    converger = _regular_root_file(SOURCE_ROOT / CONVERGER_RELATIVE, mode=0o644)
    conformance = _regular_root_file(SOURCE_ROOT / CONFORMANCE_RELATIVE, mode=0o644)
    profile = _regular_root_file(SOURCE_ROOT / PROFILE_RELATIVE, mode=0o644)
    policy = AuthorityPolicy(
        source_sha=source_sha,
        source_tree_sha=source_tree_sha,
        source_base_sha=APPROVED_BASE_SHA,
        wrapper_sha256=_sha256(wrapper),
        validator_sha256=_sha256(validator_payload),
        sudoers_sha256=_sha256(sudoers_payload),
        installer_sha256=_sha256(installer),
        converger_sha256=_sha256(converger),
        conformance_sha256=_sha256(conformance),
        profile_sha256=_sha256(profile),
    )
    validation = run(
        ("/usr/sbin/visudo", "-cf", str(SOURCE_ROOT / SUDOERS_RELATIVE)),
        _clean_env(),
    )
    if validation.returncode != 0:
        raise AuthorityError("native runtime authority sudoers asset is invalid")
    managed = (
        (LIBEXEC, wrapper, 0o755, 0o755),
        (VALIDATOR, validator_payload, 0o644, 0o755),
        (POLICY, policy.payload(), 0o600, 0o755),
        (SUDOERS, sudoers_payload, 0o440, 0o755),
    )
    presence = tuple(
        _optional_regular_root_file(path, mode=mode) is not None
        for path, _payload, mode, _parent_mode in managed
    )
    if any(presence) and not all(presence):
        raise AuthorityError("native runtime authority installation is partial")
    upgrading = all(presence)
    previous: dict[Path, bytes | None] = {}
    if upgrading:
        old_policy = _read_policy()
        _validate_runtime_assets(old_policy)
        _safe_root_directory(STATE_ROOT, mode=0o700)
        previous = {
            path: _regular_root_file(path, mode=mode)
            for path, _payload, mode, _parent_mode in managed
        }
        _validate_root_file_metadata(LOCK, mode=0o600)
        _validate_root_file_metadata(JOURNAL, mode=0o600)
    else:
        previous = {path: None for path, _payload, _mode, _parent_mode in managed}

    bootstrap_lock = _open_lock() if upgrading else None
    changed: list[Path] = []
    created_directories: list[Path] = []
    try:
        for directory, mode in (
            (POLICY.parent, 0o755),
            (STATE_ROOT, 0o700),
            (LIBEXEC.parent, 0o755),
        ):
            if _ensure_root_directory(directory, mode=mode):
                created_directories.append(directory)
        _safe_root_directory(SUDOERS.parent, mode=0o755)
        if not upgrading:
            for path in (LOCK, JOURNAL):
                if _atomic_set(
                    path,
                    b"",
                    0o600,
                    expected=None,
                    parent_mode=0o700,
                ):
                    changed.append(path)
        for path, payload, mode, parent_mode in managed:
            if _atomic_set(
                path,
                payload,
                mode,
                expected=previous[path],
                parent_mode=parent_mode,
            ):
                changed.append(path)
        installed = run(("/usr/sbin/visudo", "-cf", str(SUDOERS)), _clean_env())
        if installed.returncode != 0:
            raise AuthorityError("native runtime authority installed sudoers is invalid")
    except Exception as original:
        payloads = {path: payload for path, payload, _mode, _parent_mode in managed}
        modes = {path: mode for path, _payload, mode, _parent_mode in managed}
        parent_modes = {path: parent_mode for path, _payload, _mode, parent_mode in managed}
        rollback_failed = False
        for path in reversed(changed):
            try:
                if path in previous and previous[path] is not None:
                    _atomic_set(
                        path,
                        cast(bytes, previous[path]),
                        modes[path],
                        expected=payloads[path],
                        parent_mode=parent_modes[path],
                    )
                elif path in payloads:
                    _unlink_exact(path, payload=payloads[path], mode=modes[path])
                else:
                    _unlink_exact(path, payload=b"", mode=0o600)
            except (AuthorityError, OSError):
                rollback_failed = True
        for directory in reversed(created_directories):
            try:
                directory.rmdir()
            except OSError:
                rollback_failed = True
        if bootstrap_lock is not None:
            os.close(bootstrap_lock)
        if rollback_failed:
            raise AuthorityError(
                "native runtime authority bootstrap rollback failed safely"
            ) from original
        raise
    if bootstrap_lock is not None:
        os.close(bootstrap_lock)
    return {
        "action": "bootstrap",
        "changed": [str(path) for path in changed],
        "source_base_sha": policy.source_base_sha,
        "source_sha": policy.source_sha,
        "source_tree_sha": policy.source_tree_sha,
        "status": "ok",
    }


def _bootstrap_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    bootstrap_parser = subparsers.add_parser("bootstrap")
    bootstrap_parser.add_argument("--source-sha", required=True)
    bootstrap_parser.add_argument("--source-tree-sha", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    try:
        if arguments:
            parsed = _bootstrap_parser().parse_args(arguments)
            report = bootstrap(parsed.source_sha, parsed.source_tree_sha)
        else:
            report = dispatch(descriptor=0, environ=os.environ)
    except (AuthorityError, OSError, subprocess.SubprocessError):
        print("error: personal-dev native runtime authority failed safely", file=sys.stderr)
        return 1
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
