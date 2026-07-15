"""Strict root-owned configuration for the staging rollout operator."""

from __future__ import annotations

import grp
import hashlib
import os
import pwd
import re
import stat
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

from .model import APPROVED_FETCH_REF, APPROVED_REMOTE_URL, SchemaVersion

APPROVED_SERVICE_USER = "loom-rollout"
APPROVED_OPERATOR_GROUP = "loom-staging-operators"
APPROVED_CLUSTER_NAME = "loom-staging"
APPROVED_NAMESPACE = "loom-staging"
APPROVED_ENVIRONMENT = "staging"
APPROVED_CP_URL = "http://127.0.0.1:18081"
APPROVED_SCOPE = "current-gb10"

ServiceUser = Literal["loom-rollout"]
OperatorGroup = Literal["loom-staging-operators"]
Environment = Literal["staging"]
RolloutScope = Literal["current-gb10"]

_FINGERPRINT_RE = re.compile(r"^sha256:[0-9a-f]{12} len=[1-9][0-9]*$")
_USERNAME_RE = re.compile(r"^[a-z_][a-z0-9_-]{0,63}$")
_CONFIG_KEYS = frozenset(
    {
        "schema_version",
        "service_user",
        "operator_group",
        "remote_url",
        "target_ref",
        "runner_repo",
        "state_root",
        "runtime_root",
        "rollout_root",
        "kubeconfig_path",
        "cluster_config_path",
        "admin_token_source",
        "worker_token_source",
        "service_token_source",
        "expect_admin_token_fingerprint",
        "cluster_name",
        "namespace",
        "environment",
        "cp_url",
        "smoke_on_behalf_username",
        "smoke_on_behalf_team_id",
        "scope",
        "gb10_prep_concurrency",
    }
)
_MAX_CONFIG_BYTES = 1 << 20


class ConfigError(ValueError):
    """Raised when the protected runner configuration fails closed."""


def _require_string(raw: dict[str, object], key: str) -> str:
    value = raw[key]
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{key} must be a non-empty string")
    return value


def _require_literal(raw: dict[str, object], key: str, expected: str) -> str:
    value = _require_string(raw, key)
    if value != expected:
        if key == "remote_url":
            raise ConfigError("remote_url is not approved")
        raise ConfigError(f"{key} must be {expected}")
    return value


def _require_absolute_path(raw: dict[str, object], key: str) -> Path:
    rendered = _require_string(raw, key)
    path = Path(rendered)
    if not path.is_absolute() or ".." in path.parts:
        raise ConfigError(f"{key} must be an absolute protected path")
    return path


def _require_file_source(raw: dict[str, object], key: str) -> str:
    rendered = _require_string(raw, key)
    if not rendered.startswith("file:"):
        raise ConfigError(f"{key} must be an absolute file: source")
    path = Path(rendered.removeprefix("file:"))
    if not path.is_absolute() or ".." in path.parts:
        raise ConfigError(f"{key} must be an absolute file: source")
    return rendered


def _read_protected_config(
    path: Path,
    expected_owner_uid: int,
    *,
    expected_owner_gid: int | None = None,
    expected_mode: int | None = None,
    validate_parent_authority: bool = False,
) -> bytes:
    if not path.is_absolute():
        raise ConfigError("config path must be absolute")
    if validate_parent_authority:
        try:
            for parent in path.parents:
                metadata = os.lstat(parent)
                if (
                    not stat.S_ISDIR(metadata.st_mode)
                    or stat.S_ISLNK(metadata.st_mode)
                    or metadata.st_uid != expected_owner_uid
                    or stat.S_IMODE(metadata.st_mode) & 0o022
                ):
                    raise ConfigError("config parent authority is unsafe")
        except OSError as exc:
            raise ConfigError("config parent authority is unavailable") from exc
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise ConfigError(f"config must be a readable regular file, not a symlink: {path}") from exc

    failure: BaseException | None = None
    payload = b""
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise ConfigError(f"config must be a regular file: {path}")
        if before.st_uid != expected_owner_uid:
            raise ConfigError(
                f"config owner UID {before.st_uid} does not match expected owner UID "
                f"{expected_owner_uid}"
            )
        if expected_owner_gid is not None and before.st_gid != expected_owner_gid:
            raise ConfigError(
                f"config owner GID {before.st_gid} does not match expected owner GID "
                f"{expected_owner_gid}"
            )
        mode = stat.S_IMODE(before.st_mode)
        if expected_mode is not None and mode != expected_mode:
            raise ConfigError(
                f"config mode {mode:04o} does not match expected mode {expected_mode:04o}"
            )
        if mode & 0o022:
            raise ConfigError("config must not be group/world writable")
        if (
            before.st_nlink != 1
            or before.st_size <= 0
            or before.st_size > _MAX_CONFIG_BYTES
        ):
            raise ConfigError("config metadata is unsafe")
        chunks: list[bytes] = []
        remaining = _MAX_CONFIG_BYTES + 1
        while remaining:
            chunk = os.read(fd, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        after = os.fstat(fd)
        before_identity = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mode,
            before.st_uid,
            before.st_gid,
            before.st_nlink,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        after_identity = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mode,
            after.st_uid,
            after.st_gid,
            after.st_nlink,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        if len(payload) != before.st_size or after_identity != before_identity:
            raise ConfigError("config changed while it was read")
    except OSError as exc:
        failure = ConfigError("config could not be read safely")
        failure.__cause__ = exc
    except ConfigError as exc:
        failure = exc
    finally:
        try:
            os.close(fd)
        except OSError as exc:
            if failure is None:
                failure = ConfigError("config descriptor could not be closed safely")
                failure.__cause__ = exc
    if failure is not None:
        raise failure
    return payload


@dataclass(frozen=True, slots=True)
class OperatorConfig:
    schema_version: SchemaVersion
    service_user: ServiceUser
    operator_group: OperatorGroup
    remote_url: str
    target_ref: str
    runner_repo: Path
    state_root: Path
    runtime_root: Path
    rollout_root: Path
    kubeconfig_path: Path
    cluster_config_path: Path
    admin_token_source: str
    worker_token_source: str
    service_token_source: str
    expect_admin_token_fingerprint: str
    cluster_name: str
    namespace: str
    environment: Environment
    cp_url: str
    smoke_on_behalf_username: str
    smoke_on_behalf_team_id: str
    scope: RolloutScope
    gb10_prep_concurrency: int
    config_path: Path = Path("/etc/loom/staging-rollout.toml")
    config_sha256: str = "0" * 64

    @classmethod
    def load(
        cls,
        path: Path,
        *,
        expected_owner_uid: int | None = None,
        expected_owner_gid: int | None = None,
        expected_mode: int | None = None,
    ) -> OperatorConfig:
        installed_authority = (
            expected_owner_uid is None
            and expected_owner_gid is None
            and expected_mode is None
        )
        if expected_owner_uid is None:
            expected_owner_uid = 0
        if installed_authority:
            try:
                account = pwd.getpwnam(APPROVED_SERVICE_USER)
                group_gid = grp.getgrnam(APPROVED_SERVICE_USER).gr_gid
            except KeyError as exc:
                raise ConfigError("approved service account is unavailable") from exc
            if account.pw_uid <= 0:
                raise ConfigError("approved service account UID is invalid")
            if account.pw_gid != group_gid or group_gid <= 0:
                raise ConfigError("approved service account primary group is invalid")
            expected_owner_gid = group_gid
            expected_mode = 0o640
        if expected_owner_uid is None:  # pragma: no cover - narrowed above
            raise ConfigError("config owner authority is unavailable")
        payload = _read_protected_config(
            path,
            expected_owner_uid,
            expected_owner_gid=expected_owner_gid,
            expected_mode=expected_mode,
            validate_parent_authority=installed_authority,
        )
        try:
            decoded = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ConfigError("config must be valid UTF-8") from exc
        try:
            loaded = tomllib.loads(decoded)
        except tomllib.TOMLDecodeError as exc:
            raise ConfigError(f"invalid TOML config: {exc}") from exc
        if not isinstance(loaded, dict):
            raise ConfigError("config must be a TOML table")
        raw = cast(dict[str, object], loaded)

        actual_keys = set(raw)
        unknown = sorted(actual_keys - _CONFIG_KEYS)
        missing = sorted(_CONFIG_KEYS - actual_keys)
        if unknown:
            raise ConfigError(f"unknown config keys: {unknown}")
        if missing:
            raise ConfigError(f"missing config keys: {missing}")

        schema_version = raw["schema_version"]
        if type(schema_version) is not int or schema_version != 1:
            raise ConfigError("schema_version must be 1")

        service_user = _require_literal(raw, "service_user", APPROVED_SERVICE_USER)
        operator_group = _require_literal(raw, "operator_group", APPROVED_OPERATOR_GROUP)
        remote_url = _require_literal(raw, "remote_url", APPROVED_REMOTE_URL)
        target_ref = _require_literal(raw, "target_ref", APPROVED_FETCH_REF)
        cluster_name = _require_literal(raw, "cluster_name", APPROVED_CLUSTER_NAME)
        namespace = _require_literal(raw, "namespace", APPROVED_NAMESPACE)
        environment = _require_literal(raw, "environment", APPROVED_ENVIRONMENT)
        cp_url = _require_literal(raw, "cp_url", APPROVED_CP_URL)
        scope = _require_literal(raw, "scope", APPROVED_SCOPE)

        fingerprint = _require_string(raw, "expect_admin_token_fingerprint")
        if _FINGERPRINT_RE.fullmatch(fingerprint) is None:
            raise ConfigError(
                "expect_admin_token_fingerprint must match sha256:<12-hex> len=<positive-integer>"
            )

        smoke_username = _require_string(raw, "smoke_on_behalf_username")
        if _USERNAME_RE.fullmatch(smoke_username) is None:
            raise ConfigError("smoke_on_behalf_username must be a safe username")
        smoke_team_id = _require_string(raw, "smoke_on_behalf_team_id")

        concurrency = raw["gb10_prep_concurrency"]
        if type(concurrency) is not int or not 1 <= concurrency <= 15:
            raise ConfigError("gb10_prep_concurrency must be an integer between 1 and 15")

        return cls(
            schema_version=1,
            service_user=cast(ServiceUser, service_user),
            operator_group=cast(OperatorGroup, operator_group),
            remote_url=remote_url,
            target_ref=target_ref,
            runner_repo=_require_absolute_path(raw, "runner_repo"),
            state_root=_require_absolute_path(raw, "state_root"),
            runtime_root=_require_absolute_path(raw, "runtime_root"),
            rollout_root=_require_absolute_path(raw, "rollout_root"),
            kubeconfig_path=_require_absolute_path(raw, "kubeconfig_path"),
            cluster_config_path=_require_absolute_path(raw, "cluster_config_path"),
            admin_token_source=_require_file_source(raw, "admin_token_source"),
            worker_token_source=_require_file_source(raw, "worker_token_source"),
            service_token_source=_require_file_source(raw, "service_token_source"),
            expect_admin_token_fingerprint=fingerprint,
            cluster_name=cluster_name,
            namespace=namespace,
            environment=cast(Environment, environment),
            cp_url=cp_url,
            smoke_on_behalf_username=smoke_username,
            smoke_on_behalf_team_id=smoke_team_id,
            scope=cast(RolloutScope, scope),
            gb10_prep_concurrency=concurrency,
            config_path=path,
            config_sha256=hashlib.sha256(payload).hexdigest(),
        )


__all__ = ["APPROVED_REMOTE_URL", "ConfigError", "OperatorConfig"]
