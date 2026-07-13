"""Strict root-owned configuration for the staging rollout operator."""

from __future__ import annotations

import hashlib
import os
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


def _read_protected_config(path: Path, expected_owner_uid: int) -> bytes:
    if not path.is_absolute():
        raise ConfigError("config path must be absolute")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise ConfigError(f"config must be a readable regular file, not a symlink: {path}") from exc

    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise ConfigError(f"config must be a regular file: {path}")
        if metadata.st_uid != expected_owner_uid:
            raise ConfigError(
                f"config owner UID {metadata.st_uid} does not match expected owner UID "
                f"{expected_owner_uid}"
            )
        if stat.S_IMODE(metadata.st_mode) & 0o022:
            raise ConfigError("config must not be group/world writable")
        with os.fdopen(fd, "rb", closefd=False) as handle:
            return handle.read()
    finally:
        os.close(fd)


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
        expected_owner_uid: int = 0,
    ) -> OperatorConfig:
        payload = _read_protected_config(path, expected_owner_uid)
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
