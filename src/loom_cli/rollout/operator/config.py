"""Strict root-owned configuration for protected rollout operators."""

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

from .model import APPROVED_REMOTE_URL, SchemaVersion

APPROVED_SERVICE_USER = "loom-rollout"
APPROVED_SCOPE = "current-gb10"
APPROVED_BACKUP_MAX_OBJECTS = 1_000_000
APPROVED_BACKUP_MAX_ENTRIES = 16_000_000
CANDIDATE_REPO_NAME = "repo"

ServiceUser = Literal["loom-rollout"]
EnvironmentShortName = Literal["dev", "staging", "prod"]
OperatorGroup = Literal[
    "loom-dev-operators",
    "loom-staging-operators",
    "loom-prod-operators",
]
Environment = Literal["development", "staging", "production"]
RolloutScope = Literal["current-gb10"]
CandidateSourceMode = Literal["merged-dev", "sealed-cumulative"]

_FINGERPRINT_RE = re.compile(r"^sha256:[0-9a-f]{12} len=[1-9][0-9]*$")
_USERNAME_RE = re.compile(r"^[a-z_][a-z0-9_-]{0,63}$")
_BASE_CONFIG_KEYS = frozenset(
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
        "backup_max_objects",
        "backup_max_entries",
    }
)
_SEALED_CONFIG_KEYS = frozenset(
    {"source_mode", "source_commit_sha", "source_tree_sha", "source_base_sha"}
)
# Keys allowed on any schema but never required — absence is a valid default.
_OPTIONAL_CONFIG_KEYS = frozenset({"ownership_maintenance_allowed"})
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_MAX_CONFIG_BYTES = 1 << 20


class ConfigError(ValueError):
    """Raised when the protected runner configuration fails closed."""


@dataclass(frozen=True, slots=True)
class RolloutEnvironmentAuthority:
    """Immutable installed bindings selected only by a reviewed short name."""

    short_name: EnvironmentShortName
    environment: Environment
    operator_group: OperatorGroup
    target_ref: str
    pinned_target_ref: str
    cluster_name: str
    namespace: str
    cp_url: str
    config_path: Path
    candidate_runtime_root: Path
    candidate_cluster_config: Path
    state_root: Path
    runtime_root: Path
    rollout_root: Path
    kubeconfig_path: Path
    client_path: Path = Path("/usr/local/bin/loom-rollout")
    broker_path: Path = Path("/usr/local/libexec/loom-rollout-broker")


_ENVIRONMENT_AUTHORITIES: dict[EnvironmentShortName, RolloutEnvironmentAuthority] = {
    "dev": RolloutEnvironmentAuthority(
        short_name="dev",
        environment="development",
        operator_group="loom-dev-operators",
        target_ref="refs/heads/dev",
        pinned_target_ref="origin/dev",
        cluster_name="loom-dev",
        namespace="loom-dev",
        cp_url="http://127.0.0.1:18081",
        config_path=Path("/etc/loom/dev-rollout.toml"),
        candidate_runtime_root=Path("/opt/loom-dev-runner/candidates"),
        candidate_cluster_config=Path("deploy/environments/development.cluster.toml"),
        state_root=Path("/var/lib/loom-dev-rollout"),
        runtime_root=Path("/run/loom-dev-rollout"),
        rollout_root=Path("/data/loom-dev"),
        kubeconfig_path=Path("/var/lib/loom-dev-rollout/kubeconfig"),
    ),
    "staging": RolloutEnvironmentAuthority(
        short_name="staging",
        environment="staging",
        operator_group="loom-staging-operators",
        target_ref="refs/heads/dev",
        pinned_target_ref="origin/dev",
        cluster_name="loom-staging",
        namespace="loom-staging",
        cp_url="http://127.0.0.1:18081",
        config_path=Path("/etc/loom/staging-rollout.toml"),
        candidate_runtime_root=Path("/opt/loom-staging-runner/candidates"),
        candidate_cluster_config=Path("deploy/environments/staging.multinode.cluster.toml"),
        state_root=Path("/var/lib/loom-staging-rollout"),
        runtime_root=Path("/run/loom-staging-rollout"),
        rollout_root=Path("/data/loom-staging"),
        kubeconfig_path=Path("/var/lib/loom-staging-rollout/kubeconfig"),
    ),
    "prod": RolloutEnvironmentAuthority(
        short_name="prod",
        environment="production",
        operator_group="loom-prod-operators",
        target_ref="refs/heads/main",
        pinned_target_ref="origin/main",
        cluster_name="loom-prod",
        namespace="loom-prod",
        cp_url="http://127.0.0.1:18081",
        config_path=Path("/etc/loom/prod-rollout.toml"),
        candidate_runtime_root=Path("/opt/loom-prod-runner/candidates"),
        candidate_cluster_config=Path("deploy/environments/production.cluster.toml"),
        state_root=Path("/var/lib/loom-prod-rollout"),
        runtime_root=Path("/run/loom-prod-rollout"),
        rollout_root=Path("/data/loom-prod"),
        kubeconfig_path=Path("/var/lib/loom-prod-rollout/kubeconfig"),
    ),
}

STAGING_AUTHORITY = _ENVIRONMENT_AUTHORITIES["staging"]
APPROVED_OPERATOR_GROUP = STAGING_AUTHORITY.operator_group
APPROVED_CLUSTER_NAME = STAGING_AUTHORITY.cluster_name
APPROVED_NAMESPACE = STAGING_AUTHORITY.namespace
APPROVED_ENVIRONMENT = STAGING_AUTHORITY.environment
APPROVED_CP_URL = STAGING_AUTHORITY.cp_url
CANDIDATE_RUNTIME_ROOT = STAGING_AUTHORITY.candidate_runtime_root
CANDIDATE_CLUSTER_CONFIG = STAGING_AUTHORITY.candidate_cluster_config


def environment_authority(short_name: str) -> RolloutEnvironmentAuthority:
    """Resolve one exact authority without aliases or normalization."""
    try:
        return _ENVIRONMENT_AUTHORITIES[cast(EnvironmentShortName, short_name)]
    except KeyError as exc:
        raise ConfigError("rollout environment must be one of: dev, staging, prod") from exc


def candidate_sha_from_runner_repo(
    path: Path,
    *,
    authority: RolloutEnvironmentAuthority | None = None,
) -> str:
    """Return the full SHA encoded by one installer-published candidate path."""
    candidate_runtime_root = (
        CANDIDATE_RUNTIME_ROOT if authority is None else authority.candidate_runtime_root
    )
    if (
        not path.is_absolute()
        or ".." in path.parts
        or path.name != CANDIDATE_REPO_NAME
        or path.parent.parent != candidate_runtime_root
        or _SHA_RE.fullmatch(path.parent.name) is None
    ):
        raise ConfigError(f"runner_repo must be {candidate_runtime_root}/<full-sha>/repo")
    return path.parent.name


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
        if before.st_nlink != 1 or before.st_size <= 0 or before.st_size > _MAX_CONFIG_BYTES:
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
    backup_max_objects: int = APPROVED_BACKUP_MAX_OBJECTS
    backup_max_entries: int = APPROVED_BACKUP_MAX_ENTRIES
    config_path: Path = Path("/etc/loom/staging-rollout.toml")
    config_sha256: str = "0" * 64
    source_mode: CandidateSourceMode = "merged-dev"
    source_commit_sha: str | None = None
    source_tree_sha: str | None = None
    source_base_sha: str | None = None
    short_name: EnvironmentShortName = "staging"
    ownership_maintenance_allowed: bool = False

    def ownership_maintenance_permitted(self) -> bool:
        """Whether this runner may run manifest-ownership maintenance.

        The integrity guarantee is *exactness* — the maintenance operator only
        ever runs against the exact installer-pinned candidate (enforced in
        ``installed_manifest_ownership``). The *authority* to run it is what this
        gate decides, as an explicit (version, policy) pair rather than the
        source-mode label (#1085 phase 3): a sealed-cumulative runner is always
        permitted (its whole identity is a reviewed frozen release), and any
        other runner is permitted only when its config explicitly opts in via
        ``ownership_maintenance_allowed``.
        """
        return self.source_mode == "sealed-cumulative" or self.ownership_maintenance_allowed

    @classmethod
    def load(
        cls,
        path: Path,
        *,
        authority: RolloutEnvironmentAuthority = STAGING_AUTHORITY,
        expected_owner_uid: int | None = None,
        expected_owner_gid: int | None = None,
        expected_mode: int | None = None,
    ) -> OperatorConfig:
        installed_authority = (
            expected_owner_uid is None and expected_owner_gid is None and expected_mode is None
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
            if path != authority.config_path:
                raise ConfigError(f"installed config path must be {authority.config_path}")
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

        schema_version = raw.get("schema_version")
        if type(schema_version) is not int or schema_version not in {1, 2}:
            raise ConfigError("schema_version must be 1 or 2")
        required_keys = (
            _BASE_CONFIG_KEYS if schema_version == 1 else _BASE_CONFIG_KEYS | _SEALED_CONFIG_KEYS
        )
        expected_keys = required_keys | _OPTIONAL_CONFIG_KEYS
        actual_keys = set(raw)
        unknown = sorted(actual_keys - expected_keys)
        missing = sorted(required_keys - actual_keys)
        if unknown:
            raise ConfigError(f"unknown config keys: {unknown}")
        if missing:
            raise ConfigError(f"missing config keys: {missing}")

        ownership_maintenance_allowed = raw.get("ownership_maintenance_allowed", False)
        if type(ownership_maintenance_allowed) is not bool:
            raise ConfigError("ownership_maintenance_allowed must be a boolean")

        source_mode: CandidateSourceMode = "merged-dev"
        source_commit_sha: str | None = None
        source_tree_sha: str | None = None
        source_base_sha: str | None = None
        if schema_version == 2:
            if _require_string(raw, "source_mode") != "sealed-cumulative":
                raise ConfigError("source_mode must be sealed-cumulative")
            source_mode = "sealed-cumulative"
            values = {
                "source_commit_sha": _require_string(raw, "source_commit_sha"),
                "source_tree_sha": _require_string(raw, "source_tree_sha"),
                "source_base_sha": _require_string(raw, "source_base_sha"),
            }
            if any(_SHA_RE.fullmatch(value) is None for value in values.values()):
                raise ConfigError("sealed source SHA/tree/base must be exact lowercase Git SHAs")
            source_commit_sha = values["source_commit_sha"]
            source_tree_sha = values["source_tree_sha"]
            source_base_sha = values["source_base_sha"]
            if len({source_commit_sha, source_tree_sha, source_base_sha}) != 3:
                raise ConfigError("sealed source SHA/tree/base identities must be distinct")

        service_user = _require_literal(raw, "service_user", APPROVED_SERVICE_USER)
        operator_group = _require_literal(raw, "operator_group", authority.operator_group)
        remote_url = _require_literal(raw, "remote_url", APPROVED_REMOTE_URL)
        target_ref = _require_literal(raw, "target_ref", authority.target_ref)
        cluster_name = _require_literal(raw, "cluster_name", authority.cluster_name)
        namespace = _require_literal(raw, "namespace", authority.namespace)
        environment = _require_literal(raw, "environment", authority.environment)
        cp_url = _require_literal(raw, "cp_url", authority.cp_url)
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

        backup_max_objects = raw["backup_max_objects"]
        if type(backup_max_objects) is not int or backup_max_objects != APPROVED_BACKUP_MAX_OBJECTS:
            raise ConfigError(
                f"backup_max_objects must be the reviewed staging policy value "
                f"{APPROVED_BACKUP_MAX_OBJECTS}"
            )
        backup_max_entries = raw["backup_max_entries"]
        if type(backup_max_entries) is not int or backup_max_entries != APPROVED_BACKUP_MAX_ENTRIES:
            raise ConfigError(
                f"backup_max_entries must be the reviewed staging policy value "
                f"{APPROVED_BACKUP_MAX_ENTRIES}"
            )

        runner_repo = _require_absolute_path(raw, "runner_repo")
        candidate_sha = candidate_sha_from_runner_repo(runner_repo, authority=authority)
        cluster_config_path = _require_absolute_path(raw, "cluster_config_path")
        if cluster_config_path != runner_repo / authority.candidate_cluster_config:
            raise ConfigError("cluster_config_path must belong to the exact candidate repo")
        if source_commit_sha is not None and source_commit_sha != candidate_sha:
            raise ConfigError("sealed source commit must match the candidate runtime path")

        protected_paths = {
            "state_root": authority.state_root,
            "runtime_root": authority.runtime_root,
            "rollout_root": authority.rollout_root,
            "kubeconfig_path": authority.kubeconfig_path,
        }
        resolved_paths = {key: _require_absolute_path(raw, key) for key in protected_paths}
        for key, expected_path in protected_paths.items():
            if resolved_paths[key] != expected_path:
                raise ConfigError(f"{key} must be {expected_path}")

        return cls(
            schema_version=1,
            service_user=cast(ServiceUser, service_user),
            operator_group=cast(OperatorGroup, operator_group),
            remote_url=remote_url,
            target_ref=target_ref,
            runner_repo=runner_repo,
            state_root=resolved_paths["state_root"],
            runtime_root=resolved_paths["runtime_root"],
            rollout_root=resolved_paths["rollout_root"],
            kubeconfig_path=resolved_paths["kubeconfig_path"],
            cluster_config_path=cluster_config_path,
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
            backup_max_objects=backup_max_objects,
            backup_max_entries=backup_max_entries,
            config_path=path,
            config_sha256=hashlib.sha256(payload).hexdigest(),
            source_mode=source_mode,
            source_commit_sha=source_commit_sha,
            source_tree_sha=source_tree_sha,
            source_base_sha=source_base_sha,
            short_name=authority.short_name,
            ownership_maintenance_allowed=ownership_maintenance_allowed,
        )


__all__ = [
    "APPROVED_BACKUP_MAX_ENTRIES",
    "APPROVED_BACKUP_MAX_OBJECTS",
    "APPROVED_REMOTE_URL",
    "CANDIDATE_CLUSTER_CONFIG",
    "CANDIDATE_REPO_NAME",
    "CANDIDATE_RUNTIME_ROOT",
    "STAGING_AUTHORITY",
    "ConfigError",
    "EnvironmentShortName",
    "OperatorConfig",
    "RolloutEnvironmentAuthority",
    "candidate_sha_from_runner_repo",
    "environment_authority",
]
