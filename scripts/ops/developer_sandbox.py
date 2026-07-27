#!/usr/bin/env python3
"""Plan and safely operate one candidate-bound developer Compose sandbox.

Mutation commands are dry-run by default. Pass ``--execute`` to create,
update, check, or destroy a stack on the current host. The script never opens
an SSH session and never resolves or prints secret values.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import stat
import subprocess
import sys
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from loom.admin_secret import AdminSecretConfigError, AdminSecretVerifier

ALLOWED_SANDBOXES = ("qianyi", "hongjian", "devansh")
EXPECTED_SSH_TARGET = "oldlab-2"
EXPECTED_CANONICAL_HOSTNAME = "trt-eai-oldlab-2"
PROFILE_SCHEMA_VERSION = 1
STATE_SCHEMA_VERSION = 1
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_PROJECT_RE = re.compile(r"^loom-sandbox-([a-z][a-z0-9-]*)$")
_DATABASE_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
_PORT_FIELDS = (
    "postgres",
    "minio",
    "minio_console",
    "control_plane",
    "loom_service",
    "llm_gateway",
    "egress_xds",
    "egress_proxy",
    "egress_admin",
    "web",
)
_TOP_LEVEL_FIELDS = {
    "schema_version",
    "sandbox",
    "ssh_target",
    "canonical_hostname",
    "compose_project",
    "bind_address",
    "provider_connection_namespace",
    "candidate_root",
    "state_root",
    "cache_root",
    "evidence_root",
    "runtime_root",
    "ports",
    "database",
    "object_store",
}
_REQUIRED_SECRET_ENV_KEYS = frozenset(
    {
        "LOOM_DEV_POSTGRES_USER",
        "LOOM_DEV_POSTGRES_PASSWORD",
        "LOOM_DEV_MINIO_ROOT_USER",
        "LOOM_DEV_MINIO_ROOT_PASSWORD",
        "LOOM_CP_STEP_JWT_SIGNING_KEY",
        "LOOM_SECRET_STORE_MASTER_KEY",
        "LOOM_WORKER_TOKEN",
    }
)
_EXPECTED_SERVICES = frozenset(
    {
        "postgres",
        "minio",
        "llm-gateway",
        "control-plane",
        "loom-service",
        "worker",
        "egress-xds",
        "egress-proxy",
        "web",
    }
)


class SandboxProfileError(ValueError):
    """The checked-in sandbox profile is malformed or colliding."""


class SandboxOperationError(RuntimeError):
    """A bounded, secret-safe sandbox operation failure."""


@dataclass(frozen=True, slots=True)
class SandboxPorts:
    postgres: int
    minio: int
    minio_console: int
    control_plane: int
    loom_service: int
    llm_gateway: int
    egress_xds: int
    egress_proxy: int
    egress_admin: int
    web: int

    def values(self) -> tuple[int, ...]:
        return tuple(getattr(self, field) for field in _PORT_FIELDS)


@dataclass(frozen=True, slots=True)
class SandboxProfile:
    schema_version: int
    sandbox: str
    ssh_target: str
    canonical_hostname: str
    compose_project: str
    bind_address: str
    provider_connection_namespace: str
    candidate_root: Path
    state_root: Path
    cache_root: Path
    evidence_root: Path
    runtime_root: Path
    ports: SandboxPorts
    database_name: str
    task_bucket: str
    trajectories_bucket: str
    artifacts_bucket: str

    def validate(self) -> None:
        if self.schema_version != PROFILE_SCHEMA_VERSION:
            raise SandboxProfileError(
                f"schema_version must be {PROFILE_SCHEMA_VERSION}",
            )
        if self.sandbox not in ALLOWED_SANDBOXES:
            raise SandboxProfileError(
                f"sandbox must be one of {list(ALLOWED_SANDBOXES)}",
            )
        if self.ssh_target != EXPECTED_SSH_TARGET:
            raise SandboxProfileError(
                f"ssh_target must be {EXPECTED_SSH_TARGET!r}",
            )
        if self.canonical_hostname != EXPECTED_CANONICAL_HOSTNAME:
            raise SandboxProfileError(
                f"canonical_hostname must be {EXPECTED_CANONICAL_HOSTNAME!r}",
            )
        if self.compose_project != f"loom-sandbox-{self.sandbox}":
            raise SandboxProfileError("compose_project must be bound to sandbox identity")
        if _PROJECT_RE.fullmatch(self.compose_project) is None:
            raise SandboxProfileError("compose_project is not a safe Compose project name")
        if self.bind_address != "127.0.0.1":
            raise SandboxProfileError("bind_address must remain private loopback")
        if self.provider_connection_namespace != f"sandbox-{self.sandbox}":
            raise SandboxProfileError(
                "provider_connection_namespace must be bound to sandbox identity",
            )
        if self.database_name != f"loom_sandbox_{self.sandbox}":
            raise SandboxProfileError("database.name must be bound to sandbox identity")
        if _DATABASE_RE.fullmatch(self.database_name) is None:
            raise SandboxProfileError("database.name is invalid")

        expected_buckets = {
            "task_bucket": f"loom-sandbox-{self.sandbox}-tasks",
            "trajectories_bucket": f"loom-sandbox-{self.sandbox}-trajectories",
            "artifacts_bucket": f"loom-sandbox-{self.sandbox}-artifacts",
        }
        for field, expected in expected_buckets.items():
            value = getattr(self, field)
            if value != expected or _BUCKET_RE.fullmatch(value) is None:
                raise SandboxProfileError(f"object_store.{field} is invalid")

        roots = {
            "candidate_root": self.candidate_root,
            "state_root": self.state_root,
            "cache_root": self.cache_root,
            "evidence_root": self.evidence_root,
            "runtime_root": self.runtime_root,
        }
        for field, path in roots.items():
            if not path.is_absolute() or ".." in path.parts:
                raise SandboxProfileError(f"{field} must be an absolute normalized path")
        if (
            self.candidate_root.name != self.sandbox
            or self.candidate_root.parent.name != "sandboxes"
        ):
            raise SandboxProfileError("candidate_root must end in sandboxes/<sandbox>")
        if (
            self.state_root.name != self.sandbox
            or self.state_root.parent.name != "developer-sandboxes"
        ):
            raise SandboxProfileError(
                "state_root must end in developer-sandboxes/<sandbox>",
            )
        for field in ("cache_root", "evidence_root", "runtime_root"):
            path = getattr(self, field)
            if path.parent != self.state_root or path.name != field.removesuffix("_root"):
                raise SandboxProfileError(f"{field} must be a direct state_root child")
        if len(set(roots.values())) != len(roots):
            raise SandboxProfileError("sandbox roots must be distinct")

        ports = self.ports.values()
        if any(type(port) is not int or not 1 <= port <= 65535 for port in ports):
            raise SandboxProfileError("all ports must be integers in 1..65535")
        if len(set(ports)) != len(ports):
            raise SandboxProfileError("ports must be distinct within a sandbox")


@dataclass(frozen=True, slots=True)
class CandidateBinding:
    sha: str
    tree: str
    source_repo: Path


@dataclass(frozen=True, slots=True)
class SandboxCommand:
    argv: tuple[str, ...]
    purpose: str


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


class CommandRunner(Protocol):
    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str] | None = None,
    ) -> CommandResult: ...


class SubprocessCommandRunner:
    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str] | None = None,
    ) -> CommandResult:
        completed = subprocess.run(
            list(argv),
            cwd=cwd,
            env=dict(env) if env is not None else None,
            check=False,
            capture_output=True,
            text=True,
        )
        return CommandResult(
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )


def local_canonical_hostname() -> str:
    return socket.gethostname().split(".", 1)[0]


def _reject_unknown(raw: dict[str, Any], allowed: set[str], context: str) -> None:
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise SandboxProfileError(f"{context} has unknown fields: {', '.join(unknown)}")


def _required_table(raw: dict[str, Any], key: str, context: str) -> dict[str, Any]:
    value = raw.get(key)
    if not isinstance(value, dict):
        raise SandboxProfileError(f"{context}.{key} must be a table")
    return dict(value)


def _required_string(raw: dict[str, Any], key: str, context: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value or value != value.strip():
        raise SandboxProfileError(f"{context}.{key} must be a non-empty trimmed string")
    return value


def _required_int(raw: dict[str, Any], key: str, context: str) -> int:
    value = raw.get(key)
    if type(value) is not int:
        raise SandboxProfileError(f"{context}.{key} must be an integer")
    return value


def load_profile(path: Path) -> SandboxProfile:
    try:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    except OSError as exc:
        raise SandboxProfileError(f"could not read sandbox profile: {path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise SandboxProfileError(f"invalid sandbox profile TOML: {path}") from exc
    _reject_unknown(raw, _TOP_LEVEL_FIELDS, str(path))

    ports_raw = _required_table(raw, "ports", str(path))
    _reject_unknown(ports_raw, set(_PORT_FIELDS), f"{path}.ports")
    if set(ports_raw) != set(_PORT_FIELDS):
        missing = sorted(set(_PORT_FIELDS) - set(ports_raw))
        raise SandboxProfileError(f"{path}.ports missing fields: {', '.join(missing)}")
    database = _required_table(raw, "database", str(path))
    _reject_unknown(database, {"name"}, f"{path}.database")
    object_store = _required_table(raw, "object_store", str(path))
    bucket_fields = {"task_bucket", "trajectories_bucket", "artifacts_bucket"}
    _reject_unknown(object_store, bucket_fields, f"{path}.object_store")

    profile = SandboxProfile(
        schema_version=_required_int(raw, "schema_version", str(path)),
        sandbox=_required_string(raw, "sandbox", str(path)),
        ssh_target=_required_string(raw, "ssh_target", str(path)),
        canonical_hostname=_required_string(
            raw,
            "canonical_hostname",
            str(path),
        ),
        compose_project=_required_string(raw, "compose_project", str(path)),
        bind_address=_required_string(raw, "bind_address", str(path)),
        provider_connection_namespace=_required_string(
            raw,
            "provider_connection_namespace",
            str(path),
        ),
        candidate_root=Path(_required_string(raw, "candidate_root", str(path))),
        state_root=Path(_required_string(raw, "state_root", str(path))),
        cache_root=Path(_required_string(raw, "cache_root", str(path))),
        evidence_root=Path(_required_string(raw, "evidence_root", str(path))),
        runtime_root=Path(_required_string(raw, "runtime_root", str(path))),
        ports=SandboxPorts(
            **{
                field: _required_int(ports_raw, field, f"{path}.ports")
                for field in _PORT_FIELDS
            },
        ),
        database_name=_required_string(database, "name", f"{path}.database"),
        task_bucket=_required_string(
            object_store,
            "task_bucket",
            f"{path}.object_store",
        ),
        trajectories_bucket=_required_string(
            object_store,
            "trajectories_bucket",
            f"{path}.object_store",
        ),
        artifacts_bucket=_required_string(
            object_store,
            "artifacts_bucket",
            f"{path}.object_store",
        ),
    )
    profile.validate()
    return profile


def load_profiles(profiles_dir: Path) -> tuple[SandboxProfile, ...]:
    if not profiles_dir.is_dir():
        raise SandboxProfileError(f"profiles directory not found: {profiles_dir}")
    profiles = tuple(load_profile(path) for path in sorted(profiles_dir.glob("*.toml")))
    names = tuple(profile.sandbox for profile in profiles)
    if set(names) != set(ALLOWED_SANDBOXES) or len(names) != len(ALLOWED_SANDBOXES):
        raise SandboxProfileError(
            f"profiles must define exactly {list(ALLOWED_SANDBOXES)}",
        )
    _require_distinct(profiles, "compose_project")
    _require_distinct(profiles, "provider_connection_namespace")
    _require_distinct(profiles, "database_name")
    for field in (
        "candidate_root",
        "state_root",
        "cache_root",
        "evidence_root",
        "runtime_root",
        "task_bucket",
        "trajectories_bucket",
        "artifacts_bucket",
    ):
        _require_distinct(profiles, field)
    all_ports = [port for profile in profiles for port in profile.ports.values()]
    if len(set(all_ports)) != len(all_ports):
        raise SandboxProfileError("host ports must be distinct across all sandboxes")
    return profiles


def _require_distinct(profiles: Sequence[SandboxProfile], field: str) -> None:
    values = [getattr(profile, field) for profile in profiles]
    if len(set(values)) != len(values):
        raise SandboxProfileError(f"{field} must be distinct across sandboxes")


def _run_checked(
    runner: CommandRunner,
    argv: Sequence[str],
    *,
    cwd: Path,
    purpose: str,
    env: Mapping[str, str] | None = None,
) -> CommandResult:
    result = runner.run(argv, cwd=cwd, env=env)
    if result.returncode != 0:
        raise SandboxOperationError(
            f"{purpose} failed safely with exit code {result.returncode}",
        )
    return result


def bind_candidate(
    source_repo: Path,
    candidate_sha: str,
    *,
    expected_source_repo: Path | None = None,
    runner: CommandRunner,
) -> CandidateBinding:
    if _SHA_RE.fullmatch(candidate_sha) is None:
        raise SandboxOperationError("candidate SHA must be full lowercase 40-hex")
    try:
        resolved_repo = source_repo.resolve(strict=True)
    except OSError as exc:
        raise SandboxOperationError("candidate source repository is unavailable") from exc
    if not resolved_repo.is_dir():
        raise SandboxOperationError("candidate source repository is not a directory")
    if (
        expected_source_repo is not None
        and resolved_repo != expected_source_repo.resolve(strict=False)
    ):
        raise SandboxOperationError(
            "candidate source must be materialized at candidate_root/<sha>",
        )
    head = _run_checked(
        runner,
        ("git", "rev-parse", "--verify", "HEAD"),
        cwd=resolved_repo,
        purpose="candidate HEAD verification",
    ).stdout.strip()
    resolved = _run_checked(
        runner,
        ("git", "rev-parse", "--verify", f"{candidate_sha}^{{commit}}"),
        cwd=resolved_repo,
        purpose="candidate commit verification",
    ).stdout.strip()
    if head != candidate_sha or resolved != candidate_sha:
        raise SandboxOperationError("candidate source is not at the exact requested SHA")
    status_result = _run_checked(
        runner,
        ("git", "status", "--porcelain=v1", "--untracked-files=all"),
        cwd=resolved_repo,
        purpose="candidate cleanliness verification",
    )
    if status_result.stdout.strip():
        raise SandboxOperationError("candidate source worktree is not clean")
    tree = _run_checked(
        runner,
        ("git", "rev-parse", "--verify", "HEAD^{tree}"),
        cwd=resolved_repo,
        purpose="candidate tree verification",
    ).stdout.strip()
    if _SHA_RE.fullmatch(tree) is None:
        raise SandboxOperationError("candidate tree identity is invalid")
    return CandidateBinding(sha=candidate_sha, tree=tree, source_repo=resolved_repo)


def _secure_regular_file(path: Path, *, label: str) -> Path:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise SandboxOperationError(f"{label} is unavailable") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise SandboxOperationError(f"{label} must be a regular non-symlink file")
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise SandboxOperationError(f"{label} must not be group/world accessible")
    return path.resolve(strict=True)


def validate_secret_files(secrets_env: Path, admin_secret_file: Path) -> tuple[Path, Path]:
    env_path = _secure_regular_file(secrets_env, label="secrets env file")
    admin_path = _secure_regular_file(admin_secret_file, label="admin secret file")
    keys: set[str] = set()
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        if key in _REQUIRED_SECRET_ENV_KEYS and value:
            keys.add(key)
    missing = sorted(_REQUIRED_SECRET_ENV_KEYS - keys)
    if missing:
        raise SandboxOperationError(
            f"secrets env file is missing required keys: {', '.join(missing)}",
        )
    try:
        payload = tomllib.loads(admin_path.read_text(encoding="utf-8"))
        admin = payload.get("admin")
        token = admin.get("token") if isinstance(admin, dict) else None
        if not isinstance(token, str):
            raise AdminSecretConfigError("missing admin.token")
        AdminSecretVerifier.from_token(token)
    except (OSError, tomllib.TOMLDecodeError, AdminSecretConfigError, ValueError) as exc:
        raise SandboxOperationError("admin secret file is invalid") from exc
    return env_path, admin_path


def _compose_environment(
    profile: SandboxProfile,
    binding: CandidateBinding,
    admin_secret_file: Path,
) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "COMPOSE_PROJECT_NAME": profile.compose_project,
            "LOOM_DEV_SANDBOX_NAME": profile.sandbox,
            "LOOM_DEV_CANDIDATE_SHA": binding.sha,
            "LOOM_DEV_IMAGE_TAG": f"sandbox-{profile.sandbox}-{binding.sha[:12]}",
            "LOOM_DEV_BIND_ADDR": profile.bind_address,
            "LOOM_DEV_POSTGRES_DB": profile.database_name,
            "LOOM_DEV_TASK_BUCKET": profile.task_bucket,
            "LOOM_DEV_TRAJECTORIES_BUCKET": profile.trajectories_bucket,
            "LOOM_DEV_ARTIFACTS_BUCKET": profile.artifacts_bucket,
            "LOOM_DEV_PROVIDER_CONNECTION_NAMESPACE": (
                profile.provider_connection_namespace
            ),
            "LOOM_DEV_ADMIN_SECRET_FILE": str(admin_secret_file),
        },
    )
    for field in _PORT_FIELDS:
        environment[f"LOOM_DEV_{field.upper()}_PORT"] = str(
            getattr(profile.ports, field),
        )
    return environment


def _compose_prefix(
    profile: SandboxProfile,
    binding: CandidateBinding,
    secrets_env: Path,
) -> tuple[str, ...]:
    compose_file = binding.source_repo / "deploy/docker-compose.dev.yml"
    if not compose_file.is_file():
        raise SandboxOperationError("candidate Compose file is unavailable")
    return (
        "docker",
        "compose",
        "--project-name",
        profile.compose_project,
        "--env-file",
        str(secrets_env),
        "-f",
        str(compose_file),
    )


def build_commands(
    operation: str,
    *,
    profile: SandboxProfile,
    binding: CandidateBinding,
    secrets_env: Path,
    delete_volumes: bool = False,
) -> tuple[SandboxCommand, ...]:
    prefix = _compose_prefix(profile, binding, secrets_env)
    config = SandboxCommand((*prefix, "config", "--quiet"), "validate Compose config")
    if operation in {"create", "update"}:
        dependency_up = SandboxCommand(
            (*prefix, "up", "-d", "postgres", "minio"),
            "start stateful dependencies",
        )
        migration = SandboxCommand(
            (
                *prefix,
                "run",
                "--rm",
                "--no-deps",
                "--build",
                "control-plane",
                "sh",
                "-eu",
                "-c",
                'export LOOM_DB_URL="$LOOM_CP_DB_URL"; '
                "python -m alembic -c migrations/alembic.ini upgrade head",
            ),
            "apply database migrations",
        )
        up_args = [*prefix, "up", "-d", "--build", "--remove-orphans"]
        if operation == "update":
            up_args.append("--force-recreate")
        return (
            config,
            dependency_up,
            migration,
            SandboxCommand(tuple(up_args), f"{operation} sandbox stack"),
        )
    if operation == "check":
        return (
            config,
            SandboxCommand(
                (*prefix, "ps", "--all", "--format", "json"),
                "inspect sandbox services",
            ),
        )
    if operation == "destroy":
        args = [*prefix, "down", "--remove-orphans"]
        if delete_volumes:
            args.append("--volumes")
        return (config, SandboxCommand(tuple(args), "destroy sandbox stack"))
    raise SandboxOperationError(f"unsupported operation: {operation}")


def _state_path(profile: SandboxProfile) -> Path:
    return profile.state_root / "sandbox-state.json"


def _load_state(profile: SandboxProfile) -> dict[str, Any] | None:
    path = _state_path(profile)
    if not path.exists():
        return None
    _secure_regular_file(path, label="sandbox state")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SandboxOperationError("sandbox state is invalid") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != STATE_SCHEMA_VERSION
        or payload.get("sandbox") != profile.sandbox
        or payload.get("compose_project") != profile.compose_project
        or _SHA_RE.fullmatch(str(payload.get("candidate_sha", ""))) is None
        or _SHA_RE.fullmatch(str(payload.get("candidate_tree", ""))) is None
    ):
        raise SandboxOperationError("sandbox state binding is invalid")
    return payload


def _ensure_private_directory(path: Path) -> None:
    path.mkdir(parents=True, mode=0o700, exist_ok=True)
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise SandboxOperationError("sandbox private directory is unsafe")
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise SandboxOperationError("sandbox private directory must be mode 0700 or stricter")


def _atomic_json_write(path: Path, payload: dict[str, Any]) -> None:
    _ensure_private_directory(path.parent)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        temporary.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        temporary.chmod(0o600)
        os.replace(temporary, path)
        path.chmod(0o600)
    finally:
        if temporary.exists():
            temporary.unlink()


def _write_success_records(
    operation: str,
    *,
    profile: SandboxProfile,
    binding: CandidateBinding,
) -> None:
    timestamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    state = {
        "schema_version": STATE_SCHEMA_VERSION,
        "sandbox": profile.sandbox,
        "compose_project": profile.compose_project,
        "candidate_sha": binding.sha,
        "candidate_tree": binding.tree,
        "source_repo": str(binding.source_repo),
        "updated_at": timestamp,
    }
    _atomic_json_write(_state_path(profile), state)
    evidence = {
        **state,
        "operation": operation,
        "status": "succeeded",
    }
    _atomic_json_write(
        profile.evidence_root / f"{timestamp.replace(':', '')}-{operation}.json",
        evidence,
    )


def _remove_state(profile: SandboxProfile) -> None:
    path = _state_path(profile)
    _secure_regular_file(path, label="sandbox state")
    path.unlink()


def _decode_compose_ps(stdout: str) -> list[dict[str, Any]]:
    try:
        decoded = json.loads(stdout)
    except json.JSONDecodeError:
        rows: list[dict[str, Any]] = []
        for line in stdout.splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SandboxOperationError("Compose status output is invalid") from exc
            if not isinstance(row, dict):
                raise SandboxOperationError("Compose status output is invalid") from None
            rows.append(row)
        return rows
    if isinstance(decoded, dict):
        return [decoded]
    if isinstance(decoded, list) and all(isinstance(row, dict) for row in decoded):
        return decoded
    raise SandboxOperationError("Compose status output is invalid")


def _validate_runtime_status(result: CommandResult) -> None:
    rows = _decode_compose_ps(result.stdout)
    running = {
        str(row.get("Service"))
        for row in rows
        if str(row.get("State", "")).lower() == "running"
        and str(row.get("Health", "")).lower() not in {"unhealthy", "starting"}
    }
    missing = sorted(_EXPECTED_SERVICES - running)
    if missing:
        raise SandboxOperationError(
            f"sandbox services are not healthy/running: {', '.join(missing)}",
        )


def _plan_document(
    operation: str,
    *,
    profile: SandboxProfile,
    binding: CandidateBinding,
    secrets_env: Path,
    admin_secret_file: Path,
    commands: Sequence[SandboxCommand],
    delete_volumes: bool,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "artifact_type": "developer-sandbox-plan",
        "operation": operation,
        "mutation_authorized": False,
        "sandbox": profile.sandbox,
        "ssh_target": profile.ssh_target,
        "canonical_hostname": profile.canonical_hostname,
        "compose_project": profile.compose_project,
        "candidate_sha": binding.sha,
        "candidate_tree": binding.tree,
        "candidate_path": str(profile.candidate_root / binding.sha),
        "source_repo": str(binding.source_repo),
        "secrets_env_file": str(secrets_env),
        "admin_secret_file": str(admin_secret_file),
        "delete_volumes": delete_volumes,
        "ports": asdict(profile.ports),
        "database_name": profile.database_name,
        "object_store": {
            "task_bucket": profile.task_bucket,
            "trajectories_bucket": profile.trajectories_bucket,
            "artifacts_bucket": profile.artifacts_bucket,
        },
        "provider_connection_namespace": profile.provider_connection_namespace,
        "roots": {
            "state": str(profile.state_root),
            "cache": str(profile.cache_root),
            "evidence": str(profile.evidence_root),
            "runtime": str(profile.runtime_root),
        },
        "commands": [
            {"purpose": command.purpose, "argv": list(command.argv)}
            for command in commands
        ],
    }


def operate(
    operation: str,
    *,
    profile_path: Path,
    source_repo: Path,
    candidate_sha: str,
    secrets_env: Path,
    admin_secret_file: Path,
    execute: bool,
    delete_volumes: bool,
    runner: CommandRunner,
    canonical_hostname: Callable[[], str] = local_canonical_hostname,
) -> dict[str, Any]:
    selected = load_profile(profile_path)
    profiles = load_profiles(profile_path.parent)
    matches = [profile for profile in profiles if profile.sandbox == profile_path.stem]
    if len(matches) != 1 or selected != matches[0]:
        raise SandboxOperationError("profile filename must match sandbox identity")
    profile = selected
    binding = bind_candidate(
        source_repo,
        candidate_sha,
        expected_source_repo=(
            profile.candidate_root / candidate_sha
            if operation in {"create", "update"}
            else None
        ),
        runner=runner,
    )
    resolved_env, resolved_admin = validate_secret_files(secrets_env, admin_secret_file)
    state = _load_state(profile)
    if operation == "create" and state is not None:
        raise SandboxOperationError("sandbox already exists; use update")
    if operation in {"update", "check", "destroy"}:
        if state is None:
            raise SandboxOperationError("sandbox state is absent; use create")
        if operation in {"check", "destroy"} and state["candidate_sha"] != binding.sha:
            raise SandboxOperationError("requested candidate does not match sandbox state")
    commands = build_commands(
        operation,
        profile=profile,
        binding=binding,
        secrets_env=resolved_env,
        delete_volumes=delete_volumes,
    )
    plan = _plan_document(
        operation,
        profile=profile,
        binding=binding,
        secrets_env=resolved_env,
        admin_secret_file=resolved_admin,
        commands=commands,
        delete_volumes=delete_volumes,
    )
    if not execute:
        return plan

    actual_host = canonical_hostname()
    normalized_host = actual_host.rstrip(".").lower()
    if normalized_host != profile.canonical_hostname:
        raise SandboxOperationError(
            "execution host must have canonical hostname "
            f"{profile.canonical_hostname!r}, got {actual_host!r}",
        )
    if operation in {"create", "update"}:
        for root in (
            profile.state_root,
            profile.cache_root,
            profile.evidence_root,
            profile.runtime_root,
        ):
            _ensure_private_directory(root)
    environment = _compose_environment(profile, binding, resolved_admin)
    results: list[CommandResult] = []
    for command in commands:
        results.append(
            _run_checked(
                runner,
                command.argv,
                cwd=binding.source_repo,
                purpose=command.purpose,
                env=environment,
            ),
        )
    if operation == "check":
        _validate_runtime_status(results[-1])
    elif operation == "destroy":
        _remove_state(profile)
    else:
        _write_success_records(
            operation,
            profile=profile,
            binding=binding,
        )
    return {**plan, "mutation_authorized": True, "status": "succeeded"}


def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--source-repo", type=Path, required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--secrets-env", type=Path, required=True)
    parser.add_argument("--admin-secret-file", type=Path, required=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan", help="Render a mutation-free operation plan")
    plan.add_argument(
        "--operation",
        choices=("create", "update", "check", "destroy"),
        required=True,
    )
    _add_common_arguments(plan)
    plan.add_argument("--delete-volumes", action="store_true")
    for operation in ("create", "update", "check", "destroy"):
        command = subparsers.add_parser(operation)
        _add_common_arguments(command)
        command.add_argument(
            "--execute",
            action="store_true",
            help="Run commands on the current host; omitted means plan only.",
        )
        if operation == "destroy":
            command.add_argument(
                "--delete-volumes",
                action="store_true",
                help="Also delete this exact Compose project's named volumes.",
            )
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    runner: CommandRunner | None = None,
    canonical_hostname: Callable[[], str] | None = None,
) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    operation = args.operation if args.command == "plan" else args.command
    execute = bool(getattr(args, "execute", False)) and args.command != "plan"
    try:
        result = operate(
            operation,
            profile_path=args.profile,
            source_repo=args.source_repo,
            candidate_sha=args.candidate_sha,
            secrets_env=args.secrets_env,
            admin_secret_file=args.admin_secret_file,
            execute=execute,
            delete_volumes=bool(getattr(args, "delete_volumes", False)),
            runner=runner or SubprocessCommandRunner(),
            canonical_hostname=canonical_hostname or local_canonical_hostname,
        )
    except (SandboxOperationError, SandboxProfileError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    sys.stdout.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
