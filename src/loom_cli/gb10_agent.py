"""Host-local GB10 node-agent commands.

The agent is pull-based: each GB10 host fetches desired state from the Control
Plane, compares it with its local Docker Compose env file, and applies changes
locally. The Control Plane never receives host SSH credentials.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any

import httpx
from dotenv import dotenv_values

from loom_cli.admin_cmd import (
    _DEFAULT_ADMIN_TOKEN_SOURCE,
    _DEFAULT_CP_URL,
    _resolve_admin_token,
)
from loom_cli.secret_source import (
    SecretSourceError,
    resolve_secret_source,
    secret_source_argparse_type,
)

AGENT_VERSION = "gb10-agent-v1"
_SECRET_KEY_PARTS = (
    "TOKEN",
    "SECRET",
    "PASSWORD",
    "PASS",
    "API_KEY",
    "CREDENTIAL",
)
_SECRET_VALUE_PREFIXES = (
    "loom_w_",
    "loom_admin_",
    "sk-",
)


@dataclass(frozen=True)
class DesiredState:
    environment: str
    pool_name: str
    image_tag: str
    max_concurrent: int
    env_config_version: str
    rollout_policy: dict[str, Any]
    env: dict[str, str]
    source_git_commit: str | None = None
    target_slots: int | None = None
    host_intents: dict[str, str] | None = None
    force: bool = False
    previous_image_tag: str | None = None
    previous_max_concurrent: int | None = None
    previous_env_config_version: str | None = None
    previous_source_git_commit: str | None = None
    previous_env: dict[str, str] | None = None

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> DesiredState:
        return cls(
            environment=str(data["environment"]),
            pool_name=str(data["pool_name"]),
            image_tag=str(data["image_tag"]),
            max_concurrent=int(data["max_concurrent"]),
            env_config_version=str(data["env_config_version"]),
            source_git_commit=(
                str(data["source_git_commit"])
                if data.get("source_git_commit") is not None
                else None
            ),
            rollout_policy=dict(data.get("rollout_policy") or {}),
            env={str(k): str(v) for k, v in dict(data.get("env") or {}).items()},
            target_slots=(
                int(data["target_slots"]) if data.get("target_slots") is not None else None
            ),
            host_intents={str(k): str(v) for k, v in dict(data.get("host_intents") or {}).items()},
            force=bool(data.get("force", False)),
            previous_image_tag=data.get("previous_image_tag"),
            previous_max_concurrent=data.get("previous_max_concurrent"),
            previous_env_config_version=data.get("previous_env_config_version"),
            previous_source_git_commit=data.get("previous_source_git_commit"),
            previous_env={str(k): str(v) for k, v in dict(data.get("previous_env") or {}).items()},
        )

    def rollback_target(self) -> DesiredState:
        if (
            self.previous_image_tag is None
            or self.previous_max_concurrent is None
            or self.previous_env_config_version is None
        ):
            raise ValueError("desired state does not include a previous version")
        return DesiredState(
            environment=self.environment,
            pool_name=self.pool_name,
            image_tag=self.previous_image_tag,
            max_concurrent=int(self.previous_max_concurrent),
            env_config_version=self.previous_env_config_version,
            source_git_commit=self.previous_source_git_commit,
            rollout_policy={"mode": "all"},
            env=dict(self.previous_env or {}),
            target_slots=self.target_slots,
            host_intents=dict(self.host_intents or {}),
            force=True,
        )


@dataclass(frozen=True)
class LocalWorkerState:
    hostname: str
    image_tag: str
    pool_name: str
    max_concurrent: int
    env_config_version: str
    capacity_intent: str = "active"
    source_git_commit: str | None = None
    source_git_dirty: bool | None = None
    env: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class AgentPlan:
    environment: str
    pool_name: str
    hostname: str
    needs_apply: bool
    changes: list[str]
    blocked_reason: str | None
    desired: dict[str, Any]
    current: dict[str, Any]


def _env_int(values: Mapping[str, str | None], key: str, default: int) -> int:
    raw = values.get(key)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(str(raw))
    except ValueError as exc:
        raise ValueError(f"{key} must be an integer; got {raw!r}") from exc


def load_local_state(
    env_file: Path,
    *,
    hostname: str | None = None,
    source_dir: Path | None = None,
) -> LocalWorkerState:
    raw_values = dotenv_values(env_file)
    values = {str(key): str(value) for key, value in raw_values.items() if value is not None}
    raw_intent = str(values.get("LOOM_GB10_CAPACITY_INTENT") or "").strip()
    if not raw_intent:
        raw_intent = (
            "draining"
            if str(values.get("LOOM_WORKER_DRAIN") or "").strip() in {"1", "true", "yes"}
            else "active"
        )
    source_git_commit, source_git_dirty = _source_git_provenance(source_dir)
    return LocalWorkerState(
        hostname=hostname or str(values.get("LOOM_WORKER_HOSTNAME") or socket.gethostname()),
        image_tag=str(values.get("LOOM_IMAGE_TAG") or "dev"),
        pool_name=str(values.get("LOOM_WORKER_POOL_NAME") or "remote-worker"),
        max_concurrent=_env_int(values, "LOOM_WORKER_MAX_CONCURRENT", 5),
        env_config_version=str(values.get("LOOM_WORKER_ENV_CONFIG_VERSION") or ""),
        capacity_intent=raw_intent,
        source_git_commit=source_git_commit,
        source_git_dirty=source_git_dirty,
        env=values,
    )


def _can_apply_to_host(
    desired: DesiredState,
    local: LocalWorkerState,
    *,
    force: bool,
) -> str | None:
    if force or desired.force:
        return None
    policy = desired.rollout_policy or {}
    if policy.get("mode") != "canary":
        return None
    canary_hosts = {str(host) for host in policy.get("canary_hosts", [])}
    if local.hostname not in canary_hosts:
        return "waiting_for_canary"
    return None


def build_plan(
    desired: DesiredState,
    local: LocalWorkerState,
    *,
    force: bool = False,
) -> AgentPlan:
    changes: list[str] = []
    desired_intent = (desired.host_intents or {}).get(local.hostname, "active")
    if local.image_tag != desired.image_tag:
        changes.append("image_tag")
    if local.pool_name != desired.pool_name:
        changes.append("pool_name")
    if local.max_concurrent != desired.max_concurrent:
        changes.append("max_concurrent")
    if local.env_config_version != desired.env_config_version:
        changes.append("env_config_version")
    if desired.source_git_commit:
        if local.source_git_commit != desired.source_git_commit:
            changes.append("source_git_commit")
        elif local.source_git_dirty is not False:
            changes.append("source_git_dirty")
    if local.capacity_intent != desired_intent:
        changes.append("capacity_intent")
    for key, desired_value in sorted(desired.env.items()):
        if local.env.get(key) != desired_value:
            changes.append(f"env:{key}")
    blocked_reason = None
    if changes:
        blocked_reason = _can_apply_to_host(desired, local, force=force)
    return AgentPlan(
        environment=desired.environment,
        pool_name=desired.pool_name,
        hostname=local.hostname,
        needs_apply=bool(changes),
        changes=changes,
        blocked_reason=blocked_reason,
        desired={
            "image_tag": desired.image_tag,
            "pool_name": desired.pool_name,
            "max_concurrent": desired.max_concurrent,
            "env_config_version": desired.env_config_version,
            "capacity_intent": desired_intent,
            "source_git_commit": desired.source_git_commit,
            "source_git_dirty": False if desired.source_git_commit else None,
        },
        current={
            "image_tag": local.image_tag,
            "pool_name": local.pool_name,
            "max_concurrent": local.max_concurrent,
            "env_config_version": local.env_config_version,
            "capacity_intent": local.capacity_intent,
            "source_git_commit": local.source_git_commit,
            "source_git_dirty": local.source_git_dirty,
        },
    )


def render_env_updates(env_file: Path, updates: dict[str, str]) -> str:
    lines = env_file.read_text(encoding="utf-8").splitlines() if env_file.exists() else []
    remaining = dict(updates)
    rendered: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            rendered.append(line)
            continue
        key, _ = line.split("=", 1)
        if key in remaining:
            rendered.append(f"{key}={remaining.pop(key)}")
        else:
            rendered.append(line)
    for key, value in remaining.items():
        rendered.append(f"{key}={value}")
    return "\n".join(rendered) + "\n"


def _desired_env_updates(desired: DesiredState, *, hostname: str | None = None) -> dict[str, str]:
    desired_intent = (desired.host_intents or {}).get(hostname or "", "active")
    updates = {
        "LOOM_IMAGE_TAG": desired.image_tag,
        "LOOM_WORKER_POOL_NAME": desired.pool_name,
        "LOOM_WORKER_MAX_CONCURRENT": str(desired.max_concurrent),
        "LOOM_WORKER_ENV_CONFIG_VERSION": desired.env_config_version,
        "LOOM_GB10_CAPACITY_INTENT": desired_intent,
        "LOOM_WORKER_DRAIN": "1" if desired_intent == "draining" else "0",
    }
    updates.update(desired.env)
    return updates


def _redact_preview(key: str, value: str) -> str:
    upper_key = key.upper()
    if any(part in upper_key for part in _SECRET_KEY_PARTS) or any(
        value.strip().startswith(prefix) for prefix in _SECRET_VALUE_PREFIXES
    ):
        return "<redacted>"
    return value


def _print_env_update_preview(updates: dict[str, str]) -> None:
    print("dry-run: would update env keys:")
    for key, value in updates.items():
        print(f"  {key}={_redact_preview(key, value)}")


def _fetch_desired_state(args: argparse.Namespace) -> DesiredState:
    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc
    url = (
        f"{args.cp_url.rstrip('/')}/admin/gb10-worker-pools/"
        f"{args.environment}/{args.pool_name}/desired-state"
    )
    try:
        response = httpx.get(
            url,
            headers={"Authorization": f"Bearer {admin_token}"},
            timeout=10.0,
        )
    except httpx.RequestError as exc:
        raise RuntimeError(f"could not reach CP at {url}: {exc}") from exc
    if response.status_code != 200:
        raise RuntimeError(f"CP returned {response.status_code}: {response.text}")
    return DesiredState.from_api(response.json())


def _with_local_secret_updates(
    desired: DesiredState,
    args: argparse.Namespace,
) -> DesiredState:
    worker_token_source = getattr(args, "worker_token", None)
    if not worker_token_source:
        return desired
    try:
        worker_token = resolve_secret_source(
            worker_token_source,
            flag_name="--worker-token",
        )
    except SecretSourceError as exc:
        raise RuntimeError(str(exc)) from exc
    env = dict(desired.env)
    env["LOOM_WORKER_TOKEN"] = worker_token
    return replace(desired, env=env)


def _publish_desired_state(args: argparse.Namespace, desired: DesiredState) -> None:
    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc
    url = (
        f"{args.cp_url.rstrip('/')}/admin/gb10-worker-pools/"
        f"{desired.environment}/{desired.pool_name}/desired-state"
    )
    body = {
        "image_tag": desired.image_tag,
        "max_concurrent": desired.max_concurrent,
        "env_config_version": desired.env_config_version,
        "source_git_commit": desired.source_git_commit,
        "rollout_policy": desired.rollout_policy,
        "env": desired.env,
        "force": desired.force,
    }
    try:
        response = httpx.put(
            url,
            headers={"Authorization": f"Bearer {admin_token}"},
            json=body,
            timeout=10.0,
        )
    except httpx.RequestError as exc:
        raise RuntimeError(f"could not reach CP at {url}: {exc}") from exc
    if response.status_code != 200:
        raise RuntimeError(f"CP returned {response.status_code}: {response.text}")


def _report_node(
    args: argparse.Namespace,
    *,
    desired: DesiredState,
    local: LocalWorkerState,
    apply_state: str,
    last_apply_result: str | None = None,
    error_message: str | None = None,
) -> None:
    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError:
        return
    compose_project_dir = _source_dir_for_args(args)
    source_git_commit, source_git_dirty = _source_git_provenance(compose_project_dir)
    url = (
        f"{args.cp_url.rstrip('/')}/admin/gb10-worker-pools/"
        f"{desired.environment}/{desired.pool_name}/nodes/{local.hostname}/report"
    )
    body = {
        "current_image_tag": local.image_tag,
        "current_max_concurrent": local.max_concurrent,
        "current_env_config_version": local.env_config_version,
        "current_intent": local.capacity_intent,
        "apply_state": apply_state,
        "last_apply_result": last_apply_result,
        "error_message": error_message,
        "agent_version": AGENT_VERSION,
        "compose_project_dir": str(compose_project_dir) if compose_project_dir else None,
        "source_git_commit": source_git_commit,
        "source_git_dirty": source_git_dirty,
    }
    try:
        httpx.post(
            url,
            headers={"Authorization": f"Bearer {admin_token}"},
            json=body,
            timeout=10.0,
        )
    except httpx.RequestError:
        return


def _source_git_provenance(source_dir: Path | None) -> tuple[str | None, bool | None]:
    if source_dir is None:
        return None, None
    try:
        head = subprocess.run(
            ["git", "-C", str(source_dir), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5.0,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None, None
    if head.returncode != 0:
        return None, None
    try:
        status = subprocess.run(
            ["git", "-C", str(source_dir), "status", "--porcelain"],
            capture_output=True,
            text=True,
            timeout=5.0,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return head.stdout.strip(), None
    dirty = bool(status.stdout.strip()) if status.returncode == 0 else None
    return head.stdout.strip(), dirty


def _source_dir_for_args(args: argparse.Namespace) -> Path | None:
    source_dir = getattr(args, "source_dir", None)
    if source_dir is not None:
        return Path(source_dir).resolve()
    compose_files = getattr(args, "compose_file", None)
    if compose_files:
        return Path(compose_files[0]).resolve().parent
    return None


def _source_matches_desired(
    *,
    desired: DesiredState,
    local: LocalWorkerState,
) -> bool:
    if not desired.source_git_commit:
        return True
    return local.source_git_commit == desired.source_git_commit and local.source_git_dirty is False


def _update_source_checkout(
    *,
    desired: DesiredState,
    source_dir: Path | None,
    dry_run: bool,
) -> None:
    if not desired.source_git_commit:
        return
    if source_dir is None:
        raise RuntimeError(
            "desired state requires source_git_commit but no --source-dir or "
            "--compose-file source directory is available",
        )
    _run(
        ["git", "-C", str(source_dir), "fetch", "--quiet", "origin"],
        dry_run=dry_run,
    )
    _run(
        [
            "git",
            "-C",
            str(source_dir),
            "checkout",
            "--detach",
            desired.source_git_commit,
        ],
        dry_run=dry_run,
    )


def _print_plan(plan: AgentPlan, *, json_output: bool) -> None:
    if json_output:
        json.dump(asdict(plan), sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
        return
    print(f"{plan.environment}/{plan.pool_name} on {plan.hostname}")
    if not plan.needs_apply:
        print("  already current")
    elif plan.blocked_reason:
        print(f"  blocked: {plan.blocked_reason}")
    else:
        print(f"  changes: {', '.join(plan.changes)}")


def _plan(args: argparse.Namespace) -> int:
    try:
        desired = _fetch_desired_state(args)
        if args.rollback:
            desired = desired.rollback_target()
        desired = _with_local_secret_updates(desired, args)
        local = load_local_state(
            args.env_file,
            hostname=args.hostname,
            source_dir=_source_dir_for_args(args),
        )
        plan = build_plan(desired, local, force=args.force)
    except (RuntimeError, ValueError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    _print_plan(plan, json_output=args.format == "json")
    return 0


def _run(argv: list[str], *, dry_run: bool) -> None:
    if dry_run:
        print("dry-run:", " ".join(argv))
        return
    subprocess.run(argv, check=True)


def _pull_or_build(compose_base: list[str], service: str, *, dry_run: bool) -> None:
    try:
        _run([*compose_base, "pull", service], dry_run=dry_run)
    except subprocess.CalledProcessError:
        sys.stderr.write(
            "warning: docker compose pull failed; building worker image locally\n",
        )
        _run([*compose_base, "build", service], dry_run=dry_run)


def _compose_base(args: argparse.Namespace, env_file: Path) -> list[str]:
    compose_base = [
        "docker",
        "compose",
        "--env-file",
        str(env_file),
    ]
    for compose_file in args.compose_file:
        compose_base.extend(["-f", str(compose_file)])
    return compose_base


def _json_docs_from_compose_ps(stdout: str) -> list[dict[str, Any]]:
    text = stdout.strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        docs: list[dict[str, Any]] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                parsed_line = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed_line, dict):
                docs.append(parsed_line)
        return docs
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    if isinstance(parsed, dict):
        return [parsed]
    return []


def _compose_service_is_running(compose_base: list[str], service: str) -> bool:
    try:
        result = subprocess.run(
            [*compose_base, "ps", "--format", "json", service],
            capture_output=True,
            text=True,
            timeout=30.0,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    if result.returncode != 0:
        return False
    for doc in _json_docs_from_compose_ps(result.stdout):
        doc_service = str(doc.get("Service") or doc.get("Name") or "")
        if (
            doc_service
            and doc_service != service
            and not doc_service.endswith(f"-{service}")
            and f"-{service}-" not in doc_service
        ):
            continue
        state = str(doc.get("State") or "").lower()
        status = str(doc.get("Status") or "").lower()
        if state == "running" or status.startswith("up "):
            return True
    return False


def _write_temp_env_file(env_file: Path, rendered: str) -> Path:
    runtime_dir = _runtime_temp_env_dir()
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        delete=False,
        dir=runtime_dir,
        prefix=f"loom-{env_file.name.lstrip('.') or 'env'}-",
        suffix=".tmp",
    ) as f:
        f.write(rendered)
        temp_path = Path(f.name)
    temp_path.chmod(0o600)
    return temp_path


def _runtime_temp_env_dir() -> Path:
    base = os.environ.get("XDG_RUNTIME_DIR")
    if base:
        path = Path(base) / "loom-gb10-agent"
    else:
        path = Path(tempfile.gettempdir()) / f"loom-gb10-agent-{os.getuid()}"
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        path.chmod(0o700)
    except OSError:
        pass
    return path


def _cleanup_legacy_temp_env_files(env_file: Path) -> None:
    prefix = f".{env_file.name}."
    try:
        candidates = list(env_file.parent.iterdir())
    except OSError:
        return
    for candidate in candidates:
        if (
            candidate.name.startswith(prefix)
            and candidate.name.endswith(".tmp")
            and candidate.is_file()
        ):
            candidate.unlink(missing_ok=True)


def _apply(args: argparse.Namespace) -> int:
    try:
        desired = _fetch_desired_state(args)
        if args.rollback:
            desired = desired.rollback_target()
            if args.dry_run:
                print("dry-run: would publish rollback target to Control Plane")
            else:
                _publish_desired_state(args, desired)
        desired = _with_local_secret_updates(desired, args)
        source_dir = _source_dir_for_args(args)
        local = load_local_state(
            args.env_file,
            hostname=args.hostname,
            source_dir=source_dir,
        )
        plan = build_plan(desired, local, force=args.force)
    except (RuntimeError, ValueError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2

    if plan.blocked_reason:
        _report_node(
            args,
            desired=desired,
            local=local,
            apply_state="blocked",
            last_apply_result=plan.blocked_reason,
        )
        _print_plan(plan, json_output=args.format == "json")
        return 0
    if not plan.needs_apply:
        desired_intent = str(plan.desired.get("capacity_intent") or "active")
        last_apply_result = "already current"
        if desired_intent not in {"draining", "stopped"}:
            compose_base = _compose_base(args, args.env_file)
            try:
                if not _compose_service_is_running(compose_base, args.service):
                    _pull_or_build(compose_base, args.service, dry_run=args.dry_run)
                    _run([*compose_base, "up", "-d", args.service], dry_run=args.dry_run)
                    last_apply_result = "docker compose worker started"
            except (RuntimeError, subprocess.CalledProcessError) as exc:
                _report_node(
                    args,
                    desired=desired,
                    local=local,
                    apply_state="failed",
                    error_message=str(exc),
                )
                sys.stderr.write(f"error: {exc}\n")
                return 1
        _report_node(
            args,
            desired=desired,
            local=local,
            apply_state="applied",
            last_apply_result=last_apply_result,
        )
        _print_plan(plan, json_output=args.format == "json")
        return 0

    temp_env_file: Path | None = None
    try:
        updates = _desired_env_updates(desired, hostname=local.hostname)
        rendered = render_env_updates(args.env_file, updates)
        compose_env_file = args.env_file
        if args.dry_run:
            _print_env_update_preview(updates)
        else:
            _cleanup_legacy_temp_env_files(args.env_file)
            temp_env_file = _write_temp_env_file(args.env_file, rendered)
            compose_env_file = temp_env_file
        if not _source_matches_desired(desired=desired, local=local):
            _update_source_checkout(
                desired=desired,
                source_dir=source_dir,
                dry_run=args.dry_run,
            )
        compose_base = _compose_base(args, compose_env_file)
        desired_intent = str(plan.desired.get("capacity_intent") or "active")
        if desired_intent not in {"draining", "stopped"}:
            _pull_or_build(compose_base, args.service, dry_run=args.dry_run)
        _run(
            [
                *compose_base,
                "stop",
                "--timeout",
                str(args.drain_timeout_sec),
                args.service,
            ],
            dry_run=args.dry_run,
        )
        if desired_intent not in {"draining", "stopped"}:
            _run([*compose_base, "up", "-d", args.service], dry_run=args.dry_run)
        if not args.dry_run:
            args.env_file.write_text(rendered, encoding="utf-8")
    except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        _report_node(
            args,
            desired=desired,
            local=local,
            apply_state="failed",
            error_message=str(exc),
        )
        sys.stderr.write(f"error: {exc}\n")
        return 1
    finally:
        if temp_env_file is not None:
            temp_env_file.unlink(missing_ok=True)

    refreshed = load_local_state(
        args.env_file,
        hostname=local.hostname,
        source_dir=source_dir,
    )
    final_intent = str(plan.desired.get("capacity_intent") or "active")
    apply_state = "rolled_back" if args.rollback else "applied"
    result = "docker compose worker restarted"
    if final_intent in {"draining", "stopped"}:
        apply_state = final_intent
        result = "docker compose worker stopped after drain"
    _report_node(
        args,
        desired=desired,
        local=refreshed,
        apply_state=apply_state,
        last_apply_result=result,
    )
    _print_plan(
        build_plan(desired, refreshed, force=args.force),
        json_output=args.format == "json",
    )
    return 0


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--cp-url", default=_DEFAULT_CP_URL)
    parser.add_argument("--admin-token", default=_DEFAULT_ADMIN_TOKEN_SOURCE)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--pool-name", required=True)
    parser.add_argument("--hostname", default=None)
    parser.add_argument(
        "--env-file",
        type=Path,
        required=True,
        help="Host-local remote-worker env file to inspect/update.",
    )
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--rollback", action="store_true")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument(
        "--worker-token",
        type=secret_source_argparse_type("--worker-token"),
        default=None,
        help=(
            "Optional current environment worker token source for local "
            "host env parity. ONE of env:VAR, file:PATH, or -. The token is "
            "written only to the host-local env file and is not published to CP."
        ),
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=None,
        help=(
            "Host-local Loom source checkout to report and converge. Defaults "
            "to the first --compose-file parent when compose files are provided."
        ),
    )


def dispatch(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="loom worker gb10-agent",
        description="Pull and apply GB10 remote-worker desired state.",
    )
    sub = parser.add_subparsers(dest="gb10_cmd", required=True)

    p_plan = sub.add_parser("plan", help="Show local drift from desired state.")
    _add_common_args(p_plan)
    p_plan.set_defaults(handler=_plan)

    p_apply = sub.add_parser(
        "apply",
        help="Update env file and restart Docker Compose worker with drain.",
    )
    _add_common_args(p_apply)
    p_apply.add_argument(
        "--compose-file",
        action="append",
        type=Path,
        required=True,
        help=(
            "Docker Compose file that defines the worker service. Repeat for "
            "GB10 host-network overrides."
        ),
    )
    p_apply.add_argument("--service", default="worker")
    p_apply.add_argument("--drain-timeout-sec", type=int, default=600)
    p_apply.add_argument("--dry-run", action="store_true")
    p_apply.set_defaults(handler=_apply)

    args = parser.parse_args(argv)
    return int(args.handler(args))
