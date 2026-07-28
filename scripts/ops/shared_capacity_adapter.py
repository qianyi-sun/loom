#!/usr/bin/env python3
"""Apply one broker handoff to one sandbox-local autoscaler policy.

The broker remains the only authority that may increase ``max_slots``.  This
adapter validates a root-delivered handoff against the sandbox's durable
candidate binding, applies the exact ceiling through the Control Plane admin
API, and publishes a fenced observation for the broker's next reconcile pass.

The command emits only secret-free JSON.  The admin credential is read from a
TOML file referenced by the adapter config and is never accepted as a literal
argument.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_SCHEMA_VERSION = 1
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER_RE = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
)
_SECRET_HINT_RE = re.compile(
    r"(?i)(bearer\s+|token=|secret=|password=|api[_-]?key=)\S+|"
    r"\b(?:loom_(?:admin|w)_|sk-)[A-Za-z0-9._~+/=-]+",
)
_POLICY_COPY_FIELDS = (
    "actuator",
    "scale_up_threshold_slots",
    "scale_down_idle_seconds",
    "scale_up_cooldown_seconds",
    "scale_down_cooldown_seconds",
    "drain_timeout_seconds",
    "force",
    "actuator_config",
)


class AdapterError(RuntimeError):
    """The handoff cannot be applied without weakening a safety fence."""


@dataclass(frozen=True, slots=True)
class AdapterConfig:
    sandbox: str
    environment: str
    pool_name: str
    control_plane_url: str
    admin_secret_file: Path
    handoff_path: Path
    observation_path: Path
    adapter_state_path: Path
    sandbox_state_path: Path
    timeout_seconds: float


@dataclass(frozen=True, slots=True)
class Handoff:
    request_id: str
    lease_epoch: int
    sandbox: str
    environment: str
    candidate_sha: str
    pool_name: str
    enabled: bool
    min_slots: int
    max_slots: int
    expires_at: datetime
    preemptible: bool
    digest: str


HttpJson = Callable[..., dict[str, Any]]


def _redact(value: str) -> str:
    return _SECRET_HINT_RE.sub("<redacted>", value)


def _secure_regular_file(
    path: Path,
    *,
    label: str,
    require_owner_only: bool = False,
) -> Path:
    if not path.is_absolute():
        raise AdapterError(f"{label} must be an absolute path")
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise AdapterError(f"{label} is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
    ):
        raise AdapterError(f"{label} must be a single-link regular file")
    mode = stat.S_IMODE(metadata.st_mode)
    forbidden = 0o077 if require_owner_only else 0o022
    if mode & forbidden:
        raise AdapterError(f"{label} has unsafe permissions")
    return path.resolve(strict=True)


def _required_string(payload: Mapping[str, Any], field: str, label: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise AdapterError(f"{label}.{field} must be a non-empty string")
    return value.strip()


def _required_absolute_path(
    payload: Mapping[str, Any],
    field: str,
    label: str,
) -> Path:
    path = Path(_required_string(payload, field, label))
    if not path.is_absolute() or ".." in path.parts:
        raise AdapterError(f"{label}.{field} must be an absolute normalized path")
    return path


def load_config(path: Path) -> AdapterConfig:
    config_path = _secure_regular_file(path, label="adapter config")
    try:
        payload = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise AdapterError("adapter config is invalid") from exc
    if payload.get("schema_version") != _SCHEMA_VERSION:
        raise AdapterError("adapter config schema_version is unsupported")
    allowed = {
        "schema_version",
        "sandbox",
        "environment",
        "pool_name",
        "control_plane_url",
        "admin_secret_file",
        "handoff_path",
        "observation_path",
        "adapter_state_path",
        "sandbox_state_path",
        "timeout_seconds",
    }
    if set(payload) != allowed:
        raise AdapterError("adapter config fields do not match the closed schema")
    sandbox = _required_string(payload, "sandbox", "adapter config")
    environment = _required_string(payload, "environment", "adapter config")
    pool_name = _required_string(payload, "pool_name", "adapter config")
    if _IDENTIFIER_RE.fullmatch(sandbox) is None:
        raise AdapterError("adapter config sandbox is invalid")
    if environment != f"sandbox-{sandbox}":
        raise AdapterError("adapter environment is not bound to sandbox")
    if _IDENTIFIER_RE.fullmatch(pool_name) is None:
        raise AdapterError("adapter pool_name is invalid")
    control_plane_url = _required_string(
        payload,
        "control_plane_url",
        "adapter config",
    ).rstrip("/")
    parsed_url = urllib.parse.urlsplit(control_plane_url)
    if (
        parsed_url.scheme not in {"http", "https"}
        or not parsed_url.hostname
        or parsed_url.username is not None
        or parsed_url.password is not None
        or parsed_url.query
        or parsed_url.fragment
    ):
        raise AdapterError("adapter control_plane_url is invalid")
    timeout = payload.get("timeout_seconds")
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise AdapterError("adapter timeout_seconds must be numeric")
    timeout_seconds = float(timeout)
    if not 0.1 <= timeout_seconds <= 60.0:
        raise AdapterError("adapter timeout_seconds must be in 0.1..60")
    return AdapterConfig(
        sandbox=sandbox,
        environment=environment,
        pool_name=pool_name,
        control_plane_url=control_plane_url,
        admin_secret_file=_required_absolute_path(
            payload,
            "admin_secret_file",
            "adapter config",
        ),
        handoff_path=_required_absolute_path(
            payload,
            "handoff_path",
            "adapter config",
        ),
        observation_path=_required_absolute_path(
            payload,
            "observation_path",
            "adapter config",
        ),
        adapter_state_path=_required_absolute_path(
            payload,
            "adapter_state_path",
            "adapter config",
        ),
        sandbox_state_path=_required_absolute_path(
            payload,
            "sandbox_state_path",
            "adapter config",
        ),
        timeout_seconds=timeout_seconds,
    )


def _read_json_object(path: Path, *, label: str) -> dict[str, Any]:
    resolved = _secure_regular_file(path, label=label)
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AdapterError(f"{label} is invalid") from exc
    if not isinstance(payload, dict):
        raise AdapterError(f"{label} must contain a JSON object")
    return payload


def _parse_timestamp(value: object, *, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise AdapterError(f"{field} must be a UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise AdapterError(f"{field} must be a UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise AdapterError(f"{field} must be a UTC timestamp")
    return parsed.astimezone(UTC)


def load_handoff(config: AdapterConfig) -> Handoff:
    payload = _read_json_object(config.handoff_path, label="broker handoff")
    expected = {
        "schema_version",
        "request_id",
        "lease_epoch",
        "sandbox",
        "environment",
        "candidate_sha",
        "pool_name",
        "enabled",
        "min_slots",
        "max_slots",
        "expires_at",
        "preemptible",
    }
    if set(payload) != expected or payload.get("schema_version") != _SCHEMA_VERSION:
        raise AdapterError("broker handoff fields do not match the closed schema")
    request_id = _required_string(payload, "request_id", "broker handoff")
    if _UUID_RE.fullmatch(request_id) is None:
        raise AdapterError("broker handoff request_id is invalid")
    lease_epoch = payload.get("lease_epoch")
    min_slots = payload.get("min_slots")
    max_slots = payload.get("max_slots")
    for field, value in (
        ("lease_epoch", lease_epoch),
        ("min_slots", min_slots),
        ("max_slots", max_slots),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise AdapterError(f"broker handoff {field} must be non-negative")
    if min_slots != 0:
        raise AdapterError("broker handoff min_slots must remain zero")
    if max_slots > 10_000:
        raise AdapterError("broker handoff max_slots exceeds the hard bound")
    enabled = payload.get("enabled")
    preemptible = payload.get("preemptible")
    if not isinstance(enabled, bool) or not isinstance(preemptible, bool):
        raise AdapterError("broker handoff booleans are invalid")
    sandbox = _required_string(payload, "sandbox", "broker handoff")
    environment = _required_string(payload, "environment", "broker handoff")
    candidate_sha = _required_string(payload, "candidate_sha", "broker handoff")
    pool_name = _required_string(payload, "pool_name", "broker handoff")
    if (
        sandbox != config.sandbox
        or environment != config.environment
        or pool_name != config.pool_name
    ):
        raise AdapterError("broker handoff target does not match adapter config")
    if _SHA_RE.fullmatch(candidate_sha) is None:
        raise AdapterError("broker handoff candidate_sha is invalid")
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return Handoff(
        request_id=request_id,
        lease_epoch=lease_epoch,
        sandbox=sandbox,
        environment=environment,
        candidate_sha=candidate_sha,
        pool_name=pool_name,
        enabled=enabled,
        min_slots=min_slots,
        max_slots=max_slots,
        expires_at=_parse_timestamp(payload.get("expires_at"), field="expires_at"),
        preemptible=preemptible,
        digest=hashlib.sha256(canonical).hexdigest(),
    )


def _load_sandbox_candidate(config: AdapterConfig) -> str:
    payload = _read_json_object(config.sandbox_state_path, label="sandbox state")
    if (
        payload.get("schema_version") != _SCHEMA_VERSION
        or payload.get("sandbox") != config.sandbox
        or payload.get("compose_project") != f"loom-sandbox-{config.sandbox}"
    ):
        raise AdapterError("sandbox state identity is invalid")
    candidate_sha = payload.get("candidate_sha")
    if not isinstance(candidate_sha, str) or _SHA_RE.fullmatch(candidate_sha) is None:
        raise AdapterError("sandbox state candidate_sha is invalid")
    return candidate_sha


def _load_admin_token(path: Path) -> str:
    resolved = _secure_regular_file(
        path,
        label="admin secret file",
        require_owner_only=True,
    )
    try:
        payload = tomllib.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise AdapterError("admin secret file is invalid") from exc
    admin = payload.get("admin")
    token = admin.get("token") if isinstance(admin, dict) else None
    if not isinstance(token, str) or not token.strip():
        raise AdapterError("admin secret file is invalid")
    return token.strip()


def _http_json(
    *,
    method: str,
    base_url: str,
    token: str,
    path: str,
    body: Mapping[str, Any] | None = None,
    timeout: float,
) -> dict[str, Any]:
    data = None
    headers = {"Authorization": f"Bearer {token}"}
    if body is not None:
        data = json.dumps(dict(body), sort_keys=True, separators=(",", ":")).encode()
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        base_url.rstrip("/") + path,
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise AdapterError(
            f"{method} {path} failed HTTP {exc.code}: {_redact(detail)}",
        ) from exc
    except urllib.error.URLError as exc:
        raise AdapterError(
            f"{method} {path} failed: {_redact(str(exc.reason))}",
        ) from exc
    try:
        payload = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        raise AdapterError(f"{method} {path} returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise AdapterError(f"{method} {path} returned non-object JSON")
    return payload


def _load_adapter_state(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = _read_json_object(path, label="adapter state")
    required = {
        "schema_version",
        "request_id",
        "lease_epoch",
        "handoff_digest",
        "candidate_sha",
        "applied_enabled",
        "applied_max_slots",
        "pending_slots",
        "active_slots",
        "draining_slots",
        "terminal_slots",
        "updated_at",
    }
    if set(payload) != required or payload.get("schema_version") != _SCHEMA_VERSION:
        raise AdapterError("adapter state fields do not match the closed schema")
    for field in (
        "lease_epoch",
        "applied_max_slots",
        "pending_slots",
        "active_slots",
        "draining_slots",
        "terminal_slots",
    ):
        value = payload.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise AdapterError("adapter state counters are invalid")
    if (
        not isinstance(payload.get("applied_enabled"), bool)
        or _UUID_RE.fullmatch(str(payload.get("request_id"))) is None
        or _SHA_RE.fullmatch(str(payload.get("candidate_sha"))) is None
        or not re.fullmatch(r"[0-9a-f]{64}", str(payload.get("handoff_digest")))
    ):
        raise AdapterError("adapter state binding is invalid")
    return payload


def _validate_epoch(
    handoff: Handoff,
    state: Mapping[str, Any] | None,
) -> None:
    if state is None:
        return
    previous_request = str(state["request_id"])
    if previous_request != handoff.request_id:
        previous_nonterminal = sum(
            int(state[field]) for field in ("pending_slots", "active_slots", "draining_slots")
        )
        if int(state["applied_max_slots"]) != 0 or previous_nonterminal != 0:
            raise AdapterError("new request cannot replace a nonterminal lease")
        return
    previous_epoch = int(state["lease_epoch"])
    if handoff.lease_epoch < previous_epoch:
        raise AdapterError("broker handoff lease_epoch regressed")
    if handoff.lease_epoch == previous_epoch and handoff.digest != state["handoff_digest"]:
        raise AdapterError("broker handoff changed without an epoch increment")


def _policy_path(config: AdapterConfig) -> str:
    environment = urllib.parse.quote(config.environment, safe="")
    pool_name = urllib.parse.quote(config.pool_name, safe="")
    return f"/admin/worker-pool-autoscaler-policies/{environment}/{pool_name}"


def _validate_policy(
    policy: Mapping[str, Any],
    *,
    config: AdapterConfig,
    candidate_sha: str,
) -> None:
    if (
        policy.get("environment") != config.environment
        or policy.get("pool_name") != config.pool_name
    ):
        raise AdapterError("autoscaler policy target does not match adapter config")
    actuator_config = policy.get("actuator_config")
    if not isinstance(actuator_config, dict):
        raise AdapterError("autoscaler policy actuator_config is invalid")
    if actuator_config.get("candidate_sha") != candidate_sha:
        raise AdapterError("autoscaler policy candidate_sha does not match handoff")
    for field in _POLICY_COPY_FIELDS:
        if field not in policy:
            raise AdapterError(f"autoscaler policy is missing {field}")
    for field in ("max_slots", "min_slots"):
        value = policy.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise AdapterError(f"autoscaler policy {field} is invalid")


def _observation_counts(
    policy: Mapping[str, Any],
    *,
    state: Mapping[str, Any] | None,
    same_request: bool,
) -> tuple[int, int, int, int]:
    raw = [
        policy.get("last_pending_slots"),
        policy.get("last_actual_slots"),
        policy.get("last_draining_slots"),
    ]
    if all(value is None for value in raw):
        if same_request and state is not None:
            pending = int(state["pending_slots"])
            active = int(state["active_slots"])
            draining = int(state["draining_slots"])
        else:
            pending = int(policy["max_slots"])
            active = 0
            draining = 0
    else:
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in raw):
            raise AdapterError("autoscaler observation counters are incomplete")
        pending, active, draining = (int(value) for value in raw)
    previous_terminal = int(state["terminal_slots"]) if same_request and state is not None else 0
    previous_nonterminal = (
        sum(int(state[field]) for field in ("pending_slots", "active_slots", "draining_slots"))
        if same_request and state is not None
        else 0
    )
    nonterminal = pending + active + draining
    terminal = previous_terminal + max(0, previous_nonterminal - nonterminal)
    return pending, active, draining, terminal


def _policy_update_body(
    policy: Mapping[str, Any],
    *,
    enabled: bool,
    max_slots: int,
) -> dict[str, Any]:
    body = {field: policy[field] for field in _POLICY_COPY_FIELDS}
    body.update(
        {
            "enabled": enabled,
            "min_slots": 0,
            "max_slots": max_slots,
            "disabled_reason": (None if enabled else "shared_capacity_handoff_disabled"),
        },
    )
    return body


def _atomic_json_write(path: Path, payload: object) -> None:
    parent = path.parent
    parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = parent.lstat()
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise AdapterError("adapter output directory must be private")
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


def run_once(
    config: AdapterConfig,
    *,
    now: datetime | None = None,
    http_json: HttpJson = _http_json,
) -> dict[str, Any]:
    now = (now or datetime.now(UTC)).astimezone(UTC)
    state = _load_adapter_state(config.adapter_state_path)
    handoff_missing = False
    try:
        handoff = load_handoff(config)
    except AdapterError:
        if state is None or config.handoff_path.exists() or config.handoff_path.is_symlink():
            raise
        handoff_missing = True
        handoff = Handoff(
            request_id=str(state["request_id"]),
            lease_epoch=int(state["lease_epoch"]),
            sandbox=config.sandbox,
            environment=config.environment,
            candidate_sha=str(state["candidate_sha"]),
            pool_name=config.pool_name,
            enabled=False,
            min_slots=0,
            max_slots=0,
            expires_at=now,
            preemptible=True,
            digest=str(state["handoff_digest"]),
        )
    candidate_sha = _load_sandbox_candidate(config)
    if candidate_sha != handoff.candidate_sha:
        raise AdapterError("sandbox candidate does not match broker handoff")
    _validate_epoch(handoff, state)
    same_request = state is not None and state["request_id"] == handoff.request_id
    token = _load_admin_token(config.admin_secret_file)
    path = _policy_path(config)
    policy = http_json(
        method="GET",
        base_url=config.control_plane_url,
        token=token,
        path=path,
        timeout=config.timeout_seconds,
    )
    _validate_policy(policy, config=config, candidate_sha=candidate_sha)
    pending, active, draining, terminal = _observation_counts(
        policy,
        state=state,
        same_request=same_request,
    )
    expired = handoff_missing or now >= handoff.expires_at
    target_enabled = handoff.enabled and not expired
    target_max_slots = handoff.max_slots if target_enabled else 0
    current_enabled = policy.get("enabled")
    current_max_slots = policy.get("max_slots")
    if not isinstance(current_enabled, bool):
        raise AdapterError("autoscaler policy enabled is invalid")
    changed = current_enabled != target_enabled or current_max_slots != target_max_slots
    if changed:
        applied = http_json(
            method="PUT",
            base_url=config.control_plane_url,
            token=token,
            path=path,
            body=_policy_update_body(
                policy,
                enabled=target_enabled,
                max_slots=target_max_slots,
            ),
            timeout=config.timeout_seconds,
        )
        _validate_policy(applied, config=config, candidate_sha=candidate_sha)
        if (
            applied.get("enabled") != target_enabled
            or applied.get("min_slots") != 0
            or applied.get("max_slots") != target_max_slots
        ):
            raise AdapterError("autoscaler policy readback does not match handoff")

    observation = {
        "request_id": handoff.request_id,
        "lease_epoch": handoff.lease_epoch,
        "pending_slots": pending,
        "active_slots": active,
        "draining_slots": draining,
        "terminal_slots": terminal,
    }
    adapter_state = {
        "schema_version": _SCHEMA_VERSION,
        "request_id": handoff.request_id,
        "lease_epoch": handoff.lease_epoch,
        "handoff_digest": handoff.digest,
        "candidate_sha": candidate_sha,
        "applied_enabled": target_enabled,
        "applied_max_slots": target_max_slots,
        "pending_slots": pending,
        "active_slots": active,
        "draining_slots": draining,
        "terminal_slots": terminal,
        "updated_at": now.isoformat().replace("+00:00", "Z"),
    }
    _atomic_json_write(config.observation_path, [observation])
    _atomic_json_write(config.adapter_state_path, adapter_state)
    return {
        "schema_version": 1,
        "artifact_type": "shared-capacity-adapter-result",
        "status": "applied" if changed else "unchanged",
        "sandbox": config.sandbox,
        "environment": config.environment,
        "pool_name": config.pool_name,
        "candidate_sha": candidate_sha,
        "request_id": handoff.request_id,
        "lease_epoch": handoff.lease_epoch,
        "enabled": target_enabled,
        "max_slots": target_max_slots,
        "expired": expired,
        "handoff_missing": handoff_missing,
        "observation": observation,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("command", choices=("run",))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        report = run_once(load_config(args.config))
    except (AdapterError, OSError, UnicodeError, ValueError):
        sys.stderr.write('{"error":"shared-capacity-adapter-failed-safely"}\n')
        return 1
    sys.stdout.write(json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
