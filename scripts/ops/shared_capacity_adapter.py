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
import fcntl
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
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

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
_POLICY_TEMPLATE_DIR = (
    Path("/etc/loom/developer-shared-capacity-policies")
    if str(Path(__file__).resolve()).startswith("/usr/local/libexec/")
    else Path(__file__).resolve().parents[2] / "deploy/developer-sandboxes/shared-capacity-policies"
)
_CAPACITY_BINDING_FIELDS = {
    "schema_version",
    "request_id",
    "lease_epoch",
    "candidate_sha",
    "preemptible",
}
_CAPACITY_LEASE_STATES = {"active", "retiring", "retired"}
_ATTESTATION_TTL = timedelta(minutes=15)
_ATTESTATION_MAX_BYTES = 128 * 1024
_COLLECTOR_HOSTNAME = "trt-eai-oldlab-2"
_DOMAIN_ATTESTATION_ROOT = Path("/var/lib/loom-developer-domain-attestations")
_FLEET_ATTESTATION_ROOT = Path(
    "/var/lib/loom-developer-sandbox-links/attestations",
)
_RECEIPT_TOP_LEVEL_FIELDS = {
    "schema_version",
    "kind",
    "sandbox",
    "candidate_sha",
    "candidate_tree",
    "collector",
    "fleet_attestation",
    "domains",
    "payload_sha256",
}
_RECEIPT_DOMAIN_FIELDS = {
    "manifest_path",
    "signature_path",
    "payload_sha256",
    "signature_sha256",
    "key_id",
    "generation",
    "published_at",
    "expires_at",
}
_RECEIPT_FLEET_FIELDS = {
    "path",
    "payload_sha256",
    "generated_at",
    "expires_at",
}
_OBSERVATION_FIELDS = {
    "sandbox",
    "pool_name",
    "candidate_sha",
    "request_id",
    "lease_epoch",
    "capacity_lease_state",
    "observed_at",
    "observation_sequence",
    "pending_slots",
    "active_slots",
    "draining_slots",
    "terminal_slots",
    "payload_sha256",
}


class AdapterError(RuntimeError):
    """The handoff cannot be applied without weakening a safety fence."""


class PolicyMissingError(AdapterError):
    """The sandbox Control Plane has no policy for the configured pool."""


@dataclass(frozen=True, slots=True)
class AdapterConfig:
    sandbox: str
    environment: str
    pool_name: str
    slurm_account: str
    slurm_qos: str
    runtime_root: Path
    candidate_root: Path
    control_plane_url: str
    admin_secret_file: Path
    handoff_path: Path
    observation_path: Path
    adapter_state_path: Path
    sandbox_state_path: Path
    runtime_attestation_root: Path
    max_slots_bound: int
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
    digest: str | None


@dataclass(frozen=True, slots=True)
class CandidateBinding:
    sha: str
    tree: str


HttpJson = Callable[..., dict[str, Any]]
Clock = Callable[[], datetime]


@contextmanager
def _exclusive_adapter_lock(config: AdapterConfig) -> Iterator[None]:
    lock_path = config.adapter_state_path.with_name(
        f".{config.adapter_state_path.name}.lock",
    )
    parent = lock_path.parent
    parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    parent_metadata = parent.lstat()
    if (
        stat.S_ISLNK(parent_metadata.st_mode)
        or not stat.S_ISDIR(parent_metadata.st_mode)
        or parent_metadata.st_uid != os.geteuid()
        or stat.S_IMODE(parent_metadata.st_mode) != 0o700
    ):
        raise AdapterError("adapter lock directory must be owner-only mode 0700")
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise AdapterError("adapter lock is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise AdapterError("adapter lock file is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise AdapterError("adapter invocation is already active") from exc
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


@contextmanager
def _shared_collector_lock(config: AdapterConfig) -> Iterator[None]:
    """Prevent the collector from invalidating a receipt during activation."""

    root = config.runtime_attestation_root
    try:
        root_metadata = root.lstat()
    except OSError as exc:
        raise AdapterError("runtime attestation root is unavailable") from exc
    if (
        stat.S_ISLNK(root_metadata.st_mode)
        or not stat.S_ISDIR(root_metadata.st_mode)
        or root_metadata.st_uid != os.geteuid()
        or root_metadata.st_gid != os.getegid()
        or stat.S_IMODE(root_metadata.st_mode) != 0o700
    ):
        raise AdapterError("runtime attestation root is unsafe")
    lock_path = root / ".collector.lock"
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags)
    except OSError as exc:
        raise AdapterError("runtime attestation collector lock is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != os.geteuid()
            or metadata.st_gid != os.getegid()
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise AdapterError("runtime attestation collector lock is unsafe")
        fcntl.flock(descriptor, fcntl.LOCK_SH)
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _clock_now(clock: Clock) -> datetime:
    value = clock()
    if value.tzinfo is None:
        raise AdapterError("adapter clock must return a timezone-aware timestamp")
    return value.astimezone(UTC)


def _resolve_clock(*, now: datetime | None, clock: Clock | None) -> Clock:
    if now is not None and clock is not None:
        raise AdapterError("adapter now and clock cannot both be supplied")
    if clock is not None:
        return clock
    if now is not None:
        fixed = now.astimezone(UTC)
        return lambda: fixed
    return lambda: datetime.now(UTC)


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
        "slurm_account",
        "slurm_qos",
        "runtime_root",
        "candidate_root",
        "control_plane_url",
        "admin_secret_file",
        "handoff_path",
        "observation_path",
        "adapter_state_path",
        "sandbox_state_path",
        "runtime_attestation_root",
        "max_slots_bound",
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
    slurm_account = _required_string(payload, "slurm_account", "adapter config")
    slurm_qos = _required_string(payload, "slurm_qos", "adapter config")
    runtime_root = _required_absolute_path(payload, "runtime_root", "adapter config")
    candidate_root = _required_absolute_path(payload, "candidate_root", "adapter config")
    if (
        _IDENTIFIER_RE.fullmatch(slurm_account) is None
        or _IDENTIFIER_RE.fullmatch(slurm_qos) is None
        or not runtime_root.is_relative_to(Path("/shared_work/loom/runtime"))
        or runtime_root == Path("/shared_work/loom/runtime")
        or not candidate_root.is_relative_to(Path("/shared_work/loom/candidates"))
        or candidate_root == Path("/shared_work/loom/candidates")
    ):
        raise AdapterError("adapter registry resource binding is invalid")
    max_slots_bound = payload.get("max_slots_bound")
    if (
        isinstance(max_slots_bound, bool)
        or not isinstance(max_slots_bound, int)
        or not 0 <= max_slots_bound <= 10_000
    ):
        raise AdapterError("adapter max_slots_bound must be in 0..10000")
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
        slurm_account=slurm_account,
        slurm_qos=slurm_qos,
        runtime_root=runtime_root,
        candidate_root=candidate_root,
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
        runtime_attestation_root=_required_absolute_path(
            payload,
            "runtime_attestation_root",
            "adapter config",
        ),
        max_slots_bound=max_slots_bound,
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
    assert isinstance(lease_epoch, int)
    assert isinstance(min_slots, int)
    assert isinstance(max_slots, int)
    if min_slots != 0:
        raise AdapterError("broker handoff min_slots must remain zero")
    if max_slots > config.max_slots_bound:
        raise AdapterError("broker handoff max_slots exceeds the reviewed pool bound")
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


def _load_sandbox_binding(config: AdapterConfig) -> CandidateBinding:
    payload = _read_json_object(config.sandbox_state_path, label="sandbox state")
    if (
        payload.get("schema_version") != _SCHEMA_VERSION
        or payload.get("sandbox") != config.sandbox
        or payload.get("compose_project") != f"loom-sandbox-{config.sandbox}"
    ):
        raise AdapterError("sandbox state identity is invalid")
    candidate_sha = payload.get("candidate_sha")
    candidate_tree = payload.get("candidate_tree")
    if (
        not isinstance(candidate_sha, str)
        or _SHA_RE.fullmatch(candidate_sha) is None
        or not isinstance(candidate_tree, str)
        or _SHA_RE.fullmatch(candidate_tree) is None
    ):
        raise AdapterError("sandbox state candidate binding is invalid")
    return CandidateBinding(sha=candidate_sha, tree=candidate_tree)


def _load_sandbox_candidate(config: AdapterConfig) -> str:
    return _load_sandbox_binding(config).sha


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
        if exc.code == 404:
            raise PolicyMissingError(f"{method} {path} returned HTTP 404") from exc
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
        "preemptible",
        "applied_enabled",
        "applied_max_slots",
        "pending_slots",
        "active_slots",
        "draining_slots",
        "terminal_slots",
        "runtime_attestation_status",
        "runtime_attestation_digest",
        "blocker",
        "observation_sequence",
        "updated_at",
    }
    legacy_required = required - {"observation_sequence"}
    if (
        frozenset(payload) not in {frozenset(required), frozenset(legacy_required)}
        or payload.get("schema_version") != _SCHEMA_VERSION
    ):
        raise AdapterError("adapter state fields do not match the closed schema")
    if "observation_sequence" not in payload:
        payload["observation_sequence"] = 0
    for field in (
        "lease_epoch",
        "applied_max_slots",
        "pending_slots",
        "active_slots",
        "draining_slots",
        "terminal_slots",
        "observation_sequence",
    ):
        value = payload.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise AdapterError("adapter state counters are invalid")
    if (
        not isinstance(payload.get("applied_enabled"), bool)
        or not isinstance(payload.get("preemptible"), bool)
        or _UUID_RE.fullmatch(str(payload.get("request_id"))) is None
        or _SHA_RE.fullmatch(str(payload.get("candidate_sha"))) is None
        or (
            payload.get("handoff_digest") is not None
            and not re.fullmatch(r"[0-9a-f]{64}", str(payload.get("handoff_digest")))
        )
        or payload.get("runtime_attestation_status") not in {"verified", "not_required", "rejected"}
        or (
            payload.get("runtime_attestation_digest") is not None
            and not re.fullmatch(
                r"[0-9a-f]{64}",
                str(payload.get("runtime_attestation_digest")),
            )
        )
        or (
            payload.get("blocker") is not None
            and payload.get("blocker") != "runtime_attestation_invalid"
        )
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
    if (
        handoff.lease_epoch == previous_epoch
        and state["handoff_digest"] is not None
        and handoff.digest != state["handoff_digest"]
    ):
        raise AdapterError("broker handoff changed without an epoch increment")


def _policy_path(config: AdapterConfig) -> str:
    environment = urllib.parse.quote(config.environment, safe="")
    pool_name = urllib.parse.quote(config.pool_name, safe="")
    return f"/admin/worker-pool-autoscaler-policies/{environment}/{pool_name}"


def _bootstrap_policy_body(
    config: AdapterConfig,
    *,
    candidate_sha: str,
) -> dict[str, Any]:
    path = _POLICY_TEMPLATE_DIR / f"{config.pool_name}.toml"
    resolved = _secure_regular_file(path, label="autoscaler policy template")
    try:
        payload = tomllib.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise AdapterError("autoscaler policy template is invalid") from exc
    if set(payload) != {
        "schema_version",
        "pool_name",
        "slot_budget",
        "pending_slot_budget",
        "job_pids_max",
        "policy",
    }:
        raise AdapterError("autoscaler policy template fields do not match schema")
    if (
        payload.get("schema_version") != _SCHEMA_VERSION
        or payload.get("pool_name") != config.pool_name
        or payload.get("slot_budget") != config.max_slots_bound
    ):
        raise AdapterError("autoscaler policy template target or budget drifted")
    policy = payload.get("policy")
    if not isinstance(policy, dict):
        raise AdapterError("autoscaler policy template policy is invalid")
    if (
        policy.get("enabled") is not True
        or policy.get("min_slots") != 0
        or policy.get("max_slots") != config.max_slots_bound
    ):
        raise AdapterError("autoscaler policy template reviewed capacity drifted")
    body = cast(
        dict[str, Any],
        json.loads(
            json.dumps(policy)
            .replace("${SANDBOX}", config.sandbox)
            .replace("${SLURM_ACCOUNT}", config.slurm_account)
            .replace("${SLURM_QOS}", config.slurm_qos)
            .replace("${RUNTIME_ROOT}", str(config.runtime_root))
            .replace("${CANDIDATE_ROOT}", str(config.candidate_root))
            .replace("${CANDIDATE_SHA}", candidate_sha),
        ),
    )
    actuator_config = body.get("actuator_config")
    job_pids_max = payload.get("job_pids_max")
    pending_budget = payload.get("pending_slot_budget")
    expected_env_file = str(
        config.runtime_root / candidate_sha / f"worker-{config.pool_name}.env",
    )
    expected_repo_dir = str(config.candidate_root / candidate_sha)
    if (
        not isinstance(actuator_config, dict)
        or isinstance(job_pids_max, bool)
        or not isinstance(job_pids_max, int)
        or job_pids_max <= 0
        or actuator_config.get("job_pids_max") != job_pids_max
        or actuator_config.get("env_file") != expected_env_file
        or actuator_config.get("repo_dir") != expected_repo_dir
        or actuator_config.get("slurm_account") != config.slurm_account
        or actuator_config.get("qos_normal") != config.slurm_qos
        or any("${" in value for value in actuator_config.values() if isinstance(value, str))
        or isinstance(pending_budget, bool)
        or not isinstance(pending_budget, int)
        or pending_budget <= 0
    ):
        raise AdapterError("autoscaler policy template containment budget drifted")
    container_pids = actuator_config.get("container_pids")
    concurrency = actuator_config.get("requested_concurrency")
    if (
        isinstance(container_pids, bool)
        or not isinstance(container_pids, int)
        or container_pids <= 0
        or isinstance(concurrency, bool)
        or not isinstance(concurrency, int)
        or concurrency <= 0
        or job_pids_max < container_pids * concurrency
    ):
        raise AdapterError("autoscaler policy job PID budget is below concurrency bound")
    # The checked-in template describes the reviewed activated capacity.  A
    # bootstrap must still be fail-closed until a current broker handoff is
    # bound and independently validated by the adapter.
    body["enabled"] = False
    body["min_slots"] = 0
    body["max_slots"] = 0
    body["disabled_reason"] = "shared_capacity_handoff_disabled"
    return body


def _validate_bootstrap_policy(
    policy: Mapping[str, Any],
    *,
    config: AdapterConfig,
    candidate_sha: str,
    expected_body: Mapping[str, Any],
) -> None:
    _validate_policy(policy, config=config, candidate_sha=candidate_sha)
    actuator_config = policy["actuator_config"]
    expected_actuator_config = expected_body["actuator_config"]
    assert isinstance(actuator_config, dict)
    assert isinstance(expected_actuator_config, dict)
    immutable_fields = (
        "backend",
        "cpu_arch",
        "partition",
        "allowed_nodes",
        "env_file",
        "repo_dir",
        "requested_cpus",
        "requested_memory_mib",
        "requested_concurrency",
        "max_jobs",
        "pending_job_cap",
        "time_limit",
        "exclusive",
        "external_runner",
        "shared_capacity_managed",
        "slurm_account",
        "qos_normal",
        "container_cpus",
        "container_memory_mib",
        "container_pids",
        "job_pids_max",
        "candidate_sha",
        "gpu_tres",
    )
    if any(
        actuator_config.get(field) != expected_actuator_config.get(field)
        for field in immutable_fields
    ):
        raise AdapterError("autoscaler policy immutable bootstrap contract drifted")
    for field in (
        "container_cpus",
        "container_memory_mib",
        "container_pids",
        "job_pids_max",
    ):
        value = actuator_config.get(field)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise AdapterError("autoscaler policy containment contract drifted")
    if (
        actuator_config.get("exclusive") is not False
        or actuator_config.get("external_runner") is not True
        or actuator_config.get("shared_capacity_managed") is not True
        or actuator_config["job_pids_max"]
        < actuator_config["container_pids"] * actuator_config["requested_concurrency"]
        or not actuator_config.get("allowed_nodes")
        or not actuator_config.get("slurm_account")
        or not actuator_config.get("qos_normal")
    ):
        raise AdapterError("autoscaler policy lacks non-exclusive containment authority")


def bootstrap_policy(
    config: AdapterConfig,
    *,
    now: datetime | None = None,
    clock: Clock | None = None,
    http_json: HttpJson = _http_json,
) -> dict[str, Any]:
    # Resolve the injected time API for consistency, but bootstrap deliberately
    # does not consume a broker handoff or activate capacity.
    _clock_now(_resolve_clock(now=now, clock=clock))
    with _exclusive_adapter_lock(config):
        candidate = _load_sandbox_binding(config)
        expected = _bootstrap_policy_body(config, candidate_sha=candidate.sha)
        token = _load_admin_token(config.admin_secret_file)
        path = _policy_path(config)
        policy, missing = _get_policy(
            config,
            token=token,
            path=path,
            http_json=http_json,
        )
        status = "unchanged"
        if missing:
            http_json(
                method="PUT",
                base_url=config.control_plane_url,
                token=token,
                path=path,
                body=expected,
                timeout=config.timeout_seconds,
            )
            policy, still_missing = _get_policy(
                config,
                token=token,
                path=path,
                http_json=http_json,
            )
            if still_missing or policy is None:
                raise AdapterError("autoscaler bootstrap readback is missing")
            status = "created"
        assert policy is not None
        _validate_bootstrap_policy(
            policy,
            config=config,
            candidate_sha=candidate.sha,
            expected_body=expected,
        )
        lease_state = _capacity_lease_state(policy)
        if lease_state is None and (
            policy.get("enabled") is not False
            or policy.get("min_slots") != 0
            or policy.get("max_slots") != 0
        ):
            raise AdapterError("unbound autoscaler bootstrap is not disabled")
        return {
            "schema_version": _SCHEMA_VERSION,
            "artifact_type": "shared-capacity-policy-bootstrap-result",
            "status": status,
            "sandbox": config.sandbox,
            "environment": config.environment,
            "pool_name": config.pool_name,
            "candidate_sha": candidate.sha,
            "enabled": bool(policy.get("enabled")),
            "bound": lease_state is not None,
        }


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


def _capacity_binding(
    *,
    request_id: object,
    lease_epoch: object,
    candidate_sha: object,
    preemptible: object,
    label: str,
) -> dict[str, Any]:
    if (
        not isinstance(request_id, str)
        or _UUID_RE.fullmatch(request_id) is None
        or isinstance(lease_epoch, bool)
        or not isinstance(lease_epoch, int)
        or lease_epoch < 0
        or not isinstance(candidate_sha, str)
        or _SHA_RE.fullmatch(candidate_sha) is None
        or not isinstance(preemptible, bool)
    ):
        raise AdapterError(f"{label} is invalid")
    return {
        "schema_version": _SCHEMA_VERSION,
        "request_id": request_id,
        "lease_epoch": lease_epoch,
        "candidate_sha": candidate_sha,
        "preemptible": preemptible,
    }


def _handoff_binding(handoff: Handoff) -> dict[str, Any]:
    return _capacity_binding(
        request_id=handoff.request_id,
        lease_epoch=handoff.lease_epoch,
        candidate_sha=handoff.candidate_sha,
        preemptible=handoff.preemptible,
        label="broker handoff capacity binding",
    )


def _capacity_lease_state(policy: Mapping[str, Any]) -> dict[str, Any] | None:
    raw = policy.get("capacity_lease_state")
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise AdapterError("autoscaler capacity_lease_state must be an object")
    state = raw.get("state")
    if state not in _CAPACITY_LEASE_STATES:
        raise AdapterError("autoscaler capacity_lease_state.state is invalid")
    binding = _capacity_binding(
        request_id=raw.get("request_id"),
        lease_epoch=raw.get("lease_epoch"),
        candidate_sha=raw.get("candidate_sha"),
        preemptible=raw.get("preemptible"),
        label="autoscaler capacity_lease_state binding",
    )
    expected = _CAPACITY_BINDING_FIELDS | {"state", "activated_at"}
    if state == "retiring":
        expected |= {"retire_started_at", "retire_reason"}
    elif state == "retired":
        expected |= {"retire_started_at", "retire_reason", "retired_at"}
    if set(raw) != expected:
        raise AdapterError("autoscaler capacity_lease_state fields are invalid")
    for field in expected - _CAPACITY_BINDING_FIELDS - {"state", "retire_reason"}:
        _parse_capacity_timestamp(raw.get(field), field=field)
    if "retire_reason" in expected and (
        not isinstance(raw.get("retire_reason"), str) or not raw["retire_reason"]
    ):
        raise AdapterError("autoscaler capacity_lease_state.retire_reason is invalid")
    return {**raw, **binding}


def _parse_capacity_timestamp(value: object, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise AdapterError(f"autoscaler capacity_lease_state.{field} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AdapterError(
            f"autoscaler capacity_lease_state.{field} is invalid",
        ) from exc
    if parsed.tzinfo is None:
        raise AdapterError(f"autoscaler capacity_lease_state.{field} is invalid")
    return parsed.astimezone(UTC)


def _validate_capacity_lease_readback(
    policy: Mapping[str, Any],
    *,
    binding: Mapping[str, Any],
    enabled: bool,
) -> dict[str, Any]:
    state = _capacity_lease_state(policy)
    if state is None:
        raise AdapterError("autoscaler response omitted capacity_lease_state")
    if any(state[field] != binding[field] for field in _CAPACITY_BINDING_FIELDS):
        raise AdapterError("autoscaler capacity_lease_state binding differs from handoff")
    if enabled and state["state"] != "active":
        raise AdapterError("enabled autoscaler capacity lease is not active")
    if not enabled and state["state"] not in {"retiring", "retired"}:
        raise AdapterError("disabled autoscaler capacity lease is not retiring")
    return state


def _validate_runtime_attestation(
    config: AdapterConfig,
    *,
    candidate: CandidateBinding,
    now: datetime,
    minimum_remaining: timedelta = timedelta(0),
) -> str:
    if minimum_remaining < timedelta(0):
        raise AdapterError("runtime attestation minimum lifetime is invalid")
    path = config.runtime_attestation_root / config.sandbox / candidate.sha / "combined.json"
    resolved = _secure_runtime_attestation_file(path, config=config)
    try:
        raw = resolved.read_bytes()
    except OSError as exc:
        raise AdapterError("combined runtime attestation is unreadable") from exc
    if not raw or len(raw) > _ATTESTATION_MAX_BYTES:
        raise AdapterError("combined runtime attestation size is invalid")
    try:
        receipt = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdapterError("combined runtime attestation is invalid JSON") from exc
    if not isinstance(receipt, dict) or set(receipt) != _RECEIPT_TOP_LEVEL_FIELDS:
        raise AdapterError("combined runtime attestation fields are invalid")
    if (
        receipt.get("schema_version") != _SCHEMA_VERSION
        or receipt.get("kind") != "loom.developer-runtime-combined-activation"
        or receipt.get("sandbox") != config.sandbox
        or receipt.get("candidate_sha") != candidate.sha
        or receipt.get("candidate_tree") != candidate.tree
    ):
        raise AdapterError("combined runtime attestation identity is invalid")
    digest = receipt.get("payload_sha256")
    unsigned = dict(receipt)
    unsigned.pop("payload_sha256")
    expected_digest = hashlib.sha256(_canonical_json(unsigned)).hexdigest()
    if not isinstance(digest, str) or digest != expected_digest:
        raise AdapterError("combined runtime attestation digest is invalid")
    if raw != _canonical_json(receipt) + b"\n":
        raise AdapterError("combined runtime attestation is not canonical")

    collector = receipt.get("collector")
    fleet = receipt.get("fleet_attestation")
    domains = receipt.get("domains")
    if (
        not isinstance(collector, dict)
        or set(collector) != {"hostname", "collected_at", "expires_at"}
        or not isinstance(fleet, dict)
        or set(fleet) != _RECEIPT_FLEET_FIELDS
        or not isinstance(domains, dict)
        or set(domains) != {"oldlab", "gb10"}
    ):
        raise AdapterError("combined runtime attestation sections are invalid")
    collected_at = _parse_attestation_timestamp(
        collector.get("collected_at"),
        field="collector.collected_at",
    )
    expires_at = _parse_attestation_timestamp(
        collector.get("expires_at"),
        field="collector.expires_at",
    )
    if (
        collector.get("hostname") != _COLLECTOR_HOSTNAME
        or not timedelta(0) < expires_at - collected_at <= _ATTESTATION_TTL
        or collected_at > now + timedelta(seconds=30)
        or now - collected_at > _ATTESTATION_TTL
        or expires_at <= now + minimum_remaining
    ):
        raise AdapterError("combined runtime attestation is stale or expired")
    _validate_fleet_reference(
        fleet,
        config=config,
        candidate=candidate,
        collected_at=collected_at,
        combined_expires_at=expires_at,
        now=now,
    )
    for domain in ("oldlab", "gb10"):
        _validate_domain_reference(
            domains[domain],
            domain=domain,
            config=config,
            candidate=candidate,
            collected_at=collected_at,
            combined_expires_at=expires_at,
            now=now,
        )
    return digest


def _canonical_json(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()


def _secure_runtime_attestation_file(
    path: Path,
    *,
    config: AdapterConfig,
) -> Path:
    expected_parent = config.runtime_attestation_root / config.sandbox / path.parent.name
    if path.parent != expected_parent or path.name != "combined.json":
        raise AdapterError("combined runtime attestation path is invalid")
    for directory in (
        config.runtime_attestation_root,
        config.runtime_attestation_root / config.sandbox,
        path.parent,
    ):
        try:
            metadata = directory.lstat()
        except OSError as exc:
            raise AdapterError("combined runtime attestation directory is unavailable") from exc
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise AdapterError("combined runtime attestation directory is unsafe")
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise AdapterError("combined runtime attestation is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise AdapterError("combined runtime attestation file is unsafe")
    return path.resolve(strict=True)


def _parse_attestation_timestamp(value: object, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise AdapterError(f"combined runtime attestation {field} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AdapterError(
            f"combined runtime attestation {field} is invalid",
        ) from exc
    if parsed.tzinfo is None:
        raise AdapterError(f"combined runtime attestation {field} is invalid")
    return parsed.astimezone(UTC)


def _validate_fleet_reference(
    fleet: Mapping[str, Any],
    *,
    config: AdapterConfig,
    candidate: CandidateBinding,
    collected_at: datetime,
    combined_expires_at: datetime,
    now: datetime,
) -> None:
    expected_path = _FLEET_ATTESTATION_ROOT / config.sandbox / candidate.sha / "fleet.json"
    generated_at = _parse_attestation_timestamp(
        fleet.get("generated_at"),
        field="fleet_attestation.generated_at",
    )
    expires_at = _parse_attestation_timestamp(
        fleet.get("expires_at"),
        field="fleet_attestation.expires_at",
    )
    if (
        fleet.get("path") != str(expected_path)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", str(fleet.get("payload_sha256"))) is None
        or expires_at - generated_at != _ATTESTATION_TTL
        or generated_at > collected_at + timedelta(seconds=30)
        or collected_at - generated_at > timedelta(seconds=60)
        or expires_at < combined_expires_at
        or expires_at <= now
    ):
        raise AdapterError("combined runtime fleet attestation is invalid or stale")


def _validate_domain_reference(
    value: object,
    *,
    domain: str,
    config: AdapterConfig,
    candidate: CandidateBinding,
    collected_at: datetime,
    combined_expires_at: datetime,
    now: datetime,
) -> None:
    if not isinstance(value, dict) or set(value) != _RECEIPT_DOMAIN_FIELDS:
        raise AdapterError("combined runtime domain attestation fields are invalid")
    root = _DOMAIN_ATTESTATION_ROOT / config.sandbox / candidate.sha
    published_at = _parse_attestation_timestamp(
        value.get("published_at"),
        field=f"domains.{domain}.published_at",
    )
    expires_at = _parse_attestation_timestamp(
        value.get("expires_at"),
        field=f"domains.{domain}.expires_at",
    )
    generation = value.get("generation")
    if (
        value.get("manifest_path") != str(root / f"{domain}.json")
        or value.get("signature_path") != str(root / f"{domain}.sig")
        or any(
            re.fullmatch(r"[0-9a-f]{64}", str(value.get(field))) is None
            for field in (
                "payload_sha256",
                "signature_sha256",
                "key_id",
            )
        )
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 1
        or expires_at - published_at != _ATTESTATION_TTL
        or published_at > collected_at + timedelta(seconds=30)
        or collected_at - published_at > _ATTESTATION_TTL
        or expires_at < combined_expires_at
        or expires_at <= now
    ):
        raise AdapterError("combined runtime domain attestation is invalid or stale")


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
        counters: list[int] = []
        for value in raw:
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise AdapterError("autoscaler observation counters are incomplete")
            counters.append(value)
        pending, active, draining = counters
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
    binding: Mapping[str, Any],
) -> dict[str, Any]:
    body = {field: policy[field] for field in _POLICY_COPY_FIELDS}
    body.update(
        {
            "enabled": enabled,
            "min_slots": 0,
            "max_slots": max_slots,
            "disabled_reason": (None if enabled else "shared_capacity_handoff_disabled"),
            "shared_capacity_binding": dict(binding),
        },
    )
    return body


def _get_policy(
    config: AdapterConfig,
    *,
    token: str,
    path: str,
    http_json: HttpJson,
) -> tuple[dict[str, Any] | None, bool]:
    try:
        return (
            http_json(
                method="GET",
                base_url=config.control_plane_url,
                token=token,
                path=path,
                timeout=config.timeout_seconds,
            ),
            False,
        )
    except PolicyMissingError:
        return None, True


def _validate_policy_update_readback(
    policy: Mapping[str, Any],
    *,
    config: AdapterConfig,
    candidate_sha: str,
    expected_policy: Mapping[str, Any],
    binding: Mapping[str, Any],
    enabled: bool,
    max_slots: int,
) -> None:
    _validate_policy(policy, config=config, candidate_sha=candidate_sha)
    _validate_bootstrap_policy(
        policy,
        config=config,
        candidate_sha=candidate_sha,
        expected_body=expected_policy,
    )
    _validate_capacity_lease_readback(
        policy,
        binding=binding,
        enabled=enabled,
    )
    if (
        policy.get("enabled") != enabled
        or policy.get("min_slots") != 0
        or policy.get("max_slots") != max_slots
    ):
        raise AdapterError("autoscaler policy readback does not match handoff")


def _put_policy_and_readback(
    config: AdapterConfig,
    *,
    token: str,
    path: str,
    policy: Mapping[str, Any],
    expected_policy: Mapping[str, Any],
    candidate_sha: str,
    binding: Mapping[str, Any],
    enabled: bool,
    max_slots: int,
    http_json: HttpJson,
) -> dict[str, Any]:
    http_json(
        method="PUT",
        base_url=config.control_plane_url,
        token=token,
        path=path,
        body=_policy_update_body(
            policy,
            enabled=enabled,
            max_slots=max_slots,
            binding=binding,
        ),
        timeout=config.timeout_seconds,
    )
    readback = http_json(
        method="GET",
        base_url=config.control_plane_url,
        token=token,
        path=path,
        timeout=config.timeout_seconds,
    )
    _validate_policy_update_readback(
        readback,
        config=config,
        candidate_sha=candidate_sha,
        expected_policy=expected_policy,
        binding=binding,
        enabled=enabled,
        max_slots=max_slots,
    )
    return readback


def _retire_mismatched_active_policy(
    config: AdapterConfig,
    *,
    policy: Mapping[str, Any] | None,
    token: str,
    path: str,
    http_json: HttpJson,
) -> None:
    if policy is None:
        return
    lease_state = _capacity_lease_state(policy)
    if lease_state is None or lease_state["state"] != "active":
        return
    old_candidate = lease_state["candidate_sha"]
    if (
        policy.get("enabled") is not True
        or isinstance(policy.get("max_slots"), bool)
        or not isinstance(policy.get("max_slots"), int)
        or policy["max_slots"] <= 0
        or not isinstance(old_candidate, str)
    ):
        raise AdapterError("mismatched active capacity policy is inconsistent")
    expected_policy = _bootstrap_policy_body(config, candidate_sha=old_candidate)
    _validate_bootstrap_policy(
        policy,
        config=config,
        candidate_sha=old_candidate,
        expected_body=expected_policy,
    )
    binding = {field: lease_state[field] for field in _CAPACITY_BINDING_FIELDS}
    _put_policy_and_readback(
        config,
        token=token,
        path=path,
        policy=policy,
        expected_policy=expected_policy,
        candidate_sha=old_candidate,
        binding=binding,
        enabled=False,
        max_slots=0,
        http_json=http_json,
    )


def _load_observation_sequence(path: Path) -> int:
    if not path.exists() and not path.is_symlink():
        return 0
    resolved = _secure_regular_file(path, label="adapter observation")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AdapterError("adapter observation is invalid") from exc
    if not isinstance(payload, list) or len(payload) != 1:
        raise AdapterError("adapter observation must contain exactly one item")
    observation = payload[0]
    if not isinstance(observation, dict):
        raise AdapterError("adapter observation item is invalid")
    legacy_fields = {
        "request_id",
        "lease_epoch",
        "pending_slots",
        "active_slots",
        "draining_slots",
        "terminal_slots",
    }
    if set(observation) == legacy_fields:
        return 0
    if set(observation) != _OBSERVATION_FIELDS:
        raise AdapterError("adapter observation fields do not match the closed schema")
    sequence = observation.get("observation_sequence")
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
        raise AdapterError("adapter observation sequence is invalid")
    digest = observation.get("payload_sha256")
    unsigned = dict(observation)
    unsigned.pop("payload_sha256")
    expected_digest = hashlib.sha256(_canonical_json(unsigned)).hexdigest()
    if not isinstance(digest, str) or digest != expected_digest:
        raise AdapterError("adapter observation digest is invalid")
    return sequence


def _observation_payload(
    *,
    config: AdapterConfig,
    candidate_sha: str,
    handoff: Handoff,
    capacity_lease_state: str,
    observed_at: datetime,
    observation_sequence: int,
    pending: int,
    active: int,
    draining: int,
    terminal: int,
) -> dict[str, Any]:
    unsigned = {
        "sandbox": config.sandbox,
        "pool_name": config.pool_name,
        "candidate_sha": candidate_sha,
        "request_id": handoff.request_id,
        "lease_epoch": handoff.lease_epoch,
        "capacity_lease_state": capacity_lease_state,
        "observed_at": observed_at.isoformat().replace("+00:00", "Z"),
        "observation_sequence": observation_sequence,
        "pending_slots": pending,
        "active_slots": active,
        "draining_slots": draining,
        "terminal_slots": terminal,
    }
    return {
        **unsigned,
        "payload_sha256": hashlib.sha256(_canonical_json(unsigned)).hexdigest(),
    }


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
        data = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
        descriptor = os.open(
            temporary,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            os.write(descriptor, data)
            os.fchmod(descriptor, 0o600)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, path)
        path.chmod(0o600)
        directory = os.open(
            parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
        )
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def _synthetic_disable_handoff(
    config: AdapterConfig,
    *,
    binding: Mapping[str, Any],
    now: datetime,
    digest: str | None,
) -> Handoff:
    normalized = _capacity_binding(
        request_id=binding.get("request_id"),
        lease_epoch=binding.get("lease_epoch"),
        candidate_sha=binding.get("candidate_sha"),
        preemptible=binding.get("preemptible"),
        label="durable capacity binding",
    )
    return Handoff(
        request_id=normalized["request_id"],
        lease_epoch=normalized["lease_epoch"],
        sandbox=config.sandbox,
        environment=config.environment,
        candidate_sha=normalized["candidate_sha"],
        pool_name=config.pool_name,
        enabled=False,
        min_slots=0,
        max_slots=0,
        expires_at=now,
        preemptible=normalized["preemptible"],
        digest=digest,
    )


def _run_once_unlocked(
    config: AdapterConfig,
    *,
    now: datetime | None = None,
    clock: Clock | None = None,
    http_json: HttpJson = _http_json,
) -> dict[str, Any]:
    current_time = _resolve_clock(now=now, clock=clock)
    cycle_now = _clock_now(current_time)
    state = _load_adapter_state(config.adapter_state_path)
    handoff_missing = False
    try:
        handoff: Handoff | None = load_handoff(config)
    except AdapterError:
        if config.handoff_path.exists() or config.handoff_path.is_symlink():
            raise
        handoff_missing = True
        handoff = None
    candidate = _load_sandbox_binding(config)
    candidate_sha = candidate.sha
    if handoff is not None:
        _validate_epoch(handoff, state)
    token = _load_admin_token(config.admin_secret_file)
    path = _policy_path(config)
    candidate_mismatch = handoff is not None and candidate_sha != handoff.candidate_sha
    activation_requested = (
        handoff is not None
        and not candidate_mismatch
        and handoff.enabled
        and cycle_now < handoff.expires_at
    )
    with ExitStack() as stack:
        if activation_requested:
            stack.enter_context(_shared_collector_lock(config))
        policy, policy_missing = _get_policy(
            config,
            token=token,
            path=path,
            http_json=http_json,
        )
        if candidate_mismatch:
            _retire_mismatched_active_policy(
                config,
                policy=policy,
                token=token,
                path=path,
                http_json=http_json,
            )
            raise AdapterError(
                "sandbox candidate does not match broker handoff; old active lease retired",
            )

        expected_policy = _bootstrap_policy_body(
            config,
            candidate_sha=candidate_sha,
        )
        if policy is not None:
            _validate_bootstrap_policy(
                policy,
                config=config,
                candidate_sha=candidate_sha,
                expected_body=expected_policy,
            )
        if handoff is None and state is not None:
            handoff = _synthetic_disable_handoff(
                config,
                binding=state,
                now=cycle_now,
                digest=state["handoff_digest"],
            )
        elif handoff is None and policy is not None:
            durable_policy_state = _capacity_lease_state(policy)
            if durable_policy_state is None:
                raise AdapterError("missing handoff has no durable capacity binding")
            handoff = _synthetic_disable_handoff(
                config,
                binding=durable_policy_state,
                now=cycle_now,
                digest=None,
            )
        if handoff is None:
            raise AdapterError("missing handoff has no durable capacity binding")
        if handoff_missing:
            _validate_epoch(handoff, state)
        same_request = state is not None and state["request_id"] == handoff.request_id
        if policy is None:
            policy = {
                **expected_policy,
                "environment": config.environment,
                "pool_name": config.pool_name,
                "capacity_lease_state": None,
                "last_pending_slots": None,
                "last_actual_slots": None,
                "last_draining_slots": None,
            }
        pending, active, draining, terminal = _observation_counts(
            policy,
            state=state,
            same_request=same_request,
        )
        expired = handoff_missing or _clock_now(current_time) >= handoff.expires_at
        target_enabled = handoff.enabled and not expired
        target_max_slots = handoff.max_slots if target_enabled else 0
        binding = _handoff_binding(handoff)
        runtime_attestation_digest = None
        runtime_attestation_status = "not_required"
        blocker = None
        attestation_error: AdapterError | None = None
        safety_window = timedelta(seconds=config.timeout_seconds * 3)
        if target_enabled:
            try:
                runtime_attestation_digest = _validate_runtime_attestation(
                    config,
                    candidate=candidate,
                    now=_clock_now(current_time),
                    minimum_remaining=safety_window,
                )
                runtime_attestation_status = "verified"
            except AdapterError as exc:
                if policy_missing:
                    raise
                attestation_error = exc
                runtime_attestation_status = "rejected"
                blocker = "runtime_attestation_invalid"
                target_enabled = False
                target_max_slots = 0

        current_enabled = policy.get("enabled")
        current_max_slots = policy.get("max_slots")
        if not isinstance(current_enabled, bool):
            raise AdapterError("autoscaler policy enabled is invalid")
        current_lease_state = _capacity_lease_state(policy)
        lease_binding_matches = current_lease_state is not None and all(
            current_lease_state[field] == binding[field] for field in _CAPACITY_BINDING_FIELDS
        )
        lease_state_matches = current_lease_state is not None and (
            (target_enabled and current_lease_state["state"] == "active")
            or (not target_enabled and current_lease_state["state"] in {"retiring", "retired"})
        )
        changed = (
            policy_missing
            or current_enabled != target_enabled
            or current_max_slots != target_max_slots
            or not lease_binding_matches
            or not lease_state_matches
        )
        applied_policy = policy
        if changed:
            if target_enabled:
                mutation_now = _clock_now(current_time)
                try:
                    if mutation_now >= handoff.expires_at:
                        raise AdapterError("broker handoff expired before activation")
                    current_digest = _validate_runtime_attestation(
                        config,
                        candidate=candidate,
                        now=mutation_now,
                        minimum_remaining=safety_window,
                    )
                    if current_digest != runtime_attestation_digest:
                        raise AdapterError(
                            "combined runtime attestation changed before activation",
                        )
                except AdapterError as exc:
                    if policy_missing:
                        raise
                    attestation_error = exc
                    runtime_attestation_digest = None
                    runtime_attestation_status = "rejected"
                    blocker = "runtime_attestation_invalid"
                    target_enabled = False
                    target_max_slots = 0
            try:
                applied_policy = _put_policy_and_readback(
                    config,
                    token=token,
                    path=path,
                    policy=policy,
                    expected_policy=expected_policy,
                    candidate_sha=candidate_sha,
                    binding=binding,
                    enabled=target_enabled,
                    max_slots=target_max_slots,
                    http_json=http_json,
                )
            except AdapterError as exc:
                if not target_enabled:
                    raise
                applied_policy = _put_policy_and_readback(
                    config,
                    token=token,
                    path=path,
                    policy=policy,
                    expected_policy=expected_policy,
                    candidate_sha=candidate_sha,
                    binding=binding,
                    enabled=False,
                    max_slots=0,
                    http_json=http_json,
                )
                attestation_error = exc
                runtime_attestation_digest = None
                runtime_attestation_status = "rejected"
                blocker = "runtime_attestation_invalid"
                target_enabled = False
                target_max_slots = 0
        else:
            _validate_capacity_lease_readback(
                policy,
                binding=binding,
                enabled=target_enabled,
            )

        if target_enabled:
            try:
                proof_now = _clock_now(current_time)
                if proof_now >= handoff.expires_at:
                    raise AdapterError("broker handoff expired during activation")
                current_digest = _validate_runtime_attestation(
                    config,
                    candidate=candidate,
                    now=proof_now,
                )
                if current_digest != runtime_attestation_digest:
                    raise AdapterError(
                        "combined runtime attestation changed during activation",
                    )
            except AdapterError as exc:
                applied_policy = _put_policy_and_readback(
                    config,
                    token=token,
                    path=path,
                    policy=applied_policy,
                    expected_policy=expected_policy,
                    candidate_sha=candidate_sha,
                    binding=binding,
                    enabled=False,
                    max_slots=0,
                    http_json=http_json,
                )
                attestation_error = exc
                runtime_attestation_digest = None
                runtime_attestation_status = "rejected"
                blocker = "runtime_attestation_invalid"
                target_enabled = False
                target_max_slots = 0
                changed = True

        final_now = _clock_now(current_time)
        final_lease_state = _capacity_lease_state(applied_policy)
        if final_lease_state is None:
            raise AdapterError("autoscaler readback omitted final capacity lease state")
        previous_sequence = max(
            int(state["observation_sequence"]) if state is not None else 0,
            _load_observation_sequence(config.observation_path),
        )
        observation_sequence = previous_sequence + 1
        observation = _observation_payload(
            config=config,
            candidate_sha=candidate_sha,
            handoff=handoff,
            capacity_lease_state=str(final_lease_state["state"]),
            observed_at=final_now,
            observation_sequence=observation_sequence,
            pending=pending,
            active=active,
            draining=draining,
            terminal=terminal,
        )
        adapter_state = {
            "schema_version": _SCHEMA_VERSION,
            "request_id": handoff.request_id,
            "lease_epoch": handoff.lease_epoch,
            "handoff_digest": handoff.digest,
            "candidate_sha": candidate_sha,
            "preemptible": handoff.preemptible,
            "applied_enabled": target_enabled,
            "applied_max_slots": target_max_slots,
            "pending_slots": pending,
            "active_slots": active,
            "draining_slots": draining,
            "terminal_slots": terminal,
            "runtime_attestation_status": runtime_attestation_status,
            "runtime_attestation_digest": runtime_attestation_digest,
            "blocker": blocker,
            "observation_sequence": observation_sequence,
            "updated_at": final_now.isoformat().replace("+00:00", "Z"),
        }
        _atomic_json_write(config.observation_path, [observation])
        _atomic_json_write(config.adapter_state_path, adapter_state)
        if attestation_error is not None:
            raise AdapterError(
                "runtime attestation rejected after fail-closed disable",
            ) from attestation_error
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
            "preemptible": handoff.preemptible,
            "enabled": target_enabled,
            "max_slots": target_max_slots,
            "expired": expired,
            "handoff_missing": handoff_missing,
            "runtime_attestation_digest": runtime_attestation_digest,
            "runtime_attestation_status": runtime_attestation_status,
            "blocker": blocker,
            "observation": observation,
        }


def run_once(
    config: AdapterConfig,
    *,
    now: datetime | None = None,
    clock: Clock | None = None,
    http_json: HttpJson = _http_json,
) -> dict[str, Any]:
    with _exclusive_adapter_lock(config):
        return _run_once_unlocked(
            config,
            now=now,
            clock=clock,
            http_json=http_json,
        )


def run_cycle(
    config: AdapterConfig,
    *,
    now: datetime | None = None,
    clock: Clock | None = None,
    http_json: HttpJson = _http_json,
) -> dict[str, Any]:
    with _exclusive_adapter_lock(config):
        return _run_once_unlocked(
            config,
            now=now,
            clock=clock,
            http_json=http_json,
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("command", choices=("bootstrap", "run"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        config = load_config(args.config)
        report = bootstrap_policy(config) if args.command == "bootstrap" else run_cycle(config)
    except (AdapterError, OSError, UnicodeError, ValueError):
        sys.stderr.write('{"error":"shared-capacity-adapter-failed-safely"}\n')
        return 1
    sys.stdout.write(json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
