#!/usr/bin/env python3
"""Validate and stage prod-first shared worker capacity desired state.

The manifest is a release contract, not a live mutator. It describes how shared
physical GB10/OLDLAB hosts are assigned between production and beta/dev, then
optionally compares that desired state with a secret-free observed worker
registration artifact.

Lifecycle commands are file-only:

* ``status`` reports the effective beta lease state and TTL expiry.
* ``lease-beta`` previews or writes a bounded beta/dev lease.
* ``drain-beta`` stops new beta claims and reports running vs. idle beta slots.
* ``release-beta`` idempotently returns beta desired slots to zero.
"""

from __future__ import annotations

import argparse
import copy
import json
import re
import sys
import tomllib
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import tomli_w

SCHEMA_VERSION = 1
HOST_STATES = frozenset({"eligible", "beta_draining", "host_draining", "unreachable"})
INACTIVE_WORKER_STATES = frozenset({"drained", "stopped", "offline", "unreachable"})
BETA_DRAINING_STATES = frozenset({"draining", "drained", "stopped", "offline"})
COMMANDS = frozenset({"status", "lease-beta", "release-beta", "drain-beta"})

_PLACEHOLDER_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_SECRET_KEY_RE = re.compile(
    r"(?:authorization|credential|password|private[_-]?key|access[_-]?key|"
    r"api[_-]?key|secret|token)",
    re.IGNORECASE,
)
_SECRET_VALUE_PATTERNS = (
    re.compile(r"\b[Bb]earer\s+[A-Za-z0-9._~+/=-]{8,}"),
    re.compile(r"\bsk-[A-Za-z0-9][A-Za-z0-9_-]{10,}"),
    re.compile(r"\bghp_[A-Za-z0-9_]{10,}"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{10,}"),
    re.compile(r"\bloom_(?:api|w)_[A-Za-z0-9._-]{8,}", re.IGNORECASE),
    re.compile(
        r"(?i)(X-Amz-Signature|AWSAccessKeyId|Signature|token|api_key|access_key)="
        r"[^&\s]+",
    ),
    re.compile(r"://([^:/@\s]+):([^@\s]+)@"),
)


class ManifestError(ValueError):
    """Raised when the capacity manifest cannot be parsed safely."""


@dataclass(frozen=True)
class EnvironmentTarget:
    key: str
    name: str
    api_url: str
    image_tag: str
    source_commit: str
    compose_service: str
    k8s_deployment: str
    k8s_namespace: str

    def public_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class HostIntent:
    name: str
    pool: str
    total_slots: int
    state: str
    prod_slots: int
    beta_slots: int
    reason: str | None

    def public_dict(self, prod: EnvironmentTarget, beta: EnvironmentTarget) -> dict[str, Any]:
        return {
            "host": self.name,
            "pool": self.pool,
            "state": self.state,
            "reason": self.reason,
            "total_slots": self.total_slots,
            "prod_slots": self.prod_slots,
            "beta_slots": self.beta_slots,
            "prod": {
                "environment": prod.name,
                "api_url": prod.api_url,
                "image_tag": prod.image_tag,
                "source_commit": prod.source_commit,
                "drain_state": "active" if self.prod_slots > 0 else "idle",
            },
            "beta": {
                "environment": beta.name,
                "api_url": beta.api_url,
                "image_tag": beta.image_tag,
                "source_commit": beta.source_commit,
                "drain_state": _beta_drain_state(self),
            },
        }


@dataclass(frozen=True)
class CapacityManifest:
    path: Path
    prod: EnvironmentTarget
    beta: EnvironmentTarget
    hosts: tuple[HostIntent, ...]
    unresolved_placeholders: tuple[str, ...]

    def summary(self) -> dict[str, Any]:
        prod_slots = sum(host.prod_slots for host in self.hosts)
        beta_slots = sum(host.beta_slots for host in self.hosts)
        state_counts: dict[str, int] = defaultdict(int)
        pool_slots: dict[str, dict[str, int]] = defaultdict(
            lambda: {"total_slots": 0, "prod_slots": 0, "beta_slots": 0},
        )
        for host in self.hosts:
            state_counts[host.state] += 1
            pool_slots[host.pool]["total_slots"] += host.total_slots
            pool_slots[host.pool]["prod_slots"] += host.prod_slots
            pool_slots[host.pool]["beta_slots"] += host.beta_slots
        return {
            "host_count": len(self.hosts),
            "total_slots": sum(host.total_slots for host in self.hosts),
            "prod_slots": prod_slots,
            "beta_slots": beta_slots,
            "state_counts": dict(sorted(state_counts.items())),
            "pool_slots": dict(sorted(pool_slots.items())),
        }

    def public_hosts(self) -> list[dict[str, Any]]:
        return [host.public_dict(self.prod, self.beta) for host in self.hosts]


@dataclass(frozen=True)
class ObservedWorker:
    index: int
    worker_id: str | None
    host: str | None
    environment: str | None
    api_url: str | None
    image_tag: str | None
    source_commit: str | None
    compose_service: str | None
    k8s_deployment: str | None
    slots: int
    drain_state: str
    running_trials: int

    def is_registered(self) -> bool:
        return self.drain_state not in INACTIVE_WORKER_STATES

    def public_dict(self) -> dict[str, Any]:
        return redact(
            {
                "index": self.index,
                "worker_id": self.worker_id,
                "host": self.host,
                "environment": self.environment,
                "api_url": self.api_url,
                "image_tag": self.image_tag,
                "source_commit": self.source_commit,
                "compose_service": self.compose_service,
                "k8s_deployment": self.k8s_deployment,
                "slots": self.slots,
                "drain_state": self.drain_state,
                "running_trials": self.running_trials,
            },
        )


@dataclass(frozen=True)
class Drift:
    path: str
    desired: Any
    observed: Any

    def public_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "desired": redact(self.desired),
            "observed": redact(self.observed),
        }


def _beta_drain_state(host: HostIntent) -> str:
    if host.beta_slots <= 0:
        return "idle"
    if host.state == "beta_draining":
        return "draining"
    return "leased"


def redact(value: Any) -> Any:
    if isinstance(value, Mapping):
        redacted: dict[str, Any] = {}
        for key, child in value.items():
            key_str = str(key)
            redacted[key_str] = "<redacted>" if _SECRET_KEY_RE.search(key_str) else redact(child)
        return redacted
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact(item) for item in value)
    if isinstance(value, str):
        return _redact_string(value)
    return value


def _redact_string(value: str) -> str:
    redacted = value
    for pattern in _SECRET_VALUE_PATTERNS:
        if pattern.pattern.startswith("://"):
            redacted = pattern.sub("://<redacted>@", redacted)
        else:
            redacted = pattern.sub("<redacted>", redacted)
    return redacted


def _parse_vars(items: list[str]) -> dict[str, str]:
    variables: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise ManifestError("--var entries must be KEY=VALUE")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise ManifestError("--var keys must be shell-style identifiers")
        variables[key] = value
    return variables


def _replace_placeholders(
    value: Any,
    variables: dict[str, str],
    *,
    missing: set[str],
) -> Any:
    if isinstance(value, str):

        def _replacement(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in variables:
                missing.add(name)
                return match.group(0)
            return variables[name]

        return _PLACEHOLDER_RE.sub(_replacement, value)
    if isinstance(value, list):
        return [_replace_placeholders(item, variables, missing=missing) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _replace_placeholders(child, variables, missing=missing)
            for key, child in value.items()
        }
    return value


def load_manifest(
    path: Path,
    *,
    variables: dict[str, str] | None = None,
    require_resolved: bool = False,
) -> CapacityManifest:
    raw, unresolved = _load_manifest_raw(
        path,
        variables=variables,
        require_resolved=require_resolved,
    )
    return _manifest_from_raw(path, raw, unresolved=unresolved)


def _load_manifest_raw(
    path: Path,
    *,
    variables: dict[str, str] | None = None,
    require_resolved: bool = False,
) -> tuple[dict[str, Any], tuple[str, ...]]:
    raw = _load_toml(path)
    unresolved_set: set[str] = set()
    raw = _replace_placeholders(raw, variables or {}, missing=unresolved_set)
    if require_resolved and unresolved_set:
        names = ", ".join(sorted(unresolved_set))
        raise ManifestError(f"{path}: missing --var value(s): {names}")
    errors = _find_secret_bearing_keys(raw)
    if errors:
        raise ManifestError("; ".join(errors))
    return raw, tuple(sorted(unresolved_set))


def _manifest_from_raw(
    path: Path,
    raw: dict[str, Any],
    *,
    unresolved: tuple[str, ...],
) -> CapacityManifest:
    if raw.get("schema_version") != SCHEMA_VERSION:
        raise ManifestError(f"{path}: schema_version must be {SCHEMA_VERSION}")

    defaults = _expect_dict(raw, path, "defaults")
    default_beta_slots = _expect_int(defaults, path, "defaults.default_beta_slots", default=0)
    beta_limit = _expect_int(
        defaults,
        path,
        "defaults.beta_slot_limit_per_host",
        default=1,
    )
    prod_gets_remaining = _expect_bool(
        defaults,
        path,
        "defaults.prod_gets_remaining",
        default=True,
    )
    if default_beta_slots < 0 or beta_limit < 0:
        raise ManifestError(f"{path}: beta slot defaults must be non-negative")
    if default_beta_slots > beta_limit:
        raise ManifestError(f"{path}: default_beta_slots cannot exceed beta_slot_limit_per_host")

    environments = _expect_dict(raw, path, "environments")
    prod = _load_environment(environments, path, "prod")
    beta = _load_environment(environments, path, "beta")
    _validate_environment_pair(path, prod, beta)

    hosts_raw = raw.get("hosts")
    if not isinstance(hosts_raw, list) or not hosts_raw:
        raise ManifestError(f"{path}: hosts must be a non-empty array of tables")
    hosts: list[HostIntent] = []
    seen_hosts: set[str] = set()
    for index, item in enumerate(hosts_raw):
        if not isinstance(item, dict):
            raise ManifestError(f"{path}: hosts[{index}] must be a table")
        host = _load_host(
            path,
            item,
            index=index,
            default_beta_slots=default_beta_slots,
            beta_limit=beta_limit,
            prod_gets_remaining=prod_gets_remaining,
        )
        if host.name in seen_hosts:
            raise ManifestError(f"{path}: duplicate host {host.name!r}")
        seen_hosts.add(host.name)
        hosts.append(host)

    return CapacityManifest(
        path=path,
        prod=prod,
        beta=beta,
        hosts=tuple(hosts),
        unresolved_placeholders=unresolved,
    )


def _load_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    except OSError as exc:
        raise ManifestError(f"could not read {path}: {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ManifestError(f"invalid TOML in {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ManifestError(f"{path}: manifest root must be a table")
    return raw


def _load_environment(
    environments: dict[str, Any],
    path: Path,
    key: str,
) -> EnvironmentTarget:
    raw = environments.get(key)
    if not isinstance(raw, dict):
        raise ManifestError(f"{path}: environments.{key} must be a table")
    return EnvironmentTarget(
        key=key,
        name=_expect_str(raw, path, f"environments.{key}.name"),
        api_url=_expect_str(raw, path, f"environments.{key}.api_url"),
        image_tag=_expect_str(raw, path, f"environments.{key}.image_tag"),
        source_commit=_expect_str(raw, path, f"environments.{key}.source_commit"),
        compose_service=_expect_str(raw, path, f"environments.{key}.compose_service"),
        k8s_deployment=_expect_str(raw, path, f"environments.{key}.k8s_deployment"),
        k8s_namespace=_expect_str(raw, path, f"environments.{key}.k8s_namespace"),
    )


def _validate_environment_pair(
    path: Path, prod: EnvironmentTarget, beta: EnvironmentTarget
) -> None:
    distinct_fields = ("name", "api_url", "compose_service", "k8s_deployment", "k8s_namespace")
    for field in distinct_fields:
        if getattr(prod, field) == getattr(beta, field):
            raise ManifestError(f"{path}: environments.prod.{field} and beta.{field} must differ")
    for target in (prod, beta):
        if not target.api_url.startswith("https://"):
            raise ManifestError(f"{path}: environments.{target.key}.api_url must be https")


def _load_host(
    path: Path,
    raw: dict[str, Any],
    *,
    index: int,
    default_beta_slots: int,
    beta_limit: int,
    prod_gets_remaining: bool,
) -> HostIntent:
    prefix = f"hosts[{index}]"
    name = _expect_str(raw, path, f"{prefix}.name")
    pool = _expect_str(raw, path, f"{prefix}.pool")
    total_slots = _expect_int(raw, path, f"{prefix}.total_slots")
    state = _expect_str(raw, path, f"{prefix}.state")
    reason = _optional_str(raw, path, f"{prefix}.reason")
    if state not in HOST_STATES:
        raise ManifestError(f"{path}: {prefix}.state must be one of {sorted(HOST_STATES)}")
    if total_slots < 0:
        raise ManifestError(f"{path}: {prefix}.total_slots must be non-negative")

    if state in {"host_draining", "unreachable"}:
        prod_slots = _expect_int(raw, path, f"{prefix}.prod_slots", default=0)
        beta_slots = _expect_int(raw, path, f"{prefix}.beta_slots", default=0)
        if prod_slots != 0 or beta_slots != 0:
            raise ManifestError(f"{path}: {prefix} {state} hosts must have zero slots")
        return HostIntent(name, pool, total_slots, state, prod_slots, beta_slots, reason)

    beta_slots = _expect_int(raw, path, f"{prefix}.beta_slots", default=default_beta_slots)
    if beta_slots < 0:
        raise ManifestError(f"{path}: {prefix}.beta_slots must be non-negative")
    if beta_slots > beta_limit:
        raise ManifestError(f"{path}: {prefix}.beta_slots cannot exceed {beta_limit}")

    if "prod_slots" in raw:
        prod_slots = _expect_int(raw, path, f"{prefix}.prod_slots")
    elif prod_gets_remaining:
        prod_slots = total_slots - beta_slots
    else:
        prod_slots = 0
    if prod_slots < 0:
        raise ManifestError(f"{path}: {prefix}.prod_slots must be non-negative")
    if prod_slots + beta_slots > total_slots:
        raise ManifestError(f"{path}: {prefix} prod_slots + beta_slots exceeds total_slots")
    return HostIntent(name, pool, total_slots, state, prod_slots, beta_slots, reason)


def _find_secret_bearing_keys(value: Any, path: str = "") -> list[str]:
    if isinstance(value, dict):
        errors: list[str] = []
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            if _SECRET_KEY_RE.search(str(key)):
                errors.append(f"{child_path} is not allowed in capacity manifests")
                continue
            errors.extend(_find_secret_bearing_keys(child, child_path))
        return errors
    if isinstance(value, list):
        errors = []
        for index, child in enumerate(value):
            errors.extend(_find_secret_bearing_keys(child, f"{path}[{index}]"))
        return errors
    return []


def load_observed_workers(path: Path) -> list[ObservedWorker]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ManifestError(f"could not read observed worker artifact {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ManifestError(f"invalid JSON in observed worker artifact {path}: {exc}") from exc
    records = _extract_worker_records(raw)
    workers: list[ObservedWorker] = []
    for index, item in enumerate(records):
        if not isinstance(item, dict):
            raise ManifestError(f"{path}: observed workers[{index}] must be an object")
        workers.append(
            ObservedWorker(
                index=index,
                worker_id=_first_str(item, ("worker_id", "id", "name")),
                host=_first_str(item, ("host", "hostname", "node", "node_name")),
                environment=_first_str(
                    item,
                    ("environment", "env", "control_plane_environment"),
                ),
                api_url=_first_str(
                    item,
                    ("api_url", "worker_api_url", "control_plane_url", "cp_url"),
                ),
                image_tag=_first_str(
                    item,
                    ("image_tag", "current_image_tag", "container_image_tag"),
                ),
                source_commit=_first_str(
                    item,
                    ("source_commit", "source_git_commit", "git_commit"),
                ),
                compose_service=_first_str(item, ("compose_service", "service_name")),
                k8s_deployment=_first_str(item, ("k8s_deployment", "deployment")),
                slots=_first_int(item, ("slots", "capacity_slots", "max_concurrent"), default=1),
                drain_state=(
                    _first_str(item, ("drain_state", "worker_status", "state")) or "active"
                ).lower(),
                running_trials=_first_int(
                    item,
                    (
                        "running_trials",
                        "running_beta_trials",
                        "active_trials",
                        "claimed_trials",
                    ),
                    default=0,
                ),
            ),
        )
    return workers


def _extract_worker_records(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if not isinstance(raw, dict):
        raise ManifestError("observed worker artifact must be an object or array")
    for key in (
        "workers",
        "registrations",
        "worker_registrations",
        "worker_status",
        "nodes",
    ):
        value = raw.get(key)
        if isinstance(value, list):
            return value
    raise ManifestError(
        "observed worker artifact must contain workers, registrations, "
        "worker_registrations, worker_status, or nodes",
    )


def diff_observed_workers(
    manifest: CapacityManifest,
    workers: list[ObservedWorker],
) -> list[Drift]:
    drift: list[Drift] = []
    hosts = {host.name: host for host in manifest.hosts}
    env_targets = {manifest.prod.name: manifest.prod, manifest.beta.name: manifest.beta}
    worker_id_bindings: dict[str, tuple[str | None, str | None]] = {}
    registered_slots: dict[tuple[str, str], int] = defaultdict(int)

    for worker in workers:
        path = f"workers[{worker.index}]"
        if not worker.worker_id:
            drift.append(Drift(f"{path}.worker_id", "non-empty worker identity", None))
        elif worker.worker_id in worker_id_bindings:
            previous_env, previous_api = worker_id_bindings[worker.worker_id]
            if (previous_env, previous_api) != (worker.environment, worker.api_url):
                drift.append(
                    Drift(
                        f"{path}.worker_id",
                        {"environment": previous_env, "api_url": previous_api},
                        {
                            "worker_id": worker.worker_id,
                            "environment": worker.environment,
                            "api_url": worker.api_url,
                        },
                    ),
                )
        else:
            worker_id_bindings[worker.worker_id] = (worker.environment, worker.api_url)

        if not worker.host or worker.host not in hosts:
            drift.append(Drift(f"{path}.host", "known manifest host", worker.host))
            continue
        host = hosts[worker.host]

        target = env_targets.get(worker.environment or "")
        if target is None:
            drift.append(
                Drift(
                    f"{path}.environment",
                    sorted(env_targets),
                    worker.environment,
                ),
            )
            continue

        for field in (
            "api_url",
            "image_tag",
            "source_commit",
            "compose_service",
            "k8s_deployment",
        ):
            observed_value = getattr(worker, field)
            if observed_value is not None and observed_value != getattr(target, field):
                drift.append(
                    Drift(
                        f"{path}.{field}",
                        getattr(target, field),
                        observed_value,
                    ),
                )

        if host.state in {"host_draining", "unreachable"} and worker.is_registered():
            drift.append(
                Drift(
                    f"{path}.drain_state",
                    f"no registered workers on {host.state} host",
                    worker.drain_state,
                ),
            )
        if (
            host.state == "beta_draining"
            and target == manifest.beta
            and worker.drain_state not in BETA_DRAINING_STATES
        ):
            drift.append(Drift(f"{path}.drain_state", "draining beta worker", worker.drain_state))

        if worker.is_registered():
            registered_slots[(host.name, target.name)] += worker.slots

    for host in manifest.hosts:
        expected = {
            manifest.prod.name: host.prod_slots,
            manifest.beta.name: host.beta_slots,
        }
        for environment, desired_slots in expected.items():
            observed_slots = registered_slots[(host.name, environment)]
            if observed_slots != desired_slots:
                drift.append(
                    Drift(
                        f"hosts[{host.name}].{environment}.slots",
                        desired_slots,
                        observed_slots,
                    ),
                )
    return drift


def build_report(
    manifest: CapacityManifest,
    *,
    workers: list[ObservedWorker] | None = None,
    drift: list[Drift] | None = None,
    errors: list[str] | None = None,
) -> dict[str, Any]:
    public_drift = [item.public_dict() for item in drift or []]
    public_errors = [str(redact(error)) for error in errors or []]
    status = "pass" if not public_drift and not public_errors else "fail"
    report: dict[str, Any] = {
        "artifact_type": "worker-capacity-desired-state",
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "manifest": str(manifest.path),
        "unresolved_placeholders": list(manifest.unresolved_placeholders),
        "summary": manifest.summary(),
        "environments": {
            "prod": manifest.prod.public_dict(),
            "beta": manifest.beta.public_dict(),
        },
        "desired_hosts": manifest.public_hosts(),
        "drift": public_drift,
        "errors": public_errors,
    }
    if workers is not None:
        report["observed"] = {
            "worker_count": len(workers),
            "workers": [worker.public_dict() for worker in workers],
        }
    return redact(report)


def format_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Worker Capacity Desired State",
        "",
        f"- Status: `{report['status']}`",
    ]
    operation = report.get("operation")
    if operation:
        lines.append(f"- Operation: `{operation}`")
    lease = report.get("lease")
    if isinstance(lease, dict):
        lines.append(f"- Beta lease: `{lease.get('state', 'none')}`")
    summary = report.get("summary")
    if isinstance(summary, dict):
        lines.extend(
            [
                f"- Hosts: `{summary['host_count']}`",
                f"- Total slots: `{summary['total_slots']}`",
                f"- Prod slots: `{summary['prod_slots']}`",
                f"- Beta slots: `{summary['beta_slots']}`",
            ],
        )
    if report.get("drift"):
        lines.extend(["", "## Drift"])
        for item in report["drift"]:
            lines.append(
                f"- `{item['path']}` desired `{item['desired']}` observed `{item['observed']}`",
            )
    if report.get("errors"):
        lines.extend(["", "## Errors"])
        for error in report["errors"]:
            lines.append(f"- {error}")
    return "\n".join(lines) + "\n"


def _parse_now(value: str | None) -> datetime:
    if value is None:
        return datetime.now(UTC).replace(microsecond=0)
    normalized = value.strip()
    if not normalized:
        raise ManifestError("--now must be a non-empty ISO-8601 timestamp")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ManifestError(f"--now must be an ISO-8601 timestamp, got {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(microsecond=0)


def _format_time(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return _parse_now(value)
    except ManifestError:
        return None


def _parse_ttl_seconds(value: str | None) -> int:
    if value is None:
        raise ManifestError("lease-beta requires --ttl; unbounded beta leases are refused")
    match = re.fullmatch(r"\s*(\d+)\s*([smhd])\s*", value)
    if not match:
        raise ManifestError("--ttl must be a bounded duration like 30m, 2h, or 1d")
    amount = int(match.group(1))
    if amount <= 0:
        raise ManifestError("--ttl must be greater than zero")
    unit = match.group(2)
    multiplier = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
    return amount * multiplier


def _safe_reason(value: str | None, *, fallback: str) -> str:
    reason = (value or fallback).strip()
    if not reason:
        reason = fallback
    return _redact_string(reason)


def _lease_table(raw: dict[str, Any]) -> dict[str, Any]:
    lease = raw.get("beta_capacity_lease")
    if lease is None:
        return {}
    if not isinstance(lease, dict):
        raise ManifestError("beta_capacity_lease must be a table")
    return lease


def _effective_lease_state(raw: dict[str, Any], *, now: datetime) -> str:
    lease = _lease_table(raw)
    state = str(lease.get("state") or "none").strip() or "none"
    expires_at = _parse_timestamp(lease.get("expires_at"))
    if state == "active" and expires_at is not None and now >= expires_at:
        return "expired"
    return state


def _public_lease(raw: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    lease = dict(_lease_table(raw))
    if not lease:
        return {"state": "none"}
    lease["state"] = _effective_lease_state(raw, now=now)
    return redact(lease)


def _host_rows(raw: dict[str, Any]) -> list[dict[str, Any]]:
    hosts = raw.get("hosts")
    if not isinstance(hosts, list):
        raise ManifestError("hosts must be an array of tables")
    rows: list[dict[str, Any]] = []
    for index, item in enumerate(hosts):
        if not isinstance(item, dict):
            raise ManifestError(f"hosts[{index}] must be a table")
        rows.append(item)
    return rows


def _set_host_capacity(
    row: dict[str, Any],
    *,
    state: str,
    total_slots: int,
    beta_slots: int,
) -> None:
    row["state"] = state
    row["beta_slots"] = beta_slots
    row["prod_slots"] = 0 if state in {"host_draining", "unreachable"} else total_slots - beta_slots


def _host_signatures(manifest: CapacityManifest) -> dict[str, tuple[str, int, int]]:
    return {host.name: (host.state, host.prod_slots, host.beta_slots) for host in manifest.hosts}


def _changed_host_count(
    before: CapacityManifest,
    after: CapacityManifest,
) -> int:
    before_hosts = _host_signatures(before)
    return sum(
        1
        for name, signature in _host_signatures(after).items()
        if before_hosts.get(name) != signature
    )


def _running_beta_trials_by_host(
    manifest: CapacityManifest,
    workers: list[ObservedWorker] | None,
) -> dict[str, int]:
    running: dict[str, int] = defaultdict(int)
    if workers is None:
        return running
    for worker in workers:
        if worker.host and worker.environment == manifest.beta.name and worker.is_registered():
            running[worker.host] += worker.running_trials
    return running


def _drain_beta_capacity_raw(
    raw: dict[str, Any],
    *,
    path: Path,
    unresolved: tuple[str, ...],
    workers: list[ObservedWorker] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    before = _manifest_from_raw(path, raw, unresolved=unresolved)
    running = _running_beta_trials_by_host(before, workers)
    next_raw = copy.deepcopy(raw)
    rows = {str(row.get("name")): row for row in _host_rows(next_raw)}
    running_total = sum(running.values())
    idle_leased_slots = 0
    draining_hosts: list[str] = []
    released_idle_hosts: list[str] = []
    for host in before.hosts:
        if host.beta_slots <= 0:
            continue
        row = rows[host.name]
        host_running = running.get(host.name, 0)
        if host_running > 0:
            retained_slots = max(1, min(host.beta_slots, host_running))
            _set_host_capacity(
                row,
                state="beta_draining",
                total_slots=host.total_slots,
                beta_slots=retained_slots,
            )
            draining_hosts.append(host.name)
            idle_leased_slots += max(0, host.beta_slots - retained_slots)
        else:
            _set_host_capacity(
                row,
                state="eligible",
                total_slots=host.total_slots,
                beta_slots=0,
            )
            released_idle_hosts.append(host.name)
            idle_leased_slots += host.beta_slots
    return next_raw, {
        "running_beta_trials": running_total,
        "idle_leased_slots": idle_leased_slots,
        "draining_hosts": draining_hosts,
        "released_idle_hosts": released_idle_hosts,
    }


def _lease_beta_capacity_raw(
    raw: dict[str, Any],
    *,
    path: Path,
    unresolved: tuple[str, ...],
    now: datetime,
    reason: str,
    ttl_seconds: int,
    slots_per_host: int,
    max_total_slots: int,
    preemptible: bool,
) -> dict[str, Any]:
    before = _manifest_from_raw(path, raw, unresolved=unresolved)
    if any(host.beta_slots > 0 for host in before.hosts):
        raise ManifestError(
            "lease-beta refused: beta slots are already leased; drain or release first"
        )
    next_raw = copy.deepcopy(raw)
    rows = {str(row.get("name")): row for row in _host_rows(next_raw)}
    remaining = max_total_slots
    leased_hosts: list[str] = []
    for host in before.hosts:
        if remaining < slots_per_host:
            break
        if host.state != "eligible" or host.total_slots < slots_per_host:
            continue
        row = rows[host.name]
        _set_host_capacity(
            row,
            state="eligible",
            total_slots=host.total_slots,
            beta_slots=slots_per_host,
        )
        leased_hosts.append(host.name)
        remaining -= slots_per_host
    if not leased_hosts:
        raise ManifestError("lease-beta found no eligible hosts for the requested beta lease")
    next_raw["beta_capacity_lease"] = {
        "state": "active",
        "reason": _safe_reason(reason, fallback="beta capacity lease"),
        "created_at": _format_time(now),
        "expires_at": _format_time(now + timedelta(seconds=ttl_seconds)),
        "ttl_seconds": ttl_seconds,
        "slots_per_host": slots_per_host,
        "max_total_slots": max_total_slots,
        "preemptible": preemptible,
        "leased_hosts": leased_hosts,
    }
    return next_raw


def _release_beta_capacity_raw(
    raw: dict[str, Any],
    *,
    path: Path,
    unresolved: tuple[str, ...],
    now: datetime,
    reason: str,
) -> dict[str, Any]:
    before = _manifest_from_raw(path, raw, unresolved=unresolved)
    already_released = _lease_table(raw).get("state") == "released" and all(
        host.beta_slots == 0 for host in before.hosts
    )
    if already_released:
        return copy.deepcopy(raw)

    next_raw = copy.deepcopy(raw)
    rows = {str(row.get("name")): row for row in _host_rows(next_raw)}
    for host in before.hosts:
        row = rows[host.name]
        if host.state in {"host_draining", "unreachable"}:
            _set_host_capacity(
                row,
                state=host.state,
                total_slots=host.total_slots,
                beta_slots=0,
            )
            continue
        _set_host_capacity(
            row,
            state="eligible",
            total_slots=host.total_slots,
            beta_slots=0,
        )
    next_raw["beta_capacity_lease"] = {
        **_lease_table(raw),
        "state": "released",
        "release_reason": _safe_reason(reason, fallback="beta capacity released"),
        "released_at": _format_time(now),
        "leased_hosts": [],
    }
    return next_raw


def _mark_beta_draining_raw(
    raw: dict[str, Any],
    *,
    now: datetime,
    reason: str,
    expired: bool,
) -> dict[str, Any]:
    next_raw = copy.deepcopy(raw)
    lease = {**_lease_table(next_raw)}
    lease["state"] = "expired" if expired else "draining"
    lease["stopped_new_claims"] = True
    if expired:
        lease.setdefault("expired_at", lease.get("expires_at") or _format_time(now))
        lease["expiry_reason"] = _safe_reason(reason, fallback="beta capacity lease expired")
    else:
        lease["drain_reason"] = _safe_reason(reason, fallback="beta capacity drain")
        lease["drained_at"] = _format_time(now)
    next_raw["beta_capacity_lease"] = lease
    return next_raw


def _write_capacity_manifest(path: Path, raw: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(tomli_w.dumps(raw), encoding="utf-8")


def _new_beta_claims_allowed(
    manifest: CapacityManifest,
    raw: dict[str, Any],
    *,
    now: datetime,
) -> bool:
    if _effective_lease_state(raw, now=now) != "active":
        return False
    if manifest.summary()["beta_slots"] <= 0:
        return False
    return all(host.state != "beta_draining" for host in manifest.hosts if host.beta_slots > 0)


def _build_lifecycle_report(
    *,
    path: Path,
    raw: dict[str, Any],
    unresolved: tuple[str, ...],
    operation: str,
    applied: bool,
    before: CapacityManifest,
    workers: list[ObservedWorker] | None,
    now: datetime,
    argv: list[str],
    drain: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = _manifest_from_raw(path, raw, unresolved=unresolved)
    report = build_report(manifest, workers=workers, drift=[])
    report["operation"] = operation
    report["applied"] = applied
    report["new_beta_claims_allowed"] = _new_beta_claims_allowed(manifest, raw, now=now)
    report["lease"] = _public_lease(raw, now=now)
    report["changes"] = {
        "changed_host_count": _changed_host_count(before, manifest),
    }
    report["drain"] = drain or {
        "running_beta_trials": sum(_running_beta_trials_by_host(manifest, workers).values()),
        "idle_leased_slots": 0,
        "draining_hosts": [],
        "released_idle_hosts": [],
    }
    report["command"] = {"argv": redact(argv)}
    return redact(report)


def _expect_dict(raw: dict[str, Any], path: Path, key: str) -> dict[str, Any]:
    value = raw.get(key)
    if not isinstance(value, dict):
        raise ManifestError(f"{path}: {key} must be a table")
    return value


def _expect_str(raw: dict[str, Any], path: Path, key: str) -> str:
    leaf = key.rsplit(".", 1)[-1]
    value = raw.get(leaf)
    if not isinstance(value, str) or not value.strip():
        raise ManifestError(f"{path}: {key} must be a non-empty string")
    return value.strip()


def _optional_str(raw: dict[str, Any], path: Path, key: str) -> str | None:
    leaf = key.rsplit(".", 1)[-1]
    value = raw.get(leaf)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ManifestError(f"{path}: {key} must be a non-empty string when set")
    return value.strip()


def _expect_int(
    raw: dict[str, Any],
    path: Path,
    key: str,
    *,
    default: int | None = None,
) -> int:
    leaf = key.rsplit(".", 1)[-1]
    if leaf not in raw:
        if default is None:
            raise ManifestError(f"{path}: {key} must be an integer")
        return default
    value = raw[leaf]
    if not isinstance(value, int) or isinstance(value, bool):
        raise ManifestError(f"{path}: {key} must be an integer")
    return value


def _expect_bool(raw: dict[str, Any], path: Path, key: str, *, default: bool) -> bool:
    leaf = key.rsplit(".", 1)[-1]
    if leaf not in raw:
        return default
    value = raw[leaf]
    if not isinstance(value, bool):
        raise ManifestError(f"{path}: {key} must be a boolean")
    return value


def _first_str(raw: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = raw.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _first_int(raw: dict[str, Any], keys: tuple[str, ...], *, default: int) -> int:
    for key in keys:
        value = raw.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            return max(0, value)
    return default


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    explicit_command = bool(raw_argv and raw_argv[0] in COMMANDS)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        nargs="?",
        choices=sorted(COMMANDS),
        default="status",
        help=(
            "Lifecycle operation. Omit for the legacy desired-vs-observed "
            "validator; use explicit status for beta lease lifecycle status."
        ),
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("deploy/worker-capacity/prod-first.toml"),
        help="prod-first capacity manifest TOML",
    )
    parser.add_argument(
        "--observed-json",
        type=Path,
        help="optional observed worker registration/status JSON artifact",
    )
    parser.add_argument(
        "--var",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="placeholder value for manifest fields such as PROD_IMAGE_TAG",
    )
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="json",
        help="stdout format",
    )
    parser.add_argument("--evidence-out", type=Path, help="write sanitized JSON evidence")
    parser.add_argument(
        "--output-manifest",
        type=Path,
        default=None,
        help="manifest path to write when --apply is set; defaults to --manifest",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write the resulting desired-state manifest; without this, only preview",
    )
    parser.add_argument(
        "--reason",
        default=None,
        help="operator reason recorded in sanitized lease/drain/release evidence",
    )
    parser.add_argument(
        "--ttl",
        default=None,
        help="bounded beta lease TTL such as 30m, 2h, or 1d",
    )
    parser.add_argument(
        "--slots-per-host",
        type=int,
        default=1,
        help="beta slots per leased host; first version supports 1",
    )
    parser.add_argument(
        "--max-total-slots",
        type=int,
        default=None,
        help="maximum total beta slots to lease",
    )
    parser.set_defaults(preemptible=None)
    preemptible = parser.add_mutually_exclusive_group()
    preemptible.add_argument(
        "--preemptible",
        dest="preemptible",
        action="store_true",
        help="mark beta capacity as preemptible",
    )
    preemptible.add_argument(
        "--non-preemptible",
        dest="preemptible",
        action="store_false",
        help="request non-preemptible beta capacity; requires explicit override",
    )
    parser.add_argument(
        "--allow-non-preemptible",
        action="store_true",
        help="allow --non-preemptible beta capacity for an explicit operator exception",
    )
    parser.add_argument(
        "--now",
        default=None,
        help="UTC ISO-8601 time for deterministic lease previews/tests",
    )
    args = parser.parse_args(raw_argv)

    if args.command == "lease-beta":
        if args.ttl is None:
            parser.error("lease-beta requires --ttl; unbounded beta leases are refused")
        if args.reason is None or not args.reason.strip():
            parser.error("lease-beta requires --reason")
        if args.max_total_slots is None:
            parser.error("lease-beta requires --max-total-slots")
        if args.max_total_slots <= 0:
            parser.error("--max-total-slots must be greater than zero")
        if args.slots_per_host != 1:
            parser.error("--slots-per-host must be 1 in the first beta lease version")
        if args.preemptible is None:
            parser.error("lease-beta requires --preemptible or --non-preemptible")
        if args.preemptible is False and not args.allow_non_preemptible:
            parser.error("--non-preemptible beta capacity requires --allow-non-preemptible")

    try:
        variables = _parse_vars(args.var)
        now = _parse_now(args.now)
        raw, unresolved = _load_manifest_raw(
            args.manifest,
            variables=variables,
            require_resolved=args.observed_json is not None,
        )
        workers = load_observed_workers(args.observed_json) if args.observed_json else None
        before = _manifest_from_raw(args.manifest, raw, unresolved=unresolved)

        if not explicit_command:
            drift = diff_observed_workers(before, workers) if workers is not None else []
            report = build_report(before, workers=workers, drift=drift)
        elif args.command == "status":
            effective_raw = raw
            drain: dict[str, Any] | None = None
            if _effective_lease_state(raw, now=now) == "expired":
                drained_raw, drain = _drain_beta_capacity_raw(
                    raw,
                    path=args.manifest,
                    unresolved=unresolved,
                    workers=workers,
                )
                effective_raw = _mark_beta_draining_raw(
                    drained_raw,
                    now=now,
                    reason="beta capacity lease expired",
                    expired=True,
                )
            if args.apply:
                _write_capacity_manifest(args.output_manifest or args.manifest, effective_raw)
            report = _build_lifecycle_report(
                path=args.output_manifest or args.manifest,
                raw=effective_raw,
                unresolved=unresolved,
                operation=args.command,
                applied=bool(args.apply),
                before=before,
                workers=workers,
                now=now,
                argv=raw_argv,
                drain=drain,
            )
        elif args.command == "lease-beta":
            ttl_seconds = _parse_ttl_seconds(args.ttl)
            next_raw = _lease_beta_capacity_raw(
                raw,
                path=args.manifest,
                unresolved=unresolved,
                now=now,
                reason=str(args.reason),
                ttl_seconds=ttl_seconds,
                slots_per_host=args.slots_per_host,
                max_total_slots=int(args.max_total_slots),
                preemptible=bool(args.preemptible),
            )
            if args.apply:
                _write_capacity_manifest(args.output_manifest or args.manifest, next_raw)
            report = _build_lifecycle_report(
                path=args.output_manifest or args.manifest,
                raw=next_raw,
                unresolved=unresolved,
                operation=args.command,
                applied=bool(args.apply),
                before=before,
                workers=workers,
                now=now,
                argv=raw_argv,
            )
        elif args.command == "release-beta":
            next_raw = _release_beta_capacity_raw(
                raw,
                path=args.manifest,
                unresolved=unresolved,
                now=now,
                reason=_safe_reason(args.reason, fallback="beta capacity released"),
            )
            if args.apply:
                _write_capacity_manifest(args.output_manifest or args.manifest, next_raw)
            report = _build_lifecycle_report(
                path=args.output_manifest or args.manifest,
                raw=next_raw,
                unresolved=unresolved,
                operation=args.command,
                applied=bool(args.apply),
                before=before,
                workers=workers,
                now=now,
                argv=raw_argv,
            )
        elif args.command == "drain-beta":
            drained_raw, drain = _drain_beta_capacity_raw(
                raw,
                path=args.manifest,
                unresolved=unresolved,
                workers=workers,
            )
            next_raw = _mark_beta_draining_raw(
                drained_raw,
                now=now,
                reason=_safe_reason(args.reason, fallback="beta capacity drain"),
                expired=False,
            )
            if args.apply:
                _write_capacity_manifest(args.output_manifest or args.manifest, next_raw)
            report = _build_lifecycle_report(
                path=args.output_manifest or args.manifest,
                raw=next_raw,
                unresolved=unresolved,
                operation=args.command,
                applied=bool(args.apply),
                before=before,
                workers=workers,
                now=now,
                argv=raw_argv,
                drain=drain,
            )
        else:
            raise ManifestError(f"unsupported command: {args.command}")
    except ManifestError as exc:
        report = {
            "artifact_type": "worker-capacity-desired-state",
            "schema_version": SCHEMA_VERSION,
            "status": "fail",
            "errors": [str(redact(str(exc)))],
        }

    if args.evidence_out:
        args.evidence_out.parent.mkdir(parents=True, exist_ok=True)
        args.evidence_out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")

    if args.format == "json":
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(format_markdown(report), end="")
    return 0 if report.get("status") == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
