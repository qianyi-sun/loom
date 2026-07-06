#!/usr/bin/env python3
"""Validate prod-first shared worker capacity desired-state evidence.

The manifest is a release contract, not a live mutator. It describes how shared
physical GB10/OLDLAB hosts are assigned between production and beta/dev, then
optionally compares that desired state with a secret-free observed worker
registration artifact.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
HOST_STATES = frozenset({"eligible", "beta_draining", "host_draining", "unreachable"})
INACTIVE_WORKER_STATES = frozenset({"drained", "stopped", "offline", "unreachable"})
BETA_DRAINING_STATES = frozenset({"draining", "drained", "stopped", "offline"})

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
    raw = _load_toml(path)
    unresolved: set[str] = set()
    raw = _replace_placeholders(raw, variables or {}, missing=unresolved)
    if require_resolved and unresolved:
        names = ", ".join(sorted(unresolved))
        raise ManifestError(f"{path}: missing --var value(s): {names}")
    errors = _find_secret_bearing_keys(raw)
    if errors:
        raise ManifestError("; ".join(errors))
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
        unresolved_placeholders=tuple(sorted(unresolved)),
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


def _validate_environment_pair(path: Path, prod: EnvironmentTarget, beta: EnvironmentTarget) -> None:
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
    parser = argparse.ArgumentParser(description=__doc__)
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
    args = parser.parse_args(argv)

    try:
        variables = _parse_vars(args.var)
        manifest = load_manifest(
            args.manifest,
            variables=variables,
            require_resolved=args.observed_json is not None,
        )
        workers = load_observed_workers(args.observed_json) if args.observed_json else None
        drift = diff_observed_workers(manifest, workers) if workers is not None else []
        report = build_report(manifest, workers=workers, drift=drift)
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
