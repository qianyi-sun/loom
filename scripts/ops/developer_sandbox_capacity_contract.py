"""Closed-world readers for developer-sandbox capacity policy contracts.

The platform-health producer, live verifier, runtime-host activation gate, and
repository promotion tool must all interpret the same checked-in bytes.  This
module deliberately exposes values and source digests only after validating
the complete TOML shape and the canonical node inventory.
"""

from __future__ import annotations

import hashlib
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, cast

SCHEMA_VERSION: Final = 1
POOLS: Final = ("oldlab", "gb10")
CAPACITY_POLICY_SOURCES: Final = {
    pool: f"deploy/developer-sandboxes/shared-capacity-policies/{pool}.toml" for pool in POOLS
}
PLATFORM_HEALTH_CONFIG_SOURCE: Final = "deploy/developer-sandboxes/platform-health-authority.toml"

_POLICY_TOP_FIELDS: Final = {
    "schema_version",
    "pool_name",
    "slot_budget",
    "pending_slot_budget",
    "job_pids_max",
    "policy",
}
_POLICY_FIELDS: Final = {
    "actuator",
    "enabled",
    "min_slots",
    "max_slots",
    "scale_up_threshold_slots",
    "scale_down_idle_seconds",
    "scale_up_cooldown_seconds",
    "scale_down_cooldown_seconds",
    "drain_timeout_seconds",
    "force",
    "actuator_config",
}
_ACTUATOR_FIELDS: Final = {
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
}
_PLATFORM_HEALTH_FIELDS: Final = {
    "schema_version",
    "collector_host",
    "namespace",
    "longhorn_namespace",
    "kubeconfig",
    "acceptance_state_root",
    "authority_state_root",
    "node_transport",
    "minio_statefulset",
    "minio_pdb",
    "max_checkpoint_seconds",
    "max_clock_skew_seconds",
    "minimum_oldlab_free_cpu_cores",
    "minimum_oldlab_free_memory_bytes",
    "maximum_cpu_busy_ratio",
    "capacity_policy_sources",
    "oldlab_nodes",
    "gb10_nodes",
    "host_aliases",
}


class CapacityContractError(ValueError):
    """Raised when checked-in capacity authority bytes are not exact."""


@dataclass(frozen=True)
class CapacityPolicyContract:
    pool: str
    source: str
    source_sha256: str
    values: Mapping[str, Any]


@dataclass(frozen=True)
class PlatformHealthContract:
    source: str
    source_sha256: str
    minimum_oldlab_free_cpu_cores: int
    minimum_oldlab_free_memory_bytes: int
    maximum_cpu_busy_ratio: float
    oldlab_nodes: tuple[str, ...]
    gb10_nodes: tuple[str, ...]
    host_aliases: Mapping[str, str]

    @property
    def capacity_gb10_nodes(self) -> tuple[str, ...]:
        """Return the production-capacity GB10 inventory."""

        return self.gb10_nodes


def _read_toml(path: Path, *, label: str) -> tuple[bytes, dict[str, Any]]:
    try:
        raw = path.read_bytes()
        payload = tomllib.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise CapacityContractError(f"{label} is invalid") from exc
    if not isinstance(payload, dict):
        raise CapacityContractError(f"{label} is invalid")
    return raw, payload


def _positive_int(value: object, *, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise CapacityContractError(f"{label} is invalid")
    return value


def _nonnegative_int(value: object, *, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise CapacityContractError(f"{label} is invalid")
    return value


def load_capacity_policy(
    repo_root: Path,
    pool: str,
    *,
    expected_nodes: Sequence[str],
) -> CapacityPolicyContract:
    """Load one exact shared-capacity policy from an exact candidate tree."""

    if pool not in POOLS:
        raise CapacityContractError("shared-capacity policy pool is invalid")
    source = CAPACITY_POLICY_SOURCES[pool]
    raw, payload = _read_toml(repo_root / source, label=f"{pool} capacity policy")
    policy = payload.get("policy")
    actuator = policy.get("actuator_config") if isinstance(policy, dict) else None
    allowed_nodes = actuator.get("allowed_nodes") if isinstance(actuator, dict) else None
    expected = tuple(node.lower() for node in expected_nodes)
    if (
        set(payload) != _POLICY_TOP_FIELDS
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("pool_name") != pool
        or not isinstance(policy, dict)
        or set(policy) != _POLICY_FIELDS
        or policy.get("actuator") != "slurm"
        or policy.get("enabled") is not True
        or policy.get("min_slots") != 0
        or policy.get("force") is not False
        or not isinstance(actuator, dict)
        or set(actuator) != _ACTUATOR_FIELDS
        or actuator.get("backend") != "docker"
        or actuator.get("cpu_arch") != ("x86_64" if pool == "oldlab" else "arm64")
        or actuator.get("partition") != ("" if pool == "oldlab" else "gb10")
        or not isinstance(allowed_nodes, list)
        or not all(isinstance(node, str) for node in allowed_nodes)
        or tuple(node.lower() for node in allowed_nodes) != expected
        or len(set(expected)) != len(expected)
        or actuator.get("exclusive") is not False
        or actuator.get("external_runner") is not True
        or actuator.get("shared_capacity_managed") is not True
        or actuator.get("candidate_sha") != "${CANDIDATE_SHA}"
        or actuator.get("slurm_account") != "loom-dev-${SANDBOX}"
        or actuator.get("qos_normal") != "loom-dev"
        or actuator.get("env_file")
        != f"/shared_work/loom/runtime/sandboxes/${{SANDBOX}}/${{CANDIDATE_SHA}}/worker-{pool}.env"
        or actuator.get("repo_dir")
        != "/shared_work/loom/candidates/sandboxes/${SANDBOX}/${CANDIDATE_SHA}"
        or actuator.get("time_limit") != "02:00:00"
        or not isinstance(actuator.get("gpu_tres"), str)
        or actuator.get("gpu_tres") != ("" if pool == "oldlab" else "gpu:1")
    ):
        raise CapacityContractError(f"{pool} capacity policy binding is invalid")

    slot_budget = _positive_int(payload["slot_budget"], label=f"{pool} slot budget")
    pending_slot_budget = _positive_int(
        payload["pending_slot_budget"],
        label=f"{pool} pending slot budget",
    )
    job_pids_max = _positive_int(payload["job_pids_max"], label=f"{pool} job pids")
    values: dict[str, Any] = {
        "max_slots": _positive_int(policy["max_slots"], label=f"{pool} max slots"),
        "requested_cpus": _positive_int(
            actuator["requested_cpus"],
            label=f"{pool} requested CPUs",
        ),
        "requested_memory_mib": _positive_int(
            actuator["requested_memory_mib"],
            label=f"{pool} requested memory",
        ),
        "requested_concurrency": _positive_int(
            actuator["requested_concurrency"],
            label=f"{pool} requested concurrency",
        ),
        "max_jobs": _positive_int(actuator["max_jobs"], label=f"{pool} max jobs"),
        "pending_job_cap": _positive_int(
            actuator["pending_job_cap"],
            label=f"{pool} pending job cap",
        ),
        "container_cpus": _positive_int(
            actuator["container_cpus"],
            label=f"{pool} container CPUs",
        ),
        "container_memory_mib": _positive_int(
            actuator["container_memory_mib"],
            label=f"{pool} container memory",
        ),
        "container_pids": _positive_int(
            actuator["container_pids"],
            label=f"{pool} container pids",
        ),
        "job_pids_max": _positive_int(
            actuator["job_pids_max"],
            label=f"{pool} job pids",
        ),
        "exclusive": False,
        "external_runner": True,
        "shared_capacity_managed": True,
        "gpu_tres": actuator["gpu_tres"],
    }
    scale_integers = (
        "scale_up_threshold_slots",
        "scale_down_idle_seconds",
        "scale_up_cooldown_seconds",
        "scale_down_cooldown_seconds",
        "drain_timeout_seconds",
    )
    if (
        values["max_slots"] != slot_budget
        or policy["max_slots"] != slot_budget
        or values["pending_job_cap"] > pending_slot_budget
        or values["max_jobs"] > len(expected)
        or values["requested_concurrency"] * values["max_jobs"] > values["max_slots"]
        or job_pids_max != values["job_pids_max"]
        or any(
            _nonnegative_int(policy[field], label=f"{pool} {field}") < 0 for field in scale_integers
        )
        or values["requested_cpus"] < values["requested_concurrency"] * values["container_cpus"]
        or values["requested_memory_mib"]
        < values["requested_concurrency"] * values["container_memory_mib"]
        or values["job_pids_max"] < values["requested_concurrency"] * values["container_pids"]
    ):
        raise CapacityContractError(f"{pool} capacity policy limits are invalid")
    return CapacityPolicyContract(
        pool=pool,
        source=source,
        source_sha256=hashlib.sha256(raw).hexdigest(),
        values=values,
    )


def load_platform_health_contract(repo_root: Path) -> PlatformHealthContract:
    """Load the exact platform-health thresholds and canonical inventory."""

    source = PLATFORM_HEALTH_CONFIG_SOURCE
    raw, payload = _read_toml(repo_root / source, label="platform-health config")
    oldlab_nodes = payload.get("oldlab_nodes")
    gb10_nodes = payload.get("gb10_nodes")
    aliases = payload.get("host_aliases")
    policy_sources = payload.get("capacity_policy_sources")
    expected_oldlab = tuple(f"oldlab-{index}" for index in range(1, 6))
    expected_gb10 = tuple(f"trt-gb10-{index}" for index in range(1, 16))
    expected_aliases = {
        **{node: f"trt-eai-oldlab-{index}" for index, node in enumerate(expected_oldlab, 1)},
        "trt-gb10-1": "gx10-01c7",
        "trt-gb10-2": "gx10-0fca",
        "trt-gb10-3": "gx10-0f0d",
        "trt-gb10-4": "gx10-0d93",
        "trt-gb10-5": "gx10-1036",
        "trt-gb10-6": "gx10-1000",
        "trt-gb10-7": "gx10-0faf",
        "trt-gb10-8": "gx10-db22",
        "trt-gb10-9": "gx10-16f6",
        "trt-gb10-10": "gx10-0f82",
        "trt-gb10-11": "gx10-c38b",
        "trt-gb10-12": "gx10-e45f",
        "trt-gb10-13": "gx10-fc5d",
        "trt-gb10-14": "gx10-0a49",
        "trt-gb10-15": "gx10-0152",
    }
    minimum_cpu = payload.get("minimum_oldlab_free_cpu_cores")
    minimum_memory = payload.get("minimum_oldlab_free_memory_bytes")
    busy_ratio = payload.get("maximum_cpu_busy_ratio")
    inventory_is_typed = (
        isinstance(oldlab_nodes, list)
        and all(isinstance(node, str) for node in oldlab_nodes)
        and isinstance(gb10_nodes, list)
        and all(isinstance(node, str) for node in gb10_nodes)
        and isinstance(aliases, dict)
        and all(isinstance(node, str) and isinstance(host, str) for node, host in aliases.items())
    )
    if (
        set(payload) != _PLATFORM_HEALTH_FIELDS
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("collector_host") != "trt-eai-oldlab-2"
        or payload.get("namespace") != "loom-staging"
        or payload.get("longhorn_namespace") != "longhorn-system"
        or policy_sources != CAPACITY_POLICY_SOURCES
        or not inventory_is_typed
        or tuple(cast(list[str], oldlab_nodes)) != expected_oldlab
        or tuple(cast(list[str], gb10_nodes)) != expected_gb10
        or aliases != expected_aliases
        or not isinstance(minimum_cpu, int)
        or isinstance(minimum_cpu, bool)
        or minimum_cpu < 1
        or not isinstance(minimum_memory, int)
        or isinstance(minimum_memory, bool)
        or minimum_memory < 1
        or not isinstance(busy_ratio, (int, float))
        or isinstance(busy_ratio, bool)
        or not 0 < busy_ratio < 1
    ):
        raise CapacityContractError("platform-health contract binding is invalid")
    return PlatformHealthContract(
        source=source,
        source_sha256=hashlib.sha256(raw).hexdigest(),
        minimum_oldlab_free_cpu_cores=minimum_cpu,
        minimum_oldlab_free_memory_bytes=minimum_memory,
        maximum_cpu_busy_ratio=float(busy_ratio),
        oldlab_nodes=expected_oldlab,
        gb10_nodes=expected_gb10,
        host_aliases=expected_aliases,
    )
