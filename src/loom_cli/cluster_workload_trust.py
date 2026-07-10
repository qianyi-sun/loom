"""Shared workload-trust parsing for protected cluster release boundaries."""

from __future__ import annotations

import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loom.workload_trust import WorkloadTrustContract

PROTECTED_WORKLOAD_TRUST_ENVIRONMENTS = frozenset({"staging", "production"})

WORKLOAD_CONTRACT_WIRE_KEYS = (
    "workload_trust_mode",
    "taskset_transforms_enabled",
    "taskset_transform_network_isolated",
    "untrusted_workload_isolation",
)

WORKLOAD_CONTRACT_ENV_NAMES = {
    "workload_trust_mode": "LOOM_SVC_WORKLOAD_TRUST_MODE",
    "taskset_transforms_enabled": "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORMS_ENABLED",
    "taskset_transform_network_isolated": (
        "LOOM_SVC_TASKSET_MATERIALIZER_TRANSFORM_NETWORK_ISOLATED"
    ),
    "untrusted_workload_isolation": "LOOM_SVC_UNTRUSTED_WORKLOAD_ISOLATION",
}


@dataclass(frozen=True)
class _MalformedWorkloadContractProfile:
    """Opaque marker that keeps TOML syntax failures out of release evidence."""


_MALFORMED_WORKLOAD_CONTRACT_PROFILE = _MalformedWorkloadContractProfile()


def workload_contract_from_mapping(value: object) -> WorkloadTrustContract:
    """Parse an exact, untrusted four-key workload contract mapping."""
    if value is _MALFORMED_WORKLOAD_CONTRACT_PROFILE:
        raise ValueError("workload_contract TOML is syntactically invalid")
    if not isinstance(value, Mapping):
        raise ValueError("workload_contract must be a table with the four v1 fields")

    keys = set(value)
    expected_keys = set(WORKLOAD_CONTRACT_WIRE_KEYS)
    missing = sorted(expected_keys - keys)
    unexpected = keys - expected_keys
    if missing or unexpected:
        raise ValueError("workload_contract must contain exactly the four required v1 fields")

    workload_trust_mode = value["workload_trust_mode"]
    if not isinstance(workload_trust_mode, str):
        raise ValueError("workload_contract.workload_trust_mode must be a string")

    booleans: dict[str, bool] = {}
    for name in WORKLOAD_CONTRACT_WIRE_KEYS[1:]:
        raw = value[name]
        if type(raw) is not bool:
            raise ValueError(f"workload_contract.{name} must be a boolean")
        booleans[name] = raw

    return WorkloadTrustContract(
        workload_trust_mode=workload_trust_mode,
        taskset_transforms_enabled=booleans["taskset_transforms_enabled"],
        taskset_transform_network_isolated=booleans[
            "taskset_transform_network_isolated"
        ],
        untrusted_workload_isolation=booleans["untrusted_workload_isolation"],
    )


def workload_contract_from_cluster_config(config: Any) -> WorkloadTrustContract:
    """Read the exact tuple from the loaded cluster-config object."""
    profile = getattr(config, "workload_contract", None)
    if profile is None:
        raise ValueError("cluster config has no workload_contract table")
    return workload_contract_from_mapping(
        {
            key: getattr(profile, key, None)
            for key in WORKLOAD_CONTRACT_WIRE_KEYS
        }
    )


def workload_contract_profile_from_file(config_path: Path | None) -> object:
    """Return the raw profile table without defaulting an absent table."""
    if config_path is None:
        return None
    try:
        raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError:
        return _MALFORMED_WORKLOAD_CONTRACT_PROFILE
    if not isinstance(raw, dict):
        return None
    return raw.get("workload_contract")


def workload_contract_environment(contract: WorkloadTrustContract) -> dict[str, str]:
    """Return the exact loom-service environment representation."""
    manifest = contract.as_manifest()
    return {
        env_name: str(manifest[field_name])
        for field_name, env_name in WORKLOAD_CONTRACT_ENV_NAMES.items()
    }
