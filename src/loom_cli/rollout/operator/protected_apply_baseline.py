"""Exact Tier 2 baseline authority consumed by protected component classifiers."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType

from loom_cli.rollout.preflight_contract import (
    CheckExecution,
    CheckOperation,
    PreflightAttestation,
    StageCapability,
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CHECK_IDS = (
    "staging.health",
    "staging.auth",
    "staging.catalog-task",
    "staging.storage-db",
    "staging.network",
    "staging.release-baseline",
)


@dataclass(frozen=True, slots=True)
class ProtectedApplyBaseline:
    schema_version: int
    environment: str
    namespace: str
    mutation_epoch: int
    readonly_principal: str
    resource_digests: Mapping[str, str]
    implementation_digests: Mapping[str, str]
    evidence_hashes: Mapping[str, str]
    baseline_digest: str

    def __post_init__(self) -> None:
        maps = (
            self.resource_digests,
            self.implementation_digests,
            self.evidence_hashes,
        )
        if (
            self.schema_version != 1
            or self.environment != "staging"
            or not self.namespace
            or type(self.mutation_epoch) is not int
            or self.mutation_epoch < 0
            or not self.readonly_principal
            or any(set(values) != set(_CHECK_IDS) for values in maps)
            or any(
                _SHA256_RE.fullmatch(item) is None for values in maps for item in values.values()
            )
            or _SHA256_RE.fullmatch(self.baseline_digest) is None
        ):
            raise ValueError("protected apply baseline is invalid")
        object.__setattr__(self, "resource_digests", MappingProxyType(dict(self.resource_digests)))
        object.__setattr__(
            self,
            "implementation_digests",
            MappingProxyType(dict(self.implementation_digests)),
        )
        object.__setattr__(self, "evidence_hashes", MappingProxyType(dict(self.evidence_hashes)))

    @classmethod
    def from_executions(
        cls,
        attestation: PreflightAttestation,
        executions: Sequence[CheckExecution],
    ) -> ProtectedApplyBaseline:
        tier2 = tuple(execution for execution in executions if execution.tier == 2)
        matches = {execution.check_id: execution for execution in tier2}
        if len(tier2) != len(_CHECK_IDS) or set(matches) != set(_CHECK_IDS):
            raise ValueError("protected apply baseline coverage is incomplete")
        resource_digests: dict[str, str] = {}
        implementation_digests: dict[str, str] = {}
        evidence_hashes: dict[str, str] = {}
        principals: set[str] = set()
        for check_id in _CHECK_IDS:
            execution = matches[check_id]
            evidence = execution.evidence
            principal = evidence.get("readonly-principal")
            resource_digest = evidence.get("resource-digest")
            blockers = evidence.get("blockers")
            if (
                not execution.passed
                or execution.stage is not StageCapability.BASELINE_LIVE_READONLY
                or execution.operation is not CheckOperation.PROBE
                or evidence.get("ready") is not True
                or evidence.get("observed-epoch") != attestation.bindings.staging_mutation_epoch
                or not isinstance(principal, str)
                or not principal
                or not isinstance(resource_digest, str)
                or _SHA256_RE.fullmatch(resource_digest) is None
                or not isinstance(blockers, Mapping)
                or blockers
                or attestation.check_implementation_digests.get(check_id)
                != execution.implementation_digest
                or attestation.evidence_hashes.get(check_id) != execution.evidence_hash
            ):
                raise ValueError("protected apply baseline evidence drifted")
            principals.add(principal)
            resource_digests[check_id] = resource_digest
            implementation_digests[check_id] = execution.implementation_digest
            evidence_hashes[check_id] = execution.evidence_hash
        if len(principals) != 1:
            raise ValueError("protected apply baseline principal is ambiguous")
        payload = {
            "schema_version": 1,
            "environment": attestation.bindings.environment,
            "namespace": attestation.bindings.namespace,
            "mutation_epoch": attestation.bindings.staging_mutation_epoch,
            "readonly_principal": next(iter(principals)),
            "resource_digests": resource_digests,
            "implementation_digests": implementation_digests,
            "evidence_hashes": evidence_hashes,
        }
        return cls.from_dict({**payload, "baseline_digest": _hash_json(payload)})

    def to_dict(self) -> dict[str, object]:
        return {
            name: dict(value) if isinstance(value, Mapping) else value
            for name, value in (
                (field, getattr(self, field)) for field in self.__dataclass_fields__
            )
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ProtectedApplyBaseline:
        if set(value) != set(cls.__dataclass_fields__):
            raise ValueError("protected apply baseline fields are invalid")
        baseline = cls(
            schema_version=_integer(value, "schema_version"),
            environment=_string(value, "environment"),
            namespace=_string(value, "namespace"),
            mutation_epoch=_integer(value, "mutation_epoch"),
            readonly_principal=_string(value, "readonly_principal"),
            resource_digests=_string_map(value, "resource_digests"),
            implementation_digests=_string_map(value, "implementation_digests"),
            evidence_hashes=_string_map(value, "evidence_hashes"),
            baseline_digest=_string(value, "baseline_digest"),
        )
        payload = {
            key: item for key, item in baseline.to_dict().items() if key != "baseline_digest"
        }
        if _hash_json(payload) != baseline.baseline_digest:
            raise ValueError("protected apply baseline content drifted")
        return baseline


def _hash_json(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(dict(value), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _string(value: Mapping[str, object], key: str) -> str:
    item = value[key]
    if not isinstance(item, str):
        raise ValueError(f"protected apply baseline {key} must be a string")
    return item


def _integer(value: Mapping[str, object], key: str) -> int:
    item = value[key]
    if type(item) is not int:
        raise ValueError(f"protected apply baseline {key} must be an integer")
    return item


def _string_map(value: Mapping[str, object], key: str) -> dict[str, str]:
    item = value[key]
    if not isinstance(item, Mapping) or not all(
        isinstance(name, str) and isinstance(entry, str) for name, entry in item.items()
    ):
        raise ValueError(f"protected apply baseline {key} must be a string map")
    return dict(item)


__all__ = ["ProtectedApplyBaseline"]
