"""Canonical workload-trust contract for the v1 release boundary."""

from __future__ import annotations

from dataclasses import dataclass

INTERNAL_TRUSTED = "internal_trusted"


@dataclass(frozen=True)
class WorkloadTrustContract:
    """The workload capabilities that v1 permits in a deployment profile."""

    workload_trust_mode: str
    taskset_transforms_enabled: bool
    taskset_transform_network_isolated: bool
    untrusted_workload_isolation: bool

    def v1_violations(self) -> list[str]:
        """Return deterministic, field-specific v1 contract violations."""
        violations: list[str] = []
        if self.workload_trust_mode != INTERNAL_TRUSTED:
            violations.append("workload_trust_mode must be internal_trusted")
        if self.taskset_transforms_enabled:
            violations.append("taskset_transforms_enabled must be false")
        if self.taskset_transform_network_isolated:
            violations.append("taskset_transform_network_isolated must be false")
        if self.untrusted_workload_isolation:
            violations.append("untrusted_workload_isolation must be false")
        return violations

    def as_manifest(self) -> dict[str, str | bool]:
        """Return the exact wire representation used by render/gate callers."""
        return {
            "workload_trust_mode": self.workload_trust_mode,
            "taskset_transforms_enabled": self.taskset_transforms_enabled,
            "taskset_transform_network_isolated": self.taskset_transform_network_isolated,
            "untrusted_workload_isolation": self.untrusted_workload_isolation,
        }
