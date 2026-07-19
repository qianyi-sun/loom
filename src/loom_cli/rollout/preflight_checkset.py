"""Exact assembly boundary for the complete preflight and final gate set."""

from __future__ import annotations

from dataclasses import dataclass

from loom_cli.rollout.preflight_contract import RegisteredCheck
from loom_cli.rollout.preflight_coverage import load_coverage_manifest
from loom_cli.rollout.preflight_registry import PreflightRegistry


@dataclass(frozen=True, slots=True)
class RolloutCheckSet:
    preflight_registry: PreflightRegistry
    final_checks: tuple[RegisteredCheck, ...]

    @classmethod
    def assemble(
        cls,
        *,
        tier0: tuple[RegisteredCheck, ...],
        tier1: tuple[RegisteredCheck, ...],
        tier2: tuple[RegisteredCheck, ...],
        tier3: tuple[RegisteredCheck, ...],
        final: tuple[RegisteredCheck, ...],
    ) -> RolloutCheckSet:
        groups = {0: tier0, 1: tier1, 2: tier2, 3: tier3, 4: final}
        for tier, checks in groups.items():
            if not checks or any(check.spec.tier != tier for check in checks):
                raise ValueError(f"rollout check group tier {tier} is incomplete or misclassified")
        all_checks = tuple(check for tier in range(5) for check in groups[tier])
        coverage = load_coverage_manifest()
        coverage.require_exact_registry(all_checks, through_tier=4)
        preflight = tuple(check for check in all_checks if check.spec.tier <= 3)
        registry = PreflightRegistry.build(preflight, through_tier=3)
        final_checks = tuple(sorted(final, key=lambda check: check.spec.check_id))
        return cls(preflight_registry=registry, final_checks=final_checks)

    def __post_init__(self) -> None:
        if self.preflight_registry.through_tier != 3 or not self.final_checks:
            raise ValueError("rollout check set is incomplete")
        if any(check.spec.tier != 4 for check in self.final_checks):
            raise ValueError("rollout final check set contains a preflight check")


__all__ = ["RolloutCheckSet"]
