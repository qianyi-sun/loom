"""Fail-closed runtime registry for checked-in staged preflight coverage."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from types import MappingProxyType

from loom_cli.rollout.preflight_contract import PreflightDag, RegisteredCheck
from loom_cli.rollout.preflight_coverage import (
    DEFAULT_COVERAGE_MANIFEST,
    load_coverage_manifest,
)


@dataclass(frozen=True, slots=True)
class PreflightRegistry:
    checks: tuple[RegisteredCheck, ...]
    through_tier: int
    coverage_digest: str
    registry_digest: str

    @classmethod
    def build(
        cls,
        checks: Sequence[RegisteredCheck],
        *,
        through_tier: int,
    ) -> PreflightRegistry:
        coverage = load_coverage_manifest()
        coverage.require_exact_registry(checks, through_tier=through_tier)
        coverage_digest = hashlib.sha256(DEFAULT_COVERAGE_MANIFEST.read_bytes()).hexdigest()
        ordered = tuple(sorted(checks, key=lambda check: check.spec.check_id))
        payload = {
            "checks": [
                {
                    "check_id": check.spec.check_id,
                    "contract_digest": check.spec.contract_digest,
                    "implementation_digest": check.implementation_digest,
                }
                for check in ordered
            ],
            "coverage_digest": coverage_digest,
            "through_tier": through_tier,
        }
        registry_digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return cls(
            checks=ordered,
            through_tier=through_tier,
            coverage_digest=coverage_digest,
            registry_digest=registry_digest,
        )

    def __post_init__(self) -> None:
        if (
            not self.checks
            or self.through_tier not in {0, 1, 2, 3, 4}
            or len(self.coverage_digest) != 64
            or len(self.registry_digest) != 64
        ):
            raise ValueError("preflight registry identity is invalid")
        object.__setattr__(
            self,
            "checks",
            tuple(sorted(self.checks, key=lambda check: check.spec.check_id)),
        )

    def dag(self, *, max_concurrency: int = 8) -> PreflightDag:
        return PreflightDag(self.checks, max_concurrency=max_concurrency)

    @property
    def implementation_digests(self) -> MappingProxyType[str, str]:
        return MappingProxyType(
            {check.spec.check_id: check.implementation_digest for check in self.checks}
        )


__all__ = ["PreflightRegistry"]
