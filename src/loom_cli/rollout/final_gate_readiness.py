"""Single-source final-only rollout gate session.

Final checks keep their protected mutation boundary explicit. Probe/plan never
mutate; protected convergence, live smoke, and authenticated live browser apply
operations may report a protected staging mutation under the same rollout epoch.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from threading import Lock
from types import MappingProxyType

from loom_cli.rollout.preflight_contract import CheckOperation

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
FINAL_CHECK_IDS = (
    "final.protected-apply",
    "final.convergence",
    "final.drift",
    "final.smoke",
    "final.browser",
    "final.summary",
)
PROTECTED_MUTATION_CHECK_IDS = frozenset({"final.protected-apply", "final.smoke", "final.browser"})
FINAL_PREDICATE_IDS = MappingProxyType(
    {
        "final.protected-apply": (
            "protected.epoch-claim",
            "protected.migration-artifact",
            "protected.manifest-artifact",
            "protected.production-defaults",
            "protected.gb10-source",
            "protected.gb10-service-activation",
        ),
        "final.convergence": (
            "convergence.migration-live",
            "convergence.manifests-live",
            "convergence.defaults-live",
            "convergence.gb10-live",
        ),
        "final.drift": ("drift.attestation-live",),
        "final.smoke": (
            "smoke.token-authority",
            "smoke.payload-contract",
            "smoke.live-terminal",
        ),
        "final.browser": (
            "browser.runtime-report",
            "browser.rehearsal-binding",
            "browser.live-route",
        ),
        "final.summary": ("summary.evidence-seal",),
    }
)


@dataclass(frozen=True, slots=True)
class FinalGateResult:
    check_id: str
    operation: CheckOperation
    candidate_sha: str
    attestation_digest: str
    observed_epoch: int
    evidence_digest: str
    protected_mutation: bool
    blockers: Mapping[str, str]

    def __post_init__(self) -> None:
        blockers = dict(self.blockers)
        mutation_allowed = bool(
            self.check_id in PROTECTED_MUTATION_CHECK_IDS and self.operation is CheckOperation.APPLY
        )
        if (
            self.check_id not in FINAL_CHECK_IDS
            or self.operation not in set(CheckOperation)
            or len(self.candidate_sha) not in {40, 64}
            or any(character not in "0123456789abcdef" for character in self.candidate_sha)
            or _SHA256_RE.fullmatch(self.attestation_digest) is None
            or self.observed_epoch < 0
            or _SHA256_RE.fullmatch(self.evidence_digest) is None
            or (self.protected_mutation and not mutation_allowed)
            or len(blockers) > 64
            or any(
                not key or len(key) > 96 or not value or len(value) > 256
                for key, value in blockers.items()
            )
        ):
            raise ValueError("final gate evidence is invalid")
        object.__setattr__(self, "blockers", MappingProxyType(blockers))

    @property
    def ready(self) -> bool:
        return not self.blockers


FinalGateAction = Callable[[CheckOperation], FinalGateResult]


class FinalGateSession:
    """Execute and cache an exact final gate operation once."""

    def __init__(
        self,
        actions: Mapping[str, FinalGateAction],
        *,
        candidate_sha: str,
        attestation_digest: str,
        mutation_epoch: int,
    ) -> None:
        if (
            set(actions) != set(FINAL_CHECK_IDS)
            or len(candidate_sha) not in {40, 64}
            or any(character not in "0123456789abcdef" for character in candidate_sha)
            or _SHA256_RE.fullmatch(attestation_digest) is None
            or mutation_epoch < 0
        ):
            raise ValueError("final gate session binding is invalid")
        self._actions = dict(actions)
        self._candidate_sha = candidate_sha
        self._attestation_digest = attestation_digest
        self._mutation_epoch = mutation_epoch
        self._results: dict[tuple[str, CheckOperation], FinalGateResult] = {}
        self._lock = Lock()

    def execute(self, check_id: str, operation: CheckOperation) -> FinalGateResult:
        key = (check_id, operation)
        with self._lock:
            cached = self._results.get(key)
        if cached is not None:
            return cached
        result = self._actions[check_id](operation)
        if (
            result.check_id != check_id
            or result.operation is not operation
            or result.candidate_sha != self._candidate_sha
            or result.attestation_digest != self._attestation_digest
            or result.observed_epoch < self._mutation_epoch
        ):
            raise ValueError("final gate result binding drifted")
        with self._lock:
            existing = self._results.setdefault(key, result)
        return existing


__all__ = [
    "FINAL_CHECK_IDS",
    "FINAL_PREDICATE_IDS",
    "PROTECTED_MUTATION_CHECK_IDS",
    "FinalGateAction",
    "FinalGateResult",
    "FinalGateSession",
]
