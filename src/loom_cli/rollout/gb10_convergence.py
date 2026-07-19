"""Single-source GB10 candidate convergence classifier and mutation plan."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType

_HOST_RE = re.compile(r"trt-gb10-(?:[1-9]|1[0-5])\Z")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class GB10ConvergenceState(StrEnum):
    READY = "ready"
    EXACT = "exact"
    DRIFTED = "drifted"


class GB10MutationKind(StrEnum):
    CHECKOUT = "checkout"
    ENVIRONMENT = "environment"
    UNITS = "units"
    LEGACY_RETIRE = "legacy-retire"
    SERVICE_TIMER = "service-timer"


@dataclass(frozen=True, slots=True)
class GB10HostCandidateObservation:
    host: str
    boot_id: str
    baseline_ready: bool
    candidate_source_exact: bool
    checkout_exact: bool
    environment_exact: bool
    units_exact: bool
    legacy_absent: bool
    service_timer_exact: bool
    evidence_digest: str

    def __post_init__(self) -> None:
        if (
            _HOST_RE.fullmatch(self.host) is None
            or not self.boot_id
            or _SHA256_RE.fullmatch(self.evidence_digest) is None
            or any(
                type(value) is not bool
                for value in (
                    self.baseline_ready,
                    self.candidate_source_exact,
                    self.checkout_exact,
                    self.environment_exact,
                    self.units_exact,
                    self.legacy_absent,
                    self.service_timer_exact,
                )
            )
        ):
            raise ValueError("GB10 host convergence observation is invalid")

    @property
    def exact(self) -> bool:
        return all(
            (
                self.baseline_ready,
                self.candidate_source_exact,
                self.checkout_exact,
                self.environment_exact,
                self.units_exact,
                self.legacy_absent,
                self.service_timer_exact,
            )
        )

    @property
    def applicable(self) -> bool:
        return self.baseline_ready and self.candidate_source_exact


@dataclass(frozen=True, slots=True)
class GB10FleetCandidateObservation:
    hosts: Mapping[str, GB10HostCandidateObservation]
    candidate_source_digest: str

    def __post_init__(self) -> None:
        hosts = dict(self.hosts)
        if (
            not hosts
            or set(hosts) != {observation.host for observation in hosts.values()}
            or _SHA256_RE.fullmatch(self.candidate_source_digest) is None
        ):
            raise ValueError("GB10 fleet convergence observation is invalid")
        object.__setattr__(self, "hosts", MappingProxyType(hosts))


@dataclass(frozen=True, slots=True)
class GB10HostMutation:
    host: str
    operations: tuple[GB10MutationKind, ...]

    def __post_init__(self) -> None:
        if (
            _HOST_RE.fullmatch(self.host) is None
            or not self.operations
            or len(set(self.operations)) != len(self.operations)
        ):
            raise ValueError("GB10 host mutation plan is invalid")


@dataclass(frozen=True, slots=True)
class GB10ConvergencePlan:
    state: GB10ConvergenceState
    mutations: tuple[GB10HostMutation, ...]
    blockers: Mapping[str, str]
    evidence_digest: str

    def __post_init__(self) -> None:
        blockers = dict(self.blockers)
        if (
            _SHA256_RE.fullmatch(self.evidence_digest) is None
            or len({mutation.host for mutation in self.mutations}) != len(self.mutations)
            or (self.state is GB10ConvergenceState.EXACT and (self.mutations or blockers))
            or (self.state is GB10ConvergenceState.READY and (not self.mutations or blockers))
            or (self.state is GB10ConvergenceState.DRIFTED and (self.mutations or not blockers))
        ):
            raise ValueError("GB10 convergence plan is inconsistent")
        object.__setattr__(self, "blockers", MappingProxyType(blockers))


def plan_gb10_candidate_convergence(
    observation: GB10FleetCandidateObservation,
    *,
    expected_boot_ids: Mapping[str, str],
    expected_candidate_source_digest: str,
) -> GB10ConvergencePlan:
    """Classify exact, safely applicable, or drifted fleet state once."""
    expected = dict(expected_boot_ids)
    blockers: dict[str, str] = {}
    if (
        not expected
        or set(expected) != set(observation.hosts)
        or any(_HOST_RE.fullmatch(host) is None or not boot for host, boot in expected.items())
        or _SHA256_RE.fullmatch(expected_candidate_source_digest) is None
    ):
        blockers["fleet-identity"] = "gb10-fleet-identity-drift"
    if observation.candidate_source_digest != expected_candidate_source_digest:
        blockers["candidate-source"] = "gb10-candidate-source-drift"
    for host, observed in sorted(observation.hosts.items()):
        if expected.get(host) != observed.boot_id:
            blockers[host] = "gb10-host-boot-drift"
        elif not observed.applicable:
            blockers[host] = "gb10-host-not-safely-applicable"
    if blockers:
        return _plan(GB10ConvergenceState.DRIFTED, (), blockers, observation, expected)
    if all(observed.exact for observed in observation.hosts.values()):
        return _plan(GB10ConvergenceState.EXACT, (), {}, observation, expected)

    mutations: list[GB10HostMutation] = []
    for host, observed in sorted(observation.hosts.items()):
        operations: list[GB10MutationKind] = []
        if not observed.checkout_exact:
            operations.append(GB10MutationKind.CHECKOUT)
        if not observed.environment_exact:
            operations.append(GB10MutationKind.ENVIRONMENT)
        if not observed.units_exact:
            operations.append(GB10MutationKind.UNITS)
        if not observed.legacy_absent:
            operations.append(GB10MutationKind.LEGACY_RETIRE)
        if not observed.service_timer_exact:
            operations.append(GB10MutationKind.SERVICE_TIMER)
        if operations:
            mutations.append(GB10HostMutation(host, tuple(operations)))
    if not mutations:
        raise ValueError("GB10 non-exact fleet produced no mutations")
    return _plan(GB10ConvergenceState.READY, tuple(mutations), {}, observation, expected)


def _plan(
    state: GB10ConvergenceState,
    mutations: tuple[GB10HostMutation, ...],
    blockers: Mapping[str, str],
    observation: GB10FleetCandidateObservation,
    expected_boot_ids: Mapping[str, str],
) -> GB10ConvergencePlan:
    payload = {
        "blockers": dict(blockers),
        "candidate_source_digest": observation.candidate_source_digest,
        "expected_boot_ids": dict(sorted(expected_boot_ids.items())),
        "hosts": {
            host: {
                "applicable": observed.applicable,
                "boot_id": observed.boot_id,
                "evidence_digest": observed.evidence_digest,
                "exact": observed.exact,
            }
            for host, observed in sorted(observation.hosts.items())
        },
        "mutations": [
            {
                "host": mutation.host,
                "operations": [operation.value for operation in mutation.operations],
            }
            for mutation in mutations
        ],
        "state": state.value,
    }
    return GB10ConvergencePlan(
        state=state,
        mutations=mutations,
        blockers=blockers,
        evidence_digest=hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    )


__all__ = [
    "GB10ConvergencePlan",
    "GB10ConvergenceState",
    "GB10FleetCandidateObservation",
    "GB10HostCandidateObservation",
    "GB10HostMutation",
    "GB10MutationKind",
    "plan_gb10_candidate_convergence",
]
