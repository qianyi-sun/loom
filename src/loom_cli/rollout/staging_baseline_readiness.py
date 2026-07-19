"""Shared readonly current-staging baseline probes for Tier 2 preflight."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from threading import Lock
from types import MappingProxyType

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CHECK_IDS = (
    "staging.health",
    "staging.auth",
    "staging.catalog-task",
    "staging.storage-db",
    "staging.network",
)


@dataclass(frozen=True, slots=True)
class BaselineProbeResult:
    check_id: str
    environment: str
    namespace: str
    route: str
    readonly_principal: str
    observed_mutation_epoch: int
    resource_digest: str
    blockers: Mapping[str, str]

    def __post_init__(self) -> None:
        blockers = dict(self.blockers)
        if (
            self.check_id not in _CHECK_IDS
            or self.environment != "staging"
            or not self.namespace
            or not self.route.startswith("https://")
            or self.readonly_principal != "loom-rollout-readonly"
            or self.observed_mutation_epoch < 0
            or _SHA256_RE.fullmatch(self.resource_digest) is None
            or len(blockers) > 64
            or any(
                not key or len(key) > 96 or not value or len(value) > 256
                for key, value in blockers.items()
            )
        ):
            raise ValueError("staging baseline probe evidence is invalid")
        object.__setattr__(self, "blockers", MappingProxyType(blockers))

    @property
    def ready(self) -> bool:
        return not self.blockers


ReadonlyProbe = Callable[[], BaselineProbeResult]


class StagingBaselineSession:
    """Cache each independent readonly probe exactly once for aggregation."""

    def __init__(
        self,
        probes: Mapping[str, ReadonlyProbe],
        *,
        expected_environment: str,
        expected_namespace: str,
        expected_route: str,
        expected_mutation_epoch: int,
    ) -> None:
        if (
            set(probes) != set(_CHECK_IDS)
            or expected_environment != "staging"
            or not expected_namespace
            or not expected_route.startswith("https://")
            or expected_mutation_epoch < 0
        ):
            raise ValueError("staging baseline session binding is invalid")
        self._probes = dict(probes)
        self._environment = expected_environment
        self._namespace = expected_namespace
        self._route = expected_route
        self._epoch = expected_mutation_epoch
        self._results: dict[str, BaselineProbeResult] = {}
        self._lock = Lock()

    def probe(self, check_id: str) -> BaselineProbeResult:
        with self._lock:
            cached = self._results.get(check_id)
        if cached is not None:
            return cached
        result = self._probes[check_id]()
        if (
            result.check_id != check_id
            or result.environment != self._environment
            or result.namespace != self._namespace
            or result.route != self._route
            or result.observed_mutation_epoch != self._epoch
        ):
            raise ValueError("staging baseline probe binding drifted")
        with self._lock:
            existing = self._results.setdefault(check_id, result)
        return existing

    def aggregate(self) -> BaselineProbeResult:
        with self._lock:
            if set(self._results) != set(_CHECK_IDS):
                raise ValueError("staging baseline probe set is incomplete")
            results = dict(self._results)
        blockers = {
            f"{check_id}:{name}": code
            for check_id, result in sorted(results.items())
            for name, code in sorted(result.blockers.items())
        }
        digest = hashlib.sha256(
            json.dumps(
                {
                    check_id: {
                        "blockers": dict(result.blockers),
                        "resource_digest": result.resource_digest,
                    }
                    for check_id, result in sorted(results.items())
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        return BaselineProbeResult(
            check_id="staging.health",
            environment=self._environment,
            namespace=self._namespace,
            route=self._route,
            readonly_principal="loom-rollout-readonly",
            observed_mutation_epoch=self._epoch,
            resource_digest=digest,
            blockers=blockers,
        )


__all__ = [
    "BaselineProbeResult",
    "ReadonlyProbe",
    "StagingBaselineSession",
]
