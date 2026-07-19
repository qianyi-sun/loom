"""Exact-candidate isolated rehearsal session and journal authority."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from threading import Lock
from types import MappingProxyType

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
REHEARSAL_CHECK_IDS = (
    "rehearsal.namespace",
    "rehearsal.db-clone",
    "rehearsal.systemd-launch",
    "rehearsal.migration",
    "rehearsal.release",
    "rehearsal.api-smoke",
    "rehearsal.browser",
    "rehearsal.cleanup",
)


@dataclass(frozen=True, slots=True)
class RehearsalResult:
    check_id: str
    isolation_id: str
    candidate_sha: str
    mutation_epoch: int
    evidence_digest: str
    journal_digest: str
    protected_mutation: bool
    cleanup_verified: bool
    blockers: Mapping[str, str]

    def __post_init__(self) -> None:
        blockers = dict(self.blockers)
        if (
            self.check_id not in REHEARSAL_CHECK_IDS
            or not self.isolation_id.startswith("rehearsal-")
            or len(self.candidate_sha) not in {40, 64}
            or any(char not in "0123456789abcdef" for char in self.candidate_sha)
            or self.mutation_epoch < 0
            or _SHA256_RE.fullmatch(self.evidence_digest) is None
            or _SHA256_RE.fullmatch(self.journal_digest) is None
            or self.protected_mutation
            or (self.cleanup_verified and self.check_id != "rehearsal.cleanup")
            or len(blockers) > 64
            or any(
                not key or len(key) > 96 or not value or len(value) > 256
                for key, value in blockers.items()
            )
        ):
            raise ValueError("isolated rehearsal evidence is invalid")
        object.__setattr__(self, "blockers", MappingProxyType(blockers))

    @property
    def ready(self) -> bool:
        return not self.blockers and (self.check_id != "rehearsal.cleanup" or self.cleanup_verified)


RehearsalAction = Callable[[], RehearsalResult]


class IsolatedRehearsalSession:
    """Execute each exact isolated action once and retain its journal result."""

    def __init__(
        self,
        actions: Mapping[str, RehearsalAction],
        *,
        isolation_id: str,
        candidate_sha: str,
        mutation_epoch: int,
    ) -> None:
        if (
            set(actions) != set(REHEARSAL_CHECK_IDS)
            or not isolation_id.startswith("rehearsal-")
            or len(candidate_sha) not in {40, 64}
            or any(char not in "0123456789abcdef" for char in candidate_sha)
            or mutation_epoch < 0
        ):
            raise ValueError("isolated rehearsal session binding is invalid")
        self._actions = dict(actions)
        self._isolation_id = isolation_id
        self._candidate_sha = candidate_sha
        self._epoch = mutation_epoch
        self._results: dict[str, RehearsalResult] = {}
        self._lock = Lock()

    def execute(self, check_id: str) -> RehearsalResult:
        with self._lock:
            cached = self._results.get(check_id)
        if cached is not None:
            return cached
        result = self._actions[check_id]()
        if (
            result.check_id != check_id
            or result.isolation_id != self._isolation_id
            or result.candidate_sha != self._candidate_sha
            or result.mutation_epoch != self._epoch
        ):
            raise ValueError("isolated rehearsal result binding drifted")
        with self._lock:
            existing = self._results.setdefault(check_id, result)
        return existing


__all__ = [
    "REHEARSAL_CHECK_IDS",
    "IsolatedRehearsalSession",
    "RehearsalAction",
    "RehearsalResult",
]
