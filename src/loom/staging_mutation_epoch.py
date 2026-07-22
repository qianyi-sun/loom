"""Single compare-and-swap authority for every protected staging mutation."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol

_REQUEST_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{7,79}$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")


class ProtectedMutationClass(StrEnum):
    ROLLOUT_APPLY = "rollout_apply"
    LIFECYCLE_GC = "lifecycle_gc"
    OBJECT_REWRITE = "object_rewrite"
    ROLLBACK = "rollback"


@dataclass(frozen=True, slots=True)
class MutationEpochState:
    environment: str
    namespace: str
    epoch: int
    mutation_class: ProtectedMutationClass
    request_id: str
    evidence_sha256: str
    updated_at: datetime

    def __post_init__(self) -> None:
        if (
            self.environment != "staging"
            or not self.namespace
            or self.namespace != self.namespace.strip()
            or self.epoch < 0
            or _REQUEST_ID_RE.fullmatch(self.request_id) is None
            or _DIGEST_RE.fullmatch(self.evidence_sha256) is None
            or self.updated_at.tzinfo is None
            or self.updated_at.utcoffset() is None
        ):
            raise ValueError("staging mutation epoch identity is invalid")

    @property
    def evidence_digest(self) -> str:
        payload = {
            "environment": self.environment,
            "epoch": self.epoch,
            "evidence_sha256": self.evidence_sha256,
            "mutation_class": self.mutation_class.value,
            "namespace": self.namespace,
            "request_id": self.request_id,
            "updated_at": self.updated_at.isoformat(),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class MutationEpochAdvance:
    environment: str
    namespace: str
    expected_epoch: int
    mutation_class: ProtectedMutationClass
    request_id: str
    evidence_sha256: str
    occurred_at: datetime

    def __post_init__(self) -> None:
        MutationEpochState(
            environment=self.environment,
            namespace=self.namespace,
            epoch=self.expected_epoch,
            mutation_class=self.mutation_class,
            request_id=self.request_id,
            evidence_sha256=self.evidence_sha256,
            updated_at=self.occurred_at,
        )


class MutationEpochStore(Protocol):
    def compare_and_swap(self, advance: MutationEpochAdvance) -> MutationEpochState: ...


def advance_mutation_epoch(
    store: MutationEpochStore,
    advance: MutationEpochAdvance,
) -> MutationEpochState:
    """Advance exactly once or fail closed on stale/concurrent authority."""
    state = store.compare_and_swap(advance)
    if (
        state.environment != advance.environment
        or state.namespace != advance.namespace
        or state.epoch != advance.expected_epoch + 1
        or state.mutation_class is not advance.mutation_class
        or state.request_id != advance.request_id
        or state.evidence_sha256 != advance.evidence_sha256
        or state.updated_at != advance.occurred_at
    ):
        raise RuntimeError("mutation epoch store returned a non-authoritative transition")
    return state


__all__ = [
    "MutationEpochAdvance",
    "MutationEpochState",
    "MutationEpochStore",
    "ProtectedMutationClass",
    "advance_mutation_epoch",
]
