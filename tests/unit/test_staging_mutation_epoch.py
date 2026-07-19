from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from loom.staging_mutation_epoch import (
    MutationEpochAdvance,
    MutationEpochState,
    ProtectedMutationClass,
    advance_mutation_epoch,
)

NOW = datetime(2026, 7, 20, tzinfo=UTC)


def _advance(**overrides) -> MutationEpochAdvance:
    values = {
        "environment": "staging",
        "namespace": "loom-staging",
        "expected_epoch": 7,
        "mutation_class": ProtectedMutationClass.LIFECYCLE_GC,
        "request_id": "req-gc0000000",
        "evidence_sha256": "a" * 64,
        "occurred_at": NOW,
    }
    values.update(overrides)
    return MutationEpochAdvance(**values)


class Store:
    def __init__(self, *, drift: bool = False) -> None:
        self.current = 7
        self.drift = drift

    def compare_and_swap(self, advance: MutationEpochAdvance) -> MutationEpochState:
        if advance.expected_epoch != self.current:
            raise RuntimeError("stale mutation epoch")
        self.current += 1
        state = MutationEpochState(
            environment=advance.environment,
            namespace=advance.namespace,
            epoch=self.current,
            mutation_class=advance.mutation_class,
            request_id=advance.request_id,
            evidence_sha256=advance.evidence_sha256,
            updated_at=advance.occurred_at,
        )
        return replace(state, epoch=state.epoch + 1) if self.drift else state


def test_every_protected_mutation_advances_exactly_once() -> None:
    for mutation_class in ProtectedMutationClass:
        store = Store()
        state = advance_mutation_epoch(
            store,
            _advance(mutation_class=mutation_class),
        )
        assert state.epoch == 8
        assert state.mutation_class is mutation_class
        assert len(state.evidence_digest) == 64


def test_stale_epoch_and_non_authoritative_store_result_fail_closed() -> None:
    store = Store()
    store.current = 8
    with pytest.raises(RuntimeError, match="stale"):
        advance_mutation_epoch(store, _advance())

    with pytest.raises(RuntimeError, match="non-authoritative"):
        advance_mutation_epoch(Store(drift=True), _advance())


@pytest.mark.parametrize(
    "changes",
    [
        {"environment": "production"},
        {"namespace": ""},
        {"expected_epoch": -1},
        {"request_id": "invalid"},
        {"evidence_sha256": "invalid"},
        {"occurred_at": NOW.replace(tzinfo=None)},
    ],
)
def test_mutation_authority_rejects_incomplete_or_cross_environment_inputs(
    changes,
) -> None:
    with pytest.raises(ValueError, match="mutation epoch"):
        _advance(**changes)
