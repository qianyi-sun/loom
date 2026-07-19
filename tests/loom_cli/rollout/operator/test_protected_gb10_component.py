from __future__ import annotations

from dataclasses import replace

import pytest

from loom_cli.rollout.gb10_convergence import (
    GB10ConvergenceState,
    GB10FleetCandidateObservation,
    GB10HostCandidateObservation,
)
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
)
from loom_cli.rollout.operator.protected_gb10_component import (
    ProtectedGB10CandidateComponent,
)
from tests.loom_cli.rollout.operator.test_protected_migration_component import _published_plan


class Fleet:
    def __init__(self, *, exact: bool) -> None:
        self.exact = exact
        self.applied = []

    def observe(self, plan):
        return GB10FleetCandidateObservation(
            hosts={
                "trt-gb10-1": GB10HostCandidateObservation(
                    host="trt-gb10-1",
                    boot_id="boot-1",
                    baseline_ready=True,
                    candidate_source_exact=True,
                    checkout_exact=self.exact,
                    environment_exact=self.exact,
                    units_exact=self.exact,
                    legacy_absent=self.exact,
                    service_timer_exact=self.exact,
                    evidence_digest="1" * 64,
                )
            },
            candidate_source_digest=plan.gb10_unit_digest,
        )

    def apply(self, plan, convergence):
        assert convergence.state is GB10ConvergenceState.READY
        self.applied.append((plan.candidate_sha, convergence.evidence_digest))
        self.exact = True


def _plan(tmp_path):
    return replace(
        _published_plan(tmp_path),
        gb10_boot_ids={"trt-gb10-1": "boot-1"},
    )


def _epoch(plan, *, exact: bool = True):
    return ComponentObservation(
        state=ComponentState.EXACT if exact else ComponentState.DRIFTED,
        evidence_digest="2" * 64,
        observed_epoch=plan.starting_mutation_epoch + (1 if exact else 0),
    )


def test_component_reuses_classifier_for_apply_and_verify(tmp_path) -> None:
    plan = _plan(tmp_path)
    fleet = Fleet(exact=False)
    component = ProtectedGB10CandidateComponent(
        transport=fleet,
        epoch_guard=lambda current: _epoch(current),
    )

    before = component.classify(plan)
    assert before.state is ComponentState.READY
    component.apply(plan)
    after = component.classify(plan)

    assert after.state is ComponentState.EXACT
    assert len(fleet.applied) == 1
    assert component.component(plan).component_id == "gb10-candidate"


def test_component_fails_closed_on_epoch_or_boot_drift(tmp_path) -> None:
    plan = _plan(tmp_path)
    fleet = Fleet(exact=False)
    blocked = ProtectedGB10CandidateComponent(
        transport=fleet,
        epoch_guard=lambda current: _epoch(current, exact=False),
    )

    assert blocked.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="epoch ownership changed"):
        blocked.apply(plan)

    drifted = replace(plan, gb10_boot_ids={"trt-gb10-1": "different-boot"})
    component = ProtectedGB10CandidateComponent(
        transport=fleet,
        epoch_guard=lambda current: _epoch(current),
    )
    assert component.classify(drifted).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed"):
        component.apply(drifted)
