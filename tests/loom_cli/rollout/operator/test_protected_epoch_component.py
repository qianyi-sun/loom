from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from loom_cli.rollout.operator.protected_apply_journal import ComponentState
from loom_cli.rollout.operator.protected_epoch_component import (
    KubernetesProtectedEpochComponent,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan


class Runner:
    def __init__(self, record: dict[str, object] | None) -> None:
        self.record = record
        self.calls: list[tuple[str, ...]] = []

    def capture_stdout(self, argv, *, env, timeout_seconds):
        self.calls.append(tuple(argv))
        assert env == {"KUBECONFIG": "/exact"}
        assert timeout_seconds == 30.0
        if any("WITH bootstrapped AS" in item for item in argv):
            raw_variables = {argv[index + 1] for index, value in enumerate(argv) if value == "-v"}
            variables = {
                key: value for key, value in (item.split("=", 1) for item in raw_variables)
            }
            assert variables["request_id"] == "req-alpha"
            self.record = {
                "environment": "staging",
                "namespace": "loom-staging",
                "epoch": int(variables["expected_epoch"]) + 1,
                "mutation_class": "rollout_apply",
                "request_id": variables["request_id"],
                "evidence_sha256": variables["evidence_sha256"],
            }
        return b"" if self.record is None else json.dumps(self.record).encode()

    tmp_path: Path


def _record(epoch: int = 7) -> dict[str, object]:
    return {
        "environment": "staging",
        "namespace": "loom-staging",
        "epoch": epoch,
        "mutation_class": "lifecycle_gc" if epoch == 7 else None,
        "request_id": "req-prior0001" if epoch == 7 else None,
        "evidence_sha256": "f" * 64 if epoch == 7 else None,
    }


def test_epoch_component_classifies_claims_and_recovers_exact_state(tmp_path: Path) -> None:
    runner = Runner(_record())
    runner.tmp_path = tmp_path
    authority = KubernetesProtectedEpochComponent(
        runner=runner,
        environment={"KUBECONFIG": "/exact"},
    )
    plan = _plan(tmp_path)
    component = authority.component(plan)

    assert component.classify(plan).state is ComponentState.READY
    component.apply(plan)
    observation = component.classify(plan)

    assert observation.state is ComponentState.EXACT
    assert observation.observed_epoch == 8
    assert len(runner.calls) == 3


def test_epoch_component_fails_closed_on_concurrent_or_malformed_state(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    concurrent = Runner(
        {
            "environment": "staging",
            "namespace": "loom-staging",
            "epoch": 8,
            "mutation_class": "lifecycle_gc",
            "request_id": "req-other0001",
            "evidence_sha256": "f" * 64,
        }
    )
    concurrent.tmp_path = tmp_path
    authority = KubernetesProtectedEpochComponent(
        runner=concurrent,
        environment={"KUBECONFIG": "/exact"},
    )

    assert authority.classify(plan).state is ComponentState.DRIFTED

    malformed = Runner({"epoch": 7})
    malformed.tmp_path = tmp_path
    with pytest.raises(ValueError, match="authority is incomplete"):
        KubernetesProtectedEpochComponent(
            runner=malformed,
            environment={"KUBECONFIG": "/exact"},
        ).classify(plan)


def test_epoch_component_bootstraps_only_after_legacy_schema_migration(tmp_path: Path) -> None:
    plan = replace(
        _plan(tmp_path),
        schema_revision="0065",
        migration_target_revision="0067",
        starting_mutation_epoch=0,
    )
    runner = Runner(None)
    runner.tmp_path = tmp_path
    authority = KubernetesProtectedEpochComponent(
        runner=runner,
        environment={"KUBECONFIG": "/exact"},
    )

    assert authority.classify(plan).state is ComponentState.READY
    authority.apply(plan)
    assert authority.classify(plan).state is ComponentState.EXACT

    nonlegacy = replace(plan, schema_revision="0066")
    runner.record = None
    assert authority.classify(nonlegacy).state is ComponentState.DRIFTED
