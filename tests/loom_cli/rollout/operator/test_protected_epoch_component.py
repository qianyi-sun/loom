from __future__ import annotations

import json
import re
from dataclasses import replace
from pathlib import Path

import pytest

from loom_cli.rollout.operator.protected_apply_journal import ComponentState
from loom_cli.rollout.operator.protected_epoch_component import (
    KubernetesProtectedEpochComponent,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan as _base_plan


def _plan(tmp_path: Path):  # type: ignore[no-untyped-def]
    return replace(
        _base_plan(tmp_path),
        schema_revision="0068",
        migration_target_revision="0071",
    )


class Runner:
    def __init__(self, record: dict[str, object] | None) -> None:
        self.record = record
        self.calls: list[tuple[str, ...]] = []

    def capture_stdout(self, argv, *, env, timeout_seconds):
        self.calls.append(tuple(argv))
        assert all("\n" not in item for item in argv)
        assert env == {"KUBECONFIG": "/exact"}
        assert timeout_seconds == 30.0
        if any("WITH bootstrapped AS" in item for item in argv):
            sql = next(item for item in argv if "WITH bootstrapped AS" in item)
            request_id = re.search(r"request_id = '([^']+)'", sql)
            evidence = re.search(r"evidence_sha256 = '([0-9a-f]+)'", sql)
            epoch = re.search(r"AND epoch = ([0-9]+)::bigint", sql)
            assert request_id is not None
            assert evidence is not None
            assert epoch is not None
            assert request_id.group(1) == "req-alpha"
            assert ":'" not in sql
            assert "-v" not in argv
            self.record = {
                "environment": "staging",
                "namespace": "loom-staging",
                "epoch": int(epoch.group(1)) + 1,
                "mutation_class": "rollout_apply",
                "request_id": request_id.group(1),
                "evidence_sha256": evidence.group(1),
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
        migration_target_revision="0071",
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

    nonlegacy = replace(plan, schema_revision="0068")
    runner.record = None
    assert authority.classify(nonlegacy).state is ComponentState.DRIFTED
