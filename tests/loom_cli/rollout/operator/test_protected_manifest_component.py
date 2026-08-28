from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.operator.manifest_apply_contract import server_side_apply_argv
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
)
from loom_cli.rollout.operator.protected_manifest_component import (
    KubernetesProtectedManifestComponent,
)
from tests.loom_cli.rollout.operator.test_protected_migration_component import (
    _published_plan,
)


class Runner:
    def __init__(self, status: int = 1) -> None:
        self.status = status
        self.calls: list[tuple[tuple[str, ...], bytes | None]] = []

    def run_status(self, argv, *, env, input_payload, timeout_seconds):
        assert env == {"KUBECONFIG": "/exact"}
        assert timeout_seconds == 120.0
        assert "diff" in argv
        self.calls.append((tuple(argv), input_payload))
        return self.status

    def run_checked(self, argv, *, env, input_payload, timeout_seconds):
        assert env == {"KUBECONFIG": "/exact"}
        assert timeout_seconds == 300.0
        assert "apply" in argv
        assert "--force-conflicts" not in argv
        self.calls.append((tuple(argv), input_payload))
        self.status = 0


def _epoch(state: ComponentState = ComponentState.EXACT):
    def classify(plan):
        return ComponentObservation(
            state=state,
            evidence_digest="e" * 64,
            observed_epoch=plan.starting_mutation_epoch + 1,
        )

    return classify


def _authority(runner: Runner, *, epoch_state: ComponentState = ComponentState.EXACT):
    return KubernetesProtectedManifestComponent(
        runner=runner,
        environment={"KUBECONFIG": "/exact"},
        service_uid=os.geteuid(),
        epoch_guard=_epoch(epoch_state),
    )


def test_manifest_component_converges_guard_held_published_artifact(tmp_path: Path) -> None:
    plan = _published_plan(tmp_path)
    runner = Runner()
    authority = _authority(runner)
    original = Path(plan.rendered_manifest_path).read_bytes()
    expected_documents = list(yaml.safe_load_all(original))
    expected_documents[0]["spec"]["suspend"] = True

    assert authority.classify(plan).state is ComponentState.READY
    authority.apply(plan)
    exact = authority.classify(plan)

    assert exact.state is ComponentState.EXACT
    assert exact.observed_epoch == plan.starting_mutation_epoch + 1
    assert Path(plan.rendered_manifest_path).read_bytes() == original
    for _argv, payload in runner.calls:
        assert payload is not None
        assert list(yaml.safe_load_all(payload)) == expected_documents
    apply_argv = runner.calls[2][0]
    assert apply_argv == server_side_apply_argv(plan.namespace)
    assert "--server-side=true" in apply_argv
    assert "--field-manager=loom-staging-rollout" in apply_argv
    assert "--force-conflicts" not in apply_argv


def test_manifest_component_refuses_epoch_or_artifact_drift(tmp_path: Path) -> None:
    plan = _published_plan(tmp_path)
    runner = Runner()
    assert _authority(runner, epoch_state=ComponentState.READY).classify(plan).state is (
        ComponentState.DRIFTED
    )
    assert runner.calls == []

    Path(plan.rendered_manifest_path).write_text("changed\n")
    with pytest.raises(ValueError, match="content drifted"):
        _authority(runner).classify(plan)


def test_manifest_component_does_not_reapply_exact_state(tmp_path: Path) -> None:
    plan = _published_plan(tmp_path)
    runner = Runner(status=0)
    authority = _authority(runner)

    assert authority.classify(plan).state is ComponentState.EXACT
    with pytest.raises(RuntimeError, match="state changed before apply"):
        authority.apply(plan)
    assert all("apply" not in argv for argv, _payload in runner.calls)
