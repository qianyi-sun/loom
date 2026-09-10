"""Candidate-serving readiness must precede protected apply journal success."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from loom_cli.cluster_cmd import render_manifests
from loom_cli.cluster_config import load_cluster_config
from loom_cli.rollout.operator.manifest_apply_contract import MANIFEST_APPLY_CONTRACT_DIGEST
from loom_cli.rollout.operator.protected_application_readiness import application_deployments
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentState,
    ProtectedApplyComponent,
    ProtectedApplyJournal,
    ProtectedApplyJournalError,
)
from tests.loom_cli.rollout.operator.test_protected_manifest_component import Runner, _authority
from tests.loom_cli.rollout.operator.test_protected_migration_component import _published_plan
from tests.support.protected_application_deployments import application_manifest, ready_application


class Clock:
    now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        assert seconds > 0
        self.now += seconds


class ApplicationRunner(Runner):
    def __init__(self, documents):
        super().__init__(status=0)
        self.deployments = {
            doc["metadata"]["name"]: ready_application(doc)
            for doc in documents
            if doc and doc["kind"] == "Deployment"
        }
        self.reads = []
        self.on_read = lambda _name: None
        self.fail_after_apply = False

    def capture_stdout(self, argv, *, env, timeout_seconds):
        assert env == {"KUBECONFIG": "/exact"}
        assert 0 < timeout_seconds <= 30
        assert tuple(argv[:5]) == ("kubectl", "--namespace", "loom-staging", "get", "deployment")
        name = argv[5]
        self.reads.append((name, timeout_seconds))
        self.on_read(name)
        return json.dumps(self.deployments[name]).encode()

    def run_checked(self, *args, **kwargs):
        super().run_checked(*args, **kwargs)
        if self.fail_after_apply:
            raise RuntimeError("lost apply response")


def _case(tmp_path):
    plan = _published_plan(tmp_path)
    documents = list(yaml.safe_load_all(Path(plan.rendered_manifest_path).read_text()))
    runner = ApplicationRunner(documents)
    clock = Clock()
    authority = replace(
        _authority(runner),
        readiness_timeout_seconds=10,
        readiness_interval_seconds=2,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    return plan, runner, clock, authority


def _unready(runner):
    runner.deployments["loom-control-plane"]["status"].update(
        {"replicas": 3, "updatedReplicas": 1, "unavailableReplicas": 1}
    )


def _recover(runner):
    runner.deployments["loom-control-plane"] = ready_application(
        application_manifest("loom-control-plane")
    )


def test_exact_manifest_cannot_hide_failed_replacement_behind_old_ready_pods(
    tmp_path: Path,
) -> None:
    plan, runner, _clock, authority = _case(tmp_path)
    _unready(runner)
    runner.deployments["loom-control-plane"]["status"]["conditions"] = [
        {"type": "Progressing", "status": "False", "reason": "ProgressDeadlineExceeded"}
    ]
    with pytest.raises(RuntimeError, match="readiness progress deadline"):
        authority.classify(plan)
    assert runner.reads
    assert all("apply" not in argv for argv, _payload in runner.calls)


def test_pending_replacement_expires_without_reapply(tmp_path: Path) -> None:
    plan, runner, clock, authority = _case(tmp_path)
    _unready(runner)
    with pytest.raises(RuntimeError, match="readiness timed out"):
        authority.classify(plan)
    assert clock.now == 10
    assert all("apply" not in argv for argv, _payload in runner.calls)


def test_all_deployment_gets_share_one_deadline(tmp_path: Path) -> None:
    plan, runner, clock, authority = _case(tmp_path)

    def advance(_name):
        clock.sleep(min(3, runner.reads[-1][1]))

    runner.on_read = advance
    with pytest.raises(RuntimeError, match="readiness timed out"):
        authority.classify(plan)
    assert clock.now == 10
    assert [timeout for _name, timeout in runner.reads] == [10, 7, 4, 1]


def test_eventual_current_generation_readiness_and_stable_evidence(tmp_path: Path) -> None:
    plan, runner, clock, authority = _case(tmp_path)
    _unready(runner)
    runner.on_read = lambda _name: _recover(runner) if clock.now >= 4 else None
    first = authority.classify(plan)
    assert first.state is ComponentState.EXACT
    assert clock.now == 4
    runner.deployments["loom-web"]["metadata"]["resourceVersion"] = "999"
    assert authority.classify(plan) == first


@pytest.mark.parametrize(
    "mutation",
    [
        "stale-generation",
        "terminating",
        "unavailable",
        "not-updated",
        "missing-status",
        "wrong-image",
        "extra-container",
        "wrong-name",
        "wrong-namespace",
        "deleted",
        "bool-generation",
        "bool-replicas",
        "negative-ready",
        "null-ready",
        "malformed-conditions",
    ],
)
def test_deployment_drift_or_unready_blocks(tmp_path: Path, mutation: str) -> None:
    plan, runner, _clock, authority = _case(tmp_path)
    live = runner.deployments["loom-control-plane"]
    if mutation == "stale-generation":
        live["status"]["observedGeneration"] = 87
    elif mutation in {"terminating", "unavailable"}:
        live["status"][mutation + "Replicas"] = 1
    elif mutation == "not-updated":
        live["status"]["updatedReplicas"] = 1
    elif mutation == "missing-status":
        live.pop("status")
    elif mutation == "wrong-image":
        live["spec"]["template"]["spec"]["containers"][0]["image"] = "old:latest"
    elif mutation == "extra-container":
        live["spec"]["template"]["spec"]["containers"].append({"name": "extra"})
    elif mutation in {"wrong-name", "wrong-namespace"}:
        live["metadata"][mutation.removeprefix("wrong-")] = "foreign"
    elif mutation == "deleted":
        live["metadata"]["deletionTimestamp"] = "2026-09-09T00:01:00Z"
    elif mutation == "bool-generation":
        live["metadata"]["generation"] = True
    elif mutation == "bool-replicas":
        live["spec"]["replicas"] = True
    elif mutation == "negative-ready":
        live["status"]["readyReplicas"] = -1
    elif mutation == "null-ready":
        live["status"]["readyReplicas"] = None
    else:
        live["status"]["conditions"] = [False]
    with pytest.raises(RuntimeError, match="readiness"):
        authority.classify(plan)


def test_server_added_defaults_are_allowed(tmp_path: Path) -> None:
    plan, runner, _clock, authority = _case(tmp_path)
    template = runner.deployments["loom-control-plane"]["spec"]["template"]
    template["spec"]["dnsPolicy"] = "ClusterFirst"
    template["spec"]["containers"][0]["terminationMessagePolicy"] = "File"
    template["metadata"]["creationTimestamp"] = None
    assert authority.classify(plan).state is ComponentState.EXACT


def test_api_failure_blocks_without_mutation(tmp_path: Path) -> None:
    plan, runner, _clock, authority = _case(tmp_path)

    def unavailable(_name):
        raise RuntimeError("bounded API unavailable")

    runner.on_read = unavailable
    with pytest.raises(RuntimeError, match="API unavailable"):
        authority.classify(plan)
    assert all("apply" not in argv for argv, _payload in runner.calls)


@pytest.mark.parametrize("drift", ["epoch", "manifest", "diff-error"])
def test_post_wait_authority_is_rechecked(tmp_path: Path, drift: str) -> None:
    plan, runner, _clock, authority = _case(tmp_path)
    if drift == "epoch":
        initial = authority.epoch_guard(plan)
        authority = replace(
            authority,
            epoch_guard=lambda _plan: (
                replace(initial, observed_epoch=initial.observed_epoch + 1)
                if runner.reads
                else initial
            ),
        )
    else:

        def change(_name):
            runner.status = 1 if drift == "manifest" else 2

        runner.on_read = change
    with pytest.raises(RuntimeError, match=r"readiness .* changed"):
        authority.classify(plan)


@pytest.mark.parametrize("status", [2, -1])
def test_diff_failure_is_not_apply_permission(tmp_path: Path, status: int) -> None:
    plan, runner, _clock, authority = _case(tmp_path)
    runner.status = status
    with pytest.raises(RuntimeError, match="manifest diff failed"):
        authority.classify(plan)
    assert not runner.reads


def test_zero_replica_deployment_must_be_observed_and_empty(tmp_path: Path) -> None:
    plan, runner, _clock, authority = _case(tmp_path)
    path = Path(plan.rendered_manifest_path)
    documents = list(yaml.safe_load_all(path.read_text()))
    zero = application_manifest("loom-pipeline-orchestrator", 0)
    documents.append(zero)
    payload = yaml.safe_dump_all(documents).encode()
    path.write_bytes(payload)
    plan = replace(plan, rendered_manifest_sha256=hashlib.sha256(payload).hexdigest())
    runner.deployments[zero["metadata"]["name"]] = ready_application(zero)
    assert authority.classify(plan).state is ComponentState.EXACT
    runner.deployments[zero["metadata"]["name"]]["status"]["replicas"] = 1
    with pytest.raises(RuntimeError, match="readiness timed out"):
        authority.classify(plan)


@pytest.mark.parametrize("mutation", ["missing-core", "zero-core", "duplicate", "foreign"])
def test_manifest_cannot_make_readiness_vacuous(tmp_path: Path, mutation: str) -> None:
    plan, _runner, _clock, _unused_authority = _case(tmp_path)
    documents = list(yaml.safe_load_all(Path(plan.rendered_manifest_path).read_text()))
    cp = next(doc for doc in documents if doc["metadata"]["name"] == "loom-control-plane")
    if mutation == "missing-core":
        documents.remove(cp)
    elif mutation == "zero-core":
        cp["spec"]["replicas"] = 0
    elif mutation == "duplicate":
        documents.append(cp)
    else:
        cp["metadata"]["namespace"] = "foreign"
    with pytest.raises(ValueError, match="readiness"):
        application_deployments(yaml.safe_dump_all(documents).encode(), "loom-staging")


@pytest.mark.parametrize("interruption", ["timeout", "lost-response"])
def test_journal_resume_waits_without_reapplying_or_starting_downstream_early(
    tmp_path: Path, interruption: str
) -> None:
    plan, runner, _clock, authority = _case(tmp_path)
    state = tmp_path / "journal"
    (state / f"requests/{plan.request_id}/attempts/{plan.attempt_number}").mkdir(
        parents=True, mode=0o700
    )
    journal = ProtectedApplyJournal(
        state,
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
        service_uid=os.geteuid(),
    )
    activated = []
    exact = authority.epoch_guard(plan)
    downstream = ProtectedApplyComponent(
        component_id="downstream-workers",
        implementation_digest="d" * 64,
        input_fingerprint="f" * 64,
        classify=lambda _plan: exact if activated else replace(exact, state=ComponentState.READY),
        apply=lambda _plan: activated.append(True),
    )
    components = (authority.component(plan), downstream)
    runner.status = 1
    _unready(runner)
    runner.fail_after_apply = interruption == "lost-response"
    with pytest.raises(RuntimeError, match=r"timed out|lost apply response"):
        journal.execute(plan, components)
    assert not activated
    assert sum("apply" in argv for argv, _payload in runner.calls) == 1
    _recover(runner)
    runner.fail_after_apply = False
    first = journal.execute(plan, components)
    assert activated == [True]
    assert sum("apply" in argv for argv, _payload in runner.calls) == 1
    _unready(runner)
    with pytest.raises(RuntimeError, match="readiness timed out"):
        journal.execute(plan, components)
    _recover(runner)
    assert journal.execute(plan, components) == first
    assert activated == [True]


def test_epoch_change_during_final_diff_is_rejected(tmp_path: Path) -> None:
    plan, runner, _clock, authority = _case(tmp_path)
    initial = authority.epoch_guard(plan)
    authority = replace(
        authority,
        epoch_guard=lambda _plan: (
            replace(initial, observed_epoch=initial.observed_epoch + 1)
            if len(runner.calls) > 1
            else initial
        ),
    )
    with pytest.raises(RuntimeError, match="epoch changed during final diff"):
        authority.classify(plan)


def test_old_empty_diff_terminal_cannot_be_reused_as_readiness_proof(tmp_path: Path) -> None:
    plan, runner, _clock, authority = _case(tmp_path)
    state = tmp_path / "journal"
    (state / f"requests/{plan.request_id}/attempts/{plan.attempt_number}").mkdir(
        parents=True, mode=0o700
    )
    journal = ProtectedApplyJournal(
        state,
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
        service_uid=os.geteuid(),
    )
    component = authority.component(plan)
    old = replace(
        component,
        implementation_digest=hashlib.sha256(
            f"protected-manifest-component-v3|{MANIFEST_APPLY_CONTRACT_DIGEST}".encode()
        ).hexdigest(),
    )
    assert old.implementation_digest != component.implementation_digest
    journal.execute(plan, (old,))
    runner.reads.clear()
    with pytest.raises(ProtectedApplyJournalError, match="record cannot be replaced"):
        journal.execute(plan, (component,))
    assert not runner.reads


@pytest.mark.parametrize("changed_quantity", [False, True])
def test_actual_staging_render_accepts_only_equivalent_api_resource_quantities(
    tmp_path: Path, changed_quantity: bool
) -> None:
    plan, _runner, clock, authority = _case(tmp_path)
    root = Path(__file__).resolve().parents[4]
    rendered = render_manifests(
        load_cluster_config(root / "deploy/environments/staging.cluster.toml")
    )
    path = Path(plan.rendered_manifest_path)
    path.write_text(rendered)
    plan = replace(plan, rendered_manifest_sha256=hashlib.sha256(rendered.encode()).hexdigest())
    runner = ApplicationRunner(list(yaml.safe_load_all(rendered)))
    converted = []
    for name, live in runner.deployments.items():
        pod = live["spec"]["template"]["spec"]
        for container in [*pod["containers"], *pod.get("initContainers", [])]:
            for quantities in container.get("resources", {}).values():
                for key, value in quantities.items():
                    if type(value) is int:
                        converted.append((name, key))
                        quantities[key] = str(value)
    assert ("loom-control-plane", "cpu") in converted
    cp = runner.deployments["loom-control-plane"]["spec"]["template"]["spec"]["containers"][0]
    cp["resources"]["limits"]["cpu"] = "2000m" if changed_quantity else "1000m"
    cp["resources"]["limits"]["memory"] = "1024Mi"
    authority = replace(authority, runner=runner, monotonic=clock.monotonic)
    if changed_quantity:
        with pytest.raises(RuntimeError, match="identity drifted"):
            authority.classify(plan)
    else:
        assert authority.classify(plan).state is ComponentState.EXACT
