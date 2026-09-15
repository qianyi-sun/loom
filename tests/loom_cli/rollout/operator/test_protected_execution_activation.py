"""Activation ordering, lost replies and bounded failure recovery on real journals."""

import hashlib
import os
from dataclasses import replace
from pathlib import Path

import pytest

from loom_capacity_executor.runtime import ApprovedLaunchProfileSetV2
from loom_capacity_manager.contracts import canonical_digest
from loom_capacity_manager.executable_contracts import ExecutionAuthorityV2
from loom_cli.capacity_control_plane import (
    render_capacity_pool_executor_active_manifest_sha256,
    render_capacity_pool_executor_configs,
    render_capacity_pool_executor_service_environment,
    render_capacity_pool_inventory_policies,
)
from loom_cli.rollout.operator.protected_active_controller import ActiveControllerEvidence
from loom_cli.rollout.operator.protected_capacity_execution_preparation_component import (
    prepared_executor_profile_sha256,
)
from loom_cli.rollout.operator.protected_execution_activation import (
    ExecutionActivationJournal,
    ProtectedExecutionActivation,
)
from loom_cli.rollout.operator.protected_execution_prerequisites import (
    CapacityPoolExecutorProfileSeed,
)
from tests.capacity_fixtures import subject_configuration
from tests.loom_cli.rollout.operator.protected_execution_prerequisite_fixtures import _policy
from tests.loom_cli.rollout.operator.test_controller_admission import _bundle
from tests.loom_cli.rollout.operator.test_protected_active_controller import _request
from tests.loom_cli.rollout.operator.test_protected_capacity_execution_preparation_component import (
    _controller_evidence,
    _Manager,
)
from tests.loom_cli.rollout.operator.test_protected_controller_prerequisite_component import (
    _plan_and_artifact,
)
from tests.ops.test_install_capacity_executor import _controller_request


class Controller:
    authority_sha256 = "8" * 64
    def __init__(self, pool, calls):
        self.pool, self.calls, self.state = pool, calls, None
        self.fail = False
    def disable_timer(self, request):
        self.calls.append((self.pool, "stop"))
        return _controller_evidence(request, timer=False, tick=False)
    def observe(self, request):
        if self.state is None:
            return None
        base = _controller_evidence(request.prepared, timer=False, tick=False)
        active, files = dict(base.unit_active_state), dict(base.unit_file_state)
        if self.state == "active":
            active["loom-capacity-pool-executor-active.timer"] = "active"
            files["loom-capacity-pool-executor-active.timer"] = "enabled"
        return ActiveControllerEvidence(request.operation_id, request.pool_id, request.request_sha256,
            request.transport_authority_sha256, {path: hashlib.sha256(data).hexdigest() for path, data in request.files.items()}, active, files)
    def converge_files(self, request):
        self.calls.append((self.pool, "stage"))
        self.state = "staged"
        return self.observe(request)
    def refresh_preparation(self, request):
        self.calls.append((self.pool, "refresh"))
        return self.observe(request)
    def enable_timer(self, request):
        self.calls.append((self.pool, "enable"))
        if self.fail:
            raise RuntimeError("controller enable failed")
        self.state = "active"
        return self.observe(request)


class Manager(_Manager):
    def get_execution_preparation_status(self):
        state = (self.execution, self.expired_final_lease, frozenset(self.ready_pools))
        if getattr(self, "_readiness_state", None) != state:
            self._readiness_state = state
            self._readiness = super().get_execution_preparation_status()
        return self._readiness

    def abort_execution_preparation(self, request, key):
        if self.abort_calls:
            assert self.abort_calls[0] == (request, key)
            self.abort_calls.append((request, key))
            return self._abort_result
        self.calls.append(("manager", "abort"))
        self._abort_result = super().abort_execution_preparation(request, key)
        return self._abort_result

    def activate_execution(self, request, key):
        assert request.executable_new_capacity_ceiling == self.artifact.execution_policy.executable_new_capacity_ceiling
        assert request.executable_new_capacity_rate_per_minute == self.artifact.execution_policy.executable_new_capacity_rate_per_minute
        self.calls.append(("manager", "activate"))
        value = (request, key)
        if self.activations:
            assert self.activations[0] == value
        self.activations.append(value)
        if self.execution.execution_state == "prepared":
            status = self.get_execution_preparation_status()
            assert not self.expired_final_lease and request.prepared_readiness_sha256 == status.readiness_sha256
            assert self.calls.index(("gb10", "refresh")) < len(self.calls) - 1
            assert self.calls.index(("oldlab", "refresh")) < len(self.calls) - 1
        self.execution = self.expected
        if self.lose_reply and len(self.activations) == 1:
            raise RuntimeError("lost activation reply")
        return ExecutionAuthorityV2.model_validate_json(self.execution.model_dump_json())
    def drain_execution(self, request, key):
        self.calls.append(("manager", "drain"))
        self.execution = self.expected.model_copy(update={"execution_state": "drain-only",
            "executable_new_capacity_ceiling": 0, "executable_new_capacity_rate_per_minute": 0})
        return ExecutionAuthorityV2.model_validate_json(self.execution.model_dump_json())


def fixture(tmp_path, *, policy_ceiling=1, policy_rate=1):
    requests, controls, calls = {}, {}, []
    ca = None
    for pool in ("gb10", "oldlab"):
        path = tmp_path / pool
        path.mkdir()
        request = _request(path, prerequisite=_controller_request(path, pool))
        admission = _bundle(request.document)
        if ca is None:
            ca = admission.ca_certificate
        admission = replace(admission, ca_certificate=ca)
        request = replace(request, admission=admission,
            document=request.document.model_copy(update={"admission_directory_sha256": admission.directory_sha256}))
        requests[pool] = request
        controls[pool] = Controller(pool, calls)
    profile = requests["gb10"].profile.model_copy(update={
        "pools": tuple(requests[pool].prepared.prerequisite.binding for pool in ("gb10", "oldlab"))})
    entry = requests["gb10"].admission.entry
    subject = subject_configuration().model_copy(update={
        "subject_id": entry.subject_id, "subject_incarnation": entry.subject_incarnation,
        "tier_id": "staging", "configuration_generation": entry.configuration_generation,
        "deployment_generation": entry.deployment_generation, "candidate_generation": entry.candidate_generation})
    def compose(original):
        seed = CapacityPoolExecutorProfileSeed.from_profile(profile)
        policy = _policy(seed, core_bundle_sha256=original.core_artifact_bundle_sha256).model_copy(update={
            "executable_new_capacity_ceiling": policy_ceiling,
            "executable_new_capacity_rate_per_minute": policy_rate})
        ack = policy.subject_acknowledgements[0].model_copy(update={
            "subject_id": subject.subject_id, "subject_incarnation": subject.subject_incarnation,
            "configuration_generation": subject.configuration_generation,
            "deployment_generation": subject.deployment_generation,
            "reporter_incarnation": subject.demand_reporter_incarnation,
            "protected_admission_sha256": entry.protected_admission_sha256})
        return replace(original, executor_profile_seed=seed,
            execution_policy=policy.model_copy(update={"subject_acknowledgements": (ack,)}),
            staging_subject_id=subject.subject_id,
            desired_subject_sha256={str(subject.subject_id): canonical_digest(subject)},
            subject_protected_admission_sha256={str(subject.subject_id): entry.protected_admission_sha256})
    plan, artifact, *_ = _plan_and_artifact(tmp_path, transform=compose)
    for pool, request in requests.items():
        prerequisite = replace(request.prepared.prerequisite, source_sha=plan.candidate_sha,
            credential_metadata_sha256={key: value for key, value in artifact.credential_metadata_sha256.items()
                if key in {f"pool-executor-{pool}", f"pool-ownership-{pool}"}})
        path = Path(prerequisite.binding.config_file)
        prepared = replace(request.prepared, prerequisite=prerequisite,
            profile_sha256=prepared_executor_profile_sha256(profile), files={
                str(path): render_capacity_pool_executor_configs(profile)[pool].encode(),
                str(path.with_name(f"{pool}-inventory-policy.json")): render_capacity_pool_inventory_policies(profile)[pool].encode(),
                "/etc/loom-capacity-executor/service.env": render_capacity_pool_executor_service_environment(profile, pool).encode()})
        document = request.document.model_copy(update={
            "immutable_manifest_sha256": render_capacity_pool_executor_active_manifest_sha256(
                profile, pool, ApprovedLaunchProfileSetV2(profiles=request.document.profiles))})
        requests[pool] = replace(request, profile=profile, prepared=prepared, document=document)
    manager = Manager(artifact)
    manager.execution = requests["gb10"].prepared.execution
    manager.expected = requests["gb10"].document.execution
    manager.writer_epoch = manager.expected.writer_epoch
    manager.ready_pools = {"gb10", "oldlab"}
    manager.calls, manager.activations, manager.lose_reply = calls, [], False
    state = tmp_path / "activation-state"
    state.mkdir(mode=0o700)
    journal = ExecutionActivationJournal(state, plan.request_id, plan.attempt_number, os.geteuid())
    return ProtectedExecutionActivation(plan, artifact, requests, journal, manager, controls, controls, lambda: None, subject), manager, controls, calls


def test_activation_stages_both_pools_before_manager_and_enables_after(tmp_path):
    owner, manager, _controls, calls = fixture(tmp_path)
    assert owner.execute() == manager.expected
    assert calls == [("gb10", "stop"), ("gb10", "stage"), ("oldlab", "stop"), ("oldlab", "stage"),
        ("gb10", "refresh"), ("oldlab", "refresh"), ("manager", "activate"), ("gb10", "enable"), ("oldlab", "enable")]
    assert owner.execute() == manager.expected
    assert calls.count(("gb10", "stage")) == 1
    assert owner.journal.read("activation.terminal.json") is not None


@pytest.mark.parametrize("limits", [{"policy_ceiling": 2}, {"policy_ceiling": 158}, {"policy_rate": 2}])
def test_activation_refuses_policy_capacity_mismatch_before_effects(tmp_path, limits):
    with pytest.raises(ValueError, match="activation policy capacity"):
        fixture(tmp_path, **limits)


def test_lost_activation_reply_reuses_exact_retained_readiness_and_key(tmp_path):
    owner, manager, controls, _calls = fixture(tmp_path)
    manager.lose_reply = True
    with pytest.raises(RuntimeError, match="lost activation"):
        owner.execute()
    assert all(control.state == "staged" for control in controls.values())
    assert owner.execute() == manager.expected
    assert len(manager.activations) == 2 and manager.activations[0] == manager.activations[1]


def test_one_pool_failure_retains_drain_and_never_reactivates(tmp_path):
    owner, _manager, controls, calls = fixture(tmp_path)
    controls["oldlab"].fail = True
    with pytest.raises(RuntimeError, match="controller enable failed"):
        owner.execute()
    assert owner.journal.read("drain.intent.json") is not None
    assert owner.execute().execution_state == "drain-only"
    assert calls.count(("manager", "activate")) == 1


def test_stale_readiness_does_not_open_manager_capacity(tmp_path):
    owner, manager, controls, calls = fixture(tmp_path)
    manager.expired_final_lease = True
    with pytest.raises(RuntimeError, match="fresh prepared readiness"):
        owner.execute()
    assert ("manager", "activate") not in calls
    assert all(control.state == "staged" for control in controls.values())


def test_changed_retained_inputs_refuse_before_new_effect(tmp_path):
    owner, _manager, _controls, calls = fixture(tmp_path)
    owner.execute()
    before = list(calls)
    requests = dict(owner.requests)
    request = requests["gb10"]
    from uuid import uuid4
    requests["gb10"] = replace(request, operation_id=uuid4())
    with pytest.raises(RuntimeError, match="drifted"):
        replace(owner, requests=requests).execute()
    assert calls == before


def test_partial_controller_publication_replay_does_not_reenter_prepared_channel(tmp_path):
    owner, manager, controls, _calls = fixture(tmp_path)
    original = controls["oldlab"].converge_files
    def interrupted(request):
        original(request)
        raise RuntimeError("interrupted file publication")
    controls["oldlab"].converge_files = interrupted
    with pytest.raises(RuntimeError, match="interrupted file"):
        owner.execute()
    def forbidden(request):
        raise RuntimeError("prepared channel rejects retained active operation")
    for control in controls.values():
        control.disable_timer = forbidden
    controls["oldlab"].converge_files = original
    assert owner.execute() == manager.expected


def test_forward_dependency_loss_drains_and_replays_without_forward_guard(tmp_path):
    owner, manager, controls, calls = fixture(tmp_path)
    def guard():
        if controls["gb10"].state == "active":
            raise RuntimeError("forward dependency lost")
    owner = replace(owner, dependency_guard=guard)
    with pytest.raises(RuntimeError, match="forward dependency lost"):
        owner.execute()
    assert manager.execution.execution_state == "drain-only"
    assert manager.execution.executable_new_capacity_ceiling == 0
    assert owner.execute().execution_state == "drain-only"
    assert calls.count(("manager", "activate")) == 1


def test_lost_drain_reply_retains_same_request_and_key(tmp_path):
    owner, manager, controls, _calls = fixture(tmp_path)
    controls["oldlab"].fail = True
    original = manager.drain_execution
    drains = []
    def interrupted(request, key):
        drains.append((request, key))
        result = original(request, key)
        if len(drains) == 1:
            raise RuntimeError("lost drain reply")
        return result
    manager.drain_execution = interrupted
    with pytest.raises(RuntimeError, match="lost drain reply"):
        owner.execute()
    def forbidden():
        raise RuntimeError("forward dependency lost")
    owner = replace(owner, dependency_guard=forbidden)
    assert owner.execute().execution_state == "drain-only"
    assert len(drains) == 2 and drains[0] == drains[1]


@pytest.mark.parametrize("failure", ["terminal", "response"])
def test_failure_after_activation_reply_compensates(tmp_path, monkeypatch, failure):
    owner, manager, controls, _calls = fixture(tmp_path)
    if failure == "terminal":
        original = ExecutionActivationJournal.retain
        def fail_record(self, name, value):
            if name == "manager.terminal.json":
                raise RuntimeError("terminal publication failed")
            return original(self, name, value)
        monkeypatch.setattr(ExecutionActivationJournal, "retain", fail_record)
    else:
        original = manager.activate_execution
        def wrong_response(request, key):
            result = original(request, key)
            return result.model_copy(update={"writer_epoch": result.writer_epoch + 1})
        manager.activate_execution = wrong_response
    with pytest.raises(RuntimeError):
        owner.execute()
    assert manager.execution.execution_state == "drain-only"
    assert all(control.state == "staged" for control in controls.values())


@pytest.mark.parametrize("drift", ["candidate", "credential", "admission", "deployment", "generation"])
def test_controller_request_must_join_prerequisite_authority(tmp_path, drift):
    owner, _, _, calls = fixture(tmp_path)
    request = owner.requests["gb10"]
    if drift in {"candidate", "credential"}:
        prerequisite = request.prepared.prerequisite
        if drift == "candidate":
            prerequisite = replace(prerequisite, source_sha="f" * 40)
        else:
            credentials = dict(prerequisite.credential_metadata_sha256)
            credentials["pool-executor-gb10"] = "f" * 64
            prerequisite = replace(prerequisite, credential_metadata_sha256=credentials)
        request = replace(request, prepared=replace(request.prepared, prerequisite=prerequisite))
    else:
        field = {"admission": "protected_admission_sha256", "deployment": "deployment_generation", "generation": "candidate_generation"}[drift]
        value = "f" * 64 if drift == "admission" else getattr(request.admission.entry, field) + 1
        admission = replace(request.admission, entry=request.admission.entry.model_copy(update={field: value}))
        request = replace(request, admission=admission,
            document=request.document.model_copy(update={"admission_directory_sha256": admission.directory_sha256}))
    with pytest.raises(ValueError, match="prerequisite authority"):
        replace(owner, requests={**owner.requests, "gb10": request})
    assert calls == []


def test_profile_must_join_prerequisite_seed(tmp_path):
    owner, _, _, calls = fixture(tmp_path)
    requests = {}
    for pool, request in owner.requests.items():
        profile = request.profile.model_copy(update={"executor_image": request.profile.executor_image[:-64] + "f" * 64})
        prerequisite = replace(request.prepared.prerequisite, image=profile.executor_image)
        path = Path(prerequisite.binding.config_file)
        prepared = replace(request.prepared, prerequisite=prerequisite,
            profile_sha256=prepared_executor_profile_sha256(profile), files={
                str(path): render_capacity_pool_executor_configs(profile)[pool].encode(),
                str(path.with_name(f"{pool}-inventory-policy.json")): render_capacity_pool_inventory_policies(profile)[pool].encode(),
                "/etc/loom-capacity-executor/service.env": render_capacity_pool_executor_service_environment(profile, pool).encode()})
        document = request.document.model_copy(update={
            "immutable_manifest_sha256": render_capacity_pool_executor_active_manifest_sha256(
                profile, pool, ApprovedLaunchProfileSetV2(profiles=request.document.profiles))})
        requests[pool] = replace(request, profile=profile, prepared=prepared, document=document)
    with pytest.raises(ValueError, match="prerequisite authority"):
        replace(owner, requests=requests)
    assert calls == []


def test_controller_transport_must_join_saved_provenance(tmp_path):
    owner, _, controls, calls = fixture(tmp_path)
    controls["gb10"].authority_sha256 = "f" * 64
    with pytest.raises(ValueError, match="prerequisite authority"):
        replace(owner)
    assert calls == []


def test_lost_activation_reply_then_dependency_loss_drains(tmp_path):
    owner, manager, _, calls = fixture(tmp_path)
    manager.lose_reply = True
    with pytest.raises(RuntimeError, match="lost activation reply"):
        owner.execute()
    def failed():
        raise RuntimeError("forward dependency lost")
    owner = replace(owner, dependency_guard=failed)
    with pytest.raises(RuntimeError, match="forward dependency lost"):
        owner.execute()
    assert manager.execution.execution_state == "drain-only"
    assert owner.execute().execution_state == "drain-only"
    assert calls.count(("manager", "activate")) == 1


@pytest.mark.parametrize("failure_call", [2, 3])
def test_lost_activation_reply_then_later_guard_failure_drains(tmp_path, failure_call):
    owner, manager, _, calls = fixture(tmp_path)
    manager.lose_reply = True
    with pytest.raises(RuntimeError, match="lost activation reply"):
        owner.execute()
    count = 0
    def interrupted():
        nonlocal count
        count += 1
        if count == failure_call:
            raise RuntimeError("later dependency loss")
    owner = replace(owner, dependency_guard=interrupted)
    with pytest.raises(RuntimeError, match="later dependency loss"):
        owner.execute()
    assert manager.execution.execution_state == "drain-only"
    assert ("manager", "drain") in calls


@pytest.mark.parametrize("lose_abort_reply", [False, True])
def test_precommit_failure_with_expired_readiness_retires_preparation(tmp_path, lose_abort_reply):
    from loom_cli.rollout.operator.protected_execution_activation import (
        ActivationPreparationAbortedError,
    )
    owner, manager, controls, calls = fixture(tmp_path)
    def failed(request, key):
        raise RuntimeError("before commit")
    manager.activate_execution = failed
    with pytest.raises(RuntimeError, match="before commit"):
        owner.execute()
    manager.expired_final_lease = True
    if lose_abort_reply:
        original = manager.abort_execution_preparation
        def interrupted(request, key):
            result = original(request, key)
            if len(manager.abort_calls) == 1:
                raise RuntimeError("lost abort reply")
            return result
        manager.abort_execution_preparation = interrupted
        with pytest.raises(RuntimeError, match="lost abort reply"):
            owner.execute()
    with pytest.raises(ActivationPreparationAbortedError):
        owner.execute()
    assert owner.journal.read("abort.terminal.json") is not None
    assert manager.execution is None
    assert all(control.state == "staged" for control in controls.values())
    with pytest.raises(ActivationPreparationAbortedError):
        owner.execute()
    assert calls.count(("manager", "abort")) == 1


def test_late_activation_winning_abort_race_is_drained(tmp_path):
    owner, manager, _, calls = fixture(tmp_path)
    def failed(request, key):
        raise RuntimeError("before commit")
    manager.activate_execution = failed
    with pytest.raises(RuntimeError, match="before commit"):
        owner.execute()
    manager.expired_final_lease = True
    def racing(request, key):
        manager.execution = manager.expected
        raise RuntimeError("late activation won")
    manager.abort_execution_preparation = racing
    with pytest.raises(RuntimeError, match="late activation won"):
        owner.execute()
    assert owner.execute().execution_state == "drain-only"
    assert ("manager", "drain") in calls
    assert owner.journal.read("abort.terminal.json") is None


def test_activation_failure_before_commit_keeps_timers_stopped(tmp_path):
    owner, manager, controls, _calls = fixture(tmp_path)
    original = manager.activate_execution
    def failed(request, key):
        raise RuntimeError("activation rejected before commit")
    manager.activate_execution = failed
    with pytest.raises(RuntimeError, match="before commit"):
        owner.execute()
    assert manager.execution.execution_state == "prepared"
    assert all(control.state == "staged" for control in controls.values())
    manager.activate_execution = original
    assert owner.execute() == manager.expected


def test_concurrent_activation_is_excluded_before_effects(tmp_path):
    owner, _, _, calls = fixture(tmp_path)
    with owner.journal.exclusive():
        with pytest.raises(BlockingIOError):
            owner.execute()
    assert calls == []


def test_unknown_journal_record_refuses_before_effects(tmp_path):
    owner, _, _, calls = fixture(tmp_path)
    owner.journal._ensure_directories()
    (owner.journal.root / "foreign.json").write_text("{}")
    with pytest.raises(RuntimeError, match="inventory"):
        owner.execute()
    assert calls == []


def test_resume_recovers_exact_private_inputs_without_forward_source(tmp_path):
    owner, manager, controls, calls = fixture(tmp_path)
    controls["oldlab"].fail = True
    with pytest.raises(RuntimeError, match="controller enable failed"):
        owner.execute()
    def forbidden():
        raise AssertionError("forward source must not be reopened for drain")
    resumed = ProtectedExecutionActivation.resume(plan=owner.plan, artifact=owner.artifact,
        journal=owner.journal, manager=manager, prepared=controls, active=controls, dependency_guard=forbidden)
    assert resumed.requests == owner.requests
    assert resumed.subject == owner.subject
    assert resumed.execute().execution_state == "drain-only"
    assert calls.count(("manager", "activate")) == 1
