from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import loom_cli.rollout.operator.protected_external_supervisor_transport as transport_module
from loom_cli.rollout.external_supervisor_predecessor import (
    ABSENT_PREDECESSOR_DIGEST,
    ExternalSupervisorCanonicalIdentity,
    ExternalSupervisorPredecessorAuthority,
    external_supervisor_unit_set_digest,
    external_supervisor_unit_set_digest_or_empty,
    load_predecessor_manifest,
)
from loom_cli.rollout.external_supervisor_readiness import (
    ExternalSupervisorArtifact,
    build_external_supervisor_artifact,
)
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
)
from loom_cli.rollout.operator.protected_external_supervisor_component import (
    ProtectedExternalSupervisorComponent,
)
from loom_cli.rollout.operator.protected_external_supervisor_transport import (
    PROTECTED_USER_UNIT_DIR,
    AtomicUserUnitStore,
    ExternalSupervisorLiveObservation,
    FixedExternalSupervisorTransport,
    FixedUserSystemdControl,
    ServiceRuntimeStatus,
    TimerCompensationEvidence,
    TimerRuntimeStatus,
    classify_external_supervisor_live_state,
)
from loom_cli.rollout.preflight_contract import external_supervisor_transition_digest
from loom_cli.rollout.systemd_unit_readiness import UNIT_PATHS
from tests.loom_cli.rollout.operator.test_protected_migration_component import (
    _published_plan,
)


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _absent_authority() -> ExternalSupervisorPredecessorAuthority:
    return ExternalSupervisorPredecessorAuthority(
        kind="absent",
        authority_digest=ABSENT_PREDECESSOR_DIGEST,
        unit_sha256={},
    )


def _absent_plan(**overrides: object) -> SimpleNamespace:
    fields: dict[str, object] = {
        "supervisor_predecessor_kind": "absent",
        "supervisor_predecessor_digest": ABSENT_PREDECESSOR_DIGEST,
        "supervisor_predecessor_unit_sha256": {},
        "supervisor_predecessor_pointer_digest": ABSENT_PREDECESSOR_DIGEST,
        "supervisor_predecessor_unit_set_digest": external_supervisor_unit_set_digest_or_empty({}),
    }
    fields.update(overrides)
    return SimpleNamespace(**fields)


def test_plan_authority_bootstraps_well_formed_absent_predecessor() -> None:
    # A post-0067 first-introduction rollout carries an absent predecessor
    # (no live units, no canonical record). The apply must build the absent
    # authority so the bootstrap can establish the first canonical supervisor.
    authority = ProtectedExternalSupervisorComponent._plan_authority(_absent_plan())
    assert authority.kind == "absent"
    assert authority.authority_digest == ABSENT_PREDECESSOR_DIGEST
    assert dict(authority.unit_sha256) == {}


@pytest.mark.parametrize(
    "override",
    [
        {"supervisor_predecessor_digest": "0" * 64},
        {"supervisor_predecessor_pointer_digest": "0" * 64},
        {"supervisor_predecessor_unit_set_digest": "0" * 64},
    ],
)
def test_plan_authority_rejects_malformed_absent_predecessor(
    override: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="predecessor binding is invalid"):
        ProtectedExternalSupervisorComponent._plan_authority(_absent_plan(**override))


def _build_active_artifact(*args, **kwargs) -> ExternalSupervisorArtifact:
    with patch(
        "loom_cli.environment_state.staging_gb10_external_activation_blockers",
        return_value=(),
    ):
        return build_external_supervisor_artifact(*args, **kwargs)


def _bound_artifact(tmp_path: Path):
    plan = _published_plan(tmp_path)
    candidate_root = tmp_path / "candidate"
    profile_target = candidate_root / "deploy/environment-state/staging.toml"
    script_target = candidate_root / "scripts/ops/worker_pool_autoscaler_external_once.py"
    profile_target.parent.mkdir(parents=True, exist_ok=True)
    script_target.parent.mkdir(parents=True, exist_ok=True)
    repository = Path(__file__).resolve().parents[4]
    shutil.copyfile(repository / "deploy/environment-state/staging.toml", profile_target)
    shutil.copyfile(
        repository / "scripts/ops/worker_pool_autoscaler_external_once.py",
        script_target,
    )
    profile_target.chmod(0o600)
    script_target.chmod(0o700)
    artifact = _build_active_artifact(
        candidate_root,
        candidate_sha=plan.candidate_sha,
        candidate_tree=plan.candidate_tree,
        image_tag=f"staging-{plan.candidate_sha[:7]}",
    )
    unit_digests = {
        **{
            name: digest for name, digest in plan.systemd_unit_digests.items() if name in UNIT_PATHS
        },
        **artifact.unit_sha256,
    }
    legacy = load_predecessor_manifest()
    predecessor_live = _observation(artifact, files="legacy", runtime="exact")
    transition_digest = external_supervisor_transition_digest(
        candidate_sha=plan.candidate_sha,
        candidate_tree=plan.candidate_tree,
        environment=plan.environment,
        predecessor_kind="legacy-manifest",
        predecessor_digest=legacy.manifest_digest,
        predecessor_pointer_digest=ABSENT_PREDECESSOR_DIGEST,
        predecessor_unit_sha256=legacy.unit_sha256,
        predecessor_unit_set_digest=legacy.unit_set_digest,
        predecessor_live_evidence_digest=predecessor_live.evidence_digest,
        predecessor_pending_transition_digest=predecessor_live.pending_transition_digest,
        target_artifact_digest=artifact.artifact_digest,
        target_profile_sha256=artifact.profile_sha256,
        target_script_sha256=artifact.script_sha256,
        target_unit_sha256=artifact.unit_sha256,
        target_unit_set_digest=external_supervisor_unit_set_digest(artifact.unit_sha256),
    )
    payload = plan.to_dict()
    payload.pop("plan_digest")
    payload.update(
        {
            "supervisor_artifact_digest": artifact.artifact_digest,
            "supervisor_profile_sha256": artifact.profile_sha256,
            "supervisor_script_digests": dict(artifact.script_sha256),
            "systemd_unit_digests": unit_digests,
            "systemd_unit_set_digest": _hash_json({"failed": {}, "units": unit_digests}),
            "supervisor_predecessor_kind": "legacy-manifest",
            "supervisor_predecessor_digest": legacy.manifest_digest,
            "supervisor_predecessor_pointer_digest": ABSENT_PREDECESSOR_DIGEST,
            "supervisor_predecessor_unit_sha256": dict(legacy.unit_sha256),
            "supervisor_predecessor_unit_set_digest": legacy.unit_set_digest,
            "supervisor_predecessor_live_evidence_digest": predecessor_live.evidence_digest,
            "supervisor_predecessor_pending_transition_digest": (
                predecessor_live.pending_transition_digest
            ),
            "supervisor_transition_digest": transition_digest,
        }
    )
    bound = type(plan).from_dict({**payload, "plan_digest": _hash_json(payload)})
    return bound, candidate_root, artifact


def _epoch(plan, state: ComponentState = ComponentState.EXACT) -> ComponentObservation:
    return ComponentObservation(
        state=state,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )


def _observation(
    artifact: ExternalSupervisorArtifact,
    *,
    files: str,
    runtime: str,
    plan_digest: str = "a" * 64,
    attestation_digest: str = "b" * 64,
) -> ExternalSupervisorLiveObservation:
    legacy = load_predecessor_manifest()
    units: dict[str, bytes | None] = {}
    timers: dict[str, TimerRuntimeStatus] = {}
    services: dict[str, ServiceRuntimeStatus] = {}
    for supervisor in artifact.supervisors:
        legacy_service = legacy.unit_payloads.get(supervisor.service_name)
        legacy_timer = legacy.unit_payloads.get(supervisor.timer_name)
        if files == "legacy":
            units[supervisor.service_name] = (
                None if legacy_service is None else legacy_service.encode()
            )
            units[supervisor.timer_name] = None if legacy_timer is None else legacy_timer.encode()
        elif files == "partial":
            units[supervisor.service_name] = (
                None if legacy_service is None else supervisor.service_unit.encode()
            )
            units[supervisor.timer_name] = None if legacy_timer is None else legacy_timer.encode()
        else:
            units[supervisor.service_name] = (
                supervisor.service_unit.encode() if files == "exact" else None
            )
            units[supervisor.timer_name] = (
                supervisor.timer_unit.encode() if files == "exact" else None
            )
        pair_present = (
            units[supervisor.service_name] is not None
            and units[supervisor.timer_name] is not None
        )
        if not pair_present or runtime == "absent":
            timers[supervisor.timer_name] = _timer_status(
                supervisor.timer_name, "not-found", "not-found", "inactive"
            )
            services[supervisor.service_name] = _service_status(
                supervisor.service_name, "not-found", "", None
            )
        elif runtime == "exact" and supervisor.enabled and supervisor.active:
            timers[supervisor.timer_name] = _timer_status(
                supervisor.timer_name, "loaded", "enabled", "active"
            )
            services[supervisor.service_name] = _service_status(
                supervisor.service_name, "loaded", "success", 0
            )
        elif runtime == "exact":
            timers[supervisor.timer_name] = _timer_status(
                supervisor.timer_name, "loaded", "disabled", "inactive"
            )
            services[supervisor.service_name] = _service_status(
                supervisor.service_name, "loaded", "", None
            )
        elif runtime == "failed":
            timers[supervisor.timer_name] = _timer_status(
                supervisor.timer_name, "loaded", "enabled", "active"
            )
            services[supervisor.service_name] = _service_status(
                supervisor.service_name, "loaded", "failed", 1
            )
        elif runtime == "loaded":
            timers[supervisor.timer_name] = _timer_status(
                supervisor.timer_name, "loaded", "disabled", "inactive"
            )
            services[supervisor.service_name] = _service_status(
                supervisor.service_name, "loaded", "", None
            )
    canonical = None
    if files == "exact" and runtime == "exact":
        canonical = ExternalSupervisorCanonicalIdentity.build(
            artifact,
            plan_digest=plan_digest,
            attestation_digest=attestation_digest,
            transition_group_id="f" * 32,
            runtime_evidence_digest=transport_module._expected_activation_runtime_digest(artifact),
        )
    if canonical is not None:
        authority = ExternalSupervisorPredecessorAuthority(
            kind="canonical",
            authority_digest=canonical.evidence_digest,
            unit_sha256=canonical.unit_sha256,
        )
    elif files in {"legacy", "partial"}:
        authority = ExternalSupervisorPredecessorAuthority(
            kind="legacy-manifest",
            authority_digest=legacy.manifest_digest,
            unit_sha256=legacy.unit_sha256,
        )
    else:
        authority = ExternalSupervisorPredecessorAuthority(
            kind="absent",
            authority_digest=ABSENT_PREDECESSOR_DIGEST,
            unit_sha256={},
        )
    return ExternalSupervisorLiveObservation(
        units,
        timers,
        services,
        canonical_identity=canonical,
        predecessor_authority=authority,
    )


def _timer_status(
    name: str,
    load_state: str,
    unit_file_state: str,
    active_state: str,
    *,
    need_daemon_reload: str = "no",
) -> TimerRuntimeStatus:
    return TimerRuntimeStatus(
        load_state,
        unit_file_state,
        active_state,
        "" if load_state == "not-found" else str(PROTECTED_USER_UNIT_DIR / name),
        need_daemon_reload,
    )


def _service_status(
    name: str,
    load_state: str,
    result: str,
    exec_main_status: int | None,
    *,
    need_daemon_reload: str = "no",
) -> ServiceRuntimeStatus:
    return ServiceRuntimeStatus(
        load_state,
        result,
        exec_main_status,
        "" if load_state == "not-found" else str(PROTECTED_USER_UNIT_DIR / name),
        need_daemon_reload,
    )


class _Transport:
    def __init__(self, artifact: ExternalSupervisorArtifact, observation):
        self.artifact = artifact
        self.observation = observation
        self.applied = 0

    def observe(self, artifact, predecessor_authority=None):
        assert artifact == self.artifact
        if predecessor_authority is not None:
            assert predecessor_authority == self.observation.predecessor_authority
        return self.observation

    def apply(
        self,
        artifact,
        expected,
        *,
        plan_digest,
        attestation_digest,
        transition_digest,
    ):
        assert artifact == self.artifact
        assert expected == self.observation
        self.applied += 1
        self.observation = _observation(
            artifact,
            files="exact",
            runtime="exact",
            plan_digest=plan_digest,
            attestation_digest=attestation_digest,
        )


def test_component_classifies_bound_predecessor_partial_and_exact_states(tmp_path: Path) -> None:
    plan, candidate_root, artifact = _bound_artifact(tmp_path)
    transport = _Transport(artifact, _observation(artifact, files="legacy", runtime="exact"))
    component = ProtectedExternalSupervisorComponent(
        candidate_root=candidate_root,
        transport=transport,
        epoch_guard=lambda value: _epoch(value),
        artifact_builder=_build_active_artifact,
    )

    ready = component.classify(plan)
    assert ready.state is ComponentState.READY
    assert ready.observed_epoch == plan.starting_mutation_epoch + 1

    transport.observation = _observation(artifact, files="partial", runtime="absent")
    assert component.classify(plan).state is ComponentState.DRIFTED

    transport.observation = _observation(
        artifact,
        files="exact",
        runtime="exact",
        plan_digest=plan.plan_digest,
        attestation_digest=plan.attestation_digest,
    )
    assert component.classify(plan).state is ComponentState.EXACT


def _bound_absent(tmp_path: Path):
    plan, candidate_root, artifact = _bound_artifact(tmp_path)
    predecessor_live = _observation(artifact, files="absent", runtime="absent")
    absent_unit_set = external_supervisor_unit_set_digest_or_empty({})
    transition_digest = external_supervisor_transition_digest(
        candidate_sha=plan.candidate_sha,
        candidate_tree=plan.candidate_tree,
        environment=plan.environment,
        predecessor_kind="absent",
        predecessor_digest=ABSENT_PREDECESSOR_DIGEST,
        predecessor_pointer_digest=ABSENT_PREDECESSOR_DIGEST,
        predecessor_unit_sha256={},
        predecessor_unit_set_digest=absent_unit_set,
        predecessor_live_evidence_digest=predecessor_live.evidence_digest,
        predecessor_pending_transition_digest=predecessor_live.pending_transition_digest,
        target_artifact_digest=artifact.artifact_digest,
        target_profile_sha256=artifact.profile_sha256,
        target_script_sha256=artifact.script_sha256,
        target_unit_sha256=artifact.unit_sha256,
        target_unit_set_digest=external_supervisor_unit_set_digest(artifact.unit_sha256),
    )
    payload = plan.to_dict()
    payload.pop("plan_digest")
    payload.update(
        {
            "supervisor_predecessor_kind": "absent",
            "supervisor_predecessor_digest": ABSENT_PREDECESSOR_DIGEST,
            "supervisor_predecessor_pointer_digest": ABSENT_PREDECESSOR_DIGEST,
            "supervisor_predecessor_unit_sha256": {},
            "supervisor_predecessor_unit_set_digest": absent_unit_set,
            "supervisor_predecessor_live_evidence_digest": predecessor_live.evidence_digest,
            "supervisor_predecessor_pending_transition_digest": (
                predecessor_live.pending_transition_digest
            ),
            "supervisor_transition_digest": transition_digest,
        }
    )
    bound = type(plan).from_dict({**payload, "plan_digest": _hash_json(payload)})
    return bound, candidate_root, artifact


class _BareRefusingTransport:
    """A bare observe() refuses to self-authorize absent; observe(authority) returns."""

    def __init__(self, artifact, observation) -> None:
        self.artifact = artifact
        self.observation = observation

    def observe(self, artifact, predecessor_authority=None):
        assert artifact == self.artifact
        if predecessor_authority is None:
            raise RuntimeError("protected external supervisor predecessor is not authoritative")
        return self.observation

    def apply(self, *args, **kwargs):  # pragma: no cover - not exercised
        raise AssertionError("apply is not expected in classify")

    def reconcile_compensations(self):  # pragma: no cover - not exercised
        raise AssertionError("reconcile is not expected in classify")


def test_component_classifies_absent_bootstrap_ready_via_plan_authority(tmp_path: Path) -> None:
    # The bare observe refuses absent (not self-authoritative); the component must
    # re-observe with the plan's absent authority so the bootstrap classifies READY,
    # letting the protected-apply journal reach apply instead of failing as drifted.
    plan, candidate_root, artifact = _bound_absent(tmp_path)
    transport = _BareRefusingTransport(
        artifact, _observation(artifact, files="absent", runtime="absent")
    )
    component = ProtectedExternalSupervisorComponent(
        candidate_root=candidate_root,
        transport=transport,
        epoch_guard=lambda value: _epoch(value),
        artifact_builder=_build_active_artifact,
    )
    assert component.classify(plan).state is ComponentState.READY


def test_component_stays_drifted_when_bare_observe_refuses_non_absent(tmp_path: Path) -> None:
    # A non-absent plan does not retry with an authority: a refusing bare observe
    # stays DRIFTED (the fail-closed default is preserved for canonical/legacy).
    plan, candidate_root, artifact = _bound_artifact(tmp_path)
    transport = _BareRefusingTransport(
        artifact, _observation(artifact, files="absent", runtime="absent")
    )
    component = ProtectedExternalSupervisorComponent(
        candidate_root=candidate_root,
        transport=transport,
        epoch_guard=lambda value: _epoch(value),
        artifact_builder=_build_active_artifact,
    )
    assert component.classify(plan).state is ComponentState.DRIFTED


def test_component_applies_and_reaches_exact(tmp_path: Path) -> None:
    plan, candidate_root, artifact = _bound_artifact(tmp_path)
    transport = _Transport(artifact, _observation(artifact, files="legacy", runtime="exact"))
    component = ProtectedExternalSupervisorComponent(
        candidate_root=candidate_root,
        transport=transport,
        epoch_guard=lambda value: _epoch(value),
        artifact_builder=_build_active_artifact,
    )

    component.apply(plan)

    assert transport.applied == 1
    assert component.classify(plan).state is ComponentState.EXACT


def test_component_rejects_stale_bytes_failed_state_and_epoch(tmp_path: Path) -> None:
    plan, candidate_root, artifact = _bound_artifact(tmp_path)
    stale = _observation(artifact, files="legacy", runtime="exact")
    units = dict(stale.unit_payloads)
    units[next(iter(units))] = b"stale\n"
    transport = _Transport(
        artifact,
        ExternalSupervisorLiveObservation(
            units,
            stale.timer_statuses,
            stale.service_statuses,
            predecessor_authority=stale.predecessor_authority,
        ),
    )
    component = ProtectedExternalSupervisorComponent(
        candidate_root=candidate_root,
        transport=transport,
        epoch_guard=lambda value: _epoch(value),
        artifact_builder=_build_active_artifact,
    )
    assert component.classify(plan).state is ComponentState.DRIFTED

    transport.observation = _observation(artifact, files="legacy", runtime="failed")
    assert component.classify(plan).state is ComponentState.DRIFTED

    guarded = replace(component, epoch_guard=lambda value: _epoch(value, ComponentState.READY))
    assert guarded.classify(plan).state is ComponentState.DRIFTED


def test_component_rebuilds_and_rejects_candidate_or_plan_drift(tmp_path: Path) -> None:
    plan, candidate_root, artifact = _bound_artifact(tmp_path)
    component = ProtectedExternalSupervisorComponent(
        candidate_root=candidate_root,
        transport=_Transport(
            artifact,
            _observation(artifact, files="legacy", runtime="exact"),
        ),
        epoch_guard=lambda value: _epoch(value),
        artifact_builder=_build_active_artifact,
    )
    with pytest.raises(ValueError, match="transition identity drifted"):
        component.classify(replace(plan, supervisor_artifact_digest="f" * 64))

    profile = candidate_root / "deploy/environment-state/staging.toml"
    profile.write_bytes(profile.read_bytes() + b"\n# drift\n")
    with pytest.raises(ValueError, match="artifact drifted"):
        component.classify(plan)


class _Store:
    def __init__(self) -> None:
        self.units: dict[str, bytes] = {}
        self.publish_calls: list[str] = []
        self.compensations: list[TimerCompensationEvidence] = []
        self.canonical: ExternalSupervisorCanonicalIdentity | None = None

    def list_units(self):
        return tuple(sorted(self.units))

    def read_unit(self, name):
        return self.units.get(name)

    def read_canonical(self):
        return self.canonical

    def record_compensation(self, evidence):
        self.compensations.append(evidence)

    def compensation_blockers(self):
        blockers = {}
        by_id = {}
        for record in self.compensations:
            by_id.setdefault(record.compensation_id, {})[record.phase] = record
        for compensation_id, phases in by_id.items():
            if (
                "recovered" in phases
                or "verified" in phases
                or ("canonical" in phases and "failed" not in phases)
            ):
                continue
            blockers[compensation_id] = next(iter(phases.values())).evidence_digest
        return blockers

    def pending_compensations(self):
        by_id = {}
        for record in self.compensations:
            by_id.setdefault(record.compensation_id, {})[record.phase] = record
        return tuple(
            phases["intent"]
            for _compensation_id, phases in sorted(by_id.items())
            if "intent" in phases
            and "recovered" not in phases
            and "verified" not in phases
            and not ("canonical" in phases and "failed" not in phases)
        )

    def publish_unit(self, name, payload, *, expected_current):
        if self.units.get(name) != expected_current:
            raise RuntimeError("race")
        self.publish_calls.append(name)
        self.units[name] = payload

    def publish_transition(self, intents, units):
        for evidence in intents:
            self.record_compensation(evidence)
        for name, (expected, payload) in sorted(units.items()):
            self.publish_unit(name, payload, expected_current=expected)

    def restore_unit(self, name, payload, *, expected_current):
        if self.units.get(name) != expected_current:
            raise RuntimeError("race")
        if payload is None:
            self.units.pop(name, None)
        else:
            self.units[name] = payload

    def restore_transition(self, units):
        for name, (expected, _payload) in units.items():
            if self.units.get(name) != expected:
                raise RuntimeError("race")
        for name, (_expected, payload) in units.items():
            if payload is None:
                self.units.pop(name, None)
            else:
                self.units[name] = payload

    def promote_canonical(self, identity, *, expected_current):
        if self.canonical != expected_current:
            raise RuntimeError("canonical race")
        self.canonical = identity


class _Control:
    def __init__(self, artifact: ExternalSupervisorArtifact, store: _Store | None = None) -> None:
        self.artifact = artifact
        self.store = store
        self.loaded = False
        self.enabled = False
        self.active = False
        self.result = ""
        self.status = None
        self.calls: list[str] = []
        self.fail_service = False
        self.fail_timer_start = False
        self.fail_stop = False
        self.fail_disable = False

    def _present(self, name: str) -> bool:
        if self.store is None:
            return True
        units = getattr(self.store, "units", None)
        if isinstance(units, dict):
            return name in units
        return self.store.read_unit(name) is not None

    def timer_status(self, name):
        loaded = self.loaded and self._present(name)
        return _timer_status(
            name,
            "loaded" if loaded else "not-found",
            "enabled" if self.enabled else ("disabled" if self.loaded else "not-found"),
            "active" if self.active else "inactive",
        )

    def service_status(self, name):
        loaded = self.loaded and self._present(name)
        return _service_status(
            name,
            "loaded" if loaded else "not-found",
            self.result if loaded else "",
            self.status if loaded else None,
        )

    def daemon_reload(self):
        self.calls.append("daemon-reload")
        self.loaded = True

    def enable_timer(self, name):
        self.calls.append(f"enable:{name}")
        self.enabled = True

    def start_timer(self, name):
        self.calls.append(f"timer:{name}")
        if self.fail_timer_start:
            raise RuntimeError("timer start failed")
        self.active = True

    def stop_timer(self, name):
        self.calls.append(f"stop:{name}")
        if self.fail_stop:
            raise RuntimeError("timer stop failed")
        self.active = False

    def disable_timer(self, name):
        self.calls.append(f"disable:{name}")
        if self.fail_disable:
            raise RuntimeError("timer disable failed")
        self.enabled = False

    def stop_service(self, name):
        self.calls.append(f"stop-service:{name}")
        self.result = "success"
        self.status = 0

    def start_service(self, name, *, timeout_seconds):
        self.calls.append(f"service:{name}:{timeout_seconds}")
        if self.fail_service:
            self.result = "failed"
            self.status = 1
            raise RuntimeError("redacted failure")
        self.result = "success"
        self.status = 0


def test_fixed_transport_runs_closed_convergence_sequence(tmp_path: Path) -> None:
    _plan, _root, artifact = _bound_artifact(tmp_path)
    store = _Store()
    control = _Control(artifact, store)
    transport = FixedExternalSupervisorTransport(store=store, control=control)
    before = transport.observe(artifact, _absent_authority())

    transport.apply(
        artifact,
        before,
        plan_digest="a" * 64,
        attestation_digest="b" * 64,
        transition_digest="c" * 64,
    )

    after = transport.observe(artifact)
    assert classify_external_supervisor_live_state(artifact, after) == "exact"
    supervisor = artifact.supervisors[0]
    assert control.calls == [
        "daemon-reload",
        f"service:{supervisor.service_name}:{float(supervisor.service_timeout_sec) + 15.0}",
        f"enable:{supervisor.timer_name}",
        f"timer:{supervisor.timer_name}",
    ]
    assert [record.phase for record in store.compensations] == [
        "intent",
        "activated",
        "canonical",
    ]
    assert store.compensation_blockers() == {}


def test_fixed_transport_converges_active_canonical_to_disabled_target(
    tmp_path: Path,
) -> None:
    plan, candidate_root, active_artifact = _bound_artifact(tmp_path)
    profile = candidate_root / "deploy/environment-state/staging.toml"
    profile.write_text(
        profile.read_text(encoding="utf-8").replace(
            "enabled = true\nactive = true",
            "enabled = false\nactive = false",
            1,
        ),
        encoding="utf-8",
    )
    disabled_artifact = _build_active_artifact(
        candidate_root,
        candidate_sha=plan.candidate_sha,
        candidate_tree=plan.candidate_tree,
        image_tag=f"staging-{plan.candidate_sha[:7]}",
        environment=plan.environment,
    )
    assert len(disabled_artifact.supervisors) == 1
    disabled = disabled_artifact.supervisors[0]
    assert not disabled.enabled
    assert not disabled.active

    store = _Store()
    control = _Control(active_artifact, store)
    transport = FixedExternalSupervisorTransport(store=store, control=control)
    transport.apply(
        active_artifact,
        transport.observe(active_artifact, _absent_authority()),
        plan_digest="a" * 64,
        attestation_digest="b" * 64,
        transition_digest="c" * 64,
    )
    before = transport.observe(disabled_artifact)
    assert classify_external_supervisor_live_state(disabled_artifact, before) == "ready"

    transport.apply(
        disabled_artifact,
        before,
        plan_digest="d" * 64,
        attestation_digest="e" * 64,
        transition_digest="f" * 64,
    )

    after = transport.observe(disabled_artifact)
    assert classify_external_supervisor_live_state(disabled_artifact, after) == "exact"
    assert control.calls[-4:] == [
        "daemon-reload",
        f"stop:{disabled.timer_name}",
        f"disable:{disabled.timer_name}",
        f"stop-service:{disabled.service_name}",
    ]
    assert [record.reason for record in store.compensations if record.phase == "activated"] == [
        "timer-active",
        "timer-disabled",
    ]
    assert store.compensation_blockers() == {}


def test_fixed_transport_detects_race_and_service_failure(tmp_path: Path) -> None:
    _plan, _root, artifact = _bound_artifact(tmp_path)
    store = _Store()
    control = _Control(artifact, store)
    transport = FixedExternalSupervisorTransport(store=store, control=control)
    before = transport.observe(artifact, _absent_authority())
    store.units[artifact.supervisors[0].service_name] = b"race\n"
    with pytest.raises(RuntimeError, match="changed before apply"):
        transport.apply(
            artifact,
            before,
            plan_digest="a" * 64,
            attestation_digest="b" * 64,
            transition_digest="c" * 64,
        )
    assert not control.calls

    store.units.clear()
    control.fail_service = True
    before = transport.observe(artifact, _absent_authority())
    with pytest.raises(RuntimeError, match="safely compensated"):
        transport.apply(
            artifact,
            before,
            plan_digest="a" * 64,
            attestation_digest="b" * 64,
            transition_digest="c" * 64,
        )
    after = transport.observe(artifact, _absent_authority())
    assert classify_external_supervisor_live_state(artifact, after) == "ready"


def test_fixed_transport_rechecks_all_unit_bytes_before_systemd_mutation(
    tmp_path: Path,
) -> None:
    _plan, _root, artifact = _bound_artifact(tmp_path)

    class _PostPublishRaceStore(_Store):
        def publish_unit(self, name, payload, *, expected_current):
            super().publish_unit(name, payload, expected_current=expected_current)
            if len(self.publish_calls) == 2:
                self.units[self.publish_calls[0]] = b"race\n"

    store = _PostPublishRaceStore()
    control = _Control(artifact, store)
    transport = FixedExternalSupervisorTransport(store=store, control=control)

    with pytest.raises(RuntimeError, match="compensation failed safely"):
        transport.apply(
            artifact,
            transport.observe(artifact, _absent_authority()),
            plan_digest="a" * 64,
            attestation_digest="b" * 64,
            transition_digest="c" * 64,
        )
    assert control.calls == []


def test_timer_start_failure_is_journaled_stopped_disabled_and_verified(
    tmp_path: Path,
) -> None:
    _plan, _root, artifact = _bound_artifact(tmp_path)
    store = _Store()
    control = _Control(artifact, store)
    control.fail_timer_start = True
    transport = FixedExternalSupervisorTransport(store=store, control=control)
    supervisor = artifact.supervisors[0]

    with pytest.raises(RuntimeError, match="safely compensated"):
        transport.apply(
            artifact,
            transport.observe(artifact, _absent_authority()),
            plan_digest="a" * 64,
            attestation_digest="b" * 64,
            transition_digest="c" * 64,
        )

    assert control.calls == [
        "daemon-reload",
        f"service:{supervisor.service_name}:{float(supervisor.service_timeout_sec) + 15.0}",
        f"enable:{supervisor.timer_name}",
        f"timer:{supervisor.timer_name}",
        f"stop:{supervisor.timer_name}",
        f"disable:{supervisor.timer_name}",
        "daemon-reload",
    ]
    assert control.timer_status(supervisor.timer_name) == _timer_status(
        supervisor.timer_name, "not-found", "disabled", "inactive"
    )
    assert [record.phase for record in store.compensations] == ["intent", "verified"]
    assert store.compensation_blockers() == {}
    assert (
        classify_external_supervisor_live_state(
            artifact,
            transport.observe(artifact, _absent_authority()),
        )
        == "ready"
    )


def test_failed_timer_compensation_is_journaled_and_blocks_convergence(
    tmp_path: Path,
) -> None:
    _plan, _root, artifact = _bound_artifact(tmp_path)
    store = _Store()
    control = _Control(artifact, store)
    control.fail_timer_start = True
    control.fail_stop = True
    transport = FixedExternalSupervisorTransport(store=store, control=control)

    with pytest.raises(RuntimeError, match="compensation failed safely"):
        transport.apply(
            artifact,
            transport.observe(artifact, _absent_authority()),
            plan_digest="a" * 64,
            attestation_digest="b" * 64,
            transition_digest="c" * 64,
        )

    assert [record.phase for record in store.compensations] == ["intent", "failed"]
    assert store.compensation_blockers()
    assert (
        classify_external_supervisor_live_state(
            artifact,
            transport.observe(artifact, _absent_authority()),
        )
        == "drifted"
    )


def test_crash_intent_is_reconciled_through_fixed_identity_bound_transport(
    tmp_path: Path,
) -> None:
    _plan, _root, artifact = _bound_artifact(tmp_path)
    supervisor = artifact.supervisors[0]
    store = _Store()
    store.units = {
        supervisor.service_name: supervisor.service_unit.encode(),
        supervisor.timer_name: supervisor.timer_unit.encode(),
    }
    store.record_compensation(
        _compensation_record(
            artifact,
            compensation_id="c" * 32,
            phase="intent",
            reason="timer-activation",
        )
    )
    control = _Control(artifact, store)
    control.loaded = True
    control.enabled = True
    control.active = True
    transport = FixedExternalSupervisorTransport(store=store, control=control)

    transport.reconcile_compensations()

    assert control.calls == [
        f"stop:{supervisor.timer_name}",
        f"disable:{supervisor.timer_name}",
        "daemon-reload",
    ]
    assert [record.phase for record in store.compensations] == ["intent", "verified"]
    assert store.compensation_blockers() == {}


def test_crash_intent_reconciliation_rejects_managed_identity_drift(
    tmp_path: Path,
) -> None:
    _plan, _root, artifact = _bound_artifact(tmp_path)
    supervisor = artifact.supervisors[0]
    store = _Store()
    store.units = {
        supervisor.service_name: supervisor.service_unit.encode(),
        supervisor.timer_name: b"drifted\n",
    }
    store.record_compensation(
        _compensation_record(
            artifact,
            compensation_id="d" * 32,
            phase="intent",
            reason="timer-activation",
        )
    )
    control = _Control(artifact, store)
    transport = FixedExternalSupervisorTransport(store=store, control=control)

    with pytest.raises(RuntimeError, match="reconciliation failed safely"):
        transport.reconcile_compensations()

    assert control.calls == []
    assert [record.phase for record in store.compensations] == ["intent", "failed"]
    assert store.compensation_blockers()


def test_fixed_transport_reports_unexpected_managed_unit_as_drift(tmp_path: Path) -> None:
    _plan, _root, artifact = _bound_artifact(tmp_path)
    store = _Store()
    store.units["loom-autoscaler-stale.timer"] = b"stale\n"
    transport = FixedExternalSupervisorTransport(
        store=store,
        control=_Control(artifact, store),
    )

    assert (
        classify_external_supervisor_live_state(
            artifact,
            transport.observe(artifact, _absent_authority()),
        )
        == "drifted"
    )


def test_atomic_store_publishes_exact_without_replacing_race(tmp_path: Path) -> None:
    unit_dir = tmp_path / "user"
    unit_dir.mkdir(mode=0o700)
    store = AtomicUserUnitStore(unit_dir=unit_dir, service_uid=os.geteuid())
    name = "loom-autoscaler-test.service"

    store.publish_unit(name, b"exact\n", expected_current=None)

    assert store.read_unit(name) == b"exact\n"
    assert stat_mode(unit_dir / name) == 0o600
    with pytest.raises(RuntimeError, match="changed before publish"):
        store.publish_unit(name, b"other\n", expected_current=None)


def test_atomic_store_upgrades_with_exact_byte_cas_and_detects_mid_write_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unit_dir = tmp_path / "user"
    unit_dir.mkdir(mode=0o700)
    store = AtomicUserUnitStore(unit_dir=unit_dir, service_uid=os.geteuid())
    name = "loom-autoscaler-test.service"
    store.publish_unit(name, b"old\n", expected_current=None)

    store.publish_unit(name, b"new\n", expected_current=b"old\n")

    assert store.read_unit(name) == b"new\n"
    original = AtomicUserUnitStore._read_at
    reads = 0

    def racing_read(self, directory, unit_name):
        nonlocal reads
        reads += 1
        if reads == 2:
            (unit_dir / name).write_bytes(b"race\n")
        return original(self, directory, unit_name)

    monkeypatch.setattr(AtomicUserUnitStore, "_read_at", racing_read)
    with pytest.raises(RuntimeError, match="changed during publish"):
        store.publish_unit(name, b"next\n", expected_current=b"new\n")
    assert (unit_dir / name).read_bytes() == b"race\n"


def test_atomic_store_securely_creates_fresh_fixed_directory_chain(tmp_path: Path) -> None:
    anchor = tmp_path / "loom-staging-rollout"
    anchor.mkdir(mode=0o700)
    unit_dir = anchor / ".config/systemd/user"
    store = AtomicUserUnitStore(
        unit_dir=unit_dir,
        service_uid=os.geteuid(),
        creation_anchor=anchor,
    )

    assert store.read_unit("loom-autoscaler-test.timer") is None
    store.publish_unit(
        "loom-autoscaler-test.timer",
        b"exact\n",
        expected_current=None,
    )

    assert store.read_unit("loom-autoscaler-test.timer") == b"exact\n"
    for directory in (anchor / ".config", anchor / ".config/systemd", unit_dir):
        assert directory.is_dir()
        assert not directory.is_symlink()
        assert directory.stat().st_uid == os.geteuid()
        assert stat_mode(directory) == 0o700


def _compensation_record(
    artifact: ExternalSupervisorArtifact,
    *,
    compensation_id: str,
    phase: str,
    reason: str,
) -> TimerCompensationEvidence:
    supervisor = artifact.supervisors[0]
    target = ExternalSupervisorCanonicalIdentity.build(
        artifact,
        plan_digest="a" * 64,
        attestation_digest="b" * 64,
        transition_group_id="c" * 32,
        runtime_evidence_digest=transport_module._expected_activation_runtime_digest(artifact),
    )
    return TimerCompensationEvidence.build(
        compensation_id=compensation_id,
        artifact_digest=artifact.artifact_digest,
        service_name=supervisor.service_name,
        timer_name=supervisor.timer_name,
        service_unit_sha256=hashlib.sha256(supervisor.service_unit.encode()).hexdigest(),
        timer_unit_sha256=hashlib.sha256(supervisor.timer_unit.encode()).hexdigest(),
        predecessor_kind="absent",
        predecessor_authority_digest=transport_module.ABSENT_PREDECESSOR_DIGEST,
        predecessor_pointer_digest=transport_module.ABSENT_PREDECESSOR_DIGEST,
        predecessor_canonical_json="",
        target_canonical_json=target.to_bytes().decode(),
        transition_digest="c" * 64,
        transition_group_id=target.transition_group_id,
        phase=phase,
        reason=("supervisor-mutation" if phase == "intent" else reason),
    )


def test_compensation_journal_blocks_across_candidate_until_same_identity_verified(
    tmp_path: Path,
) -> None:
    _plan, candidate_root, artifact = _bound_artifact(tmp_path)
    unit_dir = tmp_path / "units"
    unit_dir.mkdir(mode=0o700)
    store = AtomicUserUnitStore(unit_dir=unit_dir, service_uid=os.geteuid())
    compensation_id = "a" * 32
    store.record_compensation(
        _compensation_record(
            artifact,
            compensation_id=compensation_id,
            phase="intent",
            reason="timer-activation",
        )
    )
    store.record_compensation(
        _compensation_record(
            artifact,
            compensation_id=compensation_id,
            phase="failed",
            reason="operation-failed",
        )
    )
    next_artifact = _build_active_artifact(
        candidate_root,
        candidate_sha="c" * 40,
        candidate_tree="d" * 40,
        image_tag="staging-ccccccc",
    )
    control = _Control(next_artifact, store)
    observation = FixedExternalSupervisorTransport(
        store=store,
        control=control,
    ).observe(next_artifact, _absent_authority())

    assert observation.compensation_blockers
    assert classify_external_supervisor_live_state(next_artifact, observation) == "drifted"

    store.record_compensation(
        _compensation_record(
            artifact,
            compensation_id=compensation_id,
            phase="verified",
            reason="inactive-disabled",
        )
    )
    assert store.compensation_blockers() == {}


def test_atomic_journal_crash_prefix_uses_supported_reconciliation(
    tmp_path: Path,
) -> None:
    _plan, _root, artifact = _bound_artifact(tmp_path)
    supervisor = artifact.supervisors[0]
    unit_dir = tmp_path / "units"
    unit_dir.mkdir(mode=0o700)
    store = AtomicUserUnitStore(unit_dir=unit_dir, service_uid=os.geteuid())
    store.publish_unit(
        supervisor.service_name,
        supervisor.service_unit.encode(),
        expected_current=None,
    )
    store.publish_unit(
        supervisor.timer_name,
        supervisor.timer_unit.encode(),
        expected_current=None,
    )
    store.record_compensation(
        _compensation_record(
            artifact,
            compensation_id="1" * 32,
            phase="intent",
            reason="timer-activation",
        )
    )
    control = _Control(artifact, store)
    control.loaded = True
    control.enabled = True
    control.active = True
    transport = FixedExternalSupervisorTransport(store=store, control=control)

    transport.reconcile_compensations()

    assert store.compensation_blockers() == {}
    assert store.pending_compensations() == ()
    assert control.timer_status(supervisor.timer_name) == _timer_status(
        supervisor.timer_name, "not-found", "disabled", "inactive"
    )


def test_compensation_terminal_identity_mismatch_remains_blocking(tmp_path: Path) -> None:
    _plan, _root, artifact = _bound_artifact(tmp_path)
    unit_dir = tmp_path / "units"
    unit_dir.mkdir(mode=0o700)
    store = AtomicUserUnitStore(unit_dir=unit_dir, service_uid=os.geteuid())
    compensation_id = "2" * 32
    intent = _compensation_record(
        artifact,
        compensation_id=compensation_id,
        phase="intent",
        reason="timer-activation",
    )
    store.record_compensation(intent)
    store.record_compensation(
        TimerCompensationEvidence.build(
            compensation_id=compensation_id,
            artifact_digest=intent.artifact_digest,
            service_name=intent.service_name,
            timer_name=intent.timer_name,
            service_unit_sha256=intent.service_unit_sha256,
            timer_unit_sha256=intent.timer_unit_sha256,
            predecessor_kind=intent.predecessor_kind,
            predecessor_authority_digest=intent.predecessor_authority_digest,
            predecessor_pointer_digest=intent.predecessor_pointer_digest,
            predecessor_canonical_json=intent.predecessor_canonical_json,
            target_canonical_json=intent.target_canonical_json,
            transition_digest="d" * 64,
            transition_group_id=intent.transition_group_id,
            phase="verified",
            reason="inactive-disabled",
        )
    )

    assert store.compensation_blockers()
    assert store.pending_compensations() == (intent,)


def test_only_canonical_terminal_resolves_successful_activation_intent(tmp_path: Path) -> None:
    _plan, _root, artifact = _bound_artifact(tmp_path)
    unit_dir = tmp_path / "units"
    unit_dir.mkdir(mode=0o700)
    store = AtomicUserUnitStore(unit_dir=unit_dir, service_uid=os.geteuid())
    compensation_id = "b" * 32

    store.record_compensation(
        _compensation_record(
            artifact,
            compensation_id=compensation_id,
            phase="intent",
            reason="timer-activation",
        )
    )
    assert store.compensation_blockers()
    store.record_compensation(
        _compensation_record(
            artifact,
            compensation_id=compensation_id,
            phase="activated",
            reason="timer-active",
        )
    )
    assert store.compensation_blockers()
    store.record_compensation(
        _compensation_record(
            artifact,
            compensation_id=compensation_id,
            phase="canonical",
            reason="canonical-promoted",
        )
    )
    assert store.compensation_blockers() == {}


@pytest.mark.parametrize(
    "name,payload",
    [
        (".loom-external-supervisor-compensation-garbage.json", b"{}\n"),
        (
            ".loom-external-supervisor-compensation-" + "e" * 32 + "-unknown.json",
            b"{}\n",
        ),
        (
            ".loom-external-supervisor-compensation-" + "f" * 32 + "-intent.json",
            b"{",
        ),
    ],
)
def test_compensation_journal_malformed_prefixed_entry_fails_closed(
    tmp_path: Path,
    name: str,
    payload: bytes,
) -> None:
    unit_dir = tmp_path / "units"
    unit_dir.mkdir(mode=0o700)
    (unit_dir / name).write_bytes(payload)
    store = AtomicUserUnitStore(unit_dir=unit_dir, service_uid=os.geteuid())

    with pytest.raises((RuntimeError, ValueError), match="compensation"):
        store.compensation_blockers()


def test_compensation_journal_ignores_only_distinct_temporary_and_lock_files(
    tmp_path: Path,
) -> None:
    unit_dir = tmp_path / "units"
    unit_dir.mkdir(mode=0o700)
    (unit_dir / ".loom-external-supervisor.lock").write_bytes(b"")
    (unit_dir / "..loom-external-supervisor-compensation-write.tmp").write_bytes(b"partial")
    store = AtomicUserUnitStore(unit_dir=unit_dir, service_uid=os.geteuid())

    assert store.compensation_blockers() == {}


def stat_mode(path: Path) -> int:
    return path.stat().st_mode & 0o777


def test_systemd_control_exposes_only_fixed_user_unit_operations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []

    def run(command, **kwargs):
        calls.append(tuple(command))
        if "show" in command and str(command[3]).endswith(".timer"):
            fragment = str(PROTECTED_USER_UNIT_DIR / str(command[3])).encode()
            return SimpleNamespace(
                returncode=0,
                stdout=(
                    b"LoadState=loaded\nUnitFileState=enabled\nActiveState=active\n"
                    + b"FragmentPath="
                    + fragment
                    + b"\nNeedDaemonReload=no\n"
                ),
                stderr=b"",
            )
        if "show" in command:
            fragment = str(PROTECTED_USER_UNIT_DIR / str(command[3])).encode()
            return SimpleNamespace(
                returncode=0,
                stdout=(
                    b"LoadState=loaded\nResult=success\nExecMainStatus=0\n"
                    + b"FragmentPath="
                    + fragment
                    + b"\nNeedDaemonReload=no\n"
                ),
                stderr=b"",
            )
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    monkeypatch.setattr(transport_module.subprocess, "run", run)
    control = FixedUserSystemdControl(service_uid=os.geteuid())
    timer = "loom-autoscaler-test.timer"
    service = "loom-autoscaler-test.service"

    assert control.timer_status(timer) == _timer_status(timer, "loaded", "enabled", "active")
    assert control.service_status(service) == _service_status(service, "loaded", "success", 0)
    control.daemon_reload()
    control.enable_timer(timer)
    control.start_timer(timer)
    control.stop_timer(timer)
    control.disable_timer(timer)
    control.start_service(service, timeout_seconds=20)

    assert calls[2:] == [
        ("systemctl", "--user", "daemon-reload"),
        ("systemctl", "--user", "enable", timer),
        ("systemctl", "--user", "start", timer),
        ("systemctl", "--user", "stop", timer),
        ("systemctl", "--user", "disable", timer),
        ("systemctl", "--user", "start", service),
    ]
    with pytest.raises(ValueError, match="unit name is invalid"):
        control.start_timer("other.timer")


def test_systemd_control_redacts_failed_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        transport_module.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=1,
            stdout=b"",
            stderr=b"raw-secret-value",
        ),
    )
    control = FixedUserSystemdControl(service_uid=os.geteuid())

    with pytest.raises(RuntimeError, match="failed safely") as exc:
        control.daemon_reload()
    assert "raw-secret-value" not in str(exc.value)
