from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

import pytest

import loom_cli.rollout.operator.protected_external_supervisor_transport as transport_module
from loom_cli.rollout.external_supervisor_predecessor import (
    ABSENT_PREDECESSOR_DIGEST,
    NO_TRANSITION_GROUP_ID,
    ExternalSupervisorCanonicalIdentity,
    ExternalSupervisorPredecessorAuthority,
    load_predecessor_manifest,
)
from loom_cli.rollout.external_supervisor_readiness import (
    ExternalSupervisorArtifact,
    build_external_supervisor_artifact,
)
from loom_cli.rollout.operator.protected_external_supervisor_transport import (
    PROTECTED_USER_UNIT_DIR,
    AtomicUserUnitStore,
    FixedExternalSupervisorTransport,
    ServiceRuntimeStatus,
    TimerCompensationEvidence,
    TimerRuntimeStatus,
    classify_external_supervisor_live_state,
)


def _artifact(
    tmp_path: Path,
    *,
    supervisor_count: int = 1,
    timer_on_unit_active_sec: int = 30,
) -> ExternalSupervisorArtifact:
    candidate = tmp_path / "candidate"
    profile = candidate / "deploy/environment-state/staging.toml"
    script = candidate / "scripts/ops/worker_pool_autoscaler_external_once.py"
    profile.parent.mkdir(parents=True)
    script.parent.mkdir(parents=True)
    repository = Path(__file__).resolve().parents[4]
    shutil.copyfile(repository / "deploy/environment-state/staging.toml", profile)
    shutil.copyfile(repository / "scripts/ops/worker_pool_autoscaler_external_once.py", script)
    profile_text = profile.read_text(encoding="utf-8")
    if timer_on_unit_active_sec != 30:
        enablement = "enabled = true\nactive = true\n"
        profile_text = profile_text.replace(
            enablement,
            f'timer_on_unit_active_sec = "{timer_on_unit_active_sec}"\n{enablement}',
            1,
        )
    if supervisor_count == 2:
        profile_text += f"""

[[external_slurm_autoscaler_supervisors]]
name = "gb10-arm64-staging-secondary"
pool_name = "gb10-arm64"
service_name = "loom-autoscaler-gb10-staging-secondary.service"
timer_name = "loom-autoscaler-gb10-staging-secondary.timer"
working_directory = "/opt/loom-staging-runner/candidates/${{GIT_SHA}}/repo"
python_path = "/opt/loom-staging-runner/candidates/${{GIT_SHA}}/venv/bin/python"
script_path = "/opt/loom-staging-runner/candidates/${{GIT_SHA}}/repo/scripts/ops/worker_pool_autoscaler_external_once.py"
args = [
  "--environment", "staging",
  "--pool-name", "gb10-arm64",
  "--namespace", "loom-staging",
  "--kubeconfig", "/var/lib/loom-staging-rollout/kubeconfig",
  "--db-local-host", "127.0.0.1",
  "--db-local-port", "15453",
  "--db-service", "service/loom-postgres",
  "--db-remote-port", "5432",
  "--db-port-forward-ready-timeout-sec", "10",
  "--db-port-forward-stop-timeout-sec", "5",
  "--db-connect-timeout-sec", "10",
  "--freshness-sec", "120",
]
timer_on_unit_active_sec = "{timer_on_unit_active_sec}"
enabled = true
active = true
"""
    profile.write_text(profile_text, encoding="utf-8")
    profile.chmod(0o600)
    script.chmod(0o700)
    return build_external_supervisor_artifact(
        candidate,
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        image_tag="staging-aaaaaaa",
    )


class _Control:
    def __init__(self, store: AtomicUserUnitStore) -> None:
        self.store = store
        self.loaded = True
        self.need_reload = "no"
        self.enabled = True
        self.active = True
        self.result = "success"
        self.status: int | None = 0
        self.calls: list[str] = []
        self.fail_service = False

    def timer_status(self, name: str) -> TimerRuntimeStatus:
        present = self.loaded and self.store.read_unit(name) is not None
        return TimerRuntimeStatus(
            load_state="loaded" if present else "not-found",
            unit_file_state=(
                ("enabled" if self.enabled else "disabled") if present else "not-found"
            ),
            active_state="active" if self.active and present else "inactive",
            fragment_path=str(PROTECTED_USER_UNIT_DIR / name) if present else "",
            need_daemon_reload=self.need_reload if present else "no",
        )

    def service_status(self, name: str) -> ServiceRuntimeStatus:
        present = self.loaded and self.store.read_unit(name) is not None
        return ServiceRuntimeStatus(
            load_state="loaded" if present else "not-found",
            result=self.result if present else "",
            exec_main_status=self.status if present else None,
            fragment_path=str(PROTECTED_USER_UNIT_DIR / name) if present else "",
            need_daemon_reload=self.need_reload if present else "no",
        )

    def daemon_reload(self) -> None:
        self.calls.append("daemon-reload")
        self.loaded = bool(self.store.list_units())
        self.need_reload = "no"

    def enable_timer(self, name: str) -> None:
        self.calls.append(f"enable:{name}")
        self.enabled = True

    def start_timer(self, name: str) -> None:
        self.calls.append(f"start:{name}")
        self.active = True

    def stop_timer(self, name: str) -> None:
        self.calls.append(f"stop:{name}")
        self.active = False

    def disable_timer(self, name: str) -> None:
        self.calls.append(f"disable:{name}")
        self.enabled = False

    def start_service(self, name: str, *, timeout_seconds: float) -> None:
        self.calls.append(f"service:{name}:{timeout_seconds}")
        if self.fail_service:
            raise RuntimeError("service failed")
        self.result = "success"
        self.status = 0


def _store(tmp_path: Path) -> AtomicUserUnitStore:
    unit_dir = tmp_path / "unit-store"
    unit_dir.mkdir(mode=0o700)
    return AtomicUserUnitStore(unit_dir=unit_dir, service_uid=unit_dir.stat().st_uid)


def _legacy_authority() -> ExternalSupervisorPredecessorAuthority:
    legacy = load_predecessor_manifest()
    return ExternalSupervisorPredecessorAuthority(
        kind="legacy-manifest",
        authority_digest=legacy.manifest_digest,
        unit_sha256=legacy.unit_sha256,
    )


def _install_units(
    store: AtomicUserUnitStore,
    payloads: dict[str, str],
) -> None:
    for name, payload in payloads.items():
        store.publish_unit(name, payload.encode(), expected_current=None)


def _target(
    artifact: ExternalSupervisorArtifact,
    *,
    transition_group_id: str = "f" * 32,
) -> ExternalSupervisorCanonicalIdentity:
    return ExternalSupervisorCanonicalIdentity.build(
        artifact,
        plan_digest="c" * 64,
        attestation_digest="d" * 64,
        transition_group_id=transition_group_id,
        runtime_evidence_digest=transport_module._expected_activation_runtime_digest(artifact),
    )


def _intent(
    transport: FixedExternalSupervisorTransport,
    artifact: ExternalSupervisorArtifact,
    *,
    predecessor: ExternalSupervisorCanonicalIdentity,
    authority: ExternalSupervisorPredecessorAuthority,
    compensation_id: str,
) -> TimerCompensationEvidence:
    identity = dict(
        transport._activation_identity(
            artifact,
            artifact.supervisors[0],
            target=_target(artifact),
            predecessor=predecessor,
            authority=authority,
            transition_digest="e" * 64,
        )
    )
    identity["compensation_id"] = compensation_id
    return TimerCompensationEvidence.build(
        **identity,
        phase="intent",
        reason="supervisor-mutation",
    )


def _intents(
    transport: FixedExternalSupervisorTransport,
    artifact: ExternalSupervisorArtifact,
    *,
    target: ExternalSupervisorCanonicalIdentity,
    predecessor: ExternalSupervisorCanonicalIdentity | None,
    authority: ExternalSupervisorPredecessorAuthority,
    transition_digest: str = "e" * 64,
) -> list[TimerCompensationEvidence]:
    return [
        TimerCompensationEvidence.build(
            **{
                **transport._activation_identity(
                    artifact,
                    supervisor,
                    target=target,
                    predecessor=predecessor,
                    authority=authority,
                    transition_digest=transition_digest,
                ),
                "compensation_id": hashlib.sha256(
                    f"{target.transition_group_id}:{supervisor.name}".encode()
                ).hexdigest()[:32],
            },
            phase="intent",
            reason="supervisor-mutation",
        )
        for supervisor in artifact.supervisors
    ]


def _terminal(
    intent: TimerCompensationEvidence,
    *,
    phase: str,
    reason: str,
) -> TimerCompensationEvidence:
    return TimerCompensationEvidence.build(
        compensation_id=intent.compensation_id,
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
        transition_digest=intent.transition_digest,
        transition_group_id=intent.transition_group_id,
        phase=phase,
        reason=reason,
    )


def _absent_authority() -> ExternalSupervisorPredecessorAuthority:
    return ExternalSupervisorPredecessorAuthority(
        kind="absent",
        authority_digest=ABSENT_PREDECESSOR_DIGEST,
        unit_sha256={},
    )


def _record_transition(
    store: AtomicUserUnitStore,
    transport: FixedExternalSupervisorTransport,
    artifact: ExternalSupervisorArtifact,
    *,
    target: ExternalSupervisorCanonicalIdentity,
    predecessor: ExternalSupervisorCanonicalIdentity | None,
    authority: ExternalSupervisorPredecessorAuthority,
    terminal_phase: str | None = None,
    terminal_reason: str | None = None,
) -> list[TimerCompensationEvidence]:
    intents = _intents(
        transport,
        artifact,
        target=target,
        predecessor=predecessor,
        authority=authority,
    )
    for intent in intents:
        store.record_compensation(intent)
    if terminal_phase is not None:
        assert terminal_reason is not None
        for intent in intents:
            store.record_compensation(
                _terminal(intent, phase=terminal_phase, reason=terminal_reason)
            )
    return intents


def _promote_activation(
    store: AtomicUserUnitStore,
    transport: FixedExternalSupervisorTransport,
    artifact: ExternalSupervisorArtifact,
    *,
    target: ExternalSupervisorCanonicalIdentity,
    predecessor: ExternalSupervisorCanonicalIdentity | None = None,
    authority: ExternalSupervisorPredecessorAuthority | None = None,
    close: bool = True,
) -> list[TimerCompensationEvidence]:
    if authority is None:
        authority = _absent_authority()
    intents = _record_transition(
        store,
        transport,
        artifact,
        target=target,
        predecessor=predecessor,
        authority=authority,
        terminal_phase="activated",
        terminal_reason="timer-active",
    )
    store.promote_canonical(target, expected_current=predecessor)
    if close:
        for intent in intents:
            store.record_compensation(
                _terminal(intent, phase="canonical", reason="canonical-promoted")
            )
    return intents


def _delete_transition_records(
    store: AtomicUserUnitStore,
    intents: list[TimerCompensationEvidence],
) -> None:
    for intent in intents:
        paths = tuple(
            store.unit_dir.glob(
                f".loom-external-supervisor-compensation-{intent.compensation_id}-*.json"
            )
        )
        assert paths
        for path in paths:
            path.unlink()


def test_pr907_bytes_are_a_reachable_upgrade_but_arbitrary_bytes_fail_closed(
    tmp_path: Path,
) -> None:
    artifact = _artifact(tmp_path)
    store = _store(tmp_path)
    legacy = load_predecessor_manifest()
    _install_units(store, dict(legacy.unit_payloads))
    transport = FixedExternalSupervisorTransport(store=store, control=_Control(store))
    authority = _legacy_authority()

    live = transport.observe(artifact, authority)
    assert classify_external_supervisor_live_state(artifact, live) == "ready"

    service = artifact.supervisors[0].service_name
    store.publish_unit(service, b"arbitrary\n", expected_current=store.read_unit(service))
    drifted = transport.observe(artifact, authority)
    assert classify_external_supervisor_live_state(artifact, drifted) == "drifted"


def test_successful_upgrade_promotes_one_immutable_activation_and_pointer(
    tmp_path: Path,
) -> None:
    artifact = _artifact(tmp_path)
    store = _store(tmp_path)
    legacy = load_predecessor_manifest()
    _install_units(store, dict(legacy.unit_payloads))
    control = _Control(store)
    transport = FixedExternalSupervisorTransport(store=store, control=control)

    before = transport.observe(artifact, _legacy_authority())
    transport.apply(
        artifact,
        before,
        plan_digest="c" * 64,
        attestation_digest="d" * 64,
        transition_digest="e" * 64,
    )

    active = store.read_canonical()
    assert active is not None
    assert active == _target(artifact, transition_group_id=active.transition_group_id)
    assert active.transition_group_id != NO_TRANSITION_GROUP_ID
    assert store.protected_activation_references() == (active.evidence_digest,)
    assert store.compensation_blockers() == {}
    after = transport.observe(artifact)
    assert classify_external_supervisor_live_state(artifact, after) == "exact"
    phases = {
        path.name.rsplit("-", 1)[-1].removesuffix(".json")
        for path in store.unit_dir.glob(".loom-external-supervisor-compensation-*.json")
    }
    assert phases == {"intent", "activated", "canonical"}


def test_files_current_before_daemon_reload_are_ready_never_exact(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path)
    store = _store(tmp_path)
    target = _target(artifact)
    _install_units(store, dict(target.unit_payloads))
    control = _Control(store)
    transport = FixedExternalSupervisorTransport(store=store, control=control)
    _promote_activation(store, transport, artifact, target=target)
    control.need_reload = "yes"

    assert classify_external_supervisor_live_state(artifact, transport.observe(artifact)) == "ready"


def test_promotion_rejects_complete_group_bound_to_wrong_predecessor(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path)
    predecessor = _target(artifact, transition_group_id="a" * 32)
    target = _target(artifact, transition_group_id="b" * 32)
    store = _store(tmp_path)
    _install_units(store, dict(predecessor.unit_payloads))
    transport = FixedExternalSupervisorTransport(store=store, control=_Control(store))
    _promote_activation(store, transport, artifact, target=predecessor)
    _record_transition(
        store,
        transport,
        artifact,
        target=target,
        predecessor=None,
        authority=_absent_authority(),
        terminal_phase="activated",
        terminal_reason="timer-active",
    )

    with pytest.raises(RuntimeError, match="predecessor journal drifted"):
        store.promote_canonical(target, expected_current=predecessor)

    assert store.read_canonical() == predecessor


def test_mixed_unit_crash_prefix_restores_checked_in_predecessor(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path)
    store = _store(tmp_path)
    legacy_manifest = load_predecessor_manifest()
    predecessor = ExternalSupervisorCanonicalIdentity.from_manifest(legacy_manifest)
    _install_units(store, dict(legacy_manifest.unit_payloads))
    control = _Control(store)
    transport = FixedExternalSupervisorTransport(store=store, control=control)
    intent = _intent(
        transport,
        artifact,
        predecessor=predecessor,
        authority=_legacy_authority(),
        compensation_id="1" * 32,
    )
    store.record_compensation(intent)
    service = artifact.supervisors[0].service_name
    store.publish_unit(
        service,
        _target(artifact).unit_payloads[service].encode(),
        expected_current=store.read_unit(service),
    )
    control.need_reload = "yes"

    transport.reconcile_compensations()

    assert {name: store.read_unit(name) for name in predecessor.unit_payloads} == {
        name: payload.encode() for name, payload in predecessor.unit_payloads.items()
    }
    assert store.read_canonical() is None
    assert store.compensation_blockers() == {}
    assert control.active is True
    assert control.enabled is True
    assert (
        classify_external_supervisor_live_state(
            artifact,
            transport.observe(artifact, _legacy_authority()),
        )
        == "ready"
    )


@pytest.mark.parametrize(
    ("boundary", "units", "need_reload", "activated"),
    [
        ("after-intent", "legacy", "no", False),
        ("after-all-bytes", "target", "yes", False),
        ("after-reload", "target", "no", False),
        ("after-activated", "target", "no", True),
    ],
)
def test_predecessor_pointer_crash_boundaries_restore_whole_legacy_set(
    tmp_path: Path,
    boundary: str,
    units: str,
    need_reload: str,
    activated: bool,
) -> None:
    artifact = _artifact(tmp_path)
    store = _store(tmp_path)
    legacy_manifest = load_predecessor_manifest()
    predecessor = ExternalSupervisorCanonicalIdentity.from_manifest(legacy_manifest)
    target = _target(artifact)
    payloads = legacy_manifest.unit_payloads if units == "legacy" else target.unit_payloads
    _install_units(store, dict(payloads))
    control = _Control(store)
    control.need_reload = need_reload
    transport = FixedExternalSupervisorTransport(store=store, control=control)
    intent = _intent(
        transport,
        artifact,
        predecessor=predecessor,
        authority=_legacy_authority(),
        compensation_id=hashlib.sha256(boundary.encode()).hexdigest()[:32],
    )
    store.record_compensation(intent)
    if activated:
        store.record_compensation(_terminal(intent, phase="activated", reason="timer-active"))
    assert store.compensation_blockers()

    transport.reconcile_compensations()

    assert {name: store.read_unit(name) for name in predecessor.unit_payloads} == {
        name: payload.encode() for name, payload in predecessor.unit_payloads.items()
    }
    assert store.read_canonical() is None
    assert store.compensation_blockers() == {}
    assert control.active is True
    assert control.enabled is True
    assert (
        classify_external_supervisor_live_state(
            artifact,
            transport.observe(artifact, _legacy_authority()),
        )
        == "ready"
    )


def test_pointer_promoted_before_terminal_reactivates_target_and_closes_recovery(
    tmp_path: Path,
) -> None:
    artifact = _artifact(tmp_path)
    store = _store(tmp_path)
    legacy_manifest = load_predecessor_manifest()
    predecessor = ExternalSupervisorCanonicalIdentity.from_manifest(legacy_manifest)
    target = _target(artifact)
    _install_units(store, dict(target.unit_payloads))
    control = _Control(store)
    transport = FixedExternalSupervisorTransport(store=store, control=control)
    intent = _record_transition(
        store,
        transport,
        artifact,
        target=target,
        predecessor=predecessor,
        authority=_legacy_authority(),
        terminal_phase="activated",
        terminal_reason="timer-active",
    )[0]
    store.promote_canonical(target, expected_current=None)
    assert intent.transition_group_id == target.transition_group_id
    assert store.compensation_blockers()

    transport.reconcile_compensations()

    assert store.read_canonical() == target
    assert {name: store.read_unit(name) for name in target.unit_payloads} == {
        name: payload.encode() for name, payload in target.unit_payloads.items()
    }
    assert store.compensation_blockers() == {}
    assert control.active is True
    assert control.enabled is True
    assert classify_external_supervisor_live_state(artifact, transport.observe(artifact)) == "exact"
    phases = {
        path.name.rsplit("-", 1)[-1].removesuffix(".json")
        for path in store.unit_dir.glob(".loom-external-supervisor-compensation-*.json")
    }
    assert phases == {"intent", "activated", "recovered"}


def test_target_recovery_success_supersedes_immutable_prior_failure(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path)
    store = _store(tmp_path)
    predecessor = ExternalSupervisorCanonicalIdentity.from_manifest(load_predecessor_manifest())
    target = _target(artifact)
    _install_units(store, dict(target.unit_payloads))
    control = _Control(store)
    control.fail_service = True
    transport = FixedExternalSupervisorTransport(store=store, control=control)
    _record_transition(
        store,
        transport,
        artifact,
        target=target,
        predecessor=predecessor,
        authority=_legacy_authority(),
        terminal_phase="activated",
        terminal_reason="timer-active",
    )[0]
    store.promote_canonical(target, expected_current=None)

    with pytest.raises(RuntimeError, match="reconciliation failed safely"):
        transport.reconcile_compensations()

    failed_paths = tuple(
        store.unit_dir.glob(".loom-external-supervisor-compensation-*-failed.json")
    )
    assert len(failed_paths) == 1
    failed_payload = failed_paths[0].read_bytes()
    assert store.compensation_blockers()

    control.fail_service = False
    transport.reconcile_compensations()

    assert store.read_canonical() == target
    assert store.compensation_blockers() == {}
    assert failed_paths[0].read_bytes() == failed_payload
    phases = {
        path.name.rsplit("-", 1)[-1].removesuffix(".json")
        for path in store.unit_dir.glob(".loom-external-supervisor-compensation-*.json")
    }
    assert phases == {"intent", "activated", "failed", "recovered"}
    assert control.active is True
    assert control.enabled is True
    assert classify_external_supervisor_live_state(artifact, transport.observe(artifact)) == "exact"


def test_canonical_predecessor_pointer_is_restored_and_reactivated(
    tmp_path: Path,
) -> None:
    artifact = _artifact(tmp_path)
    store = _store(tmp_path)
    predecessor = ExternalSupervisorCanonicalIdentity.build(
        artifact,
        plan_digest="1" * 64,
        attestation_digest="2" * 64,
        transition_group_id="1" * 32,
        runtime_evidence_digest=transport_module._expected_activation_runtime_digest(artifact),
    )
    _install_units(store, dict(predecessor.unit_payloads))
    control = _Control(store)
    transport = FixedExternalSupervisorTransport(store=store, control=control)
    _promote_activation(store, transport, artifact, target=predecessor)
    authority = ExternalSupervisorPredecessorAuthority(
        kind="canonical",
        authority_digest=predecessor.evidence_digest,
        unit_sha256=predecessor.unit_sha256,
    )
    intent = _intent(
        transport,
        artifact,
        predecessor=predecessor,
        authority=authority,
        compensation_id="3" * 32,
    )
    store.record_compensation(intent)

    transport.reconcile_compensations()

    assert store.read_canonical() == predecessor
    assert store.compensation_blockers() == {}
    assert control.active is True
    assert control.enabled is True
    assert (
        classify_external_supervisor_live_state(
            artifact,
            transport.observe(artifact, authority),
        )
        == "exact"
    )


def test_absent_state_is_not_self_authoritative_on_staging(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path)
    store = _store(tmp_path)
    transport = FixedExternalSupervisorTransport(store=store, control=_Control(store))

    with pytest.raises(RuntimeError, match="not authoritative"):
        transport.observe(artifact)
    absent = ExternalSupervisorPredecessorAuthority(
        kind="absent",
        authority_digest=ABSENT_PREDECESSOR_DIGEST,
        unit_sha256={},
    )
    assert (
        classify_external_supervisor_live_state(
            artifact,
            transport.observe(artifact, absent),
        )
        == "ready"
    )


def test_crash_between_sibling_intents_restores_and_reactivates_full_predecessor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = _artifact(tmp_path / "target", supervisor_count=2)
    predecessor_artifact = _artifact(
        tmp_path / "predecessor",
        supervisor_count=2,
        timer_on_unit_active_sec=60,
    )
    target = _target(artifact)
    predecessor = ExternalSupervisorCanonicalIdentity.build(
        predecessor_artifact,
        plan_digest="1" * 64,
        attestation_digest="2" * 64,
        transition_group_id="1" * 32,
        runtime_evidence_digest=transport_module._expected_activation_runtime_digest(
            predecessor_artifact
        ),
    )
    assert set(target.unit_payloads) == set(predecessor.unit_payloads)
    assert dict(target.unit_payloads) != dict(predecessor.unit_payloads)

    store = _store(tmp_path)
    _install_units(store, dict(predecessor.unit_payloads))
    control = _Control(store)
    transport = FixedExternalSupervisorTransport(store=store, control=control)
    _promote_activation(store, transport, predecessor_artifact, target=predecessor)
    authority = ExternalSupervisorPredecessorAuthority(
        kind="canonical",
        authority_digest=predecessor.evidence_digest,
        unit_sha256=predecessor.unit_sha256,
    )
    intents = _intents(
        transport,
        artifact,
        target=target,
        predecessor=predecessor,
        authority=authority,
    )
    publications = {
        name: (predecessor.unit_payloads[name].encode(), target.unit_payloads[name].encode())
        for name in target.unit_payloads
    }

    original_publish = AtomicUserUnitStore._publish_exact_locked
    intent_writes = 0

    def crash_before_second_intent(
        self: AtomicUserUnitStore,
        directory: int,
        name: str,
        payload: bytes,
        *,
        expected_current: bytes | None,
    ) -> None:
        nonlocal intent_writes
        if name.endswith("-intent.json"):
            intent_writes += 1
            if intent_writes == 2:
                raise RuntimeError("simulated crash between sibling intents")
        original_publish(
            self,
            directory,
            name,
            payload,
            expected_current=expected_current,
        )

    monkeypatch.setattr(AtomicUserUnitStore, "_publish_exact_locked", crash_before_second_intent)
    with pytest.raises(RuntimeError, match="between sibling intents"):
        store.publish_transition(intents, publications)

    pending = store.pending_compensations()
    assert len(pending) == 1
    assert {name: store.read_unit(name) for name in predecessor.unit_payloads} == {
        name: payload.encode() for name, payload in predecessor.unit_payloads.items()
    }

    transport.reconcile_compensations()

    assert store.read_canonical() == predecessor
    assert {name: store.read_unit(name) for name in predecessor.unit_payloads} == {
        name: payload.encode() for name, payload in predecessor.unit_payloads.items()
    }
    assert store.compensation_blockers() == {}
    for supervisor in artifact.supervisors:
        assert any(call.startswith(f"service:{supervisor.service_name}:") for call in control.calls)
        assert f"enable:{supervisor.timer_name}" in control.calls
        assert f"start:{supervisor.timer_name}" in control.calls


@pytest.mark.parametrize("journal_shape", ["deleted", "mismatched"])
def test_active_target_rejects_deleted_or_mismatched_sibling_journal(
    tmp_path: Path,
    journal_shape: str,
) -> None:
    artifact = _artifact(tmp_path, supervisor_count=2)
    target = _target(artifact)
    store = _store(tmp_path)
    _install_units(store, dict(target.unit_payloads))
    transport = FixedExternalSupervisorTransport(store=store, control=_Control(store))
    intents = _promote_activation(store, transport, artifact, target=target)
    assert store.compensation_blockers() == {}

    _delete_transition_records(store, [intents[1]])
    if journal_shape == "mismatched":
        first = intents[0]
        store.record_compensation(
            TimerCompensationEvidence.build(
                compensation_id=intents[1].compensation_id,
                artifact_digest=first.artifact_digest,
                service_name=first.service_name,
                timer_name=first.timer_name,
                service_unit_sha256=first.service_unit_sha256,
                timer_unit_sha256=first.timer_unit_sha256,
                predecessor_kind=first.predecessor_kind,
                predecessor_authority_digest=first.predecessor_authority_digest,
                predecessor_pointer_digest=first.predecessor_pointer_digest,
                predecessor_canonical_json=first.predecessor_canonical_json,
                target_canonical_json=first.target_canonical_json,
                transition_digest=first.transition_digest,
                transition_group_id=first.transition_group_id,
                phase="intent",
                reason="supervisor-mutation",
            )
        )

    with pytest.raises(RuntimeError, match="reconciliation failed safely"):
        transport.reconcile_compensations()

    assert store.read_canonical() == target
    assert {name: store.read_unit(name) for name in target.unit_payloads} == {
        name: payload.encode() for name, payload in target.unit_payloads.items()
    }
    assert target.transition_group_id in store.compensation_blockers()
    assert not tuple(store.unit_dir.glob(".loom-external-supervisor-compensation-*-recovered.json"))


def test_active_target_rejects_deleted_entire_transition_group(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path, supervisor_count=2)
    target = _target(artifact)
    store = _store(tmp_path)
    _install_units(store, dict(target.unit_payloads))
    transport = FixedExternalSupervisorTransport(store=store, control=_Control(store))
    intents = _promote_activation(store, transport, artifact, target=target)
    assert store.compensation_blockers() == {}

    _delete_transition_records(store, intents)

    assert store.compensation_blockers().keys() == {target.transition_group_id}
    with pytest.raises(RuntimeError, match="reconciliation failed safely"):
        transport.reconcile_compensations()
    assert store.read_canonical() == target
    assert {name: store.read_unit(name) for name in target.unit_payloads} == {
        name: payload.encode() for name, payload in target.unit_payloads.items()
    }


def test_stale_prior_group_cannot_substitute_for_deleted_active_group(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path, supervisor_count=2)
    prior = _target(artifact, transition_group_id="a" * 32)
    active = _target(artifact, transition_group_id="b" * 32)
    store = _store(tmp_path)
    _install_units(store, dict(prior.unit_payloads))
    transport = FixedExternalSupervisorTransport(store=store, control=_Control(store))
    _promote_activation(store, transport, artifact, target=prior)
    prior_authority = ExternalSupervisorPredecessorAuthority(
        kind="canonical",
        authority_digest=prior.evidence_digest,
        unit_sha256=prior.unit_sha256,
    )
    active_intents = _promote_activation(
        store,
        transport,
        artifact,
        target=active,
        predecessor=prior,
        authority=prior_authority,
    )
    assert store.compensation_blockers() == {}

    _delete_transition_records(store, active_intents)

    assert active.transition_group_id in store.compensation_blockers()
    assert prior.transition_group_id not in store.compensation_blockers()
    with pytest.raises(RuntimeError, match="reconciliation failed safely"):
        transport.reconcile_compensations()
    assert store.read_canonical() == active
