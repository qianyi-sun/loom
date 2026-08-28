from __future__ import annotations

import io
import json
import os
import signal
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.failure_authority import classify_rollout_failure
from loom_cli.rollout.final_attestation_admission import FinalAttestationAdmissionError
from loom_cli.rollout.lifecycle_protocol import LifecycleAction, LifecyclePhase
from loom_cli.rollout.operator import worker as worker_module
from loom_cli.rollout.operator.backup import BackupCreator, BackupError
from loom_cli.rollout.operator.backup_job import PreflightBackupJobEnvelope, transition_backup_job
from loom_cli.rollout.operator.broker import main as broker_main
from loom_cli.rollout.operator.model import ActivePointer, DriverEnvelope, RequestEvent
from loom_cli.rollout.operator.policy import sanitized_child_environment
from loom_cli.rollout.operator.readonly_database_client import ReadonlyDatabaseTunnelError
from loom_cli.rollout.operator.store import RequestStore, RequestStoreError
from loom_cli.rollout.operator.worker import (
    VerifiedBackupJob,
    WorkerDependencies,
    run_attempt,
    run_backup_job,
)
from loom_cli.rollout.operator.worker import main as worker_main
from loom_cli.rollout.preflight_contract import CheckOperation, StageCapability
from loom_cli.rollout.preflight_pipeline import (
    PreflightAssessmentDriftError,
    PreflightAssessmentDriftReason,
)
from tests.loom_cli.rollout.operator.test_backup import RecordingRunner
from tests.loom_cli.rollout.operator.test_backup import make_config as make_backup_config
from tests.loom_cli.rollout.operator.test_broker import fakes as broker_fakes
from tests.loom_cli.rollout.operator.test_broker import make_config
from tests.loom_cli.rollout.operator.test_store import (
    make_assessment,
    make_preflight_backup_job,
    make_preflight_request,
)

REQUEST_ID = "req-alpha"


def valid_envelope() -> DriverEnvelope:
    return DriverEnvelope(
        schema_version=1,
        request_id=REQUEST_ID,
        rollout_id="rollout-alpha",
        initiating_operator="hongjian",
        initiating_uid=2002,
        attempt_number=1,
        attempt_operator="hongjian",
        attempt_uid=2002,
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="origin/dev",
        resolved_sha="a" * 40,
        image_tag="staging-aaaaaaa",
        fetched_at="2026-07-14T12:00:00Z",
        backup_manifest_path="/data/loom-staging/backups/fixed/backup-manifest.json",
        backup_manifest_sha256="2" * 64,
        runner_config_sha256="1" * 64,
        preflight_attestation_sha256="3" * 64,
        preflight_registry_sha256="4" * 64,
        preflight_coverage_sha256="5" * 64,
        cluster_name="loom-staging",
        namespace="loom-staging",
        environment="staging",
        cp_url="http://127.0.0.1:18081",
        cluster_config_path="/opt/loom-staging-runner/repo/deploy/environments/staging.cluster.toml",
        rollout_root="/data/loom-staging",
        admin_token_source="file:/var/lib/loom-staging-rollout/credentials/admin-token",
        worker_token_source="file:/var/lib/loom-staging-rollout/credentials/worker-token",
        service_token_source="file:/var/lib/loom-staging-rollout/credentials/service-token",
        expect_admin_token_fingerprint="sha256:abc123def456 len=64",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="11111111-1111-4111-8111-111111111111",
        scope="current-gb10",
        gb10_prep_concurrency=8,
        resume=False,
        resolved_tree="b" * 40,
    )


class FakeStore:
    def __init__(self, envelope: DriverEnvelope, order: list[str]) -> None:
        self.envelope = envelope
        self.order = order
        self.events: list[RequestEvent] = []
        self.active = ActivePointer(
            request_id=envelope.request_id,
            attempt_number=envelope.attempt_number,
            unit_name=f"loom-staging-rollout-{envelope.request_id}-{envelope.attempt_number}.service",
            status="pending",
        )
        self.active_history: list[ActivePointer | None] = [self.active]
        original = make_preflight_request()
        self.preflight_request = replace(
            original,
            request_id=envelope.request_id,
            rollout_id=envelope.rollout_id,
            candidate=replace(
                original.candidate,
                resolved_sha=envelope.resolved_sha,
                image_tag=envelope.image_tag,
                resolved_tree=envelope.resolved_tree,
            ),
            candidate_tree=envelope.resolved_tree or "b" * 40,
            mutation_epoch=7,
        )

    def read_attempt_envelope(self, request_id: str, attempt_number: int) -> DriverEnvelope:
        self.order.append("envelope-read")
        return self.envelope

    def read_preflight_request(self, request_id: str):  # type: ignore[no-untyped-def]
        self.order.append("preflight-request-read")
        assert request_id == self.preflight_request.request_id
        return self.preflight_request

    def append_event(self, event: RequestEvent) -> Path:
        self.events.append(event)
        return Path("/events")

    def read_events(self, request_id: str) -> list[RequestEvent]:
        return list(self.events)

    def read_active(self) -> ActivePointer | None:
        return self.active

    def set_active(self, pointer: ActivePointer) -> Path:
        current = self.active
        if current is None or (
            current.request_id,
            current.attempt_number,
            current.unit_name,
        ) != (
            pointer.request_id,
            pointer.attempt_number,
            pointer.unit_name,
        ):
            raise RuntimeError("active pointer identity mismatch")
        self.active = pointer
        self.active_history.append(pointer)
        self.order.append(f"active-{pointer.status}")
        return Path("/active")

    def clear_active_if_matches(self, pointer: ActivePointer) -> bool:
        if self.active != pointer:
            return False
        self.active = None
        self.active_history.append(None)
        return True


class FakeLifecycle:
    def __init__(self, store: FakeStore, order: list[str]) -> None:
        self.store = store
        self.order = order

    @contextmanager
    def driver_guard(self):  # type: ignore[no-untyped-def]
        self.order.append("driver-lock-acquire")
        try:
            yield
        finally:
            self.order.append("driver-lock-release")

    def release_active(self, pointer: ActivePointer) -> bool:
        self.order.append(f"active-clear-{pointer.status}")
        return self.store.clear_active_if_matches(pointer)


@dataclass
class Bundle:
    deps: WorkerDependencies
    store: FakeStore
    order: list[str]


class FakeMutationGuard:
    def __init__(
        self,
        order: list[str],
        *,
        candidate_sha: str = "a" * 40,
        mutation_epoch: int = 7,
        release_error: Exception | None = None,
        record: bool = True,
    ) -> None:
        self.order = order
        self.candidate_sha = candidate_sha
        self.mutation_epoch = mutation_epoch
        self.release_error = release_error
        self.record = record
        self.ready: list[str] = []
        self.released: list[str] = []

    def acquire(self, request_id: str):  # type: ignore[no-untyped-def]
        raise AssertionError(f"worker must not acquire guard for {request_id}")

    def assert_ready(self, request_id: str):  # type: ignore[no-untyped-def]
        if self.record:
            self.order.append("guard-ready")
        self.ready.append(request_id)
        return SimpleNamespace(
            request_id=request_id,
            candidate_sha=self.candidate_sha,
            candidate_tree="b" * 40,
            mutation_epoch=self.mutation_epoch,
            state="ready",
        )

    def release(self, request_id: str):  # type: ignore[no-untyped-def]
        if self.record:
            self.order.append("guard-release")
        self.released.append(request_id)
        if self.release_error is not None:
            raise self.release_error
        return SimpleNamespace(request_id=request_id, state="released")


def worker_fakes(*, driver_rc: int = 0) -> Bundle:
    envelope = valid_envelope()
    order: list[str] = []
    store = FakeStore(envelope, order)
    lifecycle = FakeLifecycle(store, order)
    persist_event = store.append_event

    def append_event(event: RequestEvent) -> Path:
        order.append(event.event)
        return persist_event(event)

    store.append_event = append_event  # type: ignore[method-assign]

    def run_driver(envelope_path: Path, resume: bool) -> int:
        order.append("driver-run")
        return driver_rc

    deps = WorkerDependencies(
        store=store,  # type: ignore[arg-type]
        lifecycle=lifecycle,  # type: ignore[arg-type]
        run_driver=run_driver,
        now=lambda: "2026-07-14T12:00:00Z",
        stderr=io.StringIO(),
        state_root=Path("/var/lib/loom-staging-rollout"),
        mutation_guard=FakeMutationGuard(order, record=False),
    )
    return Bundle(deps, store, order)


def advanced_epoch_resume_fakes() -> tuple[Bundle, DriverEnvelope]:
    bundle = worker_fakes()
    envelope = replace(valid_envelope(), attempt_number=2, resume=True)
    pointer = ActivePointer(
        request_id=envelope.request_id,
        attempt_number=envelope.attempt_number,
        unit_name=f"loom-staging-rollout-{envelope.request_id}-2.service",
        status="pending",
    )
    bundle.store.envelope = envelope
    bundle.store.active = pointer
    bundle.store.active_history = [pointer]
    return bundle, envelope


@pytest.mark.parametrize("driver_rc", [0, 1])
def test_attempt_releases_guard_before_every_terminal_event(driver_rc: int) -> None:
    bundle = worker_fakes(driver_rc=driver_rc)
    guard = FakeMutationGuard(bundle.order)
    dependencies = replace(bundle.deps, mutation_guard=guard)

    assert run_attempt(valid_envelope(), dependencies) == driver_rc

    terminal = "attempt_done" if driver_rc == 0 else "attempt_failed"
    assert bundle.order.index("guard-ready") < bundle.order.index("driver-run")
    assert bundle.order.index("guard-release") < bundle.order.index(terminal)
    assert guard.released == [REQUEST_ID]


def test_attempt_claims_guard_and_validates_original_binding_before_store_or_driver_lock() -> None:
    bundle = worker_fakes()
    guard = FakeMutationGuard(bundle.order)
    dependencies = replace(bundle.deps, mutation_guard=guard)

    assert run_attempt(valid_envelope(), dependencies) == 0

    assert bundle.order.index("guard-ready") < bundle.order.index("preflight-request-read")
    assert bundle.order.index("guard-ready") < bundle.order.index("envelope-read")
    assert bundle.order.index("guard-ready") < bundle.order.index("driver-lock-acquire")


def test_attempt_accepts_exact_advanced_epoch_resume_before_driver_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle, envelope = advanced_epoch_resume_fakes()
    guard = FakeMutationGuard(bundle.order, mutation_epoch=8)

    def find_recovery(state_root: Path, **bindings: object) -> int:
        assert state_root == Path("/var/lib/loom-staging-rollout")
        assert bindings == {
            "request_id": REQUEST_ID,
            "through_attempt": 1,
            "candidate_sha": "a" * 40,
            "attestation_digest": "3" * 64,
            "starting_mutation_epoch": 7,
            "service_uid": os.geteuid(),
        }
        return 1

    monkeypatch.setattr(worker_module, "find_advanced_epoch_attempt", find_recovery)
    dependencies = replace(bundle.deps, mutation_guard=guard)

    assert run_attempt(envelope, dependencies) == 0

    assert "driver-run" in bundle.order
    assert guard.released == [REQUEST_ID]


@pytest.mark.parametrize("recovery_attempt", [None, 1])
def test_attempt_rejects_unproven_or_over_advanced_epoch_resume_before_driver_lock(
    monkeypatch: pytest.MonkeyPatch,
    recovery_attempt: int | None,
) -> None:
    bundle, envelope = advanced_epoch_resume_fakes()
    guard = FakeMutationGuard(
        bundle.order,
        mutation_epoch=8 if recovery_attempt is None else 9,
    )
    monkeypatch.setattr(
        worker_module,
        "find_advanced_epoch_attempt",
        lambda *_args, **_kwargs: recovery_attempt,
    )
    dependencies = replace(bundle.deps, mutation_guard=guard)

    with pytest.raises(ValueError, match="staging mutation guard binding drifted"):
        run_attempt(envelope, dependencies)

    assert "driver-lock-acquire" not in bundle.order
    assert "driver-run" not in bundle.order
    assert guard.released == [REQUEST_ID]


def test_attempt_rejects_invalid_advanced_epoch_recovery_before_driver_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle, envelope = advanced_epoch_resume_fakes()
    guard = FakeMutationGuard(bundle.order, mutation_epoch=8)

    def reject_recovery(*_args: object, **_kwargs: object) -> None:
        raise ValueError("protected apply recovery plan binding drifted")

    monkeypatch.setattr(worker_module, "find_advanced_epoch_attempt", reject_recovery)
    dependencies = replace(bundle.deps, mutation_guard=guard)

    with pytest.raises(ValueError, match="protected apply recovery plan binding drifted"):
        run_attempt(envelope, dependencies)

    assert "driver-lock-acquire" not in bundle.order
    assert "driver-run" not in bundle.order
    assert guard.released == [REQUEST_ID]


@pytest.mark.parametrize("drift", ["tree", "epoch"])
def test_attempt_rejects_original_tree_or_epoch_drift_before_driver_lock(drift: str) -> None:
    bundle = worker_fakes()
    guard = FakeMutationGuard(bundle.order)
    if drift == "tree":
        bundle.store.preflight_request = replace(
            bundle.store.preflight_request,
            candidate=replace(
                bundle.store.preflight_request.candidate,
                resolved_tree="c" * 40,
            ),
            candidate_tree="c" * 40,
        )
    else:
        bundle.store.preflight_request = replace(
            bundle.store.preflight_request,
            mutation_epoch=8,
        )
    dependencies = replace(bundle.deps, mutation_guard=guard)

    with pytest.raises(ValueError, match="binding drifted"):
        run_attempt(valid_envelope(), dependencies)

    assert "driver-lock-acquire" not in bundle.order
    assert "driver-run" not in bundle.order
    assert guard.released == [REQUEST_ID]


def test_attempt_release_failure_prevents_apparent_success_terminal_event() -> None:
    bundle = worker_fakes()
    guard = FakeMutationGuard(bundle.order, release_error=RuntimeError("release failed"))
    dependencies = replace(bundle.deps, mutation_guard=guard)

    with pytest.raises(RuntimeError, match="release failed"):
        run_attempt(valid_envelope(), dependencies)

    assert "attempt_done" not in bundle.order
    assert bundle.store.active is not None


def test_worker_holds_lifecycle_lock_and_runs_only_finalized_envelope() -> None:
    bundle = worker_fakes()
    assert run_attempt(valid_envelope(), bundle.deps) == 0
    assert bundle.order == [
        "preflight-request-read",
        "driver-lock-acquire",
        "envelope-read",
        "active-running",
        "attempt_running",
        "driver-run",
        "attempt_done",
        "active-clear-running",
        "driver-lock-release",
    ]
    assert [
        pointer.status if pointer is not None else None for pointer in bundle.store.active_history
    ] == [
        "pending",
        "running",
        None,
    ]


def test_worker_refuses_driver_when_final_attestation_admission_fails() -> None:
    bundle = worker_fakes()

    def reject(_envelope: DriverEnvelope) -> object:
        bundle.order.append("final-admission")
        raise ValueError("drifted")

    bundle.deps.final_admission = reject

    assert run_attempt(valid_envelope(), bundle.deps) == 1
    assert bundle.order == [
        "preflight-request-read",
        "driver-lock-acquire",
        "envelope-read",
        "final-admission",
        "attempt_failed",
        "active-clear-pending",
        "driver-lock-release",
    ]
    assert bundle.store.events[-1].reason == "preflight.attestation.final-admission@static"
    assert bundle.store.events[-1].current_step == "00-final-admission"
    assert bundle.store.active is None


def test_worker_publishes_bounded_final_admission_failure_identity() -> None:
    bundle = worker_fakes()

    def reject(_envelope: DriverEnvelope) -> object:
        raise FinalAttestationAdmissionError("evidence-drift", "secret details")

    bundle.deps.final_admission = reject

    assert run_attempt(valid_envelope(), bundle.deps) == 1
    assert bundle.store.events[-1].reason == (
        "preflight.attestation.final-admission.evidence-drift@static"
    )
    assert "secret" not in bundle.store.events[-1].reason


def test_worker_uses_attested_final_gates_instead_of_legacy_driver() -> None:
    bundle = worker_fakes()

    def run_final(_envelope: DriverEnvelope, _admission: object) -> int:
        bundle.order.append("final-gates-run")
        return 0

    bundle.deps.final_admission = lambda _envelope: object()
    bundle.deps.run_final_gates = run_final

    assert run_attempt(valid_envelope(), bundle.deps) == 0
    assert "final-gates-run" in bundle.order
    assert "driver-run" not in bundle.order


def test_attested_worker_never_falls_back_to_legacy_driver() -> None:
    bundle = worker_fakes()
    bundle.deps.final_admission = lambda _envelope: object()

    assert run_attempt(valid_envelope(), bundle.deps) == 1
    assert "driver-run" not in bundle.order
    assert bundle.store.events[-1].reason == ("final.protected-apply.runner-unavailable@final-only")
    assert bundle.store.events[-1].current_step == "00-final-gate-runner"
    assert bundle.store.active is None


def test_final_gate_runner_without_admission_never_reaches_driver() -> None:
    bundle = worker_fakes()
    bundle.deps.run_final_gates = lambda _envelope, _admission: 0

    assert run_attempt(valid_envelope(), bundle.deps) == 1
    assert "driver-run" not in bundle.order
    assert bundle.store.events[-1].reason == (
        "preflight.attestation.final-admission-missing@static"
    )
    assert bundle.store.events[-1].current_step == "00-final-admission"
    assert bundle.store.active is None


def test_worker_records_failed_driver_and_clears_active() -> None:
    bundle = worker_fakes(driver_rc=2)
    assert run_attempt(valid_envelope(), bundle.deps) == 1
    assert "attempt_failed" in bundle.order
    assert bundle.store.active is None


def test_worker_publishes_normalized_driver_failure_stage() -> None:
    bundle = worker_fakes(driver_rc=2)
    bundle.deps.read_driver_failure = lambda envelope: classify_rollout_failure(
        step_number=15,
        step_name="smoke",
        reason="protected route failed",
    )

    assert run_attempt(valid_envelope(), bundle.deps) == 1
    terminal = bundle.store.events[-1]
    assert terminal.event == "attempt_failed"
    assert terminal.reason == "final.smoke.failed@final-only"
    assert terminal.current_step == "15-smoke"


def test_worker_observes_cancel_marker_without_editing_rollout_state() -> None:
    bundle = worker_fakes(driver_rc=2)
    bundle.store.events.append(
        RequestEvent(
            request_id=REQUEST_ID,
            event="cancel_requested",
            occurred_at="2026-07-14T12:00:00Z",
            operator="devansh",
            operator_uid=2003,
            attempt_number=1,
            unit_name=f"loom-staging-rollout-{REQUEST_ID}-1.service",
            status="running",
            reason="validation abandoned",
        )
    )

    assert run_attempt(valid_envelope(), bundle.deps) == 130
    assert bundle.store.events[-1].event == "cancelled"


def test_worker_ignores_cancel_intent_compensated_after_termination_failure() -> None:
    bundle = worker_fakes()
    for event_type, reason in (
        ("cancel_requested", "validation abandoned"),
        ("cancel_failed", "unit_termination_failed"),
    ):
        bundle.store.events.append(
            RequestEvent(
                request_id=REQUEST_ID,
                event=event_type,  # type: ignore[arg-type]
                occurred_at="2026-07-14T12:00:00Z",
                operator="devansh",
                operator_uid=2003,
                attempt_number=1,
                unit_name=f"loom-staging-rollout-{REQUEST_ID}-1.service",
                status="running",
                reason=reason,
            )
        )

    assert run_attempt(valid_envelope(), bundle.deps) == 0
    assert bundle.store.events[-1].event == "attempt_done"


def test_driver_window_rechecks_pending_signal_before_entering_driver() -> None:
    controller = worker_module._SignalController(requested=True)
    entered = False

    with pytest.raises(worker_module._CancellationSignal):
        with controller.driver_window():
            entered = True

    assert entered is False
    assert controller.driver_interruptible is False


def test_worker_parser_exposes_only_internal_run_attempt_surface() -> None:
    bundle = worker_fakes()
    assert worker_main(["start"], dependencies=bundle.deps) == 2
    assert worker_main(["run-attempt"], dependencies=bundle.deps) == 2
    assert (
        worker_main(
            ["run-attempt", "--env", "/tmp/envelope.json"],
            dependencies=bundle.deps,
        )
        == 2
    )


def _backup_worker_store(tmp_path: Path) -> tuple[RequestStore, PreflightBackupJobEnvelope]:
    store = RequestStore(tmp_path / "state")
    assessment = make_assessment(tmp_path)
    request = replace(
        make_preflight_request(),
        request_id=REQUEST_ID,
        preflight_assessment_sha256=assessment.assessment_digest,
        preflight_registry_sha256=assessment.registry_digest,
        preflight_coverage_sha256=assessment.coverage_digest,
    )
    job = replace(
        make_preflight_backup_job(),
        request_id=REQUEST_ID,
        job_id="job-alpha000",
        payload_id="payload-alpha000",
        preflight_assessment_sha256=assessment.assessment_digest,
        preflight_registry_sha256=assessment.registry_digest,
        preflight_coverage_sha256=assessment.coverage_digest,
    )
    store.create_preflight_request(request)
    store.publish_preflight_assessment(REQUEST_ID, assessment)
    store.publish_preflight_backup_job(job)
    return store, job


def _backup_mutation_guard(job: PreflightBackupJobEnvelope) -> FakeMutationGuard:
    return FakeMutationGuard(
        [],
        candidate_sha=job.candidate_sha,
        mutation_epoch=job.mutation_epoch,
        record=False,
    )


def test_backup_worker_publishes_only_verified_cas_state(tmp_path: Path) -> None:
    store, job = _backup_worker_store(tmp_path)
    deps = WorkerDependencies(
        store=store,
        lifecycle=object(),
        run_driver=lambda _path, _resume: 0,
        run_backup=lambda _request, _job, _cancelled: VerifiedBackupJob(
            manifest_path=tmp_path / "backup-manifest.json",
            manifest_sha256="d" * 64,
            lease_digest="e" * 64,
            preflight_attestation_sha256="f" * 64,
        ),
        mutation_guard=_backup_mutation_guard(job),
        now=lambda: "2026-07-19T22:00:00Z",
        stderr=io.StringIO(),
    )

    assert run_backup_job(job, deps) == 0
    state = store.read_preflight_backup_job_state(REQUEST_ID)
    assert state.phase is LifecyclePhase.BACKUP_VERIFIED
    assert state.manifest_sha256 == "d" * 64
    assert state.lease_digest == "e" * 64
    assert state.preflight_attestation_sha256 == "f" * 64


def test_backup_worker_claims_guard_before_any_mutable_store_read(tmp_path: Path) -> None:
    store, job = _backup_worker_store(tmp_path)
    order: list[str] = []
    guard = FakeMutationGuard(
        order,
        candidate_sha=job.candidate_sha,
        mutation_epoch=job.mutation_epoch,
    )
    original_read = store.read_preflight_backup_job

    def read_preflight_backup_job(request_id: str):  # type: ignore[no-untyped-def]
        order.append("store-read")
        return original_read(request_id)

    store.read_preflight_backup_job = read_preflight_backup_job  # type: ignore[method-assign]
    deps = WorkerDependencies(
        store=store,
        lifecycle=object(),
        run_driver=lambda _path, _resume: 0,
        run_backup=lambda _request, _job, _cancelled: VerifiedBackupJob(
            manifest_path=tmp_path / "backup-manifest.json",
            manifest_sha256="d" * 64,
            lease_digest="e" * 64,
            preflight_attestation_sha256="f" * 64,
        ),
        mutation_guard=guard,
        now=lambda: "2026-07-19T22:00:00Z",
        stderr=io.StringIO(),
    )

    assert run_backup_job(job, deps) == 0
    assert order.index("guard-ready") < order.index("store-read")


def test_backup_worker_promotes_verified_request_then_launches_exact_attempt(
    tmp_path: Path,
) -> None:
    store, job = _backup_worker_store(tmp_path)
    config = make_config(tmp_path)
    order: list[str] = []

    class LaunchLifecycle:
        def __init__(self) -> None:
            self.launched: list[DriverEnvelope] = []

        def launch(self, envelope: DriverEnvelope) -> ActivePointer:
            order.append("launch")
            self.launched.append(envelope)
            return ActivePointer(
                request_id=envelope.request_id,
                attempt_number=envelope.attempt_number,
                unit_name=f"loom-staging-rollout-{envelope.request_id}-1.service",
                status="pending",
            )

    lifecycle = LaunchLifecycle()
    guard = FakeMutationGuard(
        order,
        candidate_sha=job.candidate_sha,
        mutation_epoch=job.mutation_epoch,
    )

    def reconcile(
        envelope: PreflightBackupJobEnvelope,
        verified: VerifiedBackupJob,
    ) -> None:
        assert envelope == job
        assert verified.manifest_sha256 == "d" * 64
        assert (
            store.read_preflight_backup_job_state(REQUEST_ID).phase is LifecyclePhase.BACKUP_RUNNING
        )
        order.append("reconcile")

    def finalize(request, verified):  # type: ignore[no-untyped-def]
        order.append("finalize")
        return worker_module._finalize_verified_backup(  # type: ignore[attr-defined]
            config,
            store,
            request,
            verified,
        )

    deps = WorkerDependencies(
        store=store,
        lifecycle=lifecycle,
        run_driver=lambda _path, _resume: 0,
        run_backup=lambda _request, _job, _cancelled: VerifiedBackupJob(
            manifest_path=tmp_path / "backup-manifest.json",
            manifest_sha256="d" * 64,
            lease_digest="e" * 64,
            preflight_attestation_sha256="f" * 64,
        ),
        finalize_backup=finalize,
        reconcile_verified_backup=reconcile,
        mutation_guard=guard,
        now=lambda: "2026-07-19T22:00:00Z",
        stderr=io.StringIO(),
    )

    assert run_backup_job(job, deps) == 0

    state = store.read_preflight_backup_job_state(REQUEST_ID)
    assert state.phase is LifecyclePhase.LAUNCH_RUNNING
    request = store.read_request(REQUEST_ID)
    envelope = store.read_attempt_envelope(REQUEST_ID, 1)
    assert request.preflight_attestation_sha256 == "f" * 64
    assert envelope.backup_manifest_sha256 == "d" * 64
    assert envelope.preflight_attestation_sha256 == "f" * 64
    assert lifecycle.launched == [envelope]
    assert order == ["guard-ready", "reconcile", "finalize", "launch"]
    assert guard.released == []


def test_backup_worker_recovery_failure_blocks_publish_and_launch(tmp_path: Path) -> None:
    store, job = _backup_worker_store(tmp_path)
    finalized = False
    order: list[str] = []
    guard = FakeMutationGuard(
        order,
        candidate_sha=job.candidate_sha,
        mutation_epoch=job.mutation_epoch,
    )

    def finalize(_request, _verified):  # type: ignore[no-untyped-def]
        nonlocal finalized
        finalized = True
        raise AssertionError("recovery failure must block finalization")

    deps = WorkerDependencies(
        store=store,
        lifecycle=object(),
        run_driver=lambda _path, _resume: 0,
        run_backup=lambda _request, _job, _cancelled: VerifiedBackupJob(
            manifest_path=tmp_path / "backup-manifest.json",
            manifest_sha256="d" * 64,
            lease_digest="e" * 64,
            preflight_attestation_sha256="f" * 64,
        ),
        finalize_backup=finalize,
        reconcile_verified_backup=lambda _envelope, _verified: (_ for _ in ()).throw(
            RuntimeError("rotation convergence failed")
        ),
        mutation_guard=guard,
        now=lambda: "2026-07-19T22:00:00Z",
        stderr=io.StringIO(),
    )

    assert run_backup_job(job, deps) == 1
    state = store.read_preflight_backup_job_state(REQUEST_ID)
    assert state.phase is LifecyclePhase.BACKUP_FAILED
    assert finalized is False
    assert order == ["guard-ready", "guard-release"]
    with pytest.raises(RequestStoreError):
        store.read_request(REQUEST_ID)


def test_backup_worker_releases_guard_when_attempt_launch_fails(tmp_path: Path) -> None:
    store, job = _backup_worker_store(tmp_path)
    config = make_config(tmp_path)
    order: list[str] = []
    guard = FakeMutationGuard(
        order,
        candidate_sha=job.candidate_sha,
        mutation_epoch=job.mutation_epoch,
    )

    class FailingLifecycle:
        def launch(self, _envelope: DriverEnvelope) -> ActivePointer:
            order.append("launch")
            raise RuntimeError("attempt launch failed")

    def finalize(request, verified):  # type: ignore[no-untyped-def]
        return worker_module._finalize_verified_backup(  # type: ignore[attr-defined]
            config,
            store,
            request,
            verified,
        )

    dependencies = WorkerDependencies(
        store=store,
        lifecycle=FailingLifecycle(),
        run_driver=lambda _path, _resume: 0,
        run_backup=lambda _request, _job, _cancelled: VerifiedBackupJob(
            manifest_path=tmp_path / "backup-manifest.json",
            manifest_sha256="d" * 64,
            lease_digest="e" * 64,
            preflight_attestation_sha256="f" * 64,
        ),
        finalize_backup=finalize,
        mutation_guard=guard,
        now=lambda: "2026-07-19T22:00:00Z",
        stderr=io.StringIO(),
    )

    with pytest.raises(RuntimeError, match="attempt launch failed"):
        run_backup_job(job, dependencies)

    assert order == ["guard-ready", "launch", "guard-release"]


def test_backup_worker_observes_durable_cancel_and_never_verifies(tmp_path: Path) -> None:
    store, job = _backup_worker_store(tmp_path)

    def cancel_during_backup(_request, _job, _cancelled):  # type: ignore[no-untyped-def]
        state = store.read_preflight_backup_job_state(REQUEST_ID)
        requested = transition_backup_job(
            state,
            LifecycleAction.REQUEST_CANCEL,
            updated_at=datetime(2026, 7, 19, 22, tzinfo=UTC),
        )
        store.replace_preflight_backup_job_state(
            requested,
            expected_sequence=state.sequence,
        )
        return VerifiedBackupJob(
            manifest_path=tmp_path / "backup-manifest.json",
            manifest_sha256="d" * 64,
            lease_digest="e" * 64,
            preflight_attestation_sha256="f" * 64,
        )

    deps = WorkerDependencies(
        store=store,
        lifecycle=object(),
        run_driver=lambda _path, _resume: 0,
        run_backup=cancel_during_backup,
        mutation_guard=_backup_mutation_guard(job),
        now=lambda: "2026-07-19T22:00:00Z",
        stderr=io.StringIO(),
    )

    assert run_backup_job(job, deps) == 130
    state = store.read_preflight_backup_job_state(REQUEST_ID)
    assert state.phase is LifecyclePhase.BACKUP_FAILED
    assert state.failure_code == "backup_cancelled"


def test_backup_worker_persists_secret_safe_backup_stage_code(tmp_path: Path) -> None:
    store, job = _backup_worker_store(tmp_path)

    def fail_backup(_request, _job, _cancelled):  # type: ignore[no-untyped-def]
        raise BackupError("postgres_dump_failed")

    deps = WorkerDependencies(
        store=store,
        lifecycle=object(),
        run_driver=lambda _path, _resume: 0,
        run_backup=fail_backup,
        mutation_guard=_backup_mutation_guard(job),
        now=lambda: "2026-07-19T22:00:00Z",
        stderr=io.StringIO(),
    )

    assert run_backup_job(job, deps) == 1
    state = store.read_preflight_backup_job_state(REQUEST_ID)
    assert state.phase is LifecyclePhase.BACKUP_FAILED
    assert state.failure_code == "postgres_dump_failed"


def test_inventory_tunnel_failure_reaches_private_diagnostic_and_public_status(
    tmp_path: Path,
) -> None:
    store, job = _backup_worker_store(tmp_path)
    backup_root = tmp_path / "backup"
    backup_root.mkdir()
    creator = BackupCreator(
        make_backup_config(backup_root),
        service_uid=os.getuid(),
        runner=RecordingRunner(),
        now=lambda: job.created_at,
        object_inventory_provider=lambda _created_at: (_ for _ in ()).throw(
            ReadonlyDatabaseTunnelError(
                "credential",
                "Unauthorized token=private-value",
            )
        ),
        publish_latest=False,
    )
    stderr = io.StringIO()

    def run_backup(request, envelope, _cancelled):  # type: ignore[no-untyped-def]
        creator.create(request, created_at=envelope.created_at)
        raise AssertionError("unreachable")

    deps = WorkerDependencies(
        store=store,
        lifecycle=object(),
        run_driver=lambda _path, _resume: 0,
        run_backup=run_backup,
        mutation_guard=_backup_mutation_guard(job),
        now=lambda: "2026-07-19T22:00:00Z",
        stderr=stderr,
    )

    assert run_backup_job(job, deps) == 1
    state = store.read_preflight_backup_job_state(REQUEST_ID)
    assert state.phase is LifecyclePhase.BACKUP_FAILED
    assert state.failure_code == "object_inventory_credentials_failed"
    event = store.read_events(REQUEST_ID)[-1]
    assert event.event == "backup_failed"
    assert event.reason == "backup_object_inventory_failed"
    assert event.operator == "hongjian"
    assert event.operator_uid == 2002
    assert event.unit_name == f"loom-staging-backup-{REQUEST_ID}.service"
    assert "private-value" not in stderr.getvalue()
    assert "[REDACTED:token]" in stderr.getvalue()

    broker = broker_fakes(tmp_path / "broker")
    status_dependencies = replace(
        broker.dependencies,
        config=replace(broker.config, state_root=store.root),
        store=store,
    )
    assert (
        broker_main(
            ["status"],
            dependencies=status_dependencies,
        )
        == 0
    )
    status = json.loads(broker.stdout.getvalue().splitlines()[-1])
    assert status["stage"] == "backup_failed"
    assert status["reason"] == "backup_object_inventory_failed"
    assert status["unit"] == f"loom-staging-backup-{REQUEST_ID}.service"
    assert "diagnostic" not in status
    assert "object_inventory_credentials_failed" not in json.dumps(status)


def test_private_diagnostic_write_failure_does_not_drop_durable_event(tmp_path: Path) -> None:
    store, job = _backup_worker_store(tmp_path)

    def fail_backup(_request, _job, _cancelled):  # type: ignore[no-untyped-def]
        raise BackupError(
            "object_inventory_transport_failed",
            diagnostic="private transport diagnostic",
        )

    class FailingStderr(io.StringIO):
        def write(self, value: str) -> int:
            del value
            raise OSError("stderr unavailable")

    deps = WorkerDependencies(
        store=store,
        lifecycle=object(),
        run_driver=lambda _path, _resume: 0,
        run_backup=fail_backup,
        mutation_guard=_backup_mutation_guard(job),
        now=lambda: "2026-07-19T22:00:00Z",
        stderr=FailingStderr(),
    )

    assert run_backup_job(job, deps) == 1
    event = store.read_events(REQUEST_ID)[-1]
    assert event.event == "backup_failed"
    assert event.reason == "backup_object_inventory_failed"


def test_backup_worker_redacts_unclassified_failure_text(tmp_path: Path) -> None:
    store, job = _backup_worker_store(tmp_path)

    def fail_backup(_request, _job, _cancelled):  # type: ignore[no-untyped-def]
        raise ValueError("secret-bearing diagnostic")

    stderr = io.StringIO()
    deps = WorkerDependencies(
        store=store,
        lifecycle=object(),
        run_driver=lambda _path, _resume: 0,
        run_backup=fail_backup,
        mutation_guard=_backup_mutation_guard(job),
        now=lambda: "2026-07-19T22:00:00Z",
        stderr=stderr,
    )

    assert run_backup_job(job, deps) == 1
    state = store.read_preflight_backup_job_state(REQUEST_ID)
    assert state.phase is LifecyclePhase.BACKUP_FAILED
    assert state.failure_code == "backup_failed"
    persisted = (
        store.root / "requests" / REQUEST_ID / "preflight-backup" / "state.json"
    ).read_text()
    assert "secret-bearing" not in persisted
    # The unclassified (non-BackupError) message is never surfaced, but the
    # exception class must be — so the generic backup_failed is no longer a
    # dead end (#924).
    assert "secret-bearing" not in stderr.getvalue()
    assert "unclassified backup failure: ValueError" in stderr.getvalue()


def test_backup_worker_unclassified_failure_pinpoints_raise_site(tmp_path: Path) -> None:
    store, job = _backup_worker_store(tmp_path)

    def fail_backup(_request, _job, _cancelled):  # type: ignore[no-untyped-def]
        raise RuntimeError("value=private-token")

    stderr = io.StringIO()
    deps = WorkerDependencies(
        store=store,
        lifecycle=object(),
        run_driver=lambda _path, _resume: 0,
        run_backup=fail_backup,
        mutation_guard=_backup_mutation_guard(job),
        now=lambda: "2026-07-19T22:00:00Z",
        stderr=stderr,
    )

    assert run_backup_job(job, deps) == 1
    rendered = stderr.getvalue()
    # Secret-safe: class + raise-site code location surface; the message does not.
    assert "private-token" not in rendered
    assert "test_worker.py" in rendered
    assert "in fail_backup" in rendered
    # The emitted diagnostic is a single well-formed JSON record whose fields
    # never carry the exception message.
    payload = json.loads(rendered.strip().splitlines()[-1])
    assert payload["failure_code"] == "backup_failed"
    assert payload["diagnostic"].startswith("unclassified backup failure: RuntimeError")
    assert "private-token" not in json.dumps(payload)
    # The durable public reason stays the safe generic fallback.
    event = store.read_events(REQUEST_ID)[-1]
    assert event.event == "backup_failed"
    assert event.reason == "backup_failed"


def test_backup_worker_classifies_assessment_drift_with_safe_check_detail(
    tmp_path: Path,
) -> None:
    store, job = _backup_worker_store(tmp_path)

    def fail_backup(_request, _job, _cancelled):  # type: ignore[no-untyped-def]
        raise PreflightAssessmentDriftError(
            check_id="systemd.user-manager",
            reason=PreflightAssessmentDriftReason.FAILED,
        )

    stderr = io.StringIO()
    deps = WorkerDependencies(
        store=store,
        lifecycle=object(),
        run_driver=lambda _path, _resume: 0,
        run_backup=fail_backup,
        mutation_guard=_backup_mutation_guard(job),
        now=lambda: "2026-07-19T22:00:00Z",
        stderr=stderr,
    )

    assert run_backup_job(job, deps) == 1
    state = store.read_preflight_backup_job_state(REQUEST_ID)
    assert state.phase is LifecyclePhase.BACKUP_FAILED
    assert state.failure_code == "preflight_assessment_drift"
    event = store.read_events(REQUEST_ID)[-1]
    assert event.event == "backup_failed"
    assert event.reason == "backup_precondition_failed"
    payload = json.loads(stderr.getvalue().strip())
    assert payload["failure_code"] == "preflight_assessment_drift"
    assert payload["diagnostic"] == (
        "pre-backup assessment evidence drifted: check_id=systemd.user-manager reason=failed"
    )


def _install_fake_signal_handlers(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[int, object]:
    handlers: dict[int, object] = {}

    def fake_signal(signum: int, handler: object) -> object:
        previous = handlers.get(signum, signal.SIG_DFL)
        handlers[signum] = handler
        return previous

    monkeypatch.setattr(signal, "signal", fake_signal)
    return handlers


def _deliver(handlers: dict[int, object], signum: int = signal.SIGTERM) -> None:
    handler = handlers[signum]
    assert callable(handler)
    handler(signum, None)


def test_worker_sigterm_during_envelope_load_is_bookkept_deterministically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = worker_fakes()
    handlers = _install_fake_signal_handlers(monkeypatch)

    def load_envelope(path: Path) -> DriverEnvelope:
        del path
        _deliver(handlers)
        return valid_envelope()

    bundle.deps.load_envelope = load_envelope
    rc = worker_main(
        [
            "run-attempt",
            "--envelope",
            "/var/lib/loom-staging-rollout/requests/req-alpha/attempts/1/envelope.json",
        ],
        dependencies=bundle.deps,
    )

    assert rc == 130
    assert bundle.store.events[-1].event == "cancelled"
    assert bundle.store.active is None


@pytest.mark.parametrize(
    ("command", "path", "loader_field"),
    [
        (
            "run-attempt",
            "/var/lib/loom-staging-rollout/requests/req-alpha/attempts/1/envelope.json",
            "load_envelope",
        ),
        (
            "run-backup",
            "/var/lib/loom-staging-rollout/requests/req-alpha/preflight-backup/job.json",
            "load_backup_job",
        ),
    ],
)
def test_worker_loader_failure_releases_path_bound_guard(
    command: str,
    path: str,
    loader_field: str,
) -> None:
    bundle = worker_fakes()
    guard = FakeMutationGuard(bundle.order)

    def fail_load(_path: Path):  # type: ignore[no-untyped-def]
        bundle.order.append("loader")
        raise ValueError("injected loader failure")

    dependencies = replace(
        bundle.deps,
        mutation_guard=guard,
        **{loader_field: fail_load},
    )

    assert (
        worker_main(
            [command, "--envelope" if command == "run-attempt" else "--job", path],
            dependencies=dependencies,
        )
        == 1
    )

    assert bundle.order[:3] == ["guard-ready", "loader", "guard-release"]
    assert guard.released == [REQUEST_ID]


@pytest.mark.parametrize(
    ("signal_event", "expected_rc", "terminal_event"),
    [
        ("attempt_running", 130, "cancelled"),
        ("attempt_done", 0, "attempt_done"),
    ],
)
def test_worker_sigterm_cannot_interrupt_terminal_bookkeeping_or_cas_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    signal_event: str,
    expected_rc: int,
    terminal_event: str,
) -> None:
    bundle = worker_fakes()
    handlers = _install_fake_signal_handlers(monkeypatch)
    bundle.deps.load_envelope = lambda path: valid_envelope()
    persist = bundle.store.append_event

    def append_event(event: RequestEvent) -> Path:
        path = persist(event)
        if event.event == signal_event:
            _deliver(handlers)
        return path

    bundle.store.append_event = append_event  # type: ignore[method-assign]

    rc = worker_main(
        [
            "run-attempt",
            "--envelope",
            "/var/lib/loom-staging-rollout/requests/req-alpha/attempts/1/envelope.json",
        ],
        dependencies=bundle.deps,
    )

    assert rc == expected_rc
    assert bundle.store.events[-1].event == terminal_event
    assert bundle.store.active is None
    assert bundle.order[-2:] == ["active-clear-running", "driver-lock-release"]


def test_default_worker_run_uses_exact_sanitized_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_cli.rollout.operator.systemd import SystemdUserManager
    from tests.loom_cli.rollout.operator.test_broker import make_config

    config = make_config(tmp_path)
    expected = sanitized_child_environment(config, service_uid=1234)
    environments: list[dict[str, str] | None] = []
    timeouts: list[object] = []

    def fake_run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        environments.append(kwargs.get("env"))  # type: ignore[arg-type]
        timeouts.append(kwargs.get("timeout"))
        return subprocess.CompletedProcess(argv, 0, "", "")

    guard_launch = SystemdUserManager(
        config,
        service_uid=1234,
        run=lambda argv: subprocess.CompletedProcess(argv, 0, "", ""),
    ).start_mutation_guard_argv(REQUEST_ID, "1" * 32)
    service_stop_timeout = int(
        next(item for item in guard_launch if item.startswith("TimeoutStopSec="))
        .removeprefix("TimeoutStopSec=")
        .removesuffix("s")
    )
    monkeypatch.setattr(subprocess, "run", fake_run)
    worker_module._run(["systemctl", "--user", "show"], environment=expected)

    assert environments == [expected]
    assert len(timeouts) == 1
    assert type(timeouts[0]) is int
    assert timeouts[0] > service_stop_timeout + 3 * 30


def test_default_attempt_dependencies_compose_historical_runtime_under_current_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installed_config = make_config(tmp_path)
    historical_config = replace(
        installed_config,
        runner_repo=tmp_path / "historical" / "repo",
        cluster_config_path=tmp_path
        / "historical"
        / "repo"
        / "deploy/environments/staging.cluster.toml",
        config_sha256="1" * 64,
    )
    envelope = replace(valid_envelope(), attempt_number=2, resume=True)
    path = (
        installed_config.state_root
        / "requests"
        / envelope.request_id
        / "attempts/2/envelope.json"
    )
    order: list[str] = []

    class Guard:
        def assert_ready(self, request_id: str) -> object:
            order.append("guard")
            return SimpleNamespace(
                request_id=request_id,
                candidate_sha=envelope.resolved_sha,
                candidate_tree=envelope.resolved_tree,
                state="ready",
            )

        def release(self, _request_id: str) -> object:
            raise AssertionError("successful composition must retain the transferred guard")

    guard = Guard()
    monkeypatch.setattr(worker_module, "SystemdUserManager", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(
        worker_module,
        "MutationGuardManager",
        lambda **_kwargs: guard,
    )
    authority = object()
    monkeypatch.setattr(
        worker_module,
        "build_installed_resume_runtime_upgrade_authority",
        lambda *_args, **_kwargs: authority,
    )

    def load_runtime(
        found_path: Path,
        config: object,
        **kwargs: object,
    ) -> tuple[DriverEnvelope, object]:
        order.append("resolve")
        assert found_path == path
        assert config == installed_config
        assert kwargs["resume_runtime_upgrade"] is authority
        return envelope, historical_config

    monkeypatch.setattr(worker_module, "load_validated_envelope_with_config", load_runtime)
    attestation = SimpleNamespace(
        attestation_digest=envelope.preflight_attestation_sha256,
        registry_digest=envelope.preflight_registry_sha256,
        coverage_digest=envelope.preflight_coverage_sha256,
        bindings=SimpleNamespace(
            candidate_sha=envelope.resolved_sha,
            candidate_tree=envelope.resolved_tree,
            runner_config_hash=envelope.runner_config_sha256,
            runner_install_hash="9" * 64,
            environment=envelope.environment,
            namespace=envelope.namespace,
        ),
    )

    class AttestationStore:
        def __init__(self, root: Path) -> None:
            assert root == installed_config.state_root

        def read(self, digest: str) -> object:
            order.append("attestation")
            assert digest == envelope.preflight_attestation_sha256
            return attestation

    monkeypatch.setattr(worker_module, "PreflightAttestationStore", AttestationStore)
    composed = object()

    def compose(config: object, **kwargs: object) -> object:
        order.append("compose")
        assert config == historical_config
        assert kwargs["installed_config"] == installed_config
        assert kwargs["runner_install_digest"] == "9" * 64
        return composed

    monkeypatch.setattr(worker_module, "_default_dependencies", compose)

    dependencies = worker_module._default_attempt_dependencies(
        installed_config,
        path,
        service_uid=os.geteuid(),
    )

    assert dependencies is composed
    assert order == ["guard", "resolve", "attestation", "compose"]


def test_default_attempt_dependencies_release_guard_when_bootstrap_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installed_config = make_config(tmp_path)
    path = (
        installed_config.state_root
        / "requests/req-alpha/attempts/2/envelope.json"
    )
    released: list[str] = []

    class Guard:
        def assert_ready(self, _request_id: str) -> object:
            raise ValueError("guard evidence drifted")

        def release(self, request_id: str) -> object:
            released.append(request_id)
            return SimpleNamespace(request_id=request_id, state="released")

    monkeypatch.setattr(worker_module, "SystemdUserManager", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(worker_module, "MutationGuardManager", lambda **_kwargs: Guard())

    with pytest.raises(ValueError, match="guard evidence drifted"):
        worker_module._default_attempt_dependencies(
            installed_config,
            path,
            service_uid=os.geteuid(),
        )

    assert released == [REQUEST_ID]


@pytest.mark.parametrize(
    ("attempt_number", "resume", "apply_attempt"),
    ((2, True, 1), (1, False, 1)),
)
def test_final_admission_resumes_only_from_exact_prior_or_current_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    attempt_number: int,
    resume: bool,
    apply_attempt: int,
) -> None:
    attestation = SimpleNamespace(
        attestation_digest="3" * 64,
        registry_digest="4" * 64,
        coverage_digest="5" * 64,
        bindings=SimpleNamespace(staging_mutation_epoch=7),
    )
    prior_admission = SimpleNamespace(attestation=attestation)
    resumed_admission = SimpleNamespace(attestation=attestation, resumed=True)
    published: list[tuple[int, object]] = []

    class AdmissionStore:
        def __init__(self, _root, *, request_id, attempt_number, service_uid):
            assert request_id == REQUEST_ID
            assert service_uid == os.geteuid()
            self.attempt_number = attempt_number

        def read(self, found_attestation):
            assert self.attempt_number == apply_attempt
            assert found_attestation is attestation
            return prior_admission

        def publish(self, admission):
            published.append((self.attempt_number, admission))

    protected_apply = SimpleNamespace(
        passed=True,
        tier=4,
        stage=StageCapability.FINAL_ONLY,
        operation=CheckOperation.APPLY,
        evidence={
            "ready": True,
            "candidate-sha": "a" * 40,
            "attestation-digest": "3" * 64,
            "observed-epoch": 8,
            "protected-mutation": True,
            "blockers": {},
        },
    )

    class GateStore:
        def __init__(self, _root, *, request_id, attempt_number, service_uid):
            assert request_id == REQUEST_ID
            assert service_uid == os.geteuid()
            self.attempt_number = attempt_number

        def read_all(self):
            return (
                {"final.protected-apply": protected_apply}
                if self.attempt_number == apply_attempt
                else {}
            )

    class DeepPreflight:
        def admit_final(self, *_args, **_kwargs):
            pytest.fail("post-apply resume repeated pre-apply admission")

        def admit_post_apply_resume(self, candidate, **kwargs):
            assert candidate.resolved_sha == "a" * 40
            assert kwargs["prior_admission"] is prior_admission
            assert kwargs["attestation_digest"] == "3" * 64
            return resumed_admission

    monkeypatch.setattr(worker_module, "FinalAdmissionStore", AdmissionStore)
    monkeypatch.setattr(worker_module, "FinalGateExecutionStore", GateStore)
    envelope = valid_envelope()
    if attempt_number == 2:
        envelope = replace(
            envelope,
            attempt_number=attempt_number,
            attempt_operator="devansh",
            attempt_uid=2003,
            resume=resume,
        )

    result = worker_module._admit_final_attempt(
        envelope,
        deep_preflight=DeepPreflight(),
        attestation_store=SimpleNamespace(read=lambda _digest: attestation),
        state_root=tmp_path,
        service_uid=os.geteuid(),
    )

    assert result is resumed_admission
    assert published == [(attempt_number, resumed_admission)]


def test_final_admission_recovers_component_journal_without_outer_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attestation = SimpleNamespace(
        attestation_digest="3" * 64,
        registry_digest="4" * 64,
        coverage_digest="5" * 64,
        bindings=SimpleNamespace(staging_mutation_epoch=7),
    )
    prior_admission = SimpleNamespace(attestation=attestation)
    resumed_admission = SimpleNamespace(attestation=attestation, resumed=True)
    published: list[tuple[int, object]] = []

    class AdmissionStore:
        def __init__(self, _root, *, request_id, attempt_number, service_uid):
            assert request_id == REQUEST_ID
            assert service_uid == os.geteuid()
            self.attempt_number = attempt_number

        def read(self, found_attestation):
            assert self.attempt_number == 1
            assert found_attestation is attestation
            return prior_admission

        def publish(self, admission):
            published.append((self.attempt_number, admission))

    class GateStore:
        def __init__(self, *_args, **_kwargs):
            pass

        def read_all(self):
            return {}

    class DeepPreflight:
        def admit_final(self, *_args, **_kwargs):
            pytest.fail("component-journal recovery repeated pre-apply admission")

        def admit_post_apply_resume(self, candidate, **kwargs):
            assert candidate.resolved_sha == "a" * 40
            assert kwargs["prior_admission"] is prior_admission
            return resumed_admission

    def find_recovery(_root, **kwargs):
        assert kwargs["through_attempt"] == 1
        assert kwargs["starting_mutation_epoch"] == 7
        return 1

    monkeypatch.setattr(worker_module, "FinalAdmissionStore", AdmissionStore)
    monkeypatch.setattr(worker_module, "FinalGateExecutionStore", GateStore)
    monkeypatch.setattr(worker_module, "find_advanced_epoch_attempt", find_recovery)
    envelope = replace(
        valid_envelope(),
        attempt_number=2,
        attempt_operator="devansh",
        attempt_uid=2003,
        resume=True,
    )

    result = worker_module._admit_final_attempt(
        envelope,
        deep_preflight=DeepPreflight(),
        attestation_store=SimpleNamespace(read=lambda _digest: attestation),
        state_root=tmp_path,
        service_uid=os.geteuid(),
    )

    assert result is resumed_admission
    assert published == [(2, resumed_admission)]
