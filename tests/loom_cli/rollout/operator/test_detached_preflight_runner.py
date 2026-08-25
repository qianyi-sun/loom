from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest

from loom_cli.rollout.operator.detached_preflight_runner import (
    DetachedPreflightBackupRunner,
)
from loom_cli.rollout.operator.store import RequestStore
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_orchestrator import CandidatePreflightOrchestrator
from tests.loom_cli.rollout.operator.test_checkpoint_coordinator import (
    NOW,
    FakeCreator,
    _checkpoint,
    _job,
    _request,
    _restore,
)
from tests.loom_cli.rollout.test_preflight_runtime import _runtime


class _FakeAttestor:
    def verify_restore(self, checkpoint, _request, _cancelled):
        return _restore(checkpoint)

    def publish_attestation(self, _checkpoint, _lease, _request):
        return "a" * 64


class _FakeOrchestrator:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def build_rehearsal_attestor(self, *, request, checkpoint, assessment, rehearsal_store):
        del checkpoint, assessment, rehearsal_store
        self.calls.append(request.request_id)
        return _FakeAttestor()


def _assessment(tmp_path: Path, request):
    def factory(candidate, epoch, _reference):
        runtime = _runtime(tmp_path)
        return replace(runtime, candidate=candidate)

    return CandidatePreflightOrchestrator(
        runtime_factory=factory,
        store=PreflightAttestationStore(tmp_path / "attestations"),
        now=lambda: NOW,
    ).assess(request.candidate, request.mutation_epoch)


def test_runner_rebuilds_attestor_and_promotes_only_verified_checkpoint(tmp_path: Path) -> None:
    request = _request()
    assessment = _assessment(tmp_path, request)
    request = replace(
        request,
        preflight_assessment_sha256=assessment.assessment_digest,
        preflight_registry_sha256=assessment.registry_digest,
        preflight_coverage_sha256=assessment.coverage_digest,
    )
    manifest_path = tmp_path / "20260719T210000Z-req-checkpoint1" / "backup-manifest.json"
    job = replace(
        _job(manifest_path.parent.name),
        preflight_assessment_sha256=assessment.assessment_digest,
        preflight_registry_sha256=assessment.registry_digest,
        preflight_coverage_sha256=assessment.coverage_digest,
    )
    store = RequestStore(tmp_path / "state")
    orchestrator = _FakeOrchestrator()
    runner = DetachedPreflightBackupRunner(
        creator=FakeCreator(manifest_path),
        store=store,
        rehearsal_store=store,
        load_assessment=lambda request_id: assessment,
        orchestrator=cast(CandidatePreflightOrchestrator, orchestrator),
        inspect_checkpoint=lambda _backup, _request: _checkpoint(manifest_path),
        now=lambda: NOW + timedelta(minutes=4),
        lease_ttl=timedelta(hours=4),
    )

    verified = runner(request, job, lambda: False)

    assert verified.preflight_attestation_sha256 == "a" * 64
    assert orchestrator.calls == [request.request_id, request.request_id]
    assert store.read_backup_rotation().active is not None


def test_runner_rejects_persisted_assessment_drift_before_checkpoint(tmp_path: Path) -> None:
    request = _request()
    assessment = _assessment(tmp_path, request)
    manifest_path = tmp_path / "20260719T210000Z-req-checkpoint1" / "backup-manifest.json"
    creator = FakeCreator(manifest_path)
    runner = DetachedPreflightBackupRunner(
        creator=creator,
        store=RequestStore(tmp_path / "state"),
        rehearsal_store=RequestStore(tmp_path / "rehearsal"),
        load_assessment=lambda _request_id: assessment,
        orchestrator=cast(CandidatePreflightOrchestrator, _FakeOrchestrator()),
        inspect_checkpoint=lambda _backup, _request: _checkpoint(manifest_path),
        now=lambda: NOW,
        lease_ttl=timedelta(hours=4),
    )

    with pytest.raises(ValueError, match="drifts from request"):
        runner(request, _job(manifest_path.parent.name), lambda: False)
    assert creator.calls == []
