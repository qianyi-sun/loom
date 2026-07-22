from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from loom_cli.rollout.operator.model import CallerIdentity, PreflightRequest
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_orchestrator import CandidatePreflightOrchestrator
from tests.loom_cli.rollout.test_preflight_runtime import _candidate, _runtime
from tests.loom_cli.rollout.test_rehearsal_restore_evidence import _checkpoint

NOW = datetime(2026, 7, 20, tzinfo=UTC)


class _RehearsalStore:
    pass


def _request(*, assessment_digest: str, registry_digest: str, coverage_digest: str):
    candidate = _candidate()
    return PreflightRequest(
        request_id="req-restore001",
        rollout_id="rollout-test0001",
        caller=CallerIdentity(username="qianyi", uid=501),
        candidate=candidate,
        candidate_tree=candidate.resolved_tree or "",
        requested_at="2026-07-20T00:00:00Z",
        runner_config_sha256="1" * 64,
        preflight_assessment_sha256=assessment_digest,
        preflight_registry_sha256=registry_digest,
        preflight_coverage_sha256=coverage_digest,
        mutation_epoch=17,
        environment="staging",
        namespace="loom-staging",
    )


def test_assessment_and_worker_rebuild_share_exact_registry(tmp_path: Path) -> None:
    calls: list[int] = []

    def factory(candidate, epoch):
        calls.append(epoch)
        runtime = _runtime(tmp_path)
        assert runtime.candidate == candidate
        return runtime

    orchestrator = CandidatePreflightOrchestrator(
        runtime_factory=factory,
        store=PreflightAttestationStore(tmp_path / "attestations"),
        now=lambda: NOW,
    )
    candidate = _candidate()
    assessment = orchestrator.assess(candidate, 17)
    assert assessment.passed
    attestor = orchestrator.build_rehearsal_attestor(
        request=_request(
            assessment_digest=assessment.assessment_digest,
            registry_digest=assessment.registry_digest,
            coverage_digest=assessment.coverage_digest,
        ),
        checkpoint=_checkpoint(tmp_path),
        assessment=assessment,
        rehearsal_store=_RehearsalStore(),  # type: ignore[arg-type]
    )
    assert attestor.assessment.registry_digest == assessment.registry_digest
    assert calls == [17, 17]


def test_runtime_factory_drift_is_rejected(tmp_path: Path) -> None:
    candidate = _candidate()

    def drifted(_candidate, _epoch):
        runtime = _runtime(tmp_path)
        other = replace(
            candidate,
            resolved_sha="d" * 40,
            image_tag="staging-ddddddd",
        )
        bindings = dict(runtime.bindings)
        bindings["candidate.sha"] = other.resolved_sha
        return replace(runtime, candidate=other, bindings=bindings)

    orchestrator = CandidatePreflightOrchestrator(
        runtime_factory=drifted,
        store=PreflightAttestationStore(tmp_path / "attestations"),
        now=lambda: NOW,
    )
    with pytest.raises(ValueError, match="changed exact authority"):
        orchestrator.assess(candidate, 17)


def test_worker_rebuild_rejects_checkpoint_drift(tmp_path: Path) -> None:
    orchestrator = CandidatePreflightOrchestrator(
        runtime_factory=lambda _candidate, _epoch: _runtime(tmp_path),
        store=PreflightAttestationStore(tmp_path / "attestations"),
        now=lambda: NOW,
    )
    candidate = _candidate()
    assessment = orchestrator.assess(candidate, 17)
    with pytest.raises(ValueError, match="identity drifted"):
        orchestrator.build_rehearsal_attestor(
            request=_request(
                assessment_digest=assessment.assessment_digest,
                registry_digest=assessment.registry_digest,
                coverage_digest=assessment.coverage_digest,
            ),
            checkpoint=replace(_checkpoint(tmp_path), mutation_epoch=18),
            assessment=assessment,
            rehearsal_store=_RehearsalStore(),  # type: ignore[arg-type]
        )
