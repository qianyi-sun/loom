from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator import deep_preflight_authority as authority_module
from loom_cli.rollout.operator.deep_preflight_authority import (
    AdmissionPreparationLifecycle,
    DeepPreflightAuthority,
    RuntimePurpose,
)
from loom_cli.rollout.operator.model import APPROVED_REMOTE_URL, CandidateBinding
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_contract import (
    CheckOperation,
    CheckProbe,
    CheckSpec,
    EvidenceField,
    MutationClass,
    RegisteredCheck,
    SecretRedactionPolicy,
    StageCapability,
)
from loom_cli.rollout.preflight_runtime import CandidatePreflightRuntime


def _candidate() -> CandidateBinding:
    return CandidateBinding(
        remote_url=APPROVED_REMOTE_URL,
        target_ref="origin/dev",
        resolved_sha="a" * 40,
        image_tag="staging-aaaaaaa",
        fetched_at="2026-07-19T12:00:00Z",
        source_mode="sealed-cumulative",
        resolved_tree="b" * 40,
        approved_base_sha="c" * 40,
    )


def _check(check_id: str, tier: int, dependency: str | None = None) -> RegisteredCheck:
    stage = StageCapability.STATIC if tier < 2 else StageCapability.BASELINE_LIVE_READONLY
    return RegisteredCheck(
        spec=CheckSpec(
            check_id=check_id,
            failure_code=f"{check_id}.failed",
            tier=tier,
            stage=stage,
            dependencies=(() if dependency is None else (dependency,)),
            mutation_class=MutationClass.NONE,
            input_keys=("candidate.sha",),
            evidence_schema=(EvidenceField("ready", "boolean"),),
            timeout_seconds=5,
            freshness_ttl_seconds=60,
            remediation=f"restore {check_id}",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(passed=True, evidence={"ready": True})
        },
    )


def _runtime(candidate: CandidateBinding, epoch: int) -> CandidatePreflightRuntime:
    tier0 = (_check("candidate.identity", 0),)
    tier1 = (_check("images.build", 1, "candidate.identity"),)
    tier2 = (_check("staging.health", 2, "images.build"),)
    return CandidatePreflightRuntime(
        candidate=candidate,
        tier0=tier0,
        tier1=tier1,
        tier2=tier2,
        bindings={
            "candidate.base.sha": candidate.approved_base_sha or "none",
            "candidate.sha": candidate.resolved_sha,
            "candidate.source-mode": candidate.source_mode,
            "staging.mutation-epoch": epoch,
        },
        rehearsal_actions=lambda *_args: {},
        rehearsal_identity=lambda *_args: ("rehearsal-exact", "d" * 64),
    )


class _Sources:
    def __init__(self, runtime: CandidatePreflightRuntime, loaded) -> None:
        self.candidate = runtime.candidate
        self.loaded_artifacts = loaded
        self._runtime = runtime

    def build(self, *, mutation_epoch: int) -> CandidatePreflightRuntime:
        assert mutation_epoch == self._runtime.bindings["staging.mutation-epoch"]
        return self._runtime


def test_broker_runtime_uses_admission_purpose(tmp_path: Path) -> None:
    candidate = _candidate()
    purposes: list[RuntimePurpose] = []

    def factory(found, epoch, purpose):
        purposes.append(purpose)
        return _Sources(_runtime(found, epoch), None)

    authority = DeepPreflightAuthority(
        sources_factory=factory,  # type: ignore[arg-type]
        attestation_store=PreflightAttestationStore(tmp_path / "state"),
        read_mutation_epoch=lambda: 7,
        now=lambda: datetime(2026, 7, 19, 12, tzinfo=UTC),
    )
    runtime = authority.admission_orchestrator().runtime_factory(candidate, 7)

    assert runtime.candidate == candidate
    assert purposes == [RuntimePurpose.ADMISSION]


def test_detached_runtime_requires_exact_persisted_outputs(tmp_path: Path) -> None:
    candidate = _candidate()

    authority = DeepPreflightAuthority(
        sources_factory=lambda found, epoch, _purpose: _Sources(  # type: ignore[arg-type]
            _runtime(found, epoch), None
        ),
        attestation_store=PreflightAttestationStore(tmp_path / "state"),
        read_mutation_epoch=lambda: 7,
        now=lambda: datetime(2026, 7, 19, 12, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="exact immutable outputs"):
        authority.detached_orchestrator().runtime_factory(candidate, 7)


def test_mutation_epoch_authority_fails_closed(tmp_path: Path) -> None:
    authority = DeepPreflightAuthority(
        sources_factory=lambda *_args: None,  # type: ignore[arg-type]
        attestation_store=PreflightAttestationStore(tmp_path / "state"),
        read_mutation_epoch=lambda: -1,
        now=lambda: datetime(2026, 7, 19, 12, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="mutation epoch"):
        authority.current_mutation_epoch()


def test_admission_preparation_is_exact_bounded_and_refreshes_after_ttl() -> None:
    candidate = _candidate()
    clock = [datetime(2026, 7, 19, 12, tzinfo=UTC)]
    prepared: list[CandidateBinding] = []
    lifecycle = AdmissionPreparationLifecycle(
        prepare=prepared.append,
        now=lambda: clock[0],
        ttl=timedelta(minutes=5),
    )

    with pytest.raises(ValueError, match="not freshly prepared"):
        lifecycle.require_fresh(candidate)
    lifecycle.prepare_admission(candidate)
    lifecycle.require_fresh(candidate)
    lifecycle.prepare_admission(candidate)
    assert prepared == [candidate]

    clock[0] += timedelta(minutes=5, seconds=1)
    with pytest.raises(ValueError, match="not freshly prepared"):
        lifecycle.require_fresh(candidate)
    lifecycle.prepare_admission(candidate)
    assert prepared == [candidate, candidate]


def test_failed_admission_producer_never_publishes_freshness() -> None:
    candidate = _candidate()

    def fail(_candidate: CandidateBinding) -> None:
        raise RuntimeError("producer failed")

    lifecycle = AdmissionPreparationLifecycle(
        prepare=fail,
        now=lambda: datetime(2026, 7, 19, 12, tzinfo=UTC),
    )

    with pytest.raises(RuntimeError, match="producer failed"):
        lifecycle.prepare_admission(candidate)
    with pytest.raises(ValueError, match="not freshly prepared"):
        lifecycle.require_fresh(candidate)


def test_final_admission_rebuilds_exact_admission_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    candidate = _candidate()
    purposes: list[RuntimePurpose] = []
    captured: dict[str, object] = {}
    attestation = SimpleNamespace(registry_digest="1" * 64, coverage_digest="2" * 64)

    class Store:
        def read(self, digest: str) -> object:
            captured["digest"] = digest
            return attestation

    class Runtime:
        def prebackup_plan(self, found: CandidateBinding) -> object:
            return SimpleNamespace(candidate=found)

    class Sources:
        def __init__(self, found: CandidateBinding) -> None:
            self.candidate = found

        def build(self, *, mutation_epoch: int) -> object:
            assert mutation_epoch == 7
            return Runtime()

    def sources(found, epoch, purpose):
        assert epoch == 7
        purposes.append(purpose)
        return Sources(found)

    def validate(**kwargs):
        captured.update(kwargs)
        return "admitted"

    monkeypatch.setattr(authority_module, "validate_final_attestation", validate)
    prepared: list[CandidateBinding] = []
    authority = DeepPreflightAuthority(
        sources_factory=sources,  # type: ignore[arg-type]
        attestation_store=Store(),  # type: ignore[arg-type]
        read_mutation_epoch=lambda: 7,
        now=lambda: datetime(2026, 7, 19, 12, tzinfo=UTC),
        admission_preparation=AdmissionPreparationLifecycle(
            prepare=prepared.append,
            now=lambda: datetime(2026, 7, 19, 12, tzinfo=UTC),
        ),
    )

    result = authority.admit_final(
        candidate,
        attestation_digest="3" * 64,
        expected_registry_digest="1" * 64,
        expected_coverage_digest="2" * 64,
    )

    assert result == "admitted"
    assert prepared == [candidate]
    assert purposes == [RuntimePurpose.ADMISSION]
    assert captured["digest"] == "3" * 64
    assert captured["candidate"] == candidate
    assert captured["current_mutation_epoch"] == 7
    assert captured["plan"].candidate == candidate  # type: ignore[union-attr]
