from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from loom_cli.rollout.operator.deep_preflight_authority import (
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
    stage = (
        StageCapability.STATIC if tier < 2 else StageCapability.BASELINE_LIVE_READONLY
    )
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
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True, evidence={"ready": True}
            )
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
