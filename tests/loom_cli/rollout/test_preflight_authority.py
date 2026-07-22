from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from loom_cli.rollout.operator.model import APPROVED_REMOTE_URL, CandidateBinding
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_authority import (
    CandidatePreflightAuthorizer,
    CandidatePreflightPlan,
)
from loom_cli.rollout.preflight_contract import CheckContext
from loom_cli.rollout.preflight_registry import PreflightRegistry
from tests.loom_cli.rollout.test_preflight_pipeline import (
    _context as pipeline_context,
)
from tests.loom_cli.rollout.test_preflight_pipeline import _registry as pipeline_registry
from tests.loom_cli.rollout.test_rehearsal_restore_evidence import _checkpoint


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


def _registry() -> PreflightRegistry:
    # Reuse the constructor guard without duplicating the large exact manifest fixture.
    return object.__new__(PreflightRegistry)


def test_plan_rejects_candidate_context_drift() -> None:
    candidate = _candidate()
    registry = _registry()
    object.__setattr__(registry, "checks", ())
    object.__setattr__(registry, "through_tier", 3)
    object.__setattr__(registry, "coverage_digest", "1" * 64)
    object.__setattr__(registry, "registry_digest", "2" * 64)
    context = CheckContext(
        {
            "candidate.sha": candidate.resolved_sha,
            "candidate.source-mode": candidate.source_mode,
            "candidate.base.sha": candidate.approved_base_sha or "none",
        }
    )
    plan = CandidatePreflightPlan(candidate=candidate, registry=registry, context=context)
    assert plan.candidate == candidate
    with pytest.raises(ValueError, match="context drifts"):
        CandidatePreflightPlan(
            candidate=candidate,
            registry=registry,
            context=CheckContext(
                {
                    **dict(context.bindings),
                    "candidate.sha": "d" * 40,
                }
            ),
        )


def test_plan_rejects_reuse_digest_without_current_bindings() -> None:
    candidate = _candidate()
    registry = _registry()
    object.__setattr__(registry, "checks", ())
    object.__setattr__(registry, "through_tier", 3)
    object.__setattr__(registry, "coverage_digest", "1" * 64)
    object.__setattr__(registry, "registry_digest", "2" * 64)
    with pytest.raises(ValueError, match="current drift bindings"):
        CandidatePreflightPlan(
            candidate=candidate,
            registry=registry,
            context=CheckContext(
                {
                    "candidate.sha": candidate.resolved_sha,
                    "candidate.source-mode": candidate.source_mode,
                    "candidate.base.sha": candidate.approved_base_sha or "none",
                }
            ),
            reusable_attestation_digest="3" * 64,
        )


class _RehearsalStore:
    pass


def test_authorizer_assessment_has_no_process_local_pending_state(tmp_path: Path) -> None:
    candidate = _candidate()
    registry = pipeline_registry()
    base_bindings = dict(pipeline_context(registry).bindings)
    base_bindings.update(
        {
            "candidate.base.sha": candidate.approved_base_sha or "none",
            "candidate.sha": candidate.resolved_sha,
            "candidate.source-mode": candidate.source_mode,
        }
    )
    planner_calls = 0

    def planner(found: CandidateBinding) -> CandidatePreflightPlan:
        nonlocal planner_calls
        planner_calls += 1
        return CandidatePreflightPlan(
            candidate=found,
            registry=registry,
            context=CheckContext(base_bindings),
        )

    def checkpoint_planner(found, checkpoint):
        return CandidatePreflightPlan(
            candidate=found,
            registry=registry,
            context=CheckContext(
                {
                    **base_bindings,
                    "checkpoint.evidence.sha256": checkpoint.evidence_digest,
                    "staging.mutation-epoch": checkpoint.mutation_epoch,
                }
            ),
        )

    authorizer = CandidatePreflightAuthorizer(
        planner=planner,
        checkpoint_planner=checkpoint_planner,
        store=PreflightAttestationStore(tmp_path / "attestations"),
        now=lambda: datetime(2026, 7, 19, 12, tzinfo=UTC),
    )

    first = authorizer.assess(candidate)
    second = authorizer.assess(candidate)
    checkpoint = _checkpoint(tmp_path)
    attestor = authorizer.build_rehearsal_attestor(
        candidate=candidate,
        checkpoint=checkpoint,
        assessment=first,
        rehearsal_store=_RehearsalStore(),  # type: ignore[arg-type]
    )

    assert first.passed and second.passed
    assert first.assessment_digest == second.assessment_digest
    assert planner_calls == 2
    assert attestor.assessment == first
    assert attestor.context.bindings["checkpoint.evidence.sha256"] == checkpoint.evidence_digest
