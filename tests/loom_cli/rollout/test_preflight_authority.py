from __future__ import annotations

import pytest

from loom_cli.rollout.operator.model import APPROVED_REMOTE_URL, CandidateBinding
from loom_cli.rollout.preflight_authority import CandidatePreflightPlan
from loom_cli.rollout.preflight_contract import CheckContext
from loom_cli.rollout.preflight_registry import PreflightRegistry


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
