from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from loom_cli.rollout.operator.model import APPROVED_REMOTE_URL, CandidateBinding
from loom_cli.rollout.preflight_contract import (
    CheckOperation,
    CheckProbe,
    CheckSpec,
    EvidenceField,
    RegisteredCheck,
    SecretRedactionPolicy,
)
from loom_cli.rollout.preflight_coverage import load_coverage_manifest
from loom_cli.rollout.preflight_runtime import CandidatePreflightRuntime
from loom_cli.rollout.rehearsal_readiness import RehearsalResult
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


def _checks(tier: int) -> tuple[RegisteredCheck, ...]:
    entries = [entry for entry in load_coverage_manifest().checks if entry.tier == tier]
    checks: list[RegisteredCheck] = []
    for entry in entries:
        spec = CheckSpec(
            check_id=entry.check_id,
            failure_code=entry.failure_code,
            tier=entry.tier,
            stage=entry.stage,
            dependencies=entry.dependencies,
            mutation_class=entry.mutation_class,
            input_keys=("runtime.binding",),
            evidence_schema=(EvidenceField("ready", "boolean"),),
            timeout_seconds=10,
            freshness_ttl_seconds=60,
            remediation="restore the declared runtime fixture",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
            final_only_justification=entry.final_only_justification,
        )
        checks.append(
            RegisteredCheck(
                spec=spec,
                implementation_version="runtime-test-v1",
                operations={
                    CheckOperation.PROBE: lambda _context: CheckProbe(
                        passed=True, evidence={"ready": True}
                    )
                },
            )
        )
    return tuple(checks)


def _runtime(tmp_path: Path) -> CandidatePreflightRuntime:
    candidate = _candidate()

    def identity(_candidate, checkpoint):
        return (
            f"rehearsal-{checkpoint.request_id.removeprefix('req-')}",
            "d" * 64,
        )

    def actions(found, checkpoint, isolation_id):
        def action(check_id):
            return lambda: RehearsalResult(
                check_id=check_id,
                isolation_id=isolation_id,
                candidate_sha=found.resolved_sha,
                mutation_epoch=checkpoint.mutation_epoch,
                evidence_digest="e" * 64,
                journal_digest="f" * 64,
                protected_mutation=False,
                cleanup_verified=check_id == "rehearsal.cleanup",
                blockers={},
            )

        return {
            entry.check_id: action(entry.check_id)
            for entry in load_coverage_manifest().checks
            if entry.tier == 3
        }

    return CandidatePreflightRuntime(
        candidate=candidate,
        tier0=_checks(0),
        tier1=_checks(1),
        tier2=_checks(2),
        bindings={
            "candidate.base.sha": candidate.approved_base_sha or "none",
            "candidate.sha": candidate.resolved_sha,
            "candidate.source-mode": candidate.source_mode,
            "environment": "staging",
            "namespace": "loom-staging",
            "runtime.binding": "exact",
            "staging.mutation-epoch": 17,
        },
        rehearsal_actions=actions,
        rehearsal_identity=identity,
    )


def test_prebackup_and_checkpoint_plans_keep_exact_registry_identity(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    prebackup = runtime.prebackup_plan(runtime.candidate)
    checkpoint = _checkpoint(tmp_path)
    checkpoint_plan = runtime.checkpoint_plan(runtime.candidate, checkpoint)

    assert prebackup.registry.registry_digest == checkpoint_plan.registry.registry_digest
    assert prebackup.registry.coverage_digest == checkpoint_plan.registry.coverage_digest
    assert prebackup.context.bindings["checkpoint.evidence.sha256"] == "0" * 64
    assert (
        checkpoint_plan.context.bindings["checkpoint.evidence.sha256"] == checkpoint.evidence_digest
    )
    assert checkpoint_plan.context.bindings["rehearsal.plan.sha256"] == "d" * 64


def test_checkpoint_plan_runs_only_exact_bound_rehearsal_actions(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    checkpoint = _checkpoint(tmp_path)
    plan = runtime.checkpoint_plan(runtime.candidate, checkpoint)

    executions = plan.registry.dag(max_concurrency=4).run(
        plan.context,
        through_tier=3,
        now=lambda: datetime(2026, 7, 19, 12, tzinfo=UTC),
    )

    tier3 = [execution for execution in executions if execution.tier == 3]
    assert tier3 and all(execution.passed for execution in tier3)
    cleanup = next(execution for execution in tier3 if execution.check_id == "rehearsal.cleanup")
    assert cleanup.evidence["cleanup-verified"] is True


def test_runtime_rejects_missing_input_candidate_or_checkpoint_drift(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    with pytest.raises(ValueError, match="candidate changed"):
        runtime.prebackup_plan(
            CandidateBinding(
                **{
                    **runtime.candidate.to_dict(),
                    "resolved_sha": "9" * 40,
                    "image_tag": "staging-9999999",
                }
            )
        )

    checkpoint = _checkpoint(tmp_path)
    object.__setattr__(checkpoint, "mutation_epoch", 18)
    with pytest.raises(ValueError, match="checkpoint identity"):
        runtime.checkpoint_plan(runtime.candidate, checkpoint)

    with pytest.raises(ValueError, match="bindings are incomplete"):
        CandidatePreflightRuntime(
            candidate=runtime.candidate,
            tier0=runtime.tier0,
            tier1=runtime.tier1,
            tier2=runtime.tier2,
            bindings={
                "candidate.base.sha": runtime.candidate.approved_base_sha or "none",
                "candidate.sha": runtime.candidate.resolved_sha,
                "candidate.source-mode": runtime.candidate.source_mode,
                "staging.mutation-epoch": 17,
            },
            rehearsal_actions=runtime.rehearsal_actions,
            rehearsal_identity=runtime.rehearsal_identity,
        )
