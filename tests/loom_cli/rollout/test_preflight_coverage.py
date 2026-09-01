from __future__ import annotations

import ast
from dataclasses import replace
from pathlib import Path

import pytest

from loom_cli.rollout.final_gate_readiness import FINAL_CHECK_IDS, FINAL_PREDICATE_IDS
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
from loom_cli.rollout.preflight_coverage import (
    DEFAULT_COVERAGE_MANIFEST,
    CoverageEntry,
    CoverageManifest,
    load_coverage_manifest,
)
from loom_cli.rollout.steps import default_step_sequence

REPO_ROOT = Path(__file__).resolve().parents[3]


def _legacy_operator_preflight_names() -> set[str]:
    source = REPO_ROOT / "src/loom_cli/rollout/operator/preflight.py"
    module = ast.parse(source.read_text(encoding="utf-8"))
    function = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == "collect_preflight"
    )
    names: set[str] = set()
    for node in ast.walk(function):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        first = node.args[0]
        if not isinstance(first, ast.Constant) or not isinstance(first.value, str):
            continue
        if isinstance(node.func, ast.Name) and node.func.id in {"add", "PreflightCheck"}:
            names.add(first.value)
    return names


def test_checked_in_coverage_manifest_is_complete_and_ordered() -> None:
    manifest = load_coverage_manifest()
    assert DEFAULT_COVERAGE_MANIFEST.is_file()
    assert len(manifest.checks) >= 35
    assert {entry.tier for entry in manifest.checks} == {0, 1, 2, 3, 4}
    assert all(entry.final_only_justification for entry in manifest.checks if entry.tier == 4)


def test_every_rollout_step_has_an_earliest_stage_consumer_mapping() -> None:
    manifest = load_coverage_manifest()
    step_names = {step.name for step in default_step_sequence()}
    assert step_names <= manifest.consumers


def test_every_legacy_operator_predicate_is_mapped_before_adapter_removal() -> None:
    manifest = load_coverage_manifest()
    assert _legacy_operator_preflight_names() == manifest.legacy_checks


def test_known_late_failures_are_shifted_before_final_only() -> None:
    manifest = load_coverage_manifest()
    entries = {entry.check_id: entry for entry in manifest.checks}
    for check_id in (
        "credentials.metadata",
        "readonly.authority",
        "manifests.field-ownership",
        "browser.runtime",
        "gb10.host-readiness",
        "capacity.high-water",
        "external-supervisor.predecessor",
        "staging.release-baseline",
        "systemd.render",
        "rehearsal.migration",
        "rehearsal.release",
        "rehearsal.production-defaults",
        "rehearsal.api-smoke",
        "rehearsal.browser",
    ):
        assert entries[check_id].tier < 4


def test_every_final_subpredicate_has_one_checked_in_earliest_stage() -> None:
    manifest = load_coverage_manifest()
    checks = {entry.check_id: entry for entry in manifest.checks}
    grouped: dict[str, tuple[str, ...]] = {}
    for final_check_id in FINAL_CHECK_IDS:
        grouped[final_check_id] = tuple(
            entry.predicate_id
            for entry in manifest.final_predicates
            if entry.final_check_id == final_check_id
        )

    assert grouped == dict(FINAL_PREDICATE_IDS)
    for predicate in manifest.final_predicates:
        earliest = checks[predicate.earliest_check_id]
        if predicate.preflight_capable:
            assert earliest.tier < 4
            assert predicate.final_only_justification is None
        else:
            assert predicate.earliest_check_id == predicate.final_check_id
            assert predicate.final_only_justification


def test_systemd_activation_is_classified_as_isolated_rehearsal() -> None:
    entries = {entry.check_id: entry for entry in load_coverage_manifest().checks}

    manager = entries["systemd.user-manager"]
    predecessor = entries["external-supervisor.predecessor"]
    render = entries["systemd.render"]
    launch_cancel = entries["lifecycle.launch-cancel"]
    activation = entries["rehearsal.systemd-launch"]
    assert manager.tier == 0
    assert manager.mutation_class is MutationClass.NONE
    assert predecessor.tier == 0
    assert predecessor.stage is StageCapability.BASELINE_LIVE_READONLY
    assert predecessor.mutation_class is MutationClass.NONE
    assert predecessor.dependencies == ("candidate.identity", "systemd.user-manager")
    assert "absent or self-authored bootstrap state fails closed" in predecessor.predicate
    assert render.tier == 1
    assert render.stage is StageCapability.STATIC
    assert render.mutation_class is MutationClass.NONE
    assert {"env-state", "gb10-prep"} <= set(render.consumers)
    assert "every enabled or active staging external autoscaler supervisor" in render.predicate
    assert "service and timer" in render.predicate
    predicates = {entry.predicate_id: entry for entry in load_coverage_manifest().final_predicates}
    assert predicates["protected.external-supervisor-artifact"].earliest_check_id == (
        "systemd.render"
    )
    assert predicates["protected.external-supervisor-predecessor"].earliest_check_id == (
        "external-supervisor.predecessor"
    )
    assert predicates["protected.external-supervisor-transition"].earliest_check_id == (
        "systemd.render"
    )
    assert predicates["protected.external-supervisor-validation"].earliest_check_id == (
        "rehearsal.release"
    )
    assert predicates["protected.external-supervisor-activation"].earliest_check_id == (
        "final.protected-apply"
    )
    assert predicates[
        "protected.external-supervisor-database-secret"
    ].earliest_check_id == "final.protected-apply"
    assert not predicates[
        "protected.external-supervisor-database-secret"
    ].preflight_capable
    assert not predicates["protected.external-supervisor-activation"].preflight_capable
    assert predicates["convergence.external-supervisor-live"].earliest_check_id == (
        "final.convergence"
    )
    assert (
        predicates["convergence.external-supervisor-active-pointer"].earliest_check_id
        == "final.convergence"
    )
    assert (
        predicates["convergence.external-supervisor-database-secret"].earliest_check_id
        == "final.convergence"
    )
    assert launch_cancel.tier == 0
    assert launch_cancel.stage is StageCapability.STATIC
    assert launch_cancel.mutation_class is MutationClass.ISOLATED
    assert activation.tier == 3
    assert activation.stage is StageCapability.ISOLATED_REHEARSAL
    assert activation.mutation_class is MutationClass.ISOLATED
    assert "rehearsal.systemd-launch" in entries["rehearsal.cleanup"].dependencies


def test_coverage_stage_tiers_allow_fast_live_readonly_but_reject_tier_one() -> None:
    base = {
        "check_id": "external-supervisor.predecessor",
        "failure_code": "external-supervisor.predecessor.drift",
        "tier": 0,
        "stage": "baseline-live-readonly",
        "dependencies": [],
        "mutation_class": "none",
        "consumers": ["preflight"],
        "legacy_checks": [],
        "predicate": "Read the exact live predecessor without protected mutation.",
    }

    assert CoverageEntry.from_dict(base).tier == 0
    with pytest.raises(ValueError, match="tier does not match stage"):
        CoverageEntry.from_dict({**base, "tier": 1})


def _registered_from_manifest(
    manifest: CoverageManifest,
    *,
    through_tier: int,
) -> tuple[RegisteredCheck, ...]:
    checks: list[RegisteredCheck] = []
    for entry in manifest.checks:
        if entry.tier > through_tier:
            continue
        checks.append(
            RegisteredCheck(
                spec=CheckSpec(
                    check_id=entry.check_id,
                    failure_code=entry.failure_code,
                    tier=entry.tier,
                    stage=entry.stage,
                    dependencies=entry.dependencies,
                    mutation_class=entry.mutation_class,
                    input_keys=("runner.config.sha256",),
                    evidence_schema=(EvidenceField("ready", "boolean"),),
                    timeout_seconds=5,
                    freshness_ttl_seconds=60,
                    remediation=f"restore {entry.check_id}",
                    secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
                    final_only_justification=entry.final_only_justification,
                ),
                implementation_version="test-v1",
                operations={
                    CheckOperation.PROBE: lambda _context: CheckProbe(
                        passed=True,
                        evidence={"ready": True},
                    )
                },
            )
        )
    return tuple(checks)


def test_coverage_manifest_accepts_one_exact_implementation_per_declared_check() -> None:
    manifest = load_coverage_manifest()
    checks = _registered_from_manifest(manifest, through_tier=1)

    manifest.require_exact_registry(checks, through_tier=1)


def test_coverage_manifest_rejects_missing_or_unexpected_implementation() -> None:
    manifest = load_coverage_manifest()
    checks = _registered_from_manifest(manifest, through_tier=0)

    with pytest.raises(ValueError, match=r"missing=.*backup.lease-eligibility"):
        manifest.require_exact_registry(checks[:-1], through_tier=0)
    with pytest.raises(ValueError, match=r"unexpected=.*extra.check"):
        manifest.require_exact_registry(
            (
                *checks,
                replace(
                    checks[0],
                    spec=replace(
                        checks[0].spec,
                        check_id="extra.check",
                        failure_code="extra.check.failed",
                    ),
                ),
            ),
            through_tier=0,
        )


@pytest.mark.parametrize(
    "field",
    ["failure_code", "tier", "dependencies"],
)
def test_coverage_manifest_rejects_registered_contract_drift(field: str) -> None:
    manifest = load_coverage_manifest()
    checks = list(_registered_from_manifest(manifest, through_tier=0))
    original = checks[0]
    replacements = {
        "failure_code": "candidate.identity.wrong",
        "tier": 1,
        "dependencies": ("runner.install",),
    }
    checks[0] = replace(
        original,
        spec=replace(original.spec, **{field: replacements[field]}),
    )

    with pytest.raises(ValueError, match="contract drifts"):
        manifest.require_exact_registry(tuple(checks), through_tier=0)
