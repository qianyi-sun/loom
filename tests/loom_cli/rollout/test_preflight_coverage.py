from __future__ import annotations

import ast
from pathlib import Path

from loom_cli.rollout.preflight_contract import MutationClass, StageCapability
from loom_cli.rollout.preflight_coverage import (
    DEFAULT_COVERAGE_MANIFEST,
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
        "browser.runtime",
        "gb10.host-readiness",
        "capacity.high-water",
        "staging.release-baseline",
        "rehearsal.migration",
        "rehearsal.api-smoke",
        "rehearsal.browser",
    ):
        assert entries[check_id].tier < 4


def test_systemd_activation_is_classified_as_isolated_rehearsal() -> None:
    entries = {entry.check_id: entry for entry in load_coverage_manifest().checks}

    manager = entries["systemd.user-manager"]
    activation = entries["rehearsal.systemd-launch"]
    assert manager.tier == 0
    assert manager.mutation_class is MutationClass.NONE
    assert activation.tier == 3
    assert activation.stage is StageCapability.ISOLATED_REHEARSAL
    assert activation.mutation_class is MutationClass.ISOLATED
    assert "rehearsal.systemd-launch" in entries["rehearsal.cleanup"].dependencies
