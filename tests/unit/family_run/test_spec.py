"""Family-run spec models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from loom.family_run.spec import (
    AdvanceDecision,
    FailureAction,
    FamilyRunSpec,
    PluginRef,
    ResolvedFamilyRunSpec,
)


def test_plugin_ref_defaults_to_empty_params() -> None:
    ref = PluginRef(name="alphabetical")
    assert ref.name == "alphabetical"
    assert ref.params == {}


def test_plugin_ref_forbids_extra_fields() -> None:
    with pytest.raises(ValidationError):
        PluginRef.model_validate({"name": "x", "unknown": 1})


def test_family_run_spec_all_optional() -> None:
    spec = FamilyRunSpec()
    assert spec.enabled is None
    assert spec.family_key_extractor is None


def test_resolved_family_run_spec_requires_every_role() -> None:
    with pytest.raises(ValidationError):
        ResolvedFamilyRunSpec.model_validate({"enabled": True})


def test_resolved_family_run_spec_round_trip() -> None:
    payload = {
        "enabled": True,
        "family_key_extractor": {"name": "instance_id_prefix", "params": {"depth": 1}},
        "sequencer": {"name": "alphabetical", "params": {}},
        "advance_predicate": {"name": "always_on_terminal", "params": {}},
        "adapter": {"name": "noop", "params": {}},
        "failure_policy": {"name": "stall_family", "params": {}},
        "state_backend": {"name": "s3_artifacts", "params": {}},
        "mount_path": "/root/.skills",
    }
    spec = ResolvedFamilyRunSpec.model_validate(payload)
    assert spec.model_dump(mode="json") == payload


def test_advance_decision_values() -> None:
    assert AdvanceDecision.ADVANCE.value == "advance"
    assert AdvanceDecision.RETRY.value == "retry"
    assert AdvanceDecision.SKIP.value == "skip"
    assert AdvanceDecision.ABORT.value == "abort"


def test_failure_action_retry_with_backoff() -> None:
    action = FailureAction.retry_with_backoff(30.0)
    assert action.kind == "retry_with_backoff"
    assert action.backoff_sec == 30.0
