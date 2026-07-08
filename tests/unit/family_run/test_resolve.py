"""Resolver: catalog defaults + trial_config override -> ResolvedFamilyRunSpec."""

from __future__ import annotations

import pytest

from loom.family_run.resolve import (
    FamilyRunNotEnabledError,
    resolve_family_run_spec,
)
from loom.family_run.spec import (
    FamilyRunSpec,
    PluginRef,
    ResolvedFamilyRunSpec,
)


def test_catalog_default_is_used_when_no_override():
    catalog = FamilyRunSpec(
        enabled=True,
        family_key_extractor=PluginRef(name="instance_id_prefix"),
        sequencer=PluginRef(name="alphabetical"),
        advance_predicate=PluginRef(name="always_on_terminal"),
        adapter=PluginRef(name="noop"),
        failure_policy=PluginRef(name="stall_family"),
        state_backend=PluginRef(name="s3_artifacts"),
    )
    resolved = resolve_family_run_spec(catalog=catalog, override=None)
    assert isinstance(resolved, ResolvedFamilyRunSpec)
    assert resolved.enabled is True


def test_override_wins_per_role():
    catalog = FamilyRunSpec(
        enabled=True,
        family_key_extractor=PluginRef(name="instance_id_prefix"),
        sequencer=PluginRef(name="alphabetical"),
        advance_predicate=PluginRef(name="always_on_terminal"),
        adapter=PluginRef(name="noop"),
        failure_policy=PluginRef(name="stall_family"),
        state_backend=PluginRef(name="s3_artifacts"),
    )
    override = FamilyRunSpec(
        adapter=PluginRef(name="skill_patcher_llm", params={"model": "opus"}),
        mount_path="/root/.custom",
    )
    resolved = resolve_family_run_spec(catalog=catalog, override=override)
    assert resolved.adapter.name == "skill_patcher_llm"
    assert resolved.adapter.params == {"model": "opus"}
    assert resolved.mount_path == "/root/.custom"
    assert resolved.sequencer.name == "alphabetical"


def test_enabled_only_override_switches_on():
    catalog = FamilyRunSpec()  # no catalog defaults
    override = FamilyRunSpec(
        enabled=True,
        family_key_extractor=PluginRef(name="instance_id_prefix"),
        sequencer=PluginRef(name="alphabetical"),
        advance_predicate=PluginRef(name="always_on_terminal"),
        adapter=PluginRef(name="noop"),
        failure_policy=PluginRef(name="stall_family"),
        state_backend=PluginRef(name="s3_artifacts"),
    )
    resolved = resolve_family_run_spec(catalog=catalog, override=override)
    assert resolved.enabled is True


def test_not_enabled_raises():
    with pytest.raises(FamilyRunNotEnabledError):
        resolve_family_run_spec(catalog=None, override=None)


def test_enabled_but_missing_role_raises():
    with pytest.raises(ValueError, match="sequencer"):
        resolve_family_run_spec(
            catalog=None,
            override=FamilyRunSpec(
                enabled=True,
                family_key_extractor=PluginRef(name="instance_id_prefix"),
                # sequencer missing
                advance_predicate=PluginRef(name="always_on_terminal"),
                adapter=PluginRef(name="noop"),
                failure_policy=PluginRef(name="stall_family"),
                state_backend=PluginRef(name="s3_artifacts"),
            ),
        )
