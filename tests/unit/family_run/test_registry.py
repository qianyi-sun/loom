"""Family-run plugin entry-point registry."""

from __future__ import annotations

import pytest

from loom.family_run.registry import (
    RegistryError,
    resolve_plugin,
)
from loom.family_run.spec import PluginRef


class _FakeAlphaSequencer:
    def sequence(self, family_key, tasks, params):
        return sorted(t.id for t in tasks)


def test_resolve_plugin_finds_registered(monkeypatch):
    def _fake_ep(group):
        assert group == "loom.family.sequencers"
        return {"alphabetical": _FakeAlphaSequencer}

    monkeypatch.setattr(
        "loom.family_run.registry._entry_points",
        _fake_ep,
    )
    plugin = resolve_plugin("loom.family.sequencers", PluginRef(name="alphabetical"))
    assert isinstance(plugin, _FakeAlphaSequencer)


def test_resolve_plugin_unknown_name_raises(monkeypatch):
    monkeypatch.setattr(
        "loom.family_run.registry._entry_points",
        lambda group: {},
    )
    with pytest.raises(RegistryError, match="not registered"):
        resolve_plugin("loom.family.sequencers", PluginRef(name="missing"))


def test_resolve_plugin_available_names_listed_in_error(monkeypatch):
    monkeypatch.setattr(
        "loom.family_run.registry._entry_points",
        lambda group: {"a": _FakeAlphaSequencer, "b": _FakeAlphaSequencer},
    )
    with pytest.raises(RegistryError, match="a, b"):
        resolve_plugin("loom.family.sequencers", PluginRef(name="missing"))


def test_shipped_plugins_all_discoverable():
    """Every zero-arg plugin declared in pyproject.toml is discoverable.

    ``s3_artifacts`` requires constructor arguments (store + bucket) so
    it isn't zero-arg constructable. The batch-submit / orchestrator
    code that instantiates it builds the correct constructor call.
    """
    combos = [
        ("loom.family.keys", "instance_id_prefix"),
        ("loom.family.sequencers", "alphabetical"),
        ("loom.family.sequencers", "ranking_file"),
        ("loom.family.sequencers", "submitted_order"),
        ("loom.family.advance", "always_on_terminal"),
        ("loom.family.advance", "success_or_retry_exhausted"),
        ("loom.family.adapters", "noop"),
        ("loom.family.failure_policies", "stall_family"),
        ("loom.family.failure_policies", "skip_and_advance"),
        ("loom.family.failure_policies", "abort_family"),
    ]
    for group, name in combos:
        plugin = resolve_plugin(group, PluginRef(name=name))
        assert plugin is not None
