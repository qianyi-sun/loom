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
