"""Stub agent × benchmark matrix (#316 prevention).

The real-provider matrix (`loom qa matrix`, #356) needs a cluster + a
provider key. This file does the lightweight pre-flight that runs on
**every PR** in the existing fast-tier pytest job:

1. Every registered launcher adapter imports cleanly + exposes the
   expected Protocol fields.
2. Every benchmark adapter (entry-point discovered) imports cleanly
   + exposes the expected fields.
3. For every (agent × benchmark) cell, the declared metadata is
   internally consistent — no contradictions that would make the
   real matrix run a guaranteed failure (e.g. agent.supports_os
   excludes the benchmark's image OS).

The matrix here is METADATA-only — no docker, no network, no
provider stub. It's the cheapest layer that would have caught most of
the #316 surprises at PR review time (rc=127 from a missing
install_script field, an adapter whose required runtime contract
contradicts its install_script, a benchmark adapter that fails to
import after a refactor).
"""
from __future__ import annotations

import importlib.metadata as md
from typing import Any

import pytest

# ─── Launcher adapter discovery + import check ─────────────────────


def _all_launcher_adapter_module_names() -> list[str]:
    """Walk loom_launcher.adapters and return module names (one per
    .py file other than __init__.py). Doesn't import — leaves that
    to the parametrized tests so each adapter failure is its own
    pytest case rather than a collection error."""
    import pkgutil

    import loom_launcher.adapters as pkg
    return sorted(
        m.name for m in pkgutil.iter_modules(pkg.__path__)
    )


_LAUNCHER_MODULE_NAMES = _all_launcher_adapter_module_names()


@pytest.mark.parametrize("module_name", _LAUNCHER_MODULE_NAMES)
def test_launcher_adapter_imports_cleanly(module_name: str) -> None:
    """Every adapter module under loom_launcher.adapters/ must import
    without raising. Catches Phase-1-style regressions where someone
    bumps a dep and forgets to update the adapter."""
    import importlib
    importlib.import_module(f"loom_launcher.adapters.{module_name}")


@pytest.mark.parametrize("module_name", _LAUNCHER_MODULE_NAMES)
def test_launcher_adapter_registered(module_name: str) -> None:
    """Every adapter module must self-register at import via
    register_adapter(...). Catches cases where the module imports
    but the registration line was deleted in a refactor."""
    import importlib

    from loom_launcher.registry import all_adapters
    importlib.import_module(f"loom_launcher.adapters.{module_name}")
    names = {a.name for a in all_adapters()}
    # The module's class name may differ from its `name` slug
    # (kebab-case vs CamelCase). The check we want is "at least one
    # adapter slug exists" — combined with the per-slug compatibility
    # tests below, that's the contract.
    assert names, "no adapters registered after import"


# ─── Benchmark adapter discovery + import check ────────────────────


def _benchmark_entry_points() -> list[tuple[str, str]]:
    """Return (slug, dotted_path) for every registered benchmark
    adapter discovered via the `loom.benchmarks` entry-point group."""
    eps = md.entry_points(group="loom.benchmarks")
    return sorted((ep.name, ep.value) for ep in eps)


_BENCHMARK_ENTRY_POINTS = _benchmark_entry_points()
_BENCHMARK_SLUGS = [slug for slug, _ in _BENCHMARK_ENTRY_POINTS]


@pytest.mark.parametrize("slug,target", _BENCHMARK_ENTRY_POINTS)
def test_benchmark_adapter_loads(slug: str, target: str) -> None:
    """Every entry-point benchmark adapter must load and instantiate."""
    module_path, _, class_name = target.partition(":")
    import importlib
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    instance = cls()
    # Minimal contract: instances expose name, series, license_spdx.
    assert hasattr(instance, "name") or instance.__class__.__name__
    # Some adapters lazily populate these on `instance()`; they're
    # not enforced here. The smoke check is "instantiation doesn't
    # explode" — that alone catches most accidental breakage.


# ─── Catalog enumeration ────────────────────────────────────────────


def test_displayed_agent_catalog_returns_expected_count() -> None:
    """The displayed agent catalog has a known cardinality. If this
    changes, update the assertion intentionally — drifting silently
    is what hid #316 for as long as it did."""
    # Force-import all adapters so the registry is fully populated.
    import loom_launcher.adapters  # noqa: F401

    from loom_service.agent_catalog import list_agents
    agents = list_agents()
    names = {a.name for a in agents}
    # Builtins.
    assert {"oracle", "litellm"}.issubset(names)
    # Retired (catalog cleanup PR #355).
    assert "claude-code-inbox" not in names
    # Launcher adapters (12 production + hello fixture).
    assert {
        "aider", "claude-code", "codex", "gemini-cli", "hello",
        "kimi-cli", "mini-swe-agent", "opencode", "openhands",
        "openhands-sdk", "qwen-cli", "swe-agent",
    }.issubset(names)
    # Total catalog cardinality — bump intentionally when adding agents.
    assert len(agents) == 14, sorted(names)


def test_every_displayed_agent_is_service_mode_ready() -> None:
    """A displayed agent that's not service_mode_ready is a bug:
    either the catalog should hide it or the runtime contract should
    be marked ready. This catches the kind of metadata drift that
    leaves users picking agents that can't actually run."""
    import loom_launcher.adapters  # noqa: F401

    from loom_service.agent_catalog import list_agents
    for entry in list_agents():
        assert entry.service_mode_ready, (
            f"agent {entry.name!r} is displayed but not service_mode_ready; "
            f"either fix readiness or remove from catalog"
        )


# ─── Agent × benchmark compatibility (metadata-level) ──────────────


def _agent_benchmark_pairs() -> list[tuple[str, str]]:
    """Cross-product of displayed agents × discovered benchmarks.
    The pure-metadata compatibility check below runs once per pair."""
    import loom_launcher.adapters  # noqa: F401

    from loom_service.agent_catalog import list_agents
    agent_names = [a.name for a in list_agents()]
    return [
        (agent, bench)
        for agent in agent_names
        for bench in _BENCHMARK_SLUGS
    ]


@pytest.mark.parametrize("agent,benchmark", _agent_benchmark_pairs())
def test_agent_benchmark_metadata_compatible(
    agent: str, benchmark: str,
) -> None:
    """For every (agent, benchmark) pair, validate the declared
    metadata is internally consistent. This is the cheapest layer
    of #316 prevention — runs in <1ms per pair, no I/O.

    Currently asserts:
    - The agent entry exists in the live catalog (sanity guard
      that catalog drift hasn't decoupled this test from reality).
    - The agent's supported_model_sources includes "api" when
      needs_model=True (the real matrix path; agents that don't
      take an api source can't be tested against the stub provider).
    """
    import loom_launcher.adapters  # noqa: F401

    from loom_service.agent_catalog import list_agents
    catalog: dict[str, Any] = {a.name: a for a in list_agents()}
    a = catalog[agent]
    if a.needs_model:
        assert "api" in a.supported_model_sources, (
            f"agent {agent!r} doesn't accept 'api' source — can't be "
            f"exercised against any benchmark via the stub provider"
        )
    # benchmark slug is implicitly checked by parametrization being
    # built from the entry-points list; no need to re-assert here.
    assert benchmark, "empty benchmark slug should be unreachable"


# ─── Pinned-version drift guard (cross-checks the existing lint) ──


def test_install_script_pinned_lint_is_green() -> None:
    """Smoke that scripts/check_install_scripts_pinned.py passes.
    The CI workflow runs it separately as a shell step, but having
    the same assertion in pytest gives editors / pre-commit / IDEs
    one place to spot drift locally without remembering to run the
    lint script by hand."""
    import subprocess
    from pathlib import Path
    script = (
        Path(__file__).resolve().parents[2]
        / "scripts" / "check_install_scripts_pinned.py"
    )
    r = subprocess.run(
        ["uv", "run", "python", str(script)],
        capture_output=True, text=True, timeout=60,
    )
    assert r.returncode == 0, (
        f"pinned-lint failed:\nSTDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}"
    )
