"""builtin.py reads entry_points(group='loom.benchmarks')."""

from __future__ import annotations

import pytest

from loom_cli.builtin import load_builtin_entries


def test_load_returns_all_shipped_adapters() -> None:
    """PR-2 (per-year AIME split): aime ships as aime-22/aime-23/aime-24/
    aime-25 — one slug per exam year so users can pick AIME-24 / AIME-25
    / both in a single click. `swe-bench-verified` restored as a peer
    of `swe-bench` and `swe-bench-multimodal`. Floor bumps to 14."""
    entries = load_builtin_entries()
    slugs = {e.slug for e in entries}
    assert "humaneval" in slugs
    assert "aime-22" in slugs
    assert "aime-23" in slugs
    assert "aime-24" in slugs
    assert "aime-25" in slugs
    assert "swe-bench" in slugs
    assert "swe-bench-verified" in slugs
    assert "swe-bench-multimodal" in slugs
    assert len(entries) >= 14


def test_each_entry_has_display_name_and_license() -> None:
    entries = load_builtin_entries()
    he = next(e for e in entries if e.slug == "humaneval")
    assert he.display_name == "HumanEval"
    assert he.license_spdx == "MIT"
    assert he.source == "builtin"


def test_entry_carries_upstream_kind_from_adapter() -> None:
    """#234: the listing layer must surface adapter.upstream_source.kind
    so `loom datasets list` can show an UPSTREAM column."""
    entries = load_builtin_entries()
    he = next(e for e in entries if e.slug == "humaneval")
    assert he.upstream_kind == "huggingface"
    # bfcl uses a git upstream — sanity-check we propagate that too.
    bfcl = next(e for e in entries if e.slug == "bfcl")
    assert bfcl.upstream_kind == "git"


def test_third_party_adapter_without_upstream_source_falls_back_to_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Defensive: an adapter that doesn't expose `upstream_source`
    (legacy third-party shape) returns None for `upstream_kind` rather
    than crashing the loader."""
    from loom_cli import builtin

    class _NoUpstreamAdapter:
        display_name = "Bare"
        license_spdx = "MIT"
        license_url = ""

    class _Stub:
        name = "no-upstream"
        module = "x"
        attr = "Y"
        value = "x:Y"

        def load(self) -> object:
            return _NoUpstreamAdapter

    def _fake_eps(group: str) -> list[_Stub]:
        return [_Stub()]

    monkeypatch.setattr(builtin, "_entry_points", _fake_eps)
    out = builtin.load_builtin_entries()
    assert len(out) == 1
    assert out[0].upstream_kind is None


def test_terminal_bench_2_surfaces_pinned_task_count() -> None:
    """`TerminalBench2Adapter` declares `task_count = 86` (the pinned
    upstream's official slate). Without this surfacing path,
    `loom datasets list` previously showed `-` for TB-2 because
    `default-catalog.json` is masked when a builtin entry-point with the
    same slug exists. Tracks G1 of #217 — the original fix mistakenly
    edited the masked catalog row."""
    entries = load_builtin_entries()
    tb2 = next(e for e in entries if e.slug == "terminal-bench-2")
    # 86 upstream tasks minus 2 (broken-networking, extract-safely) that
    # ship Dockerfiles which structurally cannot pass Loom's task-bundle
    # compatibility validators — see adapter._UNSUPPORTED_INSTANCES + #760.
    assert tb2.task_count == 84


def test_third_party_adapter_without_task_count_falls_back_to_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Defensive: adapters with no `task_count` attribute (or whose count
    is dynamic) leave the field unset; the column shows `-`."""
    from loom_cli import builtin

    class _NoCountAdapter:
        display_name = "Dynamic"
        license_spdx = "MIT"
        license_url = ""

    class _Stub:
        name = "dynamic"
        module = "x"
        attr = "Y"
        value = "x:Y"

        def load(self) -> object:
            return _NoCountAdapter

    def _fake_eps(group: str) -> list[_Stub]:
        return [_Stub()]

    monkeypatch.setattr(builtin, "_entry_points", _fake_eps)
    out = builtin.load_builtin_entries()
    assert len(out) == 1
    assert out[0].task_count is None


def test_non_int_task_count_attribute_is_ignored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An adapter that misuses `task_count` (e.g. sets it to a string or
    a callable) should fall back to None — never propagate a malformed
    value into `DatasetEntry`."""
    from loom_cli import builtin

    class _BadCountAdapter:
        display_name = "Bad"
        license_spdx = "MIT"
        license_url = ""
        task_count = "lots"

    class _Stub:
        name = "bad-count"
        module = "x"
        attr = "Y"
        value = "x:Y"

        def load(self) -> object:
            return _BadCountAdapter

    def _fake_eps(group: str) -> list[_Stub]:
        return [_Stub()]

    monkeypatch.setattr(builtin, "_entry_points", _fake_eps)
    out = builtin.load_builtin_entries()
    assert out[0].task_count is None


def test_broken_entry_point_does_not_crash_loader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_cli import builtin

    class _Boom:
        name = "broken"

        def load(self) -> object:
            raise ImportError("no such module")

    def _fake_eps(group: str) -> list[_Boom]:
        return [_Boom()]

    monkeypatch.setattr(builtin, "_entry_points", _fake_eps)
    out = builtin.load_builtin_entries()
    assert out == []
