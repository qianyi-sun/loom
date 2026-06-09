"""union_entries: precedence builtin > remote > catalog on slug dedup."""

from __future__ import annotations

from loom_cli.discovery import DatasetEntry, union_entries


def _e(slug: str, source: str, status: str) -> DatasetEntry:
    return DatasetEntry(
        slug=slug, source=source, display_name=slug.title(),
        license_spdx="MIT", license_url="", task_count=None,
        status=status, available_pip_spec=None, entry_point=None,
    )


def test_union_dedupes_with_builtin_winning() -> None:
    builtin = [_e("humaneval", "builtin", "installed")]
    catalog = [_e("humaneval", "catalog", "available"),
                _e("terminal-bench-2", "catalog", "available")]
    remote = [_e("custom", "remote", "remote-only")]
    out = union_entries(builtin=builtin, catalog=catalog, remote=remote)
    by_slug = {e.slug: e for e in out}
    assert by_slug["humaneval"].source == "builtin"
    assert by_slug["humaneval"].status == "installed"
    assert by_slug["terminal-bench-2"].source == "catalog"
    assert by_slug["custom"].source == "remote"
    assert [e.slug for e in out] == sorted(by_slug)


def test_union_remote_wins_over_catalog() -> None:
    catalog = [_e("rl-bench", "catalog", "available")]
    remote = [_e("rl-bench", "remote", "remote-only")]
    out = union_entries(builtin=[], catalog=catalog, remote=remote)
    assert len(out) == 1
    assert out[0].source == "remote"
