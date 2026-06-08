"""Local rate-card management — seeds defaults on first run, lets users
override per-model entries, and computes cost for one call."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom_cli.rate_cards import (
    USER_RATE_CARDS_FILENAME,
    load_rate_cards,
    lookup_entry,
    rate_cards_path,
    seed_default_if_missing,
)


def test_seed_creates_user_file_with_default_entries(tmp_xdg_home: Path) -> None:
    seed_default_if_missing()
    p = rate_cards_path()
    assert p.exists()
    text = p.read_text()
    assert "claude-opus-4-7" in text
    assert "gpt-4o" in text


def test_seed_does_not_overwrite_existing(tmp_xdg_home: Path) -> None:
    p = rate_cards_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text('[[entries]]\nprovider = "openai"\nmodel = "custom-1"\n'
                 'input_per_mtok = 1.0\noutput_per_mtok = 2.0\n'
                 'cache_read_per_mtok = 0.5\ncache_write_per_mtok = 1.5\n')
    seed_default_if_missing()
    assert p.read_text().startswith('[[entries]]\nprovider = "openai"\nmodel = "custom-1"')


def test_lookup_entry_returns_matching_row(tmp_xdg_home: Path) -> None:
    seed_default_if_missing()
    table = load_rate_cards()
    entry = lookup_entry(table, provider="anthropic", model="claude-opus-4-7")
    assert entry.provider == "anthropic"
    assert entry.model == "claude-opus-4-7"
    assert entry.input_per_mtok > 0
    assert entry.output_per_mtok > 0


def test_lookup_entry_unknown_raises(tmp_xdg_home: Path) -> None:
    seed_default_if_missing()
    table = load_rate_cards()
    with pytest.raises(KeyError, match="no rate card entry"):
        lookup_entry(table, provider="openai", model="nonexistent-model-xyz")


def test_rate_cards_path_under_xdg(tmp_xdg_home: Path) -> None:
    assert rate_cards_path() == tmp_xdg_home / "loom" / USER_RATE_CARDS_FILENAME
