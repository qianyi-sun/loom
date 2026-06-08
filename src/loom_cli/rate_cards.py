"""Local rate-card storage for the CLI.

The in-tree default lives at `src/loom_cli/data/default-rate-cards.toml`
and is copied to `~/.config/loom/rate-cards.toml` on first run. Users can
edit the user file to add or override entries.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

from loom_cli.config import _xdg_config_home

USER_RATE_CARDS_FILENAME = "rate-cards.toml"


def rate_cards_path() -> Path:
    return _xdg_config_home() / "loom" / USER_RATE_CARDS_FILENAME


@dataclass(frozen=True)
class RateCardEntry:
    provider: str
    model: str
    input_per_mtok: float
    output_per_mtok: float
    cache_read_per_mtok: float
    cache_write_per_mtok: float


@dataclass(frozen=True)
class RateCardTable:
    entries: tuple[RateCardEntry, ...]


def _default_text() -> str:
    return resources.files("loom_cli.data").joinpath(
        "default-rate-cards.toml",
    ).read_text(encoding="utf-8")


def seed_default_if_missing() -> None:
    p = rate_cards_path()
    if p.exists():
        return
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(_default_text())


def load_rate_cards() -> RateCardTable:
    p = rate_cards_path()
    text = p.read_text() if p.exists() else _default_text()
    raw = tomllib.loads(text)
    entries_obj = raw.get("entries", [])
    if not isinstance(entries_obj, list):
        raise ValueError(f"{p}: [[entries]] must be a list of tables")
    out: list[RateCardEntry] = []
    for row in entries_obj:
        out.append(RateCardEntry(
            provider=str(row["provider"]),
            model=str(row["model"]),
            input_per_mtok=float(row["input_per_mtok"]),
            output_per_mtok=float(row["output_per_mtok"]),
            cache_read_per_mtok=float(row["cache_read_per_mtok"]),
            cache_write_per_mtok=float(row["cache_write_per_mtok"]),
        ))
    return RateCardTable(entries=tuple(out))


def lookup_entry(
    table: RateCardTable, *, provider: str, model: str,
) -> RateCardEntry:
    for e in table.entries:
        if e.provider == provider and e.model == model:
            return e
    raise KeyError(
        f"no rate card entry for provider={provider!r} model={model!r}; "
        f"add one to {rate_cards_path()}",
    )


def compute_cost_usd(
    entry: RateCardEntry, *,
    input_tokens: int, output_tokens: int,
    cached_input_tokens: int, cache_write_tokens: int,
) -> float:
    def _at(toks: int, per_mtok: float) -> float:
        return toks * per_mtok / 1_000_000
    return (
        _at(input_tokens, entry.input_per_mtok)
        + _at(output_tokens, entry.output_per_mtok)
        + _at(cached_input_tokens, entry.cache_read_per_mtok)
        + _at(cache_write_tokens, entry.cache_write_per_mtok)
    )
