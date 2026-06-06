from datetime import UTC, datetime

import pytest

from loom.models.types import ModelSpec
from loom_llm_gateway.errors import RateCardNotFoundError
from loom_llm_gateway.rate_card import (
    RateCardEntry,
    RateCardTable,
    hash_table,
    lookup_entry,
)


def _entry(
    provider: str = "anthropic",
    model: str = "claude-opus-4-7",
    tier: str | None = None,
    region: str | None = None,
    input_: float = 3.0,
    output: float = 15.0,
    cache_read: float = 0.3,
    cache_write: float = 3.75,
) -> RateCardEntry:
    return RateCardEntry(
        provider=provider, model=model, tier=tier, region=region,
        input_per_mtok=input_, output_per_mtok=output,
        cache_read_per_mtok=cache_read, cache_write_per_mtok=cache_write,
    )


def test_exact_match():
    table = RateCardTable(id="card-1", captured_at=datetime.now(UTC),
                          entries=[_entry()])
    spec = ModelSpec(provider="anthropic", name="claude-opus-4-7")
    e = lookup_entry(table, spec)
    assert e.input_per_mtok == 3.0


def test_tier_specific_overrides_default():
    table = RateCardTable(
        id="card-2", captured_at=datetime.now(UTC),
        entries=[_entry(), _entry(tier="1m-context", input_=6.0)],
    )
    spec = ModelSpec(provider="anthropic", name="claude-opus-4-7", tier="1m-context")
    assert lookup_entry(table, spec).input_per_mtok == 6.0


def test_region_specific_overrides_default():
    table = RateCardTable(
        id="card-r", captured_at=datetime.now(UTC),
        entries=[_entry(), _entry(region="us-east-1", input_=4.5)],
    )
    spec = ModelSpec(provider="anthropic", name="claude-opus-4-7", region="us-east-1")
    assert lookup_entry(table, spec).input_per_mtok == 4.5


def test_unknown_model_raises():
    table = RateCardTable(id="card-3", captured_at=datetime.now(UTC),
                          entries=[_entry()])
    spec = ModelSpec(provider="openai", name="gpt-5")
    with pytest.raises(RateCardNotFoundError):
        lookup_entry(table, spec)


def test_tier_specific_spec_falls_back_to_generic_entry():
    """Regression for Bug 5: when no entry matches the requested tier, the
    generic (tier=None) entry should win over any tier-mismatch entry. The
    old scoring tied at 0 and returned the first candidate arbitrarily."""
    table = RateCardTable(
        id="card-fallback", captured_at=datetime.now(UTC),
        entries=[
            _entry(tier="long-context", input_=12.0),  # specialized, wrong tier
            _entry(tier=None, input_=3.0),             # generic baseline
        ],
    )
    spec = ModelSpec(
        provider="anthropic", name="claude-opus-4-7", tier="1m-context",
    )
    chosen = lookup_entry(table, spec)
    assert chosen.tier is None
    assert chosen.input_per_mtok == 3.0


def test_hash_table_is_stable():
    """Same content → same hash, regardless of captured_at."""
    a = RateCardTable(id="x", captured_at=datetime.now(UTC), entries=[_entry()])
    b = RateCardTable(
        id="x", captured_at=datetime(2020, 1, 1, tzinfo=UTC),
        entries=[_entry()],
    )
    assert hash_table(a) == hash_table(b)
