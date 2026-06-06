import pytest

from loom_llm_gateway.rate_card import RateCardEntry, compute_cost_usd


def test_basic_cost():
    entry = RateCardEntry(
        provider="anthropic", model="claude-opus-4-7",
        input_per_mtok=3.0, output_per_mtok=15.0,
        cache_read_per_mtok=0.3, cache_write_per_mtok=3.75,
    )
    cost = compute_cost_usd(
        entry, input_tokens=1_000_000, output_tokens=500_000,
        cached_input_tokens=0, cache_write_tokens=0,
    )
    assert cost == pytest.approx(3.0 + 7.5)


def test_cost_with_cache():
    entry = RateCardEntry(
        provider="anthropic", model="claude-opus-4-7",
        input_per_mtok=3.0, output_per_mtok=15.0,
        cache_read_per_mtok=0.3, cache_write_per_mtok=3.75,
    )
    cost = compute_cost_usd(
        entry, input_tokens=500_000, output_tokens=100_000,
        cached_input_tokens=500_000, cache_write_tokens=200_000,
    )
    # base input: 500k × 3/Mtok = $1.50
    # cached read: 500k × 0.3/Mtok = $0.15
    # cache write: 200k × 3.75/Mtok = $0.75
    # output: 100k × 15/Mtok = $1.50
    assert cost == pytest.approx(1.5 + 0.15 + 0.75 + 1.5)


def test_zero_tokens_zero_cost():
    entry = RateCardEntry(
        provider="x", model="y",
        input_per_mtok=10, output_per_mtok=10,
        cache_read_per_mtok=1, cache_write_per_mtok=1,
    )
    assert compute_cost_usd(
        entry, input_tokens=0, output_tokens=0,
        cached_input_tokens=0, cache_write_tokens=0,
    ) == 0.0
