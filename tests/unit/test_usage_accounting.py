from __future__ import annotations

from decimal import Decimal

from loom_service.usage_accounting import summarize_usage_counts


def test_tokens_only_usage_has_no_monetary_amount() -> None:
    summary = summarize_usage_counts(
        llm_calls_count=1,
        total_prompt_tokens=120,
        total_completion_tokens=30,
        total_cost_usd=Decimal("0"),
        priced_llm_calls_count=0,
        token_only_llm_calls_count=1,
        price_unknown_llm_calls_count=0,
    )

    assert summary["total_prompt_tokens"] == 120
    assert summary["total_completion_tokens"] == 30
    assert summary["total_tokens"] == 150
    assert summary["total_cost_usd"] == 0.0
    assert summary["estimated_cost_usd"] is None
    assert summary["cost_currency"] is None
    assert summary["cost_status"] == "not_applicable"
    assert summary["pricing_modes"] == ["tokens-only"]


def test_priced_usage_exposes_estimated_cost_amount() -> None:
    summary = summarize_usage_counts(
        llm_calls_count=2,
        total_prompt_tokens=1000,
        total_completion_tokens=400,
        total_cost_usd=Decimal("0.012345"),
        priced_llm_calls_count=2,
        token_only_llm_calls_count=0,
        price_unknown_llm_calls_count=0,
    )

    assert summary["estimated_cost_usd"] == 0.012345
    assert summary["cost_currency"] == "USD"
    assert summary["cost_status"] == "estimated"
    assert summary["pricing_modes"] == ["priced"]


def test_missing_rate_card_is_price_unknown_not_zero_cost() -> None:
    summary = summarize_usage_counts(
        llm_calls_count=1,
        total_prompt_tokens=500,
        total_completion_tokens=50,
        total_cost_usd=Decimal("0"),
        priced_llm_calls_count=0,
        token_only_llm_calls_count=0,
        price_unknown_llm_calls_count=1,
    )

    assert summary["total_cost_usd"] == 0.0
    assert summary["estimated_cost_usd"] is None
    assert summary["cost_currency"] is None
    assert summary["cost_status"] == "price_unknown"
    assert summary["pricing_modes"] == ["price-unknown"]


def test_mixed_usage_marks_partial_cost_status() -> None:
    summary = summarize_usage_counts(
        llm_calls_count=3,
        total_prompt_tokens=2000,
        total_completion_tokens=800,
        total_cost_usd=Decimal("0.040000"),
        priced_llm_calls_count=1,
        token_only_llm_calls_count=1,
        price_unknown_llm_calls_count=1,
    )

    assert summary["estimated_cost_usd"] == 0.04
    assert summary["cost_currency"] == "USD"
    assert summary["cost_status"] == "mixed"
    assert summary["pricing_modes"] == [
        "priced",
        "tokens-only",
        "price-unknown",
    ]


def test_incomplete_provider_usage_marks_estimate_confidence() -> None:
    summary = summarize_usage_counts(
        llm_calls_count=3,
        total_prompt_tokens=2000,
        total_completion_tokens=800,
        total_cost_usd=Decimal("0.040000"),
        priced_llm_calls_count=3,
        token_only_llm_calls_count=0,
        price_unknown_llm_calls_count=0,
        partial_usage_llm_calls_count=1,
        missing_usage_llm_calls_count=1,
    )

    assert summary["estimated_cost_usd"] == 0.04
    assert summary["partial_usage_llm_calls_count"] == 1
    assert summary["missing_usage_llm_calls_count"] == 1
    assert summary["usage_reporting_status"] == "partial"
    assert summary["usage_estimate_confidence"] == "partial"
