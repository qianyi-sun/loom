from __future__ import annotations

import math
from collections.abc import Iterable
from typing import Any

from agentic_data_platform.domain.run_records import RunRecord


PROVIDER_USAGE_SCHEMA_VERSION = "model-provider-usage-v1"


def normalize_model_provider_usage(
    metrics: dict[str, Any],
    *,
    source: str,
    provider: str | None = None,
    model_name: str | None = None,
) -> dict[str, Any] | None:
    usage: dict[str, Any] = {
        "schema_version": PROVIDER_USAGE_SCHEMA_VERSION,
        "source": source.strip() or "unknown",
    }

    provider_value = _optional_non_empty_string(provider)
    if provider_value is not None:
        usage["provider"] = provider_value
    model_value = _optional_non_empty_string(model_name)
    if model_value is not None:
        usage["model_name"] = model_value

    input_tokens = _non_negative_int_from_aliases(
        metrics,
        "input_tokens",
        "n_input_tokens",
        "prompt_tokens",
        "input_token_count",
        "prompt_token_count",
    )
    output_tokens = _non_negative_int_from_aliases(
        metrics,
        "output_tokens",
        "n_output_tokens",
        "completion_tokens",
        "output_token_count",
        "completion_token_count",
    )
    total_tokens = _non_negative_int_from_aliases(
        metrics,
        "total_tokens",
        "n_total_tokens",
        "token_count",
    )
    if total_tokens is None and input_tokens is not None and output_tokens is not None:
        total_tokens = input_tokens + output_tokens

    cost_usd = _non_negative_float_from_aliases(metrics, "cost_usd", "total_cost_usd")
    duration_seconds = _non_negative_float_from_aliases(
        metrics,
        "duration_seconds",
        "latency_seconds",
    )
    if duration_seconds is None:
        latency_ms = _non_negative_float_from_aliases(metrics, "latency_ms")
        if latency_ms is not None:
            duration_seconds = latency_ms / 1000

    if input_tokens is not None:
        usage["input_tokens"] = input_tokens
    if output_tokens is not None:
        usage["output_tokens"] = output_tokens
    if total_tokens is not None:
        usage["total_tokens"] = total_tokens
    if cost_usd is not None:
        usage["cost_usd"] = cost_usd
    if duration_seconds is not None:
        usage["duration_seconds"] = duration_seconds

    observed_keys = {"input_tokens", "output_tokens", "total_tokens", "cost_usd", "duration_seconds"}
    if not any(key in usage for key in observed_keys):
        return None
    return usage


def model_provider_usage_from_metadata(metadata: dict[str, Any]) -> dict[str, Any] | None:
    raw = metadata.get("provider_usage") or metadata.get("model_provider_usage")
    if not isinstance(raw, dict):
        return None
    source = _optional_non_empty_string(raw.get("source")) or "unknown"
    return normalize_model_provider_usage(
        raw,
        source=source,
        provider=_optional_non_empty_string(raw.get("provider")),
        model_name=_optional_non_empty_string(raw.get("model_name")),
    )


def aggregate_model_provider_usage(runs: Iterable[RunRecord]) -> dict[str, Any]:
    totals = _empty_usage_summary()
    by_provider: dict[str, dict[str, Any]] = {}
    by_model: dict[str, dict[str, Any]] = {}
    provider_models: dict[str, set[str]] = {}

    for run in runs:
        usage = _usage_for_run(run)
        if usage is None:
            continue
        _add_usage(totals, usage)
        provider = _optional_non_empty_string(usage.get("provider"))
        model_name = _optional_non_empty_string(usage.get("model_name"))
        if provider is not None:
            provider_summary = by_provider.setdefault(provider, _empty_usage_summary())
            _add_usage(provider_summary, usage)
            if model_name is not None:
                provider_models.setdefault(provider, set()).add(model_name)
        if model_name is not None:
            _add_usage(by_model.setdefault(model_name, _empty_usage_summary()), usage)

    for provider, summary in by_provider.items():
        summary["model_count"] = len(provider_models.get(provider, set()))

    return {
        "schema_version": PROVIDER_USAGE_SCHEMA_VERSION,
        "totals": totals,
        "by_provider": by_provider,
        "by_model": by_model,
    }


def _usage_for_run(run: RunRecord) -> dict[str, Any] | None:
    for result in reversed(run.all_evaluator_results()):
        usage = model_provider_usage_from_metadata(result.metadata)
        if usage is not None:
            return usage
    return model_provider_usage_from_metadata(run.metadata)


def _empty_usage_summary() -> dict[str, Any]:
    return {
        "run_count": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "cost_usd": 0.0,
        "duration_seconds": 0.0,
    }


def _add_usage(summary: dict[str, Any], usage: dict[str, Any]) -> None:
    summary["run_count"] += 1
    for key in ("input_tokens", "output_tokens", "total_tokens"):
        value = usage.get(key)
        if isinstance(value, int):
            summary[key] += value
    for key in ("cost_usd", "duration_seconds"):
        value = usage.get(key)
        if isinstance(value, (int, float)):
            summary[key] += float(value)


def _non_negative_int_from_aliases(metrics: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = _non_negative_float(metrics.get(key))
        if value is not None and value.is_integer():
            return int(value)
    return None


def _non_negative_float_from_aliases(metrics: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _non_negative_float(metrics.get(key))
        if value is not None:
            return value
    return None


def _non_negative_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
    elif isinstance(value, str):
        try:
            numeric = float(value.strip())
        except ValueError:
            return None
    else:
        return None
    if not math.isfinite(numeric) or numeric < 0:
        return None
    return numeric


def _optional_non_empty_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None
