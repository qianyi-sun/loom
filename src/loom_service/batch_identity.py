"""Generated batch identity helpers.

The backend owns default names/descriptions so browser, CLI, and API
submissions share the same discoverable shape.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from loom.models.batch import Combination

MAX_GENERATED_NAME_LENGTH = 160
MAX_NAME_PART_LENGTH = 32


@dataclass(frozen=True)
class BatchIdentity:
    name: str
    description: str


def build_batch_identity(
    *,
    task_filter: dict[str, Any],
    trial_config: dict[str, Any],
    combinations: Sequence[dict[str, Any] | Combination],
    n_per_task: int,
    backend: str,
    suffix: str | None = None,
    provider_model_id: str | None = None,
) -> BatchIdentity:
    task_name, task_desc = _task_part(task_filter)
    subset_name, subset_desc = _subset_part(task_filter)
    tags_desc = _tag_filter_part(task_filter)
    combo_name, combo_desc = _combination_part(
        trial_config=trial_config,
        combinations=combinations,
        n_per_task=n_per_task,
        provider_model_id=provider_model_id,
    )

    parts = [task_name]
    if subset_name and subset_name != task_name:
        parts.append(subset_name)
    name = f"{' '.join(parts)} | {combo_name}"
    suffix_clean = _clean_suffix(suffix)
    if suffix_clean:
        name = f"{name} - {suffix_clean}"
    name = _truncate(name, MAX_GENERATED_NAME_LENGTH)

    task_description = f"Tasks: {task_desc}; subset: {subset_desc}"
    if tags_desc:
        task_description = f"{task_description}; tags: {tags_desc}"
    description = f"{task_description}. Combinations: {combo_desc}. Backend: {backend}."
    return BatchIdentity(name=name, description=description)


def _task_part(task_filter: dict[str, Any]) -> tuple[str, str]:
    source_ids = _source_ids(task_filter)
    if source_ids:
        compact = _join_compact(source_ids, max_items=3)
        return compact, ", ".join(source_ids)

    task_ids = task_filter.get("task_ids")
    if isinstance(task_ids, list) and task_ids:
        count = len(task_ids)
        return f"explicit{count}", f"{count} explicit task id(s)"

    license_value = task_filter.get("license")
    if isinstance(license_value, str) and license_value:
        return _short_token(license_value), f"license {license_value}"

    return "custom", "custom task filter"


def _source_ids(task_filter: dict[str, Any]) -> list[str]:
    ids: list[str] = []

    benchmark_ids = task_filter.get("benchmark_ids")
    if isinstance(benchmark_ids, list):
        ids.extend(str(item) for item in benchmark_ids if str(item))
    else:
        benchmark_id = task_filter.get("benchmark_id")
        if isinstance(benchmark_id, str) and benchmark_id:
            ids.append(benchmark_id)

    task_set_ids = task_filter.get("task_set_ids")
    if isinstance(task_set_ids, list):
        ids.extend(str(item) for item in task_set_ids if str(item))
    else:
        task_set_id = task_filter.get("task_set_id")
        if isinstance(task_set_id, str) and task_set_id:
            ids.append(task_set_id)

    return ids


def _subset_part(task_filter: dict[str, Any]) -> tuple[str, str]:
    kind = str(task_filter.get("subset_kind") or "all")
    task_ids = task_filter.get("task_ids")
    if kind == "explicit" or isinstance(task_ids, list):
        count = len(task_ids) if isinstance(task_ids, list) else 0
        return (f"explicit{count}" if count else "explicit", f"{count} explicit task id(s)")
    if kind == "all":
        return "", "all tasks"

    n_raw = task_filter.get("n")
    n = int(n_raw) if isinstance(n_raw, int) and n_raw > 0 else None
    if kind == "random_n":
        seed = task_filter.get("seed")
        name = f"random{n}" if n is not None else "random"
        desc = f"random {n}" if n is not None else "random sample"
        if seed is not None:
            desc = f"{desc} seed {seed}"
        return name, desc
    if kind == "first_n":
        return (f"first{n}" if n is not None else "first", f"first {n}" if n is not None else "first tasks")
    if kind == "last_n":
        return (f"last{n}" if n is not None else "last", f"last {n}" if n is not None else "last tasks")
    return _short_token(kind), kind.replace("_", " ")


def _tag_filter_part(task_filter: dict[str, Any]) -> str:
    tag_filters = task_filter.get("tag_filters")
    if not isinstance(tag_filters, dict):
        return ""
    parts: list[str] = []
    for key in sorted(tag_filters):
        raw_values = tag_filters.get(key)
        if not isinstance(raw_values, list):
            continue
        values = [str(value) for value in raw_values if str(value)]
        if not values:
            continue
        parts.append(f"{key}={'|'.join(sorted(values))}")
    return ", ".join(parts)


def _combination_part(
    *,
    trial_config: dict[str, Any],
    combinations: Sequence[dict[str, Any] | Combination],
    n_per_task: int,
    provider_model_id: str | None,
) -> tuple[str, str]:
    combo_dicts = [_combo_to_dict(item) for item in combinations]
    if not combo_dicts:
        combo_dicts = [{
            "agent_name": trial_config.get("agent_name")
            or ((trial_config.get("agent") or {}).get("name") if isinstance(trial_config.get("agent"), dict) else None)
            or "default",
            "agent_model": trial_config.get("agent_model")
            or ((trial_config.get("agent") or {}).get("model") if isinstance(trial_config.get("agent"), dict) else None),
            "n_per_task": n_per_task,
            "label": None,
        }]

    name_items: list[str] = []
    desc_items: list[str] = []
    for combo in combo_dicts:
        agent = str(combo.get("agent_name") or "default")
        model = combo.get("agent_model")
        routed_model_id = str(
            combo.get("provider_model_id") or provider_model_id or "",
        )
        samples = _positive_int(combo.get("n_per_task")) or n_per_task
        if isinstance(combo.get("label"), str) and combo["label"].strip():
            name_base = _short_token(combo["label"])
        elif isinstance(model, dict):
            name_base = (
                f"{_short_token(agent)}/"
                f"{_short_model_name(str(routed_model_id or model.get('name') or 'model'))}"
            )
        else:
            name_base = _short_token(agent)
        name_items.append(f"{name_base} x{samples}")

        if isinstance(model, dict):
            provider = str(model.get("provider") or "model")
            model_name = _short_model_name(
                str(routed_model_id or model.get("name") or "model"),
            )
            desc_items.append(f"{agent}/{provider}/{model_name} x{samples}")
        else:
            desc_items.append(f"{agent}/no-model x{samples}")

    return _join_compact(name_items, max_items=2, clean=False), "; ".join(desc_items)


def _combo_to_dict(combo: dict[str, Any] | Combination) -> dict[str, Any]:
    if isinstance(combo, Combination):
        return combo.model_dump(mode="json")
    return dict(combo)


def _positive_int(value: Any) -> int | None:
    if isinstance(value, int) and value > 0:
        return value
    return None


def _short_model_name(value: str) -> str:
    raw = value.strip().strip("/")
    if "/" in raw:
        raw = raw.rsplit("/", 1)[-1]
    if raw.startswith("models/"):
        raw = raw.removeprefix("models/")
    return _short_token(raw)


def _short_token(value: str) -> str:
    cleaned = " ".join(value.strip().split())
    cleaned = cleaned.replace(" ", "-").replace("|", "-")
    cleaned = cleaned.strip("-")
    return _truncate(cleaned or "unknown", MAX_NAME_PART_LENGTH)


def _clean_suffix(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(value.strip().split())
    cleaned = cleaned.replace("|", "-")
    if not cleaned:
        return None
    return _truncate(cleaned, 48)


def _join_compact(
    items: Sequence[str],
    *,
    max_items: int,
    clean: bool = True,
) -> str:
    shown = [
        _short_token(item) if clean else item
        for item in items[:max_items]
    ]
    suffix = ""
    if len(items) > max_items:
        suffix = f"+{len(items) - max_items}"
    return "+".join(shown) + suffix


def _truncate(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    if limit <= 3:
        return value[:limit]
    return value[: limit - 3].rstrip("- ") + "..."
