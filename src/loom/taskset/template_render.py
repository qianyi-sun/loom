"""Jinja2 rendering for TaskSet ``task_template`` (#242 sub-plan 3)."""

from __future__ import annotations

from typing import Any, cast

from jinja2 import Environment, StrictUndefined


def _render_value(value: Any, env: Environment, ctx: dict[str, Any]) -> Any:
    if isinstance(value, str):
        return env.from_string(value).render(**ctx)
    if isinstance(value, dict):
        return {k: _render_value(v, env, ctx) for k, v in value.items()}
    if isinstance(value, list):
        return [_render_value(v, env, ctx) for v in value]
    return value


def render_task_template(
    task_template: dict[str, Any],
    *,
    instance: dict[str, Any],
    metadata_name: str,
    metadata_display_name: str,
) -> dict[str, Any]:
    """Render ``task_template`` placeholders using instance + metadata context."""
    env = Environment(undefined=StrictUndefined, autoescape=False)
    ctx = {
        "instance": instance,
        "metadata": {
            "name": metadata_name,
            "display_name": metadata_display_name,
        },
    }
    return cast(dict[str, Any], _render_value(task_template, env, ctx))
