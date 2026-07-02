"""Unit tests for TaskSet template rendering (#242 sub-plan 3)."""

from __future__ import annotations

import pytest
from jinja2 import UndefinedError

from loom.taskset.template_render import render_task_template


def test_render_task_template_substitutes_instance_and_metadata() -> None:
    rendered = render_task_template(
        {
            "task": {
                "id": "{{ instance.task_id }}",
                "name": "{{ metadata.display_name }}",
            },
        },
        instance={"task_id": "row-1"},
        metadata_name="my-slug",
        metadata_display_name="My Tasks",
    )
    assert rendered["task"]["id"] == "row-1"
    assert rendered["task"]["name"] == "My Tasks"


def test_render_task_template_strict_undefined() -> None:
    with pytest.raises(UndefinedError):
        render_task_template(
            {"task": {"id": "{{ instance.missing }}"}},
            instance={},
            metadata_name="slug",
            metadata_display_name="Name",
        )
