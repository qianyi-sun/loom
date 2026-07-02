"""Unit tests for ``UserTaskSetManifest`` (#242 sub-plan 2)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from loom.models.taskset import UserTaskSetManifest

_MINIMAL = {
    "apiVersion": "loom.taskset/v1",
    "kind": "UserTaskSet",
    "metadata": {"name": "my-tasks", "display_name": "My Tasks"},
    "source": {"type": "hf", "locator": "org/dataset"},
    "instance_mapping": {"prompt": "row.question"},
    "task_template": {
        "task": {"id": "{{ instance.task_id }}", "name": "t"},
        "environment": {"os": "linux"},
        "agent": {"name": "default"},
        "steps": [{"artifacts": ["out.txt"]}],
    },
}


@pytest.mark.parametrize("source_type", ["hf", "git", "https", "jsonl-inline"])
def test_accepts_each_source_type(source_type: str) -> None:
    data = {
        **_MINIMAL,
        "source": {"type": source_type, "locator": "loc"},
    }
    manifest = UserTaskSetManifest.model_validate(data)
    assert manifest.source.type == source_type


def test_trajectory_only_without_verifier() -> None:
    manifest = UserTaskSetManifest.model_validate(_MINIMAL)
    assert manifest.verifier is None
    assert manifest.intents is None


def test_both_intents_with_verifier() -> None:
    manifest = UserTaskSetManifest.model_validate({
        **_MINIMAL,
        "intents": ["trajectory_generation", "evaluation"],
        "verifier": {"type": "pytest", "file": "verifier/test.py"},
    })
    assert manifest.intents == ["trajectory_generation", "evaluation"]


def test_rejects_extra_top_level_field() -> None:
    with pytest.raises(ValidationError):
        UserTaskSetManifest.model_validate({**_MINIMAL, "extra": True})


def test_rejects_missing_required_fields() -> None:
    with pytest.raises(ValidationError):
        UserTaskSetManifest.model_validate({"apiVersion": "loom.taskset/v1"})


def test_evaluation_without_verifier_rejected() -> None:
    with pytest.raises(ValidationError, match="verifier_required_for_evaluation"):
        UserTaskSetManifest.model_validate({
            **_MINIMAL,
            "intents": ["evaluation"],
        })


@pytest.mark.parametrize("bad_path", [
    "../escape.py",
    "/abs/verifier.py",
    "verifier/../../etc/passwd",
])
def test_rejects_unsafe_verifier_file_path(bad_path: str) -> None:
    with pytest.raises(ValidationError):
        UserTaskSetManifest.model_validate({
            **_MINIMAL,
            "verifier": {"type": "pytest", "file": bad_path},
        })


@pytest.mark.parametrize("bad_path", [
    "../transform.py",
    "/transform.py",
])
def test_rejects_unsafe_transform_file_path(bad_path: str) -> None:
    with pytest.raises(ValidationError):
        UserTaskSetManifest.model_validate({
            **_MINIMAL,
            "transform": {"file": bad_path},
        })


@pytest.mark.parametrize("bad_slug", ["../escape", "has/slash", "has.dot", ""])
def test_rejects_path_traversal_slug(bad_slug: str) -> None:
    with pytest.raises(ValidationError):
        UserTaskSetManifest.model_validate({
            **_MINIMAL,
            "metadata": {"name": bad_slug, "display_name": "X"},
        })
