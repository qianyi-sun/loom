"""Unit tests for TaskSet intent normalization (#242 sub-plan 2)."""

from __future__ import annotations

from loom.models.taskset import UserTaskSetManifest
from loom.taskset.intents import normalize_intents

_BASE = {
    "apiVersion": "loom.taskset/v1",
    "kind": "UserTaskSet",
    "metadata": {"name": "my-tasks", "display_name": "My Tasks"},
    "source": {"type": "https", "locator": "https://example/data.jsonl"},
    "instance_mapping": {"prompt": "row.q"},
    "task_template": {
        "task": {"id": "1", "name": "t"},
        "environment": {"os": "linux"},
        "agent": {"name": "default"},
        "steps": [{}],
    },
}


def test_defaults_to_trajectory_generation() -> None:
    manifest = UserTaskSetManifest.model_validate(_BASE)
    normalized = normalize_intents(manifest, verifier_file_present=False)
    assert normalized.manifest_intents == ["trajectory_generation"]
    assert normalized.effective_intents == ["trajectory_generation"]
    assert normalized.capabilities == ["trajectory-only"]


def test_verifier_infers_evaluation_when_file_present() -> None:
    manifest = UserTaskSetManifest.model_validate({
        **_BASE,
        "verifier": {"type": "pytest", "file": "verifier/test.py"},
    })
    normalized = normalize_intents(manifest, verifier_file_present=True)
    assert normalized.manifest_intents == ["trajectory_generation"]
    assert normalized.effective_intents == ["evaluation", "trajectory_generation"]
    assert normalized.inferred_intents == ["evaluation"]
    assert normalized.capabilities == ["both"]
    assert normalized.warnings[0].code == "evaluation_inferred_from_verifier"


def test_explicit_evaluation_not_inferred() -> None:
    manifest = UserTaskSetManifest.model_validate({
        **_BASE,
        "intents": ["trajectory_generation", "evaluation"],
        "verifier": {"type": "script", "file": "v.sh"},
    })
    normalized = normalize_intents(manifest, verifier_file_present=True)
    assert normalized.inferred_intents == []
    assert normalized.warnings == ()
    assert normalized.capabilities == ["both"]
