from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def _workflow() -> dict[str, Any]:
    return yaml.safe_load(
        (REPO_ROOT / ".github/workflows/auto-merge.yml").read_text(encoding="utf-8")
    )


def _workflow_on(workflow: dict[str, Any]) -> dict[str, Any]:
    return workflow.get("on", workflow.get(True))


def test_auto_merge_controller_uses_trusted_metadata_only_event() -> None:
    workflow = _workflow()
    on_config = _workflow_on(workflow)
    job = workflow["jobs"]["enable"]

    assert "pull_request" not in on_config
    assert set(on_config["pull_request_target"]["types"]) == {
        "opened",
        "reopened",
        "ready_for_review",
        "synchronize",
        "edited",
    }
    assert workflow["permissions"] == {
        "contents": "write",
        "pull-requests": "write",
    }
    assert all("actions/checkout" not in str(step) for step in job["steps"])
    assert "uses" not in str(job)


def test_auto_merge_controller_is_author_neutral_and_ci_dependent() -> None:
    workflow = _workflow()
    job = workflow["jobs"]["enable"]
    condition = " ".join(job["if"].split())
    script = job["steps"][0]["run"]

    assert "github.actor" not in condition
    assert "github.event.pull_request.user" not in condition
    assert "!github.event.pull_request.draft" in condition
    assert "base.ref == 'dev'" in condition
    assert "base.ref == 'main'" in condition
    assert "head.ref == 'dev'" in condition
    assert "--auto --squash" in script
    assert "gh pr merge" in script
    assert "gh pr view" in script
    assert "gh pr review" not in script
    assert "gh pr checks" not in script
