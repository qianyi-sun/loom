"""Skipped publications get an explanation without opening the deployment path."""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def workflow():
    return yaml.load(
        (ROOT / ".github/workflows/nebius-rollout.yml").read_text(),
        Loader=yaml.BaseLoader,
    )


def selected_jobs(workflow, *, conclusion="success", enabled="true",
                  event="workflow_run", operation="", repository="qianyi-sun/loom",
                  head_repository="qianyi-sun/loom", ref="refs/heads/dev"):
    context = {
        "github": SimpleNamespace(
            repository=repository, event_name=event, ref=ref,
            event=SimpleNamespace(workflow_run=SimpleNamespace(
                conclusion=conclusion,
                head_repository=SimpleNamespace(full_name=head_repository),
            )),
        ),
        "vars": SimpleNamespace(NEBIUS_AUTO_ROLLOUT_ENABLED=enabled),
        "inputs": SimpleNamespace(operation=operation),
    }
    # Evaluate this workflow's boolean/attribute-only job conditions with event
    # fixtures, so a new explanation cannot accidentally open the rollout path.
    return {
        name for name, job in workflow["jobs"].items()
        if eval(job["if"].replace("&&", "and").replace("||", "or"),
                {"__builtins__": {}}, context)
    }


@pytest.mark.parametrize(("conclusion", "enabled", "expected"), [
    ("success", "true", {"rollout"}),
    ("success", "false", {"explain-skip"}),
    ("success", "", {"explain-skip"}),
    ("failure", "true", {"explain-skip"}),
    ("failure", "false", {"explain-skip"}),
    ("cancelled", "true", {"explain-skip"}),
    ("timed_out", "true", {"explain-skip"}),
    ("skipped", "true", {"explain-skip"}),
])
def test_completed_publication_routing(workflow, conclusion, enabled, expected):
    assert selected_jobs(workflow, conclusion=conclusion, enabled=enabled) == expected


@pytest.mark.parametrize("operation", ["inspect", "certificate"])
@pytest.mark.parametrize("enabled", ["true", "false"])
def test_manual_readback_and_certificate_remain_isolated(workflow, operation, enabled):
    assert selected_jobs(workflow, event="workflow_dispatch", operation=operation,
                         enabled=enabled) == {operation}


@pytest.mark.parametrize(("enabled", "expected"), [("true", {"rollout"}), ("false", set())])
def test_manual_rollout_keeps_existing_enablement_requirement(workflow, enabled, expected):
    assert selected_jobs(workflow, event="workflow_dispatch", operation="rollout",
                         enabled=enabled) == expected


@pytest.mark.parametrize("context", [
    {"head_repository": "untrusted/fork"},
    {"repository": "untrusted/fork", "head_repository": "untrusted/fork"},
    {"event": "workflow_dispatch", "operation": "rollout", "ref": "refs/heads/feature"},
])
def test_ineligible_sources_do_not_start_any_job(workflow, context):
    assert selected_jobs(workflow, conclusion="failure", **context) == set()


def test_explanation_has_no_protected_environment_or_deployment_credentials(workflow):
    job = workflow["jobs"]["explain-skip"]
    assert job["permissions"] == {"contents": "read"}
    assert "environment" not in job
    assert "env" not in job
    assert "secrets." not in json.dumps(job)
    checkout, report = job["steps"]
    assert checkout["uses"].startswith("actions/checkout@")
    assert checkout["with"] == {"ref": "dev", "persist-credentials": "false"}
    assert "uses" not in report
    assert report["env"] == {
        "AUTO_ROLLOUT_ENABLED": "${{ vars.NEBIUS_AUTO_ROLLOUT_ENABLED }}",
    }


@pytest.mark.parametrize(("conclusion", "enabled", "reason"), [
    ("failure", "true", "Upstream candidate publication failed"),
    ("cancelled", "true", "was cancelled"),
    ("success", "false", "NEBIUS_AUTO_ROLLOUT_ENABLED is not true"),
])
def test_selected_explanation_command_writes_actions_summary(
    workflow, tmp_path, conclusion, enabled, reason,
):
    assert selected_jobs(workflow, conclusion=conclusion, enabled=enabled) == {"explain-skip"}
    event_path = tmp_path / "event.json"
    event_path.write_text(json.dumps({
        "action": "completed",
        "workflow_run": {
            "id": 567, "conclusion": conclusion, "head_sha": "a" * 40,
            "head_repository": {"full_name": "qianyi-sun/loom"},
        },
    }))
    summary_path = tmp_path / "summary.md"
    report = workflow["jobs"]["explain-skip"]["steps"][1]
    result = subprocess.run(
        ["bash", "-e", "-c", report["run"]], cwd=ROOT,
        env={
            "PATH": os.environ["PATH"],
            "GITHUB_EVENT_PATH": str(event_path),
            "GITHUB_STEP_SUMMARY": str(summary_path),
            "GITHUB_REPOSITORY": "qianyi-sun/loom",
            "AUTO_ROLLOUT_ENABLED": enabled,
        },
        capture_output=True, text=True, check=True,
    )
    assert reason in summary_path.read_text()
    assert "https://github.com/qianyi-sun/loom/actions/runs/567" in summary_path.read_text()
    assert reason in result.stdout
