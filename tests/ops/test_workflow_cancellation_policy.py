from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]

EXPLICIT_NON_CANCELLABLE_WORKFLOWS = {
    ".github/workflows/ci-retry.yml": "classified-ci-retry-${{ inputs.source_run_id }}",
    ".github/workflows/deploy-environment.yml": "deploy-${{ inputs.environment }}",
    ".github/workflows/main-promotion-gate.yml": (
        "main-promotion-gate-${{ inputs.candidate_sha }}"
    ),
    ".github/workflows/publish-benchmarks.yml": "publish-benchmarks-hf-hub",
    ".github/workflows/release-promotion-gate.yml": (
        "release-promotion-gate-${{ inputs.candidate_sha }}"
    ),
    ".github/workflows/trusted-image-release-controller.yml": (
        "trusted-image-release-controller-dev"
    ),
}


def _workflow(path: str) -> dict[str, Any]:
    return yaml.safe_load((REPO_ROOT / path).read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    ("workflow_path", "expected_group"),
    EXPLICIT_NON_CANCELLABLE_WORKFLOWS.items(),
)
def test_mutating_workflows_are_serialized_without_cancellation(
    workflow_path: str,
    expected_group: str,
) -> None:
    concurrency = _workflow(workflow_path)["concurrency"]

    assert concurrency == {
        "group": expected_group,
        "cancel-in-progress": False,
    }


def test_reusable_images_have_no_independent_cancellation_or_publication() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    assert "concurrency" not in workflow
    assert set(workflow[True]) == {"workflow_call"}
    assert "publish" not in workflow["jobs"]


def test_cancellable_macos_workflow_has_no_write_authority() -> None:
    workflow = _workflow(".github/workflows/macos-locked-environment.yml")

    assert workflow["concurrency"]["cancel-in-progress"] is True
    assert workflow["permissions"] == {"contents": "read"}
