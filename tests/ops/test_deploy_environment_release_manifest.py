from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_deploy_script_writes_release_manifest_before_cluster_up() -> None:
    script = (REPO_ROOT / "scripts/ops/deploy_environment.sh").read_text(
        encoding="utf-8",
    )

    assert "LOOM_ROLLOUT_EVIDENCE_DIR" in script
    assert "release-manifest" in script
    assert script.index("release-manifest") < script.index("loom cluster up")
    assert "release-manifest-${LOOM_IMAGE_TAG}.json" in script


def test_deploy_workflow_uploads_rollout_evidence_artifact() -> None:
    workflow = yaml.safe_load(
        (REPO_ROOT / ".github/workflows/deploy-environment.yml").read_text(
            encoding="utf-8",
        ),
    )

    for job_name in ("deploy-development", "deploy-staging", "deploy-production"):
        job = workflow["jobs"][job_name]
        assert job["env"]["LOOM_ROLLOUT_EVIDENCE_DIR"] == "rollout-evidence"
        assert "rollout-evidence" in str(job)
        assert "actions/upload-artifact" in str(job)
