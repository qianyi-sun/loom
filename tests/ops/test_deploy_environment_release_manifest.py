from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def _script_array_block(script: str, name: str) -> str:
    start = script.index(f"{name}=(")
    end = script.index("\n)", start)
    return script[start:end]


def test_deploy_script_writes_release_manifest_before_cluster_up() -> None:
    script = (REPO_ROOT / "scripts/ops/deploy_environment.sh").read_text(
        encoding="utf-8",
    )

    manifest_block = _script_array_block(script, "manifest_args")
    cluster_up_block = _script_array_block(script, "cluster_up_args")

    assert "LOOM_ROLLOUT_EVIDENCE_DIR" in script
    assert "release-manifest" in script
    assert script.index("manifest_args=(") < script.index("cluster_up_args=(")
    assert "\n  cluster\n  release-manifest" in manifest_block
    assert "\n  cluster\n  up" in cluster_up_block
    assert "release-manifest-${LOOM_IMAGE_TAG}.json" in script
    assert "LOOM_EXPECTED_IMAGE_IDENTITIES_JSON" in script
    assert "--rollout-id \"${LOOM_IMAGE_TAG}\"" in cluster_up_block
    assert (
        "--rollout-lock-evidence "
        "\"${evidence_dir}/rollout-mutation-lock-${LOOM_IMAGE_TAG}.json\""
        in cluster_up_block
    )
    assert (
        'cluster_up_args+=(--rollout-lock-dir "${LOOM_ROLLOUT_LOCK_DIR}")'
        in script
    )
    assert 'cluster_up_args+=(--force-rollout-lock)' in script


def test_deploy_script_runs_release_gate_after_cluster_up() -> None:
    script = (REPO_ROOT / "scripts/ops/deploy_environment.sh").read_text(
        encoding="utf-8",
    )

    assert "release-gate" in script
    assert script.index('uv run loom "${cluster_up_args[@]}"') < script.index(
        "release_gate_common_args=(",
    )
    assert "--rendered-manifest \"${evidence_dir}/rendered.yaml\"" in script
    assert (
        "--manifest \"${evidence_dir}/release-manifest-${LOOM_IMAGE_TAG}.json\""
        in script
    )
    assert "--format markdown" in script
    assert "release-gate-${LOOM_IMAGE_TAG}.md" in script
    assert "gb10-workers-status-${LOOM_IMAGE_TAG}.json" in script
    assert "--gb10-workers-status" in script
    assert "minio-storage-preflight-${LOOM_IMAGE_TAG}.json" in script
    assert "minio-storage-preflight" in script
    assert "--minio-storage-preflight" in script


def test_deploy_workflow_uploads_rollout_evidence_artifact() -> None:
    workflow = yaml.safe_load(
        (REPO_ROOT / ".github/workflows/deploy-environment.yml").read_text(
            encoding="utf-8",
        ),
    )

    # #857: `deploy-development` dropped (`local` env is manual-only);
    # `deploy-dev` added (Slurm-backed shared iteration env).
    for job_name in ("deploy-dev", "deploy-staging", "deploy-production"):
        job = workflow["jobs"][job_name]
        assert job["env"]["LOOM_ROLLOUT_EVIDENCE_DIR"] == "rollout-evidence"
        assert "rollout-evidence" in str(job)
        assert "actions/upload-artifact" in str(job)
