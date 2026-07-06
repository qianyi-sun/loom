from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def _candidate_sha() -> str:
    return "0123456789abcdef0123456789abcdef01234567"


def _image_digests() -> dict[str, str]:
    return {
        "loom-control-plane": "ghcr.io/qianyi-sun/loom-control-plane@sha256:"
        + "1" * 64,
        "loom-llm-gateway": "ghcr.io/qianyi-sun/loom-llm-gateway@sha256:" + "2" * 64,
        "loom-service": "ghcr.io/qianyi-sun/loom-service@sha256:" + "3" * 64,
        "loom-worker": "ghcr.io/qianyi-sun/loom-worker@sha256:" + "4" * 64,
        "loom-web": "ghcr.io/qianyi-sun/loom-web@sha256:" + "5" * 64,
    }


def _passing_evidence(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    checks: dict[str, dict[str, Any]] = {
        "repository_ci": {
            "status": "pass",
            "url": "https://github.com/qianyi-sun/loom/actions/runs/1001",
        },
        "image_build": {
            "status": "pass",
            "url": "https://github.com/qianyi-sun/loom/actions/runs/1002",
        },
        "cluster_render_audit": {
            "status": "pass",
            "url": "https://github.com/qianyi-sun/loom/actions/runs/1003",
            "staging_config": "deploy/environments/staging.cluster.toml",
            "production_config": "deploy/environments/production.cluster.toml",
        },
        "migration_dry_run": {
            "status": "pass",
            "url": "https://github.com/qianyi-sun/loom/actions/runs/1004",
            "db_recovery_point": "postgres-backup-20260624T140000Z",
        },
        "public_api_spa_smoke": {
            "status": "pass",
            "url": "https://github.com/qianyi-sun/loom/actions/runs/1005",
            "batch_id": "batch-release-smoke",
            "trial_id": "trial-release-smoke",
            "artifact_url": "https://staging.yylx.world/api/v1/trials/trial-release-smoke/atif",
        },
        "frontend_route_evidence": {
            "status": "pass",
            "url": "https://github.com/qianyi-sun/loom/issues/486#issuecomment-route-gate",
            "production_route": "https://yylx.world/prod",
            "development_route": "https://yylx.world/dev",
            "production_api_base": "https://yylx.world/prod/api",
            "development_api_base": "https://yylx.world/dev/api",
        },
        "secret_redaction": {
            "status": "pass",
            "url": "https://github.com/qianyi-sun/loom/actions/runs/1006",
        },
        "provider_smoke": {
            "status": "pass",
            "url": "https://github.com/qianyi-sun/loom/actions/runs/1007",
            "provider_path": "lux-openai-compatible",
        },
        "benchmark_reward_gate": {
            "status": "pass",
            "url": "https://github.com/qianyi-sun/loom/actions/runs/1008",
            "batch_id": "batch-reward-gate",
            "benchmarks": ["mbpp", "humaneval"],
        },
        "score_positive_canary": {
            "status": "pass",
            "url": "https://github.com/qianyi-sun/loom/issues/445#issuecomment-score-positive",
            "batch_id": "batch-score-positive-canary",
            "positive_reward_trial_count": 1,
            "scored_trial_count": 7,
        },
        "benchmark_score_alignment": {
            "status": "pass",
            "url": "https://github.com/qianyi-sun/loom/actions/runs/1010",
            "manifest": "docs/benchmark-score-alignment.json",
            "benchmarks": ["aime-24", "aime-25", "humaneval", "mbpp"],
        },
        "worker_capacity_smoke": {
            "status": "pass",
            "url": "https://github.com/qianyi-sun/loom/actions/runs/1009",
            "batch_id": "batch-worker-capacity",
            "k8s_workers": 3,
            "oldlab_workers": 3,
            "runtime_seconds": 120,
            "failures": 0,
            "oldlab_worker_records": [
                {
                    "node_name": "TRT-EAI-OLDLAB-1",
                    "slurm_job_id": "13441",
                    "worker_id": "worker-oldlab-1",
                    "concurrency": 6,
                    "trials_claimed": 4,
                },
                {
                    "node_name": "trt-EAI-OLDLAB-2",
                    "slurm_job_id": "13442",
                    "worker_id": "worker-oldlab-2",
                    "concurrency": 6,
                    "trials_claimed": 4,
                },
                {
                    "node_name": "trt-eai-oldlab-3",
                    "slurm_job_id": "13443",
                    "worker_id": "worker-oldlab-3",
                    "concurrency": 6,
                    "trials_claimed": 4,
                },
            ],
        },
        "prod_beta_isolation": {
            "status": "pass",
            "url": "https://github.com/qianyi-sun/loom/issues/490#issuecomment-isolation-gate",
            "state_profile_evidence": "release-evidence/prod-beta-state-profile.json",
            "worker_identity_evidence": "release-evidence/prod-beta-worker-identity.json",
            "frontend_api_base_evidence": "release-evidence/prod-beta-api-base.json",
        },
        "raw_delivery_export_status": {
            "status": "pass",
            "url": "https://github.com/qianyi-sun/loom/issues/493#issuecomment-user-e2e",
            "requirement_status": "required export bundle verified for first-prod representative workflow",
        },
        "rollback_plan": {
            "status": "pass",
            "previous_production_image_digest": "ghcr.io/qianyi-sun/loom-service@sha256:"
            + "a" * 64,
            "rendered_manifest": "s3://loom-release-evidence/prod-rendered-prev.yaml",
            "db_recovery_point": "postgres-backup-20260624T140000Z",
        },
        "release_owner_approval": {
            "status": "pass",
            "owner": "qianyi-sun",
            "url": "https://github.com/qianyi-sun/loom/issues/431#release-approval",
        },
    }
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "candidate_sha": _candidate_sha(),
        "image_tag": "release-0123456789ab",
        "prod_tag": "v1.0.0",
        "staging_url": "https://staging.yylx.world",
        "image_digests": _image_digests(),
        "checks": checks,
    }
    if overrides:
        manifest.update(overrides)
    return manifest


def _run_release_gate(tmp_path: Path, manifest: dict[str, Any], *args: str) -> subprocess.CompletedProcess[str]:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return subprocess.run(
        [
            sys.executable,
            "scripts/ops/release_gate.py",
            *args,
            "--manifest",
            str(manifest_path),
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def _workflow_on(workflow: dict[Any, Any]) -> dict[str, Any]:
    # PyYAML follows YAML 1.1 and treats the unquoted GitHub Actions key `on`
    # as boolean True. Keep tests readable without requiring a custom loader.
    return workflow.get("on", workflow.get(True))


def test_release_gate_accepts_complete_manifest_and_writes_artifacts(tmp_path: Path) -> None:
    json_out = tmp_path / "release-gate-evidence.json"
    markdown_out = tmp_path / "release-gate-evidence.md"
    result = _run_release_gate(
        tmp_path,
        _passing_evidence(),
        "validate",
        "--candidate-sha",
        _candidate_sha(),
        "--image-tag",
        "release-0123456789ab",
        "--output-json",
        str(json_out),
        "--output-markdown",
        str(markdown_out),
    )

    assert result.returncode == 0, result.stderr
    evidence = json.loads(json_out.read_text(encoding="utf-8"))
    assert evidence["status"] == "pass"
    assert evidence["candidate_sha"] == _candidate_sha()
    markdown = markdown_out.read_text(encoding="utf-8")
    assert "Release Gate Evidence" in markdown
    assert "v1.0.0" in markdown
    assert "benchmark_reward_gate" in markdown
    assert "score_positive_canary" in markdown
    assert "benchmark_score_alignment" in markdown
    assert "worker_capacity_smoke" in markdown
    assert "frontend_route_evidence" in markdown
    assert "prod_beta_isolation" in markdown
    assert "raw_delivery_export_status" in markdown


def test_release_gate_rejects_missing_required_checks_and_secret_leaks(tmp_path: Path) -> None:
    manifest = _passing_evidence()
    manifest["checks"].pop("benchmark_reward_gate")
    manifest["checks"].pop("score_positive_canary")
    manifest["checks"].pop("benchmark_score_alignment")
    manifest["checks"].pop("frontend_route_evidence")
    manifest["checks"].pop("prod_beta_isolation")
    manifest["checks"].pop("raw_delivery_export_status")
    manifest["checks"]["public_api_spa_smoke"]["artifact_url"] = (
        "https://loom-minio.loom.svc.cluster.local/bucket/object"
        "?X-Amz-Signature=abc"
    )
    manifest["checks"]["provider_smoke"]["notes"] = "Authorization: Bearer raw-token"

    result = _run_release_gate(
        tmp_path,
        manifest,
        "validate",
        "--candidate-sha",
        _candidate_sha(),
        "--image-tag",
        "release-0123456789ab",
    )

    assert result.returncode == 1
    assert "missing required check 'benchmark_reward_gate'" in result.stderr
    assert "missing required check 'score_positive_canary'" in result.stderr
    assert "missing required check 'benchmark_score_alignment'" in result.stderr
    assert "missing required check 'frontend_route_evidence'" in result.stderr
    assert "missing required check 'prod_beta_isolation'" in result.stderr
    assert "missing required check 'raw_delivery_export_status'" in result.stderr
    assert "forbidden evidence value" in result.stderr
    assert "public_api_spa_smoke.artifact_url" in result.stderr
    assert "provider_smoke.notes" in result.stderr


def test_release_gate_rejects_frontend_route_api_mismatches(tmp_path: Path) -> None:
    manifest = _passing_evidence()
    manifest["checks"]["frontend_route_evidence"]["production_api_base"] = (
        "https://yylx.world/dev/api"
    )

    result = _run_release_gate(
        tmp_path,
        manifest,
        "validate",
        "--candidate-sha",
        _candidate_sha(),
        "--image-tag",
        "release-0123456789ab",
    )

    assert result.returncode == 1
    assert "frontend_route_evidence.production_api_base" in result.stderr
    assert "must be https://yylx.world/prod/api" in result.stderr


def test_release_gate_requires_immutable_semver_prod_tag(
    tmp_path: Path,
) -> None:
    manifest = _passing_evidence({"prod_tag": "release-0123456789ab"})

    result = _run_release_gate(
        tmp_path,
        manifest,
        "validate",
        "--candidate-sha",
        _candidate_sha(),
        "--image-tag",
        "release-0123456789ab",
    )

    assert result.returncode == 1
    assert "prod_tag must be an immutable SemVer tag like v1.0.0" in result.stderr


def test_release_gate_requires_oldlab_worker_records_when_enabled(
    tmp_path: Path,
) -> None:
    manifest = _passing_evidence()
    manifest["checks"]["worker_capacity_smoke"].pop("oldlab_worker_records")

    result = _run_release_gate(
        tmp_path,
        manifest,
        "validate",
        "--candidate-sha",
        _candidate_sha(),
        "--image-tag",
        "release-0123456789ab",
    )

    assert result.returncode == 1
    assert "worker_capacity_smoke.oldlab_worker_records" in result.stderr


def test_release_gate_rejects_incomplete_oldlab_worker_record(
    tmp_path: Path,
) -> None:
    manifest = _passing_evidence()
    manifest["checks"]["worker_capacity_smoke"]["oldlab_workers"] = 1
    manifest["checks"]["worker_capacity_smoke"]["oldlab_worker_records"] = [
        {
            "node_name": "trt-eai-oldlab-4",
            "slurm_job_id": "14004",
            "concurrency": 6,
            "trials_claimed": 2,
        },
    ]

    result = _run_release_gate(
        tmp_path,
        manifest,
        "validate",
        "--candidate-sha",
        _candidate_sha(),
        "--image-tag",
        "release-0123456789ab",
    )

    assert result.returncode == 1
    assert "oldlab_worker_records[0].worker_id" in result.stderr


def test_release_gate_verify_production_rejects_candidate_or_image_mismatch(
    tmp_path: Path,
) -> None:
    result = _run_release_gate(
        tmp_path,
        _passing_evidence(),
        "verify-production",
        "--candidate-sha",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "--image-tag",
        "release-other",
    )

    assert result.returncode == 1
    assert "candidate_sha mismatch" in result.stderr
    assert "image_tag mismatch" in result.stderr


def test_release_promotion_workflow_uploads_candidate_evidence() -> None:
    workflow_path = REPO_ROOT / ".github/workflows/release-promotion-gate.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))

    dispatch_inputs = _workflow_on(workflow)["workflow_dispatch"]["inputs"]
    assert dispatch_inputs["candidate_sha"]["required"] is True
    assert dispatch_inputs["image_tag"]["required"] is True
    assert dispatch_inputs["evidence_manifest_b64"]["required"] is True
    assert workflow["permissions"]["contents"] == "read"

    job = workflow["jobs"]["release-gate"]
    assert job["environment"]["name"] == "staging"
    assert "inputs.candidate_sha" in str(job)
    assert "scripts/ops/release_gate.py validate" in str(job)
    assert "scripts/validate_environment_isolation.py" in str(job)
    assert "deploy/environments/staging.cluster.toml" in str(job)
    assert "deploy/environments/production.cluster.toml" in str(job)
    assert "actions/upload-artifact" in str(job)
    assert "release-gate-evidence" in str(job)


def test_production_deploy_requires_successful_release_gate() -> None:
    workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/deploy-environment.yml").read_text())
    dispatch_inputs = _workflow_on(workflow)["workflow_dispatch"]["inputs"]
    assert dispatch_inputs["candidate_sha"]["required"] is False
    assert dispatch_inputs["release_gate_run_id"]["required"] is False
    assert workflow["permissions"]["actions"] == "read"

    prod_job = workflow["jobs"]["deploy-production"]
    assert prod_job["environment"]["name"] == "production"
    assert prod_job["env"]["LOOM_CANDIDATE_SHA"] == "${{ inputs.candidate_sha }}"
    assert prod_job["env"]["LOOM_RELEASE_GATE_RUN_ID"] == "${{ inputs.release_gate_run_id }}"
    step_names = [step.get("name", "") for step in prod_job["steps"]]
    assert step_names.index("Verify release gate evidence") < step_names.index("Deploy production")
    assert "scripts/ops/verify_production_release_gate.sh" in str(prod_job)


def test_release_pr_template_requires_promotion_evidence() -> None:
    template = (REPO_ROOT / ".github/PULL_REQUEST_TEMPLATE.md").read_text(encoding="utf-8")
    assert "## Release Promotion" in template
    for required_text in (
        "Candidate SHA",
        "Staging URL",
        "Image digests",
        "Release gate workflow run",
        "Gate evidence artifact",
        "Rollback notes",
        "Previous production image digest",
        "DB recovery point",
    ):
        assert required_text in template
