from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml
from scripts import component_ownership

REPO_ROOT = Path(__file__).resolve().parents[2]


def _candidate_sha() -> str:
    return "0123456789abcdef0123456789abcdef01234567"


def _image_digests() -> dict[str, str]:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    return {
        image_name: f"ghcr.io/qianyi-sun/{image_name}@sha256:" + f"{index:x}" * 64
        for index, image_name in enumerate(
            (
                component.release_digest
                for component in manifest.components
                if component.kind == "release-image" and component.release_digest is not None
            ),
            start=1,
        )
    }


def _prod_staging_isolation_evidence() -> dict[str, Any]:
    return {
        "status": "pass",
        "url": "https://github.com/qianyi-sun/loom/issues/490#issuecomment-isolation-gate",
        "state_profile_evidence": "release-evidence/prod-staging-state-profile.json",
        "worker_identity_evidence": "release-evidence/prod-staging-worker-identity.json",
        "frontend_api_base_evidence": "release-evidence/prod-staging-api-base.json",
        "state_profiles": {
            "production": {
                "environment": "production",
                "github_environment": "production",
                "namespace": "loom-prod",
                "database_name": "loom_prod",
                "object_storage": {
                    "task_bucket": "loom-prod-tasks",
                    "trajectories_bucket": "loom-prod-trajectories",
                    "artifacts_bucket": "loom-prod-artifacts",
                },
                "secret_refs": {
                    "secret_store_key_ref": (
                        "github-environment:production/LOOM_SECRET_STORE_MASTER_KEY"
                    ),
                    "service_api_token_ref": (
                        "github-environment:production/LOOM_SERVICE_API_TOKEN"
                    ),
                    "worker_token_ref": "github-environment:production/LOOM_WORKER_TOKEN",
                    "provider_secret_ref": (
                        "github-environment:production/LOOM_PROVIDER_SECRET_REF"
                    ),
                    "yibuapi_secret_ref": "github-environment:production/YIBUAPI_API_KEY",
                },
                "provider_connection_namespace": "production",
            },
            "staging": {
                "environment": "staging",
                "github_environment": "staging",
                "namespace": "loom-staging",
                "database_name": "loom_staging",
                "object_storage": {
                    "task_bucket": "loom-staging-tasks",
                    "trajectories_bucket": "loom-staging-trajectories",
                    "artifacts_bucket": "loom-staging-artifacts",
                },
                "secret_refs": {
                    "secret_store_key_ref": (
                        "github-environment:staging/LOOM_SECRET_STORE_MASTER_KEY"
                    ),
                    "service_api_token_ref": ("github-environment:staging/LOOM_SERVICE_API_TOKEN"),
                    "worker_token_ref": "github-environment:staging/LOOM_WORKER_TOKEN",
                    "provider_secret_ref": ("github-environment:staging/LOOM_PROVIDER_SECRET_REF"),
                    "yibuapi_secret_ref": "github-environment:staging/YIBUAPI_API_KEY",
                },
                "provider_connection_namespace": "staging",
            },
        },
        "frontend": {
            "production": {
                "route": "https://yylx.world/prod",
                "api_base": "https://yylx.world/prod/api",
                "environment_label": "Production",
            },
            "staging": {
                "route": "https://yylx.world/staging",
                "api_base": "https://yylx.world/staging/api",
                "environment_label": "Staging",
            },
        },
        "workers": {
            "production": {
                "environment": "production",
                "api_url": "https://yylx.world/prod/api",
                "image": "ghcr.io/qianyi-sun/loom-worker:release-0123456789ab",
                "image_digest": _image_digests()["loom-worker"],
                "source_commit": _candidate_sha(),
                "k8s_namespace": "loom-prod",
                "k8s_deployment": "loom-prod-worker",
            },
            "staging": {
                "environment": "staging",
                "api_url": "https://yylx.world/staging/api",
                "image": "ghcr.io/qianyi-sun/loom-worker:staging-abc1234",
                "image_digest": "ghcr.io/qianyi-sun/loom-worker@sha256:" + "6" * 64,
                "source_commit": "abcdef0123456789abcdef0123456789abcdef01",
                "k8s_namespace": "loom-staging",
                "k8s_deployment": "loom-staging-worker",
            },
        },
        "staging_capacity": {
            "lease_state": "none",
            "staging_slots": 0,
            "new_staging_claims_allowed": False,
            "override": {
                "approved": False,
            },
        },
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
            "artifact_url": "https://yylx.world/staging/api/v1/trials/trial-release-smoke/atif",
        },
        "frontend_route_evidence": {
            "status": "pass",
            "url": "https://github.com/qianyi-sun/loom/issues/486#issuecomment-route-gate",
            "production_route": "https://yylx.world/prod",
            "staging_route": "https://yylx.world/staging",
            "production_api_base": "https://yylx.world/prod/api",
            "staging_api_base": "https://yylx.world/staging/api",
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
            "non_full_reward_trial_count": 6,
            "scored_trial_count": 7,
        },
        "benchmark_score_alignment": {
            "status": "pass",
            "url": "https://github.com/qianyi-sun/loom/actions/runs/1010",
            "manifest": "docs/score-alignment/manifest.json",
            "benchmarks": ["aime-24", "aime-25", "humaneval", "mbpp"],
        },
        "hf_mirror_token_boundary": {
            "status": "pass",
            "url": "https://github.com/qianyi-sun/loom/issues/320#issuecomment-hf-boundary",
            "benchmark_id": "skilllearnbench",
            "environment": "staging",
            "runtime_source_scheme": "s3",
            "runtime_source_prefix": "s3://loom-benchmarks/skilllearnbench/",
            "runnable_tasks": 100,
            "internal_s3_sources": 100,
            "total_task_sources": 100,
            "hf_provenance_retained": True,
            "upstream_kind": "huggingface",
            "upstream_locator": "PRHW/SkillLearnBench",
            "upstream_revision": "abc123def456",
            "worker_hf_token_present": False,
            "direct_hf_egress_required": False,
            "secret_safe": True,
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
        "prod_staging_isolation": {
            **_prod_staging_isolation_evidence(),
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
        "staging_url": "https://yylx.world/staging",
        "image_digests": _image_digests(),
        "checks": checks,
    }
    if overrides:
        manifest.update(overrides)
    return manifest


def _run_release_gate(
    tmp_path: Path, manifest: dict[str, Any], *args: str
) -> subprocess.CompletedProcess[str]:
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


def _release_promotion_workflow() -> dict[str, Any]:
    return yaml.safe_load(
        (REPO_ROOT / ".github/workflows/release-promotion-gate.yml").read_text(encoding="utf-8")
    )


def _run_release_input_preflight(
    *,
    candidate_sha: str,
    image_selector: str,
) -> subprocess.CompletedProcess[str]:
    workflow = _release_promotion_workflow()
    preflight = workflow["jobs"]["preflight"]
    step = next(step for step in preflight["steps"] if step.get("name") == "Validate inputs")
    env = os.environ.copy()
    env.update(
        {
            "CANDIDATE_SHA": candidate_sha,
            "IMAGE_SELECTOR": image_selector,
        }
    )
    return subprocess.run(
        ["bash", "-c", step["run"]],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


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
    isolation = evidence["checks"]["prod_staging_isolation"]
    assert (
        isolation["state_profiles"]["production"]["secret_refs"]["worker_token_ref"]
        == "github-environment:production/LOOM_WORKER_TOKEN"
    )
    assert isolation["workers"]["production"]["source_commit"] == _candidate_sha()
    markdown = markdown_out.read_text(encoding="utf-8")
    assert "Release Gate Evidence" in markdown
    assert "v1.0.0" in markdown
    assert "benchmark_reward_gate" in markdown
    assert "score_positive_canary" in markdown
    assert "benchmark_score_alignment" in markdown
    assert "hf_mirror_token_boundary" in markdown
    assert "worker_capacity_smoke" in markdown
    assert "frontend_route_evidence" in markdown
    assert "prod_staging_isolation" in markdown
    assert "raw_delivery_export_status" in markdown
    assert all(image_name in markdown for image_name in _image_digests())


def test_release_gate_requires_every_manifest_release_digest(tmp_path: Path) -> None:
    manifest = _passing_evidence()
    manifest["image_digests"].pop("loom-agent-sandbox")

    result = _run_release_gate(
        tmp_path,
        manifest,
        "validate",
        "--candidate-sha",
        _candidate_sha(),
        "--image-tag",
        "release-0123456789ab",
    )

    assert result.returncode != 0
    assert "image_digests.loom-agent-sandbox must end with @sha256:<64 hex>" in result.stderr


def test_release_gate_rejects_digest_without_manifest_owner(tmp_path: Path) -> None:
    manifest = _passing_evidence()
    manifest["image_digests"]["loom-unowned-extra"] = (
        "ghcr.io/qianyi-sun/loom-unowned-extra@sha256:" + "a" * 64
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

    assert result.returncode != 0
    assert (
        "image_digests contains images without a manifest release owner: "
        "loom-unowned-extra" in result.stderr
    )


def test_release_gate_official_json_round_trips_through_production_verifier(
    tmp_path: Path,
) -> None:
    json_out = tmp_path / "release-gate-evidence.json"
    validate = _run_release_gate(
        tmp_path,
        _passing_evidence(),
        "validate",
        "--candidate-sha",
        _candidate_sha(),
        "--image-tag",
        "release-0123456789ab",
        "--output-json",
        str(json_out),
    )

    assert validate.returncode == 0, validate.stderr
    evidence = json.loads(json_out.read_text(encoding="utf-8"))
    assert evidence["schema_version"] == 1
    assert type(evidence["schema_version"]) is int

    verify = subprocess.run(
        [
            sys.executable,
            "scripts/ops/release_gate.py",
            "verify-production",
            "--manifest",
            str(json_out),
            "--candidate-sha",
            _candidate_sha(),
            "--image-tag",
            "release-0123456789ab",
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert verify.returncode == 0, verify.stderr


def test_release_gate_requires_hf_mirror_token_boundary_check(tmp_path: Path) -> None:
    manifest = _passing_evidence()
    manifest["checks"].pop("hf_mirror_token_boundary")

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
    assert "missing required check 'hf_mirror_token_boundary'" in result.stderr


def test_release_gate_rejects_hf_boundary_worker_token_or_direct_egress(
    tmp_path: Path,
) -> None:
    manifest = _passing_evidence()
    manifest["checks"]["hf_mirror_token_boundary"].update(
        {
            "runtime_source_scheme": "hf",
            "worker_hf_token_present": True,
            "direct_hf_egress_required": True,
        }
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
    assert "hf_mirror_token_boundary.runtime_source_scheme must be 's3'" in result.stderr
    assert "hf_mirror_token_boundary.worker_hf_token_present must be false" in result.stderr
    assert "hf_mirror_token_boundary.direct_hf_egress_required must be false" in result.stderr


@pytest.mark.parametrize(
    ("seed_name", "mutate", "expected_error"),
    [
        (
            "frontend_api_base",
            lambda check: check["frontend"]["production"].update(
                {"api_base": "https://yylx.world/dev/api"},
            ),
            "prod_staging_isolation.frontend.production.api_base",
        ),
        (
            "worker_api_url",
            lambda check: check["workers"]["staging"].update(
                {"api_url": "https://yylx.world/prod/api"},
            ),
            "prod_staging_isolation.workers.staging.api_url",
        ),
        (
            "worker_source",
            lambda check: check["workers"]["production"].update(
                {
                    "source_commit": "abcdef0123456789abcdef0123456789abcdef01",
                    "image": "ghcr.io/qianyi-sun/loom-worker:staging-abc1234",
                },
            ),
            "prod_staging_isolation.workers.production.source_commit",
        ),
        (
            "db_target",
            lambda check: check["state_profiles"]["production"].update(
                {"database_name": "loom_dev"},
            ),
            "prod_staging_isolation.state_profiles.production.database_name",
        ),
        (
            "object_storage_target",
            lambda check: check["state_profiles"]["production"]["object_storage"].update(
                {"task_bucket": "loom-dev-tasks"},
            ),
            "prod_staging_isolation.state_profiles.production.object_storage.task_bucket",
        ),
        (
            "token_refs",
            lambda check: check["state_profiles"]["production"]["secret_refs"].update(
                {
                    "service_api_token_ref": (
                        "github-environment:development/LOOM_SERVICE_API_TOKEN"
                    ),
                },
            ),
            "prod_staging_isolation.state_profiles.production.secret_refs.service_api_token_ref",
        ),
        (
            "active_staging_lease",
            lambda check: check["staging_capacity"].update(
                {
                    "lease_state": "active",
                    "staging_slots": 2,
                    "new_staging_claims_allowed": True,
                },
            ),
            "prod_staging_isolation.staging_capacity requires staging_slots=0",
        ),
    ],
)
def test_release_gate_rejects_seeded_prod_staging_crossings(
    tmp_path: Path,
    seed_name: str,
    mutate: Any,
    expected_error: str,
) -> None:
    manifest = _passing_evidence()
    check = manifest["checks"]["prod_staging_isolation"]
    mutate(check)

    result = _run_release_gate(
        tmp_path,
        manifest,
        "validate",
        "--candidate-sha",
        _candidate_sha(),
        "--image-tag",
        "release-0123456789ab",
    )

    assert result.returncode == 1, seed_name
    assert expected_error in result.stderr


def test_release_gate_allows_documented_staging_lease_override(tmp_path: Path) -> None:
    manifest = _passing_evidence()
    manifest["checks"]["prod_staging_isolation"]["staging_capacity"].update(
        {
            "lease_state": "active",
            "staging_slots": 1,
            "new_staging_claims_allowed": True,
            "override": {
                "approved": True,
                "reason": "Qianyi approved one running staging drain slot before prod promote",
                "url": "https://github.com/qianyi-sun/loom/issues/490#issuecomment-override",
            },
        },
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

    assert result.returncode == 0, result.stderr


def test_release_gate_allows_shared_object_bucket_with_documented_prefix_policy(
    tmp_path: Path,
) -> None:
    manifest = _passing_evidence()
    check = manifest["checks"]["prod_staging_isolation"]
    for profile in check["state_profiles"].values():
        profile["object_storage"] = {
            "task_bucket": "loom-shared-tasks",
            "trajectories_bucket": "loom-shared-trajectories",
            "artifacts_bucket": "loom-shared-artifacts",
        }
    check["state_profiles"]["production"]["object_storage"]["prefix_policy"] = {
        "approved": True,
        "production_prefix": "prod/",
        "development_prefix": "dev/",
        "url": "https://github.com/qianyi-sun/loom/issues/490#issuecomment-prefix-policy",
    }

    result = _run_release_gate(
        tmp_path,
        manifest,
        "validate",
        "--candidate-sha",
        _candidate_sha(),
        "--image-tag",
        "release-0123456789ab",
    )

    assert result.returncode == 0, result.stderr


def test_release_gate_rejects_raw_secret_values_without_echoing_them(tmp_path: Path) -> None:
    manifest = _passing_evidence()
    raw_token = "sk-thisrawsecretmustnotappear1234567890"
    manifest["checks"]["prod_staging_isolation"]["state_profiles"]["production"][
        "operator_note"
    ] = raw_token

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
    assert "forbidden evidence value" in result.stderr
    assert raw_token not in result.stderr
    assert raw_token not in result.stdout


def test_release_gate_rejects_missing_required_checks_and_secret_leaks(tmp_path: Path) -> None:
    manifest = _passing_evidence()
    manifest["checks"].pop("benchmark_reward_gate")
    manifest["checks"].pop("score_positive_canary")
    manifest["checks"].pop("benchmark_score_alignment")
    manifest["checks"].pop("frontend_route_evidence")
    manifest["checks"].pop("prod_staging_isolation")
    manifest["checks"].pop("raw_delivery_export_status")
    manifest["checks"]["public_api_spa_smoke"]["artifact_url"] = (
        "https://loom-minio.loom.svc.cluster.local/bucket/object?X-Amz-Signature=abc"
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
    assert "missing required check 'prod_staging_isolation'" in result.stderr
    assert "missing required check 'raw_delivery_export_status'" in result.stderr
    assert "forbidden evidence value" in result.stderr
    assert "public_api_spa_smoke.artifact_url" in result.stderr
    assert "provider_smoke.notes" in result.stderr


def test_release_gate_rejects_all_full_reward_canary(tmp_path: Path) -> None:
    manifest = _passing_evidence()
    manifest["checks"]["score_positive_canary"]["positive_reward_trial_count"] = 7
    manifest["checks"]["score_positive_canary"]["non_full_reward_trial_count"] = 0
    manifest["checks"]["score_positive_canary"]["scored_trial_count"] = 7

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
    assert "score_positive_canary.non_full_reward_trial_count" in result.stderr
    assert "must be an integer > 0" in result.stderr


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


@pytest.mark.parametrize(
    "invalid_schema_version",
    [None, True, 1.0, "1"],
    ids=["missing", "boolean", "float", "string"],
)
def test_release_gate_verify_production_rejects_invalid_schema_version_type(
    tmp_path: Path,
    invalid_schema_version: Any,
) -> None:
    manifest = _passing_evidence()
    if invalid_schema_version is None:
        manifest.pop("schema_version")
    else:
        manifest["schema_version"] = invalid_schema_version

    result = _run_release_gate(
        tmp_path,
        manifest,
        "verify-production",
        "--candidate-sha",
        _candidate_sha(),
        "--image-tag",
        "release-0123456789ab",
    )

    assert result.returncode == 1
    assert "schema_version must be 1" in result.stderr


@pytest.mark.parametrize(
    "image_selector",
    ["release-0123456789ab", "sha256:" + "a" * 64],
    ids=["tag", "digest"],
)
def test_release_promotion_preflight_accepts_safe_inputs(image_selector: str) -> None:
    result = _run_release_input_preflight(
        candidate_sha=_candidate_sha(),
        image_selector=image_selector,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "candidate_sha",
    [
        "A" * 40,
        "a" * 39,
        "a" * 40 + "\ncommand",
        "$(touch should-not-exist)",
    ],
    ids=["uppercase", "short", "newline", "shell"],
)
def test_release_promotion_preflight_rejects_unsafe_candidate(candidate_sha: str) -> None:
    result = _run_release_input_preflight(
        candidate_sha=candidate_sha,
        image_selector="release-0123456789ab",
    )

    assert result.returncode != 0
    assert "candidate_sha" in result.stderr


@pytest.mark.parametrize(
    "image_selector",
    [
        "$(touch should-not-exist)",
        "release-ok\ncommand",
        "../release",
        "-release",
        "release;command",
        "registry.example/loom:release",
    ],
    ids=["shell", "newline", "traversal", "leading-dash", "separator", "slash"],
)
def test_release_promotion_preflight_rejects_unsafe_image_selector(
    image_selector: str,
) -> None:
    result = _run_release_input_preflight(
        candidate_sha=_candidate_sha(),
        image_selector=image_selector,
    )

    assert result.returncode != 0
    assert "image selector" in result.stderr


def test_release_promotion_preflight_is_unprivileged_and_gates_checkout() -> None:
    workflow = _release_promotion_workflow()
    preflight = workflow["jobs"]["preflight"]
    release_gate = workflow["jobs"]["release-gate"]

    assert preflight.get("permissions") == {}
    assert "environment" not in preflight
    assert all("actions/checkout" not in str(step) for step in preflight["steps"])
    assert release_gate["needs"] == "preflight"

    checkout_index = next(
        index
        for index, step in enumerate(release_gate["steps"])
        if "actions/checkout" in str(step.get("uses", ""))
    )
    identity_index = next(
        index
        for index, step in enumerate(release_gate["steps"])
        if step.get("name") == "Assert candidate checkout identity"
    )
    assert checkout_index < identity_index
    assert "git rev-parse HEAD" in release_gate["steps"][identity_index]["run"]
    assert "CANDIDATE_SHA" in release_gate["steps"][identity_index]["run"]


def test_release_promotion_shell_never_interpolates_dispatch_inputs() -> None:
    workflow = _release_promotion_workflow()

    for job in workflow["jobs"].values():
        for step in job.get("steps", []):
            if "run" in step:
                assert "${{ inputs." not in step["run"]

    release_gate = workflow["jobs"]["release-gate"]
    assert release_gate["env"]["CANDIDATE_SHA"] == "${{ inputs.candidate_sha }}"
    assert release_gate["env"]["IMAGE_SELECTOR"] == "${{ inputs.image_tag }}"
    assert "python3 scripts/ops/release_gate.py validate" in str(release_gate)
    assert "python3 scripts/ops/release_gate.py verify-production" in str(release_gate)
    assert "uv run python scripts/ops/release_gate.py" not in str(release_gate)


def test_release_promotion_workflow_uploads_candidate_evidence() -> None:
    workflow = _release_promotion_workflow()

    dispatch_inputs = _workflow_on(workflow)["workflow_dispatch"]["inputs"]
    assert dispatch_inputs["candidate_sha"]["required"] is True
    assert dispatch_inputs["image_tag"]["required"] is True
    assert dispatch_inputs["evidence_manifest_b64"]["required"] is True
    assert workflow["permissions"]["contents"] == "read"

    job = workflow["jobs"]["release-gate"]
    assert job["environment"]["name"] == "staging"
    assert "inputs.candidate_sha" in str(job)
    assert "scripts/ops/release_gate.py validate" in str(job)
    assert "scripts/ops/release_gate.py verify-production" in str(job)
    validate_step = next(
        index
        for index, step in enumerate(job["steps"])
        if "scripts/ops/release_gate.py validate" in str(step)
    )
    verify_step = next(
        index
        for index, step in enumerate(job["steps"])
        if "scripts/ops/release_gate.py verify-production" in str(step)
    )
    upload_step = next(
        index for index, step in enumerate(job["steps"]) if "actions/upload-artifact" in str(step)
    )
    assert validate_step < verify_step < upload_step
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
    assert "refs/heads/main" in prod_job["if"]
    assert "refs/tags/" not in prod_job["if"]

    steps = prod_job["steps"]
    verify_index = next(
        index
        for index, step in enumerate(steps)
        if step.get("name") == "Verify release gate evidence"
    )
    setup_uv_index = next(
        index for index, step in enumerate(steps) if "astral-sh/setup-uv" in str(step)
    )
    assert verify_index < setup_uv_index


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


def test_first_prod_runbook_frontend_evidence_matches_gate_schema() -> None:
    """The documented release-evidence example must stay aligned with the gate
    schema: following the runbook must produce evidence this gate accepts, not
    rejects. Guards against the /dev-vs-/staging drift (qianyi review on #880)."""
    from scripts.ops import release_gate

    runbook = (REPO_ROOT / "docs/runbooks/first-prod-release-runbook.md").read_text(
        encoding="utf-8",
    )
    canonical = release_gate.CANONICAL_FRONTEND_ROUTES

    # The renamed 3-env fields + /staging values must appear; the retired 2-env
    # field names must not (they would produce gate-rejected evidence).
    assert '"staging_route": "https://yylx.world/staging"' in runbook
    assert '"staging_api_base": "https://yylx.world/staging/api"' in runbook
    assert '"development_route"' not in runbook
    assert '"development_api_base"' not in runbook
    for key in ("production_route", "staging_route", "production_api_base", "staging_api_base"):
        assert f'"{key}": "{canonical[key]}"' in runbook, f"runbook missing canonical {key}"
