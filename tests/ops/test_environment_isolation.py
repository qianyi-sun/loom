from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def _run_validator() -> dict:
    completed = subprocess.run(
        [
            sys.executable,
            "scripts/validate_environment_isolation.py",
            "--profiles-dir",
            "deploy/environments",
            "--workflow",
            ".github/workflows/deploy-environment.yml",
            "--json",
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


def test_environment_profiles_pin_approved_names_and_isolated_state() -> None:
    report = _run_validator()
    assert report["status"] == "pass"

    profiles = {profile["environment"]: profile for profile in report["profiles"]}
    assert set(profiles) == {"development", "staging", "production"}

    assert profiles["development"]["namespace"] == "loom-dev"
    assert profiles["development"]["ingress_host"] == "yylx.world"
    assert profiles["development"]["frontend_route"] == "https://yylx.world/dev"
    assert profiles["development"]["frontend_api_base"] == "https://yylx.world/dev/api"
    assert profiles["staging"]["namespace"] == "loom-staging"
    assert profiles["staging"]["ingress_host"] == "staging.yylx.world"
    assert profiles["production"]["namespace"] == "loom-prod"
    assert profiles["production"]["ingress_host"] == "yylx.world"
    assert profiles["production"]["frontend_route"] == "https://yylx.world/prod"
    assert profiles["production"]["frontend_api_base"] == "https://yylx.world/prod/api"

    assert len({profile["namespace"] for profile in profiles.values()}) == 3
    assert len({profile["database_name"] for profile in profiles.values()}) == 3
    assert len({profile["trajectories_bucket"] for profile in profiles.values()}) == 3
    assert len({profile["artifacts_bucket"] for profile in profiles.values()}) == 3
    assert len({profile["secret_store_key_ref"] for profile in profiles.values()}) == 3
    assert len({profile["service_api_token_ref"] for profile in profiles.values()}) == 3
    assert len({profile["worker_token_ref"] for profile in profiles.values()}) == 3
    assert len({profile["provider_secret_ref"] for profile in profiles.values()}) == 3
    assert len({profile["yibuapi_secret_ref"] for profile in profiles.values()}) == 3
    assert len({profile["provider_connection_namespace"] for profile in profiles.values()}) == 3


def test_environment_isolation_dry_run_artifact_contains_safe_target_identities(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "environment-isolation-dry-run.json"
    completed = subprocess.run(
        [
            sys.executable,
            "scripts/validate_environment_isolation.py",
            "--profiles-dir",
            "deploy/environments",
            "--workflow",
            ".github/workflows/deploy-environment.yml",
            "--dry-run-artifact",
            str(artifact),
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    assert payload["artifact_type"] == "environment-isolation-dry-run"

    profiles = {profile["environment"]: profile for profile in payload["profiles"]}
    assert profiles["production"]["database_name"] == "loom_prod"
    assert profiles["production"]["object_storage"]["task_bucket"] == "loom-prod-tasks"
    assert profiles["production"]["ingress_host"] == "yylx.world"

    refs = profiles["production"]["secret_refs"]
    assert refs["service_api_token_ref"] == "github-environment:production/LOOM_SERVICE_API_TOKEN"
    assert refs["provider_secret_ref"] == "github-environment:production/LOOM_PROVIDER_SECRET_REF"
    assert refs["yibuapi_secret_ref"] == "github-environment:production/YIBUAPI_API_KEY"
    rendered = json.dumps(payload)
    for forbidden in ("Bearer ", "sk-", "X-Amz-Signature", "loom_w_", "loom_api_"):
        assert forbidden not in rendered


def test_environment_isolation_rejects_shared_prod_secret_refs(tmp_path: Path) -> None:
    profiles_dir = tmp_path / "profiles"
    shutil.copytree(REPO_ROOT / "deploy/environments", profiles_dir)
    production = profiles_dir / "production.toml"
    text = production.read_text(encoding="utf-8")
    text = text.replace(
        'service_api_token_ref = "github-environment:production/LOOM_SERVICE_API_TOKEN"',
        'service_api_token_ref = "github-environment:staging/LOOM_SERVICE_API_TOKEN"',
    )
    text = text.replace(
        'provider_secret_ref = "github-environment:production/LOOM_PROVIDER_SECRET_REF"',
        'provider_secret_ref = "github-environment:staging/LOOM_PROVIDER_SECRET_REF"',
    )
    text = text.replace(
        'yibuapi_secret_ref = "github-environment:production/YIBUAPI_API_KEY"',
        'yibuapi_secret_ref = "github-environment:staging/YIBUAPI_API_KEY"',
    )
    production.write_text(text, encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/validate_environment_isolation.py",
            "--profiles-dir",
            str(profiles_dir),
            "--workflow",
            ".github/workflows/deploy-environment.yml",
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 1
    assert "service_api_token_ref must be distinct across environments" in completed.stderr
    assert "provider_secret_ref must be distinct across environments" in completed.stderr
    assert "yibuapi_secret_ref must be distinct across environments" in completed.stderr
    assert "production: service_api_token_ref must start with" in completed.stderr


def test_deploy_workflow_keeps_production_secrets_on_main_or_semver_tags() -> None:
    workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/deploy-environment.yml").read_text())
    jobs = workflow["jobs"]

    dev_job = jobs["deploy-development"]
    staging_job = jobs["deploy-staging"]
    prod_job = jobs["deploy-production"]

    assert dev_job["environment"]["name"] == "development"
    assert staging_job["environment"]["name"] == "staging"
    assert prod_job["environment"]["name"] == "production"

    assert "refs/heads/dev" in dev_job["if"]
    assert "refs/heads/dev" in staging_job["if"]
    assert "production" not in dev_job["if"]
    assert "production" not in staging_job["if"]

    assert "refs/heads/main" in prod_job["if"]
    assert "refs/tags/v" in prod_job["if"]
    assert "environment == 'production'" in prod_job["if"]

    for secret_name in (
        "LOOM_KUBECONFIG_B64",
        "LOOM_CLUSTER_CONFIG_B64",
        "LOOM_DEPLOY_TOKEN",
    ):
        assert dev_job["env"][secret_name] == f"${{{{ secrets.{secret_name} }}}}"
        assert staging_job["env"][secret_name] == f"${{{{ secrets.{secret_name} }}}}"
        assert prod_job["env"][secret_name] == f"${{{{ secrets.{secret_name} }}}}"


def test_repository_checks_run_environment_isolation_tests() -> None:
    workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/ci.yml").read_text())
    steps = workflow["jobs"]["tests-root"]["steps"]
    pytest_steps = [step for step in steps if "run" in step and "uv run pytest" in str(step["run"])]
    assert any("tests/ops" in str(step["run"]) for step in pytest_steps)


def test_dockerignore_excludes_operator_local_artifacts_from_image_context() -> None:
    patterns = {
        line.strip()
        for line in (REPO_ROOT / ".dockerignore").read_text().splitlines()
        if line.strip() and not line.startswith("#")
    }

    assert ".staging" in patterns
    assert ".staging-*" in patterns
    assert ".worktrees" in patterns
    assert "worktrees" in patterns
