#!/usr/bin/env python3
"""Validate Loom's dev/staging/prod deployment isolation contract.

This is intentionally static: it checks committed profile files and the
GitHub Actions deployment workflow before a production release, without
requiring access to live clusters or secrets.
"""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml

EXPECTED_ENVIRONMENTS = {
    "local": {
        "namespace": "loom-local",
        "ingress_host": "yylx.world",
        "frontend_route": "https://yylx.world/local",
        "frontend_api_base": "https://yylx.world/local/api",
        "github_environment": "local",
        "allowed_refs": ("refs/heads/dev",),
        "allowed_tag_prefixes": (),
    },
    "dev": {
        "namespace": "loom-dev",
        "ingress_host": "yylx.world",
        "frontend_route": "https://yylx.world/dev",
        "frontend_api_base": "https://yylx.world/dev/api",
        "github_environment": "dev",
        "allowed_refs": ("refs/heads/dev",),
        "allowed_tag_prefixes": (),
    },
    "staging": {
        "namespace": "loom-staging",
        "ingress_host": "yylx.world",
        "frontend_route": "https://yylx.world/staging",
        "frontend_api_base": "https://yylx.world/staging/api",
        "github_environment": "staging",
        "allowed_refs": ("refs/heads/dev",),
        "allowed_tag_prefixes": (),
    },
    "production": {
        "namespace": "loom-prod",
        "ingress_host": "yylx.world",
        "frontend_route": "https://yylx.world/prod",
        "frontend_api_base": "https://yylx.world/prod/api",
        "github_environment": "production",
        "allowed_refs": ("refs/heads/main",),
        "allowed_tag_prefixes": (),
    },
}

REQUIRED_DISTINCT_FIELDS = (
    "namespace",
    "database_name",
    "task_bucket",
    "trajectories_bucket",
    "artifacts_bucket",
    "secret_store_key_ref",
    "service_api_token_ref",
    "worker_token_ref",
    "provider_secret_ref",
    "yibuapi_secret_ref",
    "provider_connection_namespace",
)

SAFE_SECRET_REF_PREFIXES = ("github-environment:", "env:", "file:")


@dataclass(frozen=True)
class EnvironmentProfile:
    environment: str
    short_name: str
    github_environment: str
    namespace: str
    ingress_host: str
    frontend_route: str
    frontend_api_base: str
    cluster_config: str
    allowed_refs: tuple[str, ...]
    allowed_tag_prefixes: tuple[str, ...]
    database_name: str
    task_bucket: str
    trajectories_bucket: str
    artifacts_bucket: str
    secret_store_key_ref: str
    service_api_token_ref: str
    worker_token_ref: str
    provider_secret_ref: str
    yibuapi_secret_ref: str
    provider_connection_namespace: str

    @classmethod
    def from_toml(cls, path: Path) -> EnvironmentProfile:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
        required = {
            "environment",
            "short_name",
            "github_environment",
            "namespace",
            "ingress_host",
            "frontend_route",
            "frontend_api_base",
            "cluster_config",
            "allowed_refs",
            "allowed_tag_prefixes",
            "database_name",
            "task_bucket",
            "trajectories_bucket",
            "artifacts_bucket",
            "secret_store_key_ref",
            "service_api_token_ref",
            "worker_token_ref",
            "provider_secret_ref",
            "yibuapi_secret_ref",
            "provider_connection_namespace",
        }
        missing = sorted(required - set(raw))
        if missing:
            raise ValueError(f"{path}: missing keys: {missing}")
        return cls(
            environment=_expect_str(raw, path, "environment"),
            short_name=_expect_str(raw, path, "short_name"),
            github_environment=_expect_str(raw, path, "github_environment"),
            namespace=_expect_str(raw, path, "namespace"),
            ingress_host=_expect_str(raw, path, "ingress_host"),
            frontend_route=_expect_str(raw, path, "frontend_route"),
            frontend_api_base=_expect_str(raw, path, "frontend_api_base"),
            cluster_config=_expect_str(raw, path, "cluster_config"),
            allowed_refs=_expect_str_tuple(raw, path, "allowed_refs"),
            allowed_tag_prefixes=_expect_str_tuple(raw, path, "allowed_tag_prefixes"),
            database_name=_expect_str(raw, path, "database_name"),
            task_bucket=_expect_str(raw, path, "task_bucket"),
            trajectories_bucket=_expect_str(raw, path, "trajectories_bucket"),
            artifacts_bucket=_expect_str(raw, path, "artifacts_bucket"),
            secret_store_key_ref=_expect_str(raw, path, "secret_store_key_ref"),
            service_api_token_ref=_expect_str(raw, path, "service_api_token_ref"),
            worker_token_ref=_expect_str(raw, path, "worker_token_ref"),
            provider_secret_ref=_expect_str(raw, path, "provider_secret_ref"),
            yibuapi_secret_ref=_expect_str(raw, path, "yibuapi_secret_ref"),
            provider_connection_namespace=_expect_str(
                raw,
                path,
                "provider_connection_namespace",
            ),
        )


def _expect_str(raw: dict[str, Any], path: Path, key: str) -> str:
    value = raw[key]
    if not isinstance(value, str) or not value:
        raise ValueError(f"{path}: {key} must be a non-empty string")
    return value


def _expect_str_tuple(raw: dict[str, Any], path: Path, key: str) -> tuple[str, ...]:
    value = raw[key]
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{path}: {key} must be a TOML array of strings")
    return tuple(value)


def load_profiles(profiles_dir: Path) -> list[EnvironmentProfile]:
    if not profiles_dir.is_dir():
        raise ValueError(f"profiles directory not found: {profiles_dir}")
    profiles = [
        EnvironmentProfile.from_toml(path)
        for path in sorted(profiles_dir.glob("*.toml"))
        if not path.name.endswith(".cluster.toml")
    ]
    if not profiles:
        raise ValueError(f"no environment profiles found in {profiles_dir}")
    return profiles


def validate_profiles(profiles: list[EnvironmentProfile], repo_root: Path) -> list[str]:
    errors: list[str] = []
    by_env = {profile.environment: profile for profile in profiles}
    if set(by_env) != set(EXPECTED_ENVIRONMENTS):
        errors.append(
            "environment profiles must be exactly "
            f"{sorted(EXPECTED_ENVIRONMENTS)}; got {sorted(by_env)}",
        )
        return errors

    for env_name, expected in EXPECTED_ENVIRONMENTS.items():
        profile = by_env[env_name]
        for key, expected_value in expected.items():
            actual = getattr(profile, key)
            if actual != expected_value:
                errors.append(
                    f"{env_name}: expected {key}={expected_value!r}, got {actual!r}",
                )

        cluster_config = (repo_root / profile.cluster_config).resolve()
        if not cluster_config.is_file():
            errors.append(f"{env_name}: cluster_config not found: {profile.cluster_config}")
            continue
        raw_cluster = tomllib.loads(cluster_config.read_text(encoding="utf-8"))
        for key in (
            "namespace",
            "ingress_host",
            "trajectories_bucket",
            "artifacts_bucket",
        ):
            if raw_cluster.get(key) != getattr(profile, key):
                errors.append(
                    f"{env_name}: {profile.cluster_config} {key}={raw_cluster.get(key)!r} "
                    f"does not match profile {getattr(profile, key)!r}",
                )
        expected_route_path = _url_path(profile.frontend_route)
        try:
            expected_api_base_path = _api_base_path(profile.frontend_api_base)
        except ValueError as exc:
            errors.append(f"{env_name}: {exc}")
            expected_api_base_path = None
        for key, expected_value in (
            ("frontend_route_path", expected_route_path),
            ("frontend_api_base_path", expected_api_base_path),
        ):
            if expected_value is None:
                continue
            actual = raw_cluster.get(key, "")
            if actual != expected_value:
                errors.append(
                    f"{env_name}: {profile.cluster_config} {key}={actual!r} "
                    f"does not match profile {expected_value!r}",
                )
        expected_frontend_environment = "production" if env_name == "production" else env_name
        if raw_cluster.get("frontend_environment") != expected_frontend_environment:
            errors.append(
                f"{env_name}: {profile.cluster_config} frontend_environment="
                f"{raw_cluster.get('frontend_environment')!r} does not match "
                f"{expected_frontend_environment!r}",
            )

    for field in REQUIRED_DISTINCT_FIELDS:
        values = [getattr(profile, field) for profile in profiles]
        if len(values) != len(set(values)):
            errors.append(f"{field} must be distinct across environments")

    if set(by_env) == set(EXPECTED_ENVIRONMENTS):
        # Post-#857: 4 envs (local, dev, staging, production) each with a
        # distinct frontend path. Route isolation requires all 4 to be
        # pairwise-distinct; the earlier "prod must differ from non-prod"
        # rule is subsumed by the generic distinctness check below.
        for field in ("frontend_route", "frontend_api_base"):
            values = [getattr(by_env[env_name], field) for env_name in EXPECTED_ENVIRONMENTS]
            if len(values) != len(set(values)):
                errors.append(
                    f"{field} must be distinct across all environments for route isolation",
                )

    for profile in profiles:
        for field in (
            "database_name",
            "task_bucket",
            "trajectories_bucket",
            "artifacts_bucket",
        ):
            value = getattr(profile, field)
            if profile.short_name not in value:
                errors.append(
                    f"{profile.environment}: {field}={value!r} must include "
                    f"short environment name {profile.short_name!r}",
                )
        for field in (
            "secret_store_key_ref",
            "service_api_token_ref",
            "worker_token_ref",
            "provider_secret_ref",
            "yibuapi_secret_ref",
        ):
            value = getattr(profile, field)
            expected_prefix = f"github-environment:{profile.github_environment}/"
            if not value.startswith(expected_prefix):
                errors.append(
                    f"{profile.environment}: {field} must start with {expected_prefix!r}",
                )
            if not value.startswith(SAFE_SECRET_REF_PREFIXES):
                errors.append(
                    f"{profile.environment}: {field} must use a safe secret ref "
                    f"prefix from {SAFE_SECRET_REF_PREFIXES!r}",
                )
        if profile.environment == "production" and "beta" in profile.frontend_route.lower():
            errors.append("production: frontend_route must not contain beta wording")

    return errors


def build_dry_run_artifact(profiles: list[EnvironmentProfile]) -> dict[str, Any]:
    """Return release-safe target identities without resolving secret values."""
    return {
        "schema_version": 1,
        "artifact_type": "environment-isolation-dry-run",
        "profiles": [
            {
                "environment": profile.environment,
                "github_environment": profile.github_environment,
                "namespace": profile.namespace,
                "ingress_host": profile.ingress_host,
                "database_name": profile.database_name,
                "object_storage": {
                    "task_bucket": profile.task_bucket,
                    "trajectories_bucket": profile.trajectories_bucket,
                    "artifacts_bucket": profile.artifacts_bucket,
                },
                "secret_refs": {
                    "secret_store_key_ref": profile.secret_store_key_ref,
                    "service_api_token_ref": profile.service_api_token_ref,
                    "worker_token_ref": profile.worker_token_ref,
                    "provider_secret_ref": profile.provider_secret_ref,
                    "yibuapi_secret_ref": profile.yibuapi_secret_ref,
                },
                "provider_connection_namespace": profile.provider_connection_namespace,
            }
            for profile in profiles
        ],
    }


def _url_path(url: str) -> str:
    from urllib.parse import urlparse

    path = urlparse(url).path.rstrip("/")
    return "" if path == "/" else path


def _api_base_path(url: str) -> str:
    path = _url_path(url)
    if path.endswith("/api"):
        return path[:-4]
    raise ValueError(f"frontend_api_base must end in /api: {url}")


def validate_workflow(workflow_path: Path) -> list[str]:
    if not workflow_path.is_file():
        return [f"workflow not found: {workflow_path}"]

    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    jobs = workflow.get("jobs", {})
    errors: list[str] = []
    for env_name, expected in EXPECTED_ENVIRONMENTS.items():
        # #857: `local` env is not CI-deployed (manual kind-cluster only) —
        # only staging + production have deploy jobs.
        if env_name == "local":
            if f"deploy-{env_name}" in jobs:
                errors.append(
                    f"deploy-{env_name}: local env must not have a CI deploy job",
                )
            continue
        job_name = f"deploy-{env_name}"
        job = jobs.get(job_name)
        if job is None:
            errors.append(f"workflow missing job {job_name}")
            continue
        environment = job.get("environment", {})
        if environment.get("name") != expected["github_environment"]:
            errors.append(
                f"{job_name}: environment.name must be {expected['github_environment']!r}",
            )
        condition = str(job.get("if", ""))
        if f"environment == '{env_name}'" not in condition:
            errors.append(f"{job_name}: if condition must select input {env_name!r}")
        for ref in expected["allowed_refs"]:
            if ref not in condition:
                errors.append(f"{job_name}: if condition missing allowed ref {ref!r}")
        for tag_prefix in expected["allowed_tag_prefixes"]:
            if tag_prefix not in condition:
                errors.append(
                    f"{job_name}: if condition missing allowed tag prefix {tag_prefix!r}",
                )
        if not expected["allowed_tag_prefixes"] and "refs/tags/" in condition:
            errors.append(f"{job_name}: if condition must not allow tag refs")
        if env_name != "production" and "production" in condition:
            errors.append(f"{job_name}: non-production job references production")
        for secret_name in (
            "LOOM_KUBECONFIG_B64",
            "LOOM_CLUSTER_CONFIG_B64",
            "LOOM_DEPLOY_TOKEN",
        ):
            expected_secret = f"${{{{ secrets.{secret_name} }}}}"
            if job.get("env", {}).get(secret_name) != expected_secret:
                errors.append(
                    f"{job_name}: env.{secret_name} must use environment secret {secret_name}",
                )

    return errors


def build_report(
    *,
    repo_root: Path,
    profiles_dir: Path,
    workflow_path: Path,
) -> dict[str, Any]:
    profiles = load_profiles(profiles_dir)
    errors = validate_profiles(profiles, repo_root)
    errors.extend(validate_workflow(workflow_path))
    return {
        "status": "pass" if not errors else "fail",
        "errors": errors,
        "profiles": [asdict(profile) for profile in profiles],
        "dry_run_artifact": build_dry_run_artifact(profiles),
    }


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate committed dev/staging/prod deployment isolation files.",
    )
    parser.add_argument(
        "--profiles-dir",
        default="deploy/environments",
        help="Directory containing environment profile TOML files.",
    )
    parser.add_argument(
        "--workflow",
        default=".github/workflows/deploy-environment.yml",
        help="Deployment workflow to validate.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON report.")
    parser.add_argument(
        "--dry-run-artifact",
        type=Path,
        help="Write a release-safe JSON artifact with environment target identities.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    repo_root = Path.cwd()
    report = build_report(
        repo_root=repo_root,
        profiles_dir=(repo_root / args.profiles_dir).resolve(),
        workflow_path=(repo_root / args.workflow).resolve(),
    )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        if report["status"] == "pass":
            print("Environment isolation validation: PASS")
        else:
            print("Environment isolation validation: FAIL", file=sys.stderr)
            for error in report["errors"]:
                print(f"- {error}", file=sys.stderr)
    if args.dry_run_artifact is not None:
        args.dry_run_artifact.write_text(
            json.dumps(report["dry_run_artifact"], indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
