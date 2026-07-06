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
    "development": {
        "namespace": "loom-dev",
        "ingress_host": "dev.yylx.world",
        "github_environment": "development",
        "allowed_refs": ("refs/heads/dev",),
        "allowed_tag_prefixes": (),
    },
    "staging": {
        "namespace": "loom-staging",
        "ingress_host": "staging.yylx.world",
        "github_environment": "staging",
        "allowed_refs": ("refs/heads/dev",),
        "allowed_tag_prefixes": (),
    },
    "production": {
        "namespace": "loom-prod",
        "ingress_host": "yylx.world",
        "github_environment": "production",
        "allowed_refs": ("refs/heads/main",),
        "allowed_tag_prefixes": ("refs/tags/v",),
    },
}

REQUIRED_DISTINCT_FIELDS = (
    "namespace",
    "ingress_host",
    "database_name",
    "task_bucket",
    "trajectories_bucket",
    "artifacts_bucket",
    "secret_store_key_ref",
    "worker_token_ref",
    "provider_connection_namespace",
)


@dataclass(frozen=True)
class EnvironmentProfile:
    environment: str
    short_name: str
    github_environment: str
    namespace: str
    ingress_host: str
    cluster_config: str
    allowed_refs: tuple[str, ...]
    allowed_tag_prefixes: tuple[str, ...]
    database_name: str
    task_bucket: str
    trajectories_bucket: str
    artifacts_bucket: str
    secret_store_key_ref: str
    worker_token_ref: str
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
            "cluster_config",
            "allowed_refs",
            "allowed_tag_prefixes",
            "database_name",
            "task_bucket",
            "trajectories_bucket",
            "artifacts_bucket",
            "secret_store_key_ref",
            "worker_token_ref",
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
            cluster_config=_expect_str(raw, path, "cluster_config"),
            allowed_refs=_expect_str_tuple(raw, path, "allowed_refs"),
            allowed_tag_prefixes=_expect_str_tuple(raw, path, "allowed_tag_prefixes"),
            database_name=_expect_str(raw, path, "database_name"),
            task_bucket=_expect_str(raw, path, "task_bucket"),
            trajectories_bucket=_expect_str(raw, path, "trajectories_bucket"),
            artifacts_bucket=_expect_str(raw, path, "artifacts_bucket"),
            secret_store_key_ref=_expect_str(raw, path, "secret_store_key_ref"),
            worker_token_ref=_expect_str(raw, path, "worker_token_ref"),
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

    for field in REQUIRED_DISTINCT_FIELDS:
        values = [getattr(profile, field) for profile in profiles]
        if len(values) != len(set(values)):
            errors.append(f"{field} must be distinct across environments")

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
        for field in ("secret_store_key_ref", "worker_token_ref"):
            value = getattr(profile, field)
            expected_prefix = f"github-environment:{profile.github_environment}/"
            if not value.startswith(expected_prefix):
                errors.append(
                    f"{profile.environment}: {field} must start with "
                    f"{expected_prefix!r}",
                )

    return errors


def validate_workflow(workflow_path: Path) -> list[str]:
    if not workflow_path.is_file():
        return [f"workflow not found: {workflow_path}"]

    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    jobs = workflow.get("jobs", {})
    errors: list[str] = []
    for env_name, expected in EXPECTED_ENVIRONMENTS.items():
        job_name = f"deploy-{env_name}"
        job = jobs.get(job_name)
        if job is None:
            errors.append(f"workflow missing job {job_name}")
            continue
        environment = job.get("environment", {})
        if environment.get("name") != expected["github_environment"]:
            errors.append(
                f"{job_name}: environment.name must be "
                f"{expected['github_environment']!r}",
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
                    f"{job_name}: env.{secret_name} must use environment secret "
                    f"{secret_name}",
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
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
