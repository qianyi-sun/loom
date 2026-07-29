"""Staging release manifest helpers for cluster rollouts."""

from __future__ import annotations

import hashlib
import json
import subprocess
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from alembic.config import Config
from alembic.script import ScriptDirectory

from loom.security.redaction import is_sensitive_environment_key, redact_text
from loom_cli import __version__ as _loom_cli_version
from loom_cli.cluster_backup_guard import infer_environment
from loom_cli.cluster_cmd import _rendered_deployment_images
from loom_cli.cluster_config import ClusterConfig
from loom_cli.cluster_workload_trust import (
    PROTECTED_WORKLOAD_TRUST_ENVIRONMENTS,
    workload_contract_from_cluster_config,
    workload_contract_from_mapping,
    workload_contract_profile_from_file,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_ALEMBIC_INI = _REPO_ROOT / "migrations" / "alembic.ini"


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_text(text: str) -> str:
    return _sha256_bytes(text.encode("utf-8"))


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _git_head_sha() -> str:
    proc = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=_REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "git rev-parse HEAD failed")
    return proc.stdout.strip()


def _alembic_heads(alembic_ini: Path = _DEFAULT_ALEMBIC_INI) -> list[str]:
    cfg = Config(str(alembic_ini))
    script = ScriptDirectory.from_config(cfg)
    return sorted(script.get_heads())


def _substitute_release_vars(
    value: Any,
    *,
    image_tag: str,
    env_config_version: str,
    git_sha: str,
) -> Any:
    if isinstance(value, str):
        return (
            value.replace("${IMAGE_TAG}", image_tag)
            .replace("${ENV_CONFIG_VERSION}", env_config_version)
            .replace("${GIT_SHA}", git_sha)
        )
    if isinstance(value, list):
        return [
            _substitute_release_vars(
                item,
                image_tag=image_tag,
                env_config_version=env_config_version,
                git_sha=git_sha,
            )
            for item in value
        ]
    if isinstance(value, dict):
        return {
            key: _substitute_release_vars(
                child,
                image_tag=image_tag,
                env_config_version=env_config_version,
                git_sha=git_sha,
            )
            for key, child in value.items()
        }
    return value


def _external_worker_summary(
    *,
    environment_state_path: Path | None,
    image_tag: str,
    env_config_version: str,
    git_sha: str,
) -> dict[str, Any]:
    if environment_state_path is None:
        return {
            "environment_state_file": None,
            "control_plane_environment": None,
            "slurm_pools": [],
            "gb10_desired_states": [],
            "external_slurm_runner_prerequisites": {},
            "external_slurm_autoscaler_supervisors": [],
        }

    raw_bytes = environment_state_path.read_bytes()
    raw = tomllib.loads(raw_bytes.decode("utf-8"))
    resolved = _substitute_release_vars(
        raw,
        image_tag=image_tag,
        env_config_version=env_config_version,
        git_sha=git_sha,
    )
    control_plane_environment = resolved.get(
        "control_plane_environment",
        resolved.get("environment"),
    )

    slurm_pools: list[dict[str, Any]] = []
    for policy in resolved.get("worker_pool_autoscaler_policies", []):
        if not isinstance(policy, dict):
            continue
        actuator_config = policy.get("actuator_config")
        if not isinstance(actuator_config, dict):
            actuator_config = {}
        slurm_pools.append(
            {
                "pool_name": policy.get("pool_name"),
                "actuator": policy.get("actuator"),
                "enabled": policy.get("enabled", False),
                "disabled_reason": policy.get("disabled_reason"),
                "external_runner": bool(actuator_config.get("external_runner")),
                "env_file": actuator_config.get("env_file"),
                "repo_dir": actuator_config.get("repo_dir"),
            }
        )

    gb10_desired_states: list[dict[str, Any]] = []
    for desired in resolved.get("gb10_worker_pool_desired_states", []):
        if not isinstance(desired, dict):
            continue
        gb10_desired_states.append(
            {
                "environment": desired.get(
                    "environment",
                    control_plane_environment,
                ),
                "pool_name": desired.get("pool_name"),
                "image_tag": desired.get("image_tag"),
                "max_concurrent": desired.get("max_concurrent"),
                "env_config_version": desired.get("env_config_version"),
                "source_git_commit": desired.get("source_git_commit"),
                "target_slots": desired.get("target_slots"),
                "host_intents": desired.get("host_intents"),
            }
        )

    prerequisites: dict[str, Any] = {}
    raw_prerequisites = resolved.get("external_slurm_runner_prerequisites")
    if isinstance(raw_prerequisites, dict):
        # Record only the requirement.  An artifact path, digest, or pass claim
        # would let candidate-controlled repository state attest itself.
        for field in (
            "pools",
            "materialize",
            "require_external_allocation_authority",
        ):
            if field in raw_prerequisites:
                prerequisites[field] = raw_prerequisites[field]

    supervisors: list[dict[str, Any]] = []
    for supervisor in resolved.get("external_slurm_autoscaler_supervisors", []):
        if not isinstance(supervisor, dict):
            continue
        supervisors.append(
            {
                "pool_name": supervisor.get("pool_name"),
                "enabled": supervisor.get("enabled", True),
                "active": supervisor.get("active", True),
            }
        )

    return {
        "environment_state_file": {
            "path": str(environment_state_path),
            "sha256": _sha256_bytes(raw_bytes),
        },
        "control_plane_environment": control_plane_environment,
        "slurm_pools": slurm_pools,
        "gb10_desired_states": gb10_desired_states,
        "external_slurm_runner_prerequisites": prerequisites,
        "external_slurm_autoscaler_supervisors": supervisors,
    }


def _catalog_provisioning_summary(
    *,
    environment_state_path: Path | None,
    image_tag: str,
    env_config_version: str,
    git_sha: str,
) -> dict[str, Any]:
    if environment_state_path is None:
        return {"required": False}

    raw = tomllib.loads(environment_state_path.read_text(encoding="utf-8"))
    resolved = _substitute_release_vars(
        raw,
        image_tag=image_tag,
        env_config_version=env_config_version,
        git_sha=git_sha,
    )
    catalog = resolved.get("catalog_provisioning")
    if not isinstance(catalog, dict):
        return {"required": False}
    summary: dict[str, Any] = {"required": bool(catalog.get("required"))}
    command = catalog.get("command")
    if isinstance(command, str) and command:
        summary["command"] = command
    env_file = catalog.get("env_file")
    if isinstance(env_file, str) and env_file:
        summary["env_file"] = env_file
    env = catalog.get("env")
    if isinstance(env, dict):
        summary["env"] = {
            key: "[REDACTED]" if is_sensitive_environment_key(key) else redact_text(value)
            for key, value in env.items()
            if isinstance(key, str) and key and isinstance(value, str) and value
        }
    env_sources = catalog.get("env_sources")
    if isinstance(env_sources, dict):
        summary["env_sources"] = {
            key: redact_text(value)
            for key, value in env_sources.items()
            if isinstance(key, str) and key and isinstance(value, str) and value
        }
    required_env = catalog.get("required_env")
    if isinstance(required_env, list):
        summary["required_env"] = [item for item in required_env if isinstance(item, str) and item]
    return summary


def build_release_manifest(
    *,
    config: ClusterConfig,
    config_path: Path | None,
    rendered_manifests: str,
    environment: str,
    image_tag: str,
    git_sha: str | None = None,
    environment_state_path: Path | None = None,
    env_config_version: str | None = None,
    generated_at: str | None = None,
    loom_cli_version: str = _loom_cli_version,
    alembic_ini: Path = _DEFAULT_ALEMBIC_INI,
    expected_image_identities: dict[str, dict[str, dict[str, str]]] | None = None,
) -> dict[str, Any]:
    environment = infer_environment(
        environment=environment,
        namespace=config.namespace,
    )
    release_env_config_version = env_config_version or image_tag
    release_git_sha = git_sha or _git_head_sha()
    config_bytes = (
        config_path.read_bytes()
        if config_path is not None
        else render_release_manifest_json(config.to_render_context()).encode("utf-8")
    )
    if environment in PROTECTED_WORKLOAD_TRUST_ENVIRONMENTS:
        workload_contract = workload_contract_from_mapping(
            workload_contract_profile_from_file(config_path)
        )
        violations = workload_contract.v1_violations()
        if violations:
            raise ValueError(
                "protected release workload contract violates v1: " + "; ".join(violations)
            )
    else:
        workload_contract = workload_contract_from_cluster_config(config)
    return {
        "schema_version": 1,
        "release": {
            "environment": environment,
            "git_sha": release_git_sha,
            "image_tag": image_tag,
            "generated_at": generated_at or _utc_now(),
        },
        "tooling": {
            "loom_cli_version": loom_cli_version,
        },
        "workload_contract": workload_contract.as_manifest(),
        "cluster_config": {
            "path": str(config_path) if config_path is not None else None,
            "sha256": _sha256_bytes(config_bytes),
            "namespace": config.namespace,
            "k8s_worker_enabled": config.k8s_worker.enabled,
        },
        "rendered_manifest": {
            "sha256": _sha256_text(rendered_manifests),
            "deployment_images": _rendered_deployment_images(rendered_manifests),
            "deployment_image_identities": expected_image_identities or {},
        },
        "alembic": {
            "expected_heads": _alembic_heads(alembic_ini),
            "compatible_heads": _alembic_heads(alembic_ini),
        },
        "external_workers": _external_worker_summary(
            environment_state_path=environment_state_path,
            image_tag=image_tag,
            env_config_version=release_env_config_version,
            git_sha=release_git_sha,
        ),
        "catalog_provisioning": _catalog_provisioning_summary(
            environment_state_path=environment_state_path,
            image_tag=image_tag,
            env_config_version=release_env_config_version,
            git_sha=release_git_sha,
        ),
    }


def render_release_manifest_json(manifest: dict[str, Any]) -> str:
    return json.dumps(manifest, indent=2, sort_keys=True) + "\n"


def write_release_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_release_manifest_json(manifest), encoding="utf-8")
