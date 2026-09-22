"""Staging release manifest helpers for cluster rollouts."""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from alembic.config import Config
from alembic.script import ScriptDirectory

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
_DEFAULT_ALEMBIC_INI = _REPO_ROOT / "database" / "migrations" / "alembic.ini"


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


def build_release_manifest(
    *,
    config: ClusterConfig,
    config_path: Path | None,
    rendered_manifests: str,
    environment: str,
    image_tag: str,
    git_sha: str | None = None,
    generated_at: str | None = None,
    loom_cli_version: str = _loom_cli_version,
    alembic_ini: Path = _DEFAULT_ALEMBIC_INI,
    expected_image_identities: dict[str, dict[str, dict[str, str]]] | None = None,
) -> dict[str, Any]:
    environment = infer_environment(
        environment=environment,
        namespace=config.namespace,
    )
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

    }


def render_release_manifest_json(manifest: dict[str, Any]) -> str:
    return json.dumps(manifest, indent=2, sort_keys=True) + "\n"


def write_release_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_release_manifest_json(manifest), encoding="utf-8")
