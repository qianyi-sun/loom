from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom_service.config import LoomServiceSettings
from loom_service.dev_instance_runtime import build_dev_instance_provisioner_factory


class _Minio:
    pass


def _settings(tmp_path: Path, **overrides: object) -> LoomServiceSettings:
    kubectl = tmp_path / "kubectl"
    kubectl.write_text("#!/bin/sh\n", encoding="utf-8")
    kubectl.chmod(0o700)
    values: dict[str, object] = {
        "db_url": "postgresql+psycopg://svc:pw@db/management",
        "minio_access_key": "access",
        "minio_secret_key": "secret",
        "dev_instances_enabled": True,
        "dev_instance_database_admin_url": "postgresql+psycopg://admin:pw@db/postgres",
        "dev_instance_candidate_sha": "a1b2c3d" + "0" * 33,
        "dev_instance_deployment_generation": 4,
        "dev_instance_image_tag": "dev-a1b2c3d",
        "dev_instance_container_registry": "registry.example/loom",
        "dev_instance_kubectl_path": kubectl,
        "dev_instance_slurm_actuator_config_json": json.dumps(
            {
                "allowed_nodes": ["worker-a"],
                "env_file": "/var/lib/loom-dev-workers/{environment}.env",
                "repo_dir": "/opt/loom/repo",
                "requested_cpus": 8,
                "requested_memory_mib": 32768,
                "requested_concurrency": 4,
                "container_cpus": 8,
                "container_memory_mib": 32768,
                "container_pids": 2048,
                "job_pids_max": 4096,
            }
        ),
    }
    values.update(overrides)
    return LoomServiceSettings(_env_file=None, **values)  # type: ignore[arg-type]


def test_controller_wiring_is_absent_by_default(tmp_path: Path) -> None:
    settings = _settings(tmp_path, dev_instances_enabled=False)
    assert build_dev_instance_provisioner_factory(settings, minio_client=_Minio()) is None


def test_controller_wiring_requires_complete_candidate_and_slurm_contract(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    factory = build_dev_instance_provisioner_factory(settings, minio_client=_Minio())
    assert factory is not None


def test_controller_rejects_embedded_slurm_secret(tmp_path: Path) -> None:
    settings = _settings(
        tmp_path,
        dev_instance_slurm_actuator_config_json=json.dumps(
            {
                "worker_token": "raw-secret",
            }
        ),
    )
    with pytest.raises(RuntimeError, match="not embed sensitive field"):
        build_dev_instance_provisioner_factory(settings, minio_client=_Minio())


def test_controller_rejects_missing_fixture_admin_credential(tmp_path: Path) -> None:
    settings = _settings(tmp_path, dev_instance_database_admin_url=None)
    with pytest.raises(RuntimeError, match="DATABASE_ADMIN_URL"):
        build_dev_instance_provisioner_factory(settings, minio_client=_Minio())


def test_controller_rejects_unmanaged_worker_env_path(tmp_path: Path) -> None:
    settings = _settings(
        tmp_path,
        dev_instance_slurm_actuator_config_json=json.dumps(
            {
                "allowed_nodes": ["worker-a"],
                "env_file": "/tmp/{environment}.env",
                "repo_dir": "/opt/loom/repo",
                "requested_cpus": 8,
                "requested_memory_mib": 32768,
                "requested_concurrency": 4,
                "container_cpus": 8,
                "container_memory_mib": 32768,
                "container_pids": 2048,
                "job_pids_max": 4096,
            },
        ),
    )
    with pytest.raises(RuntimeError, match="derived protected template"):
        build_dev_instance_provisioner_factory(settings, minio_client=_Minio())
