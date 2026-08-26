from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom.personal_dev_capacity import CapacityManagerPersonalDevProjector
from loom_service.config import LoomServiceSettings
from loom_service.dev_instance_runtime import (
    build_dev_instance_provisioner_factory,
    build_personal_dev_preparation_runtime,
)
from loom_service.personal_dev_lifecycle import build_personal_dev_capacity_runtime


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
        "personal_dev_builder_enabled": True,
        "personal_dev_runtime_mode": "acceptance",
        "personal_dev_acceptance_binding_json": _acceptance_binding(),
        "personal_dev_acceptance_plan_sha256": "a" * 64,
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


def _acceptance_binding() -> str:
    return json.dumps(
        {
            "acceptance_plan_sha256": "a" * 64,
            "expires_at": "2026-08-18T00:00:00Z",
            "manager": {
                "authority_incarnation": "00000000-0000-0000-0000-000000000101",
                "configuration_epoch": 7,
                "executable_new_capacity_ceiling": 0,
                "execution_epoch": 0,
                "execution_state": "shadow",
                "observer_principal_id": "personal-dev-lifecycle",
            },
            "schema_version": 1,
            "started_at": "2026-08-17T00:00:00Z",
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def test_controller_wiring_is_absent_by_default(tmp_path: Path) -> None:
    settings = _settings(
        tmp_path,
        dev_instances_enabled=False,
        personal_dev_builder_enabled=False,
        personal_dev_runtime_mode="shadow",
    )
    assert build_dev_instance_provisioner_factory(settings, minio_client=_Minio()) is None
    assert build_personal_dev_preparation_runtime(settings, minio_client=_Minio()) is None
    assert build_personal_dev_capacity_runtime(settings) is None


def test_capacity_runtime_wires_global_projector_and_trusted_installer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Projector:
        async def current_manager_binding(self):  # type: ignore[no-untyped-def]
            raise AssertionError("this wiring test must not contact the manager")

    projector = _Projector()
    captured: dict[str, object] = {}

    def credential(name: str, value: str) -> Path:
        path = tmp_path / name
        path.write_text(value)
        path.chmod(0o600)
        return path

    agent_ca = credential("agent-ca.pem", "agent-ca")
    agent_certificate = credential("agent-certificate.pem", "agent-certificate")
    agent_private_key = credential("agent-private-key.pem", "agent-private-key")
    lifecycle_ca = credential("lifecycle-ca.pem", "lifecycle-ca")
    lifecycle_certificate = credential("lifecycle-certificate.pem", "lifecycle-certificate")
    lifecycle_private_key = credential("lifecycle-private-key.pem", "lifecycle-private-key")

    def projector_from_files(connection: object) -> object:
        captured["connection"] = connection
        return projector

    monkeypatch.setattr(
        CapacityManagerPersonalDevProjector,
        "from_files",
        projector_from_files,
    )
    monkeypatch.setattr(
        "loom_service.personal_dev_lifecycle.build_reporter_tls_context",
        lambda _files: object(),
    )
    settings = _settings(
        tmp_path,
        personal_dev_builder_enabled=True,
        personal_dev_acceptance_binding_json=_acceptance_binding(),
        personal_dev_acceptance_plan_sha256="a" * 64,
        personal_dev_capacity_agent_image=("registry.example/loom-service@sha256:" + "1" * 64),
        personal_dev_capacity_ca_file=agent_ca,
        personal_dev_capacity_certificate_file=agent_certificate,
        personal_dev_capacity_private_key_file=agent_private_key,
        personal_dev_capacity_lifecycle_ca_file=lifecycle_ca,
        personal_dev_capacity_lifecycle_certificate_file=lifecycle_certificate,
        personal_dev_capacity_lifecycle_private_key_file=lifecycle_private_key,
    )

    runtime = build_personal_dev_capacity_runtime(settings)

    assert runtime is not None
    assert runtime.projector is projector
    assert runtime.acceptance_interlock is not None
    assert runtime.acceptance_interlock.projector is projector
    assert runtime.installer is not None
    connection = captured["connection"]
    assert connection.tls_files.ca_file == lifecycle_ca  # type: ignore[attr-defined]
    assert runtime.installer._config.tls_files.ca_file == agent_ca  # type: ignore[attr-defined]


def test_capacity_runtime_rejects_shared_lifecycle_and_agent_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def credential(name: str, value: str) -> Path:
        path = tmp_path / name
        path.write_text(value)
        path.chmod(0o600)
        return path

    agent_ca = credential("shared-ca.pem", "ca")
    shared_certificate = credential("shared-certificate.pem", "certificate")
    shared_private_key = credential("shared-private-key.pem", "private-key")
    monkeypatch.setattr(
        "loom_service.personal_dev_lifecycle.build_reporter_tls_context",
        lambda _files: object(),
    )

    with pytest.raises(RuntimeError, match="must use distinct identities"):
        build_personal_dev_capacity_runtime(
            _settings(
                tmp_path,
                personal_dev_capacity_agent_image=(
                    "registry.example/loom-service@sha256:" + "1" * 64
                ),
                personal_dev_capacity_ca_file=agent_ca,
                personal_dev_capacity_certificate_file=shared_certificate,
                personal_dev_capacity_private_key_file=shared_private_key,
                personal_dev_capacity_lifecycle_ca_file=agent_ca,
                personal_dev_capacity_lifecycle_certificate_file=shared_certificate,
                personal_dev_capacity_lifecycle_private_key_file=shared_private_key,
            )
        )


def test_capacity_runtime_opens_owned_projector_only_after_local_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def credential(name: str, value: str) -> Path:
        path = tmp_path / name
        path.write_text(value)
        path.chmod(0o600)
        return path

    def unexpected_projector(_connection: object) -> object:
        raise AssertionError("projector opened before local construction completed")

    monkeypatch.setattr(
        CapacityManagerPersonalDevProjector,
        "from_files",
        unexpected_projector,
    )
    monkeypatch.setattr(
        "loom_service.personal_dev_lifecycle.build_reporter_tls_context",
        lambda _files: object(),
    )
    monkeypatch.setattr(
        "loom_service.personal_dev_lifecycle.KubectlClient",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ValueError("synthetic local construction failure")
        ),
    )

    with pytest.raises(ValueError, match="local construction failure"):
        build_personal_dev_capacity_runtime(
            _settings(
                tmp_path,
                personal_dev_capacity_agent_image=(
                    "registry.example/loom-service@sha256:" + "1" * 64
                ),
                personal_dev_capacity_ca_file=credential("agent-ca.pem", "agent-ca"),
                personal_dev_capacity_certificate_file=credential(
                    "agent-certificate.pem", "agent-certificate"
                ),
                personal_dev_capacity_private_key_file=credential(
                    "agent-private-key.pem", "agent-private-key"
                ),
                personal_dev_capacity_lifecycle_ca_file=credential(
                    "lifecycle-ca.pem", "lifecycle-ca"
                ),
                personal_dev_capacity_lifecycle_certificate_file=credential(
                    "lifecycle-certificate.pem", "lifecycle-certificate"
                ),
                personal_dev_capacity_lifecycle_private_key_file=credential(
                    "lifecycle-private-key.pem", "lifecycle-private-key"
                ),
            )
        )


@pytest.mark.parametrize(
    "image",
    ["", "@sha256:" + "1" * 64],
)
def test_capacity_runtime_rejects_invalid_agent_image(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    image: str,
) -> None:
    def unexpected_projector(_connection: object) -> object:
        raise AssertionError("credentials must not be opened before config validation")

    monkeypatch.setattr(
        CapacityManagerPersonalDevProjector,
        "from_files",
        unexpected_projector,
    )
    with pytest.raises(RuntimeError, match="configuration is invalid"):
        build_personal_dev_capacity_runtime(
            _settings(tmp_path, personal_dev_capacity_agent_image=image)
        )


def test_personal_runtime_uses_candidate_aware_executor_without_legacy_candidate(
    tmp_path: Path,
) -> None:
    settings = _settings(
        tmp_path,
        dev_instance_candidate_sha="",
        dev_instance_image_tag="",
        dev_instance_container_registry="",
        dev_instance_slurm_actuator_config_json="{}",
    )

    runtime = build_personal_dev_preparation_runtime(settings, minio_client=_Minio())

    assert runtime is not None
    assert runtime.config.minio_endpoint == settings.minio_endpoint


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
