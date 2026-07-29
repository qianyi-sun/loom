from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.gb10_readiness import GB10ProbeTarget
from loom_cli.rollout.operator.installed_preflight_inputs import InstalledPreflightInputs
from loom_cli.rollout.operator.preflight import (
    EXPECTED_GB10_SSH_CONFIG_SHA256,
    GB10PreflightInputs,
    shared_repository_binding_digest,
)
from tests.loom_cli.rollout.operator.test_checkpoint_inventory_provider import _config


def _binding() -> dict[str, int]:
    return {
        "authority_device": 1,
        "authority_inode": 2,
        "consumer_primary_gid": 3,
        "consumer_uid": 4,
        "parent_device": 5,
        "parent_inode": 6,
        "repository_device": 7,
        "repository_inode": 8,
        "service_primary_gid": 9,
        "service_uid": 10,
        "shared_gid": 11,
    }


def test_loader_binds_installed_static_authorities_without_secret_values(tmp_path: Path) -> None:
    config = _config(tmp_path)
    ssh_config = tmp_path / "ssh-config"
    identity = tmp_path / "identity"
    catalog = tmp_path / "catalog.env"
    policy = tmp_path / "migration-policy.json"
    ssh_payload = (
        Path(__file__).resolve().parents[4] / "deploy/worker-pools/gb10/ssh_config"
    ).read_bytes()
    policy.write_text("{}\n", encoding="utf-8")

    def read_file(path: Path, **_kwargs):
        if path == ssh_config:
            return SimpleNamespace(payload=ssh_payload, metadata_fingerprint="a" * 64)
        if path == identity:
            return SimpleNamespace(payload=b"secret", metadata_fingerprint="b" * 64)
        if path == config.kubeconfig_path:
            return SimpleNamespace(payload=b"kube", metadata_fingerprint="c" * 64)
        raise AssertionError(path)

    inputs = InstalledPreflightInputs.load(
        config,
        service_uid=501,
        verify_install=lambda **_kwargs: SimpleNamespace(
            ready=True,
            attestation=SimpleNamespace(payload_digest="d" * 64),
        ),
        read_file=read_file,
        catalog_path_loader=lambda *_args, **_kwargs: catalog,
        gb10_inputs_loader=lambda *_args, **_kwargs: GB10PreflightInputs(
            ssh_config=ssh_config,
            identity=identity,
            targets=(GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service"),),
        ),
        shared_binding_loader=lambda **_kwargs: _binding(),
        migration_policy_path=policy,
    )

    assert inputs.runner_install_digest == "d" * 64
    assert inputs.kubeconfig_metadata_digest == "c" * 64
    assert inputs.gb10_identity_metadata_fingerprint == "b" * 64
    assert inputs.browser_token_path == Path(config.admin_token_source.removeprefix("file:"))
    assert {source.label for source in inputs.credential_sources} == {
        "admin",
        "worker",
        "service",
        "catalog",
        "readonly-probe",
        "readonly-kubeconfig",
        "readonly-database",
        "readonly-minio",
        "rehearsal-kubeconfig",
        "server-dry-run-kubeconfig",
    }
    assert all("secret" not in repr(source) for source in inputs.credential_sources)
    assert inputs.migration_policy_digest == hashlib.sha256(policy.read_bytes()).hexdigest()
    assert inputs.migration_policy_path == policy


def test_loader_uses_exact_candidate_policy_path_in_packaged_runtime(tmp_path: Path) -> None:
    config = _config(tmp_path)
    policy = config.runner_repo / "config/staging-migration-policy.json"
    policy.parent.mkdir(parents=True)
    policy.write_text("{}\n", encoding="utf-8")
    ssh_config = tmp_path / "ssh-config"
    identity = tmp_path / "identity"
    ssh_payload = (
        Path(__file__).resolve().parents[4] / "deploy/worker-pools/gb10/ssh_config"
    ).read_bytes()

    def read_file(path: Path, **_kwargs):
        if path == ssh_config:
            return SimpleNamespace(payload=ssh_payload, metadata_fingerprint="a" * 64)
        return SimpleNamespace(payload=b"private", metadata_fingerprint="b" * 64)

    inputs = InstalledPreflightInputs.load(
        config,
        service_uid=501,
        verify_install=lambda **_kwargs: SimpleNamespace(
            ready=True,
            attestation=SimpleNamespace(payload_digest="d" * 64),
        ),
        read_file=read_file,
        catalog_path_loader=lambda *_args, **_kwargs: tmp_path / "catalog.env",
        gb10_inputs_loader=lambda *_args, **_kwargs: GB10PreflightInputs(
            ssh_config=ssh_config,
            identity=identity,
            targets=(GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service"),),
        ),
        shared_binding_loader=lambda **_kwargs: _binding(),
    )

    assert inputs.migration_policy_path == policy
    assert inputs.migration_policy_digest == hashlib.sha256(policy.read_bytes()).hexdigest()


def test_loader_uses_only_system_binding_for_external_gb10_profile(tmp_path: Path) -> None:
    config = _config(tmp_path)
    policy = tmp_path / "migration-policy.json"
    policy.write_text("{}\n", encoding="utf-8")
    ssh_config = tmp_path / "ssh-config"
    identity = tmp_path / "identity"
    ssh_payload = (
        Path(__file__).resolve().parents[4] / "deploy/worker-pools/gb10/ssh_config"
    ).read_bytes()
    system_binding = {**_binding(), "repository_inode": 99}
    system_reads: list[str] = []

    def read_file(path: Path, **_kwargs):
        if path == ssh_config:
            return SimpleNamespace(payload=ssh_payload, metadata_fingerprint="a" * 64)
        return SimpleNamespace(payload=b"private", metadata_fingerprint="b" * 64)

    inputs = InstalledPreflightInputs.load(
        config,
        service_uid=501,
        verify_install=lambda **_kwargs: SimpleNamespace(
            ready=True,
            attestation=SimpleNamespace(payload_digest="d" * 64),
        ),
        read_file=read_file,
        catalog_path_loader=lambda *_args, **_kwargs: tmp_path / "catalog.env",
        gb10_inputs_loader=lambda *_args, **_kwargs: GB10PreflightInputs(
            ssh_config=ssh_config,
            identity=identity,
            targets=(GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service"),),
        ),
        shared_binding_loader=lambda **_kwargs: pytest.fail(
            "legacy shared binding must not run for the external authority"
        ),
        system_binding_loader=lambda **_kwargs: (
            system_reads.append("system-mount") or system_binding
        ),
        external_profile_loader=lambda *_args, **_kwargs: SimpleNamespace(profile_digest="e" * 64),
        migration_policy_path=policy,
    )

    assert inputs.gb10_mount_binding is None
    assert inputs.gb10_mount_binding_digest is None
    assert system_reads == []
    assert inputs.resolve_gb10_mount_binding() == (
        system_binding,
        shared_repository_binding_digest(system_binding),
    )
    assert system_reads == ["system-mount"]
    assert inputs.gb10_external_profile_digest == "e" * 64


def test_loader_rejects_ssh_topology_digest_drift(tmp_path: Path) -> None:
    config = _config(tmp_path)
    ssh_config = tmp_path / "ssh-config"
    identity = tmp_path / "identity"
    assert hashlib.sha256(b"drift").hexdigest() != EXPECTED_GB10_SSH_CONFIG_SHA256

    def read_file(path: Path, **_kwargs):
        return SimpleNamespace(payload=b"drift", metadata_fingerprint="a" * 64)

    with pytest.raises(ValueError, match="SSH topology drifted"):
        InstalledPreflightInputs.load(
            config,
            service_uid=501,
            verify_install=lambda **_kwargs: SimpleNamespace(
                ready=True,
                attestation=SimpleNamespace(payload_digest="d" * 64),
            ),
            read_file=read_file,
            catalog_path_loader=lambda *_args, **_kwargs: tmp_path / "catalog.env",
            gb10_inputs_loader=lambda *_args, **_kwargs: GB10PreflightInputs(
                ssh_config=ssh_config,
                identity=identity,
                targets=(GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service"),),
            ),
            shared_binding_loader=lambda **_kwargs: _binding(),
            migration_policy_path=tmp_path / "missing-policy.json",
        )
