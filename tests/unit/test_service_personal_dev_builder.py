from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_service.personal_dev_builder import build_personal_dev_builder_runtime
from loom_service.personal_dev_candidate_gc import build_personal_dev_artifact_collector


def _settings(tmp_path: Path, **overrides):
    executable = tmp_path / "tool"
    executable.write_text("#!/bin/sh\nexit 0\n")
    executable.chmod(0o755)
    cache = tmp_path / "scanner-cache"
    (cache / "db").mkdir(parents=True)
    (cache / "java-db").mkdir()
    (cache / "db" / "trivy.db").write_bytes(b"trivy-db")
    (cache / "java-db" / "trivy-java.db").write_bytes(b"trivy-java-db")
    scanner_identity = (
        "trivy-bin-sha256:"
        + hashlib.sha256(executable.read_bytes()).hexdigest()
        + ":db-sha256:"
        + hashlib.sha256(b"trivy-db").hexdigest()
        + ":java-db-sha256:"
        + hashlib.sha256(b"trivy-java-db").hexdigest()
    )
    registry_auth = tmp_path / "registry" / "config.json"
    registry_auth.parent.mkdir()
    registry_auth.write_text('{"auths":{"registry.example":{}}}')
    registry_auth.chmod(0o640)
    values = {
        "dev_instances_enabled": True,
        "personal_dev_builder_enabled": True,
        "personal_dev_candidate_gc_retention_sec": 86400,
        "personal_dev_candidate_gc_lease_sec": 900,
        "personal_dev_candidate_gc_poll_interval_sec": 30.0,
        "artifacts_bucket": "artifacts",
        "personal_dev_source_max_archive_bytes": 1024 * 1024,
        "personal_dev_builder_lease_sec": 4200,
        "personal_dev_builder_max_artifact_bytes": 8 * 1024 * 1024,
        "personal_dev_builder_max_image_archive_bytes": 512 * 1024,
        "personal_dev_builder_image": "registry.example/builder@sha256:" + "a" * 64,
        "personal_dev_builder_runtime_class_name": "loom-personal-dev-builder",
        "dev_instance_kubectl_path": executable,
        "dev_instance_kube_context": "dev",
        "personal_dev_builder_scanner_path": executable,
        "personal_dev_builder_scanner_cache_dir": cache,
        "personal_dev_builder_scanner_identity": scanner_identity,
        "personal_dev_builder_scanner_policy_sha256": "c" * 64,
        "personal_dev_builder_skopeo_path": executable,
        "personal_dev_builder_docker_path": executable,
        "personal_dev_builder_registry_prefix": "registry.example/personal-dev",
        "personal_dev_builder_registry_auth_file": registry_auth,
        "personal_dev_builder_publisher_identity": (
            "system:serviceaccount:loom-dev:candidate-exporter"
        ),
        "personal_dev_trusted_launcher_profile_sha256": "d" * 64,
        "personal_dev_protocol_versions_json": json.dumps(
            {
                "capacity-agent": "v1",
                "claim-guard": "v1",
                "control-plane-worker": "v1",
                "database-migrations": "expand-compatible-v1",
                "personal-dev-activation": "v1",
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_builder_runtime_is_inert_unless_explicitly_enabled(tmp_path: Path) -> None:
    settings = _settings(tmp_path, personal_dev_builder_enabled=False)
    assert (
        build_personal_dev_builder_runtime(
            settings,  # type: ignore[arg-type]
            minio_client=object(),
        )
        is None
    )


def test_builder_runtime_wires_one_exact_bounded_authority(tmp_path: Path) -> None:
    runtime = build_personal_dev_builder_runtime(
        _settings(tmp_path),  # type: ignore[arg-type]
        minio_client=object(),
    )

    assert runtime is not None
    assert runtime.manifest_config.max_artifact_bytes == 8 * 1024 * 1024
    assert runtime.manifest_config.runtime_class_name == "loom-personal-dev-builder"
    assert runtime.capabilities.max_artifact_bytes == 8 * 1024 * 1024
    assert runtime.exporter.registry_prefix == "registry.example/personal-dev"


def test_builder_runtime_rejects_placeholder_safety_authority(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="launcher profile"):
        build_personal_dev_builder_runtime(
            _settings(tmp_path, personal_dev_trusted_launcher_profile_sha256=""),  # type: ignore[arg-type]
            minio_client=object(),
        )


def test_artifact_collector_is_wired_only_for_personal_lifecycle(tmp_path: Path) -> None:
    disabled_root = tmp_path / "disabled"
    enabled_root = tmp_path / "enabled"
    disabled_root.mkdir()
    enabled_root.mkdir()
    settings = _settings(disabled_root, dev_instances_enabled=False)
    assert (
        build_personal_dev_artifact_collector(
            settings,  # type: ignore[arg-type]
            minio_client=object(),
        )
        is None
    )
    builder_disabled_root = tmp_path / "builder-disabled"
    builder_disabled_root.mkdir()
    builder_disabled = _settings(
        builder_disabled_root,
        personal_dev_builder_enabled=False,
    )
    assert (
        build_personal_dev_artifact_collector(
            builder_disabled,  # type: ignore[arg-type]
            minio_client=object(),
        )
        is None
    )

    configured = _settings(enabled_root)
    collector = build_personal_dev_artifact_collector(
        configured,  # type: ignore[arg-type]
        minio_client=object(),
    )
    assert collector is not None
    assert collector.objects.expected_bucket == "artifacts"
    assert collector.registry.expected_registry_prefix == "registry.example/personal-dev"


def test_artifact_collector_rejects_invalid_gc_settings_at_startup(tmp_path: Path) -> None:
    for override in (
        {"personal_dev_candidate_gc_retention_sec": -1},
        {"personal_dev_candidate_gc_lease_sec": 0},
        {"personal_dev_candidate_gc_poll_interval_sec": 0.0},
    ):
        root = tmp_path / str(len(list(tmp_path.iterdir())))
        root.mkdir()
        with pytest.raises(RuntimeError, match="GC settings"):
            build_personal_dev_artifact_collector(
                _settings(root, **override),  # type: ignore[arg-type]
                minio_client=object(),
            )
