from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

import loom.personal_dev_control_plane_config as config_module
from loom.personal_dev_control_plane_config import (
    PersonalDevTrustedReleaseError,
    load_personal_dev_control_plane_profile,
    load_personal_dev_trusted_release,
)

_ROOT = Path(__file__).resolve().parents[2]
_PROFILE = _ROOT / "deploy/dev-fleet/personal-dev-control-plane.toml"


def _release() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "source_sha": "1" * 40,
        "source_tree": "2" * 40,
        "images": {
            "loom_service": "ghcr.io/qianyi-sun/loom-service@sha256:" + "3" * 64,
            "personal_dev_builder": (
                "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "4" * 64
            ),
            "personal_dev_activation_agent": (
                "ghcr.io/qianyi-sun/loom-personal-dev-activation-agent@sha256:" + "5" * 64
            ),
            "postgres": "docker.io/library/postgres@sha256:" + "6" * 64,
            "minio": "quay.io/minio/minio@sha256:" + "7" * 64,
        },
        "release_evidence_sha256": "8" * 64,
    }


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _write_release(
    tmp_path: Path,
    value: object | None = None,
    *,
    payload: bytes | None = None,
) -> tuple[Path, str]:
    encoded = payload if payload is not None else _canonical(_release() if value is None else value)
    path = tmp_path / "trusted-release.json"
    path.write_bytes(encoded)
    path.chmod(0o600)
    return path, hashlib.sha256(encoded).hexdigest()


def _write_profile(tmp_path: Path, transform: Callable[[str], str]) -> Path:
    source = _PROFILE.read_text(encoding="utf-8")
    changed = transform(source)
    assert changed != source
    path = tmp_path / "profile.toml"
    path.write_text(changed, encoding="utf-8")
    return path


def test_checked_in_shadow_profile_is_exact_and_canonical() -> None:
    profile = load_personal_dev_control_plane_profile(_PROFILE)

    assert profile.schema_version == 1
    assert profile.namespace == "loom-dev"
    assert profile.personal_namespace_prefix == "loom-dev-"
    assert profile.min_slots_default == 0
    assert profile.max_slots_limit == 8
    assert profile.executable_new_capacity_ceiling == 0
    assert profile.dev_instances_enabled is False
    assert profile.personal_dev_builder_enabled is False
    assert profile.activation_agent_replicas == 0
    assert [(item.pool_id, item.architecture) for item in profile.pools] == [
        ("gb10", "arm64"),
        ("oldlab", "x86_64"),
    ]
    assert all(not hasattr(item, "weight") for item in profile.pools)
    assert profile.identities.management_secret == "loom-personal-dev-management"
    assert profile.identities.activation_public_secret == ("loom-personal-dev-activation-public")
    assert profile.identities.activation_private_secret == ("loom-personal-dev-activation-agent")
    assert profile.identities.scanner_cache_pvc == ("loom-personal-dev-scanner-cache")
    assert profile.builder.prepared is False
    assert profile.builder.runtime_class_name == "loom-personal-dev-builder"
    assert profile.builder.registry_prefix == "ghcr.io/qianyi-sun/loom-dev"
    assert profile.network.public_origin == "https://loom-service.dev.yylx.world"
    assert profile.network.kubernetes_api_cidr != "0.0.0.0/0"
    assert profile.network.kubernetes_api_port == 443
    assert profile.protocol_versions == {
        "capacity-agent": "v1",
        "claim-guard": "v1",
        "control-plane-worker": "v1",
        "database-migrations": "expand-compatible-v1",
        "personal-dev-activation": "v1",
    }
    assert profile.canonical_bytes() == _canonical(profile.canonical_value())
    assert (
        profile.canonical_bytes()
        == load_personal_dev_control_plane_profile(_PROFILE).canonical_bytes()
    )


@pytest.mark.parametrize(
    "transform",
    [
        lambda text: text + "unknown_key = true\n",
        lambda text: text.replace("schema_version = 1\n", "", 1),
        lambda text: text.replace('namespace = "loom-dev"', 'namespace = "loom-dev-shared"'),
        lambda text: text.replace(
            'personal_namespace_prefix = "loom-dev-"',
            'personal_namespace_prefix = "loom-personal-"',
        ),
        lambda text: text.replace("min_slots_default = 0", "min_slots_default = 1"),
        lambda text: text.replace("max_slots_limit = 8", "max_slots_limit = 9"),
        lambda text: text.replace("max_slots_limit = 8", 'max_slots_limit = "8"'),
        lambda text: text.replace(
            "executable_new_capacity_ceiling = 0",
            "executable_new_capacity_ceiling = 1",
        ),
        lambda text: text.replace("dev_instances_enabled = false", "dev_instances_enabled = true"),
        lambda text: text.replace(
            "personal_dev_builder_enabled = false",
            "personal_dev_builder_enabled = true",
        ),
        lambda text: text.replace("activation_agent_replicas = 0", "activation_agent_replicas = 1"),
        lambda text: text.replace('pool_id = "oldlab"', 'pool_id = "other"', 1),
        lambda text: text.replace('architecture = "x86_64"', 'architecture = "arm64"', 1),
        lambda text: text.replace(
            'architecture = "x86_64"', 'architecture = "x86_64"\nweight = 1', 1
        ),
        lambda text: text + '\n[[pools]]\npool_id = "extra"\narchitecture = "x86_64"\n',
        lambda text: text.replace(
            'management_secret = "loom-personal-dev-management"',
            'management_secret = "Not_A_DNS_Name"',
        ),
        lambda text: text.replace(
            'scanner_cache_pvc = "loom-personal-dev-scanner-cache"',
            'scanner_cache_pvc = "/tmp/cache"',
        ),
        lambda text: text.replace(
            'public_origin = "https://loom-service.dev.yylx.world"',
            'public_origin = "http://loom-service.dev.yylx.world"',
        ),
        lambda text: text.replace(
            'publisher_identity = "system:serviceaccount:loom-dev:loom-personal-dev-management"',
            'publisher_identity = "system:serviceaccount:other:loom-personal-dev-management"',
        ),
        lambda text: text.replace(
            'capacity_manager_pod_label_key = "app.kubernetes.io/name"',
            'capacity_manager_pod_label_key = "/name"',
        ),
        lambda text: text.replace(
            'kubernetes_api_cidr = "10.43.0.1/32"', 'kubernetes_api_cidr = "0.0.0.0/0"'
        ),
        lambda text: text.replace("kubernetes_api_port = 443", "kubernetes_api_port = 0"),
        lambda text: text.replace('postgres_storage = "20Gi"', 'postgres_storage = "unbounded"'),
        lambda text: text.replace("prepared = false", "prepared = true"),
        lambda text: text.replace('cpu_request = "250m"', 'cpu_request = "3"', 1),
        lambda text: text.replace(
            'protocol_versions_json = "{',
            'protocol_versions_json = "{ \\',
            1,
        ),
    ],
)
def test_profile_rejects_unsafe_or_non_shadow_configuration(
    tmp_path: Path,
    transform: Callable[[str], str],
) -> None:
    path = _write_profile(tmp_path, transform)

    with pytest.raises((ValidationError, ValueError)):
        load_personal_dev_control_plane_profile(path)


def test_profile_rejects_missing_and_duplicate_pools(tmp_path: Path) -> None:
    source = _PROFILE.read_text(encoding="utf-8")
    marker = '[[pools]]\npool_id = "gb10"'
    first, second = source.split(marker, 1)
    missing = tmp_path / "missing.toml"
    missing.write_text(first, encoding="utf-8")
    duplicate = tmp_path / "duplicate.toml"
    duplicate.write_text(source + marker + second, encoding="utf-8")

    with pytest.raises((ValidationError, ValueError)):
        load_personal_dev_control_plane_profile(missing)
    with pytest.raises((ValidationError, ValueError)):
        load_personal_dev_control_plane_profile(duplicate)


def test_trusted_release_loads_exact_owner_only_canonical_bytes(tmp_path: Path) -> None:
    path, digest = _write_release(tmp_path)

    release = load_personal_dev_trusted_release(path, digest)

    assert release.source_sha == "1" * 40
    assert release.source_tree == "2" * 40
    assert release.images.loom_service == ("ghcr.io/qianyi-sun/loom-service@sha256:" + "3" * 64)
    assert release.images.personal_dev_builder.endswith("4" * 64)
    assert release.images.personal_dev_activation_agent.endswith("5" * 64)
    assert release.images.postgres.endswith("6" * 64)
    assert release.images.minio.endswith("7" * 64)
    assert release.release_evidence_sha256 == "8" * 64
    assert release.canonical_bytes() == path.read_bytes()


@pytest.mark.parametrize(
    ("mutate", "expected_digest"),
    [
        (lambda value: {**value, "unknown": True}, None),
        (lambda value: {key: item for key, item in value.items() if key != "source_tree"}, None),
        (lambda value: {**value, "schema_version": 2}, None),
        (lambda value: {**value, "source_sha": "1" * 39}, None),
        (lambda value: {**value, "source_tree": "A" * 40}, None),
        (lambda value: {**value, "release_evidence_sha256": "0" * 64}, None),
        (
            lambda value: {
                **value,
                "images": {**value["images"], "unexpected": "unused"},
            },
            None,
        ),
        (
            lambda value: {
                **value,
                "images": {key: item for key, item in value["images"].items() if key != "minio"},
            },
            None,
        ),
        (
            lambda value: {
                **value,
                "images": {
                    **value["images"],
                    "loom_service": "ghcr.io/qianyi-sun/loom-service:dev",
                },
            },
            None,
        ),
        (
            lambda value: {
                **value,
                "images": {
                    **value["images"],
                    "loom_service": "ghcr.io/qianyi-sun/loom-service@sha256:" + "0" * 64,
                },
            },
            None,
        ),
        (
            lambda value: {
                **value,
                "images": {
                    **value["images"],
                    "loom_service": "ghcr.io/other/loom-service@sha256:" + "3" * 64,
                },
            },
            None,
        ),
        (
            lambda value: {
                **value,
                "images": {
                    **value["images"],
                    "personal_dev_builder": (
                        "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "A" * 64
                    ),
                },
            },
            None,
        ),
        (
            lambda value: {
                **value,
                "images": {
                    **value["images"],
                    "personal_dev_activation_agent": (
                        "ghcr.io/qianyi-sun/loom-personal-dev-activation-agent@sha256:" + "5" * 63
                    ),
                },
            },
            None,
        ),
        (
            lambda value: {
                **value,
                "images": {
                    **value["images"],
                    "minio": value["images"]["postgres"],
                },
            },
            None,
        ),
    ],
)
def test_trusted_release_rejects_invalid_contract(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], dict[str, Any]],
    expected_digest: str | None,
) -> None:
    path, digest = _write_release(tmp_path, mutate(_release()))

    with pytest.raises(PersonalDevTrustedReleaseError, match="trusted release is invalid"):
        load_personal_dev_trusted_release(
            path, digest if expected_digest is None else expected_digest
        )


@pytest.mark.parametrize(
    "payload",
    [
        _canonical(_release()) + b"\n",
        json.dumps(_release(), indent=2).encode("ascii"),
        b'{"schema_version":1,"schema_version":1}',
        b"not-json",
    ],
)
def test_trusted_release_rejects_noncanonical_payload(tmp_path: Path, payload: bytes) -> None:
    path, digest = _write_release(tmp_path, payload=payload)

    with pytest.raises(PersonalDevTrustedReleaseError, match="trusted release is invalid"):
        load_personal_dev_trusted_release(path, digest)


@pytest.mark.parametrize("digest", ["f" * 63, "F" * 64, "0" * 64])
def test_trusted_release_rejects_invalid_supplied_digest(
    tmp_path: Path,
    digest: str,
) -> None:
    path, _ = _write_release(tmp_path)

    with pytest.raises(PersonalDevTrustedReleaseError, match="trusted release is invalid"):
        load_personal_dev_trusted_release(path, digest)


def test_trusted_release_rejects_digest_mismatch(tmp_path: Path) -> None:
    path, _ = _write_release(tmp_path)

    with pytest.raises(PersonalDevTrustedReleaseError, match="trusted release is invalid"):
        load_personal_dev_trusted_release(path, "f" * 64)


def test_trusted_release_rejects_unsafe_file_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, digest = _write_release(tmp_path)
    path.chmod(0o640)
    with pytest.raises(PersonalDevTrustedReleaseError, match="trusted release is invalid"):
        load_personal_dev_trusted_release(path, digest)

    path.chmod(0o600)
    linked = tmp_path / "linked.json"
    os.link(path, linked)
    with pytest.raises(PersonalDevTrustedReleaseError, match="trusted release is invalid"):
        load_personal_dev_trusted_release(path, digest)
    linked.unlink()

    current_euid = os.geteuid()
    monkeypatch.setattr(config_module.os, "geteuid", lambda: current_euid + 1)
    with pytest.raises(PersonalDevTrustedReleaseError, match="trusted release is invalid"):
        load_personal_dev_trusted_release(path, digest)


def test_trusted_release_rejects_symlink_and_oversize_file(tmp_path: Path) -> None:
    target, digest = _write_release(tmp_path)
    link = tmp_path / "release-link.json"
    link.symlink_to(target)

    with pytest.raises(PersonalDevTrustedReleaseError, match="trusted release is invalid"):
        load_personal_dev_trusted_release(link, digest)

    target.write_bytes(b"x" * (1024 * 1024 + 1))
    target.chmod(0o600)
    with pytest.raises(PersonalDevTrustedReleaseError, match="trusted release is invalid"):
        load_personal_dev_trusted_release(
            target,
            hashlib.sha256(target.read_bytes()).hexdigest(),
        )
