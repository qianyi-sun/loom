from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
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
_LINEAGE_RENDER_INPUT_SHA256 = "f260f9de95aa6f38416bf49c561c9c1f1388911e2123ce25a6691529e161239b"
_LINEAGE_TRUSTED_RELEASE_SHA256 = "70e01346639855b45ca4d01203ba3d369afa544c8e8b59d7b34b599a305b5760"


def _scanner() -> dict[str, Any]:
    return {
        "binary_platform": "linux/amd64",
        "binary_sha256": "b" * 64,
        "cache_identity_sha256": (
            "b1c136b8577f3813c62588d6930db21b0f2343b7f70278836741387c43c33761"
        ),
        "database_metadata_sha256": "c" * 64,
        "database_sha256": "d" * 64,
        "java_database_metadata_sha256": "e" * 64,
        "java_database_sha256": "f" * 64,
        "lock_sha256": "1" * 64,
        "trivy_version": "v0.70.0",
    }


def _release() -> dict[str, Any]:
    return {
        "schema_version": 3,
        "source_sha": "1" * 40,
        "source_tree": "2" * 40,
        "images": {
            "loom_service": "ghcr.io/qianyi-sun/loom-service@sha256:" + "3" * 64,
            "loom_web": "ghcr.io/qianyi-sun/loom-web@sha256:" + "b" * 64,
            "personal_dev_builder": (
                "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "4" * 64
            ),
            "personal_dev_activation_agent": (
                "ghcr.io/qianyi-sun/loom-personal-dev-activation-agent@sha256:" + "5" * 64
            ),
            "personal_dev_scanner_cache": (
                "ghcr.io/qianyi-sun/loom-personal-dev-scanner-cache@sha256:" + "a" * 64
            ),
            "postgres": "docker.io/library/postgres@sha256:" + "6" * 64,
            "minio": "quay.io/minio/minio@sha256:" + "7" * 64,
            "minio_client": "quay.io/minio/mc@sha256:" + "9" * 64,
        },
        "scanner": _scanner(),
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


def _with_ingress_controller_source_cidrs(text: str, value: object) -> str:
    entry = "ingress_controller_source_cidrs = " + json.dumps(value) + "\n"
    lines = [
        line
        for line in text.splitlines()
        if not line.startswith("ingress_controller_source_cidrs = ")
    ]
    return "\n".join(lines).replace("[network]\n", "[network]\n" + entry, 1) + "\n"


def _with_kubernetes_api_endpoints(text: str, cidrs: object, port: object = 6443) -> str:
    entries = (
        "kubernetes_api_endpoint_cidrs = " + json.dumps(cidrs) + "\n"
        "kubernetes_api_endpoint_port = " + json.dumps(port) + "\n"
    )
    lines = [
        line
        for line in text.splitlines()
        if not line.startswith("kubernetes_api_endpoint_cidrs = ")
        and not line.startswith("kubernetes_api_endpoint_port = ")
    ]
    return "\n".join(lines).replace("[network]\n", "[network]\n" + entries, 1) + "\n"


def test_checked_in_shadow_profile_is_exact_and_canonical() -> None:
    profile = load_personal_dev_control_plane_profile(_PROFILE)

    assert profile.schema_version == 2
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
    assert profile.storage.lineage_render_input_sha256 == _LINEAGE_RENDER_INPUT_SHA256
    assert profile.storage.lineage_trusted_release_sha256 == _LINEAGE_TRUSTED_RELEASE_SHA256
    assert profile.builder.prepared is False
    assert profile.builder.runtime_class_name == "loom-personal-dev-builder"
    assert profile.builder.runtime_handler == "runsc-personal-dev"
    assert profile.builder.runtime_profile_sha256 == (
        "6ee2c283e5bf0783e192787522ea9550caadff4131590cc0a26dbf7dd2a6869b"
    )
    assert profile.builder.registry_prefix == "ghcr.io/qianyi-sun/loom-dev"
    assert profile.network.public_origin == "https://loom-service.dev.yylx.world"
    assert profile.network.kubernetes_api_cidr != "0.0.0.0/0"
    assert profile.network.kubernetes_api_port == 443
    assert profile.network.kubernetes_api_endpoint_cidrs == ("192.168.50.103/32",)
    assert profile.network.kubernetes_api_endpoint_port == 6443
    assert isinstance(profile.network.kubernetes_api_endpoint_cidrs, tuple)
    assert profile.network.ingress_controller_source_cidrs == ()
    assert isinstance(profile.network.ingress_controller_source_cidrs, tuple)
    assert profile.network.acme_http01_solver_port == 8089
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


def test_previous_profile_and_release_schemas_remain_loadable_for_rollback(
    tmp_path: Path,
) -> None:
    profile_path = _write_profile(
        tmp_path,
        lambda text: re.sub(
            r"\n\[resources\.web\]\n(?:[^\n]*\n){4}",
            "\n",
            text.replace("schema_version = 2\n", "schema_version = 1\n", 1),
            count=1,
        ),
    )
    release_value = _release()
    release_value["schema_version"] = 2
    del release_value["images"]["loom_web"]
    release_path, release_sha256 = _write_release(tmp_path, release_value)

    profile = load_personal_dev_control_plane_profile(profile_path)
    release = load_personal_dev_trusted_release(release_path, release_sha256)

    assert profile.schema_version == 1
    assert profile.resources.web is None
    assert release.schema_version == 2
    assert release.images.loom_web is None
    assert "web" not in profile.canonical_value()["resources"]
    assert "loom_web" not in release.canonical_value()["images"]


@pytest.mark.parametrize("schema_version", [1, 2])
def test_profile_rejects_web_resources_schema_mismatch(
    tmp_path: Path,
    schema_version: int,
) -> None:
    def mismatch(text: str) -> str:
        if schema_version == 1:
            return text.replace("schema_version = 2\n", "schema_version = 1\n", 1)
        return re.sub(
            r"\n\[resources\.web\]\n(?:[^\n]*\n){4}",
            "\n",
            text,
            count=1,
        )

    path = _write_profile(tmp_path, mismatch)

    with pytest.raises(
        ValidationError,
        match="personal-dev web resources do not match profile schema",
    ):
        load_personal_dev_control_plane_profile(path)


@pytest.mark.parametrize(
    ("schema_version", "include_web"),
    [(2, True), (3, False)],
)
def test_trusted_release_rejects_web_image_schema_mismatch(
    tmp_path: Path,
    schema_version: int,
    include_web: bool,
) -> None:
    release_value = _release()
    release_value["schema_version"] = schema_version
    if not include_web:
        del release_value["images"]["loom_web"]
    path, digest = _write_release(tmp_path, release_value)

    with pytest.raises(PersonalDevTrustedReleaseError):
        load_personal_dev_trusted_release(path, digest)


def test_profile_accepts_paired_storage_lineage(tmp_path: Path) -> None:
    render_input_sha256 = "a" * 64
    trusted_release_sha256 = "b" * 64
    path = _write_profile(
        tmp_path,
        lambda text: text.replace(_LINEAGE_RENDER_INPUT_SHA256, render_input_sha256, 1).replace(
            _LINEAGE_TRUSTED_RELEASE_SHA256, trusted_release_sha256, 1
        ),
    )

    profile = load_personal_dev_control_plane_profile(path)

    assert profile.storage.lineage_render_input_sha256 == render_input_sha256
    assert profile.storage.lineage_trusted_release_sha256 == trusted_release_sha256


@pytest.mark.parametrize(
    "lineage_entry",
    [
        'lineage_render_input_sha256 = "' + "a" * 64 + '"\n',
        'lineage_trusted_release_sha256 = "' + "b" * 64 + '"\n',
    ],
)
def test_profile_rejects_unpaired_storage_lineage(
    tmp_path: Path,
    lineage_entry: str,
) -> None:
    path = _write_profile(
        tmp_path,
        lambda text: (
            text.replace(
                f'lineage_render_input_sha256 = "{_LINEAGE_RENDER_INPUT_SHA256}"\n',
                "",
                1,
            )
            .replace(
                f'lineage_trusted_release_sha256 = "{_LINEAGE_TRUSTED_RELEASE_SHA256}"\n',
                "",
                1,
            )
            .replace("[storage]\n", "[storage]\n" + lineage_entry, 1)
        ),
    )

    with pytest.raises(ValidationError, match="storage lineage must be completely pinned"):
        load_personal_dev_control_plane_profile(path)


@pytest.mark.parametrize("invalid_digest", ["a" * 63, "A" * 64, "0" * 64])
def test_profile_rejects_invalid_storage_lineage_digest(
    tmp_path: Path,
    invalid_digest: str,
) -> None:
    path = _write_profile(
        tmp_path,
        lambda text: text.replace(
            _LINEAGE_RENDER_INPUT_SHA256,
            invalid_digest,
            1,
        ),
    )

    with pytest.raises(ValidationError, match="storage lineage digest is invalid"):
        load_personal_dev_control_plane_profile(path)


@pytest.mark.parametrize(
    "transform",
    [
        lambda text: text + "unknown_key = true\n",
        lambda text: text.replace("schema_version = 2\n", "", 1),
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
            'registry_prefix = "ghcr.io/qianyi-sun/loom-dev"',
            'registry_prefix = "ghcr..io/qianyi-sun/loom-dev"',
        ),
        lambda text: text.replace(
            'registry_prefix = "ghcr.io/qianyi-sun/loom-dev"',
            'registry_prefix = "ghcr.io:99999/qianyi-sun/loom-dev"',
        ),
        lambda text: text.replace(
            'capacity_manager_pod_label_key = "app.kubernetes.io/name"',
            'capacity_manager_pod_label_key = "/name"',
        ),
        lambda text: text.replace("acme_http01_solver_port = 8089", "acme_http01_solver_port = 0"),
        lambda text: text.replace(
            "acme_http01_solver_port = 8089", "acme_http01_solver_port = 8090"
        ),
        lambda text: text.replace(
            'kubernetes_api_cidr = "10.43.0.1/32"', 'kubernetes_api_cidr = "0.0.0.0/0"'
        ),
        lambda text: text.replace("kubernetes_api_port = 443", "kubernetes_api_port = 0"),
        lambda text: text.replace('postgres_storage = "20Gi"', 'postgres_storage = "unbounded"'),
        lambda text: text.replace("prepared = false", "prepared = true"),
        lambda text: text.replace('runtime_handler = "runsc-personal-dev"\n', ""),
        lambda text: text.replace(
            'runtime_handler = "runsc-personal-dev"',
            'runtime_handler = "runc"',
        ),
        lambda text: text.replace(
            "runtime_profile_sha256 = "
            '"6ee2c283e5bf0783e192787522ea9550caadff4131590cc0a26dbf7dd2a6869b"\n',
            "",
        ),
        lambda text: text.replace(
            "runtime_profile_sha256 = "
            '"6ee2c283e5bf0783e192787522ea9550caadff4131590cc0a26dbf7dd2a6869b"',
            'runtime_profile_sha256 = "D' + "d" * 63 + '"',
        ),
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


def test_profile_accepts_legacy_ingress_controller_source_for_rollback(
    tmp_path: Path,
) -> None:
    path = _write_profile(
        tmp_path,
        lambda text: _with_ingress_controller_source_cidrs(
            text,
            ["192.168.50.14/32"],
        ),
    )

    profile = load_personal_dev_control_plane_profile(path)

    assert profile.network.ingress_controller_source_cidrs == ("192.168.50.14/32",)
    assert isinstance(profile.network.ingress_controller_source_cidrs, tuple)


def test_profile_defaults_omitted_acme_solver_port_for_rollback_compatibility(
    tmp_path: Path,
) -> None:
    path = _write_profile(
        tmp_path,
        lambda text: text.replace("acme_http01_solver_port = 8089\n", "", 1),
    )

    profile = load_personal_dev_control_plane_profile(path)

    assert profile.network.acme_http01_solver_port == 8089


@pytest.mark.parametrize(
    "cidrs",
    [
        ["192.168.50.14/32", "192.168.50.14/32"],
        ["192.168.50.0/24"],
        ["8.8.8.8/32"],
        ["127.0.0.1/32"],
        ["169.254.1.1/32"],
        ["224.0.0.1/32"],
        ["0.0.0.0/32"],
        ["240.0.0.1/32"],
        ["192.168.50.14/32", True],
        ["192.168.50.14/32", 1921685015],
        [f"10.0.0.{number}/32" for number in range(1, 34)],
    ],
)
def test_profile_rejects_non_host_or_unsafe_ingress_controller_sources(
    tmp_path: Path,
    cidrs: list[object],
) -> None:
    path = _write_profile(
        tmp_path,
        lambda text: _with_ingress_controller_source_cidrs(text, cidrs),
    )

    with pytest.raises(ValidationError):
        load_personal_dev_control_plane_profile(path)


@pytest.mark.parametrize(
    "cidr",
    [
        "0.0.0.1/32",
        "198.18.0.1/32",
        "198.51.100.1/32",
        "2001:db8::1/128",
    ],
)
def test_profile_rejects_special_use_sources_that_is_private_would_accept(
    tmp_path: Path,
    cidr: str,
) -> None:
    """Replacing explicit private-use membership with is_private must fail this matrix."""

    assert ipaddress.ip_network(cidr, strict=True).network_address.is_private
    path = _write_profile(
        tmp_path,
        lambda text: _with_ingress_controller_source_cidrs(text, [cidr]),
    )

    with pytest.raises(ValidationError):
        load_personal_dev_control_plane_profile(path)


def test_profile_accepts_ipv6_ula_ingress_controller_source(tmp_path: Path) -> None:
    path = _write_profile(
        tmp_path,
        lambda text: _with_ingress_controller_source_cidrs(text, ["fd00::1/128"]),
    )

    profile = load_personal_dev_control_plane_profile(path)

    assert profile.network.ingress_controller_source_cidrs == ("fd00::1/128",)


@pytest.mark.parametrize(
    ("cidrs", "port"),
    [
        ([], 6443),
        (["192.168.50.103/32", "192.168.50.103/32"], 6443),
        (["192.168.50.104/32", "192.168.50.103/32"], 6443),
        (["192.168.50.0/24"], 6443),
        (["8.8.8.8/32"], 6443),
        (["0.0.0.0/32"], 6443),
        (["127.0.0.1/32"], 6443),
        (["169.254.1.1/32"], 6443),
        (["224.0.0.1/32"], 6443),
        (["240.0.0.1/32"], 6443),
        ([f"10.0.0.{number}/32" for number in range(1, 34)], 6443),
        (["192.168.50.103/32"], 0),
        (["192.168.50.103/32"], 65536),
    ],
)
def test_profile_rejects_unsafe_kubernetes_api_endpoints(
    tmp_path: Path,
    cidrs: list[str],
    port: int,
) -> None:
    path = _write_profile(
        tmp_path,
        lambda text: _with_kubernetes_api_endpoints(text, cidrs, port),
    )

    with pytest.raises(ValidationError):
        load_personal_dev_control_plane_profile(path)


@pytest.mark.parametrize("unsafe", ["symlink", "hardlink", "empty", "oversized"])
def test_profile_rejects_unstable_or_unbounded_file_identity(
    tmp_path: Path,
    unsafe: str,
) -> None:
    source = tmp_path / "profile.toml"
    source.write_bytes(_PROFILE.read_bytes())
    selected = source
    if unsafe == "symlink":
        selected = tmp_path / "linked-profile.toml"
        selected.symlink_to(source)
    elif unsafe == "hardlink":
        selected = tmp_path / "hardlinked-profile.toml"
        os.link(source, selected)
    elif unsafe == "empty":
        source.write_bytes(b"")
    else:
        source.write_bytes(b"x" * (1024 * 1024 + 1))

    with pytest.raises(ValueError, match="profile is invalid"):
        load_personal_dev_control_plane_profile(selected)


def test_trusted_release_loads_exact_owner_only_canonical_bytes(tmp_path: Path) -> None:
    path, digest = _write_release(tmp_path)

    release = load_personal_dev_trusted_release(path, digest)

    assert release.source_sha == "1" * 40
    assert release.source_tree == "2" * 40
    assert release.images.loom_service == ("ghcr.io/qianyi-sun/loom-service@sha256:" + "3" * 64)
    assert release.images.personal_dev_builder.endswith("4" * 64)
    assert release.images.personal_dev_activation_agent.endswith("5" * 64)
    assert release.images.personal_dev_scanner_cache.endswith("a" * 64)
    assert release.images.postgres.endswith("6" * 64)
    assert release.images.minio.endswith("7" * 64)
    assert release.images.minio_client.endswith("9" * 64)
    assert release.scanner.binary_platform == "linux/amd64"
    assert release.scanner.binary_sha256 == "b" * 64
    assert release.scanner.cache_identity_sha256 == (
        "b1c136b8577f3813c62588d6930db21b0f2343b7f70278836741387c43c33761"
    )
    assert release.scanner.database_metadata_sha256 == "c" * 64
    assert release.scanner.database_sha256 == "d" * 64
    assert release.scanner.java_database_metadata_sha256 == "e" * 64
    assert release.scanner.java_database_sha256 == "f" * 64
    assert release.scanner.lock_sha256 == "1" * 64
    assert release.scanner.trivy_version == "v0.70.0"
    assert release.release_evidence_sha256 == "8" * 64
    assert release.canonical_bytes() == path.read_bytes()


def test_trusted_release_repeatedly_loads_exact_open_proc_descriptor(
    tmp_path: Path,
) -> None:
    path, digest = _write_release(tmp_path)
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
    try:
        first = load_personal_dev_trusted_release(
            Path(f"/proc/self/fd/{descriptor}"),
            digest,
        )
        second = load_personal_dev_trusted_release(
            Path(f"/proc/self/fd/{descriptor}"),
            digest,
        )
    finally:
        os.close(descriptor)

    assert first == second
    assert first.source_sha == "1" * 40
    assert first.source_tree == "2" * 40
    assert first.canonical_bytes() == path.read_bytes()


def test_trusted_release_rejects_unrepresentable_proc_descriptor(tmp_path: Path) -> None:
    _, digest = _write_release(tmp_path)
    unrepresentable = Path("/proc/self/fd/" + "9" * 100)

    with pytest.raises(PersonalDevTrustedReleaseError, match="trusted release is invalid"):
        load_personal_dev_trusted_release(unrepresentable, digest)


@pytest.mark.parametrize(
    ("mutate", "expected_digest"),
    [
        (lambda value: {**value, "unknown": True}, None),
        (lambda value: {key: item for key, item in value.items() if key != "source_tree"}, None),
        (lambda value: {key: item for key, item in value.items() if key != "scanner"}, None),
        (lambda value: {**value, "schema_version": 1}, None),
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
                    key: item
                    for key, item in value["images"].items()
                    if key != "personal_dev_scanner_cache"
                },
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
        (
            lambda value: {
                **value,
                "images": {
                    **value["images"],
                    "personal_dev_scanner_cache": (
                        "ghcr.io/other/loom-personal-dev-scanner-cache@sha256:" + "a" * 64
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
                    "personal_dev_scanner_cache": (
                        "ghcr.io/qianyi-sun/loom-personal-dev-scanner-cache@sha256:" + "3" * 64
                    ),
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


@pytest.mark.parametrize("field", list(_scanner()))
def test_trusted_release_requires_every_scanner_field(
    tmp_path: Path,
    field: str,
) -> None:
    value = _release()
    value["scanner"].pop(field)
    path, digest = _write_release(tmp_path, value)

    with pytest.raises(PersonalDevTrustedReleaseError, match="trusted release is invalid"):
        load_personal_dev_trusted_release(path, digest)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda scanner: scanner.update(unexpected=True),
        lambda scanner: scanner.update(binary_platform="linux/arm64"),
        lambda scanner: scanner.update(trivy_version="v0.70.1"),
        lambda scanner: scanner.update(binary_sha256="B" * 64),
        lambda scanner: scanner.update(lock_sha256="0" * 64),
        lambda scanner: scanner.update(database_sha256="0" * 64),
        lambda scanner: scanner.update(cache_identity_sha256="f" * 64),
    ],
)
def test_trusted_release_rejects_scanner_drift(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], object],
) -> None:
    value = _release()
    mutate(value["scanner"])
    path, digest = _write_release(tmp_path, value)

    with pytest.raises(PersonalDevTrustedReleaseError, match="trusted release is invalid"):
        load_personal_dev_trusted_release(path, digest)


@pytest.mark.parametrize(
    "payload",
    [
        _canonical(_release()) + b"\n",
        json.dumps(_release(), indent=2).encode("ascii"),
        b'{"schema_version":3,"schema_version":3}',
        b"not-json",
    ],
)
def test_trusted_release_rejects_noncanonical_payload(tmp_path: Path, payload: bytes) -> None:
    path, digest = _write_release(tmp_path, payload=payload)

    with pytest.raises(PersonalDevTrustedReleaseError, match="trusted release is invalid"):
        load_personal_dev_trusted_release(path, digest)


def test_trusted_release_rejects_pathologically_deep_json(tmp_path: Path) -> None:
    payload = b"[" * 2_000 + b"0" + b"]" * 2_000
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
