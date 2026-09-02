from __future__ import annotations

import argparse
import hashlib
import importlib
import io
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from loom.personal_dev_acceptance_evidence import (
    build_personal_dev_scanner_finding_policy,
    build_personal_dev_trusted_launcher_profile,
)
from loom.personal_dev_control_plane_config import (
    load_personal_dev_acceptance_plan,
    load_personal_dev_control_plane_profile,
    load_personal_dev_operational_plan,
    load_personal_dev_trusted_release,
)
from loom.personal_dev_control_plane_render import (
    render_acceptance_personal_dev_control_plane,
    render_operational_personal_dev_control_plane,
    render_shadow_personal_dev_control_plane,
)
from loom.personal_dev_control_plane_status import (
    PersonalDevAcceptanceStatus,
    PersonalDevOperationalStatus,
    PersonalDevShadowComponent,
    PersonalDevShadowStatus,
)
from loom.personal_dev_minio_backup import PersonalDevMinioManifest
from loom_cli.__main__ import main
from loom_cli.admin_cmd import dispatch
from loom_cli.personal_dev_minio_backup_cmd import PersonalDevMinioCommandResult
from tests.unit.test_personal_dev_acceptance_evidence import (
    _native_result_plan,
    _native_result_value,
    _result_plan,
    _result_value,
    _rollback_shadow_manifest_payload,
    _rollback_shadow_status_value,
)

_ROOT = Path(__file__).resolve().parents[2]
_PROFILE = _ROOT / "deploy/dev-fleet/personal-dev-control-plane.toml"
_NOW = datetime(2026, 8, 17, 21, 0, 0, tzinfo=UTC)


def _synthetic_checkout_env(**overrides: str) -> dict[str, str]:
    env = os.environ.copy()
    for key in tuple(env):
        if key.startswith("COV_CORE_"):
            del env[key]
    env.update(overrides)
    return env


def _git_identity() -> tuple[str, str]:
    def git(*arguments: str) -> str:
        return subprocess.run(
            ["git", *arguments],
            cwd=_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    return git("rev-parse", "HEAD"), git("rev-parse", "HEAD^{tree}")


_SOURCE_SHA, _SOURCE_TREE = _git_identity()


def _release_value() -> dict[str, object]:
    return {
        "schema_version": 4,
        "source_sha": _SOURCE_SHA,
        "source_tree": _SOURCE_TREE,
        "images": {
            "loom_service": "ghcr.io/qianyi-sun/loom-service@sha256:" + "3" * 64,
            "loom_web": "ghcr.io/qianyi-sun/loom-web@sha256:" + "b" * 64,
            "personal_dev_builder": (
                "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "4" * 64
            ),
            "personal_dev_activation_agent": (
                "ghcr.io/qianyi-sun/loom-personal-dev-activation-agent@sha256:" + "5" * 64
            ),
            "personal_dev_native_builder_agent": (
                "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:" + "c" * 64
            ),
            "personal_dev_scanner_cache": (
                "ghcr.io/qianyi-sun/loom-personal-dev-scanner-cache@sha256:" + "a" * 64
            ),
            "postgres": "docker.io/library/postgres@sha256:" + "6" * 64,
            "minio": "quay.io/minio/minio@sha256:" + "7" * 64,
            "minio_client": "quay.io/minio/mc@sha256:" + "9" * 64,
        },
        "scanner": {
            "binary_platform": "linux/amd64",
            "binary_sha256": "b" * 64,
            "cache_identity_sha256": (
                "35f7d0f279b656552b1eb362a0599938ff112e5103590dcfc0eece25e8326082"
            ),
            "database_metadata_sha256": "c" * 64,
            "database_sha256": "d" * 64,
            "java_database_metadata_sha256": "e" * 64,
            "java_database_sha256": "f" * 64,
            "lock_sha256": "1" * 64,
            "trivy_version": "v0.74.0",
        },
        "release_evidence_sha256": "8" * 64,
    }


def _release(tmp_path: Path) -> tuple[Path, str]:
    payload = json.dumps(
        _release_value(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    path = tmp_path / "trusted-release.json"
    path.write_bytes(payload)
    path.chmod(0o600)
    return path, hashlib.sha256(payload).hexdigest()


def _prepared_profile(tmp_path: Path, *, source: Path = _PROFILE) -> Path:
    value = source.read_text(encoding="utf-8")
    inert = (
        "prepared = false\n"
        'agent_instance_id = ""\n'
        'agent_key_id = ""\n'
        'public_key_sha256 = ""\n'
        'host_name = ""\n'
        'runtime_profile_sha256 = ""\n'
        'public_store_origin = ""\n'
        "public_store_endpoint_cidrs = []\n"
        'provider = "gb10-gvisor-docker-v1"'
    )
    prepared = (
        "prepared = true\n"
        'agent_instance_id = "10000000-0000-0000-0000-000000000001"\n'
        'agent_key_id = "gb10-native-builder-v1"\n'
        f'public_key_sha256 = "{"d" * 64}"\n'
        'host_name = "gx10-01c7"\n'
        f'runtime_profile_sha256 = "{"e" * 64}"\n'
        'public_store_origin = "https://objects.dev.yylx.world"\n'
        'public_store_endpoint_cidrs = ["207.35.188.227/32"]\n'
        'provider = "gb10-gvisor-docker-v1"'
    )
    assert inert in value
    path = tmp_path / "prepared-personal-dev-control-plane.toml"
    path.write_text(value.replace(inert, prepared, 1), encoding="utf-8")
    path.chmod(0o600)
    return path


def _acceptance_plan(
    tmp_path: Path,
    release_path: Path,
    release_digest: str,
    *,
    profile_path: Path,
):
    profile = load_personal_dev_control_plane_profile(profile_path)
    release = load_personal_dev_trusted_release(release_path, release_digest)
    assert profile.native_builder is not None
    assert profile.native_builder.prepared
    shadow = render_shadow_personal_dev_control_plane(profile, release)
    protocol_sha256 = hashlib.sha256(
        json.dumps(
            dict(profile.protocol_versions),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    ).hexdigest()
    launcher_profile = build_personal_dev_trusted_launcher_profile(
        profile=profile,
        release=release,
        source_root=_ROOT,
    )
    scanner_policy = build_personal_dev_scanner_finding_policy(
        profile=profile,
        release=release,
        source_root=_ROOT,
    )
    launcher_payload = json.dumps(launcher_profile, sort_keys=True, separators=(",", ":")).encode(
        "ascii"
    )
    scanner_payload = json.dumps(scanner_policy, sort_keys=True, separators=(",", ":")).encode(
        "ascii"
    )
    launcher_path = tmp_path / "trusted-launcher-profile.json"
    scanner_path = tmp_path / "scanner-finding-policy.json"
    launcher_path.write_bytes(launcher_payload)
    scanner_path.write_bytes(scanner_payload)
    launcher_path.chmod(0o600)
    scanner_path.chmod(0o600)
    backup_value = {
        "cleanup": {
            "isolated_minio_absent": True,
            "isolated_network_absent": True,
            "isolated_postgres_absent": True,
        },
        "completed_at": "2026-08-17T20:55:00Z",
        "manager": {
            "executable_new_capacity_ceiling": 0,
            "personal_worker_count": 0,
        },
        "minio": {
            "backup_manifest_sha256": "a" * 64,
            "image": release.images.minio,
            "restored_manifest_sha256": "a" * 64,
            "restored_object_count": 0,
            "source_object_count": 0,
        },
        "namespace": "loom-dev",
        "postgres": {
            "dump_sha256": "b" * 64,
            "image": release.images.postgres,
            "restored_schema_head": "0128",
            "restored_state_sha256": "c" * 64,
            "source_schema_head": "0128",
            "source_state_sha256": "c" * 64,
        },
        "release_sha256": release_digest,
        "schema": "loom-personal-dev-backup-restore-evidence-v1",
        "secrets": {
            "key_inventory_sha256": "d" * 64,
            "values_included": False,
        },
        "source": {"commit": release.source_sha, "tree": release.source_tree},
        "started_at": "2026-08-17T20:45:00Z",
        "storage": {
            "minio_pvc": "data-loom-dev-minio-0",
            "postgres_pvc": "data-loom-dev-postgres-0",
            "storage_class": "longhorn",
        },
    }
    backup_payload = json.dumps(backup_value, sort_keys=True, separators=(",", ":")).encode("ascii")
    backup_path = tmp_path / "backup-restore-evidence.json"
    backup_path.write_bytes(backup_payload)
    backup_path.chmod(0o600)
    value = {
        "acceptance_owners": [
            {
                "team_id": "00000000-0000-0000-0000-000000000201",
                "user_id": "00000000-0000-0000-0000-000000000301",
            },
            {
                "team_id": "00000000-0000-0000-0000-000000000202",
                "user_id": "00000000-0000-0000-0000-000000000302",
            },
        ],
        "activation": {
            "key_id": "personal-dev-agent-v1",
            "public_key_sha256": "c" * 64,
        },
        "builder": {
            "protocol_map_sha256": protocol_sha256,
            "publisher_identity": profile.builder.publisher_identity,
            "registry_prefix": profile.builder.registry_prefix,
            "runtime_class_name": profile.builder.runtime_class_name,
            "runtime_handler": profile.builder.runtime_handler,
            "runtime_profile_sha256": profile.builder.runtime_profile_sha256,
            "scanner_binary_sha256": release.scanner.binary_sha256,
            "scanner_cache_identity_sha256": release.scanner.cache_identity_sha256,
            "scanner_database_sha256": release.scanner.database_sha256,
            "scanner_database_metadata_sha256": (release.scanner.database_metadata_sha256),
            "scanner_finding_policy_sha256": hashlib.sha256(scanner_payload).hexdigest(),
            "scanner_java_database_sha256": release.scanner.java_database_sha256,
            "scanner_java_database_metadata_sha256": (
                release.scanner.java_database_metadata_sha256
            ),
            "trusted_launcher_profile_sha256": hashlib.sha256(launcher_payload).hexdigest(),
        },
        "manager": {
            "authority_incarnation": "00000000-0000-0000-0000-000000000101",
            "configuration_epoch": 7,
            "executable_new_capacity_ceiling": 0,
            "execution_epoch": 11,
            "execution_state": "prepared",
        },
        "native_builder": {
            "agent_instance_id": profile.native_builder.agent_instance_id,
            "agent_key_id": profile.native_builder.agent_key_id,
            "public_key_sha256": profile.native_builder.public_key_sha256,
            "host_name": profile.native_builder.host_name,
            "host_boot_id": "20000000-0000-0000-0000-000000000001",
            "runtime_profile_sha256": profile.native_builder.runtime_profile_sha256,
            "public_store_origin": profile.native_builder.public_store_origin,
            "public_store_endpoint_cidrs": list(profile.native_builder.public_store_endpoint_cidrs),
            "provider": profile.native_builder.provider,
            "platform": profile.native_builder.platform,
            "protocol_version": profile.native_builder.protocol_version,
            "freshness_seconds": profile.native_builder.freshness_seconds,
            "max_concurrency": profile.native_builder.max_concurrency,
        },
        "principals": {
            "lifecycle_principal_id": "personal-dev-lifecycle",
            "reporter_principal_id": "personal-dev-reporter",
        },
        "quotas": {
            "builder_global_concurrency": profile.limits.builder_global_concurrency,
            "builder_per_owner_concurrency": profile.limits.builder_per_owner_concurrency,
            "candidate_retained_bytes": profile.limits.candidate_retained_bytes,
            "candidate_retained_count": profile.limits.candidate_retained_count,
            "global_live_instances": profile.limits.global_live_instances,
            "per_owner_aggregate_max_slots": profile.limits.per_owner_aggregate_max_slots,
            "per_owner_aggregate_min_slots": profile.limits.per_owner_aggregate_min_slots,
            "per_owner_live_instances": profile.limits.per_owner_live_instances,
            "source_max_archive_bytes": profile.limits.source_max_archive_bytes,
        },
        "release": {
            "images": release.canonical_value()["images"],
            "release_evidence_sha256": release.release_evidence_sha256,
            "shadow_manifest_sha256": hashlib.sha256(shadow.yaml_text.encode("utf-8")).hexdigest(),
            "trusted_release_sha256": hashlib.sha256(release.canonical_bytes()).hexdigest(),
        },
        "schema_version": 3,
        "source": {"commit": release.source_sha, "tree": release.source_tree},
        "storage": {
            "backup_restore_evidence_sha256": hashlib.sha256(backup_payload).hexdigest(),
            "schema_head": "0128",
        },
        "window": {
            "expires_at": "2099-12-31T23:00:00Z",
            "rollback_expires_at": "2100-01-31T23:00:00Z",
            "started_at": "2026-01-01T00:00:00Z",
        },
    }
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    path = tmp_path / "acceptance-plan.json"
    path.write_bytes(payload)
    path.chmod(0o600)
    digest = hashlib.sha256(payload).hexdigest()
    return path, digest, load_personal_dev_acceptance_plan(path, digest)


def _operational_plan(
    tmp_path: Path,
    release_path: Path,
    release_digest: str,
    *,
    profile_path: Path,
):
    _path, _digest, acceptance = _acceptance_plan(
        tmp_path,
        release_path,
        release_digest,
        profile_path=profile_path,
    )
    value = acceptance.canonical_value()
    value.pop("acceptance_owners")
    value.pop("window")
    value["schema_version"] = 2
    value["approval"] = {
        "acceptance_result_sha256": "4" * 64,
        "approved_at": "2026-08-17T20:00:00Z",
        "rollback_evidence_sha256": "5" * 64,
    }
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    path = tmp_path / "operational-plan.json"
    path.write_bytes(payload)
    path.chmod(0o600)
    digest = hashlib.sha256(payload).hexdigest()
    return path, digest, load_personal_dev_operational_plan(path, digest)


def _reviewed_kubeconfig(tmp_path: Path) -> Path:
    path = tmp_path / "reviewed-kubeconfig"
    path.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v1",
                "clusters": [
                    {
                        "cluster": {
                            "certificate-authority-data": "cmV2aWV3ZWQtY2E=",
                            "server": "https://127.0.0.1:6443",
                        },
                        "name": "reviewed",
                    }
                ],
                "contexts": [
                    {
                        "context": {"cluster": "reviewed", "user": "reviewed"},
                        "name": "reviewed-loom-dev",
                    }
                ],
                "current-context": "reviewed-loom-dev",
                "kind": "Config",
                "preferences": {},
                "users": [{"name": "reviewed", "user": {"token": "reviewed-token"}}],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path


def _argv(release: Path, digest: str, *, profile: Path = _PROFILE) -> list[str]:
    return [
        "personal-dev-control-plane",
        "render",
        "--file",
        str(profile),
        "--trusted-release-file",
        str(release),
        "--trusted-release-sha256",
        digest,
    ]


def _status_argv(
    release: Path,
    digest: str,
    kubeconfig: Path,
    *,
    profile: Path = _PROFILE,
) -> list[str]:
    return [
        "personal-dev-control-plane",
        "status",
        "--namespace",
        "loom-dev",
        "--kubeconfig",
        str(kubeconfig),
        "--file",
        str(profile),
        "--trusted-release-file",
        str(release),
        "--trusted-release-sha256",
        digest,
    ]


def _capture_minio_argv(kubeconfig: Path, tmp_path: Path) -> list[str]:
    return [
        "personal-dev-control-plane",
        "capture-minio-backup",
        "--namespace",
        "loom-dev",
        "--kubeconfig",
        str(kubeconfig),
        "--source-manifest-file",
        str(tmp_path / "minio.source.json"),
        "--payload-root",
        str(tmp_path / "payloads"),
    ]


def _restore_minio_argv(
    release: Path,
    digest: str,
    tmp_path: Path,
) -> list[str]:
    suffix = digest[:12]
    return [
        "personal-dev-control-plane",
        "restore-minio-backup",
        "--trusted-release-file",
        str(release),
        "--trusted-release-sha256",
        digest,
        "--source-manifest-file",
        str(tmp_path / "minio.source.json"),
        "--payload-root",
        str(tmp_path / "payloads"),
        "--restored-manifest-file",
        str(tmp_path / "minio.restored.json"),
        "--restore-env-file",
        str(tmp_path / "restore.env"),
        "--isolated-minio-name",
        f"loom-personal-dev-minio-restore-{suffix}",
        "--isolated-network-name",
        f"loom-personal-dev-restore-{suffix}",
    ]


def _isolated_docker_documents(
    *,
    minio_image: str,
    minio_name: str,
    network_name: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    container_id = "a" * 64
    network_id = "b" * 64
    container = [
        {
            "Config": {"Image": minio_image},
            "HostConfig": {"NetworkMode": network_name, "PortBindings": {}},
            "Id": container_id,
            "Name": f"/{minio_name}",
            "NetworkSettings": {
                "Networks": {
                    network_name: {
                        "Aliases": [container_id[:12], "minio-restore"],
                        "DriverOpts": None,
                        "EndpointID": "c" * 64,
                        "Gateway": "172.31.0.1",
                        "GlobalIPv6Address": "",
                        "GlobalIPv6PrefixLen": 0,
                        "GwPriority": 0,
                        "IPAMConfig": None,
                        "IPAddress": "172.31.0.2",
                        "IPPrefixLen": 16,
                        "IPv6Gateway": "",
                        "Links": None,
                        "MacAddress": "02:42:ac:1f:00:02",
                        "NetworkID": network_id,
                    }
                },
                "Ports": {"9000/tcp": None, "9001/tcp": None},
            },
            "State": {"Running": True, "Status": "running"},
        }
    ]
    network = [
        {
            "Attachable": False,
            "Containers": {
                container_id: {
                    "EndpointID": "c" * 64,
                    "IPv4Address": "172.31.0.2/16",
                    "IPv6Address": "",
                    "MacAddress": "02:42:ac:1f:00:02",
                    "Name": minio_name,
                }
            },
            "Driver": "bridge",
            "EnableIPv4": True,
            "EnableIPv6": False,
            "Id": network_id,
            "Ingress": False,
            "Internal": True,
            "Labels": {},
            "Name": network_name,
            "Options": {},
            "Scope": "local",
        }
    ]
    return container, network


def _fake_docker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    container_document: list[dict[str, object]],
    network_document: list[dict[str, object]],
    stdout: bytes = b"bounded",
    stderr: bytes = b"",
    returncode: int = 0,
) -> Path:
    executable = tmp_path / "docker"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import json\n"
        "import os\n"
        "import pathlib\n"
        "import sys\n"
        "argv = sys.argv[1:]\n"
        "with pathlib.Path(os.environ['LOOM_TEST_DOCKER_LOG']).open('a', encoding='utf-8') as stream:\n"
        "    stream.write(json.dumps(argv, separators=(',', ':')) + '\\n')\n"
        "if argv and argv[0] == 'inspect':\n"
        "    sys.stdout.write(os.environ['LOOM_TEST_CONTAINER_INSPECT'])\n"
        "elif argv[:2] == ['network', 'inspect']:\n"
        "    sys.stdout.write(os.environ['LOOM_TEST_NETWORK_INSPECT'])\n"
        "elif argv and argv[0] == 'run':\n"
        "    sys.stdout.buffer.write(bytes.fromhex(os.environ['LOOM_TEST_DOCKER_STDOUT']))\n"
        "    sys.stderr.buffer.write(bytes.fromhex(os.environ['LOOM_TEST_DOCKER_STDERR']))\n"
        "    raise SystemExit(int(os.environ['LOOM_TEST_DOCKER_RETURNCODE']))\n"
        "else:\n"
        "    raise SystemExit(91)\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    log = tmp_path / "docker.calls.jsonl"
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("LOOM_TEST_DOCKER_LOG", str(log))
    monkeypatch.setenv(
        "LOOM_TEST_CONTAINER_INSPECT",
        json.dumps(container_document, sort_keys=True, separators=(",", ":")),
    )
    monkeypatch.setenv(
        "LOOM_TEST_NETWORK_INSPECT",
        json.dumps(network_document, sort_keys=True, separators=(",", ":")),
    )
    monkeypatch.setenv("LOOM_TEST_DOCKER_STDOUT", stdout.hex())
    monkeypatch.setenv("LOOM_TEST_DOCKER_STDERR", stderr.hex())
    monkeypatch.setenv("LOOM_TEST_DOCKER_RETURNCODE", str(returncode))
    return log


def _acceptance_argv(
    release: Path,
    digest: str,
    plan: Path,
    plan_digest: str,
    *,
    profile: Path = _PROFILE,
) -> list[str]:
    return [
        "personal-dev-control-plane",
        "render-acceptance",
        "--file",
        str(profile),
        "--trusted-release-file",
        str(release),
        "--trusted-release-sha256",
        digest,
        "--acceptance-plan-file",
        str(plan),
        "--acceptance-plan-sha256",
        plan_digest,
        "--source-root",
        str(_ROOT),
        "--trusted-launcher-profile-file",
        str(plan.parent / "trusted-launcher-profile.json"),
        "--scanner-finding-policy-file",
        str(plan.parent / "scanner-finding-policy.json"),
        "--backup-restore-evidence-file",
        str(plan.parent / "backup-restore-evidence.json"),
    ]


def _acceptance_status_argv(
    release: Path,
    digest: str,
    plan: Path,
    plan_digest: str,
    kubeconfig: Path,
    *,
    profile: Path = _PROFILE,
) -> list[str]:
    return [
        "personal-dev-control-plane",
        "status-acceptance",
        "--namespace",
        "loom-dev",
        "--kubeconfig",
        str(kubeconfig),
        "--file",
        str(profile),
        "--trusted-release-file",
        str(release),
        "--trusted-release-sha256",
        digest,
        "--acceptance-plan-file",
        str(plan),
        "--acceptance-plan-sha256",
        plan_digest,
        "--source-root",
        str(_ROOT),
        "--trusted-launcher-profile-file",
        str(plan.parent / "trusted-launcher-profile.json"),
        "--scanner-finding-policy-file",
        str(plan.parent / "scanner-finding-policy.json"),
        "--backup-restore-evidence-file",
        str(plan.parent / "backup-restore-evidence.json"),
    ]


def _operational_argv(
    release: Path,
    digest: str,
    plan: Path,
    plan_digest: str,
    *,
    profile: Path = _PROFILE,
) -> list[str]:
    return [
        "personal-dev-control-plane",
        "render-operational",
        "--file",
        str(profile),
        "--trusted-release-file",
        str(release),
        "--trusted-release-sha256",
        digest,
        "--operational-plan-file",
        str(plan),
        "--operational-plan-sha256",
        plan_digest,
        "--source-root",
        str(_ROOT),
        "--trusted-launcher-profile-file",
        str(plan.parent / "trusted-launcher-profile.json"),
        "--scanner-finding-policy-file",
        str(plan.parent / "scanner-finding-policy.json"),
        "--backup-restore-evidence-file",
        str(plan.parent / "backup-restore-evidence.json"),
    ]


def _operational_status_argv(
    release: Path,
    digest: str,
    plan: Path,
    plan_digest: str,
    kubeconfig: Path,
    *,
    profile: Path = _PROFILE,
) -> list[str]:
    return [
        "personal-dev-control-plane",
        "status-operational",
        "--namespace",
        "loom-dev",
        "--kubeconfig",
        str(kubeconfig),
        "--file",
        str(profile),
        "--trusted-release-file",
        str(release),
        "--trusted-release-sha256",
        digest,
        "--operational-plan-file",
        str(plan),
        "--operational-plan-sha256",
        plan_digest,
        "--source-root",
        str(_ROOT),
        "--trusted-launcher-profile-file",
        str(plan.parent / "trusted-launcher-profile.json"),
        "--scanner-finding-policy-file",
        str(plan.parent / "scanner-finding-policy.json"),
        "--backup-restore-evidence-file",
        str(plan.parent / "backup-restore-evidence.json"),
    ]


def _verify_acceptance_result_argv(
    plan_path: Path,
    plan_sha256: str,
    result_path: Path,
    result_sha256: str,
    manifest_sha256: str,
    rollback_shadow_manifest_path: Path,
    rollback_shadow_status_path: Path,
) -> list[str]:
    return [
        "personal-dev-control-plane",
        "verify-acceptance-result",
        "--acceptance-plan-file",
        str(plan_path),
        "--acceptance-plan-sha256",
        plan_sha256,
        "--acceptance-result-file",
        str(result_path),
        "--acceptance-result-sha256",
        result_sha256,
        "--acceptance-manifest-sha256",
        manifest_sha256,
        "--rollback-shadow-manifest-file",
        str(rollback_shadow_manifest_path),
        "--rollback-shadow-status-file",
        str(rollback_shadow_status_path),
    ]


def _write_canonical_owner_only(path: Path, value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    path.write_bytes(payload)
    path.chmod(0o600)
    return hashlib.sha256(payload).hexdigest()


def _rewrite_acceptance_result_rollback_digest(
    result_path: Path,
    rollback_sha256: str,
) -> str:
    value = json.loads(result_path.read_text(encoding="ascii"))
    value["status_sha256s"]["rollback_shadow"] = rollback_sha256
    return _write_canonical_owner_only(result_path, value)


def _acceptance_result_files(
    tmp_path: Path,
    *,
    native: bool = False,
) -> tuple[Path, str, Path, str, Path, Path, str]:
    if native:
        plan = _native_result_plan(tmp_path)
        plan_path = tmp_path / "native-result-plan" / "acceptance-plan.json"
    else:
        plan, _v1_plan = _result_plan(tmp_path)
        plan_path = tmp_path / "result-plan" / "acceptance-plan.json"
    rollback_input_sha256 = "1" * 64
    rollback_manifest_path = tmp_path / "rollback-shadow.yaml"
    rollback_manifest_payload = _rollback_shadow_manifest_payload(
        input_sha256=rollback_input_sha256,
        release_sha256=plan.release.trusted_release_sha256,
    )
    rollback_manifest_path.write_bytes(rollback_manifest_payload)
    rollback_manifest_path.chmod(0o600)
    plan_value = plan.canonical_value()
    plan_value["release"]["shadow_manifest_sha256"] = hashlib.sha256(  # type: ignore[index]
        rollback_manifest_payload
    ).hexdigest()
    plan_sha256 = _write_canonical_owner_only(plan_path, plan_value)
    plan = load_personal_dev_acceptance_plan(plan_path, plan_sha256)
    rollback_value = _rollback_shadow_status_value()
    rollback_value["input_sha256"] = rollback_input_sha256
    rollback_value["release_sha256"] = plan.release.trusted_release_sha256
    rollback_path = tmp_path / "rollback-shadow.status.json"
    rollback_sha256 = _write_canonical_owner_only(rollback_path, rollback_value)
    result_value = _native_result_value(plan) if native else _result_value(plan)
    result_value["status_sha256s"]["rollback_shadow"] = rollback_sha256
    result_path = tmp_path / "acceptance-result.json"
    result_sha256 = _write_canonical_owner_only(result_path, result_value)
    return (
        plan_path,
        plan.sha256,
        result_path,
        result_sha256,
        rollback_manifest_path,
        rollback_path,
        rollback_sha256,
    )


@pytest.mark.parametrize("native", [False, True])
def test_verify_acceptance_result_emits_canonical_secret_free_projection(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    native: bool,
) -> None:
    (
        plan_path,
        plan_sha256,
        result_path,
        result_sha256,
        rollback_manifest_path,
        rollback_path,
        rollback_sha256,
    ) = _acceptance_result_files(tmp_path, native=native)
    plan = load_personal_dev_acceptance_plan(plan_path, plan_sha256)

    rc = dispatch(
        _verify_acceptance_result_argv(
            plan_path,
            plan_sha256,
            result_path,
            result_sha256,
            "a" * 64,
            rollback_manifest_path,
            rollback_path,
        )
    )

    captured = capsys.readouterr()
    assert rc == 0
    assert captured.err == ""
    record = json.loads(captured.out)
    assert (
        captured.out
        == json.dumps(
            record,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        + "\n"
    )
    assert record == {
        "acceptance_manifest_sha256": "a" * 64,
        "acceptance_plan_sha256": plan_sha256,
        "acceptance_result_sha256": result_sha256,
        "cross_owner_denial_count": 6,
        "native": native,
        "owner_count": 2,
        "release_sha256": plan.release.trusted_release_sha256,
        "rollback_shadow_status_sha256": rollback_sha256,
        "schema": "loom-personal-dev-zero-capacity-acceptance-verification-v1",
        "shadow_manifest_sha256": plan.release.shadow_manifest_sha256,
        "verified": True,
    }


@pytest.mark.parametrize(
    "invalid_input",
    [
        "unsafe-plan",
        "unsafe-result",
        "unsafe-rollback-manifest",
        "unsafe-rollback",
        "v1-plan",
        "wrong-plan-sha",
        "wrong-result-sha",
        "wrong-manifest",
        "wrong-rollback-manifest-digest",
        "wrong-rollback-digest",
        "rollback-status-input-drift",
        "invalid-rollback",
        "wrong-rollback-release",
        "invalid-result",
    ],
)
def test_verify_acceptance_result_rejects_invalid_inputs_before_kubernetes_runner(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    invalid_input: str,
) -> None:
    (
        plan_path,
        plan_sha256,
        result_path,
        result_sha256,
        rollback_manifest_path,
        rollback_path,
        _rollback_sha256,
    ) = _acceptance_result_files(tmp_path)
    manifest_sha256 = "a" * 64
    if invalid_input == "unsafe-plan":
        plan_path.chmod(0o644)
    elif invalid_input == "unsafe-result":
        result_path.chmod(0o644)
    elif invalid_input == "unsafe-rollback-manifest":
        rollback_manifest_path.chmod(0o644)
    elif invalid_input == "unsafe-rollback":
        rollback_path.chmod(0o644)
    elif invalid_input == "v1-plan":
        plan, v1_plan = _result_plan(tmp_path / "v1")
        del plan
        payload = json.dumps(
            v1_plan.canonical_value(),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        plan_path = tmp_path / "v1-plan.json"
        plan_path.write_bytes(payload)
        plan_path.chmod(0o600)
        plan_sha256 = hashlib.sha256(payload).hexdigest()
    elif invalid_input == "wrong-plan-sha":
        plan_sha256 = "b" * 64
    elif invalid_input == "wrong-result-sha":
        result_sha256 = "b" * 64
    elif invalid_input == "wrong-manifest":
        manifest_sha256 = "b" * 64
    elif invalid_input == "wrong-rollback-manifest-digest":
        rollback_manifest_path.write_bytes(rollback_manifest_path.read_bytes() + b"\n")
    elif invalid_input == "wrong-rollback-digest":
        value = json.loads(rollback_path.read_text(encoding="ascii"))
        value["input_sha256"] = "3" * 64
        _write_canonical_owner_only(rollback_path, value)
    elif invalid_input == "rollback-status-input-drift":
        value = json.loads(rollback_path.read_text(encoding="ascii"))
        value["input_sha256"] = "3" * 64
        rollback_sha256 = _write_canonical_owner_only(rollback_path, value)
        result_sha256 = _rewrite_acceptance_result_rollback_digest(
            result_path,
            rollback_sha256,
        )
    elif invalid_input in {"invalid-rollback", "wrong-rollback-release"}:
        value = json.loads(rollback_path.read_text(encoding="ascii"))
        if invalid_input == "invalid-rollback":
            value["ready"] = False
        else:
            value["release_sha256"] = "f" * 64
        rollback_sha256 = _write_canonical_owner_only(rollback_path, value)
        result_sha256 = _rewrite_acceptance_result_rollback_digest(
            result_path,
            rollback_sha256,
        )
    else:
        value = json.loads(result_path.read_text(encoding="ascii"))
        value["owners"][0]["initial"]["worker_available"] = True
        payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
        result_path.write_bytes(payload)
        result_path.chmod(0o600)
        result_sha256 = hashlib.sha256(payload).hexdigest()

    with (
        patch(
            "loom_cli.personal_dev_control_plane_cmd._SubprocessKubectlRunner",
            side_effect=AssertionError("verification must not construct a Kubernetes runner"),
        ),
        patch(
            "loom_cli.personal_dev_control_plane_cmd.subprocess.run",
            side_effect=AssertionError("verification must not create a subprocess"),
        ),
    ):
        rc = dispatch(
            _verify_acceptance_result_argv(
                plan_path,
                plan_sha256,
                result_path,
                result_sha256,
                manifest_sha256,
                rollback_manifest_path,
                rollback_path,
            )
        )

    assert rc == 2
    assert capsys.readouterr().out == ""


def test_verify_acceptance_result_parser_has_no_mutation_options() -> None:
    from loom_cli import personal_dev_control_plane_cmd

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True)
    personal_dev_control_plane_cmd.add_personal_dev_control_plane_subparser(subparsers)
    verify = next(
        action for action in parser._actions if isinstance(action, argparse._SubParsersAction)
    ).choices["personal-dev-control-plane"]
    operations = next(
        action for action in verify._actions if isinstance(action, argparse._SubParsersAction)
    )
    option_names = {
        option
        for action in operations.choices["verify-acceptance-result"]._actions
        for option in action.option_strings
    }
    forbidden = {
        "--apply",
        "--activate",
        "--kubeconfig",
        "--database",
        "--secret",
        "--slurm",
        "--capacity",
    }
    assert option_names.isdisjoint(forbidden)
    assert "--rollback-shadow-manifest-file" in option_names


def test_verify_acceptance_result_requires_all_digest_pinned_arguments() -> None:
    with pytest.raises(SystemExit) as exc:
        dispatch(
            [
                "personal-dev-control-plane",
                "verify-acceptance-result",
                "--acceptance-plan-file",
                "plan.json",
            ]
        )

    assert exc.value.code == 2


@pytest.mark.parametrize(
    ("present_option", "present_value"),
    [
        ("--rollback-shadow-manifest-file", "shadow.yaml"),
        ("--rollback-shadow-status-file", "shadow-status.json"),
    ],
)
def test_verify_acceptance_result_requires_both_rollback_shadow_files(
    present_option: str,
    present_value: str,
) -> None:
    with pytest.raises(SystemExit) as exc:
        dispatch(
            [
                "personal-dev-control-plane",
                "verify-acceptance-result",
                "--acceptance-plan-file",
                "plan.json",
                "--acceptance-plan-sha256",
                "1" * 64,
                "--acceptance-result-file",
                "result.json",
                "--acceptance-result-sha256",
                "2" * 64,
                "--acceptance-manifest-sha256",
                "3" * 64,
                present_option,
                present_value,
            ]
        )

    assert exc.value.code == 2


def test_render_emits_exact_yaml_and_canonical_evidence(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    profile = load_personal_dev_control_plane_profile(_PROFILE)
    release = load_personal_dev_trusted_release(release_path, release_digest)
    expected = render_shadow_personal_dev_control_plane(profile, release)

    result = dispatch(_argv(release_path, release_digest))

    captured = capsys.readouterr()
    assert result == 0
    assert captured.out == expected.yaml_text
    documents = [item for item in yaml.safe_load_all(captured.out) if item]
    assert len(documents) == expected.resource_count
    evidence = json.loads(captured.err)
    assert (
        captured.err
        == json.dumps(
            evidence,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        + "\n"
    )
    assert evidence == {
        "input_sha256": expected.input_sha256,
        "mode": "shadow",
        "release_sha256": expected.release_sha256,
        "resource_count": expected.resource_count,
        "schema": "loom-personal-dev-control-plane-render-v1",
        "source_sha": _SOURCE_SHA,
        "source_tree": _SOURCE_TREE,
        "yaml_sha256": hashlib.sha256(expected.yaml_text.encode("utf-8")).hexdigest(),
    }


@pytest.mark.parametrize(
    ("operation", "kind"),
    [
        ("render-trusted-launcher-profile", "trusted-launcher-profile"),
        ("render-scanner-finding-policy", "scanner-finding-policy"),
    ],
)
def test_render_policy_evidence_is_canonical_and_source_derived(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    operation: str,
    kind: str,
) -> None:
    release_path, release_digest = _release(tmp_path)

    result = dispatch(
        [
            "personal-dev-control-plane",
            operation,
            "--file",
            str(_PROFILE),
            "--trusted-release-file",
            str(release_path),
            "--trusted-release-sha256",
            release_digest,
            "--source-root",
            str(_ROOT),
        ]
    )

    captured = capsys.readouterr()
    assert result == 0
    value = json.loads(captured.out)
    assert captured.out == json.dumps(value, sort_keys=True, separators=(",", ":"))
    evidence = json.loads(captured.err)
    assert evidence == {
        "kind": kind,
        "schema": "loom-personal-dev-policy-evidence-render-v1",
        "sha256": hashlib.sha256(captured.out.encode("ascii")).hexdigest(),
        "source_sha": _SOURCE_SHA,
        "source_tree": _SOURCE_TREE,
    }


def _backup_evidence_argv(
    release_path: Path,
    release_digest: str,
    paths: dict[str, Path],
    *,
    names_are_bound: bool,
) -> list[str]:
    suffix = release_digest[:12]
    postgres_name = f"loom-personal-dev-pg-restore-{suffix}"
    minio_name = f"loom-personal-dev-minio-restore-{suffix}"
    network_name = f"loom-personal-dev-restore-{suffix}"
    if not names_are_bound:
        postgres_name = "unbound-postgres-restore"
    return [
        "personal-dev-control-plane",
        "render-backup-restore-evidence",
        "--file",
        str(_PROFILE),
        "--trusted-release-file",
        str(release_path),
        "--trusted-release-sha256",
        release_digest,
        "--started-at",
        "2026-08-26T19:00:00Z",
        "--completed-at",
        "2026-08-26T19:05:00Z",
        "--postgres-dump-file",
        str(paths["postgres.dump"]),
        "--postgres-source-state-file",
        str(paths["postgres.source.tsv"]),
        "--postgres-restored-state-file",
        str(paths["postgres.restored.tsv"]),
        "--source-schema-head",
        "0112",
        "--restored-schema-head",
        "0112",
        "--minio-source-manifest-file",
        str(paths["minio.source.json"]),
        "--minio-restored-manifest-file",
        str(paths["minio.restored.json"]),
        "--minio-payload-root",
        str(paths["minio-payloads"]),
        "--secret-key-inventory-file",
        str(paths["secrets.json"]),
        "--pre-shadow-status-file",
        str(paths["pre.json"]),
        "--post-shadow-status-file",
        str(paths["post.json"]),
        "--storage-inventory-file",
        str(paths["storage.json"]),
        "--isolated-postgres-name",
        postgres_name,
        "--isolated-minio-name",
        minio_name,
        "--isolated-network-name",
        network_name,
    ]


def test_render_backup_restore_evidence_uses_supported_derived_command(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_path, release_digest = _release(tmp_path)
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    cleanup_checked: list[bool] = []
    captured_inputs: dict[str, object] = {}
    expected = {"schema": "loom-personal-dev-backup-restore-evidence-v1"}

    monkeypatch.setattr(
        command,
        "_assert_isolated_restore_cleanup",
        lambda _args: cleanup_checked.append(True),
    )

    def build(**kwargs: object) -> dict[str, object]:
        captured_inputs.update(kwargs)
        return expected

    monkeypatch.setattr(command, "build_personal_dev_backup_restore_evidence", build)
    paths = {
        name: tmp_path / name
        for name in (
            "postgres.dump",
            "postgres.source.tsv",
            "postgres.restored.tsv",
            "minio.source.json",
            "minio.restored.json",
            "minio-payloads",
            "secrets.json",
            "pre.json",
            "post.json",
            "storage.json",
        )
    }
    result = dispatch(
        _backup_evidence_argv(
            release_path,
            release_digest,
            paths,
            names_are_bound=True,
        )
    )

    captured = capsys.readouterr()
    assert result == 0
    assert cleanup_checked == [True, True]
    assert captured.out == json.dumps(expected, sort_keys=True, separators=(",", ":"))
    evidence = json.loads(captured.err)
    assert evidence["kind"] == "backup-restore-evidence"
    assert evidence["sha256"] == hashlib.sha256(captured.out.encode("ascii")).hexdigest()
    assert captured_inputs["postgres_dump_path"] == paths["postgres.dump"]
    assert captured_inputs["minio_payload_root"] == paths["minio-payloads"]
    assert captured_inputs["storage_inventory_path"] == paths["storage.json"]


def test_render_backup_restore_evidence_rejects_unbound_cleanup_names(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_path, release_digest = _release(tmp_path)
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    monkeypatch.setattr(command, "_assert_isolated_restore_cleanup", lambda _args: None)
    monkeypatch.setattr(
        command,
        "build_personal_dev_backup_restore_evidence",
        lambda **_kwargs: {"schema": "unexpected"},
    )
    paths = {
        name: tmp_path / name
        for name in (
            "postgres.dump",
            "postgres.source.tsv",
            "postgres.restored.tsv",
            "minio.source.json",
            "minio.restored.json",
            "minio-payloads",
            "secrets.json",
            "pre.json",
            "post.json",
            "storage.json",
        )
    }

    result = dispatch(
        _backup_evidence_argv(
            release_path,
            release_digest,
            paths,
            names_are_bound=False,
        )
    )

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: personal-dev acceptance evidence inputs are invalid\n"


def test_render_schema_transition_emits_exact_job_and_canonical_plan(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_path, release_digest = _release(tmp_path)
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    from loom.personal_dev_schema_transition import PreparedPersonalDevSchemaTransition

    job = b'{"kind":"Job","metadata":{"name":"migration"}}'
    plan = {
        "migration": {"job_sha256": hashlib.sha256(job).hexdigest()},
        "schema": "loom-personal-dev-schema-transition-plan-v1",
    }
    plan_json = json.dumps(plan, sort_keys=True, separators=(",", ":")).encode("ascii")
    captured_inputs: dict[str, object] = {}
    source_validation_count = 0

    def prepare(**kwargs: object) -> PreparedPersonalDevSchemaTransition:
        captured_inputs.update(kwargs)
        return PreparedPersonalDevSchemaTransition(
            plan=plan,
            plan_json=plan_json,
            migration_job_json=job,
        )

    def validate_source(*_args: object, **_kwargs: object) -> None:
        nonlocal source_validation_count
        source_validation_count += 1

    monkeypatch.setattr(command, "prepare_personal_dev_schema_transition", prepare)
    monkeypatch.setattr(
        command,
        "validate_personal_dev_schema_transition_source_root",
        validate_source,
    )
    result = dispatch(
        [
            "personal-dev-control-plane",
            "render-schema-transition",
            "--file",
            str(_PROFILE),
            "--trusted-release-file",
            str(release_path),
            "--trusted-release-sha256",
            release_digest,
            "--source-root",
            str(_ROOT),
            "--predecessor-trusted-release-file",
            str(release_path),
            "--predecessor-trusted-release-sha256",
            release_digest,
            "--backup-restore-evidence-file",
            str(tmp_path / "backup.json"),
            "--backup-restore-evidence-sha256",
            "a" * 64,
            "--postgres-dump-file",
            str(tmp_path / "postgres.dump"),
            "--postgres-source-state-file",
            str(tmp_path / "postgres.tsv"),
            "--predecessor-shadow-manifest-file",
            str(tmp_path / "shadow.yaml"),
            "--predecessor-shadow-manifest-sha256",
            "b" * 64,
            "--alembic-config-file",
            str(_ROOT / "migrations/alembic.ini"),
            "--expected-predecessor-schema-head",
            "0112",
            "--expected-target-schema-head",
            "0128",
        ]
    )

    captured = capsys.readouterr()
    assert result == 0
    assert captured.out.encode("ascii") == job
    assert captured.err.encode("ascii") == plan_json + b"\n"
    assert captured_inputs["backup_evidence_sha256"] == "a" * 64
    assert captured_inputs["predecessor_shadow_sha256"] == "b" * 64
    assert captured_inputs["expected_predecessor_head"] == "0112"
    assert captured_inputs["expected_target_head"] == "0128"
    assert source_validation_count == 2


def test_render_schema_transition_real_cli_binds_exact_checkout_and_inputs(
    tmp_path: Path,
) -> None:
    from tests.unit.test_personal_dev_schema_transition import (
        _transition_inputs,
        _write_json,
    )

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    shutil.copytree(_ROOT / "src", checkout / "src")
    shutil.copytree(_ROOT / "migrations", checkout / "migrations")
    profile = checkout / "deploy/dev-fleet/personal-dev-control-plane.toml"
    profile.parent.mkdir(parents=True)
    shutil.copy2(_PROFILE, profile)
    (checkout / ".gitignore").write_text("__pycache__/\n*.pyc\n", encoding="ascii")
    subprocess.run(["/usr/bin/git", "init", "-q", str(checkout)], check=True)
    subprocess.run(
        ["/usr/bin/git", "-C", str(checkout), "add", "."],
        check=True,
    )
    subprocess.run(
        [
            "/usr/bin/git",
            "-C",
            str(checkout),
            "-c",
            "user.name=Loom tests",
            "-c",
            "user.email=loom-tests@example.invalid",
            "commit",
            "-qm",
            "exact CLI source fixture",
        ],
        check=True,
    )
    source_sha = subprocess.run(
        ["/usr/bin/git", "-C", str(checkout), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    source_tree = subprocess.run(
        ["/usr/bin/git", "-C", str(checkout), "rev-parse", "HEAD^{tree}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    inputs = _transition_inputs(artifacts)
    current_release_path = artifacts / "current-release.json"
    current_release_value = json.loads(current_release_path.read_text(encoding="ascii"))
    current_release_value["source_sha"] = source_sha
    current_release_value["source_tree"] = source_tree
    current_release_sha256 = _write_json(current_release_path, current_release_value)
    arguments = [
        "personal-dev-control-plane",
        "render-schema-transition",
        "--file",
        str(profile),
        "--trusted-release-file",
        str(current_release_path),
        "--trusted-release-sha256",
        current_release_sha256,
        "--source-root",
        str(checkout),
        "--predecessor-trusted-release-file",
        str(artifacts / "predecessor-release.json"),
        "--predecessor-trusted-release-sha256",
        str(inputs["predecessor_release_sha256"]),
        "--backup-restore-evidence-file",
        str(inputs["backup_evidence_path"]),
        "--backup-restore-evidence-sha256",
        str(inputs["backup_evidence_sha256"]),
        "--postgres-dump-file",
        str(inputs["postgres_dump_path"]),
        "--postgres-source-state-file",
        str(inputs["postgres_source_state_path"]),
        "--predecessor-shadow-manifest-file",
        str(inputs["predecessor_shadow_path"]),
        "--predecessor-shadow-manifest-sha256",
        str(inputs["predecessor_shadow_sha256"]),
        "--alembic-config-file",
        str(checkout / "migrations/alembic.ini"),
        "--expected-predecessor-schema-head",
        "0112",
        "--expected-target-schema-head",
        "0128",
    ]
    program = (
        "import json, sys\n"
        "from loom_cli.admin_cmd import dispatch\n"
        "raise SystemExit(dispatch(json.loads(sys.argv[1])))\n"
    )
    environment = _synthetic_checkout_env(
        PYTHONDONTWRITEBYTECODE="1",
        PYTHONPATH=str(checkout / "src"),
    )

    result = subprocess.run(
        [sys.executable, "-c", program, json.dumps(arguments)],
        cwd=checkout,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    job = json.loads(result.stdout)
    plan = json.loads(result.stderr)
    assert job["kind"] == "Job"
    assert job["metadata"]["namespace"] == "loom-dev"
    assert plan["schema"] == "loom-personal-dev-schema-transition-plan-v1"
    assert plan["predecessor"]["schema_head"] == "0112"
    assert plan["target"]["schema_head"] == "0128"
    assert plan["target"]["source_commit"] == source_sha
    assert plan["target"]["source_tree"] == source_tree
    assert plan["capacity"]["executable_new_capacity_ceiling"] == 0

    outside_cli_root = tmp_path / "outside-cli"
    shutil.copytree(checkout / "src/loom_cli", outside_cli_root / "loom_cli")
    with (outside_cli_root / "loom_cli/personal_dev_control_plane_cmd.py").open(
        "a", encoding="utf-8"
    ) as stream:
        stream.write("\n# outside CLI source\n")
    outside_cli_environment = environment.copy()
    outside_cli_environment["PYTHONPATH"] = f"{outside_cli_root}{os.pathsep}{checkout / 'src'}"
    outside_cli = subprocess.run(
        [sys.executable, "-c", program, json.dumps(arguments)],
        cwd=checkout,
        env=outside_cli_environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert outside_cli.returncode == 2
    assert outside_cli.stdout == ""
    assert outside_cli.stderr == "error: personal-dev schema transition inputs are invalid\n"

    outside_profile = tmp_path / "outside-profile.toml"
    shutil.copy2(profile, outside_profile)
    outside_profile_arguments = list(arguments)
    profile_index = outside_profile_arguments.index("--file") + 1
    outside_profile_arguments[profile_index] = str(outside_profile)
    outside_profile_result = subprocess.run(
        [sys.executable, "-c", program, json.dumps(outside_profile_arguments)],
        cwd=checkout,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert outside_profile_result.returncode == 2
    assert outside_profile_result.stdout == ""
    assert outside_profile_result.stderr == (
        "error: personal-dev schema transition inputs are invalid\n"
    )

    profile.unlink()
    profile.symlink_to(outside_profile)
    subprocess.run(
        ["/usr/bin/git", "-C", str(checkout), "add", str(profile.relative_to(checkout))],
        check=True,
    )
    subprocess.run(
        [
            "/usr/bin/git",
            "-C",
            str(checkout),
            "-c",
            "user.name=Loom tests",
            "-c",
            "user.email=loom-tests@example.invalid",
            "commit",
            "-qm",
            "replace profile with outside symlink",
        ],
        check=True,
    )
    symlink_source_sha = subprocess.run(
        ["/usr/bin/git", "-C", str(checkout), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    symlink_source_tree = subprocess.run(
        ["/usr/bin/git", "-C", str(checkout), "rev-parse", "HEAD^{tree}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    current_release_value["source_sha"] = symlink_source_sha
    current_release_value["source_tree"] = symlink_source_tree
    symlink_release_sha256 = _write_json(current_release_path, current_release_value)
    symlink_profile_arguments = list(arguments)
    release_digest_index = symlink_profile_arguments.index("--trusted-release-sha256") + 1
    symlink_profile_arguments[release_digest_index] = symlink_release_sha256
    symlink_profile_result = subprocess.run(
        [sys.executable, "-c", program, json.dumps(symlink_profile_arguments)],
        cwd=checkout,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert symlink_profile_result.returncode == 2
    assert symlink_profile_result.stdout == ""
    assert symlink_profile_result.stderr == (
        "error: personal-dev schema transition inputs are invalid\n"
    )


def test_render_schema_transition_has_a_specific_fail_closed_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)

    result = dispatch(
        [
            "personal-dev-control-plane",
            "render-schema-transition",
            "--file",
            str(_PROFILE),
            "--trusted-release-file",
            str(release_path),
            "--trusted-release-sha256",
            release_digest,
            "--source-root",
            str(tmp_path),
            "--predecessor-trusted-release-file",
            str(release_path),
            "--predecessor-trusted-release-sha256",
            release_digest,
            "--backup-restore-evidence-file",
            str(tmp_path / "backup.json"),
            "--backup-restore-evidence-sha256",
            "a" * 64,
            "--postgres-dump-file",
            str(tmp_path / "postgres.dump"),
            "--postgres-source-state-file",
            str(tmp_path / "postgres.tsv"),
            "--predecessor-shadow-manifest-file",
            str(tmp_path / "shadow.yaml"),
            "--predecessor-shadow-manifest-sha256",
            "b" * 64,
            "--alembic-config-file",
            str(_ROOT / "migrations/alembic.ini"),
            "--expected-predecessor-schema-head",
            "0112",
            "--expected-target-schema-head",
            "0128",
        ]
    )

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: personal-dev schema transition inputs are invalid\n"


def test_render_acceptance_requires_exact_plan_and_emits_only_yaml_and_evidence(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    profile_path = _prepared_profile(tmp_path)
    plan_path, plan_digest, plan = _acceptance_plan(
        tmp_path,
        release_path,
        release_digest,
        profile_path=profile_path,
    )
    profile = load_personal_dev_control_plane_profile(profile_path)
    release = load_personal_dev_trusted_release(release_path, release_digest)
    expected = render_acceptance_personal_dev_control_plane(
        profile,
        release,
        plan,
        now=_NOW,
    )

    result = dispatch(
        _acceptance_argv(
            release_path,
            release_digest,
            plan_path,
            plan_digest,
            profile=profile_path,
        )
    )

    captured = capsys.readouterr()
    assert result == 0
    assert captured.out == expected.yaml_text
    evidence = json.loads(captured.err)
    assert captured.err == (
        json.dumps(evidence, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    )
    assert evidence == {
        "acceptance_plan_sha256": plan.sha256,
        "input_sha256": expected.input_sha256,
        "mode": "acceptance",
        "release_sha256": expected.release_sha256,
        "resource_count": expected.resource_count,
        "schema": "loom-personal-dev-control-plane-render-v1",
        "source_sha": _SOURCE_SHA,
        "source_tree": _SOURCE_TREE,
        "yaml_sha256": hashlib.sha256(expected.yaml_text.encode("utf-8")).hexdigest(),
    }


def test_render_acceptance_loads_v3_two_owner_local_input(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    profile_source = tmp_path / "personal-dev-control-plane-v3.toml"
    profile_source.write_text(
        _PROFILE.read_text(encoding="utf-8")
        .replace("global_live_instances = 16", "global_live_instances = 2")
        .replace("builder_global_concurrency = 4", "builder_global_concurrency = 2"),
        encoding="utf-8",
    )
    profile_source.chmod(0o600)
    profile_path = _prepared_profile(tmp_path, source=profile_source)
    plan_path, plan_digest, plan = _acceptance_plan(
        tmp_path,
        release_path,
        release_digest,
        profile_path=profile_path,
    )
    profile = load_personal_dev_control_plane_profile(profile_path)
    release = load_personal_dev_trusted_release(release_path, release_digest)
    expected = render_acceptance_personal_dev_control_plane(profile, release, plan, now=_NOW)

    result = dispatch(
        _acceptance_argv(
            release_path,
            release_digest,
            plan_path,
            plan_digest,
            profile=profile_path,
        )
    )

    captured = capsys.readouterr()
    assert result == 0
    assert captured.out == expected.yaml_text
    assert plan.schema_version == 3
    assert len(plan.acceptance_owners) == 2
    assert plan.quotas.global_live_instances == 2
    assert plan.quotas.builder_global_concurrency == 2


def test_render_operational_emits_durable_plan_binding(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    profile_path = _prepared_profile(tmp_path)
    plan_path, plan_digest, plan = _operational_plan(
        tmp_path,
        release_path,
        release_digest,
        profile_path=profile_path,
    )
    profile = load_personal_dev_control_plane_profile(profile_path)
    release = load_personal_dev_trusted_release(release_path, release_digest)
    expected = render_operational_personal_dev_control_plane(
        profile,
        release,
        plan,
        now=_NOW,
    )

    result = dispatch(
        _operational_argv(
            release_path,
            release_digest,
            plan_path,
            plan_digest,
            profile=profile_path,
        )
    )

    captured = capsys.readouterr()
    assert result == 0
    assert captured.out == expected.yaml_text
    evidence = json.loads(captured.err)
    assert evidence == {
        "input_sha256": expected.input_sha256,
        "mode": "operational",
        "operational_plan_sha256": plan.sha256,
        "release_sha256": expected.release_sha256,
        "resource_count": expected.resource_count,
        "schema": "loom-personal-dev-control-plane-render-v1",
        "source_sha": _SOURCE_SHA,
        "source_tree": _SOURCE_TREE,
        "yaml_sha256": hashlib.sha256(expected.yaml_text.encode("utf-8")).hexdigest(),
    }


@pytest.mark.parametrize(
    ("operation", "omitted"),
    [
        ("render-acceptance", "--acceptance-plan-file"),
        ("render-acceptance", "--acceptance-plan-sha256"),
        ("status-acceptance", "--acceptance-plan-file"),
        ("status-acceptance", "--acceptance-plan-sha256"),
    ],
)
def test_acceptance_commands_reject_partial_plan_bindings(
    tmp_path: Path,
    operation: str,
    omitted: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    profile_path = _prepared_profile(tmp_path)
    plan_path, plan_digest, _plan = _acceptance_plan(
        tmp_path,
        release_path,
        release_digest,
        profile_path=profile_path,
    )
    if operation == "render-acceptance":
        argv = _acceptance_argv(
            release_path,
            release_digest,
            plan_path,
            plan_digest,
            profile=profile_path,
        )
    else:
        kubeconfig = _reviewed_kubeconfig(tmp_path)
        argv = _acceptance_status_argv(
            release_path,
            release_digest,
            plan_path,
            plan_digest,
            kubeconfig.resolve(),
            profile=profile_path,
        )
    index = argv.index(omitted)
    del argv[index : index + 2]

    with pytest.raises(SystemExit) as stopped:
        dispatch(argv)

    captured = capsys.readouterr()
    assert stopped.value.code == 2
    assert captured.out == ""
    assert f"the following arguments are required: {omitted}" in captured.err


def test_render_is_byte_deterministic_across_repeated_invocations(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)

    assert dispatch(_argv(release_path, release_digest)) == 0
    first = capsys.readouterr()
    assert dispatch(_argv(release_path, release_digest)) == 0
    second = capsys.readouterr()

    assert second == first


def test_render_accepts_exact_open_proc_release_descriptor(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    descriptor = os.open(release_path, os.O_RDONLY | os.O_CLOEXEC)
    try:
        result = dispatch(
            _argv(Path(f"/proc/self/fd/{descriptor}"), release_digest),
        )
    finally:
        os.close(descriptor)

    captured = capsys.readouterr()
    evidence = json.loads(captured.err)
    documents = list(yaml.safe_load_all(captured.out))
    assert result == 0
    assert documents
    assert len(documents) == evidence["resource_count"]
    assert evidence["schema"] == "loom-personal-dev-control-plane-render-v1"
    assert evidence["source_sha"] == _SOURCE_SHA
    assert evidence["source_tree"] == _SOURCE_TREE
    assert evidence["yaml_sha256"] == hashlib.sha256(captured.out.encode()).hexdigest()


@pytest.mark.parametrize(
    "omitted",
    ["--file", "--trusted-release-file", "--trusted-release-sha256"],
)
def test_render_requires_every_trust_binding_argument(
    tmp_path: Path,
    omitted: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    argv = _argv(release_path, release_digest)
    index = argv.index(omitted)
    del argv[index : index + 2]

    with pytest.raises(SystemExit) as stopped:
        dispatch(argv)

    captured = capsys.readouterr()
    assert stopped.value.code == 2
    assert captured.out == ""
    assert f"the following arguments are required: {omitted}" in captured.err


@pytest.mark.parametrize("option", ["--trusted-release-sha", "--unknown-option"])
def test_render_rejects_abbreviated_and_unknown_options(
    tmp_path: Path,
    option: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    argv = _argv(release_path, release_digest)
    argv.extend([option, "do-not-accept"])

    with pytest.raises(SystemExit) as stopped:
        dispatch(argv)

    captured = capsys.readouterr()
    assert stopped.value.code == 2
    assert captured.out == ""
    assert f"unrecognized arguments: {option} do-not-accept" in captured.err


def test_render_rejects_unsafe_release_before_stdout(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    release_path.chmod(0o644)

    result = dispatch(_argv(release_path, release_digest))

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: personal-dev control-plane render inputs are invalid\n"


def test_render_redacts_invalid_profile_payload_before_stdout(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    secret_value = "do-not-log-this-accidental-secret"
    profile = tmp_path / "profile.toml"
    profile.write_text(
        _PROFILE.read_text(encoding="utf-8") + f'\naccidental_secret = "{secret_value}"\n',
        encoding="utf-8",
    )

    result = dispatch(_argv(release_path, release_digest, profile=profile))

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: personal-dev control-plane render inputs are invalid\n"
    assert secret_value not in captured.err


def test_render_handles_broken_stdout_without_false_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_path, release_digest = _release(tmp_path)
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")

    class _BrokenStdout:
        def write(self, _value: str) -> int:
            raise BrokenPipeError

    errors = io.StringIO()
    monkeypatch.setattr(command.sys, "stdout", _BrokenStdout())
    monkeypatch.setattr(command.sys, "stderr", errors)

    assert dispatch(_argv(release_path, release_digest)) == 0
    assert errors.getvalue() == ""


def test_help_describes_only_render_and_read_only_zero_capacity_authority(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as stopped:
        dispatch(["personal-dev-control-plane", "--help"])

    captured = capsys.readouterr()
    assert stopped.value.code == 0
    assert "render-only" in captured.out
    assert "shadow" in captured.out
    assert "acceptance" in captured.out
    assert "read-only" in captured.out
    assert "physical capacity unchanged" in captured.out
    assert "apply" not in captured.out.casefold()
    assert "activate" not in captured.out.casefold()


@pytest.mark.parametrize("operation", ["apply", "activate"])
def test_personal_control_plane_has_no_apply_or_activate_operation(
    operation: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as stopped:
        dispatch(["personal-dev-control-plane", operation])

    captured = capsys.readouterr()
    assert stopped.value.code == 2
    assert captured.out == ""
    assert "invalid choice" in captured.err


def test_admin_help_lists_personal_control_plane_command(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as stopped:
        dispatch(["--help"])

    captured = capsys.readouterr()
    assert stopped.value.code == 0
    assert "personal-dev-control-plane" in captured.out


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (
            ["service", "up", "--help"],
            "--environment ENVIRONMENT",
        ),
        (["dev", "--help"], "{create,list,status,destroy}"),
    ],
)
def test_personal_control_plane_registration_does_not_extend_service_or_dev(
    argv: list[str],
    expected: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as stopped:
        main(argv)

    captured = capsys.readouterr()
    assert stopped.value.code == 0
    assert expected in captured.out
    assert "personal-dev-control-plane" not in captured.out


@pytest.mark.parametrize("ready", [True, False])
def test_status_emits_one_canonical_record_and_readiness_exit_code(
    tmp_path: Path,
    ready: bool,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    status = PersonalDevShadowStatus(
        ready=ready,
        blockers=() if ready else ("manager_probe_unavailable",),
        input_sha256="a" * 64,
        release_sha256="b" * 64,
        manager_ceiling=0 if ready else None,
        components=(PersonalDevShadowComponent("manager", int(ready), ready),),
    )

    class _Runner:
        def __init__(self, path: Path) -> None:
            assert path == kubeconfig

    def _observe(
        runner: object,
        *,
        expected: object,
        namespace: str,
    ) -> PersonalDevShadowStatus:
        assert isinstance(runner, _Runner)
        assert expected.resource_count == 38
        assert namespace == "loom-dev"
        return status

    monkeypatch.setattr(command, "_SubprocessKubectlRunner", _Runner)
    monkeypatch.setattr(command, "observe_personal_dev_shadow_status", _observe)

    result = dispatch(_status_argv(release_path, release_digest, kubeconfig.resolve()))

    captured = capsys.readouterr()
    expected_output = (
        json.dumps(
            status.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        + "\n"
    )
    assert result == (0 if ready else 1)
    assert captured.out == expected_output
    assert captured.err == ""


@pytest.mark.parametrize("ready", [True, False])
def test_status_acceptance_emits_one_canonical_read_only_record(
    tmp_path: Path,
    ready: bool,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    profile_path = _prepared_profile(tmp_path)
    plan_path, plan_digest, plan = _acceptance_plan(
        tmp_path,
        release_path,
        release_digest,
        profile_path=profile_path,
    )
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    status = PersonalDevAcceptanceStatus(
        ready=ready,
        blockers=() if ready else ("manager_binding_drift",),
        input_sha256="a" * 64,
        release_sha256="b" * 64,
        acceptance_plan_sha256=plan.sha256,
        manager_ceiling=0,
        components=(PersonalDevShadowComponent("manager", 1, ready),),
        application_ready=True,
        capacity_publication_ready=ready,
        worker_available=False,
    )

    class _Runner:
        def __init__(self, path: Path) -> None:
            assert path == kubeconfig

    def _observe(
        runner: object,
        *,
        expected: object,
        plan: object,
        namespace: str,
    ) -> PersonalDevAcceptanceStatus:
        assert isinstance(runner, _Runner)
        assert expected.resource_count == 40
        assert plan.sha256 == plan_digest
        assert namespace == "loom-dev"
        return status

    monkeypatch.setattr(command, "_SubprocessKubectlRunner", _Runner)
    monkeypatch.setattr(command, "observe_personal_dev_acceptance_status", _observe)

    result = dispatch(
        _acceptance_status_argv(
            release_path,
            release_digest,
            plan_path,
            plan_digest,
            kubeconfig.resolve(),
            profile=profile_path,
        )
    )

    captured = capsys.readouterr()
    expected_output = (
        json.dumps(status.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    )
    assert result == (0 if ready else 1)
    assert captured.out == expected_output
    assert captured.err == ""


@pytest.mark.parametrize("ready", [True, False])
def test_status_operational_emits_one_canonical_read_only_record(
    tmp_path: Path,
    ready: bool,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    profile_path = _prepared_profile(tmp_path)
    plan_path, plan_digest, plan = _operational_plan(
        tmp_path,
        release_path,
        release_digest,
        profile_path=profile_path,
    )
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    status = PersonalDevOperationalStatus(
        ready=ready,
        blockers=() if ready else ("manager_binding_drift",),
        input_sha256="a" * 64,
        release_sha256="b" * 64,
        operational_plan_sha256=plan.sha256,
        manager_ceiling=0,
        components=(PersonalDevShadowComponent("manager", 1, ready),),
        application_ready=True,
        capacity_publication_ready=ready,
        worker_available=False,
    )

    class _Runner:
        def __init__(self, path: Path) -> None:
            assert path == kubeconfig

    def _observe(
        runner: object,
        *,
        expected: object,
        plan: object,
        namespace: str,
    ) -> PersonalDevOperationalStatus:
        assert isinstance(runner, _Runner)
        assert expected.resource_count == 40
        assert plan.sha256 == plan_digest
        assert namespace == "loom-dev"
        return status

    monkeypatch.setattr(command, "_SubprocessKubectlRunner", _Runner)
    monkeypatch.setattr(command, "observe_personal_dev_operational_status", _observe)

    result = dispatch(
        _operational_status_argv(
            release_path,
            release_digest,
            plan_path,
            plan_digest,
            kubeconfig.resolve(),
            profile=profile_path,
        )
    )

    captured = capsys.readouterr()
    expected_output = (
        json.dumps(status.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    )
    assert result == (0 if ready else 1)
    assert captured.out == expected_output
    assert captured.err == ""


def test_status_acceptance_rejects_invalid_plan_before_constructing_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    profile_path = _prepared_profile(tmp_path)
    plan_path, plan_digest, _plan = _acceptance_plan(
        tmp_path,
        release_path,
        release_digest,
        profile_path=profile_path,
    )
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    plan_path.chmod(0o644)
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")

    class _UnexpectedRunner:
        def __init__(self, _path: Path) -> None:
            raise AssertionError("invalid acceptance plan reached kubectl")

    monkeypatch.setattr(command, "_SubprocessKubectlRunner", _UnexpectedRunner)

    result = dispatch(
        _acceptance_status_argv(
            release_path,
            release_digest,
            plan_path,
            plan_digest,
            kubeconfig.resolve(),
            profile=profile_path,
        )
    )

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: personal-dev control-plane status inputs are invalid\n"


@pytest.mark.parametrize(
    "omitted",
    [
        "--kubeconfig",
        "--file",
        "--trusted-release-file",
        "--trusted-release-sha256",
    ],
)
def test_status_requires_kubeconfig_and_every_trust_binding_argument(
    tmp_path: Path,
    omitted: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    argv = _status_argv(release_path, release_digest, kubeconfig.resolve())
    index = argv.index(omitted)
    del argv[index : index + 2]

    with pytest.raises(SystemExit) as stopped:
        dispatch(argv)

    captured = capsys.readouterr()
    assert stopped.value.code == 2
    assert captured.out == ""
    assert f"the following arguments are required: {omitted}" in captured.err


def test_status_rejects_abbreviated_option(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    argv = _status_argv(release_path, release_digest, kubeconfig.resolve())
    argv.extend(["--trusted-release-sha", "do-not-accept"])

    with pytest.raises(SystemExit) as stopped:
        dispatch(argv)

    captured = capsys.readouterr()
    assert stopped.value.code == 2
    assert captured.out == ""
    assert "unrecognized arguments: --trusted-release-sha do-not-accept" in captured.err


@pytest.mark.parametrize(
    "unsafe",
    [
        "relative",
        "symlink",
        "parent-symlink",
        "symlink-loop",
        "world-readable",
        "hardlink",
        "empty",
        "oversized",
        "external-cluster-ca",
        "external-client-key",
        "exec-credential-plugin",
    ],
)
def test_status_rejects_nonabsolute_or_symlink_kubeconfig_before_observation(
    tmp_path: Path,
    unsafe: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    if unsafe == "relative":
        selected = Path("relative-kubeconfig")
    elif unsafe == "symlink":
        selected = tmp_path / "linked-kubeconfig"
        selected.symlink_to(kubeconfig)
    elif unsafe == "parent-symlink":
        real_parent = tmp_path / "real-parent"
        real_parent.mkdir()
        nested_kubeconfig = real_parent / "reviewed-kubeconfig"
        nested_kubeconfig.write_text("reviewed", encoding="utf-8")
        nested_kubeconfig.chmod(0o600)
        linked_parent = tmp_path / "linked-parent"
        linked_parent.symlink_to(real_parent, target_is_directory=True)
        selected = linked_parent / "reviewed-kubeconfig"
    elif unsafe == "symlink-loop":
        selected = tmp_path / "looped-kubeconfig"
        selected.symlink_to(selected)
    elif unsafe == "world-readable":
        kubeconfig.chmod(0o644)
        selected = kubeconfig
    elif unsafe == "hardlink":
        selected = tmp_path / "hardlinked-kubeconfig"
        os.link(kubeconfig, selected)
    elif unsafe == "empty":
        kubeconfig.write_bytes(b"")
        selected = kubeconfig
    elif unsafe == "oversized":
        kubeconfig.write_bytes(b"x" * (1024 * 1024 + 1))
        selected = kubeconfig
    else:
        document = yaml.safe_load(kubeconfig.read_text(encoding="utf-8"))
        if unsafe == "external-cluster-ca":
            document["clusters"][0]["cluster"] = {
                "certificate-authority": "/tmp/external-ca.pem",
                "server": "https://127.0.0.1:6443",
            }
        elif unsafe == "external-client-key":
            document["users"][0]["user"] = {"client-key": "/tmp/external-key.pem"}
        else:
            document["users"][0]["user"] = {
                "exec": {"apiVersion": "client.authentication.k8s.io/v1", "command": "helper"}
            }
        kubeconfig.write_text(yaml.safe_dump(document, sort_keys=True), encoding="utf-8")
        selected = kubeconfig
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")

    def _unexpected(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("unsafe kubeconfig reached observation")

    monkeypatch.setattr(command, "observe_personal_dev_shadow_status", _unexpected)

    result = dispatch(_status_argv(release_path, release_digest, selected))

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: personal-dev control-plane status inputs are invalid\n"


def test_status_redacts_invalid_release_before_observation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    release_path.chmod(0o644)
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")

    def _unexpected(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("invalid release reached observation")

    monkeypatch.setattr(command, "observe_personal_dev_shadow_status", _unexpected)

    result = dispatch(_status_argv(release_path, release_digest, kubeconfig.resolve()))

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: personal-dev control-plane status inputs are invalid\n"


def test_status_subprocess_runner_stops_at_combined_output_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = tmp_path / "kubectl"
    executable.write_text(
        "#!/usr/bin/env python3\nimport sys\nsys.stdout.write('x' * (4 * 1024 * 1024 + 1))\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    runner = command._SubprocessKubectlRunner(kubeconfig)

    with pytest.raises(OSError, match="output exceeds"):
        runner.run(["get", "namespaces"], timeout_seconds=5)


def test_status_subprocess_runner_returns_bounded_stdout_stderr_and_exit_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = tmp_path / "kubectl"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import sys\n"
        "sys.stdout.write('bounded-out')\n"
        "sys.stderr.write('bounded-err')\n"
        "raise SystemExit(3)\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    runner = command._SubprocessKubectlRunner(kubeconfig)

    result = runner.run(["get", "namespaces"], timeout_seconds=5)

    assert result.args == [
        "kubectl",
        "--kubeconfig",
        str(kubeconfig),
        "get",
        "namespaces",
    ]
    assert result.returncode == 3
    assert result.stdout == "bounded-out"
    assert result.stderr == "bounded-err"


def test_status_subprocess_runner_rejects_kubeconfig_change_during_observation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = tmp_path / "kubectl"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import os\n"
        "import pathlib\n"
        "import sys\n"
        "pathlib.Path(os.environ['LOOM_TEST_KUBECONFIG']).write_text('changed', encoding='utf-8')\n"
        "sys.stdout.write('{}')\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    monkeypatch.setenv("LOOM_TEST_KUBECONFIG", str(kubeconfig))
    runner = command._SubprocessKubectlRunner(kubeconfig)

    with pytest.raises(OSError, match="kubeconfig changed during observation"):
        runner.run(["get", "namespaces"], timeout_seconds=5)


def test_status_subprocess_runner_pins_kubeconfig_against_swap_and_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ready = tmp_path / "ready"
    proceed = tmp_path / "proceed"
    consumed = tmp_path / "consumed"
    restored = tmp_path / "restored"
    executable = tmp_path / "kubectl"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import os\n"
        "import pathlib\n"
        "import sys\n"
        "import time\n"
        "ready = pathlib.Path(os.environ['LOOM_TEST_READY'])\n"
        "proceed = pathlib.Path(os.environ['LOOM_TEST_PROCEED'])\n"
        "consumed = pathlib.Path(os.environ['LOOM_TEST_CONSUMED'])\n"
        "restored = pathlib.Path(os.environ['LOOM_TEST_RESTORED'])\n"
        "ready.touch()\n"
        "for _ in range(500):\n"
        "    if proceed.exists():\n"
        "        break\n"
        "    time.sleep(0.01)\n"
        "else:\n"
        "    raise SystemExit(2)\n"
        "value = pathlib.Path(sys.argv[2]).read_text(encoding='utf-8')\n"
        "consumed.write_text(value, encoding='utf-8')\n"
        "for _ in range(500):\n"
        "    if restored.exists():\n"
        "        break\n"
        "    time.sleep(0.01)\n"
        "else:\n"
        "    raise SystemExit(3)\n"
        "sys.stdout.write(value)\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("LOOM_TEST_READY", str(ready))
    monkeypatch.setenv("LOOM_TEST_PROCEED", str(proceed))
    monkeypatch.setenv("LOOM_TEST_CONSUMED", str(consumed))
    monkeypatch.setenv("LOOM_TEST_RESTORED", str(restored))
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    original = tmp_path / "reviewed-kubeconfig.original"
    reviewed_payload = kubeconfig.read_text(encoding="utf-8")
    runner = command._SubprocessKubectlRunner(kubeconfig)

    def swap_path() -> None:
        for _ in range(500):
            if ready.exists():
                break
            time.sleep(0.01)
        else:
            return
        kubeconfig.rename(original)
        kubeconfig.write_text("attacker", encoding="utf-8")
        proceed.touch()
        for _ in range(500):
            if consumed.exists():
                break
            time.sleep(0.01)
        kubeconfig.unlink()
        original.rename(kubeconfig)
        restored.touch()

    swapper = threading.Thread(target=swap_path)
    swapper.start()
    with pytest.raises(OSError, match="kubeconfig changed during observation"):
        runner.run(["get", "namespaces"], timeout_seconds=10)
    swapper.join(timeout=10)

    assert not swapper.is_alive()
    assert consumed.read_text(encoding="utf-8") == reviewed_payload


def test_status_subprocess_runner_pins_kubeconfig_bytes_against_in_place_rewrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ready = tmp_path / "ready"
    proceed = tmp_path / "proceed"
    consumed = tmp_path / "consumed"
    restored = tmp_path / "restored"
    executable = tmp_path / "kubectl"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import os\n"
        "import pathlib\n"
        "import sys\n"
        "import time\n"
        "ready = pathlib.Path(os.environ['LOOM_TEST_READY'])\n"
        "proceed = pathlib.Path(os.environ['LOOM_TEST_PROCEED'])\n"
        "consumed = pathlib.Path(os.environ['LOOM_TEST_CONSUMED'])\n"
        "restored = pathlib.Path(os.environ['LOOM_TEST_RESTORED'])\n"
        "ready.touch()\n"
        "for _ in range(500):\n"
        "    if proceed.exists():\n"
        "        break\n"
        "    time.sleep(0.01)\n"
        "else:\n"
        "    raise SystemExit(2)\n"
        "value = pathlib.Path(sys.argv[2]).read_text(encoding='utf-8')\n"
        "consumed.write_text(value, encoding='utf-8')\n"
        "for _ in range(500):\n"
        "    if restored.exists():\n"
        "        break\n"
        "    time.sleep(0.01)\n"
        "else:\n"
        "    raise SystemExit(3)\n"
        "sys.stdout.write(value)\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("LOOM_TEST_READY", str(ready))
    monkeypatch.setenv("LOOM_TEST_PROCEED", str(proceed))
    monkeypatch.setenv("LOOM_TEST_CONSUMED", str(consumed))
    monkeypatch.setenv("LOOM_TEST_RESTORED", str(restored))
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    reviewed_payload = kubeconfig.read_text(encoding="utf-8")
    runner = command._SubprocessKubectlRunner(kubeconfig)

    def rewrite_path() -> None:
        for _ in range(500):
            if ready.exists():
                break
            time.sleep(0.01)
        else:
            return
        kubeconfig.write_text("attacker-controlled", encoding="utf-8")
        proceed.touch()
        for _ in range(500):
            if consumed.exists():
                break
            time.sleep(0.01)
        kubeconfig.write_text(reviewed_payload, encoding="utf-8")
        restored.touch()

    rewriter = threading.Thread(target=rewrite_path)
    rewriter.start()
    with pytest.raises(OSError, match="kubeconfig changed during observation"):
        runner.run(["get", "namespaces"], timeout_seconds=10)
    rewriter.join(timeout=10)

    assert not rewriter.is_alive()
    assert consumed.read_text(encoding="utf-8") == reviewed_payload


# Production break caught: capture can be invoked without one of its fixed authorities.
@pytest.mark.parametrize(
    "omitted",
    ["--namespace", "--kubeconfig", "--source-manifest-file", "--payload-root"],
)
def test_capture_minio_backup_requires_every_authority_argument(
    tmp_path: Path,
    omitted: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    argv = _capture_minio_argv(_reviewed_kubeconfig(tmp_path), tmp_path)
    index = argv.index(omitted)
    del argv[index : index + 2]

    with pytest.raises(SystemExit) as stopped:
        dispatch(argv)

    captured = capsys.readouterr()
    assert stopped.value.code == 2
    assert captured.out == ""
    assert f"the following arguments are required: {omitted}" in captured.err


# Production break caught: capture is widened to a namespace other than loom-dev.
def test_capture_minio_backup_accepts_only_the_exact_live_namespace(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    argv = _capture_minio_argv(_reviewed_kubeconfig(tmp_path), tmp_path)
    argv[argv.index("loom-dev")] = "attacker-namespace"

    with pytest.raises(SystemExit) as stopped:
        dispatch(argv)

    captured = capsys.readouterr()
    assert stopped.value.code == 2
    assert captured.out == ""
    assert "invalid choice: 'attacker-namespace'" in captured.err


# Production break caught: restore can run without a release, retained payload, or isolation binding.
@pytest.mark.parametrize(
    "omitted",
    [
        "--trusted-release-file",
        "--trusted-release-sha256",
        "--source-manifest-file",
        "--payload-root",
        "--restored-manifest-file",
        "--restore-env-file",
        "--isolated-minio-name",
        "--isolated-network-name",
    ],
)
def test_restore_minio_backup_requires_every_authority_argument(
    tmp_path: Path,
    omitted: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release, digest = _release(tmp_path)
    argv = _restore_minio_argv(release, digest, tmp_path)
    index = argv.index(omitted)
    del argv[index : index + 2]

    with pytest.raises(SystemExit) as stopped:
        dispatch(argv)

    captured = capsys.readouterr()
    assert stopped.value.code == 2
    assert captured.out == ""
    assert f"the following arguments are required: {omitted}" in captured.err


# Production break caught: live MinIO credentials or raw object keys escape positional in-pod argv.
def test_kubectl_minio_transport_confines_credentials_and_raw_keys_to_admin_exec() -> None:
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    pod_payload = json.dumps(
        {
            "apiVersion": "v1",
            "items": [
                {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "labels": {"app": "loom-dev-minio"},
                        "name": "loom-dev-minio-0",
                        "namespace": "loom-dev",
                    },
                    "status": {"phase": "Running"},
                }
            ],
            "kind": "List",
            "metadata": {"resourceVersion": "123"},
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    calls: list[tuple[str, tuple[str, ...]]] = []

    class _Runner:
        def run(
            self,
            argv: list[str] | tuple[str, ...],
            *,
            timeout_seconds: int,
        ) -> subprocess.CompletedProcess[str]:
            calls.append(("run", tuple(argv)))
            assert timeout_seconds == 30
            return subprocess.CompletedProcess(argv, 0, pod_payload, "")

        def stream(
            self,
            argv: list[str] | tuple[str, ...],
            *,
            destination: object,
            maximum_stdout_bytes: int,
            expected_size: int | None,
            maximum_stderr_bytes: int,
            timeout_seconds: int,
        ) -> PersonalDevMinioCommandResult:
            calls.append(("stream", tuple(argv)))
            assert destination is None
            assert maximum_stdout_bytes == 4096
            assert expected_size is None
            assert maximum_stderr_bytes == 64 * 1024
            assert timeout_seconds == 60
            return PersonalDevMinioCommandResult(0, b"bounded", b"")

    raw_key = "owner/raw key $(credential-marker).tar"
    transport = command._KubectlMinioTransport(_Runner(), namespace="loom-dev")
    result = transport.run(
        ("stat", "--json", f"local/artifacts/{raw_key}"),
        maximum_stdout_bytes=4096,
        timeout_seconds=60,
    )

    assert result == PersonalDevMinioCommandResult(0, b"bounded", b"")
    assert calls[0] == (
        "run",
        (
            "--namespace",
            "loom-dev",
            "get",
            "pods",
            "--selector",
            "app=loom-dev-minio",
            "--output=json",
        ),
    )
    exec_argv = calls[1][1]
    assert exec_argv[:9] == (
        "--namespace",
        "loom-dev",
        "exec",
        "loom-dev-minio-0",
        "-c",
        "admin",
        "--",
        "/bin/sh",
        "-euc",
    )
    assert exec_argv[9:] == (
        'export MC_HOST_local="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@127.0.0.1:9000"; exec mc "$@"',
        "sh",
        "stat",
        "--json",
        f"local/artifacts/{raw_key}",
    )
    assert exec_argv.count(f"local/artifacts/{raw_key}") == 1


# Production break caught: kubectl binary output is buffered whole or may exceed its exact size.
def test_kubectl_stream_writes_incrementally_and_enforces_exact_size(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = tmp_path / "kubectl"
    executable.write_text(
        "#!/usr/bin/env python3\nimport sys\nsys.stdout.buffer.write(b'abcdefghijkl')\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    monkeypatch.setattr(command, "_KUBECTL_READ_BYTES", 4)
    writes: list[bytes] = []

    class _Destination:
        def write(self, value: bytes) -> int:
            writes.append(value)
            return len(value)

    runner = command._SubprocessKubectlRunner(_reviewed_kubeconfig(tmp_path))
    result = runner.stream(
        ["exec", "pod"],
        destination=_Destination(),
        maximum_stdout_bytes=12,
        expected_size=12,
        maximum_stderr_bytes=64,
        timeout_seconds=5,
    )

    assert result == PersonalDevMinioCommandResult(0, b"", b"")
    assert b"".join(writes) == b"abcdefghijkl"
    assert len(writes) >= 3
    with pytest.raises(OSError, match="size"):
        runner.stream(
            ["exec", "pod"],
            destination=io.BytesIO(),
            maximum_stdout_bytes=11,
            expected_size=11,
            maximum_stderr_bytes=64,
            timeout_seconds=5,
        )


# Production break caught: capture success exposes object identities instead of the safe canonical summary.
def test_capture_minio_backup_emits_only_safe_canonical_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    manifest = PersonalDevMinioManifest(())
    observed: dict[str, object] = {}

    class _Transport:
        def __init__(self, runner: object, *, namespace: str) -> None:
            observed.update(runner=runner, namespace=namespace)

    class _Runner:
        def __init__(self, path: Path) -> None:
            observed["kubeconfig"] = path

    def _capture(**kwargs: object) -> PersonalDevMinioManifest:
        observed.update(kwargs)
        return manifest

    monkeypatch.setattr(command, "_SubprocessKubectlRunner", _Runner)
    monkeypatch.setattr(command, "_KubectlMinioTransport", _Transport)
    monkeypatch.setattr(command, "capture_personal_dev_minio_backup", _capture)

    result = dispatch(_capture_minio_argv(kubeconfig, tmp_path))

    captured = capsys.readouterr()
    assert result == 0
    assert captured.err == ""
    assert captured.out == (
        '{"object_count":0,"payload_bytes":0,'
        '"schema":"loom-personal-dev-minio-backup-summary-v1",'
        f'"source_manifest_sha256":"{hashlib.sha256(manifest.canonical_bytes).hexdigest()}"}}\n'
    )
    assert observed["namespace"] == "loom-dev"
    assert observed["kubeconfig"] == kubeconfig
    assert observed["source_manifest_path"] == tmp_path / "minio.source.json"
    assert observed["payload_root"] == tmp_path / "payloads"


# Production break caught: Docker restore accepts a non-owner-only or aliased credential file.
@pytest.mark.parametrize("unsafe", ["relative", "world-readable", "symlink"])
def test_docker_minio_transport_requires_absolute_owner_only_restore_env_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unsafe: str,
) -> None:
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    release_path, release_digest = _release(tmp_path)
    release = load_personal_dev_trusted_release(release_path, release_digest)
    suffix = release_digest[:12]
    payload_root = tmp_path / "payloads"
    payload_root.mkdir(mode=0o700)
    env_file = tmp_path / "restore.env"
    env_file.write_text("SENSITIVE_RESTORE_MARKER=value\n", encoding="utf-8")
    env_file.chmod(0o600)
    if unsafe == "relative":
        selected = Path("restore.env")
    elif unsafe == "world-readable":
        env_file.chmod(0o644)
        selected = env_file
    else:
        selected = tmp_path / "restore-link.env"
        selected.symlink_to(env_file)

    with pytest.raises(ValueError):
        command._DockerMinioTransport(
            client_image=release.images.minio_client,
            minio_image=release.images.minio,
            restore_env_file=selected,
            payload_root=payload_root,
            isolated_minio_name=f"loom-personal-dev-minio-restore-{suffix}",
            isolated_network_name=f"loom-personal-dev-restore-{suffix}",
        )


# Production break caught: restore can target a published, untrusted, or shared Docker store.
@pytest.mark.parametrize(
    "unsafe",
    ["image", "published-port", "wrong-network", "external-network", "extra-peer", "alias"],
)
def test_docker_minio_transport_rejects_untrusted_isolation_topology(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unsafe: str,
) -> None:
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    release_path, release_digest = _release(tmp_path)
    release = load_personal_dev_trusted_release(release_path, release_digest)
    suffix = release_digest[:12]
    minio_name = f"loom-personal-dev-minio-restore-{suffix}"
    network_name = f"loom-personal-dev-restore-{suffix}"
    container, network = _isolated_docker_documents(
        minio_image=release.images.minio,
        minio_name=minio_name,
        network_name=network_name,
    )
    if unsafe == "image":
        container[0]["Config"] = {"Image": "quay.io/minio/minio@sha256:" + "f" * 64}
    elif unsafe == "published-port":
        container[0]["HostConfig"] = {
            "NetworkMode": network_name,
            "PortBindings": {"9000/tcp": [{"HostIp": "0.0.0.0", "HostPort": "9000"}]},
        }
    elif unsafe == "wrong-network":
        container[0]["NetworkSettings"] = {"Networks": {"bridge": {}}, "Ports": {}}
    elif unsafe == "external-network":
        network[0]["Internal"] = False
    elif unsafe == "extra-peer":
        network[0]["Containers"]["d" * 64] = {  # type: ignore[index]
            "EndpointID": "e" * 64,
            "IPv4Address": "172.31.0.3/16",
            "IPv6Address": "",
            "MacAddress": "02:42:ac:1f:00:03",
            "Name": "unexpected-peer",
        }
    else:
        networks = container[0]["NetworkSettings"]["Networks"]  # type: ignore[index]
        networks[network_name]["Aliases"] = ["a" * 12]  # type: ignore[index]
    _fake_docker(
        tmp_path,
        monkeypatch,
        container_document=container,
        network_document=network,
    )
    payload_root = tmp_path / "payloads"
    payload_root.mkdir(mode=0o700)
    env_file = tmp_path / "restore.env"
    env_file.write_text("SENSITIVE_RESTORE_MARKER=value\n", encoding="utf-8")
    env_file.chmod(0o600)

    with pytest.raises(ValueError):
        command._DockerMinioTransport(
            client_image=release.images.minio_client,
            minio_image=release.images.minio,
            restore_env_file=env_file,
            payload_root=payload_root,
            isolated_minio_name=minio_name,
            isolated_network_name=network_name,
        )


# Production break caught: client execution loses its pinned image/network/env/read-only mount or argv separation.
def test_docker_minio_transport_uses_fixed_isolated_client_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    release_path, release_digest = _release(tmp_path)
    release = load_personal_dev_trusted_release(release_path, release_digest)
    suffix = release_digest[:12]
    minio_name = f"loom-personal-dev-minio-restore-{suffix}"
    network_name = f"loom-personal-dev-restore-{suffix}"
    container, network = _isolated_docker_documents(
        minio_image=release.images.minio,
        minio_name=minio_name,
        network_name=network_name,
    )
    log = _fake_docker(
        tmp_path,
        monkeypatch,
        container_document=container,
        network_document=network,
    )
    payload_root = tmp_path / "payloads"
    payload_root.mkdir(mode=0o700)
    digest_path = payload_root / ("d" * 64)
    digest_path.write_bytes(b"payload")
    digest_path.chmod(0o600)
    env_file = tmp_path / "restore.env"
    secret_marker = "SENSITIVE_RESTORE_MARKER"
    env_file.write_text(f"{secret_marker}=value\n", encoding="utf-8")
    env_file.chmod(0o600)
    raw_target = "restore/artifacts/owner/raw key $(marker).tar"
    transport = command._DockerMinioTransport(
        client_image=release.images.minio_client,
        minio_image=release.images.minio,
        restore_env_file=env_file,
        payload_root=payload_root,
        isolated_minio_name=minio_name,
        isolated_network_name=network_name,
    )

    result = transport.run(
        ("cp", "--attr", "Content-Type=application/x-tar", str(digest_path), raw_target),
        maximum_stdout_bytes=4096,
        timeout_seconds=60,
    )

    captured = capsys.readouterr()
    calls = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    client_calls = [call for call in calls if call[0] == "run"]
    assert result == PersonalDevMinioCommandResult(0, b"bounded", b"")
    assert captured == ("", "")
    assert len(client_calls) == 1
    client_call = client_calls[0]
    cid_path = Path(client_call[client_call.index("--cidfile") + 1])
    env_argument = client_call[client_call.index("--env-file") + 1]
    assert client_call[:2] == ["run", "--rm"]
    assert client_call[client_call.index("--network") + 1] == network_name
    assert client_call[client_call.index("--mount") + 1] == (
        f"type=bind,src={payload_root},dst=/loom-payloads,readonly"
    )
    assert env_argument.startswith("/proc/self/fd/")
    assert env_argument.removeprefix("/proc/self/fd/").isdigit()
    assert client_call[client_call.index("--entrypoint") :] == [
        "--entrypoint",
        "/bin/sh",
        release.images.minio_client,
        "-euc",
        'export MC_HOST_restore="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio-restore:9000"; exec mc "$@"',
        "sh",
        "cp",
        "--attr",
        "Content-Type=application/x-tar",
        f"/loom-payloads/{digest_path.name}",
        raw_target,
    ]
    assert not cid_path.exists()
    assert client_call.count(raw_target) == 1
    assert secret_marker not in json.dumps(calls)


# Production break caught: restore accepts caller-chosen names/images or emits more than the safe summary.
def test_restore_minio_backup_derives_release_images_and_emits_safe_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    release_path, release_digest = _release(tmp_path)
    release = load_personal_dev_trusted_release(release_path, release_digest)
    suffix = release_digest[:12]
    minio_name = f"loom-personal-dev-minio-restore-{suffix}"
    network_name = f"loom-personal-dev-restore-{suffix}"
    container, network = _isolated_docker_documents(
        minio_image=release.images.minio,
        minio_name=minio_name,
        network_name=network_name,
    )
    _fake_docker(
        tmp_path,
        monkeypatch,
        container_document=container,
        network_document=network,
    )
    env_file = tmp_path / "restore.env"
    env_file.write_text("MINIO_ROOT_USER=restore\nMINIO_ROOT_PASSWORD=marker\n", encoding="utf-8")
    env_file.chmod(0o600)
    manifest = PersonalDevMinioManifest(())
    observed: dict[str, object] = {}

    def _restore(**kwargs: object) -> PersonalDevMinioManifest:
        observed.update(kwargs)
        return manifest

    monkeypatch.setattr(command, "restore_personal_dev_minio_backup", _restore)

    result = dispatch(_restore_minio_argv(release_path, release_digest, tmp_path))

    captured = capsys.readouterr()
    assert result == 0
    assert captured.err == ""
    assert captured.out == (
        '{"object_count":0,"payload_bytes":0,'
        '"schema":"loom-personal-dev-minio-backup-summary-v1",'
        f'"source_manifest_sha256":"{hashlib.sha256(manifest.canonical_bytes).hexdigest()}"}}\n'
    )
    transport = observed["transport"]
    assert transport._client_image == release.images.minio_client
    assert transport._minio_image == release.images.minio
    assert observed["source_manifest_path"] == tmp_path / "minio.source.json"
    assert observed["payload_root"] == tmp_path / "payloads"
    assert observed["restored_manifest_path"] == tmp_path / "minio.restored.json"


# Production break caught: capture accepts zero/multiple/non-Running pods from the exact selector.
@pytest.mark.parametrize("pod_state", ["empty", "multiple", "pending"])
def test_kubectl_minio_transport_requires_one_exact_running_pod(pod_state: str) -> None:
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "labels": {"app": "loom-dev-minio"},
            "name": "loom-dev-minio-0",
            "namespace": "loom-dev",
        },
        "status": {"phase": "Pending" if pod_state == "pending" else "Running"},
    }
    items = [] if pod_state == "empty" else [pod]
    if pod_state == "multiple":
        items.append(
            {
                **pod,
                "metadata": {**pod["metadata"], "name": "loom-dev-minio-1"},
            }
        )
    payload = json.dumps(
        {"apiVersion": "v1", "items": items, "kind": "List", "metadata": {}},
        sort_keys=True,
        separators=(",", ":"),
    )

    class _Runner:
        def run(self, argv: object, *, timeout_seconds: int) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(argv, 0, payload, "")

    with pytest.raises(ValueError):
        command._KubectlMinioTransport(_Runner(), namespace="loom-dev")


# Production break caught: a stream uses the mutable kubeconfig path or omits post-command identity validation.
def test_kubectl_stream_uses_only_proc_fd_and_revalidates_kubeconfig(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_path = tmp_path / "observed-kubeconfig-path"
    executable = tmp_path / "kubectl"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import os\n"
        "import pathlib\n"
        "import sys\n"
        "pathlib.Path(os.environ['LOOM_TEST_OBSERVED']).write_text(sys.argv[2], encoding='utf-8')\n"
        "pathlib.Path(os.environ['LOOM_TEST_MUTATE']).write_text('sensitive-kubeconfig-change', encoding='utf-8')\n"
        "sys.stdout.buffer.write(b'x')\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("LOOM_TEST_OBSERVED", str(observed_path))
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    monkeypatch.setenv("LOOM_TEST_MUTATE", str(kubeconfig))
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    runner = command._SubprocessKubectlRunner(kubeconfig)

    with pytest.raises(OSError, match="kubeconfig changed"):
        runner.stream(
            ["exec", "pod"],
            destination=io.BytesIO(),
            maximum_stdout_bytes=1,
            expected_size=1,
            maximum_stderr_bytes=64,
            timeout_seconds=5,
        )

    assert observed_path.read_text(encoding="utf-8").startswith("/proc/self/fd/")
    assert str(kubeconfig) not in observed_path.read_text(encoding="utf-8")


# Production break caught: a timeout or stderr overflow leaves its kubectl child running.
@pytest.mark.parametrize("failure", ["timeout", "stderr-bound"])
def test_kubectl_stream_kills_and_reaps_process_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    pid_path = tmp_path / "kubectl.pid"
    executable = tmp_path / "kubectl"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import os\n"
        "import pathlib\n"
        "import sys\n"
        "import time\n"
        "pathlib.Path(os.environ['LOOM_TEST_PID']).write_text(str(os.getpid()), encoding='ascii')\n"
        "if os.environ['LOOM_TEST_FAILURE'] == 'stderr-bound':\n"
        "    sys.stderr.buffer.write(b'sensitive-stderr-marker' * 8)\n"
        "    sys.stderr.buffer.flush()\n"
        "time.sleep(30)\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("LOOM_TEST_PID", str(pid_path))
    monkeypatch.setenv("LOOM_TEST_FAILURE", failure)
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    runner = command._SubprocessKubectlRunner(_reviewed_kubeconfig(tmp_path))

    with pytest.raises((OSError, subprocess.TimeoutExpired)) as raised:
        runner.stream(
            ["exec", "pod"],
            destination=io.BytesIO(),
            maximum_stdout_bytes=1,
            expected_size=1,
            maximum_stderr_bytes=32,
            timeout_seconds=1,
        )

    pid = int(pid_path.read_text(encoding="ascii"))
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
    assert "sensitive-stderr-marker" not in str(raised.value)


# Production break caught: Docker readback buffers payloads or trusts a non-exact byte count.
def test_docker_minio_stream_hashes_incrementally_without_retaining_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    release_path, release_digest = _release(tmp_path)
    release = load_personal_dev_trusted_release(release_path, release_digest)
    suffix = release_digest[:12]
    minio_name = f"loom-personal-dev-minio-restore-{suffix}"
    network_name = f"loom-personal-dev-restore-{suffix}"
    container, network = _isolated_docker_documents(
        minio_image=release.images.minio,
        minio_name=minio_name,
        network_name=network_name,
    )
    _fake_docker(
        tmp_path,
        monkeypatch,
        container_document=container,
        network_document=network,
        stdout=b"readback-payload",
    )
    payload_root = tmp_path / "payloads"
    payload_root.mkdir(mode=0o700)
    env_file = tmp_path / "restore.env"
    env_file.write_text("MINIO_ROOT_USER=restore\nMINIO_ROOT_PASSWORD=marker\n", encoding="utf-8")
    env_file.chmod(0o600)
    transport = command._DockerMinioTransport(
        client_image=release.images.minio_client,
        minio_image=release.images.minio,
        restore_env_file=env_file,
        payload_root=payload_root,
        isolated_minio_name=minio_name,
        isolated_network_name=network_name,
    )
    destination = io.BytesIO()

    digest = transport.stream(
        ("cat", "restore/artifacts/raw key"),
        destination=destination,
        expected_size=len(b"readback-payload"),
        timeout_seconds=60,
    )

    assert destination.getvalue() == b"readback-payload"
    assert digest == hashlib.sha256(b"readback-payload").hexdigest()


# Production break caught: release-bound isolated restore names can be caller-selected.
@pytest.mark.parametrize("option", ["--isolated-minio-name", "--isolated-network-name"])
def test_restore_minio_backup_rejects_names_not_bound_to_release(
    tmp_path: Path,
    option: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release, digest = _release(tmp_path)
    argv = _restore_minio_argv(release, digest, tmp_path)
    argv[argv.index(option) + 1] = "sensitive-caller-selected-name"

    result = dispatch(argv)

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: personal-dev MinIO backup inputs are invalid\n"


# Production break caught: a looping payload-root symlink escapes the stable public error surface.
def test_restore_minio_backup_sanitizes_payload_root_symlink_loop(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release, digest = _release(tmp_path)
    env_file = tmp_path / "restore.env"
    env_file.write_text("MINIO_ROOT_USER=restore\nMINIO_ROOT_PASSWORD=marker\n", encoding="utf-8")
    env_file.chmod(0o600)
    payload_root = tmp_path / "looped-payloads"
    payload_root.symlink_to(payload_root, target_is_directory=True)
    argv = _restore_minio_argv(release, digest, tmp_path)
    argv[argv.index("--payload-root") + 1] = str(payload_root)

    result = dispatch(argv)

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: personal-dev MinIO backup inputs are invalid\n"


# Production break caught: sensitive Kubernetes/workflow/timeout failures reach public streams.
@pytest.mark.parametrize("failure", ["kubeconfig", "object-metadata", "timeout"])
def test_capture_minio_backup_sanitizes_every_public_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failure: str,
) -> None:
    marker = f"SENSITIVE_{failure}_MARKER"
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    kubeconfig = _reviewed_kubeconfig(tmp_path)
    if failure == "kubeconfig":

        class _Runner:
            def __init__(self, _path: Path) -> None:
                raise OSError(marker)

        monkeypatch.setattr(command, "_SubprocessKubectlRunner", _Runner)
    else:

        class _Runner:
            def __init__(self, _path: Path) -> None:
                pass

        class _Transport:
            def __init__(self, _runner: object, *, namespace: str) -> None:
                assert namespace == "loom-dev"

        def _failed_capture(**_kwargs: object) -> PersonalDevMinioManifest:
            if failure == "timeout":
                raise subprocess.TimeoutExpired(["kubectl", marker], 60, stderr=marker)
            raise ValueError(f"raw-key={marker}; metadata={marker}")

        monkeypatch.setattr(command, "_SubprocessKubectlRunner", _Runner)
        monkeypatch.setattr(command, "_KubectlMinioTransport", _Transport)
        monkeypatch.setattr(command, "capture_personal_dev_minio_backup", _failed_capture)

    result = dispatch(_capture_minio_argv(kubeconfig, tmp_path))

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: personal-dev MinIO backup inputs are invalid\n"
    assert marker not in captured.err


# Production break caught: Docker inspect/stderr/transport markers reach public streams.
@pytest.mark.parametrize("failure", ["inspect", "command-stderr", "transport"])
def test_restore_minio_backup_sanitizes_every_public_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failure: str,
) -> None:
    marker = f"SENSITIVE_{failure}_MARKER"
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    release_path, release_digest = _release(tmp_path)
    release = load_personal_dev_trusted_release(release_path, release_digest)
    suffix = release_digest[:12]
    minio_name = f"loom-personal-dev-minio-restore-{suffix}"
    network_name = f"loom-personal-dev-restore-{suffix}"
    container, network = _isolated_docker_documents(
        minio_image=release.images.minio,
        minio_name=minio_name,
        network_name=network_name,
    )
    if failure == "inspect":
        container[0]["Config"] = {"Image": marker}
    _fake_docker(
        tmp_path,
        monkeypatch,
        container_document=container,
        network_document=network,
        stdout=b"" if failure == "command-stderr" else b"bounded",
        stderr=marker.encode() if failure == "command-stderr" else b"",
        returncode=9 if failure == "command-stderr" else 0,
    )
    env_file = tmp_path / "restore.env"
    env_file.write_text(
        f"MINIO_ROOT_USER=restore\nMINIO_ROOT_PASSWORD={marker}\n", encoding="utf-8"
    )
    env_file.chmod(0o600)
    if failure == "transport":

        class _Transport:
            def __init__(self, **_kwargs: object) -> None:
                raise TimeoutError(marker)

        monkeypatch.setattr(command, "_DockerMinioTransport", _Transport)
    elif failure == "command-stderr":

        def _failed_restore(*, transport: object, **_kwargs: object) -> PersonalDevMinioManifest:
            result = transport.run(
                ("ls", "--json", "restore"),
                maximum_stdout_bytes=4096,
                timeout_seconds=60,
            )
            raise ValueError(result.stderr.decode("ascii"))

        monkeypatch.setattr(command, "restore_personal_dev_minio_backup", _failed_restore)

    result = dispatch(_restore_minio_argv(release_path, release_digest, tmp_path))

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: personal-dev MinIO backup inputs are invalid\n"
    assert marker not in captured.err


def test_minio_stream_helpers_discard_bounded_sensitive_stderr(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = b"SENSITIVE_STREAM_STDERR_MARKER"
    executable = tmp_path / "kubectl"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import sys\n"
        f"sys.stderr.buffer.write({marker!r})\n"
        "raise SystemExit(9)\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    docker = tmp_path / "docker"
    docker.write_text(executable.read_text(encoding="utf-8"), encoding="utf-8")
    docker.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")

    kubectl_result = command._SubprocessKubectlRunner(_reviewed_kubeconfig(tmp_path)).stream(
        ["exec", "pod"],
        destination=None,
        maximum_stdout_bytes=0,
        expected_size=None,
        maximum_stderr_bytes=1024,
        timeout_seconds=5,
        retain_stderr=False,
    )
    docker_result = command._stream_docker_command(
        ["version"],
        destination=None,
        maximum_stdout_bytes=0,
        expected_size=None,
        maximum_stderr_bytes=1024,
        timeout_seconds=5,
        retain_stderr=False,
    )

    for result in (kubectl_result, docker_result):
        assert result.returncode == 9
        assert result.stderr
        assert marker not in result.stderr


@pytest.mark.parametrize("surface", ["pod", "docker"])
def test_minio_json_surfaces_normalize_bounded_recursion_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    surface: str,
) -> None:
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    deeply_nested = "[" * 1100 + "0" + "]" * 1100
    if surface == "pod":

        class _Runner:
            def run(
                self,
                argv: object,
                *,
                timeout_seconds: int,
            ) -> subprocess.CompletedProcess[str]:
                return subprocess.CompletedProcess(argv, 0, deeply_nested, "")

        with pytest.raises(ValueError):
            command._KubectlMinioTransport(_Runner(), namespace="loom-dev")
        return

    tmp_path.chmod(0o700)
    payload_root = tmp_path / "payloads"
    payload_root.mkdir(mode=0o700)
    env_file = tmp_path / "restore.env"
    env_file.write_text("MINIO_ROOT_USER=restore\n", encoding="utf-8")
    env_file.chmod(0o600)

    def _deep_inspect(*_args: object, **_kwargs: object) -> PersonalDevMinioCommandResult:
        return PersonalDevMinioCommandResult(0, deeply_nested.encode("ascii"), b"")

    monkeypatch.setattr(command, "_stream_docker_command", _deep_inspect)
    with pytest.raises(ValueError):
        command._DockerMinioTransport(
            client_image="quay.io/minio/mc@sha256:" + "a" * 64,
            minio_image="quay.io/minio/minio@sha256:" + "b" * 64,
            restore_env_file=env_file,
            payload_root=payload_root,
            isolated_minio_name="loom-personal-dev-minio-restore-123456789abc",
            isolated_network_name="loom-personal-dev-restore-123456789abc",
        )


def test_docker_minio_transport_requires_owner_only_restore_env_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    unsafe_parent = tmp_path / "unsafe-parent"
    unsafe_parent.mkdir(mode=0o755)
    env_file = unsafe_parent / "restore.env"
    env_file.write_text("MINIO_ROOT_USER=restore\n", encoding="utf-8")
    env_file.chmod(0o600)
    payload_root = tmp_path / "payloads"
    payload_root.mkdir(mode=0o700)

    def _must_not_run(*_args: object, **_kwargs: object) -> PersonalDevMinioCommandResult:
        raise AssertionError("Docker was invoked before env-parent validation")

    monkeypatch.setattr(command, "_stream_docker_command", _must_not_run)
    with pytest.raises(ValueError):
        command._DockerMinioTransport(
            client_image="quay.io/minio/mc@sha256:" + "a" * 64,
            minio_image="quay.io/minio/minio@sha256:" + "b" * 64,
            restore_env_file=env_file,
            payload_root=payload_root,
            isolated_minio_name="loom-personal-dev-minio-restore-123456789abc",
            isolated_network_name="loom-personal-dev-restore-123456789abc",
        )


@pytest.mark.parametrize("failure", ["path-replaced", "stream-timeout"])
def test_docker_minio_client_uses_verified_env_fd_and_force_removes_recorded_cid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    tmp_path.chmod(0o700)
    minio_name = "loom-personal-dev-minio-restore-123456789abc"
    network_name = "loom-personal-dev-restore-123456789abc"
    minio_image = "quay.io/minio/minio@sha256:" + "b" * 64
    client_image = "quay.io/minio/mc@sha256:" + "a" * 64
    container, network = _isolated_docker_documents(
        minio_image=minio_image,
        minio_name=minio_name,
        network_name=network_name,
    )
    payload_root = tmp_path / "payloads"
    payload_root.mkdir(mode=0o700)
    env_file = tmp_path / "restore.env"
    original = b"MINIO_ROOT_USER=original\n"
    env_file.write_bytes(original)
    env_file.chmod(0o600)
    observed: dict[str, object] = {}

    def _stream(
        argv: list[str] | tuple[str, ...],
        **kwargs: object,
    ) -> PersonalDevMinioCommandResult:
        values = list(argv)
        if values[0] == "inspect":
            return PersonalDevMinioCommandResult(
                0,
                json.dumps(container, sort_keys=True, separators=(",", ":")).encode(),
                b"",
            )
        if values[:2] == ["network", "inspect"]:
            return PersonalDevMinioCommandResult(
                0,
                json.dumps(network, sort_keys=True, separators=(",", ":")).encode(),
                b"",
            )
        assert values[0] == "run"
        env_argument = values[values.index("--env-file") + 1]
        passed = kwargs.get("pass_fds")
        assert isinstance(passed, tuple) and len(passed) == 1
        descriptor = passed[0]
        assert env_argument == f"/proc/self/fd/{descriptor}"
        replacement = tmp_path / "replacement.env"
        replacement.write_bytes(b"MINIO_ROOT_USER=replaced\n")
        replacement.chmod(0o600)
        os.replace(replacement, env_file)
        observed["env"] = os.pread(descriptor, 4096, 0)
        cid_path = Path(values[values.index("--cidfile") + 1])
        cid_path.write_text("c" * 64 + "\n", encoding="ascii")
        observed["cid_path"] = cid_path
        if failure == "stream-timeout":
            raise subprocess.TimeoutExpired(["docker", *values], 1)
        return PersonalDevMinioCommandResult(0, b"bounded", b"")

    cleanup_calls: list[tuple[str, ...]] = []

    def _cleanup(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        cleanup_calls.append(tuple(argv))
        return subprocess.CompletedProcess(argv, 0, b"", b"")

    monkeypatch.setattr(command, "_stream_docker_command", _stream)
    monkeypatch.setattr(command.subprocess, "run", _cleanup)
    transport = command._DockerMinioTransport(
        client_image=client_image,
        minio_image=minio_image,
        restore_env_file=env_file,
        payload_root=payload_root,
        isolated_minio_name=minio_name,
        isolated_network_name=network_name,
    )

    with pytest.raises((OSError, subprocess.TimeoutExpired)):
        transport.run(("ls", "--json", "restore"), maximum_stdout_bytes=4096, timeout_seconds=1)

    assert observed["env"] == original
    assert cleanup_calls == [("docker", "rm", "--force", "c" * 64)]
    assert not Path(observed["cid_path"]).exists()


def test_docker_minio_client_rejects_noncanonical_cid_file(tmp_path: Path) -> None:
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    cid_path = tmp_path / "cid"
    cid_path.write_text(" " + "c" * 64, encoding="ascii")

    with pytest.raises(OSError, match="CID is invalid"):
        command._DockerMinioTransport._read_client_cid(cid_path)


def test_docker_minio_client_closes_the_verified_env_fd_if_cid_setup_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = importlib.import_module("loom_cli.personal_dev_control_plane_cmd")
    tmp_path.chmod(0o700)
    minio_name = "loom-personal-dev-minio-restore-123456789abc"
    network_name = "loom-personal-dev-restore-123456789abc"
    minio_image = "quay.io/minio/minio@sha256:" + "b" * 64
    container, network = _isolated_docker_documents(
        minio_image=minio_image,
        minio_name=minio_name,
        network_name=network_name,
    )
    _fake_docker(
        tmp_path,
        monkeypatch,
        container_document=container,
        network_document=network,
    )
    payload_root = tmp_path / "payloads"
    payload_root.mkdir(mode=0o700)
    env_file = tmp_path / "restore.env"
    env_file.write_text("MINIO_ROOT_USER=restore\n", encoding="utf-8")
    env_file.chmod(0o600)
    transport = command._DockerMinioTransport(
        client_image="quay.io/minio/mc@sha256:" + "a" * 64,
        minio_image=minio_image,
        restore_env_file=env_file,
        payload_root=payload_root,
        isolated_minio_name=minio_name,
        isolated_network_name=network_name,
    )
    real_open = command._open_owner_only_file
    observed: dict[str, int] = {}

    def _record_open(*args: object, **kwargs: object) -> int:
        descriptor = real_open(*args, **kwargs)
        observed["descriptor"] = descriptor
        return descriptor

    def _fail_mkdtemp(*_args: object, **_kwargs: object) -> str:
        raise OSError("CID setup failed")

    monkeypatch.setattr(command, "_open_owner_only_file", _record_open)
    monkeypatch.setattr(command.tempfile, "mkdtemp", _fail_mkdtemp)

    with pytest.raises(OSError, match="CID setup failed"):
        transport.run(("ls", "--json", "restore"), maximum_stdout_bytes=4096, timeout_seconds=1)

    descriptor = observed["descriptor"]
    descriptor_closed = False
    try:
        os.fstat(descriptor)
    except OSError:
        descriptor_closed = True
    finally:
        if not descriptor_closed:
            os.close(descriptor)
    assert descriptor_closed
