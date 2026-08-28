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
from loom_cli.__main__ import main
from loom_cli.admin_cmd import dispatch
from tests.unit.test_personal_dev_acceptance_evidence import (
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
        "schema_version": 3,
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
                "b1c136b8577f3813c62588d6930db21b0f2343b7f70278836741387c43c33761"
            ),
            "database_metadata_sha256": "c" * 64,
            "database_sha256": "d" * 64,
            "java_database_metadata_sha256": "e" * 64,
            "java_database_sha256": "f" * 64,
            "lock_sha256": "1" * 64,
            "trivy_version": "v0.70.0",
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


def _acceptance_plan(
    tmp_path: Path,
    release_path: Path,
    release_digest: str,
    *,
    profile_path: Path = _PROFILE,
):
    profile = load_personal_dev_control_plane_profile(profile_path)
    release = load_personal_dev_trusted_release(release_path, release_digest)
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
            "restored_schema_head": "0120",
            "restored_state_sha256": "c" * 64,
            "source_schema_head": "0120",
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
        "acceptance_owner": {
            "team_id": "00000000-0000-0000-0000-000000000201",
            "user_id": "00000000-0000-0000-0000-000000000301",
        },
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
        "schema_version": 1,
        "source": {"commit": release.source_sha, "tree": release.source_tree},
        "storage": {
            "backup_restore_evidence_sha256": hashlib.sha256(backup_payload).hexdigest(),
            "schema_head": "0120",
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
):
    _path, _digest, acceptance = _acceptance_plan(
        tmp_path,
        release_path,
        release_digest,
    )
    value = acceptance.canonical_value()
    value.pop("acceptance_owner")
    value.pop("window")
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
) -> list[str]:
    return [
        "personal-dev-control-plane",
        "status-acceptance",
        "--namespace",
        "loom-dev",
        "--kubeconfig",
        str(kubeconfig),
        "--file",
        str(_PROFILE),
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
) -> list[str]:
    return [
        "personal-dev-control-plane",
        "render-operational",
        "--file",
        str(_PROFILE),
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
) -> list[str]:
    return [
        "personal-dev-control-plane",
        "status-operational",
        "--namespace",
        "loom-dev",
        "--kubeconfig",
        str(kubeconfig),
        "--file",
        str(_PROFILE),
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
) -> tuple[Path, str, Path, str, Path, Path, str]:
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
    result_value = _result_value(plan)
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


def test_verify_acceptance_result_emits_canonical_secret_free_projection(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    (
        plan_path,
        plan_sha256,
        result_path,
        result_sha256,
        rollback_manifest_path,
        rollback_path,
        rollback_sha256,
    ) = _acceptance_result_files(tmp_path)
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
            "0120",
        ]
    )

    captured = capsys.readouterr()
    assert result == 0
    assert captured.out.encode("ascii") == job
    assert captured.err.encode("ascii") == plan_json + b"\n"
    assert captured_inputs["backup_evidence_sha256"] == "a" * 64
    assert captured_inputs["predecessor_shadow_sha256"] == "b" * 64
    assert captured_inputs["expected_predecessor_head"] == "0112"
    assert captured_inputs["expected_target_head"] == "0120"
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
        "0120",
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
    assert plan["target"]["schema_head"] == "0120"
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
            "0120",
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
    plan_path, plan_digest, plan = _acceptance_plan(
        tmp_path,
        release_path,
        release_digest,
    )
    profile = load_personal_dev_control_plane_profile(_PROFILE)
    release = load_personal_dev_trusted_release(release_path, release_digest)
    expected = render_acceptance_personal_dev_control_plane(
        profile,
        release,
        plan,
        now=_NOW,
    )

    result = dispatch(_acceptance_argv(release_path, release_digest, plan_path, plan_digest))

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


def test_render_acceptance_loads_v2_two_owner_local_input_without_shape_change(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    profile_path = tmp_path / "personal-dev-control-plane-v2.toml"
    profile_path.write_text(
        _PROFILE.read_text(encoding="utf-8")
        .replace("global_live_instances = 16", "global_live_instances = 2")
        .replace("builder_global_concurrency = 4", "builder_global_concurrency = 2"),
        encoding="utf-8",
    )
    profile_path.chmod(0o600)
    plan_path, _plan_digest, v1_plan = _acceptance_plan(
        tmp_path,
        release_path,
        release_digest,
        profile_path=profile_path,
    )
    profile = load_personal_dev_control_plane_profile(profile_path)
    release = load_personal_dev_trusted_release(release_path, release_digest)
    v1_expected = render_acceptance_personal_dev_control_plane(
        profile,
        release,
        v1_plan,
        now=_NOW,
    )
    value = v1_plan.canonical_value()
    value["schema_version"] = 2
    owner_0 = value.pop("acceptance_owner")
    owner_1 = {
        "team_id": "00000000-0000-0000-0000-000000000006",
        "user_id": "00000000-0000-0000-0000-000000000005",
    }
    value["acceptance_owners"] = sorted(
        [owner_0, owner_1],
        key=lambda item: (item["team_id"], item["user_id"]),
    )
    value["quotas"]["global_live_instances"] = 2
    value["quotas"]["builder_global_concurrency"] = 2
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    plan_path.write_bytes(payload)
    plan_path.chmod(0o600)
    plan_digest = hashlib.sha256(payload).hexdigest()
    plan = load_personal_dev_acceptance_plan(plan_path, plan_digest)
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
    assert expected.resource_count == v1_expected.resource_count
    assert {
        (item["kind"], item["metadata"].get("namespace", ""), item["metadata"]["name"])
        for item in yaml.safe_load_all(expected.yaml_text)
        if item and item["kind"] != "Job"
    } == {
        (item["kind"], item["metadata"].get("namespace", ""), item["metadata"]["name"])
        for item in yaml.safe_load_all(v1_expected.yaml_text)
        if item and item["kind"] != "Job"
    }


def test_render_operational_emits_durable_plan_binding(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_path, release_digest = _release(tmp_path)
    plan_path, plan_digest, plan = _operational_plan(
        tmp_path,
        release_path,
        release_digest,
    )
    profile = load_personal_dev_control_plane_profile(_PROFILE)
    release = load_personal_dev_trusted_release(release_path, release_digest)
    expected = render_operational_personal_dev_control_plane(
        profile,
        release,
        plan,
        now=_NOW,
    )

    result = dispatch(_operational_argv(release_path, release_digest, plan_path, plan_digest))

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
    plan_path, plan_digest, _plan = _acceptance_plan(
        tmp_path,
        release_path,
        release_digest,
    )
    if operation == "render-acceptance":
        argv = _acceptance_argv(release_path, release_digest, plan_path, plan_digest)
    else:
        kubeconfig = _reviewed_kubeconfig(tmp_path)
        argv = _acceptance_status_argv(
            release_path,
            release_digest,
            plan_path,
            plan_digest,
            kubeconfig.resolve(),
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
    plan_path, plan_digest, plan = _acceptance_plan(
        tmp_path,
        release_path,
        release_digest,
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
        assert expected.resource_count == 38
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
    plan_path, plan_digest, plan = _operational_plan(
        tmp_path,
        release_path,
        release_digest,
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
        assert expected.resource_count == 38
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
    plan_path, plan_digest, _plan = _acceptance_plan(
        tmp_path,
        release_path,
        release_digest,
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
