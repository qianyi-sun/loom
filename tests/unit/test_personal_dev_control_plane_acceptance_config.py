from __future__ import annotations

import copy
import hashlib
import json
import os
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from loom.personal_dev_control_plane_config import (
    PersonalDevAcceptancePlanError,
    PersonalDevControlPlaneProfile,
    PersonalDevOperationalPlanError,
    PersonalDevTrustedRelease,
    load_personal_dev_acceptance_plan,
    load_personal_dev_control_plane_profile,
    load_personal_dev_operational_plan,
    load_personal_dev_trusted_release,
    validate_personal_dev_acceptance_plan,
    validate_personal_dev_operational_plan,
)

_ROOT = Path(__file__).resolve().parents[2]
_PROFILE_PATH = _ROOT / "deploy/dev-fleet/personal-dev-control-plane.toml"
_NOW = datetime(2026, 8, 17, 21, 0, 0, tzinfo=UTC)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _scanner_value() -> dict[str, str]:
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


def _release_value() -> dict[str, Any]:
    return {
        "schema_version": 2,
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
            "personal_dev_scanner_cache": (
                "ghcr.io/qianyi-sun/loom-personal-dev-scanner-cache@sha256:" + "a" * 64
            ),
            "postgres": "docker.io/library/postgres@sha256:" + "6" * 64,
            "minio": "quay.io/minio/minio@sha256:" + "7" * 64,
            "minio_client": "quay.io/minio/mc@sha256:" + "9" * 64,
        },
        "scanner": _scanner_value(),
        "release_evidence_sha256": "8" * 64,
    }


def _write_release(tmp_path: Path) -> tuple[PersonalDevTrustedRelease, str]:
    payload = _canonical(_release_value())
    path = tmp_path / "trusted-release.json"
    path.write_bytes(payload)
    path.chmod(0o600)
    digest = hashlib.sha256(payload).hexdigest()
    return load_personal_dev_trusted_release(path, digest), digest


def _protocol_sha256(profile: PersonalDevControlPlaneProfile) -> str:
    return hashlib.sha256(_canonical(dict(profile.protocol_versions))).hexdigest()


def _quota_value(profile: PersonalDevControlPlaneProfile) -> dict[str, int]:
    return {
        "global_live_instances": profile.limits.global_live_instances,
        "per_owner_live_instances": profile.limits.per_owner_live_instances,
        "per_owner_aggregate_min_slots": profile.limits.per_owner_aggregate_min_slots,
        "per_owner_aggregate_max_slots": profile.limits.per_owner_aggregate_max_slots,
        "builder_global_concurrency": profile.limits.builder_global_concurrency,
        "builder_per_owner_concurrency": profile.limits.builder_per_owner_concurrency,
        "source_max_archive_bytes": profile.limits.source_max_archive_bytes,
        "candidate_retained_count": profile.limits.candidate_retained_count,
        "candidate_retained_bytes": profile.limits.candidate_retained_bytes,
    }


def _plan_value(
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
    release_sha256: str,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "source": {
            "commit": release.source_sha,
            "tree": release.source_tree,
        },
        "release": {
            "trusted_release_sha256": release_sha256,
            "release_evidence_sha256": release.release_evidence_sha256,
            "shadow_manifest_sha256": "a" * 64,
            "images": release.canonical_value()["images"],
        },
        "storage": {
            "schema_head": "0120",
            "backup_restore_evidence_sha256": "b" * 64,
        },
        "activation": {
            "public_key_sha256": "c" * 64,
            "key_id": "personal-dev-agent-v1",
        },
        "builder": {
            "runtime_class_name": profile.builder.runtime_class_name,
            "runtime_handler": profile.builder.runtime_handler,
            "runtime_profile_sha256": profile.builder.runtime_profile_sha256,
            "trusted_launcher_profile_sha256": "e" * 64,
            "scanner_binary_sha256": release.scanner.binary_sha256,
            "scanner_cache_identity_sha256": release.scanner.cache_identity_sha256,
            "scanner_database_sha256": release.scanner.database_sha256,
            "scanner_database_metadata_sha256": (release.scanner.database_metadata_sha256),
            "scanner_java_database_sha256": release.scanner.java_database_sha256,
            "scanner_java_database_metadata_sha256": (
                release.scanner.java_database_metadata_sha256
            ),
            "scanner_finding_policy_sha256": "3" * 64,
            "publisher_identity": profile.builder.publisher_identity,
            "registry_prefix": profile.builder.registry_prefix,
            "protocol_map_sha256": _protocol_sha256(profile),
        },
        "manager": {
            "authority_incarnation": "00000000-0000-0000-0000-000000000101",
            "configuration_epoch": 7,
            "execution_state": "shadow",
            "execution_epoch": 0,
            "executable_new_capacity_ceiling": 0,
        },
        "principals": {
            "lifecycle_principal_id": "personal-dev-lifecycle",
            "reporter_principal_id": "personal-dev-reporter",
        },
        "quotas": _quota_value(profile),
        "acceptance_owner": {
            "team_id": "00000000-0000-0000-0000-000000000201",
            "user_id": "00000000-0000-0000-0000-000000000301",
        },
        "window": {
            "started_at": "2026-08-17T20:00:00Z",
            "expires_at": "2026-08-17T23:00:00Z",
            "rollback_expires_at": "2026-08-18T23:00:00Z",
        },
    }


def _v2_plan_value(value: dict[str, Any]) -> dict[str, Any]:
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
    return value


def _write_plan(
    tmp_path: Path,
    value: object,
    *,
    payload: bytes | None = None,
) -> tuple[Path, str]:
    encoded = _canonical(value) if payload is None else payload
    path = tmp_path / "acceptance-plan.json"
    path.write_bytes(encoded)
    path.chmod(0o600)
    return path, hashlib.sha256(encoded).hexdigest()


def _inputs(
    tmp_path: Path,
) -> tuple[
    PersonalDevControlPlaneProfile,
    PersonalDevTrustedRelease,
    dict[str, Any],
]:
    profile = load_personal_dev_control_plane_profile(_PROFILE_PATH)
    release, release_sha256 = _write_release(tmp_path)
    return profile, release, _plan_value(profile, release, release_sha256)


def test_acceptance_plan_loads_exact_owner_only_canonical_contract(tmp_path: Path) -> None:
    profile, release, value = _inputs(tmp_path)
    path, digest = _write_plan(tmp_path, value)

    plan = load_personal_dev_acceptance_plan(path, digest)

    assert plan.schema_version == 1
    assert str(plan.manager.authority_incarnation) == ("00000000-0000-0000-0000-000000000101")
    assert plan.manager.execution_state == "shadow"
    assert plan.manager.execution_epoch == 0
    assert plan.manager.executable_new_capacity_ceiling == 0
    assert plan.builder.runtime_handler == "runsc-personal-dev"
    assert plan.builder.runtime_profile_sha256 == profile.builder.runtime_profile_sha256
    assert plan.builder.scanner_binary_sha256 == release.scanner.binary_sha256
    assert plan.builder.scanner_cache_identity_sha256 == (release.scanner.cache_identity_sha256)
    assert plan.builder.scanner_database_metadata_sha256 == (
        release.scanner.database_metadata_sha256
    )
    assert plan.builder.scanner_java_database_metadata_sha256 == (
        release.scanner.java_database_metadata_sha256
    )
    assert plan.quotas.per_owner_aggregate_min_slots == 8
    assert str(plan.acceptance_owner.user_id) == (
        "00000000-0000-0000-0000-000000000301"
    )
    assert plan.acceptance_owners == (plan.acceptance_owner,)
    assert plan.canonical_bytes() == path.read_bytes()
    assert plan.sha256 == digest
    validate_personal_dev_acceptance_plan(
        profile,
        release,
        "a" * 64,
        plan,
        now=_NOW,
    )


def test_acceptance_plan_v2_requires_exactly_two_sorted_owners(tmp_path: Path) -> None:
    _profile, _release, value = _inputs(tmp_path)
    path, digest = _write_plan(tmp_path, _v2_plan_value(value))

    plan = load_personal_dev_acceptance_plan(path, digest)

    assert plan.schema_version == 2
    assert [str(owner.team_id) for owner in plan.acceptance_owners] == [
        "00000000-0000-0000-0000-000000000006",
        "00000000-0000-0000-0000-000000000201",
    ]
    assert plan.canonical_bytes() == path.read_bytes()
    assert plan.sha256 == digest
    with pytest.raises(PersonalDevAcceptancePlanError):
        _ = plan.acceptance_owner


def test_acceptance_plan_v1_preserves_historically_valid_lower_global_limits(
    tmp_path: Path,
) -> None:
    _profile, _release, value = _inputs(tmp_path)
    value["quotas"]["global_live_instances"] = 1
    value["quotas"]["per_owner_live_instances"] = 1
    value["quotas"]["builder_global_concurrency"] = 1
    path, digest = _write_plan(tmp_path, value)

    plan = load_personal_dev_acceptance_plan(path, digest)

    assert plan.schema_version == 1
    assert plan.quotas.global_live_instances == 1
    assert plan.quotas.builder_global_concurrency == 1


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.update(acceptance_owners=value["acceptance_owners"][:1]),
        lambda value: value.update(
            acceptance_owners=[*value["acceptance_owners"], value["acceptance_owners"][0]]
        ),
        lambda value: value["acceptance_owners"][1].update(
            user_id=value["acceptance_owners"][0]["user_id"]
        ),
        lambda value: value["acceptance_owners"][1].update(
            team_id=value["acceptance_owners"][0]["team_id"]
        ),
        lambda value: value["acceptance_owners"][0].update(
            team_id="00000000-0000-0000-0000-000000000000"
        ),
        lambda value: value["acceptance_owners"][0].update(
            user_id="{00000000-0000-0000-0000-000000000005}"
        ),
        lambda value: value.update(acceptance_owners=list(reversed(value["acceptance_owners"]))),
        lambda value: value.update(
            acceptance_owner={
                "team_id": "00000000-0000-0000-0000-000000000007",
                "user_id": "00000000-0000-0000-0000-000000000008",
            }
        ),
        lambda value: value["quotas"].update(
            global_live_instances=1,
            per_owner_live_instances=1,
        ),
        lambda value: value["quotas"].update(builder_global_concurrency=1),
        lambda value: value["quotas"].update(per_owner_live_instances=0),
        lambda value: value["quotas"].update(builder_per_owner_concurrency=0),
    ],
)
def test_acceptance_plan_v2_rejects_invalid_owner_contract(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], object],
) -> None:
    _profile, _release, value = _inputs(tmp_path)
    value = _v2_plan_value(value)
    mutate(value)
    path, digest = _write_plan(tmp_path, value)

    with pytest.raises(PersonalDevAcceptancePlanError):
        load_personal_dev_acceptance_plan(path, digest)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: {**value, "unexpected": True},
        lambda value: {key: item for key, item in value.items() if key != "storage"},
        lambda value: {**value, "schema_version": 2},
        lambda value: {
            **value,
            "manager": {**value["manager"], "executable_new_capacity_ceiling": 1},
        },
        lambda value: {
            **value,
            "manager": {
                **value["manager"],
                "execution_state": "active",
                "execution_epoch": 1,
            },
        },
        lambda value: {
            **value,
            "manager": {**value["manager"], "execution_epoch": 1},
        },
        lambda value: {
            **value,
            "release": {
                **value["release"],
                "images": {
                    **value["release"]["images"],
                    "loom_service": "ghcr.io/qianyi-sun/loom-service:dev",
                },
            },
        },
        lambda value: {
            **value,
            "builder": {**value["builder"], "scanner_binary_sha256": "0" * 64},
        },
        lambda value: {
            **value,
            "acceptance_owner": {
                **value["acceptance_owner"],
                "team_id": "00000000-0000-0000-0000-000000000000",
            },
        },
        lambda value: {
            **value,
            "principals": {
                "lifecycle_principal_id": "personal-dev-lifecycle",
                "reporter_principal_id": "personal-dev-lifecycle",
            },
        },
        lambda value: {
            **value,
            "quotas": {**value["quotas"], "global_live_instances": True},
        },
        lambda value: {
            **value,
            "window": {
                **value["window"],
                "started_at": "2026-08-17T20:00:00+00:00",
            },
        },
    ],
)
def test_acceptance_plan_rejects_invalid_nested_contract(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], dict[str, Any]],
) -> None:
    _profile, _release, value = _inputs(tmp_path)
    path, digest = _write_plan(tmp_path, mutate(copy.deepcopy(value)))

    with pytest.raises((PersonalDevAcceptancePlanError, ValidationError)):
        load_personal_dev_acceptance_plan(path, digest)


@pytest.mark.parametrize(
    "payload",
    [
        b'{"schema_version":1,"schema_version":1}',
        b"not-json",
    ],
)
def test_acceptance_plan_rejects_duplicate_or_invalid_json(
    tmp_path: Path,
    payload: bytes,
) -> None:
    _profile, _release, value = _inputs(tmp_path)
    path, digest = _write_plan(tmp_path, value, payload=payload)

    with pytest.raises(PersonalDevAcceptancePlanError):
        load_personal_dev_acceptance_plan(path, digest)


def test_acceptance_plan_rejects_noncanonical_or_wrong_digest(tmp_path: Path) -> None:
    _profile, _release, value = _inputs(tmp_path)
    canonical = _canonical(value)
    path, digest = _write_plan(tmp_path, value, payload=canonical + b"\n")
    with pytest.raises(PersonalDevAcceptancePlanError):
        load_personal_dev_acceptance_plan(path, digest)

    path.write_bytes(canonical)
    path.chmod(0o600)
    with pytest.raises(PersonalDevAcceptancePlanError):
        load_personal_dev_acceptance_plan(path, "f" * 64)


def test_acceptance_plan_rejects_unsafe_file_metadata(tmp_path: Path) -> None:
    _profile, _release, value = _inputs(tmp_path)
    path, digest = _write_plan(tmp_path, value)
    path.chmod(0o640)
    with pytest.raises(PersonalDevAcceptancePlanError):
        load_personal_dev_acceptance_plan(path, digest)


def test_acceptance_plan_rejects_path_replacement_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _profile, _release, value = _inputs(tmp_path)
    path, digest = _write_plan(tmp_path, value)
    replacement = tmp_path / "replacement-plan.json"
    replacement.write_bytes(path.read_bytes())
    replacement.chmod(0o600)
    original_lstat = Path.lstat
    calls = 0

    def raced_lstat(selected: Path) -> os.stat_result:
        nonlocal calls
        if selected == path:
            calls += 1
            if calls > 1:
                return original_lstat(replacement)
        return original_lstat(selected)

    monkeypatch.setattr(Path, "lstat", raced_lstat)

    with pytest.raises(PersonalDevAcceptancePlanError):
        load_personal_dev_acceptance_plan(path, digest)

    path.chmod(0o600)
    linked = tmp_path / "linked-plan.json"
    linked.symlink_to(path)
    with pytest.raises(PersonalDevAcceptancePlanError):
        load_personal_dev_acceptance_plan(linked, digest)

    hardlink = tmp_path / "hardlinked-plan.json"
    os.link(path, hardlink)
    with pytest.raises(PersonalDevAcceptancePlanError):
        load_personal_dev_acceptance_plan(path, digest)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["source"].update(commit="f" * 40),
        lambda value: value["source"].update(tree="f" * 40),
        lambda value: value["release"].update(trusted_release_sha256="f" * 64),
        lambda value: value["release"].update(release_evidence_sha256="f" * 64),
        lambda value: value["release"].update(shadow_manifest_sha256="f" * 64),
        lambda value: value["release"]["images"].update(
            personal_dev_builder=("ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "d" * 64)
        ),
        lambda value: value["release"]["images"].update(
            personal_dev_scanner_cache=(
                "ghcr.io/qianyi-sun/loom-personal-dev-scanner-cache@sha256:" + "2" * 64
            )
        ),
        lambda value: value["builder"].update(scanner_binary_sha256="2" * 64),
        lambda value: value["builder"].update(scanner_cache_identity_sha256="2" * 64),
        lambda value: value["builder"].update(scanner_database_sha256="2" * 64),
        lambda value: value["builder"].update(scanner_database_metadata_sha256="2" * 64),
        lambda value: value["builder"].update(scanner_java_database_sha256="2" * 64),
        lambda value: value["builder"].update(scanner_java_database_metadata_sha256="2" * 64),
        lambda value: value["storage"].update(schema_head="0097"),
        lambda value: value["builder"].update(runtime_class_name="other-runtime"),
        lambda value: value["builder"].update(runtime_handler="runc"),
        lambda value: value["builder"].update(runtime_profile_sha256="2" * 64),
        lambda value: value["builder"].update(
            publisher_identity="system:serviceaccount:loom-dev:other-publisher"
        ),
        lambda value: value["builder"].update(registry_prefix="registry.example/other"),
        lambda value: value["builder"].update(protocol_map_sha256="f" * 64),
        lambda value: value["quotas"].update(global_live_instances=15),
        lambda value: value["window"].update(
            started_at="2026-08-17T22:00:00Z",
        ),
        lambda value: value["window"].update(
            expires_at="2026-08-17T21:00:00Z",
        ),
        lambda value: value["window"].update(
            rollback_expires_at="2026-08-17T22:00:00Z",
            expires_at="2026-08-17T23:00:00Z",
        ),
    ],
)
def test_acceptance_plan_cross_validation_rejects_drift(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], object],
) -> None:
    profile, release, value = _inputs(tmp_path)
    mutate(value)
    path, digest = _write_plan(tmp_path, value)
    plan = load_personal_dev_acceptance_plan(path, digest)

    with pytest.raises(PersonalDevAcceptancePlanError):
        validate_personal_dev_acceptance_plan(
            profile,
            release,
            "a" * 64,
            plan,
            now=_NOW,
        )


@pytest.mark.parametrize(
    ("state", "epoch"),
    [("prepared", 4), ("drain-only", 5)],
)
def test_acceptance_plan_permits_only_coherent_non_executable_manager_states(
    tmp_path: Path,
    state: str,
    epoch: int,
) -> None:
    profile, release, value = _inputs(tmp_path)
    value["manager"].update(execution_state=state, execution_epoch=epoch)
    path, digest = _write_plan(tmp_path, value)

    plan = load_personal_dev_acceptance_plan(path, digest)

    validate_personal_dev_acceptance_plan(
        profile,
        release,
        "a" * 64,
        plan,
        now=_NOW,
    )


def test_acceptance_cross_validation_normalizes_invalid_runtime_types(
    tmp_path: Path,
) -> None:
    profile, release, value = _inputs(tmp_path)
    path, digest = _write_plan(tmp_path, value)
    plan = load_personal_dev_acceptance_plan(path, digest)

    with pytest.raises(PersonalDevAcceptancePlanError):
        validate_personal_dev_acceptance_plan(
            profile,
            release,
            True,  # type: ignore[arg-type]
            plan,
            now=_NOW,
        )


@pytest.mark.parametrize(
    "profile_drift",
    [
        {"dev_instances_enabled": True},
        {"personal_dev_builder_enabled": True},
        {"activation_agent_replicas": 1},
        {"min_slots_default": 1},
    ],
)
def test_acceptance_validation_requires_the_inert_shadow_profile(
    tmp_path: Path,
    profile_drift: dict[str, object],
) -> None:
    profile, release, value = _inputs(tmp_path)
    path, digest = _write_plan(tmp_path, value)
    plan = load_personal_dev_acceptance_plan(path, digest)

    with pytest.raises(PersonalDevAcceptancePlanError):
        validate_personal_dev_acceptance_plan(
            replace(profile, **profile_drift),  # type: ignore[arg-type]
            release,
            "a" * 64,
            plan,
            now=_NOW,
        )


def _operational_plan_value(value: dict[str, Any]) -> dict[str, Any]:
    operational = copy.deepcopy(value)
    operational.pop("acceptance_owner")
    operational.pop("window")
    operational["approval"] = {
        "acceptance_result_sha256": "4" * 64,
        "approved_at": "2026-08-17T20:30:00Z",
        "rollback_evidence_sha256": "5" * 64,
    }
    return operational


def test_operational_plan_loads_accepted_non_expiring_zero_capacity_contract(
    tmp_path: Path,
) -> None:
    profile, release, acceptance = _inputs(tmp_path)
    value = _operational_plan_value(acceptance)
    path, digest = _write_plan(tmp_path, value)

    plan = load_personal_dev_operational_plan(path, digest)

    assert plan.approval.acceptance_result_sha256 == "4" * 64
    assert plan.approval.rollback_evidence_sha256 == "5" * 64
    assert plan.canonical_bytes() == path.read_bytes()
    assert plan.sha256 == digest
    assert "expires_at" not in plan.manager_runtime_json()
    validate_personal_dev_operational_plan(
        profile,
        release,
        "a" * 64,
        plan,
        now=_NOW,
    )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["approval"].update(acceptance_result_sha256="0" * 64),
        lambda value: value["approval"].update(rollback_evidence_sha256="0" * 64),
        lambda value: value.update(acceptance_owner={}),
        lambda value: value.update(window={}),
    ],
)
def test_operational_plan_rejects_unaccepted_or_acceptance_only_fields(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], object],
) -> None:
    _profile, _release, acceptance = _inputs(tmp_path)
    value = _operational_plan_value(acceptance)
    mutate(value)
    path, digest = _write_plan(tmp_path, value)

    with pytest.raises((PersonalDevOperationalPlanError, ValidationError)):
        load_personal_dev_operational_plan(path, digest)


def test_operational_plan_rejects_future_approval(tmp_path: Path) -> None:
    profile, release, acceptance = _inputs(tmp_path)
    value = _operational_plan_value(acceptance)
    value["approval"]["approved_at"] = "2026-08-17T21:00:01Z"
    path, digest = _write_plan(tmp_path, value)
    plan = load_personal_dev_operational_plan(path, digest)

    with pytest.raises(PersonalDevOperationalPlanError):
        validate_personal_dev_operational_plan(
            profile,
            release,
            "a" * 64,
            plan,
            now=_NOW,
        )
