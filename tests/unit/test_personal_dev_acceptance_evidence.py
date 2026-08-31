from __future__ import annotations

import hashlib
import json
import subprocess
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pytest

from loom import personal_dev_acceptance_evidence as acceptance_evidence
from loom.personal_dev_acceptance_evidence import (
    PersonalDevAcceptanceEvidenceError,
    PersonalDevAcceptanceResultV2,
    build_personal_dev_backup_restore_evidence,
    build_personal_dev_scanner_finding_policy,
    build_personal_dev_trusted_launcher_profile,
    load_personal_dev_acceptance_result,
    load_personal_dev_backup_restore_evidence,
    validate_personal_dev_policy_evidence,
)
from loom.personal_dev_control_plane_config import (
    load_personal_dev_acceptance_plan,
    load_personal_dev_control_plane_profile,
)
from loom.personal_dev_minio_backup import load_personal_dev_minio_manifest
from tests.unit.test_personal_dev_control_plane_acceptance_config import (
    _PROFILE_PATH,
    _plan_value,
    _write_plan,
    _write_release,
)


def _write_owner_only(path: Path, value: object) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    path.write_bytes(payload)
    path.chmod(0o600)
    return hashlib.sha256(payload).hexdigest()


def _rollback_shadow_status_value() -> dict[str, object]:
    return {
        "blockers": [],
        "components": [
            {"name": "cluster-resources", "observed": 6, "ready": True},
            {"name": "manager", "observed": 1, "ready": True},
            {"name": "namespaced-resources", "observed": 31, "ready": True},
            {"name": "namespaces", "observed": 1, "ready": True},
            {"name": "personal-workers", "observed": 0, "ready": True},
            {"name": "runtime-class", "observed": 1, "ready": True},
        ],
        "input_sha256": "1" * 64,
        "manager_ceiling": 0,
        "mode": "shadow",
        "ready": True,
        "release_sha256": "2" * 64,
        "schema": "loom-personal-dev-control-plane-status-v1",
        "worker_available": False,
    }


def _rollback_shadow_manifest_payload(
    *,
    input_sha256: str,
    release_sha256: str,
) -> bytes:
    return (
        "apiVersion: v1\n"
        "kind: Namespace\n"
        "metadata:\n"
        "  annotations:\n"
        f'    loom.dev/render-input-sha256: "{input_sha256}"\n'
        f'    loom.dev/trusted-release-sha256: "{release_sha256}"\n'
        "  name: loom-dev\n"
        "---\n"
        "apiVersion: apps/v1\n"
        "kind: Deployment\n"
        "metadata:\n"
        "  annotations:\n"
        f'    loom.dev/render-input-sha256: "{input_sha256}"\n'
        f'    loom.dev/trusted-release-sha256: "{release_sha256}"\n'
        "  name: loom-personal-dev-management\n"
        "  namespace: loom-dev\n"
    ).encode("ascii")


@pytest.mark.parametrize(
    ("expected_input_sha256", "expected_release_sha256"),
    [
        ("3" * 64, "2" * 64),
        ("1" * 64, "3" * 64),
    ],
    ids=["status-input-drift", "release-drift"],
)
def test_rollback_shadow_manifest_rejects_annotation_binding_drift(
    tmp_path: Path,
    expected_input_sha256: str,
    expected_release_sha256: str,
) -> None:
    path = tmp_path / "rollback-shadow.yaml"
    payload = _rollback_shadow_manifest_payload(
        input_sha256="1" * 64,
        release_sha256="2" * 64,
    )
    path.write_bytes(payload)
    path.chmod(0o600)

    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        acceptance_evidence.validate_personal_dev_rollback_shadow_manifest(
            path,
            hashlib.sha256(payload).hexdigest(),
            expected_input_sha256=expected_input_sha256,
            expected_release_sha256=expected_release_sha256,
        )


def test_rollback_shadow_manifest_rejects_duplicate_yaml_bindings(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rollback-shadow.yaml"
    payload = _rollback_shadow_manifest_payload(
        input_sha256="1" * 64,
        release_sha256="2" * 64,
    )
    valid_binding = ('    loom.dev/render-input-sha256: "' + "1" * 64 + '"\n').encode("ascii")
    duplicate_bindings = (
        '    loom.dev/render-input-sha256: "'
        + "3" * 64
        + '"\n    loom.dev/render-input-sha256: "'
        + "1" * 64
        + '"\n'
    ).encode("ascii")
    payload = payload.replace(valid_binding, duplicate_bindings, 1)
    path.write_bytes(payload)
    path.chmod(0o600)

    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        acceptance_evidence.validate_personal_dev_rollback_shadow_manifest(
            path,
            hashlib.sha256(payload).hexdigest(),
            expected_input_sha256="1" * 64,
            expected_release_sha256="2" * 64,
        )


def test_rollback_shadow_manifest_accepts_exact_status_and_release_bindings(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rollback-shadow.yaml"
    payload = _rollback_shadow_manifest_payload(
        input_sha256="1" * 64,
        release_sha256="2" * 64,
    )
    path.write_bytes(payload)
    path.chmod(0o600)

    acceptance_evidence.validate_personal_dev_rollback_shadow_manifest(
        path,
        hashlib.sha256(payload).hexdigest(),
        expected_input_sha256="1" * 64,
        expected_release_sha256="2" * 64,
    )


def test_rollback_shadow_status_loads_canonical_zero_capacity_evidence(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rollback-shadow.status.json"
    value = _rollback_shadow_status_value()
    sha256 = _write_owner_only(path, value)

    loaded = acceptance_evidence.load_personal_dev_rollback_shadow_status(
        path,
        sha256,
    )

    assert loaded == value


def test_rollback_shadow_status_loads_schema_three_web_component(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rollback-shadow.status.json"
    value = _rollback_shadow_status_value()
    value["components"].append(  # type: ignore[union-attr]
        {"name": "web", "observed": 1, "ready": True}
    )
    sha256 = _write_owner_only(path, value)

    loaded = acceptance_evidence.load_personal_dev_rollback_shadow_status(
        path,
        sha256,
    )

    assert loaded == value


def test_shadow_status_validation_requires_release_specific_web_component() -> None:
    schema_two = _rollback_shadow_status_value()
    schema_three = deepcopy(schema_two)
    schema_three["components"].append(  # type: ignore[union-attr]
        {"name": "web", "observed": 1, "ready": True}
    )

    acceptance_evidence._validate_shadow_status(schema_two, web_expected=False)
    acceptance_evidence._validate_shadow_status(schema_three, web_expected=True)
    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        acceptance_evidence._validate_shadow_status(schema_two, web_expected=True)
    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        acceptance_evidence._validate_shadow_status(schema_three, web_expected=False)
    invalid_web_count = deepcopy(schema_three)
    invalid_web_count["components"][-1]["observed"] = 0  # type: ignore[index]
    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        acceptance_evidence._validate_shadow_status(
            invalid_web_count,
            web_expected=True,
        )


@pytest.mark.parametrize(
    "invalid_kind",
    [
        "wrong-digest",
        "malformed-json",
        "duplicate-key",
        "noncanonical",
        "unsafe-mode",
        "status-extra-field",
        "wrong-schema",
        "invalid-input-digest",
        "invalid-release-digest",
        "non-shadow-mode",
        "not-ready",
        "blockers",
        "nonzero-ceiling",
        "worker-available",
        "component-not-ready",
        "component-extra-field",
        "duplicate-component",
        "malformed-component",
        "empty-components",
        "missing-canonical-component",
        "missing-personal-workers",
        "noncanonical-component-order",
        "invalid-cluster-resource-count",
        "invalid-manager-count",
        "invalid-namespaced-resource-count",
        "invalid-namespace-count",
        "personal-workers-nonzero",
        "invalid-runtime-class-count",
    ],
)
def test_rollback_shadow_status_rejects_untrusted_or_nonzero_capacity_evidence(
    tmp_path: Path,
    invalid_kind: str,
) -> None:
    path = tmp_path / "rollback-shadow.status.json"
    value = _rollback_shadow_status_value()

    if invalid_kind == "status-extra-field":
        value["unexpected"] = False
    elif invalid_kind == "wrong-schema":
        value["schema"] = "loom-personal-dev-control-plane-status-v2"
    elif invalid_kind == "invalid-input-digest":
        value["input_sha256"] = None
    elif invalid_kind == "invalid-release-digest":
        value["release_sha256"] = "0" * 64
    elif invalid_kind == "non-shadow-mode":
        value["mode"] = "operational"
    elif invalid_kind == "not-ready":
        value["ready"] = False
    elif invalid_kind == "blockers":
        value["blockers"] = ["capacity is not inert"]
    elif invalid_kind == "nonzero-ceiling":
        value["manager_ceiling"] = 1
    elif invalid_kind == "worker-available":
        value["worker_available"] = True
    elif invalid_kind == "component-not-ready":
        value["components"][0]["ready"] = False  # type: ignore[index]
    elif invalid_kind == "component-extra-field":
        value["components"][0]["unexpected"] = False  # type: ignore[index]
    elif invalid_kind == "duplicate-component":
        value["components"].append(  # type: ignore[union-attr]
            {"name": "personal-workers", "observed": 0, "ready": True}
        )
    elif invalid_kind == "malformed-component":
        value["components"].append("not-a-component")  # type: ignore[union-attr]
    elif invalid_kind == "empty-components":
        value["components"] = []
    elif invalid_kind == "missing-canonical-component":
        value["components"] = value["components"][1:]  # type: ignore[index]
    elif invalid_kind == "missing-personal-workers":
        value["components"] = [  # type: ignore[index]
            component
            for component in value["components"]  # type: ignore[union-attr]
            if component["name"] != "personal-workers"  # type: ignore[index]
        ]
    elif invalid_kind == "noncanonical-component-order":
        value["components"][0], value["components"][1] = (  # type: ignore[index]
            value["components"][1],  # type: ignore[index]
            value["components"][0],  # type: ignore[index]
        )
    elif invalid_kind == "invalid-cluster-resource-count":
        value["components"][0]["observed"] = 0  # type: ignore[index]
    elif invalid_kind == "invalid-manager-count":
        value["components"][1]["observed"] = 0  # type: ignore[index]
    elif invalid_kind == "invalid-namespaced-resource-count":
        value["components"][2]["observed"] = 0  # type: ignore[index]
    elif invalid_kind == "invalid-namespace-count":
        value["components"][3]["observed"] = 2  # type: ignore[index]
    elif invalid_kind == "personal-workers-nonzero":
        value["components"][4]["observed"] = 1  # type: ignore[index]
    elif invalid_kind == "invalid-runtime-class-count":
        value["components"][5]["observed"] = 0  # type: ignore[index]

    sha256 = _write_owner_only(path, value)
    if invalid_kind == "wrong-digest":
        sha256 = "f" * 64
    elif invalid_kind == "malformed-json":
        path.write_bytes(b"{")
        sha256 = hashlib.sha256(b"{").hexdigest()
    elif invalid_kind == "duplicate-key":
        payload = path.read_bytes().replace(
            b'"schema":',
            b'"schema":"loom-personal-dev-control-plane-status-v1","schema":',
            1,
        )
        path.write_bytes(payload)
        sha256 = hashlib.sha256(payload).hexdigest()
    elif invalid_kind == "noncanonical":
        payload = path.read_bytes() + b"\n"
        path.write_bytes(payload)
        sha256 = hashlib.sha256(payload).hexdigest()
    elif invalid_kind == "unsafe-mode":
        path.chmod(0o644)

    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        acceptance_evidence.load_personal_dev_rollback_shadow_status(
            path,
            sha256,
        )


def _git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _source_root(tmp_path: Path, *, marker: str = "exact") -> tuple[Path, str, str]:
    root = tmp_path / "source"
    for relative in (
        "src/loom/personal_dev_builder_tools.py",
        "src/loom_capacity_executor/bootstrap_handoff.py",
        "src/loom_capacity_executor/runtime.py",
        "src/loom_capacity_executor/trusted_launcher.py",
    ):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{marker}:{relative}\n", encoding="ascii")
    _git(root, "init", "--quiet")
    _git(root, "config", "user.name", "Loom test")
    _git(root, "config", "user.email", "loom-test@example.invalid")
    _git(root, "add", ".")
    _git(root, "commit", "--quiet", "-m", f"{marker} source")
    return root, _git(root, "rev-parse", "HEAD"), _git(root, "rev-parse", "HEAD^{tree}")


def _inputs(tmp_path: Path):  # type: ignore[no-untyped-def]
    profile = load_personal_dev_control_plane_profile(_PROFILE_PATH)
    source_root, source_sha, source_tree = _source_root(tmp_path)
    release, release_sha256 = _write_release(tmp_path)
    release = replace(release, source_sha=source_sha, source_tree=source_tree)
    plan_value = _plan_value(profile, release, release_sha256)
    launcher = build_personal_dev_trusted_launcher_profile(
        profile=profile,
        release=release,
        source_root=source_root,
    )
    scanner = build_personal_dev_scanner_finding_policy(
        profile=profile,
        release=release,
        source_root=source_root,
    )
    launcher_path = tmp_path / "trusted-launcher-profile.json"
    scanner_path = tmp_path / "scanner-finding-policy.json"
    launcher_sha256 = _write_owner_only(launcher_path, launcher)
    scanner_sha256 = _write_owner_only(scanner_path, scanner)
    plan_value["builder"]["trusted_launcher_profile_sha256"] = launcher_sha256
    plan_value["builder"]["scanner_finding_policy_sha256"] = scanner_sha256
    backup_path = tmp_path / "backup-restore-evidence.json"
    backup_value = {
        "cleanup": {
            "isolated_minio_absent": True,
            "isolated_network_absent": True,
            "isolated_postgres_absent": True,
        },
        "completed_at": "2026-08-26T19:05:00Z",
        "manager": {
            "executable_new_capacity_ceiling": 0,
            "personal_worker_count": 0,
        },
        "minio": {
            "backup_manifest_sha256": "1" * 64,
            "image": release.images.minio,
            "restored_manifest_sha256": "1" * 64,
            "restored_object_count": 0,
            "source_object_count": 0,
        },
        "namespace": "loom-dev",
        "postgres": {
            "dump_sha256": "2" * 64,
            "image": release.images.postgres,
            "restored_schema_head": "0112",
            "restored_state_sha256": "3" * 64,
            "source_schema_head": "0112",
            "source_state_sha256": "3" * 64,
        },
        "release_sha256": release_sha256,
        "schema": "loom-personal-dev-backup-restore-evidence-v1",
        "secrets": {
            "key_inventory_sha256": "4" * 64,
            "values_included": False,
        },
        "source": {
            "commit": release.source_sha,
            "tree": release.source_tree,
        },
        "started_at": "2026-08-26T19:00:00Z",
        "storage": {
            "minio_pvc": "data-loom-dev-minio-0",
            "postgres_pvc": "data-loom-dev-postgres-0",
            "storage_class": "longhorn",
        },
    }
    backup_sha256 = _write_owner_only(backup_path, backup_value)
    plan_value["storage"]["backup_restore_evidence_sha256"] = backup_sha256
    plan_path, plan_sha256 = _write_plan(tmp_path, plan_value)
    plan = load_personal_dev_acceptance_plan(plan_path, plan_sha256)
    return (
        profile,
        release,
        release_sha256,
        plan,
        source_root,
        launcher_path,
        scanner_path,
        backup_path,
    )


@pytest.mark.parametrize("target", ["launcher", "scanner"])
def test_policy_evidence_rejects_checkout_from_a_different_release(
    tmp_path: Path,
    target: str,
) -> None:
    (
        profile,
        release,
        _release_sha256,
        _plan,
        _source_root_value,
        _launcher_path,
        _scanner_path,
        _backup_path,
    ) = _inputs(tmp_path)
    wrong_root, _wrong_sha, _wrong_tree = _source_root(
        tmp_path / "wrong",
        marker="different",
    )

    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        if target == "launcher":
            build_personal_dev_trusted_launcher_profile(
                profile=profile,
                release=release,
                source_root=wrong_root,
            )
        else:
            build_personal_dev_scanner_finding_policy(
                profile=profile,
                release=release,
                source_root=wrong_root,
            )


def test_policy_evidence_rejects_git_environment_source_redirection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        profile,
        release,
        _release_sha256,
        _plan,
        source_root,
        _launcher_path,
        _scanner_path,
        _backup_path,
    ) = _inputs(tmp_path)
    redirected_root = tmp_path / "redirected"
    redirected_root.mkdir()
    monkeypatch.setenv("GIT_DIR", str(source_root / ".git"))
    monkeypatch.setenv("GIT_WORK_TREE", str(redirected_root))

    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        build_personal_dev_trusted_launcher_profile(
            profile=profile,
            release=release,
            source_root=redirected_root,
        )


def test_policy_evidence_rejects_modified_bound_source_file(tmp_path: Path) -> None:
    (
        profile,
        release,
        _release_sha256,
        _plan,
        source_root,
        _launcher_path,
        _scanner_path,
        _backup_path,
    ) = _inputs(tmp_path)
    (source_root / "src/loom/personal_dev_builder_tools.py").write_text(
        "modified after the trusted commit\n",
        encoding="ascii",
    )

    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        build_personal_dev_scanner_finding_policy(
            profile=profile,
            release=release,
            source_root=source_root,
        )


@pytest.mark.parametrize("index_flag", ["--assume-unchanged", "--skip-worktree"])
def test_policy_evidence_rejects_index_hidden_bound_source_change(
    tmp_path: Path,
    index_flag: str,
) -> None:
    (
        profile,
        release,
        _release_sha256,
        _plan,
        source_root,
        _launcher_path,
        _scanner_path,
        _backup_path,
    ) = _inputs(tmp_path)
    relative = "src/loom/personal_dev_builder_tools.py"
    _git(source_root, "update-index", index_flag, relative)
    (source_root / relative).write_text(
        "modified but hidden from Git status\n",
        encoding="ascii",
    )
    assert _git(source_root, "status", "--porcelain=v1", "--", relative) == ""

    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        build_personal_dev_scanner_finding_policy(
            profile=profile,
            release=release,
            source_root=source_root,
        )


def test_policy_evidence_is_exactly_derived_from_source_and_release(tmp_path: Path) -> None:
    (
        profile,
        release,
        _release_sha256,
        plan,
        source_root,
        launcher_path,
        scanner_path,
        _backup_path,
    ) = _inputs(tmp_path)

    validate_personal_dev_policy_evidence(
        profile=profile,
        release=release,
        plan=plan,
        source_root=source_root,
        trusted_launcher_profile_path=launcher_path,
        scanner_finding_policy_path=scanner_path,
    )

    launcher = json.loads(launcher_path.read_text(encoding="ascii"))
    assert launcher["contract"]["immutable_candidate_snapshot"] is True
    assert launcher["protocol_versions"] == dict(profile.protocol_versions)
    scanner = json.loads(scanner_path.read_text(encoding="ascii"))
    assert scanner["argv"] == [
        "image",
        "--input",
        "<verified-oci-archive>",
        "--format",
        "json",
        "--scanners",
        "vuln,secret",
        "--severity",
        "HIGH,CRITICAL",
        "--exit-code",
        "1",
        "--no-progress",
        "--offline-scan",
        "--skip-db-update",
        "--skip-java-db-update",
        "--cache-dir",
        "<release-bound-cache>",
    ]


@pytest.mark.parametrize("target", ["launcher", "scanner"])
def test_policy_evidence_rejects_semantic_or_source_drift(
    tmp_path: Path,
    target: str,
) -> None:
    (
        profile,
        release,
        _release_sha256,
        plan,
        source_root,
        launcher_path,
        scanner_path,
        _backup_path,
    ) = _inputs(tmp_path)
    path = launcher_path if target == "launcher" else scanner_path
    value = json.loads(path.read_text(encoding="ascii"))
    if target == "launcher":
        value["contract"]["immutable_candidate_snapshot"] = False
    else:
        value["argv"][8] = "0"
    _write_owner_only(path, value)

    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        validate_personal_dev_policy_evidence(
            profile=profile,
            release=release,
            plan=plan,
            source_root=source_root,
            trusted_launcher_profile_path=launcher_path,
            scanner_finding_policy_path=scanner_path,
        )


def test_backup_restore_evidence_requires_equal_restored_state_and_cleanup(
    tmp_path: Path,
) -> None:
    (
        _profile_value,
        release,
        release_sha256,
        plan,
        _source_root_value,
        _launcher_path,
        _scanner_path,
        backup_path,
    ) = _inputs(tmp_path)

    evidence = load_personal_dev_backup_restore_evidence(
        backup_path,
        expected_sha256=plan.storage.backup_restore_evidence_sha256,
        release=release,
        release_sha256=release_sha256,
        expected_schema_head="0112",
    )
    assert evidence.postgres.source_state_sha256 == evidence.postgres.restored_state_sha256
    assert evidence.minio.source_object_count == 0

    value = json.loads(backup_path.read_text(encoding="ascii"))
    value["cleanup"]["isolated_postgres_absent"] = False
    changed_sha256 = _write_owner_only(backup_path, value)
    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        load_personal_dev_backup_restore_evidence(
            backup_path,
            expected_sha256=changed_sha256,
            release=release,
            release_sha256=release_sha256,
            expected_schema_head="0112",
        )


def test_evidence_loader_rejects_noncanonical_or_non_owner_only_file(tmp_path: Path) -> None:
    (
        _profile_value,
        release,
        release_sha256,
        plan,
        _source_root_value,
        _launcher_path,
        _scanner_path,
        backup_path,
    ) = _inputs(tmp_path)
    backup_path.chmod(0o644)

    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        load_personal_dev_backup_restore_evidence(
            backup_path,
            expected_sha256=plan.storage.backup_restore_evidence_sha256,
            release=release,
            release_sha256=release_sha256,
            expected_schema_head="0112",
        )


@pytest.mark.parametrize("web_expected", [False, True], ids=["schema-2", "schema-3"])
def test_backup_restore_evidence_is_derived_from_supporting_artifacts(
    tmp_path: Path,
    web_expected: bool,
) -> None:
    (
        profile,
        release,
        release_sha256,
        _plan,
        _source_root_value,
        _launcher_path,
        _scanner_path,
        _backup_path,
    ) = _inputs(tmp_path)
    release = replace(
        release,
        source_sha="a" * 40,
        source_tree="b" * 40,
    )
    release_sha256 = "c" * 64
    if not web_expected:
        release = replace(
            release,
            schema_version=2,
            images=replace(release.images, loom_web=None),
        )

    def owner_file(name: str, payload: bytes) -> Path:
        path = tmp_path / name
        path.write_bytes(payload)
        path.chmod(0o600)
        return path

    postgres_dump = owner_file("postgres.dump", b"exact-postgres-dump")
    source_state = owner_file(
        "postgres.source.tsv",
        f"table\tpublic.alembic_version\t1\t{'5' * 64}\n".encode("ascii"),
    )
    restored_state = owner_file("postgres.restored.tsv", source_state.read_bytes())
    manifest_payload = json.dumps(
        {
            "buckets": ["artifacts", "trajectories"],
            "objects": [],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    source_manifest = owner_file("minio.source.json", manifest_payload)
    restored_manifest = owner_file("minio.restored.json", manifest_payload)
    minio_payload_root = tmp_path / "minio-payloads"
    minio_payload_root.mkdir(mode=0o700)
    secret_inventory = {
        "items": [
            {"keys": ["private-key"], "name": "loom-personal-dev-activation-agent"},
            {"keys": ["public-key"], "name": "loom-personal-dev-activation-public"},
            {
                "keys": [
                    "admin-secrets.toml",
                    "capacity-lifecycle-ca.pem",
                    "capacity-lifecycle-certificate.pem",
                    "capacity-lifecycle-private-key.pem",
                    "capacity-lifecycle-token",
                    "capacity-reporter-ca.pem",
                    "capacity-reporter-certificate.pem",
                    "capacity-reporter-private-key.pem",
                    "config.json",
                    "dev-instance-database-admin-url",
                    "minio-access-key",
                    "minio-secret-key",
                    "postgres-database",
                    "postgres-password",
                    "postgres-user",
                    "secret-store-master-key",
                    "svc-db-url",
                ],
                "name": "loom-personal-dev-management",
            },
        ]
    }
    secret_path = owner_file(
        "secret-inventory.json",
        json.dumps(secret_inventory, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n",
    )
    shadow_status = _rollback_shadow_status_value()
    if web_expected:
        shadow_status["components"].append(  # type: ignore[union-attr]
            {"name": "web", "observed": 1, "ready": True}
        )
    shadow_status["release_sha256"] = release_sha256
    status_payload = json.dumps(shadow_status).encode("ascii") + b"\n"
    pre_status = owner_file("pre-status.json", status_payload)
    post_status = owner_file("post-status.json", status_payload)
    storage = {
        "items": [
            {
                "kind": "PersistentVolumeClaim",
                "metadata": {"name": "data-loom-dev-postgres-0"},
                "spec": {"storageClassName": "longhorn"},
            },
            {
                "kind": "PersistentVolumeClaim",
                "metadata": {"name": "data-loom-dev-minio-0"},
                "spec": {"storageClassName": "longhorn"},
            },
            {
                "kind": "StatefulSet",
                "metadata": {"name": "loom-dev-postgres"},
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [{"name": "postgres", "image": release.images.postgres}]
                        }
                    }
                },
            },
            {
                "kind": "StatefulSet",
                "metadata": {"name": "loom-dev-minio"},
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [
                                {"name": "minio", "image": release.images.minio},
                                {"name": "admin", "image": release.images.minio_client},
                            ]
                        }
                    }
                },
            },
        ]
    }
    storage_path = owner_file("storage.json", json.dumps(storage).encode("ascii") + b"\n")

    value = build_personal_dev_backup_restore_evidence(
        profile=profile,
        release=release,
        release_sha256=release_sha256,
        started_at="2026-08-26T19:00:00Z",
        completed_at="2026-08-26T19:05:00Z",
        postgres_dump_path=postgres_dump,
        postgres_source_state_path=source_state,
        postgres_restored_state_path=restored_state,
        source_schema_head="0112",
        restored_schema_head="0112",
        minio_source_manifest_path=source_manifest,
        minio_restored_manifest_path=restored_manifest,
        minio_payload_root=minio_payload_root,
        secret_key_inventory_path=secret_path,
        pre_shadow_status_path=pre_status,
        post_shadow_status_path=post_status,
        storage_inventory_path=storage_path,
    )

    assert (
        value["postgres"]["dump_sha256"]
        == hashlib.sha256(  # type: ignore[index]
            postgres_dump.read_bytes()
        ).hexdigest()
    )
    assert value["minio"]["source_object_count"] == 0  # type: ignore[index]
    assert value["cleanup"]["isolated_network_absent"] is True  # type: ignore[index]
    canonical_v1 = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    assert canonical_v1 == bytes.fromhex(
        "7b22636c65616e7570223a7b2269736f6c617465645f6d696e696f5f616273656e74223a747275652c2269736f6c6174"
        "65645f6e6574776f726b5f616273656e74223a747275652c2269736f6c617465645f706f7374677265735f616273656e"
        "74223a747275657d2c22636f6d706c657465645f6174223a22323032362d30382d32365431393a30353a30305a222c22"
        "6d616e61676572223a7b2265786563757461626c655f6e65775f63617061636974795f6365696c696e67223a302c2270"
        "6572736f6e616c5f776f726b65725f636f756e74223a307d2c226d696e696f223a7b226261636b75705f6d616e696665"
        "73745f736861323536223a22653338383939303861326234346636353466663364346536363263303533393734303930"
        "38643530633336663732646339663239393133656531363963336235222c22696d616765223a22717561792e696f2f6d"
        "696e696f2f6d696e696f407368613235363a373737373737373737373737373737373737373737373737373737373737"
        "37373737373737373737373737373737373737373737373737373737373737373737222c22726573746f7265645f6d61"
        "6e69666573745f736861323536223a226533383839393038613262343466363534666633643465363632633035333937"
        "3430393038643530633336663732646339663239393133656531363963336235222c22726573746f7265645f6f626a65"
        "63745f636f756e74223a302c22736f757263655f6f626a6563745f636f756e74223a307d2c226e616d65737061636522"
        "3a226c6f6f6d2d646576222c22706f737467726573223a7b2264756d705f736861323536223a22343464376436343463"
        "306633643534373637613464313231633136303636316663373762353035303532633363386361666331656561613966"
        "37373766383131222c22696d616765223a22646f636b65722e696f2f6c6962726172792f706f73746772657340736861"
        "3235363a3636363636363636363636363636363636363636363636363636363636363636363636363636363636363636"
        "3636363636363636363636363636363636363636222c22726573746f7265645f736368656d615f68656164223a223031"
        "3132222c22726573746f7265645f73746174655f736861323536223a2237653335656164353838303232393238343462"
        "616336643765313966373966343166306266646437373262623631303939306530653763363433376561663334222c22"
        "736f757263655f736368656d615f68656164223a2230313132222c22736f757263655f73746174655f73686132353622"
        "3a2237653335656164353838303232393238343462616336643765313966373966343166306266646437373262623631"
        "303939306530653763363433376561663334227d2c2272656c656173655f736861323536223a22636363636363636363"
        "636363636363636363636363636363636363636363636363636363636363636363636363636363636363636363636363"
        "63636363636363222c22736368656d61223a226c6f6f6d2d706572736f6e616c2d6465762d6261636b75702d72657374"
        "6f72652d65766964656e63652d7631222c2273656372657473223a7b226b65795f696e76656e746f72795f7368613235"
        "36223a223635666334376561343564626439653439373834343137626436626137363331656139363366363538306332"
        "3966613534613762353834336263363432616463222c2276616c7565735f696e636c75646564223a66616c73657d2c22"
        "736f75726365223a7b22636f6d6d6974223a226161616161616161616161616161616161616161616161616161616161"
        "6161616161616161616161222c2274726565223a22626262626262626262626262626262626262626262626262626262"
        "62626262626262626262626262227d2c22737461727465645f6174223a22323032362d30382d32365431393a30303a30"
        "305a222c2273746f72616765223a7b226d696e696f5f707663223a22646174612d6c6f6f6d2d6465762d6d696e696f2d"
        "30222c22706f7374677265735f707663223a22646174612d6c6f6f6d2d6465762d706f7374677265732d30222c227374"
        "6f726167655f636c617373223a226c6f6e67686f726e227d7d"
    )
    assert hashlib.sha256(canonical_v1).hexdigest() == (
        "d0f28f4c1429644bc519728f7d9737093189581da82b888ea1fc8135608923e9"
    )

    payload = b"retained payload"
    payload_digest = hashlib.sha256(payload).hexdigest()
    nonempty_manifest_payload = json.dumps(
        {
            "buckets": ["artifacts", "trajectories"],
            "objects": [
                {
                    "bucket": bucket,
                    "cache_control": None,
                    "content_type": "application/octet-stream",
                    "key": key,
                    "metadata": {},
                    "payload_sha256": payload_digest,
                    "size_bytes": len(payload),
                }
                for bucket, key in (
                    ("artifacts", "owner/first"),
                    ("trajectories", "owner/second"),
                )
            ],
            "schema": "loom-personal-dev-minio-backup-manifest-v1",
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    source_manifest.write_bytes(nonempty_manifest_payload)
    restored_manifest.write_bytes(nonempty_manifest_payload)
    retained_payload = minio_payload_root / payload_digest
    retained_payload.write_bytes(payload)
    retained_payload.chmod(0o600)

    def build_retained(*, payload_root: Path = minio_payload_root) -> dict[str, object]:
        return build_personal_dev_backup_restore_evidence(
            profile=profile,
            release=release,
            release_sha256=release_sha256,
            started_at="2026-08-26T19:00:00Z",
            completed_at="2026-08-26T19:05:00Z",
            postgres_dump_path=postgres_dump,
            postgres_source_state_path=source_state,
            postgres_restored_state_path=restored_state,
            source_schema_head="0112",
            restored_schema_head="0112",
            minio_source_manifest_path=source_manifest,
            minio_restored_manifest_path=restored_manifest,
            minio_payload_root=payload_root,
            secret_key_inventory_path=secret_path,
            pre_shadow_status_path=pre_status,
            post_shadow_status_path=post_status,
            storage_inventory_path=storage_path,
        )

    value = build_retained()

    assert value["schema"] == "loom-personal-dev-backup-restore-evidence-v2"
    assert value["minio"]["source_object_count"] == 2  # type: ignore[index]
    assert value["minio"]["restored_object_count"] == 2  # type: ignore[index]
    assert value["minio"]["retained_payload_count"] == 1  # type: ignore[index]
    assert value["minio"]["retained_payload_bytes"] == len(payload)  # type: ignore[index]
    manifest = load_personal_dev_minio_manifest(source_manifest)
    assert (
        value["minio"]["retained_payload_inventory_sha256"]
        == hashlib.sha256(  # type: ignore[index]
            manifest.payload_inventory_bytes
        ).hexdigest()
    )

    restored_manifest.write_bytes(
        nonempty_manifest_payload.replace(b"owner/second", b"owner/different")
    )
    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        build_retained()
    restored_manifest.write_bytes(nonempty_manifest_payload)

    retained_payload.unlink()
    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        build_retained()
    retained_payload.write_bytes(payload)
    retained_payload.chmod(0o600)

    extra_payload = minio_payload_root / "unexpected"
    extra_payload.write_bytes(b"unexpected")
    extra_payload.chmod(0o600)
    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        build_retained()
    extra_payload.unlink()

    retained_payload.write_bytes(b"corrupt payload")
    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        build_retained()
    retained_payload.write_bytes(payload)
    retained_payload.chmod(0o600)

    wrong_payload_root = tmp_path / "wrong-minio-payloads"
    wrong_payload_root.mkdir(mode=0o700)
    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        build_retained(payload_root=wrong_payload_root)

    for digest_key in (
        "backup_manifest_sha256",
        "restored_manifest_sha256",
        "retained_payload_inventory_sha256",
    ):
        zero_digest = deepcopy(value)
        zero_digest["minio"][digest_key] = "0" * 64  # type: ignore[index]
        with pytest.raises(ValueError):
            acceptance_evidence.PersonalDevBackupRestoreEvidence.model_validate(zero_digest)

    v1_nonempty = deepcopy(value)
    v1_nonempty["schema"] = "loom-personal-dev-backup-restore-evidence-v1"
    with pytest.raises(ValueError):
        acceptance_evidence.PersonalDevBackupRestoreEvidence.model_validate(v1_nonempty)

    v2_empty = deepcopy(value)
    v2_empty["schema"] = "loom-personal-dev-backup-restore-evidence-v2"
    v2_empty["minio"] = {
        "backup_manifest_sha256": hashlib.sha256(manifest_payload).hexdigest(),
        "image": release.images.minio,
        "restored_manifest_sha256": hashlib.sha256(manifest_payload).hexdigest(),
        "restored_object_count": 0,
        "source_object_count": 0,
    }
    with pytest.raises(ValueError):
        acceptance_evidence.PersonalDevBackupRestoreEvidence.model_validate(v2_empty)

    evidence_path = owner_file(
        "retained-backup-restore-evidence.json",
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii"),
    )
    evidence_sha256 = hashlib.sha256(evidence_path.read_bytes()).hexdigest()
    assert (
        load_personal_dev_backup_restore_evidence(
            evidence_path,
            expected_sha256=evidence_sha256,
            release=release,
            release_sha256=release_sha256,
            expected_schema_head="0112",
        ).schema_name
        == "loom-personal-dev-backup-restore-evidence-v2"
    )
    for invalid_variant in (v1_nonempty, v2_empty):
        evidence_payload = json.dumps(
            invalid_variant,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        evidence_path.write_bytes(evidence_payload)
        with pytest.raises(PersonalDevAcceptanceEvidenceError):
            load_personal_dev_backup_restore_evidence(
                evidence_path,
                expected_sha256=hashlib.sha256(evidence_payload).hexdigest(),
                release=release,
                release_sha256=release_sha256,
                expected_schema_head="0112",
            )

    incomplete_bucket_payload = json.dumps(
        {"buckets": ["artifacts"], "objects": []},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    source_manifest.write_bytes(incomplete_bucket_payload)
    restored_manifest.write_bytes(incomplete_bucket_payload)
    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        build_personal_dev_backup_restore_evidence(
            profile=profile,
            release=release,
            release_sha256=release_sha256,
            started_at="2026-08-26T19:00:00Z",
            completed_at="2026-08-26T19:05:00Z",
            postgres_dump_path=postgres_dump,
            postgres_source_state_path=source_state,
            postgres_restored_state_path=restored_state,
            source_schema_head="0112",
            restored_schema_head="0112",
            minio_source_manifest_path=source_manifest,
            minio_restored_manifest_path=restored_manifest,
            minio_payload_root=minio_payload_root,
            secret_key_inventory_path=secret_path,
            pre_shadow_status_path=pre_status,
            post_shadow_status_path=post_status,
            storage_inventory_path=storage_path,
        )


def test_postgres_state_accepts_canonical_sequence_and_table_inventory() -> None:
    payload = (
        f"sequence\tpublic.example_id_seq\t7\tt\ntable\tpublic.alembic_version\t1\t{'5' * 64}\n"
    )

    acceptance_evidence._validate_postgres_state(payload.encode("ascii"))


@pytest.mark.parametrize(
    "payload",
    [
        "sequence\tpublic.example_id_seq\t7\tt\n",
        (f"table\tpublic.alembic_version\t1\t{'5' * 64}\nsequence\tpublic.example_id_seq\t7\tt\n"),
        (
            "sequence\tpublic.example_id_seq\t7\tt\n"
            "sequence\tpublic.example_id_seq\t8\tt\n"
            f"table\tpublic.alembic_version\t1\t{'5' * 64}\n"
        ),
        (
            "sequence\tpublic.example_id_seq\t7\ttrue\n"
            f"table\tpublic.alembic_version\t1\t{'5' * 64}\n"
        ),
    ],
)
def test_postgres_state_rejects_incomplete_or_noncanonical_inventory(payload: str) -> None:
    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        acceptance_evidence._validate_postgres_state(payload.encode("ascii"))


_EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
_DENIAL_RECEIPT_SHA256S = {
    "read": "5221aecb79691760f14503ae676f2741375d052ee5e2f02724c179b52a32deba",
    "update": "e7e5b514f646a59d310af7cdfcdaa57ec1aca3255e6f742c1fda5a83e0356123",
    "destroy": "da570383210d38d6392b19185ed0d2bddd72a9e8c4f6200733d00c811197e822",
}
_RESULT_IDENTITIES = (
    {
        "artifacts_bucket": "loom-dev-owner-a-artifacts",
        "database": "loom_dev_owner_a",
        "environment": "dev-owner-a",
        "namespace": "loom-dev-owner-a",
        "route_host": "owner-a.dev.yylx.world",
        "route_path": "/dev-owner-a",
        "task_bucket": "loom-dev-owner-a-tasks",
        "trajectories_bucket": "loom-dev-owner-a-trajectories",
        "worker_control_plane_host": "cp-owner-a.dev.yylx.world",
        "worker_gateway_host": "gw-owner-a.dev.yylx.world",
        "worker_pool": "dev-owner-a",
    },
    {
        "artifacts_bucket": "loom-dev-owner-b-artifacts",
        "database": "loom_dev_owner_b",
        "environment": "dev-owner-b",
        "namespace": "loom-dev-owner-b",
        "route_host": "owner-b.dev.yylx.world",
        "route_path": "/dev-owner-b",
        "task_bucket": "loom-dev-owner-b-tasks",
        "trajectories_bucket": "loom-dev-owner-b-trajectories",
        "worker_control_plane_host": "cp-owner-b.dev.yylx.world",
        "worker_gateway_host": "gw-owner-b.dev.yylx.world",
        "worker_pool": "dev-owner-b",
    },
)


def _result_plan(tmp_path: Path):  # type: ignore[no-untyped-def]
    (
        _profile,
        _release,
        _release_sha256,
        v1_plan,
        _source_root,
        _launcher_path,
        _scanner_path,
        _backup_path,
    ) = _inputs(tmp_path)
    value = v1_plan.canonical_value()
    value["schema_version"] = 2
    owner_0 = value.pop("acceptance_owner")
    value["acceptance_owners"] = sorted(
        [
            owner_0,
            {
                "team_id": "00000000-0000-0000-0000-000000000006",
                "user_id": "00000000-0000-0000-0000-000000000005",
            },
        ],
        key=lambda owner: (owner["team_id"], owner["user_id"]),
    )
    value["quotas"]["global_live_instances"] = 2
    value["quotas"]["builder_global_concurrency"] = 2
    plan_root = tmp_path / "result-plan"
    plan_root.mkdir()
    plan_path, plan_sha256 = _write_plan(plan_root, value)
    plan = load_personal_dev_acceptance_plan(plan_path, plan_sha256)
    return plan, v1_plan


def _native_result_plan(tmp_path: Path):  # type: ignore[no-untyped-def]
    plan, _v1_plan = _result_plan(tmp_path)
    value = plan.canonical_value()
    value["schema_version"] = 3
    value["native_builder"] = {
        "agent_instance_id": "00000000-0000-0000-0000-000000000501",
        "agent_key_id": "gb10-native-builder-v1",
        "freshness_seconds": 60,
        "host_boot_id": "00000000-0000-0000-0000-000000000502",
        "host_name": "gx10-01c7",
        "max_concurrency": 2,
        "platform": "linux/arm64",
        "protocol_version": 1,
        "provider": "gb10-gvisor-docker-v1",
        "public_key_sha256": "1" * 64,
        "public_store_endpoint_cidrs": ["8.8.8.8/32"],
        "public_store_origin": "https://store.example.test",
        "runtime_profile_sha256": "2" * 64,
    }
    plan_root = tmp_path / "native-result-plan"
    plan_root.mkdir()
    plan_path, plan_sha256 = _write_plan(plan_root, value)
    return load_personal_dev_acceptance_plan(plan_path, plan_sha256)


def _result_snapshot(
    plan,  # type: ignore[no-untyped-def]
    owner_index: int,
    *,
    phase: str,
) -> dict[str, object]:
    owner = plan.acceptance_owners[owner_index]
    name = f"owner-{'a' if owner_index == 0 else 'b'}"
    subject = f"00000000-0000-0000-0000-000000000{100 if owner_index == 0 else 200}"
    incarnation = f"00000000-0000-0000-0000-000000000{101 if owner_index == 0 else 201}"
    initial_candidate = "9" * 64 if owner_index == 0 else "b" * 64
    updated_candidate = "a" * 64 if owner_index == 0 else "c" * 64
    updated_max = 3 if owner_index == 0 else 4
    values = {
        "initial": {
            "candidate_sha": initial_candidate,
            "capacity_prepared": True,
            "capacity_status": "prepared",
            "deployment_generation": 1,
            "keep_data": False,
            "max_slots": 2,
            "operation_epoch": 1,
            "status": "ready",
        },
        "updated": {
            "candidate_sha": updated_candidate,
            "capacity_prepared": True,
            "capacity_status": "prepared",
            "deployment_generation": 2,
            "keep_data": False,
            "max_slots": updated_max,
            "operation_epoch": 2,
            "status": "ready",
        },
        "destroyed": {
            "candidate_sha": updated_candidate,
            "capacity_prepared": False,
            "capacity_status": "shadow",
            "deployment_generation": 2,
            "keep_data": owner_index == 1,
            "max_slots": updated_max,
            "operation_epoch": 3,
            "status": "deleted",
        },
        "redeployed": {
            "candidate_sha": "d" * 64,
            "capacity_prepared": True,
            "capacity_status": "prepared",
            "deployment_generation": 1,
            "keep_data": False,
            "max_slots": 2,
            "operation_epoch": 4,
            "status": "ready",
        },
        "final_destroyed": {
            "candidate_sha": "d" * 64,
            "capacity_prepared": False,
            "capacity_status": "shadow",
            "deployment_generation": 1,
            "keep_data": False,
            "max_slots": 2,
            "operation_epoch": 5,
            "status": "deleted",
        },
    }
    selected = values[phase]
    if owner_index == 1 and phase in {"redeployed", "final_destroyed"}:
        incarnation = "00000000-0000-0000-0000-000000000202"
    return {
        "application_status": selected["status"],
        "candidate_sha": selected["candidate_sha"],
        "capacity_prepared": selected["capacity_prepared"],
        "capacity_status": selected["capacity_status"],
        "deployment_generation": selected["deployment_generation"],
        "identity": deepcopy(_RESULT_IDENTITIES[owner_index]),
        "keep_data": selected["keep_data"],
        "max_slots": selected["max_slots"],
        "min_slots": 0,
        "name": name,
        "operation_epoch": selected["operation_epoch"],
        "owner_team_id": str(owner.team_id),
        "owner_user_id": str(owner.user_id),
        "status": selected["status"],
        "subject_id": subject,
        "subject_incarnation": incarnation,
        "worker_available": False,
    }


def _result_value(plan) -> dict[str, object]:  # type: ignore[no-untyped-def]
    owner_results: list[dict[str, object]] = []
    for owner_index in range(2):
        owner_results.append(
            {
                "destroyed": _result_snapshot(plan, owner_index, phase="destroyed"),
                "final_destroyed": (
                    _result_snapshot(plan, owner_index, phase="final_destroyed")
                    if owner_index == 1
                    else None
                ),
                "initial": _result_snapshot(plan, owner_index, phase="initial"),
                "redeployed": (
                    _result_snapshot(plan, owner_index, phase="redeployed")
                    if owner_index == 1
                    else None
                ),
                "updated": _result_snapshot(plan, owner_index, phase="updated"),
            }
        )
    denials: list[dict[str, object]] = []
    for actor_index, target_index in ((0, 1), (1, 0)):
        actor = plan.acceptance_owners[actor_index]
        target = plan.acceptance_owners[target_index]
        for operation in ("read", "update", "destroy"):
            denials.append(
                {
                    "actor_team_id": str(actor.team_id),
                    "actor_user_id": str(actor.user_id),
                    "exit_code": 1,
                    "operation": operation,
                    "stderr_sha256": _DENIAL_RECEIPT_SHA256S[operation],
                    "stdout_sha256": _EMPTY_SHA256,
                    "target_after_sha256": "e" * 64,
                    "target_before_sha256": "e" * 64,
                    "target_environment": f"owner-{'a' if target_index == 0 else 'b'}",
                    "target_team_id": str(target.team_id),
                    "target_user_id": str(target.user_id),
                }
            )
    return {
        "acceptance_manifest_sha256": "a" * 64,
        "acceptance_plan_sha256": plan.sha256,
        "cross_owner_denials": denials,
        "owners": owner_results,
        "release_sha256": plan.release.trusted_release_sha256,
        "schema": "loom-personal-dev-zero-capacity-acceptance-result-v2",
        "shadow_manifest_sha256": plan.release.shadow_manifest_sha256,
        "status_sha256s": {
            "after_denials": "1" * 64,
            "after_destroy": "2" * 64,
            "after_initial": "3" * 64,
            "after_redeploy": "4" * 64,
            "after_updates": "5" * 64,
            "pre_deploy": "6" * 64,
            "pre_rollback": "7" * 64,
            "rollback_shadow": "8" * 64,
        },
    }


def _native_result_value(plan) -> dict[str, object]:  # type: ignore[no-untyped-def]
    value = _result_value(plan)
    value["schema"] = "loom-personal-dev-zero-capacity-acceptance-result-v3"
    initial_candidates = [
        owner["initial"]["candidate_sha"] for owner in value["owners"]  # type: ignore[index,union-attr]
    ]
    accepted_candidates = [
        owner["updated"]["candidate_sha"] for owner in value["owners"]  # type: ignore[index,union-attr]
    ]
    grant_ids = [
        "00000000-0000-0000-0000-000000000601",
        "00000000-0000-0000-0000-000000000602",
    ]
    indexes = []
    for owner_index, candidate in enumerate(accepted_candidates):
        for component_index, component in enumerate(("service", "web")):
            digest = str(3 + owner_index * 2 + component_index) * 64
            indexes.append(
                {
                    "candidate_sha": candidate,
                    "component": component,
                    "manifest_sha256": digest,
                    "platforms": ["linux/amd64", "linux/arm64"],
                    "reference": f"ghcr.io/qianyi-sun/loom-dev-{component}@sha256:{digest}",
                }
            )
    value["native"] = {
        "completions": [
            {
                "buildkit_container_id": "7" * 64,
                "buildkit_running": True,
                "candidate_sha": accepted_candidates[0],
                "client_container_id": "8" * 64,
                "client_exit_code": 0,
                "client_oom_killed": False,
                "emulated": False,
                "fallback_used": False,
                "platform": "linux/arm64",
                "provider": "gb10-gvisor-docker-v1",
                "runtime_name": "runsc-personal-dev-native",
            },
            {
                "buildkit_container_id": "9" * 64,
                "buildkit_running": True,
                "candidate_sha": accepted_candidates[1],
                "client_container_id": "a" * 64,
                "client_exit_code": 0,
                "client_oom_killed": False,
                "emulated": False,
                "fallback_used": False,
                "platform": "linux/arm64",
                "provider": "gb10-gvisor-docker-v1",
                "runtime_name": "runsc-personal-dev-native",
            },
        ],
        "evidence_sha256s": {
            "after_slurm": "1" * 64,
            "before_slurm": "2" * 64,
            "candidate_publications": "3" * 64,
            "final_capacity": "4" * 64,
            "final_zero_grants": "5" * 64,
            "final_zero_namespaces": "6" * 64,
            "final_zero_tasks": "7" * 64,
            "final_zero_workers": "8" * 64,
            "native_runtime": "9" * 64,
            "simultaneous_containers": "a" * 64,
            "simultaneous_grants": "b" * 64,
            "simultaneous_jobs": "c" * 64,
        },
        "indexes": indexes,
        "overlap": {
            "amd64_jobs": [
                {
                    "candidate": initial_candidates[0][:12],
                    "name": "loom-build-owner-a-amd64",
                    "namespace": "loom-build-owner-a",
                    "runtime_class": "loom-personal-dev-builder",
                    "uid": "00000000-0000-0000-0000-000000000701",
                },
                {
                    "candidate": initial_candidates[1][:12],
                    "name": "loom-build-owner-b-amd64",
                    "namespace": "loom-build-owner-b",
                    "runtime_class": "loom-personal-dev-builder",
                    "uid": "00000000-0000-0000-0000-000000000702",
                },
            ],
            "arm64_containers": [
                {
                    "grant_id": grant_ids[0],
                    "id": "b" * 64,
                    "image": "sha256:" + "c" * 64,
                    "platform": "linux/arm64",
                    "role": "buildkit",
                    "runtime": "runsc-personal-dev-native",
                },
                {
                    "grant_id": grant_ids[0],
                    "id": "d" * 64,
                    "image": "sha256:" + "e" * 64,
                    "platform": "linux/arm64",
                    "role": "client",
                    "runtime": "runsc-personal-dev-native",
                },
                {
                    "grant_id": grant_ids[1],
                    "id": "f" * 64,
                    "image": "sha256:" + "1" * 64,
                    "platform": "linux/arm64",
                    "role": "buildkit",
                    "runtime": "runsc-personal-dev-native",
                },
                {
                    "grant_id": grant_ids[1],
                    "id": "2" * 64,
                    "image": "sha256:" + "3" * 64,
                    "platform": "linux/arm64",
                    "role": "client",
                    "runtime": "runsc-personal-dev-native",
                },
            ],
            "arm64_grants": [
                {
                    "candidate": initial_candidates[0][:12],
                    "grant_id": grant_ids[0],
                    "platform": "linux/arm64",
                    "provider": "gb10-gvisor-docker-v1",
                    "state": "running",
                },
                {
                    "candidate": initial_candidates[1][:12],
                    "grant_id": grant_ids[1],
                    "platform": "linux/arm64",
                    "provider": "gb10-gvisor-docker-v1",
                    "state": "running",
                },
            ],
        },
        "zero_capacity": {
            "active_native_grants": 0,
            "dynamic_namespace_count": 0,
            "executable_new_capacity_ceiling": 0,
            "loom_slurm_jobs_after": 0,
            "loom_slurm_jobs_before": 0,
            "tasks_after": 0,
            "tasks_before": 0,
            "worker_available": False,
            "workers_after": 0,
            "workers_before": 0,
        },
    }
    return value


def test_acceptance_result_v3_loads_strict_native_platform_evidence(
    tmp_path: Path,
) -> None:
    plan = _native_result_plan(tmp_path)
    value = _native_result_value(plan)
    path = tmp_path / "native-acceptance-result.json"
    sha256 = _write_owner_only(path, value)

    result = load_personal_dev_acceptance_result(
        path,
        sha256,
        plan=plan,
        expected_acceptance_manifest_sha256="a" * 64,
    )

    assert result.schema_name == "loom-personal-dev-zero-capacity-acceptance-result-v3"
    assert result.native is not None
    assert len(result.native.overlap.amd64_jobs) == 2
    assert len(result.native.overlap.arm64_grants) == 2
    assert len(result.native.overlap.arm64_containers) == 4
    assert len(result.native.completions) == 2
    assert len(result.native.indexes) == 4


def test_acceptance_result_v3_rejects_reused_overlap_container_identity(
    tmp_path: Path,
) -> None:
    plan = _native_result_plan(tmp_path)
    value = _native_result_value(plan)
    native = value["native"]  # type: ignore[assignment]
    native["completions"][0]["buildkit_container_id"] = (  # type: ignore[index]
        native["overlap"]["arm64_containers"][0]["id"]  # type: ignore[index]
    )
    path = tmp_path / "native-acceptance-result.json"
    sha256 = _write_owner_only(path, value)

    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        load_personal_dev_acceptance_result(
            path,
            sha256,
            plan=plan,
            expected_acceptance_manifest_sha256="a" * 64,
        )


@pytest.mark.parametrize(
    "mutation",
    [
        "job-count",
        "job-candidate",
        "job-runtime",
        "grant-candidate",
        "grant-duplicate",
        "container-grant",
        "container-role",
        "completion-candidate",
        "completion-emulated",
        "completion-fallback",
        "index-candidate",
        "index-component",
        "index-platform",
        "index-digest",
        "empty-evidence",
        "active-grant",
        "task-count-drift",
    ],
)
def test_acceptance_result_v3_rejects_native_evidence_drift(
    tmp_path: Path,
    mutation: str,
) -> None:
    plan = _native_result_plan(tmp_path)
    value = _native_result_value(plan)
    native = value["native"]  # type: ignore[assignment]
    overlap = native["overlap"]  # type: ignore[index]
    if mutation == "job-count":
        overlap["amd64_jobs"].pop()  # type: ignore[index]
    elif mutation == "job-candidate":
        overlap["amd64_jobs"][0]["candidate"] = "0" * 12  # type: ignore[index]
    elif mutation == "job-runtime":
        overlap["amd64_jobs"][0]["runtime_class"] = "runc"  # type: ignore[index]
    elif mutation == "grant-candidate":
        overlap["arm64_grants"][0]["candidate"] = "0" * 12  # type: ignore[index]
    elif mutation == "grant-duplicate":
        overlap["arm64_grants"][1]["grant_id"] = overlap["arm64_grants"][0][  # type: ignore[index]
            "grant_id"
        ]
    elif mutation == "container-grant":
        overlap["arm64_containers"][0]["grant_id"] = (  # type: ignore[index]
            "00000000-0000-0000-0000-000000000699"
        )
    elif mutation == "container-role":
        overlap["arm64_containers"][1]["role"] = "buildkit"  # type: ignore[index]
    elif mutation == "completion-candidate":
        native["completions"][0]["candidate_sha"] = "0" * 64  # type: ignore[index]
    elif mutation == "completion-emulated":
        native["completions"][0]["emulated"] = True  # type: ignore[index]
    elif mutation == "completion-fallback":
        native["completions"][0]["fallback_used"] = True  # type: ignore[index]
    elif mutation == "index-candidate":
        native["indexes"][0]["candidate_sha"] = "0" * 64  # type: ignore[index]
    elif mutation == "index-component":
        native["indexes"][1]["component"] = "service"  # type: ignore[index]
    elif mutation == "index-platform":
        native["indexes"][0]["platforms"] = ["linux/amd64", "linux/amd64"]  # type: ignore[index]
    elif mutation == "index-digest":
        native["indexes"][0]["manifest_sha256"] = "f" * 64  # type: ignore[index]
    elif mutation == "empty-evidence":
        native["evidence_sha256s"]["native_runtime"] = _EMPTY_SHA256  # type: ignore[index]
    elif mutation == "active-grant":
        native["zero_capacity"]["active_native_grants"] = 1  # type: ignore[index]
    else:
        native["zero_capacity"]["tasks_after"] = 1  # type: ignore[index]

    path = tmp_path / "native-acceptance-result.json"
    sha256 = _write_owner_only(path, value)
    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        load_personal_dev_acceptance_result(
            path,
            sha256,
            plan=plan,
            expected_acceptance_manifest_sha256="a" * 64,
        )


def test_acceptance_result_schema_cannot_cross_native_plan_boundary(tmp_path: Path) -> None:
    plan, _v1_plan = _result_plan(tmp_path)
    native_plan_root = tmp_path / "native-boundary"
    native_plan_root.mkdir()
    native_value = plan.canonical_value()
    native_value["schema_version"] = 3
    native_value["native_builder"] = {
        "agent_instance_id": "00000000-0000-0000-0000-000000000501",
        "agent_key_id": "gb10-native-builder-v1",
        "freshness_seconds": 60,
        "host_boot_id": "00000000-0000-0000-0000-000000000502",
        "host_name": "gx10-01c7",
        "max_concurrency": 2,
        "platform": "linux/arm64",
        "protocol_version": 1,
        "provider": "gb10-gvisor-docker-v1",
        "public_key_sha256": "1" * 64,
        "public_store_endpoint_cidrs": ["8.8.8.8/32"],
        "public_store_origin": "https://store.example.test",
        "runtime_profile_sha256": "2" * 64,
    }
    native_path, native_sha256 = _write_plan(native_plan_root, native_value)
    native_plan = load_personal_dev_acceptance_plan(native_path, native_sha256)

    v2_path = tmp_path / "v2-result.json"
    v2_sha256 = _write_owner_only(v2_path, _result_value(plan))
    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        load_personal_dev_acceptance_result(
            v2_path,
            v2_sha256,
            plan=native_plan,
            expected_acceptance_manifest_sha256="a" * 64,
        )

    v3_path = tmp_path / "v3-result.json"
    v3_sha256 = _write_owner_only(v3_path, _native_result_value(native_plan))
    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        load_personal_dev_acceptance_result(
            v3_path,
            v3_sha256,
            plan=plan,
            expected_acceptance_manifest_sha256="a" * 64,
        )


def _load_result(tmp_path: Path, value: object, plan) -> PersonalDevAcceptanceResultV2:  # type: ignore[no-untyped-def]
    path = tmp_path / "acceptance-result.json"
    sha256 = _write_owner_only(path, value)
    return load_personal_dev_acceptance_result(
        path,
        sha256,
        plan=plan,
        expected_acceptance_manifest_sha256="a" * 64,
    )


def test_acceptance_result_v2_loads_canonical_concurrent_owner_evidence(
    tmp_path: Path,
) -> None:
    plan, _v1_plan = _result_plan(tmp_path)
    value = _result_value(plan)

    result = _load_result(tmp_path, value, plan)

    assert result.canonical_bytes() == json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    assert result.owners[0].updated.max_slots == 3
    assert result.owners[1].updated.max_slots == 4
    assert result.owners[1].redeployed is not None
    assert result.owners[1].redeployed.subject_id == result.owners[1].initial.subject_id
    assert (
        result.owners[1].redeployed.subject_incarnation
        != result.owners[1].initial.subject_incarnation
    )


def test_acceptance_result_v2_rejects_equal_updated_candidates_across_owners(
    tmp_path: Path,
) -> None:
    plan, _v1_plan = _result_plan(tmp_path)
    value = _result_value(plan)
    owners = value["owners"]  # type: ignore[assignment]
    owner_0_updated_candidate = owners[0]["updated"]["candidate_sha"]
    owners[1]["updated"]["candidate_sha"] = owner_0_updated_candidate
    owners[1]["destroyed"]["candidate_sha"] = owner_0_updated_candidate

    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        _load_result(tmp_path, value, plan)


@pytest.mark.parametrize("denial_index", range(6))
def test_acceptance_result_v2_rejects_empty_denial_target_status_evidence(
    tmp_path: Path,
    denial_index: int,
) -> None:
    plan, _v1_plan = _result_plan(tmp_path)
    value = _result_value(plan)
    denials = value["cross_owner_denials"]  # type: ignore[assignment]
    denials[denial_index]["target_before_sha256"] = _EMPTY_SHA256
    denials[denial_index]["target_after_sha256"] = _EMPTY_SHA256

    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        _load_result(tmp_path, value, plan)


@pytest.mark.parametrize(
    "status_field",
    [
        "after_denials",
        "after_destroy",
        "after_initial",
        "after_redeploy",
        "after_updates",
        "pre_deploy",
        "pre_rollback",
        "rollback_shadow",
    ],
)
def test_acceptance_result_v2_rejects_empty_mandatory_status_evidence(
    tmp_path: Path,
    status_field: str,
) -> None:
    plan, _v1_plan = _result_plan(tmp_path)
    value = _result_value(plan)
    status_sha256s = value["status_sha256s"]  # type: ignore[assignment]
    status_sha256s[status_field] = _EMPTY_SHA256

    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        _load_result(tmp_path, value, plan)


@pytest.mark.parametrize("unsafe_kind", ["mode", "hardlink", "symlink", "race"])
def test_acceptance_result_v2_rejects_unsafe_file_metadata_or_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unsafe_kind: str,
) -> None:
    plan, _v1_plan = _result_plan(tmp_path)
    path = tmp_path / "acceptance-result.json"
    sha256 = _write_owner_only(path, _result_value(plan))
    if unsafe_kind == "mode":
        path.chmod(0o644)
    elif unsafe_kind == "hardlink":
        (tmp_path / "acceptance-result-link.json").hardlink_to(path)
    elif unsafe_kind == "symlink":
        target = path
        path = tmp_path / "acceptance-result-symlink.json"
        path.symlink_to(target)
    else:
        replacement = tmp_path / "acceptance-result-replacement.json"
        replacement.write_bytes(path.read_bytes())
        replacement.chmod(0o600)
        assert replacement.stat().st_ino != path.stat().st_ino
        real_read = acceptance_evidence.os.read
        changed = False

        def racing_read(descriptor: int, size: int) -> bytes:
            nonlocal changed
            payload = real_read(descriptor, size)
            if not changed:
                changed = True
                replacement.replace(path)
            return payload

        monkeypatch.setattr(acceptance_evidence.os, "read", racing_read)

    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        load_personal_dev_acceptance_result(
            path,
            sha256,
            plan=plan,
            expected_acceptance_manifest_sha256="a" * 64,
        )


@pytest.mark.parametrize("encoding", ["wrong-digest", "duplicate", "noncanonical"])
def test_acceptance_result_v2_rejects_untrusted_json_encoding(
    tmp_path: Path,
    encoding: str,
) -> None:
    plan, _v1_plan = _result_plan(tmp_path)
    path = tmp_path / "acceptance-result.json"
    sha256 = _write_owner_only(path, _result_value(plan))
    if encoding == "wrong-digest":
        sha256 = "f" * 64
    elif encoding == "duplicate":
        payload = path.read_bytes().replace(
            b'"schema":',
            b'"schema":"loom-personal-dev-zero-capacity-acceptance-result-v2","schema":',
            1,
        )
        path.write_bytes(payload)
        sha256 = hashlib.sha256(payload).hexdigest()
    else:
        payload = path.read_bytes() + b"\n"
        path.write_bytes(payload)
        sha256 = hashlib.sha256(payload).hexdigest()

    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        load_personal_dev_acceptance_result(
            path,
            sha256,
            plan=plan,
            expected_acceptance_manifest_sha256="a" * 64,
        )


_RESULT_MUTATIONS = (
    "extra-field",
    "missing-field",
    "snapshot-extra-field",
    "snapshot-missing-field",
    "owner-order",
    "identity-mismatch",
    "malformed-name",
    "malformed-uuid",
    "malformed-digest",
    "manifest-binding",
    "release-binding",
    "plan-binding",
    "shadow-binding",
    "denial-incomplete",
    "denial-duplicate",
    "denial-reordered",
    "denial-wrong-target",
    "denial-success",
    "denial-wrong-exit",
    "denial-nonempty-stdout",
    "denial-empty-stderr",
    "denial-wrong-phase-receipt",
    "denial-candidate-receipt",
    "denial-401-receipt",
    "denial-403-receipt",
    "denial-500-receipt",
    "denial-target-detail",
    "denial-state-change",
    "worker-available",
    "nonzero-minimum",
    "wrong-initial-maximum",
    "wrong-updated-maximum",
    "candidate-regression",
    "generation-regression",
    "epoch-regression",
    "cross-owner-identity-equality",
    "owner0-keep-data",
    "owner1-no-keep-data",
    "owner0-redeploy",
    "rotated-subject",
    "unrotated-incarnation",
    "missing-final-destroy",
)


def _mutate_result(value: dict[str, object], mutation: str) -> None:
    owners = value["owners"]  # type: ignore[assignment]
    denials = value["cross_owner_denials"]  # type: ignore[assignment]
    if mutation == "extra-field":
        value["unexpected"] = True
    elif mutation == "missing-field":
        del value["status_sha256s"]
    elif mutation == "snapshot-extra-field":
        owners[0]["initial"]["unexpected"] = True
    elif mutation == "snapshot-missing-field":
        del owners[0]["initial"]["capacity_prepared"]
    elif mutation == "owner-order":
        owners.reverse()
    elif mutation == "identity-mismatch":
        owners[0]["initial"]["identity"]["namespace"] = "loom-dev-wrong"
    elif mutation == "malformed-name":
        owners[0]["initial"]["name"] = "INVALID"
    elif mutation == "malformed-uuid":
        owners[0]["initial"]["subject_id"] = "not-a-uuid"
    elif mutation == "malformed-digest":
        owners[0]["initial"]["candidate_sha"] = "A" * 64
    elif mutation == "manifest-binding":
        value["acceptance_manifest_sha256"] = "9" * 64
    elif mutation == "release-binding":
        value["release_sha256"] = "9" * 64
    elif mutation == "plan-binding":
        value["acceptance_plan_sha256"] = "9" * 64
    elif mutation == "shadow-binding":
        value["shadow_manifest_sha256"] = "9" * 64
    elif mutation == "denial-incomplete":
        denials.pop()
    elif mutation == "denial-duplicate":
        denials[-1] = deepcopy(denials[0])
    elif mutation == "denial-reordered":
        denials[0], denials[1] = denials[1], denials[0]
    elif mutation == "denial-wrong-target":
        denials[0]["target_environment"] = "owner-a"
    elif mutation == "denial-success":
        denials[0]["exit_code"] = 0
    elif mutation == "denial-wrong-exit":
        denials[0]["exit_code"] = 2
    elif mutation == "denial-nonempty-stdout":
        denials[0]["stdout_sha256"] = "1" * 64
    elif mutation == "denial-empty-stderr":
        denials[0]["stderr_sha256"] = _EMPTY_SHA256
    elif mutation == "denial-wrong-phase-receipt":
        denials[0]["stderr_sha256"] = (
            "e7e5b514f646a59d310af7cdfcdaa57ec1aca3255e6f742c1fda5a83e0356123"
        )
    elif mutation == "denial-candidate-receipt":
        denials[0]["stderr_sha256"] = (
            "1bf777bf8fa65daf05519e35e21c5070000dd44e3ab6c4eb5ab816aaafff1869"
        )
    elif mutation == "denial-401-receipt":
        denials[0]["stderr_sha256"] = (
            "7f2f198b538da2eb84861d3b8cb11a2a9bf4048e3c7e16653d4d9299825a98db"
        )
    elif mutation == "denial-403-receipt":
        denials[0]["stderr_sha256"] = (
            "88c06651a7a64419e5ec7b19d615d3a2edecafa3eb04c44e7ace70a32068a3ca"
        )
    elif mutation == "denial-500-receipt":
        denials[0]["stderr_sha256"] = (
            "e912f9365fee2e45c3965a237fce784c4e910ff5fa618c710d0ff5a80b252194"
        )
    elif mutation == "denial-target-detail":
        denials[0]["stderr_sha256"] = (
            "04bb25735501a6b19809e2ce2e672a76e222e8ff48b943b75a614bccc883ae52"
        )
    elif mutation == "denial-state-change":
        denials[0]["target_after_sha256"] = "d" * 64
    elif mutation == "worker-available":
        owners[0]["updated"]["worker_available"] = True
    elif mutation == "nonzero-minimum":
        owners[0]["initial"]["min_slots"] = 1
    elif mutation == "wrong-initial-maximum":
        owners[0]["initial"]["max_slots"] = 1
    elif mutation == "wrong-updated-maximum":
        owners[1]["updated"]["max_slots"] = 3
    elif mutation == "candidate-regression":
        owners[0]["updated"]["candidate_sha"] = owners[0]["initial"]["candidate_sha"]
    elif mutation == "generation-regression":
        owners[0]["updated"]["deployment_generation"] = 1
    elif mutation == "epoch-regression":
        owners[0]["updated"]["operation_epoch"] = 1
    elif mutation == "cross-owner-identity-equality":
        for phase in ("initial", "updated", "destroyed", "redeployed", "final_destroyed"):
            owners[1][phase]["name"] = "owner-a"
            owners[1][phase]["identity"] = deepcopy(_RESULT_IDENTITIES[0])
        for denial in denials[:3]:
            denial["target_environment"] = "owner-a"
    elif mutation == "owner0-keep-data":
        owners[0]["destroyed"]["keep_data"] = True
    elif mutation == "owner1-no-keep-data":
        owners[1]["destroyed"]["keep_data"] = False
    elif mutation == "owner0-redeploy":
        owners[0]["redeployed"] = deepcopy(owners[1]["redeployed"])
    elif mutation == "rotated-subject":
        owners[1]["redeployed"]["subject_id"] = "00000000-0000-0000-0000-000000000999"
    elif mutation == "unrotated-incarnation":
        owners[1]["redeployed"]["subject_incarnation"] = owners[1]["destroyed"][
            "subject_incarnation"
        ]
    elif mutation == "missing-final-destroy":
        owners[1]["final_destroyed"] = None


@pytest.mark.parametrize("mutation", _RESULT_MUTATIONS)
def test_acceptance_result_v2_rejects_contract_or_transition_mutation(
    tmp_path: Path,
    mutation: str,
) -> None:
    plan, _v1_plan = _result_plan(tmp_path)
    value = _result_value(plan)
    _mutate_result(value, mutation)

    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        _load_result(tmp_path, value, plan)


def test_acceptance_result_v2_rejects_single_owner_v1_plan(tmp_path: Path) -> None:
    plan, v1_plan = _result_plan(tmp_path)

    with pytest.raises(PersonalDevAcceptanceEvidenceError):
        _load_result(tmp_path, _result_value(plan), v1_plan)
