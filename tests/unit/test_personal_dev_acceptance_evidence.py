from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

from loom import personal_dev_acceptance_evidence as acceptance_evidence
from loom.personal_dev_acceptance_evidence import (
    PersonalDevAcceptanceEvidenceError,
    build_personal_dev_backup_restore_evidence,
    build_personal_dev_scanner_finding_policy,
    build_personal_dev_trusted_launcher_profile,
    load_personal_dev_backup_restore_evidence,
    validate_personal_dev_policy_evidence,
)
from loom.personal_dev_control_plane_config import (
    load_personal_dev_acceptance_plan,
    load_personal_dev_control_plane_profile,
)
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


def test_backup_restore_evidence_is_derived_from_supporting_artifacts(
    tmp_path: Path,
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

    def owner_file(name: str, payload: bytes) -> Path:
        path = tmp_path / name
        path.write_bytes(payload)
        path.chmod(0o600)
        return path

    postgres_dump = owner_file("postgres.dump", b"exact-postgres-dump")
    source_state = owner_file(
        "postgres.source.tsv",
        (
            "sequence\tpublic.example_id_seq\t7\tt\n"
            f"table\tpublic.alembic_version\t1\t{'5' * 64}\n"
        ).encode("ascii"),
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
    shadow_status = {
        "blockers": [],
        "components": [{"name": "personal-workers", "observed": 0}],
        "manager_ceiling": 0,
        "mode": "shadow",
        "ready": True,
        "worker_available": False,
    }
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

    nonempty_manifest_payload = json.dumps(
        {
            "buckets": ["artifacts", "trajectories"],
            "objects": [
                {
                    "bucket": "artifacts",
                    "key": "owner/object",
                    "sha256": "5" * 64,
                    "size": 7,
                }
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    source_manifest.write_bytes(nonempty_manifest_payload)
    restored_manifest.write_bytes(nonempty_manifest_payload)
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
            secret_key_inventory_path=secret_path,
            pre_shadow_status_path=pre_status,
            post_shadow_status_path=post_status,
            storage_inventory_path=storage_path,
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
            secret_key_inventory_path=secret_path,
            pre_shadow_status_path=pre_status,
            post_shadow_status_path=post_status,
            storage_inventory_path=storage_path,
        )


@pytest.mark.parametrize(
    "payload",
    [
        f"table\tpublic.alembic_version\t1\t{'5' * 64}\n",
        "sequence\tpublic.example_id_seq\t7\tt\n",
        (
            f"table\tpublic.alembic_version\t1\t{'5' * 64}\n"
            "sequence\tpublic.example_id_seq\t7\tt\n"
        ),
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
