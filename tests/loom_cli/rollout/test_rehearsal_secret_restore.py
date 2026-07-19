from __future__ import annotations

import base64
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]

from loom_cli.cluster_backup_guard import backup_manifest_sha256, write_backup_manifest
from loom_cli.rollout.rehearsal_secret_restore import build_rehearsal_secret_artifact


def _private_file(path: Path, payload: bytes) -> None:
    path.write_bytes(payload)
    path.chmod(0o600)


def _checkpoint(
    tmp_path: Path,
    *,
    complete_database_authority: bool = True,
    valid_encoding: bool = True,
) -> Path:
    root = tmp_path / "backup"
    root.mkdir(mode=0o700, parents=True)
    postgres = root / "postgres"
    postgres.mkdir(mode=0o700)
    _private_file(postgres / "loom.dump", b"exact-dump")
    inventory = root / "object-inventory.json"
    _private_file(inventory, b'{"inventory_root":"' + b"1" * 64 + b'"}\n')
    secrets = root / "secrets"
    secrets.mkdir(mode=0o700)
    for name in ("loom-admin-secret", "loom-secrets", "loom-staging-tls"):
        data = {"key": base64.b64encode((name + "-value").encode()).decode()}
        if not valid_encoding and name == "loom-admin-secret":
            data["key"] = "not-base64!"
        if name == "loom-secrets" and complete_database_authority:
            data.update(
                {
                    key: base64.b64encode(("old-" + key).encode()).decode()
                    for key in (
                        "cp-db-url",
                        "cp-db-url-pool",
                        "gw-db-url",
                        "gw-db-url-pool",
                        "postgres-password",
                        "postgres-user",
                        "svc-db-url",
                        "svc-db-url-pool",
                    )
                }
            )
        payload = yaml.safe_dump(
            {
                "apiVersion": "v1",
                "data": data,
                "kind": "Secret",
                "metadata": {
                    "creationTimestamp": "old",
                    "name": name,
                    "namespace": "loom-staging",
                    "resourceVersion": "42",
                    "uid": "old",
                },
                "type": "Opaque",
            },
            sort_keys=True,
        ).encode()
        _private_file(secrets / f"{name}.yaml", payload)
    manifest = root / "backup-manifest.json"
    write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest,
        components={
            "k8s_secrets": secrets,
            "object_inventory": inventory,
            "postgres": postgres / "loom.dump",
        },
        now=datetime(2026, 7, 19, tzinfo=UTC),
        schema_version=2,
    )
    manifest.chmod(0o600)
    return manifest


def test_secret_artifact_revalidates_checkpoint_and_binds_isolated_database(
    tmp_path: Path,
) -> None:
    manifest = _checkpoint(tmp_path)
    digest = backup_manifest_sha256(manifest, expected_owner_uid=os.geteuid())

    artifact = build_rehearsal_secret_artifact(
        manifest,
        manifest_sha256=digest,
        namespace="loom-rehearsal-" + "a" * 24,
        database="loom_rehearsal_" + "a" * 24,
        plan_digest="b" * 64,
    )

    documents = list(yaml.safe_load_all(artifact.payload))
    assert tuple(document["metadata"]["name"] for document in documents) == (
        "loom-admin-secret",
        "loom-secrets",
        "loom-staging-tls",
    )
    assert all(
        document["metadata"]
        == {
            "annotations": {"loom.openai.dev/plan-sha256": "b" * 64},
            "name": document["metadata"]["name"],
            "namespace": "loom-rehearsal-" + "a" * 24,
        }
        for document in documents
    )
    assert len(artifact.artifact_sha256) == 64
    assert len(artifact.source_component_sha256) == 64
    loom_secrets = next(
        document for document in documents if document["metadata"]["name"] == "loom-secrets"
    )
    decoded = {key: base64.b64decode(value).decode() for key, value in loom_secrets["data"].items()}
    expected_url = (
        "postgresql+psycopg://loom_rehearsal@loom-postgres:5432/loom_rehearsal_" + "a" * 24
    )
    assert decoded["postgres-user"] == "loom_rehearsal"
    assert decoded["postgres-password"] == "rehearsal-trust-only"
    assert {decoded[key] for key in decoded if key.endswith("-db-url")} == {expected_url}
    assert {decoded[key] for key in decoded if key.endswith("-db-url-pool")} == {expected_url}


def test_secret_artifact_fails_closed_on_manifest_or_secret_drift(tmp_path: Path) -> None:
    manifest = _checkpoint(tmp_path)
    digest = backup_manifest_sha256(manifest, expected_owner_uid=os.geteuid())
    secret = manifest.parent / "secrets" / "loom-secrets.yaml"
    secret.write_text(secret.read_text() + "---\n")

    with pytest.raises(ValueError, match="exact validation"):
        build_rehearsal_secret_artifact(
            manifest,
            manifest_sha256=digest,
            namespace="loom-rehearsal-" + "a" * 24,
            database="loom_rehearsal_" + "a" * 24,
            plan_digest="b" * 64,
        )

    _checkpoint(tmp_path / "second")
    second = tmp_path / "second" / "backup" / "backup-manifest.json"
    with pytest.raises(ValueError, match="exact validation"):
        build_rehearsal_secret_artifact(
            second,
            manifest_sha256="0" * 64,
            namespace="loom-rehearsal-" + "a" * 24,
            database="loom_rehearsal_" + "a" * 24,
            plan_digest="b" * 64,
        )


def test_secret_artifact_rejects_nonprivate_or_extra_document(tmp_path: Path) -> None:
    manifest = _checkpoint(tmp_path)
    digest = backup_manifest_sha256(manifest, expected_owner_uid=os.geteuid())
    secret = manifest.parent / "secrets" / "loom-secrets.yaml"
    secret.chmod(0o644)

    with pytest.raises(ValueError, match="exact validation"):
        build_rehearsal_secret_artifact(
            manifest,
            manifest_sha256=digest,
            namespace="loom-rehearsal-" + "a" * 24,
            database="loom_rehearsal_" + "a" * 24,
            plan_digest="b" * 64,
        )


def test_secret_artifact_requires_complete_database_authority(tmp_path: Path) -> None:
    manifest = _checkpoint(tmp_path, complete_database_authority=False)
    digest = backup_manifest_sha256(manifest, expected_owner_uid=os.geteuid())

    with pytest.raises(ValueError, match="database Secret authority is incomplete"):
        build_rehearsal_secret_artifact(
            manifest,
            manifest_sha256=digest,
            namespace="loom-rehearsal-" + "a" * 24,
            database="loom_rehearsal_" + "a" * 24,
            plan_digest="b" * 64,
        )


def test_secret_artifact_rejects_invalid_base64_from_valid_manifest(tmp_path: Path) -> None:
    manifest = _checkpoint(tmp_path, valid_encoding=False)
    digest = backup_manifest_sha256(manifest, expected_owner_uid=os.geteuid())

    with pytest.raises(ValueError, match="data encoding is invalid"):
        build_rehearsal_secret_artifact(
            manifest,
            manifest_sha256=digest,
            namespace="loom-rehearsal-" + "a" * 24,
            database="loom_rehearsal_" + "a" * 24,
            plan_digest="b" * 64,
        )
