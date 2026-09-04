from __future__ import annotations

import base64
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
import yaml  # type: ignore[import-untyped]

from loom_cli.cluster_backup_guard import backup_manifest_sha256, write_backup_manifest
from loom_cli.rollout.operator.checkpoint_database_authority import DatabaseAuthorityEvidence
from loom_cli.rollout.operator.protected_secret_inventory import (
    PROTECTED_SECRET_SPECS,
    build_secret_inventory,
)
from loom_cli.rollout.rehearsal_secret_restore import build_rehearsal_secret_artifact


def _private_file(path: Path, payload: bytes) -> None:
    path.write_bytes(payload)
    path.chmod(0o600)


def _checkpoint(
    tmp_path: Path,
    *,
    complete_database_authority: bool = True,
    include_pool_authority: bool = True,
    valid_encoding: bool = True,
    optional_protected_present: bool = False,
    unknown_database_url: str | None = None,
    opaque_binary_data: bytes | None = None,
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
        if name == "loom-admin-secret":
            data = {
                "secrets.toml": base64.b64encode(
                    ('[admin]\ntoken = "loom_admin_' + "a" * 40 + '"\n').encode()
                ).decode()
            }
        if not valid_encoding and name == "loom-admin-secret":
            data["key"] = "not-base64!"
        if name == "loom-secrets" and complete_database_authority:
            keys = [
                "cp-db-url",
                "gw-db-url",
                "postgres-password",
                "postgres-user",
                "svc-db-url",
            ]
            if include_pool_authority:
                keys.extend(("cp-db-url-pool", "gw-db-url-pool", "svc-db-url-pool"))
            data.update({key: base64.b64encode(("old-" + key).encode()).decode() for key in keys})
            if unknown_database_url is not None:
                data["unknown-dsn"] = base64.b64encode(unknown_database_url.encode()).decode()
        if name == "loom-staging-tls" and opaque_binary_data is not None:
            data["opaque-binary"] = base64.b64encode(opaque_binary_data).decode()
        payload = (
            json.dumps(
                {
                    "apiVersion": "v1",
                    "data": data,
                    "kind": "Secret",
                    "metadata": {"name": name, "namespace": "loom-staging"},
                    "type": "Opaque",
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode()
        _private_file(secrets / f"{name}.yaml", payload)
    observations = {}
    for spec in PROTECTED_SECRET_SPECS:
        present = spec.required or optional_protected_present
        raw = None
        if present:
            protected_data = {
                "database-url": base64.b64encode(
                    (
                        "postgresql+psycopg://live:secret@"
                        f"loom-postgres-rw.{spec.namespace}.svc.cluster.local:5432/loom"
                    ).encode()
                ).decode()
            }
            if spec.name == "loom-capacity-execution-operator":
                protected_data = {
                    "manager-read.bearer-token": base64.b64encode(
                        b"live-manager-read-token"
                    ).decode(),
                    "manager-read.certificate.pem": base64.b64encode(
                        b"live-manager-read-certificate"
                    ).decode(),
                    "manager-read.manager-ca.pem": base64.b64encode(b"live-manager-ca").decode(),
                    "manager-read.private-key.pem": base64.b64encode(
                        b"live-manager-read-private-key"
                    ).decode(),
                }
            if spec.name in {
                "loom-capacity-executor-gb10",
                "loom-capacity-executor-oldlab",
            }:
                protected_data = {
                    "bearer-token": base64.b64encode(b"live-executor-token").decode(),
                    "client-certificate.pem": base64.b64encode(
                        b"live-executor-certificate"
                    ).decode(),
                    "client-private-key.pem": base64.b64encode(
                        b"live-executor-private-key"
                    ).decode(),
                    "manager-ca.pem": base64.b64encode(b"live-manager-ca").decode(),
                    "ownership-private-key": base64.b64encode(
                        b"live-ownership-private-key"
                    ).decode(),
                }
            if spec.name == "loom-capacity-manager":
                protected_data.update(
                    {
                        "postgres-database": base64.b64encode(b"loom").decode(),
                        "postgres-password": base64.b64encode(b"secret").decode(),
                        "postgres-user": base64.b64encode(b"live").decode(),
                    }
                )
            raw = (
                json.dumps(
                    {
                        "apiVersion": "v1",
                        "data": protected_data,
                        "immutable": spec.name.startswith("loom-capacity-execut"),
                        "kind": "Secret",
                        "metadata": {
                            "annotations": {"source": "live"},
                            "creationTimestamp": "2026-01-01T00:00:00Z",
                            "labels": {"source": "live"},
                            "managedFields": [],
                            "name": spec.name,
                            "namespace": spec.namespace,
                            "resourceVersion": "42",
                            "uid": "11111111-1111-4111-8111-111111111111",
                        },
                        "type": "Opaque",
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode()
        observations[(spec.namespace, spec.name)] = (raw, raw)
    protected = build_secret_inventory(observations)
    _private_file(
        secrets / "protected-capacity-secret-inventory.json",
        protected.inventory_payload,
    )
    for filename, payload in protected.exported_objects.items():
        _private_file(secrets / filename, payload)
    authority = DatabaseAuthorityEvidence(
        public_schema_revision="0066",
        capacity_guard_schema_revision="guard_0027",
        configuration_epoch=9,
        configuration_digest="9" * 64,
        authority_incarnation=UUID("00000000-0000-4000-8000-0000000000aa"),
        writer_epoch=4,
        execution_state="shadow",
        execution_epoch=0,
        execution_manifest_sha256=None,
        executable_new_capacity_ceiling=0,
        increase_freeze=True,
    )
    authority_path = root / "database-authority.json"
    _private_file(authority_path, authority.payload)
    manifest = root / "backup-manifest.json"
    write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest,
        components={
            "k8s_secrets": secrets,
            "object_inventory": inventory,
            "postgres": postgres / "loom.dump",
            "database_authority": authority_path,
        },
        now=datetime(2026, 7, 19, tzinfo=UTC),
        schema_version=3,
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
        "loom-capacity-manager",
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
    admin_secret = next(
        document for document in documents if document["metadata"]["name"] == "loom-admin-secret"
    )
    assert base64.b64decode(admin_secret["data"]["admin-token"]).decode() == (
        "loom_admin_" + "a" * 40
    )

    manager = next(
        document
        for document in documents
        if document["metadata"]["name"] == "loom-capacity-manager"
    )
    manager_data = {key: base64.b64decode(value).decode() for key, value in manager["data"].items()}
    assert manager_data["postgres-user"] == "loom_rehearsal"
    assert manager_data["postgres-database"] == "loom_rehearsal_" + "a" * 24
    assert manager_data["postgres-password"] == "rehearsal-trust-only"
    assert manager_data["database-url"] == expected_url


def test_secret_artifact_reconstructs_present_optional_inventory_and_strips_live_metadata(
    tmp_path: Path,
) -> None:
    manifest = _checkpoint(tmp_path, optional_protected_present=True)
    digest = backup_manifest_sha256(manifest, expected_owner_uid=os.geteuid())

    artifact = build_rehearsal_secret_artifact(
        manifest,
        manifest_sha256=digest,
        namespace="loom-rehearsal-" + "a" * 24,
        database="loom_rehearsal_" + "a" * 24,
        plan_digest="b" * 64,
    )

    documents = list(yaml.safe_load_all(artifact.payload))
    names = tuple(document["metadata"]["name"] for document in documents)
    assert names == (
        "loom-admin-secret",
        "loom-secrets",
        "loom-staging-tls",
        "loom-capacity-manager",
        "loom-capacity-agent",
        "loom-protected-worker-runtime",
        "loom-capacity-execution-operator",
        "loom-capacity-executor-gb10",
        "loom-capacity-executor-oldlab",
    )
    for document in documents:
        assert document["metadata"] == {
            "annotations": {"loom.openai.dev/plan-sha256": "b" * 64},
            "name": document["metadata"]["name"],
            "namespace": "loom-rehearsal-" + "a" * 24,
        }
        decoded = {
            key: base64.b64decode(value).decode(errors="ignore")
            for key, value in document["data"].items()
        }
        for key, value in decoded.items():
            if key.endswith("db-url") or key.endswith("db-url-pool") or key == "database-url":
                assert value == (
                    "postgresql+psycopg://loom_rehearsal@loom-postgres:5432/"
                    "loom_rehearsal_" + "a" * 24
                )
        if document["metadata"]["name"].startswith("loom-capacity-execut"):
            assert document["immutable"] is True
            assert all(
                value.startswith("rehearsal-inert-execution-credential-v1:")
                for value in decoded.values()
            )
            assert not any("live" in value for value in decoded.values())


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


def test_secret_artifact_preserves_absent_optional_pool_urls(tmp_path: Path) -> None:
    manifest = _checkpoint(tmp_path, include_pool_authority=False)
    digest = backup_manifest_sha256(manifest, expected_owner_uid=os.geteuid())

    artifact = build_rehearsal_secret_artifact(
        manifest,
        manifest_sha256=digest,
        namespace="loom-rehearsal-" + "a" * 24,
        database="loom_rehearsal_" + "a" * 24,
        plan_digest="b" * 64,
    )

    loom_secrets = next(
        document
        for document in yaml.safe_load_all(artifact.payload)
        if document["metadata"]["name"] == "loom-secrets"
    )
    decoded = {key: base64.b64decode(value).decode() for key, value in loom_secrets["data"].items()}
    expected_url = (
        "postgresql+psycopg://loom_rehearsal@loom-postgres:5432/loom_rehearsal_" + "a" * 24
    )
    assert {decoded[key] for key in decoded if key.endswith("-db-url")} == {expected_url}
    assert not any(key.endswith("-db-url-pool") for key in decoded)


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


@pytest.mark.parametrize(
    "database_url",
    [
        "postgresql://live:secret@live-db.example:5432/loom",
        "postgresql+psycopg://live:secret@live-db.example:5432/loom",
        "postgresql+asyncpg://live:secret@live-db.example:5432/loom",
        "mysql+pymysql://live:secret@live-db.example:3306/loom",
    ],
)
def test_secret_artifact_rejects_unknown_database_field_with_live_dsn(
    tmp_path: Path,
    database_url: str,
) -> None:
    manifest = _checkpoint(tmp_path, unknown_database_url=database_url)
    digest = backup_manifest_sha256(manifest, expected_owner_uid=os.geteuid())
    with pytest.raises(ValueError, match=r"database field|endpoint"):
        build_rehearsal_secret_artifact(
            manifest,
            manifest_sha256=digest,
            namespace="loom-rehearsal-" + "a" * 24,
            database="loom_rehearsal_" + "a" * 24,
            plan_digest="b" * 64,
        )


def test_secret_artifact_preserves_non_utf8_opaque_data(tmp_path: Path) -> None:
    opaque = b"\x00\xff\x10opaque-secret-data\x80"
    manifest = _checkpoint(tmp_path, opaque_binary_data=opaque)
    digest = backup_manifest_sha256(manifest, expected_owner_uid=os.geteuid())

    artifact = build_rehearsal_secret_artifact(
        manifest,
        manifest_sha256=digest,
        namespace="loom-rehearsal-" + "a" * 24,
        database="loom_rehearsal_" + "a" * 24,
        plan_digest="b" * 64,
    )

    tls = next(
        document
        for document in yaml.safe_load_all(artifact.payload)
        if document["metadata"]["name"] == "loom-staging-tls"
    )
    assert base64.b64decode(tls["data"]["opaque-binary"]) == opaque
