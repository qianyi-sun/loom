from __future__ import annotations

import base64
import importlib
import importlib.util
import json
import os
import re
import tomllib
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric import ed25519
from scripts.ops.bootstrap_staging_capacity_credentials import (
    BootstrapRequest,
    bootstrap,
)

from loom_capacity_manager.ownership import OwnershipKeyring, public_key_fingerprint
from loom_cli.capacity_control_plane import CapacityPoolExecutorProfile
from tests.ops.test_bootstrap_staging_capacity_credentials import _source

MODULE = "loom_cli.rollout.operator.protected_staging_capacity_execution_credentials"
_EXPECTED_METADATA = {
    "manager-abort",
    "manager-activate",
    "manager-drain",
    "manager-prepare",
    "manager-read",
    "manager-retire",
    "pool-executor-gb10",
    "pool-executor-oldlab",
    "pool-ownership-gb10",
    "pool-ownership-oldlab",
}


def _credentials(tmp_path: Path) -> Path:
    source_root, pki_root = _source(tmp_path)
    state_root = tmp_path / "state"
    state_root.mkdir(mode=0o700)
    return bootstrap(
        BootstrapRequest(
            source_client_root=source_root,
            source_client_ca_certificate=pki_root / "client-ca.pem",
            source_client_ca_private_key=pki_root / "client-ca-private-key.pem",
            source_manager_ca_certificate=pki_root / "server-ca.pem",
            state_root=state_root,
            source_uid=os.geteuid(),
            source_gid=os.getegid(),
            target_uid=os.geteuid(),
            target_gid=os.getegid(),
        ),
        require_root=False,
    )


def test_loader_returns_exact_secret_free_execution_credential_metadata(
    tmp_path: Path,
) -> None:
    assert importlib.util.find_spec(MODULE) is not None, "execution credential reader is missing"
    module = importlib.import_module(MODULE)
    loader = getattr(module, "load_execution_credential_bundle", None)
    assert loader is not None, "execution credential loader is missing"
    root = _credentials(tmp_path)

    bundle = loader(root, expected_uid=os.geteuid(), expected_gid=os.getegid())

    assert set(bundle.clients) == _EXPECTED_METADATA - {
        "pool-ownership-gb10",
        "pool-ownership-oldlab",
    }
    assert set(bundle.ownership_private_keys) == {"gb10", "oldlab"}
    assert set(bundle.metadata_sha256) == _EXPECTED_METADATA
    assert all(re.fullmatch(r"[0-9a-f]{64}", value) for value in bundle.metadata_sha256.values())
    assert len(set(bundle.metadata_sha256.values())) == len(_EXPECTED_METADATA)
    assert bundle.clients["manager-read"].spiffe_uri == (
        "spiffe://loom.openai.dev/staging/capacity/manager-read"
    )
    private_marker = bundle.clients["manager-read"].bearer_token.decode("ascii")
    assert private_marker not in repr(bundle)
    assert (
        bundle.metadata_sha256
        == loader(
            root,
            expected_uid=os.geteuid(),
            expected_gid=os.getegid(),
        ).metadata_sha256
    )


def test_loader_rejects_client_certificate_identity_swap(tmp_path: Path) -> None:
    root = _credentials(tmp_path)
    read_directory = root / "manager-read"
    prepare_directory = root / "manager-prepare"
    read_certificate = (read_directory / "certificate.pem").read_bytes()
    read_private_key = (read_directory / "private-key.pem").read_bytes()
    prepare_certificate = (prepare_directory / "certificate.pem").read_bytes()
    prepare_private_key = (prepare_directory / "private-key.pem").read_bytes()
    (read_directory / "certificate.pem").write_bytes(prepare_certificate)
    (read_directory / "private-key.pem").write_bytes(prepare_private_key)
    (prepare_directory / "certificate.pem").write_bytes(read_certificate)
    (prepare_directory / "private-key.pem").write_bytes(read_private_key)

    module = importlib.import_module(MODULE)
    with pytest.raises(ValueError, match="client identity"):
        module.load_execution_credential_bundle(
            root,
            expected_uid=os.geteuid(),
            expected_gid=os.getegid(),
        )


@pytest.mark.parametrize(
    ("source", "target", "message"),
    (
        (
            "manager-read/bearer-token",
            "manager-prepare/bearer-token",
            "bearer credential is reused",
        ),
        (
            "pool-ownership-gb10/ownership-private-key",
            "pool-ownership-oldlab/ownership-private-key",
            "ownership credential is reused",
        ),
    ),
    ids=("bearer-token", "ownership-key"),
)
def test_loader_rejects_reused_execution_secret(
    tmp_path: Path,
    source: str,
    target: str,
    message: str,
) -> None:
    root = _credentials(tmp_path)
    (root / target).write_bytes((root / source).read_bytes())

    module = importlib.import_module(MODULE)
    with pytest.raises(ValueError, match=message):
        module.load_execution_credential_bundle(
            root,
            expected_uid=os.geteuid(),
            expected_gid=os.getegid(),
        )


def test_execution_authority_documents_bind_single_purpose_principals_and_pool_keys(
    tmp_path: Path,
) -> None:
    module = importlib.import_module(MODULE)
    build_registry = getattr(module, "build_execution_principal_registry", None)
    build_keyring = getattr(module, "build_execution_ownership_keyring", None)
    assert build_registry is not None, "execution principal registry builder is missing"
    assert build_keyring is not None, "execution ownership keyring builder is missing"
    bundle = module.load_execution_credential_bundle(
        _credentials(tmp_path),
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
    )
    profile = CapacityPoolExecutorProfile.model_validate(
        tomllib.loads(
            Path("deploy/dev-fleet/capacity-pool-executor.toml.example").read_text(encoding="utf-8")
        )
    )
    pools = tuple(
        pool.model_copy(
            update={
                "signing_key_sha256": public_key_fingerprint(
                    ed25519.Ed25519PrivateKey.from_private_bytes(
                        bundle.ownership_private_keys[pool.pool_id]
                    ).public_key()
                )
            }
        )
        for pool in profile.pools
    )
    original = {
        "schema_version": 1,
        "principals": [
            {
                "principal_id": "existing-operator",
                "token_sha256": "1" * 64,
                "scopes": ["capacity:read", "capacity:reconcile"],
                "subject_id": None,
                "subject_incarnation": None,
                "demand_reporter_incarnation": None,
                "pool_id": None,
                "pool_reporter_incarnation": None,
            }
        ],
    }

    registry_payload = build_registry(
        (json.dumps(original) + "\n").encode("ascii"),
        bundle=bundle,
        pools=pools,
    )
    keyring_payload = build_keyring(
        b'{"schema_version":1,"keys":[]}\n',
        bundle=bundle,
        pools=pools,
    )

    principals = {
        principal["principal_id"]: principal
        for principal in json.loads(registry_payload)["principals"]
    }
    assert principals["manager-read"]["scopes"] == ["capacity:read"]
    for action in ("prepare", "activate", "drain", "retire", "abort"):
        assert principals[f"manager-{action}"]["scopes"] == [f"capacity:execution:{action}"]
    for pool in pools:
        principal = principals[f"pool-executor-{pool.pool_id}"]
        assert principal["scopes"] == ["capacity:execute:pool"]
        assert (
            principal["pool_id"],
            principal["executor_id"],
            principal["executor_incarnation"],
            principal["executor_pool_generation"],
        ) == (
            pool.pool_id,
            pool.executor_id,
            pool.executor_incarnation,
            pool.pool_generation,
        )
    keyring = OwnershipKeyring.from_json(keyring_payload.decode("ascii"))
    for pool in pools:
        assert keyring.matches(pool.signing_key_id, pool.signing_key_sha256)
    for credential in bundle.clients.values():
        assert credential.bearer_token not in registry_payload
        assert credential.private_key not in registry_payload
        assert credential.bearer_token not in keyring_payload
        assert credential.private_key not in keyring_payload
    for private_key in bundle.ownership_private_keys.values():
        assert private_key not in registry_payload
        assert private_key not in keyring_payload


def test_ownership_keyring_rejects_profile_fingerprint_mismatch(tmp_path: Path) -> None:
    module = importlib.import_module(MODULE)
    bundle = module.load_execution_credential_bundle(
        _credentials(tmp_path),
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
    )
    profile = CapacityPoolExecutorProfile.model_validate(
        tomllib.loads(
            Path("deploy/dev-fleet/capacity-pool-executor.toml.example").read_text(encoding="utf-8")
        )
    )
    pools = tuple(
        pool.model_copy(
            update={
                "signing_key_sha256": (
                    "0" * 64
                    if pool.pool_id == "gb10"
                    else public_key_fingerprint(
                        ed25519.Ed25519PrivateKey.from_private_bytes(
                            bundle.ownership_private_keys[pool.pool_id]
                        ).public_key()
                    )
                )
            }
        )
        for pool in profile.pools
    )

    with pytest.raises(ValueError, match="ownership key differs"):
        module.build_execution_ownership_keyring(
            b'{"schema_version":1,"keys":[]}\n',
            bundle=bundle,
            pools=pools,
        )


def test_backup_secret_documents_are_immutable_and_pool_local(tmp_path: Path) -> None:
    module = importlib.import_module(MODULE)
    builder = getattr(module, "build_execution_backup_secret_documents", None)
    assert builder is not None, "execution backup Secret builder is missing"
    bundle = module.load_execution_credential_bundle(
        _credentials(tmp_path),
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
    )

    documents = {name: json.loads(payload) for name, payload in builder(bundle).items()}

    assert set(documents) == {
        "loom-capacity-execution-operator",
        "loom-capacity-executor-gb10",
        "loom-capacity-executor-oldlab",
    }
    for name, document in documents.items():
        assert document["immutable"] is True
        assert document["metadata"]["name"] == name
        assert document["metadata"]["namespace"] == "loom-dev"
    operator_data = documents["loom-capacity-execution-operator"]["data"]
    assert set(operator_data) == {
        f"{principal}.{filename}"
        for principal in (
            "manager-read",
            "manager-prepare",
            "manager-activate",
            "manager-drain",
            "manager-retire",
            "manager-abort",
        )
        for filename in (
            "bearer-token",
            "certificate.pem",
            "manager-ca.pem",
            "private-key.pem",
        )
    }
    for pool in ("gb10", "oldlab"):
        data = documents[f"loom-capacity-executor-{pool}"]["data"]
        assert set(data) == {
            "bearer-token",
            "client-certificate.pem",
            "client-private-key.pem",
            "manager-ca.pem",
            "ownership-private-key",
        }
        credential = bundle.clients[f"pool-executor-{pool}"]
        assert base64.b64decode(data["bearer-token"], validate=True) == credential.bearer_token
        assert (
            base64.b64decode(data["client-certificate.pem"], validate=True)
            == credential.certificate
        )
        assert (
            base64.b64decode(data["client-private-key.pem"], validate=True)
            == credential.private_key
        )
        assert (
            base64.b64decode(data["ownership-private-key"], validate=True)
            == bundle.ownership_private_keys[pool]
        )
        other = "oldlab" if pool == "gb10" else "gb10"
        assert (
            base64.b64encode(bundle.ownership_private_keys[other]).decode("ascii")
            not in data.values()
        )
