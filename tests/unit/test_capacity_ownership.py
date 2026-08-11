from __future__ import annotations

import base64
import json
from uuid import UUID

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from loom_capacity_manager.contracts import ResourceVectorV1
from loom_capacity_manager.grant_contracts import OwnershipMetadataV1
from loom_capacity_manager.ownership import (
    OwnershipKeyring,
    OwnershipKeyringError,
    public_key_fingerprint,
    sign_ownership,
)


def _metadata() -> OwnershipMetadataV1:
    return OwnershipMetadataV1(
        authority_incarnation=UUID(int=1),
        writer_epoch=1,
        configuration_epoch=1,
        allocation_epoch=1,
        tranche_id=UUID(int=2),
        intent_id=UUID(int=3),
        shape_instance_id="shape-0001",
        subject_id=UUID(int=4),
        subject_incarnation=UUID(int=5),
        account_id="owner-1",
        tier_id="development",
        candidate_generation=1,
        deployment_generation=1,
        pool_id="oldlab",
        pool_generation=1,
        shape_id="one-slot",
        profile_id="oldlab-profile",
        profile_generation=1,
        profile_digest="a" * 64,
        concurrency_slots=1,
        resources=ResourceVectorV1(slots=1, cpu_millicores=1_000),
        node_ids=("oldlab-node-1",),
        executor_id="oldlab-executor",
        executor_incarnation=UUID(int=6),
    )


def test_ed25519_ownership_verification_binds_key_and_every_metadata_field() -> None:
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    keyring = OwnershipKeyring({"oldlab-key-1": public_key})
    proof = sign_ownership(
        private_key,
        signing_key_id="oldlab-key-1",
        metadata=_metadata(),
    )
    fingerprint = public_key_fingerprint(public_key)

    assert keyring.verify(proof, expected_public_key_sha256=fingerprint) is True
    tampered = proof.model_copy(
        update={"metadata": proof.metadata.model_copy(update={"allocation_epoch": 2})}
    )
    assert keyring.verify(tampered, expected_public_key_sha256=fingerprint) is False
    assert keyring.verify(proof, expected_public_key_sha256="f" * 64) is False


def test_public_keyring_json_is_strict_and_duplicate_safe() -> None:
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    encoded = base64.b64encode(
        public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
    ).decode("ascii")
    document = {
        "schema_version": 1,
        "keys": [
            {
                "signing_key_id": "oldlab-key-1",
                "public_key_base64": encoded,
            }
        ],
    }
    keyring = OwnershipKeyring.from_json(json.dumps(document))
    proof = sign_ownership(
        private_key,
        signing_key_id="oldlab-key-1",
        metadata=_metadata(),
    )
    assert (
        keyring.verify(
            proof,
            expected_public_key_sha256=public_key_fingerprint(public_key),
        )
        is True
    )
    assert (
        keyring.matches(
            "oldlab-key-1",
            public_key_fingerprint(public_key),
        )
        is True
    )
    assert keyring.matches("oldlab-key-1", "f" * 64) is False

    document["keys"] = document["keys"] * 2
    with pytest.raises(OwnershipKeyringError, match="duplicate"):
        OwnershipKeyring.from_json(json.dumps(document))
    with pytest.raises(OwnershipKeyringError, match="fields"):
        OwnershipKeyring.from_json('{"schema_version":1,"keys":[],"extra":true}')
    with pytest.raises(OwnershipKeyringError, match="duplicate JSON fields"):
        OwnershipKeyring.from_json('{"schema_version":1,"schema_version":1,"keys":[]}')
    with pytest.raises(OwnershipKeyringError, match="fields"):
        OwnershipKeyring.from_json('{"schema_version":true,"keys":[]}')
    alias = {
        "schema_version": 1,
        "keys": [
            {
                "signing_key_id": "oldlab-key-1",
                "public_key_base64": encoded,
            },
            {
                "signing_key_id": "oldlab-key-2",
                "public_key_base64": encoded,
            },
        ],
    }
    with pytest.raises(OwnershipKeyringError, match="duplicate ownership public key"):
        OwnershipKeyring.from_json(json.dumps(alias))
