"""Retained execution authority cannot mint new image publications."""

import asyncio
import hashlib
from pathlib import Path

import pytest

from loom_task_image_authority.publication_keyset import (
    PublicationVerificationKey,
    verify_publication_keyset,
)
from loom_task_image_authority.publication_keyset_store import KeysetPreparation
from loom_task_image_authority.publication_signing import PublicationState
from loom_task_image_signer.keys import FileSigningKey
from loom_task_image_signer.policy import PublicationSelection, SignerPolicy, _request
from loom_task_image_signer.server import SignerServer, SignerServerLimits
from tests.unit.test_task_image_publication_keyset import fixture
from tests.unit.test_task_image_publication_signing import NOW


async def test_publication_route_rejected_before_body_or_policy_dispatch():
    # No listener or keys are needed to reject a retired operation. Even an old
    # peer authorization cannot make the removed route dispatchable.
    server = object.__new__(SignerServer)
    server._limits = SignerServerLimits()
    reader = asyncio.StreamReader()
    reader.feed_data(
        b"POST /v1/publications/sign HTTP/1.1\r\n"
        b"Host: signer.example\r\nContent-Type: application/json\r\n"
        b"Content-Length: 2\r\n\r\n"
    )
    with pytest.raises(ValueError, match="unsupported signer operation"):
        await server._dispatch(reader, frozenset({"publication"}))


def test_retired_peer_operation_rejected_before_loading_tls():
    with pytest.raises(ValueError, match="operation pins"):
        SignerServer(
            None, ca_file=Path("missing-ca"), certificate_file=Path("missing-cert"),
            private_key_file=Path("missing-key"),
            peer_operations={"a" * 64: frozenset({"publication"})},
        )


async def test_verification_keyset_signing_needs_only_execution_private_key(monkeypatch):
    private, root, payload, _, unsigned, _ = fixture()
    preparation = KeysetPreparation(
        environment=root.environment,
        root_sha256=hashlib.sha256(root.public_key).hexdigest(),
        execution_key_id=root.key_id,
        previous_state=PublicationState(revocation_epoch=2, keyset_version=2),
        keys=tuple(PublicationVerificationKey.model_validate(key) for key in payload["keys"]),
    )
    policy = SignerPolicy(
        None, trust_root=root, execution_provider=FileSigningKey(private),
        selections=(PublicationSelection.from_unsigned(unsigned),), clock=lambda: NOW,
    )
    reads = 0

    async def current_authority():
        nonlocal reads
        reads += 1
        return preparation

    monkeypatch.setattr(policy, "_read", current_authority)
    wire = await policy.sign_keyset(_request(preparation))
    verified = verify_publication_keyset(
        wire, trust_root=root, expected_state=preparation.proposed_state, now=NOW,
    )
    assert verified.keyset.keys == preparation.keys
    assert reads == 2  # The post-sign authority fence is still required.
