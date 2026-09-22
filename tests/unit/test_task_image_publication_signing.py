from __future__ import annotations

import base64
import hashlib
import importlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
import rfc8785
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from tests.unit.test_task_image_publication_contracts import contracts, unsigned_payload

NOW = datetime(2026, 9, 5, 12, tzinfo=UTC)
DOMAIN = b"loom-task-image-publication-v1\x00"


def setup_signing():
    c = contracts()
    name = "loom_task_image_authority.publication_signing"
    assert importlib.util.find_spec(name) is not None, "publication signer boundary is missing"
    s = importlib.import_module(name)
    private = Ed25519PrivateKey.generate()
    key = s.PublicationKeyRecord(
        key_id="publication-1",
        public_key=private.public_key().public_bytes_raw(),
        activated_at=NOW - timedelta(minutes=1),
    )
    state = s.PublicationState(revocation_epoch=2, keyset_version=3)
    distribution = s.DistributedKeysetSnapshot(
        keyset_version=3,
        revocation_epoch=2,
        key_ids=(key.key_id,),
        issued_at=NOW - timedelta(seconds=10),
        expires_at=NOW + timedelta(minutes=1),
    )
    unsigned = c.decode_unsigned_input(rfc8785.dumps(unsigned_payload()))
    statement = c.PublicationStatement.model_validate({
        **unsigned.model_dump(mode="json", by_alias=True, exclude_none=True),
        "issued_at": NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "signing_key_id": key.key_id,
        "distributed_keyset_version": state.keyset_version,
        "revocation_epoch": state.revocation_epoch,
    })
    canonical = c.canonical_publication_bytes(statement)
    signature = private.sign(DOMAIN + canonical)
    reply = {
        "canonical_statement": canonical.decode(),
        "statement_sha256": hashlib.sha256(canonical).hexdigest(),
        "key_id": key.key_id,
        "algorithm": "Ed25519",
        "signature": base64.urlsafe_b64encode(signature).rstrip(b"=").decode(),
    }
    return c, s, private, key, state, distribution, unsigned, reply


def test_historical_signature_preserves_time_and_exact_domain():
    _c, s, private, key, _state, _distribution, _unsigned, reply = setup_signing()
    result = s.verify_historical_publication(rfc8785.dumps(reply), key=key)
    assert result.statement.issued_at == "2026-09-05T12:00:00Z"
    signature = base64.urlsafe_b64decode(reply["signature"] + "==")
    private.public_key().verify(signature, DOMAIN + reply["canonical_statement"].encode())
    with pytest.raises(InvalidSignature):
        private.public_key().verify(signature, reply["canonical_statement"].encode())


@pytest.mark.parametrize(
    "field",
    [
        *list(unsigned_payload()),
        "issued_at",
        "signing_key_id",
        "distributed_keyset_version",
        "revocation_epoch",
    ],
)
def test_every_statement_field_is_cryptographically_bound(field):
    _c, s, private, key, _state, _distribution, _unsigned, reply = setup_signing()
    payload = json.loads(reply["canonical_statement"])
    payload[field] = "substitution"
    changed = rfc8785.dumps(payload)
    signature = base64.urlsafe_b64decode(reply["signature"] + "==")
    with pytest.raises(InvalidSignature):
        private.public_key().verify(signature, DOMAIN + changed)
    reply.update(
        canonical_statement=changed.decode(), statement_sha256=hashlib.sha256(changed).hexdigest()
    )
    with pytest.raises(ValueError):
        s.verify_historical_publication(rfc8785.dumps(reply), key=key)


@pytest.mark.parametrize(
    "mutation",
    [
        "algorithm",
        "signature",
        "digest",
        "key",
        "canonical",
        "duplicate",
        "oversize",
        "domain",
        "unknown_public_key",
    ],
)
def test_historical_signature_rejects_substitution_and_unknown_keys(mutation):
    _c, s, private, key, _state, _distribution, _unsigned, reply = setup_signing()
    raw = None
    if mutation == "algorithm":
        reply["algorithm"] = "RS256"
    if mutation == "signature":
        reply["signature"] = "A" * 86
    if mutation == "digest":
        reply["statement_sha256"] = "f" * 64
    if mutation == "key":
        reply["key_id"] = "unknown"
    if mutation == "canonical":
        reply["canonical_statement"] += " "
    if mutation == "duplicate":
        raw = rfc8785.dumps(reply).replace(
            b'"algorithm":"Ed25519"', b'"algorithm":"Ed25519","algorithm":"Ed25519"'
        )
    if mutation == "oversize":
        raw = b"x" * (256 * 1024)
    if mutation == "domain":
        canonical = reply["canonical_statement"].encode()
        reply["signature"] = base64.urlsafe_b64encode(
            private.sign(b"different\x00" + canonical)
        ).rstrip(b"=").decode()
    if mutation == "unknown_public_key":
        key = replace(key, public_key=Ed25519PrivateKey.generate().public_key().public_bytes_raw())
    with pytest.raises(ValueError):
        s.verify_historical_publication(raw or rfc8785.dumps(reply), key=key)


@pytest.mark.parametrize("status", ["verify_only", "revoked"])
def test_retired_keys_verify_historical_statements(status):
    _c, s, _private, key, _state, _distribution, _unsigned, reply = setup_signing()
    key = replace(
        key,
        status=status,
        retired_at=NOW + timedelta(seconds=1),
        revoked_at=NOW + timedelta(seconds=2) if status == "revoked" else None,
    )
    assert (
        s.verify_historical_publication(rfc8785.dumps(reply), key=key).statement.task_id
        == "task-123"
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"activated_at": NOW + timedelta(seconds=1)},
        {"status": "verify_only", "retired_at": NOW},
        {"status": "revoked", "revoked_at": NOW},
    ],
)
def test_historical_verification_enforces_actual_key_interval(changes):
    _c, s, _private, key, _state, _distribution, _unsigned, reply = setup_signing()
    with pytest.raises(ValueError):
        s.verify_historical_publication(rfc8785.dumps(reply), key=replace(key, **changes))


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "verify_only"},
        {"status": "revoked"},
        {"public_key": b"short"},
        {"retired_at": NOW},
        {"activated_at": NOW.replace(tzinfo=None)},
        {"revoked_at": NOW - timedelta(minutes=2), "status": "revoked"},
    ],
)
def test_key_record_rejects_invalid_lifecycle(changes):
    _c, _s, _private, key, _state, _distribution, _unsigned, _reply = setup_signing()
    with pytest.raises(ValueError):
        replace(key, **changes)


@pytest.mark.parametrize(
    "issued",
    [
        "2026-09-05T12:00:00+00:00",
        "2026-09-05T12:00:00.000Z",
        "2026-09-05 12:00:00Z",
        "2026-09-05T12:00:60Z",
        "2026-09-05T12:00:00z",
        "٢٠٢٦-09-05T12:00:00Z",
        "2026-09- 5T12:00:00Z",
        None,
    ],
)
def test_signed_timestamps_do_not_accept_alternate_wire_forms(issued):
    _c, s, private, key, _state, _distribution, _unsigned, reply = setup_signing()
    data = json.loads(reply["canonical_statement"])
    data["issued_at"] = issued
    canonical = rfc8785.dumps(data)
    reply.update(
        canonical_statement=canonical.decode(),
        statement_sha256=hashlib.sha256(canonical).hexdigest(),
        signature=base64.urlsafe_b64encode(private.sign(DOMAIN + canonical)).rstrip(b"=").decode(),
    )
    with pytest.raises(ValueError):
        s.verify_historical_publication(rfc8785.dumps(reply), key=key)


def test_distribution_snapshot_has_a_hard_freshness_ceiling():
    _c, _s, _private, _key, _state, distribution, _unsigned, _reply = setup_signing()
    with pytest.raises(ValueError):
        replace(distribution, expires_at=distribution.issued_at + timedelta(minutes=15, seconds=1))


def test_base64url_signature_rejects_nonzero_padding_bits_without_changing_signature_bytes():
    _c, s, _private, key, _state, _distribution, _unsigned, reply = setup_signing()
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    original = reply["signature"]
    reply["signature"] = original[:-1] + alphabet[alphabet.index(original[-1]) + 1]
    assert base64.urlsafe_b64decode(reply["signature"] + "==") == base64.urlsafe_b64decode(
        original + "=="
    )
    with pytest.raises(ValueError, match="noncanonical publication signature"):
        s.verify_historical_publication(rfc8785.dumps(reply), key=key)
