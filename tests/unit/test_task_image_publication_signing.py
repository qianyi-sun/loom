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
    statement = s.prepare_publication_statement(
        unsigned,
        key=key,
        state=state,
        distribution=distribution,
        signer_now=NOW,
    )
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


def test_signer_stamps_own_clock_and_independent_ed25519_verifier_accepts_exact_domain():
    _c, s, private, key, state, distribution, unsigned, reply = setup_signing()
    result = s.verify_publication_reply(
        rfc8785.dumps(reply),
        unsigned=unsigned,
        key=key,
        state=state,
        distribution=distribution,
        requested_at=NOW,
        received_at=NOW,
    )
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
    _c, s, private, key, state, distribution, unsigned, reply = setup_signing()
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
        s.verify_publication_reply(
            rfc8785.dumps(reply),
            unsigned=unsigned,
            key=key,
            state=state,
            distribution=distribution,
            requested_at=NOW,
            received_at=NOW,
        )


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
        "backdate",
        "future",
        "input",
        "retired",
        "revoked",
        "unknown_public_key",
        "undistributed",
        "expired_distribution",
        "stale_epoch",
        "stale_version",
        "missing_membership",
        "new_epoch",
        "new_version",
    ],
)
def test_reply_rejects_substitution_clock_key_and_distribution_failures(mutation):
    _c, s, private, key, state, distribution, unsigned, reply = setup_signing()
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
    if mutation in {"domain", "backdate", "future", "input"}:
        data = json.loads(reply["canonical_statement"])
        if mutation == "backdate":
            data["issued_at"] = "2026-09-05T11:59:50Z"
        if mutation == "future":
            data["issued_at"] = "2026-09-05T12:00:10Z"
        if mutation == "input":
            data["task_id"] = "different-task"
        canonical = rfc8785.dumps(data)
        reply.update(
            canonical_statement=canonical.decode(),
            statement_sha256=hashlib.sha256(canonical).hexdigest(),
            signature=base64.urlsafe_b64encode(
                private.sign((b"different\x00" if mutation == "domain" else DOMAIN) + canonical)
            )
            .rstrip(b"=")
            .decode(),
        )
    if mutation == "retired":
        key = replace(key, status="verify_only", retired_at=NOW + timedelta(seconds=1))
    if mutation == "revoked":
        key = replace(key, status="revoked", revoked_at=NOW + timedelta(seconds=1))
    if mutation == "unknown_public_key":
        key = replace(key, public_key=Ed25519PrivateKey.generate().public_key().public_bytes_raw())
    if mutation == "undistributed":
        distribution = None
    if mutation == "expired_distribution":
        distribution = replace(distribution, expires_at=NOW)
    if mutation == "stale_epoch":
        distribution = replace(distribution, revocation_epoch=1)
    if mutation == "stale_version":
        distribution = replace(distribution, keyset_version=2)
    if mutation == "missing_membership":
        distribution = replace(distribution, key_ids=())
    if mutation == "new_epoch":
        state = replace(state, revocation_epoch=3)
    if mutation == "new_version":
        state = replace(state, keyset_version=4)
    with pytest.raises(ValueError):
        s.verify_publication_reply(
            raw or rfc8785.dumps(reply),
            unsigned=unsigned,
            key=key,
            state=state,
            distribution=distribution,
            requested_at=NOW,
            received_at=NOW,
        )


@pytest.mark.parametrize("status", ["verify_only", "revoked"])
def test_retired_keys_verify_historical_statements_but_never_sign_new_ones(status):
    _c, s, _private, key, state, distribution, unsigned, reply = setup_signing()
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
    with pytest.raises(ValueError):
        s.prepare_publication_statement(
            unsigned, key=key, state=state, distribution=distribution, signer_now=NOW
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


def test_active_key_does_not_open_uncomposed_distribution_gate():
    _c, s, _private, key, state, _distribution, unsigned, _reply = setup_signing()
    with pytest.raises(ValueError):
        s.prepare_publication_statement(unsigned, key=key, state=state, signer_now=NOW)


@pytest.mark.asyncio
async def test_dedicated_protocol_only_accepts_unsigned_schema_and_returns_checked_envelope():
    _c, s, _private, key, state, distribution, unsigned, reply = setup_signing()

    class TestTransport:
        async def sign_publication(
            self, canonical_unsigned_input: bytes, *, maximum_reply_bytes: int
        ) -> bytes:
            assert json.loads(canonical_unsigned_input) == unsigned_payload()
            assert "issued_at" not in json.loads(canonical_unsigned_input)
            assert maximum_reply_bytes <= 256 * 1024
            return rfc8785.dumps(reply)

    result = await s.request_publication_signature(
        TestTransport(),
        unsigned,
        key=key,
        state=state,
        distribution=distribution,
        clock=lambda: NOW,
    )
    assert result.statement.task_id == "task-123"


@pytest.mark.asyncio
async def test_signer_call_has_bounded_deadline():
    import asyncio

    _c, s, _private, key, state, distribution, unsigned, _reply = setup_signing()

    class StalledTransport:
        async def sign_publication(
            self, canonical_unsigned_input: bytes, *, maximum_reply_bytes: int
        ) -> bytes:
            await asyncio.Event().wait()

    with pytest.raises(TimeoutError):
        await s.request_publication_signature(
            StalledTransport(),
            unsigned,
            key=key,
            state=state,
            distribution=distribution,
            clock=lambda: NOW,
            timeout_seconds=0.01,
        )


@pytest.mark.parametrize("field", [name for name in unsigned_payload() if name != "schema"])
def test_validly_resigned_substitution_of_every_unsigned_field_is_rejected(field):
    _c, s, private, key, state, distribution, unsigned, reply = setup_signing()
    data = json.loads(reply["canonical_statement"])
    old = data[field]
    if field in {"root", "manifest"}:
        data["root"]["digest"] = "sha256:" + "b" * 64
        data["manifest"]["digest"] = "sha256:" + "b" * 64
    elif field == "config":
        data["config"]["digest"] = "sha256:" + "b" * 64
    elif field == "layers":
        data["layers"] *= 2
    elif field == "observed_base_digests":
        data[field] = ["sha256:" + "c" * 64]
    elif field in {"platform", "slurm_cluster_id", "repository"}:
        data.update(platform="linux/amd64", slurm_cluster_id="oldlab")
        data["repository"] = data["repository"].replace("/arm64/", "/x86_64/")
    elif field == "component":
        data[field] = "sidecar:redis"
        segment = hashlib.sha256(b"sidecar:redis").hexdigest()
        data["repository"] = data["repository"].removesuffix("task") + "sidecar-sha256-" + segment
    elif field == "purpose":
        campaign = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
        data.update(purpose="shadow", shadow_campaign_id=campaign)
        data["repository"] = data["repository"].replace(
            "loom-task-image-attempts/", f"loom-task-image-shadow/{campaign}/"
        )
    elif type(old) is int:
        data[field] += 1
    elif field.endswith("_id") and len(old) == 36:
        data[field] = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
        if field == "attempt_id":
            data["repository"] = data["repository"].replace(old, data[field])
    elif field == "registry_origin":
        data[field] = "https://other-registry.example"
    elif len(old) == 64:
        data[field] = "c" * 64
    elif field == "slurm_job_id":
        data[field] = "4321"
    else:
        data[field] = "different-identity"
    canonical = rfc8785.dumps(data)
    reply.update(
        canonical_statement=canonical.decode(),
        statement_sha256=hashlib.sha256(canonical).hexdigest(),
        signature=base64.urlsafe_b64encode(private.sign(DOMAIN + canonical)).rstrip(b"=").decode(),
    )
    # This is a VALID schema and signature, not merely a malformed-input test.
    s.verify_historical_publication(rfc8785.dumps(reply), key=key)
    with pytest.raises(ValueError, match="binding mismatch"):
        s.verify_publication_reply(
            rfc8785.dumps(reply),
            unsigned=unsigned,
            key=key,
            state=state,
            distribution=distribution,
            requested_at=NOW,
            received_at=NOW,
        )


@pytest.mark.parametrize(
    "issued",
    [
        "2026-09-05T12:00:00+00:00",
        "2026-09-05T12:00:00.000Z",
        "2026-09-05 12:00:00Z",
        "2026-09-05T12:00:60Z",
        "2026-09-05T12:00:00z",
        "٢٠٢٦-09-05T12:00:00Z",
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


@pytest.mark.parametrize("skew,accepted", [(-6, False), (-5, True), (5, True), (6, False)])
def test_signer_clock_skew_is_bounded_at_exactly_five_seconds(skew, accepted):
    _c, s, private, key, state, distribution, unsigned, reply = setup_signing()
    data = json.loads(reply["canonical_statement"])
    data["issued_at"] = (NOW + timedelta(seconds=skew)).strftime("%Y-%m-%dT%H:%M:%SZ")
    canonical = rfc8785.dumps(data)
    reply.update(
        canonical_statement=canonical.decode(),
        statement_sha256=hashlib.sha256(canonical).hexdigest(),
        signature=base64.urlsafe_b64encode(private.sign(DOMAIN + canonical)).rstrip(b"=").decode(),
    )
    if accepted:
        assert (
            s.verify_publication_reply(
                rfc8785.dumps(reply),
                unsigned=unsigned,
                key=key,
                state=state,
                distribution=distribution,
                requested_at=NOW,
                received_at=NOW,
            ).statement.issued_at
            == data["issued_at"]
        )
    else:
        with pytest.raises(ValueError):
            s.verify_publication_reply(
                rfc8785.dumps(reply),
                unsigned=unsigned,
                key=key,
                state=state,
                distribution=distribution,
                requested_at=NOW,
                received_at=NOW,
            )


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
