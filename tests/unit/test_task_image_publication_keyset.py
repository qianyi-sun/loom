"""Independent execution/publication keys exercise the worker trust chain."""

import base64
import copy
import hashlib
import importlib
import json
from dataclasses import replace
from datetime import timedelta
from uuid import UUID

import pytest
import rfc8785
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from loom_task_image_authority.registry_token import publication_repository
from tests.unit.test_task_image_publication_signing import NOW, setup_signing

DOMAIN = b"loom-task-image-publication-keyset-v1\x00"


def module():
    name = "loom_task_image_authority.publication_keyset"
    assert importlib.util.find_spec(name) is not None, "signed keyset verifier is missing"
    return importlib.import_module(name)


def _time(value):
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _b64(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _sign(payload, execution_key, *, domain=DOMAIN):
    canonical = rfc8785.dumps(payload)
    return rfc8785.dumps(dict(
        key_id="execution-1", algorithm="Ed25519", canonical_keyset=canonical.decode(),
        keyset_sha256=hashlib.sha256(canonical).hexdigest(),
        signature=_b64(execution_key.sign(domain + canonical)),
    ))


def fixture():
    _c, _s, _private, key, state, _distribution, unsigned, reply = setup_signing()
    execution_key = Ed25519PrivateKey.generate()
    root = module().ExecutionGrantTrustRoot(
        key_id="execution-1", environment="production",
        public_key=execution_key.public_key().public_bytes_raw(),
        activated_at=NOW - timedelta(days=1), expires_at=NOW + timedelta(days=1),
    )
    payload = dict(
        schema="loom.task-image-publication-keyset/v1", environment="production",
        keyset_version=3, revocation_epoch=2,
        issued_at=_time(NOW - timedelta(seconds=10)),
        expires_at=_time(NOW + timedelta(minutes=5)),
        keys=[dict(key_id=key.key_id, public_key=_b64(key.public_key), status="active", activated_at=_time(key.activated_at))],
    )
    return execution_key, root, payload, state, unsigned, rfc8785.dumps(reply)


def test_exact_signed_keyset_and_publication_chain_verifies_without_distribution_claim():
    m = module()
    private, root, payload, state, unsigned, publication = fixture()
    wire = _sign(payload, private)
    verified = m.verify_publication_keyset(wire, trust_root=root, expected_state=state, now=NOW)
    assert verified.keyset.keyset_version == state.keyset_version
    assert verified.snapshot_sha256 == hashlib.sha256(wire).hexdigest()
    assert m.canonical_keyset_bytes(verified.envelope) == wire
    result = m.verify_keyset_publication(
        publication, keyset_wire=wire, trust_root=root, expected_state=state,
        expected_snapshot_sha256=verified.snapshot_sha256, expected_unsigned=unsigned, now=NOW,
    )
    assert result.statement.unsigned_input() == unsigned
    assert not hasattr(verified, "distribution"), "a signature must not synthesize distribution evidence"


@pytest.mark.parametrize("field", ["schema", "environment", "keyset_version", "revocation_epoch", "issued_at", "expires_at", "keys"])
def test_every_keyset_field_is_bound_by_execution_key(field):
    m = module()
    private, root, payload, state, *_ = fixture()
    envelope = json.loads(_sign(payload, private))
    payload[field] = "substitution"
    canonical = rfc8785.dumps(payload)
    with pytest.raises(InvalidSignature):
        private.public_key().verify(base64.urlsafe_b64decode(envelope["signature"] + "=="), DOMAIN + canonical)
    envelope.update(canonical_keyset=canonical.decode(), keyset_sha256=hashlib.sha256(canonical).hexdigest())
    with pytest.raises(ValueError):
        m.verify_publication_keyset(rfc8785.dumps(envelope), trust_root=root, expected_state=state, now=NOW)


@pytest.mark.parametrize("change", [
    "wrong-domain", "wrong-root", "wrong-environment", "unknown-root-id", "digest",
    "noncanonical", "duplicate-json", "expired", "future", "overlong", "null",
    "duplicate-key-id", "duplicate-key-bytes", "bad-public-key", "same-signing-key",
    "invalid-lifecycle", "empty", "too-many", "stale-version", "stale-epoch",
    "new-version", "new-epoch", "boolean-version", "boolean-epoch", "root-expired",
    "root-not-active", "outlives-root", "unknown-field", "oversize",
])
def test_keyset_rejects_untrusted_stale_ambiguous_and_invalid_authority(change):
    m = module()
    private, root, payload, state, *_ = fixture()
    wire = None
    if change == "wrong-domain":
        wire = _sign(payload, private, domain=b"loom-task-image-publication-v1\x00")
    elif change == "wrong-root":
        root = replace(root, public_key=Ed25519PrivateKey.generate().public_key().public_bytes_raw())
    elif change == "unknown-root-id":
        root = replace(root, key_id="another-execution-key")
    elif change == "wrong-environment":
        payload["environment"] = "staging"
    elif change == "expired":
        payload["expires_at"] = _time(NOW)
    elif change == "future":
        payload["issued_at"] = _time(NOW + timedelta(seconds=1))
    elif change == "overlong":
        payload["expires_at"] = _time(NOW + timedelta(minutes=15))
    elif change == "null":
        payload["keys"][0]["retired_at"] = None
    elif change in {"duplicate-key-id", "duplicate-key-bytes"}:
        payload["keys"].append(copy.deepcopy(payload["keys"][0]))
        if change == "duplicate-key-bytes":
            payload["keys"][1]["key_id"] = "publication-2"
    elif change == "bad-public-key":
        payload["keys"][0]["public_key"] = "!" * 43
    elif change == "same-signing-key":
        payload["keys"][0]["public_key"] = _b64(root.public_key)
    elif change == "invalid-lifecycle":
        payload["keys"][0]["status"] = "verify_only"
    elif change == "empty":
        payload["keys"] = []
    elif change == "too-many":
        payload["keys"] *= 129
    elif change in {"stale-version", "new-version", "boolean-version"}:
        payload["keyset_version"] = {"stale-version": 2, "new-version": 4, "boolean-version": True}[change]
    elif change in {"stale-epoch", "new-epoch", "boolean-epoch"}:
        payload["revocation_epoch"] = {"stale-epoch": 1, "new-epoch": 3, "boolean-epoch": True}[change]
    elif change == "root-expired":
        root = replace(root, expires_at=NOW)
    elif change == "root-not-active":
        root = replace(root, activated_at=NOW + timedelta(seconds=1))
    elif change == "outlives-root":
        root = replace(root, expires_at=NOW + timedelta(seconds=1))
    elif change == "unknown-field":
        payload["authority"] = True
    if wire is None:
        wire = _sign(payload, private)
    if change == "noncanonical":
        wire += b" "
    elif change == "duplicate-json":
        wire = wire.replace(b'"algorithm":"Ed25519"', b'"algorithm":"Ed25519","algorithm":"Ed25519"')
    elif change == "digest":
        wire = wire.replace(hashlib.sha256(rfc8785.dumps(payload)).hexdigest().encode(), b"0" * 64)
    elif change == "oversize":
        wire = b"x" * (128 * 1024 + 1)
    with pytest.raises(ValueError):
        m.verify_publication_keyset(wire, trust_root=root, expected_state=state, now=NOW)


@pytest.mark.parametrize("change", ["snapshot-digest", "unknown-publication-key", "revoked", "different-image", "future-version", "future-epoch", "rotation"])
def test_worker_binds_exact_publication_and_allows_only_valid_routine_rotation(change):
    m = module()
    private, root, payload, state, unsigned, publication = fixture()
    expected = unsigned
    if change == "unknown-publication-key":
        payload["keys"][0]["key_id"] = "different-publication-key"
    elif change == "revoked":
        payload["keys"][0].update(status="revoked", revoked_at=_time(NOW))
    elif change == "different-image":
        expected = unsigned.model_copy(update={"task_id": "different-task"})
    elif change == "future-version":
        payload["keyset_version"] = 2
        state = replace(state, keyset_version=2)
    elif change == "future-epoch":
        payload["revocation_epoch"] = 1
        state = replace(state, revocation_epoch=1)
    elif change == "rotation":
        payload["keyset_version"] = 4
        payload["keys"][0].update(status="verify_only", retired_at=_time(NOW + timedelta(seconds=1)))
        state = replace(state, keyset_version=4)
    wire = _sign(payload, private)
    digest = "0" * 64 if change == "snapshot-digest" else hashlib.sha256(wire).hexdigest()
    kwargs = dict(keyset_wire=wire, trust_root=root, expected_state=state, expected_snapshot_sha256=digest, expected_unsigned=expected, now=NOW + timedelta(seconds=2) if change == "rotation" else NOW)
    if change == "rotation":
        assert m.verify_keyset_publication(publication, **kwargs).statement.unsigned_input() == unsigned
    else:
        with pytest.raises(ValueError):
            m.verify_keyset_publication(publication, **kwargs)


@pytest.mark.parametrize("change", ["signature-bits", "public-key-bits", "inner-duplicate", "inner-noncanonical", "unsorted", "unknown-key-field"])
def test_nested_wire_and_base64_are_canonical_not_merely_decodable(change):
    m = module()
    private, root, payload, state, *_ = fixture()
    if change == "public-key-bits":
        value = payload["keys"][0]["public_key"]
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        payload["keys"][0]["public_key"] = value[:-1] + alphabet[alphabet.index(value[-1]) + 1]
    elif change == "unsorted":
        second = copy.deepcopy(payload["keys"][0])
        second.update(key_id="aaa", public_key=_b64(Ed25519PrivateKey.generate().public_key().public_bytes_raw()))
        payload["keys"].append(second)
    elif change == "unknown-key-field":
        payload["keys"][0]["secret"] = "not-a-keyset-field"
    envelope = json.loads(_sign(payload, private))
    if change == "signature-bits":
        value = envelope["signature"]
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        envelope["signature"] = value[:-1] + alphabet[alphabet.index(value[-1]) + 1]
    elif change in {"inner-duplicate", "inner-noncanonical"}:
        raw = envelope["canonical_keyset"].encode()
        raw = raw.replace(b'"keyset_version":3', b'"keyset_version":3,"keyset_version":3') if change == "inner-duplicate" else raw + b" "
        envelope.update(canonical_keyset=raw.decode(), keyset_sha256=hashlib.sha256(raw).hexdigest(), signature=_b64(private.sign(DOMAIN + raw)))
    with pytest.raises(ValueError):
        m.verify_publication_keyset(rfc8785.dumps(envelope), trust_root=root, expected_state=state, now=NOW)


@pytest.mark.parametrize("field", ["task_id", "task_checksum", "component", "platform", "environment", "purpose", "materialization_id", "materialization_key", "attempt_id", "lease_epoch", "repository", "frozen_plan_sha256"])
def test_every_expected_execution_binding_is_checked(field):
    m = module()
    private, root, payload, state, unsigned, publication = fixture()
    wire = _sign(payload, private)
    payload = unsigned.model_dump(mode="json", by_alias=True, exclude_none=True)
    if field in {"component", "repository"}:
        payload["component"] = "sidecar:db"
    elif field == "platform":
        payload.update(platform="linux/amd64", slurm_cluster_id="oldlab")
    elif field == "purpose":
        payload.update(purpose="shadow", shadow_campaign_id="11111111-1111-4111-8111-111111111111")
    else:
        payload[field] = {
            "task_id": "different-task", "task_checksum": "c" * 64,
            "environment": "staging", "materialization_key": "c" * 64,
            "materialization_id": "11111111-1111-4111-8111-111111111111",
            "attempt_id": "11111111-1111-4111-8111-111111111111", "lease_epoch": 3,
            "frozen_plan_sha256": "c" * 64,
        }[field]
    payload["repository"] = publication_repository(purpose="production", shadow_campaign_id=None, cpu_arch="arm64" if payload["platform"] == "linux/arm64" else "x86_64", attempt_id=UUID(payload["attempt_id"]), component=payload["component"])
    if payload["purpose"] == "shadow":
        payload["repository"] = payload["repository"].replace("loom-task-image-attempts/", "loom-task-image-shadow/" + payload["shadow_campaign_id"] + "/")
    # These alternatives are independently well-formed. Failure must come from
    # exact execution binding, not an unrelated malformed-model check.
    changed = type(unsigned).model_validate(payload)
    with pytest.raises(ValueError):
        m.verify_keyset_publication(publication, keyset_wire=wire, trust_root=root, expected_state=state, expected_snapshot_sha256=hashlib.sha256(wire).hexdigest(), expected_unsigned=changed, now=NOW)


def test_new_epoch_from_unrelated_revocation_preserves_nonrevoked_publication():
    m = module()
    private, root, payload, state, unsigned, publication = fixture()
    payload.update(keyset_version=4, revocation_epoch=3)
    state = replace(state, keyset_version=4, revocation_epoch=3)
    wire = _sign(payload, private)
    result = m.verify_keyset_publication(publication, keyset_wire=wire, trust_root=root, expected_state=state, expected_snapshot_sha256=hashlib.sha256(wire).hexdigest(), expected_unsigned=unsigned, now=NOW)
    assert result.statement.revocation_epoch == 2


def test_signature_does_not_refresh_original_expiry():
    m = module()
    private, root, payload, state, *_ = fixture()
    wire = _sign(payload, private)
    initial = m.verify_publication_keyset(wire, trust_root=root, expected_state=state, now=NOW)
    later = m.verify_publication_keyset(wire, trust_root=root, expected_state=state, now=NOW + timedelta(minutes=4))
    assert initial == later
    with pytest.raises(ValueError):
        m.verify_publication_keyset(wire, trust_root=root, expected_state=state, now=NOW + timedelta(minutes=5))


@pytest.mark.parametrize("change", ["retirement-boundary", "key-public-bytes"])
def test_valid_keyset_cannot_hide_publication_interval_or_signature_mismatch(change):
    m = module()
    private, root, payload, state, unsigned, publication = fixture()
    if change == "retirement-boundary":
        payload["keys"][0].update(status="verify_only", retired_at=_time(NOW))
    else:
        payload["keys"][0]["public_key"] = _b64(Ed25519PrivateKey.generate().public_key().public_bytes_raw())
    wire = _sign(payload, private)
    # The keyset itself is authentic and valid; its key cannot verify this image.
    m.verify_publication_keyset(wire, trust_root=root, expected_state=state, now=NOW + timedelta(seconds=2))
    with pytest.raises(ValueError):
        m.verify_keyset_publication(publication, keyset_wire=wire, trust_root=root, expected_state=state, expected_snapshot_sha256=hashlib.sha256(wire).hexdigest(), expected_unsigned=unsigned, now=NOW + timedelta(seconds=2))
