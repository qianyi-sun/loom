from __future__ import annotations

import hashlib
import importlib
import json
from uuid import UUID

import pytest
import rfc8785


def receipts():
    name = "loom_task_image_authority.publication_receipts"
    assert importlib.util.find_spec(name) is not None, "publication receipt contract missing"
    return importlib.import_module(name)


def receipt_payload():
    return dict(
        schema="loom.task-image-publication-receipt/v1",
        operation_id=str(UUID(int=1)),
        materialization_id=str(UUID(int=2)),
        attempt_id=str(UUID(int=3)),
        lease_epoch=2,
        worker_generation=1,
        snapshot_sha256="1" * 64,
        candidate_set_sha256="2" * 64,
        publication_set_sha256="3" * 64,
        component_count=128,
        completed_at="2026-09-08T12:00:00Z",
    )


def test_receipt_is_constant_size_canonical_and_owned():
    r = receipts()
    payload = receipt_payload()
    wire = rfc8785.dumps(payload)
    receipt = r.decode_publication_receipt(wire)
    assert r.canonical_receipt_bytes(receipt) == wire
    assert len(wire) < r.MAX_RECEIPT_BYTES <= 2048
    payload["component_count"] = 1
    assert receipt.component_count == 128
    with pytest.raises(ValueError):
        receipt.component_count = 1
    forged = receipt.model_copy(update={"component_count": 129})
    with pytest.raises(ValueError):
        r.canonical_receipt_bytes(forged)


@pytest.mark.parametrize(
    "mutation",
    [
        "unknown",
        "null",
        "bool",
        "zero",
        "count",
        "digest",
        "uuid",
        "time",
        "duplicate",
        "spacing",
        "oversize",
        "nonfinite",
    ],
)
def test_receipt_rejects_invalid_or_noncanonical_wire(mutation):
    r = receipts()
    payload = receipt_payload()
    changes = {
        "unknown": {"credential": "must-not-leak"},
        "null": {"completed_at": None},
        "bool": {"lease_epoch": True},
        "zero": {"worker_generation": 0},
        "count": {"component_count": 129},
        "digest": {"snapshot_sha256": "SHA256:" + "1" * 64},
        "uuid": {"operation_id": str(UUID(int=0))},
        "time": {"completed_at": "2026-09-08T12:00:00.000Z"},
    }
    payload.update(changes.get(mutation, {}))
    wire = rfc8785.dumps(payload)
    if mutation == "duplicate":
        wire = b'{"lease_epoch":2,' + wire[1:]
    elif mutation == "spacing":
        wire = json.dumps(payload).encode()
    elif mutation == "oversize":
        wire += b" " * r.MAX_RECEIPT_BYTES
    elif mutation == "nonfinite":
        wire = wire.replace(b'"lease_epoch":2', b'"lease_epoch":NaN')
    with pytest.raises(ValueError):
        r.decode_publication_receipt(wire)


def test_complete_set_hashes_match_independent_canonical_inputs():
    r = receipts()
    identities = tuple(
        r.PublicationCandidateIdentity(
            candidate_id=str(UUID(int=index + 1)),
            component="task" if index == 0 else f"sidecar:s{index:03d}",
        )
        for index in range(128)
    )
    expected_candidates = {
        "schema": "loom.task-image-publication-candidate-set/v1",
        "components": [identity.model_dump() for identity in identities],
    }
    assert (
        r.candidate_set_sha256(identities)
        == hashlib.sha256(rfc8785.dumps(expected_candidates)).hexdigest()
    )
    members = tuple(
        r.PublicationEnvelopeIdentity(**identity.model_dump(), envelope_sha256=f"{index + 1:064x}")
        for index, identity in enumerate(identities)
    )
    expected_envelopes = {
        "schema": "loom.task-image-publication-envelope-set/v1",
        "components": [member.model_dump() for member in members],
    }
    assert (
        r.publication_set_sha256(members)
        == hashlib.sha256(rfc8785.dumps(expected_envelopes)).hexdigest()
    )
    changed = (members[0].model_copy(update={"envelope_sha256": "f" * 64}), *members[1:])
    assert r.publication_set_sha256(changed) != r.publication_set_sha256(members)
    changed_identity = (
        identities[0].model_copy(update={"candidate_id": str(UUID(int=999))}),
        *identities[1:],
    )
    assert r.candidate_set_sha256(changed_identity) != r.candidate_set_sha256(identities)


def test_sidecar_only_set_is_valid_like_the_frozen_build_plan():
    r = receipts()
    identity = r.PublicationCandidateIdentity(candidate_id=str(UUID(int=1)), component="sidecar:db")
    assert len(r.candidate_set_sha256((identity,))) == 64


@pytest.mark.parametrize(
    "mutation",
    [
        "empty",
        "duplicate_name",
        "duplicate_id",
        "order",
        "invalid_component",
        "oversize",
        "unchecked",
    ],
)
def test_set_hash_rejects_ambiguous_or_unchecked_members(mutation):
    r = receipts()
    identities = (
        r.PublicationCandidateIdentity(candidate_id=str(UUID(int=1)), component="task"),
        r.PublicationCandidateIdentity(candidate_id=str(UUID(int=2)), component="sidecar:db"),
    )
    if mutation == "empty":
        identities = ()
    elif mutation == "duplicate_name":
        identities = (identities[0], identities[1].model_copy(update={"component": "task"}))
    elif mutation == "duplicate_id":
        identities = (
            identities[0],
            identities[1].model_copy(update={"candidate_id": identities[0].candidate_id}),
        )
    elif mutation == "order":
        identities = tuple(reversed(identities))
    elif mutation == "invalid_component":
        identities = (identities[0].model_copy(update={"component": "unknown"}),)
    elif mutation == "oversize":
        identities = identities * 65
    else:
        identities = (identities[0].model_copy(update={"candidate_id": "invalid"}),)
    with pytest.raises(ValueError):
        r.candidate_set_sha256(identities)
    members = tuple(
        r.PublicationEnvelopeIdentity.model_construct(**item.model_dump(), envelope_sha256="a" * 64)
        for item in identities
    )
    with pytest.raises(ValueError):
        r.publication_set_sha256(members)
