from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import rfc8785

from loom_task_image_authority.publication_receipts import (
    PublicationCandidateIdentity,
    candidate_set_sha256,
    canonical_receipt_bytes,
    decode_publication_receipt,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
VECTORS = (
    REPO_ROOT
    / "cmd"
    / "loom-task-image-builder-supervisor"
    / "testdata"
    / "publication_receipt_vectors.json"
)


def _vectors() -> dict[str, Any]:
    return json.loads(VECTORS.read_text(encoding="utf-8"))


def test_go_receipt_vector_is_emitted_by_real_python_contract() -> None:
    vector = _vectors()["receipts"][0]
    payload = vector["payload"].encode("ascii")
    receipt = decode_publication_receipt(payload)

    assert canonical_receipt_bytes(receipt) == payload
    assert rfc8785.dumps(json.loads(payload)) == payload
    for field, expected in vector["binding"].items():
        assert getattr(receipt, field) == expected
    for field, expected in vector["expected"].items():
        assert getattr(receipt, field) == expected


def test_go_candidate_vectors_match_contract_and_independent_rfc8785_hash() -> None:
    vectors = _vectors()["candidate_sets"]
    assert {vector["name"]: len(vector["identities"]) for vector in vectors} == {
        "optional_task": 3,
        "sidecar_only": 2,
        "maximum_128": 128,
    }

    for vector in vectors:
        identities = tuple(
            PublicationCandidateIdentity.model_validate(identity)
            for identity in vector["identities"]
        )
        independent_wire = rfc8785.dumps(
            {
                "schema": "loom.task-image-publication-candidate-set/v1",
                "components": vector["identities"],
            }
        )
        independent_digest = hashlib.sha256(independent_wire).hexdigest()

        assert vector["sha256"] == independent_digest
        assert candidate_set_sha256(identities) == independent_digest
