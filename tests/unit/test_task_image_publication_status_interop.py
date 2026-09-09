from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import rfc8785

from loom_task_image_authority.publication_status import (
    canonical_status_bytes,
    decode_publication_status,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
STATUS_VECTORS = (
    REPO_ROOT
    / "cmd"
    / "loom-task-image-builder-supervisor"
    / "testdata"
    / "publication_status_vectors.json"
)
RECEIPT_VECTORS = (
    REPO_ROOT
    / "cmd"
    / "loom-task-image-builder-supervisor"
    / "testdata"
    / "publication_receipt_vectors.json"
)


def _json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_go_status_vectors_are_emitted_by_real_python_contract() -> None:
    vectors = _json(STATUS_VECTORS)
    assert [vector["name"] for vector in vectors["statuses"]] == [
        "queued",
        "running",
        "failed",
        "completed",
    ]

    for vector in vectors["statuses"]:
        payload = vector["payload"].encode("ascii")
        status = decode_publication_status(payload)

        assert canonical_status_bytes(status) == payload
        assert rfc8785.dumps(json.loads(payload)) == payload
        assert status.snapshot_sha256 == vectors["snapshot_sha256"]
        for field, expected in vectors["binding"].items():
            if field != "pinned_snapshot_sha256":
                assert getattr(status, field) == expected
        assert status.state == vector["expected"]["state"]
        assert status.failure_code == vector["expected"]["failure_code"]
        if status.receipt is None:
            assert vector["expected"]["publication_set_sha256"] is None
        else:
            assert (
                status.receipt.publication_set_sha256
                == vector["expected"]["publication_set_sha256"]
            )


def test_status_candidate_hash_is_independently_bound_to_complete_identity_set() -> None:
    status_vectors = _json(STATUS_VECTORS)
    receipt_vectors = _json(RECEIPT_VECTORS)
    maximum = next(
        vector for vector in receipt_vectors["candidate_sets"] if vector["name"] == "maximum_128"
    )
    independent_wire = rfc8785.dumps(
        {
            "schema": "loom.task-image-publication-candidate-set/v1",
            "components": maximum["identities"],
        }
    )
    independent_digest = hashlib.sha256(independent_wire).hexdigest()

    assert len(maximum["identities"]) == 128
    assert independent_digest == maximum["sha256"]
    assert independent_digest == status_vectors["binding"]["candidate_set_sha256"]
    for vector in status_vectors["statuses"]:
        assert json.loads(vector["payload"])["candidate_set_sha256"] == independent_digest
