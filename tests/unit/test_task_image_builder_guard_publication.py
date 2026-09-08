from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
import rfc8785

from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.publication import parse_publication_status

VECTORS = json.loads(
    (Path(__file__).resolve().parents[2] / "cmd/loom-task-image-builder-supervisor"
     / "testdata/publication_status_vectors.json").read_text()
)
BINDING = {
    key: UUID(value) if key.endswith("_id") else value
    for key, value in VECTORS["binding"].items()
    if key in {"grant_id", "operation_id", "materialization_id", "attempt_id", "lease_epoch"}
}


def _document(state: str = "completed") -> dict[str, Any]:
    return json.loads(next(v["payload"] for v in VECTORS["statuses"] if v["name"] == state))


def _wire(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")


@pytest.mark.parametrize("vector", VECTORS["statuses"], ids=lambda v: v["name"])
def test_guard_accepts_exact_python_go_status_bytes_and_owns_them(vector: dict[str, Any]) -> None:
    payload = vector["payload"].encode("ascii")
    parsed = parse_publication_status(payload, **BINDING)
    assert parsed.canonical_bytes == payload == rfc8785.dumps(json.loads(payload))
    document = parsed.as_dict()
    document["state"] = "unsafe"
    if "receipt" in document:
        document["receipt"]["worker_generation"] = 0
    assert parsed.as_dict() == json.loads(payload)
    with pytest.raises(FrozenInstanceError):
        parsed.canonical_bytes = b"{}"


@pytest.mark.parametrize("field", list(_document()))
@pytest.mark.parametrize("value", [None, True, [], {}])
def test_guard_rejects_null_and_wrong_status_types(field: str, value: object) -> None:
    document = _document()
    document[field] = value
    with pytest.raises(GuardError, match="^authority_publication_invalid$"):
        parse_publication_status(_wire(document), **BINDING)


@pytest.mark.parametrize("field", list(_document()["receipt"]))
@pytest.mark.parametrize("value", [None, True, [], {}])
def test_guard_rejects_null_and_wrong_receipt_types(field: str, value: object) -> None:
    document = _document()
    document["receipt"][field] = value
    with pytest.raises(GuardError, match="^authority_publication_invalid$"):
        parse_publication_status(_wire(document), **BINDING)


@pytest.mark.parametrize("field", list(_document()))
def test_guard_rejects_missing_status_fields(field: str) -> None:
    document = _document()
    del document[field]
    with pytest.raises(GuardError, match="authority_publication_invalid"):
        parse_publication_status(_wire(document), **BINDING)


@pytest.mark.parametrize("field", list(_document()["receipt"]))
def test_guard_rejects_missing_receipt_fields(field: str) -> None:
    document = _document()
    del document["receipt"][field]
    with pytest.raises(GuardError, match="authority_publication_invalid"):
        parse_publication_status(_wire(document), **BINDING)


@pytest.mark.parametrize("field", list(BINDING))
def test_guard_rejects_wrong_request_binding(field: str) -> None:
    binding = BINDING | {field: 1 if field == "lease_epoch" else UUID(int=99)}
    with pytest.raises(GuardError, match="authority_publication_invalid"):
        parse_publication_status(_wire(_document()), **binding)


@pytest.mark.parametrize("field", [
    "operation_id", "materialization_id", "attempt_id", "lease_epoch",
    "snapshot_sha256", "candidate_set_sha256", "component_count",
])
def test_guard_rejects_receipt_internal_binding_change(field: str) -> None:
    document = _document()
    document["receipt"][field] = (
        str(UUID(int=99)) if field.endswith("_id") else "b" * 64
        if field.endswith("sha256") else 1
    )
    with pytest.raises(GuardError, match="authority_publication_invalid"):
        parse_publication_status(_wire(document), **BINDING)


@pytest.mark.parametrize(("field", "value"), [
    ("schema", "loom.task-image-publication-status/v2"),
    ("schema_name", "loom.task-image-publication-status/v1"),
    ("private", "sentinel-private-registry-token"),
    ("grant_id", str(UUID(int=0))),
    ("grant_id", "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"),
    ("grant_id", "000000000000000000000000000001f4"),
    ("snapshot_sha256", "0" * 64), ("snapshot_sha256", "A" * 64),
    ("snapshot_sha256", "a" * 64 + "\n"),
    ("lease_epoch", 0), ("lease_epoch", -1),
    ("lease_epoch", 9007199254740992), ("lease_epoch", 1.0),
    ("component_count", 0), ("component_count", 129),
    ("state", "ready"), ("failure_code", "integrity"),
])
def test_guard_rejects_status_schema_and_scalar_mutations(field: str, value: object) -> None:
    with pytest.raises(GuardError, match="authority_publication_invalid"):
        parse_publication_status(_wire(_document() | {field: value}), **BINDING)


@pytest.mark.parametrize(("field", "value"), [
    ("schema", "loom.task-image-publication-receipt/v2"),
    ("schema_name", "loom.task-image-publication-receipt/v1"),
    ("private", "sentinel-private-registry-token"),
    ("worker_generation", 0), ("worker_generation", 9007199254740992),
    ("worker_generation", 1.0), ("publication_set_sha256", "0" * 64),
    ("completed_at", "2026-09-08T12:34:56.0Z"),
    ("completed_at", "2026-09-08T12:34:56+00:00"),
    ("completed_at", "2026-09-08 12:34:56Z"),
    ("completed_at", "2026-02-29T12:34:56Z"),
    ("completed_at", "2026-09-08T12:34:60Z"),
    ("completed_at", "0000-09-08T12:34:56Z"),
])
def test_guard_rejects_receipt_scalar_mutations(field: str, value: object) -> None:
    document = _document()
    document["receipt"][field] = value
    with pytest.raises(GuardError, match="authority_publication_invalid"):
        parse_publication_status(_wire(document), **BINDING)


@pytest.mark.parametrize("state", ["queued", "running", "failed"])
def test_guard_rejects_receipt_in_noncompleted_state(state: str) -> None:
    document = _document(state) | {"receipt": _document()["receipt"]}
    with pytest.raises(GuardError, match="authority_publication_invalid"):
        parse_publication_status(_wire(document), **BINDING)


@pytest.mark.parametrize("code", ["integrity", "authority_lost", "verification_failed", "deadline"])
def test_guard_accepts_only_safe_failure_vocabulary(code: str) -> None:
    document = _document("failed") | {"failure_code": code}
    assert parse_publication_status(_wire(document), **BINDING).as_dict() == document


@pytest.mark.parametrize("code", [None, True, [], {}, "", "sentinel-private-registry-token"])
def test_guard_rejects_unsafe_failure_vocabulary(code: object) -> None:
    with pytest.raises(GuardError, match="authority_publication_invalid"):
        parse_publication_status(_wire(_document("failed") | {"failure_code": code}), **BINDING)


@pytest.mark.parametrize("payload", [
    b"", b"{}", b"null", b"[]", b"\xff", b" " * 4097,
    b"[" * 2000 + b"]" * 2000,
    _wire(_document()) + b"\n",
    _wire(_document()).replace(b'"state":', b'"state":"queued","state":'),
    _wire(_document()).replace(b'"worker_generation":', b'"worker_generation":1,"worker_generation":'),
    _wire(_document()).replace(b'"completed"', b'"\\u0063ompleted"'),
    _wire(_document()).replace(b'"worker_generation":9007199254740991', b'"worker_generation":NaN'),
    _wire(_document()).decode().encode("utf-16"),
])
def test_guard_rejects_noncanonical_duplicate_encoding_and_oversized_bytes(payload: bytes) -> None:
    with pytest.raises(GuardError, match="^authority_publication_invalid$") as caught:
        parse_publication_status(payload, **BINDING)
    assert "sentinel" not in str(caught.value)
