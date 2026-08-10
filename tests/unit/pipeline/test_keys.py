from __future__ import annotations

import hashlib
from decimal import Decimal
from uuid import UUID, uuid5

import pytest
from pydantic import BaseModel

from loom.pipeline.keys import (
    MAX_SAFE_INTEGER,
    CanonicalizationError,
    canonical_digest,
    canonical_document,
    canonical_identity,
    canonical_uuid5,
)


def test_canonical_document_is_rfc8785_jcs_with_one_ascii_lf() -> None:
    value = {
        "z": [3, 2, 1],
        "a": {"two": 2.0, "one": 1e-7},
        "unicode": "e\u0301",
    }

    identity = canonical_identity(value)

    assert identity == ('{"a":{"one":1e-7,"two":2},"unicode":"e\u0301","z":[3,2,1]}'.encode())
    assert canonical_document(value) == identity + b"\n"
    assert not identity.endswith(b"\n")
    assert canonical_document(value).endswith(b"\n")
    assert not canonical_document(value).endswith(b"\n\n")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (1.0, b"1"),
        (-0.0, b"0"),
        (1e-7, b"1e-7"),
        (1e21, b"1e+21"),
        (0.002, b"0.002"),
        (333333333.33333329, b"333333333.3333333"),
    ],
)
def test_canonical_identity_uses_ecmascript_number_serialization(
    value: float, expected: bytes
) -> None:
    assert canonical_identity(value) == expected


def test_digest_distinguishes_persisted_document_from_raw_identity() -> None:
    value = {"kind": "attempt", "ordinal": 2}
    persisted = canonical_document(value)
    identity = canonical_identity(value)

    assert canonical_digest(value) == f"sha256:{hashlib.sha256(persisted).hexdigest()}"
    assert canonical_digest(value, persisted=False) == (
        f"sha256:{hashlib.sha256(identity).hexdigest()}"
    )
    assert canonical_digest(value) != canonical_digest(value, persisted=False)


def test_uuid5_name_is_raw_jcs_without_lf() -> None:
    namespace = UUID("12345678-1234-5678-1234-567812345678")
    value = {"stage": "judge", "shard": "case-001"}

    assert canonical_uuid5(namespace, value) == uuid5(
        namespace, canonical_identity(value).decode("utf-8")
    )
    assert canonical_uuid5(namespace, value) != uuid5(
        namespace, canonical_document(value).decode("utf-8")
    )


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_numbers_are_rejected(value: float) -> None:
    with pytest.raises(CanonicalizationError, match="NaN and Infinity"):
        canonical_identity(value)


@pytest.mark.parametrize("value", [MAX_SAFE_INTEGER + 1, -MAX_SAFE_INTEGER - 1])
def test_integers_outside_interoperable_range_are_rejected(value: int) -> None:
    with pytest.raises(CanonicalizationError, match="interoperable JSON range"):
        canonical_document(value)


def test_jcs_preserves_unicode_code_points_without_global_nfc_normalization() -> None:
    decomposed = {"e\u0301": "A\u030a"}
    composed = {"\u00e9": "\u00c5"}

    assert canonical_identity(decomposed) != canonical_identity(composed)
    assert canonical_identity(decomposed) == '{"e\u0301":"A\u030a"}'.encode()


def test_distinct_unicode_property_names_remain_distinct() -> None:
    assert canonical_identity({"e\u0301": 1, "\u00e9": 2}) == ('{"e\u0301":1,"\u00e9":2}'.encode())


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ({1: "not a JSON key"}, "keys must be strings"),
        ("\ud800", "lone Unicode surrogate"),
        (b"raw bytes", "unsupported canonical JSON value"),
    ],
)
def test_non_json_values_are_rejected(value: object, message: str) -> None:
    with pytest.raises(CanonicalizationError, match=message):
        canonical_identity(value)


def test_base_model_free_form_values_do_not_coerce_before_jcs() -> None:
    class Holder(BaseModel):
        value: object

    with pytest.raises(CanonicalizationError, match="unsupported canonical JSON value"):
        canonical_identity(Holder(value=Decimal("1.25")))
