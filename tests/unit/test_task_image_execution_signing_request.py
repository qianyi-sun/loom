"""Fixed request bounds and canonical bytes before signing transport."""

import hashlib

import pytest
import rfc8785

from loom_task_image_authority.execution_signing_request import (
    MAX_EXECUTION_SIGNING_REQUEST_BYTES,
    decode_execution_signing_request,
)
from tests.unit.test_task_image_execution_grant import fixture


def payload():
    grant, _, values = fixture()
    return dict(
        schema="loom.task-image-execution-signing-request/v1", grant_id=grant["grant_id"],
        revision=grant["revision"], grant_sha256=hashlib.sha256(rfc8785.dumps(grant)).hexdigest(),
        frozen_plan=values["plan_wire"].decode(), publications=[item.decode() for item in values["publication_wires"]],
    )


def test_fixed_preparation_request_preserves_original_attachment_bytes():
    data = payload()
    wire = rfc8785.dumps(data)
    request = decode_execution_signing_request(wire)
    assert request.canonical_bytes() == wire
    assert request.publications == tuple(data["publications"])
    assert request.frozen_plan == data["frozen_plan"]


@pytest.mark.parametrize("change", ["key", "domain", "clock", "grant", "duplicate", "space", "oversize", "aggregate", "empty", "boolean", "float"])
def test_request_refuses_caller_authority_and_unbounded_ambiguous_inputs(change):
    data = payload()
    if change in {"key", "domain", "clock", "grant"}:
        data[change] = "caller-selected"
    elif change == "aggregate":
        data["publications"] = ["x" * 131072] * 17
    elif change == "empty":
        data["publications"] = []
    elif change == "boolean":
        data["revision"] = True
    wire = rfc8785.dumps(data)
    if change == "duplicate":
        wire = wire.replace(b'"revision":1', b'"revision":1,"revision":1')
    elif change == "space":
        wire += b" "
    elif change == "oversize":
        wire = b"x" * (MAX_EXECUTION_SIGNING_REQUEST_BYTES + 1)
    elif change == "float":
        wire = wire.replace(b'"revision":1', b'"revision":1.0')
    with pytest.raises(ValueError):
        decode_execution_signing_request(wire)
