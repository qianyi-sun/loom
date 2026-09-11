"""Fresh native execution permission is distinct from source/claim metadata."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from loom_capacity_agent import build_admission as protocol
from loom_capacity_manager.contracts import canonical_digest
from tests.unit.test_capacity_build_admission_client import native_registration
from tests.unit.test_native_build_context import claim_for


def execution_request(pool="oldlab"):
    return protocol.BuildExecutionRequestV1(claim=claim_for(native_registration(pool).binding),
        challenge=uuid4(), source_binding_sha256="a" * 64)


@pytest.mark.parametrize("pool", ["oldlab", "gb10"])
@pytest.mark.parametrize("boundary", ["exact", "expired", "reversed", "too-long", "naive", "digest"])
def test_native_execution_permit_is_exact_aware_and_short_lived(pool, boundary):
    request = execution_request(pool)
    now = datetime.now(UTC)
    values = dict(request=request, request_digest=canonical_digest(request), issued_at=now,
        not_after=now + timedelta(seconds=10))
    if boundary == "expired":
        values["not_after"] = now
    elif boundary == "reversed":
        values["not_after"] = now - timedelta(microseconds=1)
    elif boundary == "too-long":
        values["not_after"] += timedelta(microseconds=1)
    elif boundary == "naive":
        values["issued_at"] = now.replace(tzinfo=None)
    elif boundary == "digest":
        values["request_digest"] = "f" * 64
    if boundary == "exact":
        permit = protocol.BuildExecutionPermitV1(**values)
        assert permit.executable is True
        assert permit.request == request
        assert protocol.BuildExecutionPermitV1.model_validate_json(permit.model_dump_json()) == permit
    else:
        with pytest.raises(ValueError):
            protocol.BuildExecutionPermitV1(**values)


def test_execution_credential_is_transport_only_and_hidden_from_repr():
    request = execution_request()
    envelope = protocol.BuildExecutionExchangeV1(request=request, worker_credential="x" * 43)
    assert "x" * 43 not in repr(envelope)
    assert "worker_credential" not in request.model_dump_json()
