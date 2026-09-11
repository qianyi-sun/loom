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


@pytest.mark.parametrize("boundary", ["exact", "challenge", "source", "claim", "noncanonical", "http"])
async def test_permission_client_validates_fresh_request_not_only_receipt_shape(boundary):
    import httpx

    from loom_capacity_manager.contracts import canonical_bytes
    from tests.unit.test_capacity_build_admission_client import client_for

    request = execution_request()
    now = datetime.now(UTC)

    async def handle(incoming):
        assert incoming.url.path.endswith("/execution")
        received = protocol.BuildExecutionExchangeV1.model_validate_json(incoming.content)
        assert received.request == request and received.worker_credential == "x" * 43
        changed = request
        if boundary == "challenge":
            changed = request.model_copy(update={"challenge": uuid4()})
        elif boundary == "source":
            changed = request.model_copy(update={"source_binding_sha256": "f" * 64})
        elif boundary == "claim":
            changed = request.model_copy(update={"claim": execution_request().claim})
        permit = protocol.BuildExecutionPermitV1(request=changed, request_digest=canonical_digest(changed),
            issued_at=now, not_after=now + timedelta(seconds=10))
        wire = canonical_bytes(permit) + (b" " if boundary == "noncanonical" else b"")
        return httpx.Response(409 if boundary == "http" else 200, content=wire)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = client_for(http, request.claim)
        if boundary == "exact":
            assert (await client.authorize_execution(request, worker_credential="x" * 43)).request == request
        else:
            with pytest.raises(RuntimeError):
                await client.authorize_execution(request, worker_credential="x" * 43)


def test_production_config_cannot_enable_uninstalled_execution_runtime():
    from loom_service.personal_dev_build_admission import BuildAdmissionServiceConfigV1

    with pytest.raises(ValueError):
        BuildAdmissionServiceConfigV1(mode="native-execution", database_url_file="/etc/loom/db",
            database_url_sha256="a" * 64, principals_file="/etc/loom/principals", principals_sha256="b" * 64)


@pytest.mark.parametrize("purpose", ["personal-build-worker", "application-worker"])
@pytest.mark.parametrize("boundary", ["exact", "wrong-challenge", "invalid"])
async def test_execution_permission_never_uses_application_route(tmp_path, purpose, boundary):
    from types import SimpleNamespace

    from tests.unit.test_capacity_typed_admission_routing import configured

    module, physical, document, path, digest = configured(tmp_path, "oldlab", purpose)
    request = execution_request().model_copy(update={"claim": claim_for(physical.binding)})
    now = datetime.now(UTC)
    calls = []

    async def authorize(incoming, **kwargs):
        calls.append("authorize")
        assert incoming == request and kwargs == {"worker_credential": "x" * 43}
        changed = request.model_copy(update={"challenge": uuid4()}) if boundary == "wrong-challenge" else request
        return object() if boundary == "invalid" else protocol.BuildExecutionPermitV1(
            request=changed, request_digest=canonical_digest(changed), issued_at=now, not_after=now + timedelta(seconds=10))

    async def close():
        calls.append("close")

    def factory(*args):
        return SimpleNamespace(authorize_execution=authorize, aclose=close)

    def forbidden(*args, **kwargs):
        pytest.fail("execution permission reached application authority")

    router = module.TypedAdmissionRouter(path, expected_sha256=digest, executor=document.executor,
        build_client_factory=factory, application_client_factory=forbidden)
    if purpose == "personal-build-worker" and boundary == "exact":
        assert (await router.authorize_execution(request, worker_credential="x" * 43)).request == request
    else:
        with pytest.raises(ValueError):
            await router.authorize_execution(request, worker_credential="x" * 43)
    assert calls == ([] if purpose == "application-worker" else ["authorize", "close"])
