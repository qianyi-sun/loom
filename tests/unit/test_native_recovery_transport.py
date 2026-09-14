"""Recovery transport never borrows application authority or trusts changed history."""

from types import SimpleNamespace
from uuid import uuid4

import pytest

from loom_capacity_agent.build_admission import BuildClaimRequestV1
from loom_capacity_agent.native_recovery_publication import (
    NativeRecoveryHistoryV1,
    NativeRecoveryPublicationV1,
    NativeRecoveryReceiptV1,
)
from loom_capacity_manager.contracts import canonical_digest
from tests.unit.test_capacity_typed_admission_routing import configured
from tests.unit.test_native_recovery_contracts import observation


@pytest.mark.parametrize("purpose", ["personal-build-worker", "application-worker"])
@pytest.mark.parametrize("operation", ["publish_recovery", "read_recovery"])
@pytest.mark.parametrize("boundary", ["exact", "changed", "invalid"])
async def test_recovery_route_binds_exact_claim_and_closes_transport(tmp_path, purpose, operation, boundary):
    module, binding, document, path, digest = configured(tmp_path, "oldlab", purpose)
    _, final = observation()
    locator = final.preparation.locator.model_copy(update={"physical": final.preparation.locator.physical.model_copy(update={"binding": binding.binding})})
    prepared = final.preparation.model_copy(update={"locator": locator})
    claim = BuildClaimRequestV1(binding=binding.binding, operation_id=uuid4(), request_id=uuid4(),
        worker_id=locator.worker_id, worker_incarnation=locator.worker_incarnation)
    request = NativeRecoveryPublicationV1(claim=claim, record=prepared)
    changed = request.model_copy(update={"claim": claim.model_copy(update={"operation_id": uuid4()})}) if boundary == "changed" else request
    receipt = NativeRecoveryReceiptV1(request=changed, request_digest=canonical_digest(changed))
    calls = []

    async def invoke(incoming, **kwargs):
        assert incoming == (request if operation == "publish_recovery" else claim)
        assert kwargs == {"worker_credential": "x" * 43}
        calls.append("invoke")
        if boundary == "invalid":
            return object()
        return receipt if operation == "publish_recovery" else NativeRecoveryHistoryV1(preparation=receipt, finalization=None)

    async def close():
        calls.append("close")

    def forbidden(*args, **kwargs):
        pytest.fail("native recovery opened application authority")

    router = module.TypedAdmissionRouter(path, expected_sha256=digest, executor=document.executor,
        build_client_factory=lambda *args: SimpleNamespace(**{operation: invoke, "aclose": close}),
        application_client_factory=forbidden)
    argument = request if operation == "publish_recovery" else claim
    if purpose == "personal-build-worker" and boundary == "exact":
        assert await getattr(router, operation)(argument, worker_credential="x" * 43) is not None
    else:
        with pytest.raises(ValueError):
            await getattr(router, operation)(argument, worker_credential="x" * 43)
    assert calls == ([] if purpose == "application-worker" else ["invoke", "close"])


@pytest.mark.parametrize("purpose", ["personal-build-worker", "application-worker"])
@pytest.mark.parametrize("boundary", ["exact", "changed", "invalid"])
async def test_recovery_admission_routes_exact_request_without_application_authority(tmp_path, purpose, boundary):
    from loom_capacity_agent.native_recovery_publication import (
        NativeRecoveryAdmissionRequestV1,
        NativeRecoveryAdmissionV1,
        NativeRecoveryHostIdentityV1,
        NativeRecoveryProfileV1,
    )

    module, binding, document, path, digest = configured(tmp_path, "oldlab", purpose)
    claim = BuildClaimRequestV1(binding=binding.binding, operation_id=uuid4(), request_id=uuid4(),
        worker_id=uuid4(), worker_incarnation=uuid4())
    request = NativeRecoveryAdmissionRequestV1(claim=claim, node_id=claim.binding.node_ids[0], boot_id=uuid4())
    changed = request.model_copy(update={"claim": claim.model_copy(update={"operation_id": uuid4()})}) if boundary == "changed" else request
    response = NativeRecoveryAdmissionV1(request=changed, profile=NativeRecoveryProfileV1(
        installation_id=uuid4(), pool_id="oldlab", launch_profile_sha256="a" * 64,
        worker_config_sha256="b" * 64, release_manifest_sha256="c" * 64),
        host=NativeRecoveryHostIdentityV1(node_id=request.node_id, boot_id=request.boot_id,
            original_uid=24850, original_gid=24851, cgroup_namespace_device=4, cgroup_namespace_inode=100))
    calls = []

    async def invoke(incoming, **kwargs):
        assert incoming == request and kwargs == {"worker_credential": "x" * 43}
        calls.append("invoke")
        return object() if boundary == "invalid" else response

    async def close():
        calls.append("close")

    router = module.TypedAdmissionRouter(path, expected_sha256=digest, executor=document.executor,
        build_client_factory=lambda *args: SimpleNamespace(read_recovery_admission=invoke, aclose=close),
        application_client_factory=lambda *args: pytest.fail("application authority opened"))
    if purpose == "personal-build-worker" and boundary == "exact":
        assert await router.read_recovery_admission(request, worker_credential="x" * 43) == response
    else:
        with pytest.raises(ValueError):
            await router.read_recovery_admission(request, worker_credential="x" * 43)
    assert calls == ([] if purpose == "application-worker" else ["invoke", "close"])
