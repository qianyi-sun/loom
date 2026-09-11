"""Retirement retains names so a delayed create cannot reactivate a released fence."""

import copy
import json
from uuid import uuid4

import pytest

from loom_cli.rollout.operator.protected_cnpg_fence_recovery import (
    CNPGFenceCreateIntent,
    CNPGFenceRequest,
)
from tests.loom_cli.rollout.operator.test_cnpg_fence_acquisition import FenceRunner


def _policy(ordinal=0):
    from loom_cli.rollout.operator.protected_cnpg_fence_recovery import CNPGFenceObjectReceipt
    request = CNPGFenceRequest("a" * 64, ())
    pending = CNPGFenceCreateIntent.prepare(request, ordinal=ordinal, nonce="b" * 32)
    runner = FenceRunner()
    payload = runner.capture_stdout_with_input(
        ("kubectl", "create", "--field-manager=loom-cnpg-fence"), env=runner.environment,
        input_payload=json.dumps(pending.document(request)).encode(), timeout_seconds=30,
    )
    value = json.loads(payload)
    receipt = CNPGFenceObjectReceipt(request.intent_digest, ordinal, value["metadata"]["uid"], request.document_sha256(ordinal))
    return request, pending, receipt, value


def _prepare(request, pending, receipt, value):
    from loom_cli.rollout.operator.protected_cnpg_fence_retirement import (
        prepare_cnpg_fence_retirement_patch,
    )
    return prepare_cnpg_fence_retirement_patch(request=request, pending=pending, receipt=receipt,
                                             observed=json.dumps(value).encode())


def test_retirement_patch_tests_exact_identity_and_spec_and_only_disables_matching():
    request, pending, receipt, value = _policy()
    original = copy.deepcopy(value)
    patch = json.loads(_prepare(request, pending, receipt, value))
    assert patch[:3] == [
        {"op": "test", "path": "/metadata/uid", "value": receipt.uid},
        {"op": "test", "path": "/metadata/resourceVersion", "value": "101"},
        {"op": "test", "path": "/spec", "value": value["spec"]},
    ]
    assert patch[3:] == [{"op": "replace", "path": "/spec/matchConditions",
                         "value": [{"name": "retired-handoff", "expression": "false"}]}]
    assert value == original


def test_retirement_refuses_even_a_correctly_bound_binding():
    request, pending, receipt, value = _policy(ordinal=1)
    assert value["kind"] == "ValidatingAdmissionPolicyBinding"
    with pytest.raises(ValueError, match="cannot alter a binding"):
        _prepare(request, pending, receipt, value)


@pytest.mark.parametrize("change", ["uid", "spec", "nonce", "receipt-digest", "ordinal"])
def test_retirement_refuses_changed_objects_or_rebound_receipts(change):
    from dataclasses import replace
    request, pending, receipt, value = _policy()
    if change == "uid":
        value["metadata"]["uid"] = str(uuid4())
    elif change == "spec":
        value["spec"]["failurePolicy"] = "Ignore"
    elif change == "nonce":
        value["metadata"]["annotations"]["loom.dev/fence-create-nonce"] = "c" * 32
    elif change == "receipt-digest":
        receipt = replace(receipt, document_sha256="f" * 64)
    else:
        receipt = replace(receipt, ordinal=1)
    with pytest.raises(ValueError, match="CNPG"):
        _prepare(request, pending, receipt, value)


def test_exact_retired_readback_is_idempotent_but_never_reactivates():
    request, pending, receipt, value = _policy()
    value["spec"]["matchConditions"] = [{"name": "retired-handoff", "expression": "false"}]
    value["metadata"].update(generation=2, resourceVersion="102")
    assert _prepare(request, pending, receipt, value) is None
    value["spec"]["matchConditions"][0]["expression"] = "true"
    with pytest.raises(ValueError, match="CNPG"):
        _prepare(request, pending, receipt, value)


@pytest.mark.parametrize("change", ["generation", "spec", "uid", "finalizer", "manager"])
def test_retired_shape_does_not_hide_unrelated_drift(change):
    request, pending, receipt, value = _policy()
    value["spec"]["matchConditions"] = [{"name": "retired-handoff", "expression": "false"}]
    value["metadata"].update(generation=2, resourceVersion="102")
    if change == "generation":
        value["metadata"]["generation"] = 3
    elif change == "uid":
        value["metadata"]["uid"] = str(uuid4())
    elif change == "spec":
        value["spec"]["failurePolicy"] = "Ignore"
    elif change == "finalizer":
        value["metadata"]["finalizers"] = ["foreign"]
    else:
        value["metadata"]["managedFields"][0]["manager"] = "foreign"
    with pytest.raises(ValueError, match="CNPG"):
        _prepare(request, pending, receipt, value)
