"""Prepare one irreversible retained-name retirement patch, never execute it.

The protected caller must durably admit a safe database/workload outcome before
sending this patch. No boolean, timestamp or matching snapshot supplies that
authority. Retaining ALL policy and binding names prevents this operation's
delayed CREATE requests from restoring an active fence; deletion/garbage collection
still requires request retirement and separate authority. External policy writers
must remain excluded. This module neither performs release nor authorizes handoff.
"""

from __future__ import annotations

from .protected_cnpg_fence_acquisition import _bytes, _decode_fence_object, _inspect, _mapping
from .protected_cnpg_fence_recovery import (
    CNPGFenceCreateIntent,
    CNPGFenceObjectReceipt,
    CNPGFenceRequest,
)


def prepare_cnpg_fence_retirement_patch(
    *, request: CNPGFenceRequest, pending: CNPGFenceCreateIntent,
    receipt: CNPGFenceObjectReceipt, observed: bytes,
) -> bytes | None:
    """Return UID/RV/spec-tested JSON Patch; None means the exact retained target.

    Only policy matchConditions change. Bindings, names, UIDs, and other policy
    inputs remain retained. This pure preparation is not a safe-outcome check.
    Send through the installed runner's kubectl JSON Patch transport with
    --field-manager=loom-cnpg-fence. Kubectl normalizes string escapes to the
    apiserver's Go encoding; re-serializing through other clients can make
    equivalent CEL strings fail the spec test. Other field managers fail readback.
    """
    if (receipt.intent_digest != request.intent_digest or receipt.ordinal != pending.ordinal
            or receipt.document_sha256 != request.document_sha256(receipt.ordinal)):
        raise ValueError("CNPG fence retirement receipt binding changed")
    desired = pending.document(request)
    if desired["kind"] != "ValidatingAdmissionPolicy":
        raise ValueError("CNPG fence retirement cannot alter a binding")
    value = _decode_fence_object(observed)
    metadata, spec = _mapping(value.get("metadata")), _mapping(value.get("spec"))
    retired_conditions = [{"name": "retired-handoff", "expression": "false"}]
    if spec.get("matchConditions") == retired_conditions:
        if type(metadata.get("generation")) is not int or metadata["generation"] != 2:
            raise ValueError("CNPG fence retired generation changed")
        # Validate every remaining field against the same admitted active object.
        # Only the known monotonic spec transition changes generation from 1 to 2.
        spec["matchConditions"] = _mapping(desired["spec"])["matchConditions"]
        metadata["generation"] = 1
        _inspect(request, pending, _bytes(value), expected_uid=receipt.uid)
        return None
    _inspect(request, pending, observed, expected_uid=receipt.uid)
    return _bytes([
        {"op": "test", "path": "/metadata/uid", "value": receipt.uid},
        {"op": "test", "path": "/metadata/resourceVersion", "value": metadata["resourceVersion"]},
        {"op": "test", "path": "/spec", "value": spec},
        {"op": "replace", "path": "/spec/matchConditions", "value": retired_conditions},
    ])
