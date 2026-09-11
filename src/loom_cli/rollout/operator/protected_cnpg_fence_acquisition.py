"""Acquire a fixed CNPG input fence under already exclusive policy authority.

The installed caller MUST exclude policy/authority writers and retire their
outstanding writes before using acquisition. A nonce, field manager, generation
or probe cannot establish that exclusion. Pending CREATE recovery is valid only
inside that same protected authority window. This helper does not delete policies
or authorize database handoff. Its own outstanding API requests must be retired
before later removal; a timeout or client exit is not server-request retirement.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Protocol

from .final_gate_plan import FinalGatePlan
from .protected_cnpg_fence_recovery import (
    CNPGFenceCreateIntent,
    CNPGFenceObjectReceipt,
    CNPGFenceRequest,
)

if TYPE_CHECKING:
    from .protected_apply_journal import ProtectedApplyJournal

_API = "admissionregistration.k8s.io/v1"
_MANAGER = "loom-cnpg-fence"
_RESOURCES = {"ValidatingAdmissionPolicy": "validatingadmissionpolicies.admissionregistration.k8s.io",
              "ValidatingAdmissionPolicyBinding": "validatingadmissionpolicybindings.admissionregistration.k8s.io"}


class CNPGFenceAcquisitionRunner(Protocol):
    @property
    def environment(self) -> Mapping[str, str]: ...

    def capture_stdout(self, argv: Sequence[str], *, env: Mapping[str, str], timeout_seconds: float) -> bytes: ...

    def capture_stdout_with_input(self, argv: Sequence[str], *, env: Mapping[str, str],
                                  input_payload: bytes, timeout_seconds: float) -> bytes: ...

    def probe_cnpg_input_fence(self, *, intent_digest: str, target_pooler_names: tuple[str, ...]) -> bool: ...


def acquire_cnpg_input_fence(
    plan: FinalGatePlan, *, journal: ProtectedApplyJournal, runner: CNPGFenceAcquisitionRunner,
) -> tuple[CNPGFenceObjectReceipt, ...]:
    """Converge creates only; never overwrite, rebind or remove an existing UID.

    Lost replies can recover only from durable absent-to-create intents plus
    exact nonce/spec/metadata readback under the outer exclusive authority window.
    Callers must re-run this live check on recovery, not cache its return value.
    """
    request = journal.read_application_cnpg_fence(plan)
    if request is None:
        raise RuntimeError("CNPG fence acquisition requires a durable request")
    receipts = []
    for ordinal, document in enumerate(request.documents()):
        known = journal.read_application_cnpg_fence_object(plan, ordinal=ordinal)
        pending = journal.read_application_cnpg_fence_create(plan, ordinal=ordinal)
        payload = _read(runner, document)
        if known is not None and (not payload or pending is None):
            raise RuntimeError("CNPG fence known object or create intent disappeared")
        if pending is None:
            if payload:
                raise RuntimeError("CNPG fence existing object has no owned pending create")
            pending = journal.prepare_application_cnpg_fence_create(plan, ordinal=ordinal)
        if not payload:
            payload = runner.capture_stdout_with_input(
                ("kubectl", "create", "--filename=-", f"--field-manager={_MANAGER}",
                 "--validate=strict", "--show-managed-fields=true", "--output=json", "--request-timeout=30s"),
                env=runner.environment, input_payload=_bytes(pending.document(request)), timeout_seconds=30,
            )
        uid = _inspect(request, pending, payload, expected_uid=known.uid if known else None)
        receipt = journal.record_application_cnpg_fence_object(plan, ordinal=ordinal, uid=uid)
        _inspect(request, pending, _read(runner, document), expected_uid=receipt.uid)
        receipts.append(receipt)
    if not runner.probe_cnpg_input_fence(intent_digest=request.intent_digest,
                                       target_pooler_names=request.target_pooler_names):
        raise RuntimeError("CNPG input fence is not enforcing all protected inputs")
    # A command success or earlier per-object read is not the final live state.
    for receipt, document in zip(receipts, request.documents(), strict=True):
        pending = journal.read_application_cnpg_fence_create(plan, ordinal=receipt.ordinal)
        if pending is None:
            raise RuntimeError("CNPG fence create intent disappeared")
        _inspect(request, pending, _read(runner, document), expected_uid=receipt.uid, require_typechecked=True)
    return tuple(receipts)


def _read(runner: CNPGFenceAcquisitionRunner, document: Mapping[str, object]) -> bytes:
    metadata = _mapping(document["metadata"])
    kind, name = document["kind"], metadata["name"]
    assert isinstance(kind, str) and isinstance(name, str)
    return runner.capture_stdout(
        ("kubectl", "get", _RESOURCES[kind], name, "--ignore-not-found=true", "--show-managed-fields=true",
         "--output=json", "--request-timeout=30s"), env=runner.environment, timeout_seconds=30,
    )


def _inspect(request: CNPGFenceRequest, pending: CNPGFenceCreateIntent, payload: bytes,
             *, expected_uid: str | None, require_typechecked: bool = False) -> str:
    desired = pending.document(request)
    observed = _decode_fence_object(payload)
    return _inspect_object(request, pending, observed, desired=desired,
                           expected_uid=expected_uid, require_typechecked=require_typechecked)


def _decode_fence_object(payload: bytes) -> dict[str, object]:
    if not payload or len(payload) > 256 * 1024:
        raise ValueError("CNPG fence object response is absent or oversized")

    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("CNPG fence object repeats fields")
            result[key] = value
        return result

    def nonfinite(value: str) -> object:
        raise ValueError("CNPG fence object contains nonfinite numbers")

    return _mapping(json.loads(payload, object_pairs_hook=unique, parse_constant=nonfinite))


def _inspect_object(request: CNPGFenceRequest, pending: CNPGFenceCreateIntent, observed: dict[str, object],
                    *, desired: dict[str, object], expected_uid: str | None, require_typechecked: bool) -> str:
    metadata = _mapping(observed.get("metadata"))
    expected_metadata = _mapping(desired["metadata"])
    uid, version = metadata.get("uid"), metadata.get("resourceVersion")
    if (observed.get("apiVersion") != _API or observed.get("kind") != desired["kind"]
            or set(observed) - {"apiVersion", "kind", "metadata", "spec", "status"}
            or metadata.get("name") != expected_metadata["name"] or metadata.get("namespace") not in (None, "")
            or not isinstance(uid, str) or (expected_uid is not None and uid != expected_uid)
            or not isinstance(version, str) or re.fullmatch(r"[1-9][0-9]{0,31}", version) is None
            or type(metadata.get("generation")) is not int or metadata["generation"] != 1
            or metadata.get("deletionTimestamp") is not None or metadata.get("deletionGracePeriodSeconds") is not None
            or metadata.get("labels", {}) != {} or metadata.get("finalizers", []) != []
            or metadata.get("ownerReferences", []) != []
            or _bytes(metadata.get("annotations")) != _bytes(expected_metadata["annotations"])
            or _bytes(observed.get("spec")) != _bytes(desired["spec"])):
        raise ValueError("CNPG fence object identity or inputs changed")
    CNPGFenceObjectReceipt(request.intent_digest, pending.ordinal, uid, pending.document_sha256)
    # Consistency checks, not authenticated creator identity. Admission-authority
    # exclusion is a precondition; Kubernetes field managers can be user supplied.
    managed = metadata.get("managedFields")
    if not isinstance(managed, list) or not managed:
        raise ValueError("CNPG fence managed fields are absent")
    owns_inputs = False
    for raw in managed:
        entry = _mapping(raw)
        fields = _mapping(entry.get("fieldsV1"))
        if (entry.get("apiVersion") != _API or entry.get("operation") not in {"Update", "Apply"}
                or entry.get("fieldsType") != "FieldsV1"):
            raise ValueError("CNPG fence managed field contract changed")
        if entry.get("subresource") == "status" and set(fields) <= {"f:status"}:
            continue
        annotations = _mapping(_mapping(fields.get("f:metadata")).get("f:annotations"))
        if (entry.get("manager") != _MANAGER or entry.get("operation") != "Update"
                or entry.get("subresource", "") != ""
                or not isinstance(fields.get("f:spec"), dict)
                or any(annotations.get("f:" + key) != {} for key in _mapping(expected_metadata["annotations"]))):
            raise ValueError("CNPG fence input field manager changed")
        owns_inputs = True
    if not owns_inputs:
        raise ValueError("CNPG fence has no input field manager")
    if require_typechecked and observed["kind"] == "ValidatingAdmissionPolicy":
        status = _mapping(observed.get("status", {}))
        if (type(status.get("observedGeneration")) is not int or status["observedGeneration"] != 1
                or not isinstance(status.get("typeChecking"), dict)):
            raise RuntimeError("CNPG fence type checking is not ready")
        if _mapping(status["typeChecking"]).get("expressionWarnings", []) != []:
            raise ValueError("CNPG fence policy has type checking warnings")
    return uid


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError("CNPG fence object mapping is invalid")
    return value


def _bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
