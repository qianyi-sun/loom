"""Retire handoff policies monotonically after durable observed restoration.

The protected caller must durably admit a safe database/workload outcome before
sending this patch. No boolean, timestamp or matching snapshot supplies that
authority. Retaining ALL policy and binding names prevents this operation's
delayed CREATE requests from restoring an active fence; deletion/garbage collection
still requires request retirement and separate authority. External policy writers
must remain excluded. This phase does not release the database guard or authorize
the enclosing handoff.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from .final_gate_plan import FinalGatePlan
from .protected_application_restoration import ApplicationRestorationRunner
from .protected_application_workload_runtime import _live_guard
from .protected_cnpg_fence_acquisition import (
    _RESOURCES,
    CNPGFenceAcquisitionRunner,
    _bytes,
    _decode_fence_object,
    _inspect,
    _mapping,
    _read,
)
from .protected_cnpg_fence_recovery import (
    CNPGFenceCreateIntent,
    CNPGFenceObjectReceipt,
    CNPGFenceRequest,
)
from .protected_cnpg_input_fence import cnpg_input_fence_probe_commands

if TYPE_CHECKING:
    from .protected_apply_journal import ProtectedApplyComponent, ProtectedApplyJournal
    from .staging_mutation_guard import MutationGuardEvidence


class CNPGFenceRetirementRunner(ApplicationRestorationRunner, CNPGFenceAcquisitionRunner, Protocol):
    pass


def observe_application_cnpg_fence_retirement(
    plan: FinalGatePlan, *, journal: ProtectedApplyJournal, component: ProtectedApplyComponent,
    ordinal: int, runner: CNPGFenceAcquisitionRunner,
) -> str:
    """Read original retired objects and actual admission propagation, without writes.

    This is the fence portion of classification only. The complete component
    independently observes current database/credential/workload restoration and
    continuous original guard/writer authority before deriving its terminal.
    """
    saved = journal.read_application_cnpg_fence_retirement_view(plan, component, ordinal=ordinal)
    if saved is None:
        raise RuntimeError("CNPG fence retirement decision is absent")
    request, inventory, digest = saved
    _observe_retired_inventory(request, inventory, runner)
    if journal.read_application_cnpg_fence_retirement_view(plan, component, ordinal=ordinal) != saved:
        raise RuntimeError("CNPG fence retirement records changed during observation")
    return digest


def _observe_retired_inventory(
    request: CNPGFenceRequest, inventory: tuple[tuple[CNPGFenceCreateIntent, CNPGFenceObjectReceipt], ...],
    runner: CNPGFenceAcquisitionRunner,
) -> None:
    def inspect() -> None:
        if tuple(receipt.ordinal for _, receipt in inventory) != tuple(range(len(request.documents()))):
            raise RuntimeError("CNPG fence retired inventory is incomplete")
        for pending, receipt in inventory:
            document = pending.document(request)
            observed = _read(runner, document)
            if document["kind"] == "ValidatingAdmissionPolicy":
                if prepare_cnpg_fence_retirement_patch(request=request, pending=pending,
                        receipt=receipt, observed=observed) is not None:
                    raise RuntimeError("CNPG fence policy is not at its exact retired endpoint")
            else:
                _inspect(request, pending, observed, expected_uid=receipt.uid)
    inspect()
    for _, argv, payload in cnpg_input_fence_probe_commands(
        intent_digest=request.intent_digest, target_pooler_names=request.target_pooler_names,
    ):
        if payload is None:
            runner.capture_stdout(argv, env=runner.environment, timeout_seconds=30)
        else:
            runner.capture_stdout_with_input(argv, env=runner.environment, input_payload=payload, timeout_seconds=30)
    inspect()


def retire_cnpg_input_fence(
    plan: FinalGatePlan, *, journal: ProtectedApplyJournal, runner: CNPGFenceRetirementRunner,
    guard: MutationGuardEvidence,
) -> tuple[CNPGFenceObjectReceipt, ...]:
    """Disable policy matching while retaining ALL original names and UIDs.

    The admitted enclosing component excludes external policy/process/SQL writers
    continuously. Every entry repeats actual restoration observation and durable
    publication. Ambiguous PATCH replies propagate; a retry reconciles the exact
    active or retired endpoints, never deleting, recreating or reactivating them.
    This phase does not release the database guard or publish a component terminal.
    """
    _live_guard(plan, journal, runner, guard)
    journal.observe_and_record_application_restoration(plan, runner=runner, guard=guard)
    request = journal.read_application_cnpg_fence(plan)
    if request is None:
        raise RuntimeError("CNPG fence retirement requires its original durable request")
    records = []
    for ordinal, document in enumerate(request.documents()):
        pending = journal.read_application_cnpg_fence_create(plan, ordinal=ordinal)
        receipt = journal.read_application_cnpg_fence_object(plan, ordinal=ordinal)
        if pending is None or receipt is None:
            raise RuntimeError("CNPG fence retirement lacks original create and object receipts")
        records.append((document, pending, receipt))

    def inspect_all() -> None:
        # Validate the entire inventory before the first patch.
        for document, pending, receipt in records:
            payload = _read(runner, document)
            if document["kind"] == "ValidatingAdmissionPolicy":
                prepare_cnpg_fence_retirement_patch(request=request, pending=pending,
                                                    receipt=receipt, observed=payload)
            else:
                _inspect(request, pending, payload, expected_uid=receipt.uid)

    inspect_all()
    journal.begin_application_cnpg_fence_retirement(plan, guard=guard)
    for document, pending, receipt in records:
        if document["kind"] != "ValidatingAdmissionPolicy":
            continue
        _live_guard(plan, journal, runner, guard)
        patch = prepare_cnpg_fence_retirement_patch(request=request, pending=pending,
                                                   receipt=receipt, observed=_read(runner, document))
        if patch is not None:
            name = _mapping(document["metadata"])["name"]
            assert isinstance(name, str)
            runner.capture_stdout_with_input(
                ("kubectl", "patch", _RESOURCES["ValidatingAdmissionPolicy"], name,
                 "--type=json", "--patch-file=/dev/stdin", "--field-manager=loom-cnpg-fence",
                 "--show-managed-fields=true", "--output=json", "--request-timeout=30s"),
                env=runner.environment, input_payload=patch, timeout_seconds=30,
            )
        if prepare_cnpg_fence_retirement_patch(request=request, pending=pending,
                                              receipt=receipt, observed=_read(runner, document)) is not None:
            raise RuntimeError("CNPG fence retirement patch did not converge")
        _live_guard(plan, journal, runner, guard)
    _observe_retired_inventory(request, tuple((pending, receipt) for _, pending, receipt in records), runner)
    _live_guard(plan, journal, runner, guard)
    return tuple(receipt for _, _, receipt in records)


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
