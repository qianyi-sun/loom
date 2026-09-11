"""Exact release witnesses for reclaiming executor lifecycle dependencies."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from uuid import UUID

from loom_capacity_executor.journal import JournalRecord, JournalRegressionError
from loom_capacity_executor.launch_facts_journal import LaunchFactsReferenceV3
from loom_capacity_executor.slurm_contracts import SlurmCancelRequestV2
from loom_capacity_manager.executable_contracts import (
    ExecutableIntentBindingV2,
    ExecutablePartialReleaseV2,
    ExecutableReservationAcceptanceV2,
    canonical_executable_bytes,
)

if TYPE_CHECKING:
    from loom_capacity_executor.executable import ExecutablePoolExecutor


def lifecycle_retention(
    executor: ExecutablePoolExecutor, records: tuple[JournalRecord, ...], command_high_water: int,
) -> tuple[set[int], set[UUID], int]:
    """Drop only exact released identities, retaining unknown dependencies closed.

    Durable release confirmation and manager command high-water are both
    necessary. The ensuing checkpoint acknowledgement commits the entire
    selected history before the old journal generation can disappear.
    """
    released: dict[UUID, tuple[ExecutableIntentBindingV2, int]] = {}
    for record in records:
        if record.event_kind != "reservation-release-confirmed":
            continue
        payload = record.durable_payload()
        if payload is None:
            raise JournalRegressionError("release retention lacks durable command bytes")
        release = ExecutablePartialReleaseV2.model_validate_json(payload)
        executor._assert_execution(release.execution)
        if (record.object_kind != "tranche" or record.object_id != str(release.tranche_id)
            or canonical_executable_bytes(release) != payload
            or release.command_sequence > command_high_water):
            raise JournalRegressionError("release retention command high-water or binding changed")
        for item in release.releases:
            executor._assert_binding(item.binding)
            prior = released.get(item.binding.intent_id)
            if prior is not None and prior[0] != item.binding:
                raise JournalRegressionError("release retention has conflicting intent bindings")
            released[item.binding.intent_id] = (item.binding, record.sequence)

    def reclaimable(binding: ExecutableIntentBindingV2, record: JournalRecord) -> bool:
        executor._assert_binding(binding)
        witness = released.get(binding.intent_id)
        if witness is None:
            return False
        if witness[0] != binding or record.sequence > witness[1]:
            raise JournalRegressionError("released lifecycle binding changed or reopened")
        return True

    selected: set[int] = set()
    fact_ids: set[str] = set()
    active_jobs: set[UUID] = set()
    envelopes = []
    for record in records:
        if record.object_kind != "job" or record.event_kind.startswith("pending-cancel-"):
            continue
        envelope = executor._launch_envelope_from_record(UUID(record.object_id), record)
        envelopes.append(envelope)
        binding = envelope.rendered.ownership_proof.metadata.binding
        if reclaimable(binding, record):
            continue
        selected.add(record.sequence)
        active_jobs.add(binding.intent_id)
        payload = record.durable_payload()
        assert payload is not None
        value = json.loads(payload)
        if "launch_facts" in value:
            reference = LaunchFactsReferenceV3.model_validate(value["launch_facts"])
            fact_ids.update(f"launch-facts:{reference.sha256}:{index}"
                for index in range(reference.chunk_count))

    for record in records:
        if record.object_kind == "job" and not record.event_kind.startswith("pending-cancel-"):
            continue
        if record.event_kind == "launch-facts-retained":
            if record.object_kind != "executor" or not record.object_id.startswith("launch-facts:"):
                raise JournalRegressionError("unsupported launch facts retention binding")
            if record.object_id in fact_ids:
                selected.add(record.sequence)
            continue
        payload = record.durable_payload()
        if payload is None:
            raise JournalRegressionError("lifecycle retention lacks durable payload")
        if record.object_kind == "tranche":
            model = (ExecutablePartialReleaseV2 if record.event_kind.startswith("reservation-release-")
                else ExecutableReservationAcceptanceV2)
            command = model.model_validate_json(payload)
            executor._assert_execution(command.execution)
            if (record.object_id != str(command.tranche_id)
                or command.executor_id != executor.registration.executor_id
                or command.executor_incarnation != executor.registration.executor_incarnation
                or canonical_executable_bytes(command) != payload):
                raise JournalRegressionError("tranche retention command binding changed")
            # No runtime reader consumes resolved tranche RPC history. An
            # unconsumed rejection remains until the manager has passed it.
            if command.command_sequence > command_high_water:
                selected.add(record.sequence)
            continue
        if record.event_kind.startswith("pending-cancel-"):
            cancel = SlurmCancelRequestV2.model_validate_json(payload)
            if record.object_kind != "job" or record.object_id != cancel.job_id:
                raise JournalRegressionError("cancellation retention physical identity changed")
            matches = {}
            for envelope in envelopes:
                physical = executor._physical_binding(envelope)
                if physical is not None and executor._cancel_request_from_physical(envelope, physical) == cancel:
                    binding = envelope.rendered.ownership_proof.metadata.binding
                    matches[binding.intent_id] = binding
            if len(matches) != 1:
                raise JournalRegressionError("cancellation retention ownership is ambiguous")
            if not reclaimable(next(iter(matches.values())), record):
                selected.add(record.sequence)
            continue
        value = json.loads(payload)
        if not isinstance(value, dict) or "binding" not in value:
            raise JournalRegressionError("unsupported lifecycle dependency payload")
        binding = ExecutableIntentBindingV2.model_validate_json(json.dumps(value["binding"]))
        expected_id = (f"physical-bind:{binding.intent_id}" if record.object_kind == "executor"
            else str(binding.intent_id))
        if (record.object_kind not in {"intent", "executor", "bootstrap", "prepared-revocation"}
            or record.object_id != expected_id):
            raise JournalRegressionError("lifecycle retention object binding changed")
        if not reclaimable(binding, record):
            selected.add(record.sequence)
            active_jobs.add(binding.intent_id)
        elif record.object_kind == "prepared-revocation":
            latest = executor.journal.latest("prepared-revocation", record.object_id)
            if latest is None or latest.event_kind != "prepared-handoff-deleted":
                raise JournalRegressionError("released lifecycle still needs prepared handoff cleanup")
    return selected, set(released), len(active_jobs)
