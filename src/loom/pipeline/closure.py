"""Terminal-barrier closure truth and deterministic snapshot helpers (#1212)."""

from __future__ import annotations

from enum import StrEnum
from uuid import UUID

from loom.pipeline.gates import GateSelection, strict_and_gate_target
from loom.pipeline.keys import canonical_digest, canonical_document
from loom.pipeline.spec import PipelineTerminalSnapshotDocumentV1, TerminalStageDescriptorV1
from loom.pipeline.state import PipelineStageRunState


class TerminalBarrierDecision(StrEnum):
    BLOCKED = "blocked"
    READY = "ready"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


def terminal_barrier_decision(
    *,
    gate_selections: list[GateSelection],
    declared_rows_closed: bool,
    all_expansion_markers_present: bool,
    active_attempt_or_retry: bool,
    pending_cancel_or_cleanup: bool,
    run_terminal_cause: str | None,
) -> TerminalBarrierDecision:
    if run_terminal_cause is not None:
        return TerminalBarrierDecision.CANCELLED
    gate = strict_and_gate_target(gate_selections)
    if gate is GateSelection.NOT_SELECTED:
        return TerminalBarrierDecision.SKIPPED
    if (
        gate is GateSelection.PENDING
        or not declared_rows_closed
        or not all_expansion_markers_present
        or active_attempt_or_retry
        or pending_cancel_or_cleanup
    ):
        return TerminalBarrierDecision.BLOCKED
    return TerminalBarrierDecision.READY


def build_terminal_snapshot(
    *,
    pipeline_run_id: UUID,
    run_graph_digest: str,
    snapshot_id: UUID,
    terminal_stage_keys: list[str],
    stages: list[TerminalStageDescriptorV1],
) -> tuple[PipelineTerminalSnapshotDocumentV1, bytes, str]:
    document = PipelineTerminalSnapshotDocumentV1(
        schema_version="loom.pipeline-terminal-snapshot.v1",
        pipeline_run_id=pipeline_run_id,
        run_graph_digest=run_graph_digest,
        snapshot_id=snapshot_id,
        terminal_stage_keys=terminal_stage_keys,
        stages=stages,
    )
    return document, canonical_document(document), canonical_digest(document)


def descriptor_state(state: PipelineStageRunState) -> str:
    if state not in {
        PipelineStageRunState.SUCCEEDED,
        PipelineStageRunState.FAILED,
        PipelineStageRunState.SKIPPED,
    }:
        raise ValueError("terminal snapshot cannot contain an open/cancelled StageRun")
    return state.value
