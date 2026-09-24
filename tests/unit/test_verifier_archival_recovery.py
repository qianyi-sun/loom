"""Retain failed historical verifier evidence without inventing a runtime score."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from loom.execution_runtime_contract import ExecutionRuntimeResultV1
from loom.models.result import ExceptionInfo
from loom_control_plane.service_execution_materializer import (
    MaterializationIntegrityError,
    build_canonical_events,
)
from tests.unit.test_service_execution_materialization import (
    _REVISION,
    _RUNTIME_IMAGE,
    _TASK_IMAGE,
    _task,
    _trial,
)


def legacy_result():
    now = datetime.now(UTC)
    log = {"path": "02-verifier.stdout", "sha256": "sha256:" + "4" * 64,
           "bytes_seen": 0, "bytes_saved": 0, "truncated": False}
    outputs = [
        {"source_path": ".loom/verifier/exception.json", "relative_path": "diagnostics/verifier-exception.json",
         "kind": "verifier", "required": False, "state": "captured", "size_bytes": 140, "sha256": "sha256:" + "5" * 64},
        {"source_path": ".loom/verifier/output.json", "relative_path": "verifier/output.json",
         "kind": "verifier", "required": True, "state": "captured", "size_bytes": 30, "sha256": "sha256:" + "6" * 64},
    ]
    return ExecutionRuntimeResultV1.model_validate({
        "schema_version": "loom.execution-runtime-result.v1", "runtime_contract_sha256": "sha256:" + "1" * 64,
        "candidate_sha": "1" * 40, "task_revision_sha256": _REVISION,
        "command_identity_sha256": "sha256:" + "2" * 64, "execution_role": "attempt",
        "container_roles": ["execution", "agent", "verifier"], "task_image_ref": _TASK_IMAGE,
        "runtime_image_ref": _RUNTIME_IMAGE, "runtime_binary_sha256": "sha256:" + "3" * 64,
        "execution_class_id": "linux-amd64-cpu-pod-v1", "status": "verifier_error",
        "started_at": now, "finished_at": now + timedelta(seconds=2),
        "phases": [{"role": "verifier", "ordinal": 1, "started_at": now, "finished_at": now + timedelta(seconds=2),
                    "exit_code": 1, "signal": None, "timed_out": False, "stdout": log,
                    "stderr": {**log, "path": "02-verifier.stderr"}}],
        "outputs": outputs, "verifier_rewards": None, "partial_evidence": True,
    })


def build(runtime, *, exception=True, verifier_body=b'{"rewards":{"passed":0}}'):
    return build_canonical_events(
        trial_id=uuid4(), task_id="task-1", task_config=_task(), trial_config=_trial(),
        runtime_result=runtime, trace_body=None, verifier_body=verifier_body,
        exception_info=ExceptionInfo(exception_type="ServiceExecutionTaskError",
            exception_message="isolated verifier process failed", occurred_at=runtime.finished_at) if exception else None,
    )


def test_legacy_diagnostic_before_score_preserves_failed_outcome_and_original_null_reward():
    runtime = legacy_result()
    before = runtime.model_dump_json()
    events = build(runtime)
    verifier = next(event for event in events if event.kind.value == "verifier_end")
    assert verifier.result.rewards == {"passed": 0}
    assert events[-1].final_state == "failed" and events[-1].reward is None
    assert any(event.kind.value == "trial_error" for event in events)
    assert runtime.model_dump_json() == before


@pytest.mark.parametrize("change", [
    "nonnull_score", "success", "not_partial", "diagnostic_after_score", "missing_diagnostic",
    "missing_exception", "successful_verifier_phase", "unrelated_diagnostic",
])
def test_legacy_recovery_does_not_accept_other_reward_drift(change):
    runtime = legacy_result()
    if change == "nonnull_score":
        runtime = runtime.model_copy(update={"verifier_rewards": {"passed": 1}})
    elif change == "success":
        runtime = runtime.model_copy(update={"status": "succeeded"})
    elif change == "not_partial":
        runtime = runtime.model_copy(update={"partial_evidence": False})
    elif change == "diagnostic_after_score":
        runtime = runtime.model_copy(update={"outputs": tuple(reversed(runtime.outputs))})
    elif change == "missing_diagnostic":
        runtime = runtime.model_copy(update={"outputs": runtime.outputs[1:]})
    elif change == "successful_verifier_phase":
        runtime = runtime.model_copy(update={"phases": (runtime.phases[0].model_copy(update={"exit_code": 0}),)})
    elif change == "unrelated_diagnostic":
        runtime = runtime.model_copy(update={"outputs": (runtime.outputs[0].model_copy(
            update={"relative_path": "diagnostics/other.json"}), runtime.outputs[1])})
    with pytest.raises(MaterializationIntegrityError, match="verifier_reward_drift"):
        build(runtime, exception=change != "missing_exception")


def test_legacy_recovery_requires_the_captured_verifier_score_body():
    with pytest.raises(MaterializationIntegrityError, match="verifier_output_missing"):
        build(legacy_result(), verifier_body=None)
