from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import PurePosixPath
from uuid import uuid4

import pytest

from loom.execution_runtime_contract import ExecutionRuntimeResultV1
from loom.models.trajectory import LLMCallEvent, Terminus2UserPromptEvent
from loom.service_execution_materialization import (
    automatic_service_execution_rejections,
    compile_service_execution_plan,
)
from loom.service_execution_terminus_trace import parse_terminus_events, terminus_usage
from loom_control_plane.service_execution_materializer import (
    MaterializationIntegrityError,
    build_canonical_events,
    validate_usage_accounting,
)
from tests.support.execution_image_admission import signed_image_admission_bundle
from tests.unit.test_service_execution_materialization import (
    _REVISION,
    _RUNTIME_IMAGE,
    _TASK_IMAGE,
    _profile,
    _provenance,
    _task,
    _trial,
)

_CONTROLLER = "registry.example/worker@sha256:" + "9" * 64


def _inputs():
    task = _task()
    task = task.model_copy(update={"environment": task.environment.model_copy(
        update={"workdir": PurePosixPath("/app")},
    )})
    trial = _trial().model_copy(update={"agent_name": "terminus-2"})
    profile = _profile().model_copy(update={
        "agent_image_ref": _CONTROLLER,
        "image_admission": signed_image_admission_bundle((_TASK_IMAGE, _RUNTIME_IMAGE, _CONTROLLER)),
    })
    return task, trial, profile


def test_terminus_plan_preserves_task_environment_and_has_fresh_private_verifier():
    task, trial, profile = _inputs()
    assert not automatic_service_execution_rejections(task, trial, source_provenance=_provenance())
    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    )
    assert plan.agent_image_ref == _CONTROLLER
    assert plan.task_image_ref == task.environment.docker_image
    assert [s.role_name for s in plan.sidecars] == ["task-sandbox", "verifier-sandbox"]
    assert all(s.private_sandbox and s.image_ref == _TASK_IMAGE for s in plan.sidecars)
    assert plan.main.argv[-1] == "terminus-2"
    assert plan.verifier.argv[-1] == "verify-sandbox"
    assert json.loads(plan.main.environment["LOOM_TASK_TRIAL_JSON"])["agent_name"] == "terminus-2"
    paths = {item.relative_path for item in plan.output_declarations}
    assert not next(item for item in plan.output_declarations
                    if item.relative_path == "artifacts/answer.txt").required
    assert {"trajectory/events.jsonl", "artifacts/harbor/trajectory.json",
            "artifacts/workspace.tar", "accounting/usage.json"} <= paths


def test_terminus_rejects_missing_controller_and_disabled_private_isolation():
    task, trial, _ = _inputs()
    with pytest.raises(ValueError, match="no Terminus controller"):
        compile_service_execution_plan(task=task, trial=trial, profile=_profile(),
            source_provenance=_provenance(), task_revision_sha256=_REVISION)
    reasons = automatic_service_execution_rejections(task, trial.model_copy(
        update={"workspace_staging_policy_name": "none"}), source_provenance=_provenance())
    assert "private_workspace_isolation_required" in reasons


def _events():
    _, trial, _ = _inputs()
    now = datetime.now(UTC)
    identity = uuid4()
    events = [Terminus2UserPromptEvent(emitted_at=now, trial_id=identity, step_id="agent", seq=0,
                                     prompt_id=str(uuid4()), harbor_step_id=1,
                                     message="Solve the original task")]
    events.append(LLMCallEvent(
        emitted_at=now, trial_id=identity, step_id="agent", seq=1,
        model=trial.agent_model, rate_card_hash="rate-v1", system_prompt=None,
        messages=[{"role": "user", "content": "Solve"}],
        response={"role": "assistant", "content": "done"}, finish_reason="stop",
        input_tokens=5, output_tokens=3, cached_input_tokens=0, cache_write_tokens=0,
        thinking_tokens=0, provider_extras={}, request_params=trial.request_params,
        cost_usd_snapshot=0.001, duration_sec=1, streamed=False, time_to_first_token_sec=None,
        gateway_request_id=str(uuid4()),
    ))
    return trial, identity, events


def test_typed_trace_keeps_native_events_and_real_call_accounting():
    trial, identity, events = _events()
    body = b"\n".join(e.model_dump_json().encode() for e in events) + b"\n"
    assert parse_terminus_events(body, trial=trial, trial_id=identity) == events
    usage = terminus_usage(events, trial)
    validate_usage_accounting(trace_body=body, usage_body=json.dumps(usage).encode(), trial_config=trial)
    result = ExecutionRuntimeResultV1.model_validate({
        "schema_version": "loom.execution-runtime-result.v1",
        "runtime_contract_sha256": "sha256:" + "1" * 64,
        "candidate_sha": "1" * 40, "task_revision_sha256": _REVISION,
        "command_identity_sha256": "sha256:" + "2" * 64,
        "execution_role": "attempt", "container_roles": ["execution", "agent", "verifier"],
        "task_image_ref": _TASK_IMAGE, "runtime_image_ref": _RUNTIME_IMAGE,
        "runtime_binary_sha256": "sha256:" + "3" * 64,
        "execution_class_id": "linux-amd64-cpu-pod-v1", "status": "succeeded",
        "started_at": events[0].emitted_at, "finished_at": events[-1].emitted_at,
        "phases": [], "outputs": [], "verifier_rewards": {"passed": 0}, "partial_evidence": False,
    })
    canonical = build_canonical_events(
        trial_id=identity, task_id="task-1", task_config=_inputs()[0], trial_config=trial,
        runtime_result=result, trace_body=body, verifier_body=b'{"rewards":{"passed":0}}',
    )
    assert canonical[2].kind == "terminus2_user_prompt"
    assert canonical[3].gateway_request_id == events[1].gateway_request_id
    assert canonical[-1].final_state == "succeeded"
    assert canonical[-1].reward == {"passed": 0}
    assert [e.seq for e in canonical] == list(range(len(canonical)))
    usage["totals"]["input_tokens"] += 1
    with pytest.raises(MaterializationIntegrityError, match="usage_output_identity_drift"):
        validate_usage_accounting(trace_body=body, usage_body=json.dumps(usage).encode(),
                                  trial_config=trial)
    with pytest.raises(ValueError, match="another Trial"):
        parse_terminus_events(body, trial=trial, trial_id=uuid4())
