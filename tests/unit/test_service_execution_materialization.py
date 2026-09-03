from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from loom.execution_runtime_contract import ExecutionRuntimeResultV1
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.service_execution_materialization import (
    ServiceExecutionRuntimeProfileV1,
    automatic_service_execution_rejections,
    build_service_execution_input_manifest,
    compile_service_execution_plan,
)
from loom.trajectory.storage import FakeObjectStore
from loom_control_plane.service_execution_materializer import (
    MaterializationIntegrityError,
    ServiceExecutionMaterializer,
    build_canonical_events,
    run_service_execution_materializer_loop,
)
from tests.support.execution_image_admission import signed_image_admission_bundle

_TASK_IMAGE = "registry.example/task@sha256:" + "a" * 64
_RUNTIME_IMAGE = "registry.example/runtime@sha256:" + "b" * 64
_REVISION = "sha256:" + "c" * 64


def _task(**updates: object) -> TaskConfig:
    raw: dict[str, object] = {
        "schema_version": "1",
        "task": {"id": "task-1", "name": "Automatic Nebius task"},
        "environment": {
            "os": "linux",
            "cpu_arch": "x86_64",
            "gpu_vendor": "none",
            "docker_image": _TASK_IMAGE,
            "cpus": 1,
            "memory_mb": 1024,
            "storage_mb": 2048,
            "network_policies_supported": ["gateway-only"],
            "baseline_network_policy": {"kind": "gateway-only"},
        },
        "agent": {"name": "direct-completion"},
        "verifier": {
            "name": "script",
            "args": {"script_path": "verifier/check.sh"},
        },
        "steps": [
            {
                "name": "main",
                "instruction_file": "instruction.md",
                "artifacts": ["answer.txt"],
            }
        ],
    }
    raw.update(updates)
    return TaskConfig.model_validate(raw)


def _trial() -> TrialConfig:
    return TrialConfig(
        agent_name="direct-completion",
        agent_model=ModelSpec(provider="openai", name="gpt-5"),
        request_params={"temperature": 0.2},
    )


def _provenance() -> dict[str, object]:
    return {
        "service_execution_input": {
            "schema_version": "loom.service-execution-input.v1",
            "manifest_uri": "s3://artifacts/task-inputs/task-1.json",
            "manifest_sha256": "sha256:" + "d" * 64,
            "file_count": 3,
            "total_bytes": 4096,
        }
    }


def _profile() -> ServiceExecutionRuntimeProfileV1:
    return ServiceExecutionRuntimeProfileV1(
        candidate_sha="1" * 40,
        execution_class_id="linux-amd64-cpu-pod-v1",
        task_image_ref=_TASK_IMAGE,
        runtime_image_ref=_RUNTIME_IMAGE,
        runtime_binary_sha256="sha256:" + "e" * 64,
        image_admission=signed_image_admission_bundle((_TASK_IMAGE, _RUNTIME_IMAGE)),
    )


def test_input_manifest_is_canonical_and_preserves_executable_mode(tmp_path: Path) -> None:
    (tmp_path / "instruction.md").write_text("hello\n", encoding="utf-8")
    script = tmp_path / "verifier" / "check.sh"
    script.parent.mkdir()
    script.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    script.chmod(0o755)

    manifest = build_service_execution_input_manifest(
        tmp_path,
        task_checksum=_REVISION,
    )

    assert [item.relative_path for item in manifest.files] == [
        "instruction.md",
        "verifier/check.sh",
    ]
    assert [item.mode for item in manifest.files] == ["0644", "0755"]
    assert json.loads(manifest.canonical_bytes()) == manifest.model_dump(mode="json")


def test_ordinary_task_compiles_to_profile_owned_nebius_plan() -> None:
    task = _task()
    trial = _trial()

    plan = compile_service_execution_plan(
        task=task,
        trial=trial,
        task_revision_sha256=_REVISION,
        source_provenance=_provenance(),
        profile=_profile(),
    )

    assert plan.execution_class_id == "linux-amd64-cpu-pod-v1"
    assert plan.task_input is not None
    assert plan.task_input.file_count == 3
    assert plan.main.argv == (
        "python",
        "-m",
        "loom.service_execution_task",
        "direct-completion",
    )
    assert plan.main.environment["LOOM_TASK_MODEL"] == "openai/gpt-5"
    assert plan.verifier is not None
    assert plan.verifier.argv == ("/bin/sh", "verifier/check.sh")
    assert plan.task_resources.cpu_millis == 1000
    assert plan.workspace_mib == 2048
    assert [item.model_dump(mode="json") for item in plan.output_declarations] == [
        {
            "source_path": "answer.txt",
            "relative_path": "artifacts/answer.txt",
            "kind": "task_artifact",
            "required": True,
        },
        {
            "source_path": ".loom/agent/trajectory.jsonl",
            "relative_path": "trajectory/events.jsonl",
            "kind": "trajectory",
            "required": True,
        },
        {
            "source_path": ".loom/agent/usage.json",
            "relative_path": "accounting/usage.json",
            "kind": "usage",
            "required": True,
        },
        {
            "source_path": ".loom/verifier/output.json",
            "relative_path": "verifier/output.json",
            "kind": "verifier",
            "required": True,
        },
    ]


def test_ordinary_task_preserves_complete_multi_artifact_and_trace_contract() -> None:
    raw = _task().model_dump(mode="json")
    raw["steps"][0]["artifacts"] = ["answer.txt"]
    raw["steps"][0]["required_artifacts"] = ["answer.txt", "reasoning.md"]
    plan = compile_service_execution_plan(
        task=TaskConfig.model_validate(raw),
        trial=_trial(),
        task_revision_sha256=_REVISION,
        source_provenance=_provenance(),
        profile=_profile(),
    )

    assert plan.main.environment["LOOM_TASK_ARTIFACTS_JSON"] == ('["answer.txt","reasoning.md"]')
    assert [item.relative_path for item in plan.output_declarations] == [
        "artifacts/answer.txt",
        "artifacts/reasoning.md",
        "trajectory/events.jsonl",
        "accounting/usage.json",
        "verifier/output.json",
    ]
    assert all(item.required for item in plan.output_declarations)
    assert plan.verifier_execution == "in_attempt"


def test_automatic_compiler_rejects_unimplemented_task_semantics() -> None:
    task = _task(
        environment={
            **_task().environment.model_dump(mode="json"),
            "environment": {"UNHANDLED": "value"},
        }
    )

    assert automatic_service_execution_rejections(
        task,
        _trial(),
        source_provenance={},
    ) == (
        "immutable_task_input_unavailable",
        "extended_environment_unsupported",
    )


def test_manifest_digest_matches_persisted_binding_bytes(tmp_path: Path) -> None:
    (tmp_path / "task.toml").write_text("schema_version = '1'\n", encoding="utf-8")
    manifest = build_service_execution_input_manifest(tmp_path, task_checksum=_REVISION)
    body = manifest.canonical_bytes()
    assert "sha256:" + hashlib.sha256(body).hexdigest() == (
        "sha256:" + hashlib.sha256(manifest.canonical_bytes()).hexdigest()
    )


def test_complete_source_trace_projects_to_training_grade_loom_events() -> None:
    now = datetime.now(UTC)
    plan = compile_service_execution_plan(
        task=_task(),
        trial=_trial(),
        task_revision_sha256=_REVISION,
        source_provenance=_provenance(),
        profile=_profile(),
    )
    payloads = {
        "artifacts/answer.txt": b"42\n",
        "trajectory/events.jsonl": b"trace\n",
        "accounting/usage.json": b"{}\n",
        "verifier/output.json": b'{"rewards":{"passed":1.0}}',
    }
    runtime_result = ExecutionRuntimeResultV1.model_validate(
        {
            "schema_version": "loom.execution-runtime-result.v1",
            "runtime_contract_sha256": "sha256:" + "f" * 64,
            "candidate_sha": plan.candidate_sha,
            "task_revision_sha256": plan.task_revision_sha256,
            "command_identity_sha256": plan.command_identity_sha256,
            "execution_role": "attempt",
            "container_roles": ["execution", "agent", "verifier"],
            "task_image_ref": plan.task_image_ref,
            "runtime_image_ref": plan.runtime_image_ref,
            "runtime_binary_sha256": plan.runtime_binary_sha256,
            "execution_class_id": plan.execution_class_id,
            "status": "succeeded",
            "started_at": now,
            "finished_at": now + timedelta(seconds=2),
            "phases": [],
            "outputs": [
                {
                    **declaration.model_dump(mode="json"),
                    "state": "captured",
                    "size_bytes": len(payloads[declaration.relative_path]),
                    "sha256": "sha256:"
                    + hashlib.sha256(payloads[declaration.relative_path]).hexdigest(),
                }
                for declaration in plan.output_declarations
            ],
            "verifier_rewards": {"passed": 1.0},
            "partial_evidence": False,
        }
    )
    trace = (
        json.dumps(
            {
                "schema_version": "loom.service-execution-llm-call.v1",
                "turn": 0,
                "started_at": now.isoformat(),
                "finished_at": (now + timedelta(seconds=1)).isoformat(),
                "model": "openai/gpt-5",
                "request": {
                    "messages": [{"role": "user", "content": "Solve it"}],
                    "request_params": {"temperature": 0.2},
                },
                "response": {"role": "assistant", "content": "42"},
                "usage": {
                    "rate_card_hash": "rate-card-v1",
                    "gateway_request_id": "request-1",
                    "finish_reason": "stop",
                    "input_tokens": 2,
                    "cached_input_tokens": 0,
                    "cache_write_tokens": 0,
                    "output_tokens": 1,
                    "thinking_tokens": 0,
                    "provider_extras": {"accepted_prediction_tokens": 0},
                    "cost_usd": 0.01,
                    "duration_sec": 1.0,
                    "streamed": False,
                    "time_to_first_token_sec": None,
                    "attempt": 1,
                },
            },
            separators=(",", ":"),
        )
        + "\n"
    ).encode()

    events = build_canonical_events(
        trial_id=uuid4(),
        task_id="task-1",
        task_config=_task(),
        trial_config=_trial(),
        runtime_result=runtime_result,
        trace_body=trace,
        verifier_body=payloads["verifier/output.json"],
    )

    assert [event.seq for event in events] == list(range(len(events)))
    assert [event.kind.value for event in events] == [
        "trial_start",
        "step_start",
        "llm_call",
        "step_end",
        "verifier_start",
        "verifier_end",
        "trial_end",
    ]
    llm_call = events[2]
    assert llm_call.kind.value == "llm_call"
    assert llm_call.model_dump()["messages"] == [
        {
            "role": "user",
            "content": "Solve it",
            "name": None,
            "tool_calls": None,
            "tool_call_id": None,
        }
    ]
    assert llm_call.model_dump()["response"]["content"] == "42"
    assert llm_call.model_dump()["input_tokens"] == 2
    assert events[-1].model_dump()["reward"] == {"passed": 1.0}


async def test_materializer_rejects_source_digest_mismatch() -> None:
    store = FakeObjectStore(objects={("source", "bundle/file"): b"tampered"})
    materializer = ServiceExecutionMaterializer(
        session_factory=None,  # type: ignore[arg-type]
        source_store=store,
        source_bucket="source",
        canonical_store=store,
        artifacts_bucket="canonical",
        trajectories_bucket="trajectories",
    )

    with pytest.raises(MaterializationIntegrityError, match="source_object_digest_mismatch"):
        await materializer._read_exact(
            key="bundle/file",
            expected="sha256:" + hashlib.sha256(b"expected").hexdigest(),
            size=len(b"expected"),
        )


async def test_materializer_loop_recovers_after_control_database_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RecoveringMaterializer:
        run_calls = 0
        cleanup_calls = 0
        metric_calls = 0

        async def run_once(self) -> bool:
            self.run_calls += 1
            if self.run_calls == 1:
                raise OSError("database temporarily unavailable")
            return False

        async def cleanup_source_once(self) -> bool:
            self.cleanup_calls += 1
            return False

        async def refresh_metrics(self) -> None:
            self.metric_calls += 1

    materializer = RecoveringMaterializer()
    sleep_calls = 0

    async def retry_sleep(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(
        "loom_control_plane.service_execution_materializer.asyncio.sleep",
        retry_sleep,
    )
    with pytest.raises(asyncio.CancelledError):
        await run_service_execution_materializer_loop(
            materializer=materializer,  # type: ignore[arg-type]
            interval_seconds=0.01,
        )
    assert materializer.run_calls == 2
    assert materializer.cleanup_calls == 1
    assert materializer.metric_calls == 1
