import unittest
from datetime import datetime, timezone

from agentic_data_platform.domain.run_records import (
    ArtifactKind,
    ArtifactRef,
    BenchmarkTaskInstance,
    JudgeConfig,
    ModelConfig,
    ModelMode,
    RunnerConfig,
    RunnerKind,
    RunRecord,
    RunStatus,
    SandboxBackend,
)
from agentic_data_platform.evaluation.mock import MockEvaluatorAdapter
from agentic_data_platform.evaluation.types import EvaluatorInput


class MockEvaluatorTest(unittest.TestCase):
    def test_mock_evaluator_returns_completed_feedback_with_metrics_and_judge_metadata(self):
        run = _minimal_run()
        trajectory_ref = _artifact(
            artifact_id="run_001-trajectory",
            kind=ArtifactKind.TRAJECTORY,
            uri="minio://runs/run_001/tasks/task_001/trajectory/trajectory.jsonl",
        )
        workspace_ref = _artifact(
            artifact_id="run_001-workspace",
            kind=ArtifactKind.WORKSPACE_SNAPSHOT,
            uri="minio://runs/run_001/tasks/task_001/workspace/snapshot.json",
        )
        skill_ref = _artifact(
            artifact_id="continuous-skill-object",
            kind=ArtifactKind.SKILL_OBJECT,
            uri="minio://runs/run_001/skills/skill-vector.bin",
        )

        evaluator_input = EvaluatorInput.from_run(
            run,
            trajectory_ref=trajectory_ref,
            workspace_ref=workspace_ref,
            artifact_refs=[skill_ref],
            rubric_id="latent-skill-v0",
            judge=JudgeConfig(
                provider="mock",
                model_name="deterministic-judge",
                rubric_version="latent-skill-v0",
                metadata={"temperature": 0},
            ),
        )

        result = MockEvaluatorAdapter(
            evaluator_id="mock-judge-v0",
            score=0.82,
            verbal_feedback="Mock feedback: workspace and trajectory are available for review.",
        ).evaluate(evaluator_input)

        self.assertEqual(result.status, "completed")
        self.assertEqual(result.score, 0.82)
        self.assertTrue(result.metrics["task_success"])
        self.assertEqual(result.metrics["artifact_count"], 3)
        self.assertEqual(result.metrics["trajectory_available"], 1.0)
        self.assertEqual(result.metrics["workspace_available"], 1.0)
        self.assertEqual(result.judge.model_name, "deterministic-judge")
        self.assertEqual(result.judge.metadata["temperature"], 0)
        self.assertIn("trajectory", result.artifact_refs[0])
        self.assertTrue(any(ref.endswith("skill-vector.bin") for ref in result.artifact_refs))

    def test_failed_mock_evaluator_result_can_be_attached_to_run_record(self):
        run = _minimal_run()
        run.transition_to(RunStatus.PROVISIONING)
        run.transition_to(RunStatus.RUNNING)
        run.transition_to(RunStatus.EVALUATING)

        evaluator_input = EvaluatorInput.from_run(
            run,
            trajectory_ref=_artifact("run_001-trajectory", ArtifactKind.TRAJECTORY),
            workspace_ref=_artifact("run_001-workspace", ArtifactKind.WORKSPACE_SNAPSHOT),
            artifact_refs=[],
            rubric_id="latent-skill-v0",
            judge=JudgeConfig(
                provider="mock",
                model_name="deterministic-judge",
                rubric_version="latent-skill-v0",
            ),
        )

        result = MockEvaluatorAdapter(
            evaluator_id="mock-judge-v0",
            failure_reason="judge prompt could not be rendered",
        ).evaluate(evaluator_input)
        run.attach_evaluator_result(result)
        run.transition_to(RunStatus.FAILED)

        payload = run.to_dict()

        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["evaluator_result"]["status"], "failed")
        self.assertIsNone(payload["evaluator_result"].get("score"))
        self.assertFalse(payload["evaluator_result"]["metrics"]["task_success"])
        self.assertEqual(payload["evaluator_result"]["failure_reason"], "judge prompt could not be rendered")

    def test_evaluator_input_serializes_task_workspace_trajectory_rubric_and_artifacts(self):
        run = _minimal_run()
        trajectory_ref = _artifact("run_001-trajectory", ArtifactKind.TRAJECTORY)
        workspace_ref = _artifact("run_001-workspace", ArtifactKind.WORKSPACE_SNAPSHOT)
        generated_ref = _artifact("answer-xlsx", ArtifactKind.GENERATED_FILE)

        evaluator_input = EvaluatorInput.from_run(
            run,
            trajectory_ref=trajectory_ref,
            workspace_ref=workspace_ref,
            artifact_refs=[generated_ref],
            rubric_id="latent-skill-v0",
            judge=JudgeConfig(
                provider="mock",
                model_name="deterministic-judge",
                rubric_version="latent-skill-v0",
            ),
            metadata={"workspace_file_count": 4},
        )

        payload = evaluator_input.to_dict()

        self.assertEqual(payload["run_id"], "run_001")
        self.assertEqual(payload["task"]["benchmark_suite"], "SkillFlow")
        self.assertEqual(payload["trajectory_ref"]["kind"], "trajectory")
        self.assertEqual(payload["workspace_ref"]["kind"], "workspace_snapshot")
        self.assertEqual(payload["artifact_refs"][0]["kind"], "generated_file")
        self.assertEqual(payload["rubric_id"], "latent-skill-v0")
        self.assertEqual(payload["judge"]["provider"], "mock")
        self.assertEqual(payload["metadata"]["workspace_file_count"], 4)


def _minimal_run() -> RunRecord:
    created = datetime(2026, 5, 28, 12, 0, 0, tzinfo=timezone.utc)
    return RunRecord(
        run_id="run_001",
        project_id="pilot-project",
        owner_team="pilot group",
        task=BenchmarkTaskInstance(
            benchmark_suite="SkillFlow",
            benchmark_version="hf:zhang-ziao/SkillFlow-Task@2026-05-28",
            task_family="receipt-to-spreadsheet",
            instance_id="conference-expense-03",
            source_uri="https://huggingface.co/datasets/zhang-ziao/SkillFlow-Task",
            input_artifact_refs=["minio://benchmarks/skillflow/input.tar.zst"],
            required_artifacts=["trajectory", "workspace_snapshot", "evaluator_report"],
            metadata={"expected_output": "receipts.xlsx"},
        ),
        model=ModelConfig(
            provider="openai",
            model_name="gpt-5",
            mode=ModelMode.API,
            prompt_template_version="terminal-agent-v0",
        ),
        runner=RunnerConfig(
            kind=RunnerKind.ORIGINAL_BENCHMARK,
            sandbox_backend=SandboxBackend.DOCKER_TERMINAL,
            image="python:3.12-slim",
            entrypoint=["python", "-m", "skillflow.runner"],
            internet_access=True,
            resource_limits={"cpu": 2, "memory_gib": 8, "timeout_seconds": 3600},
        ),
        status=RunStatus.QUEUED,
        created_at=created,
        updated_at=created,
    )


def _artifact(
    artifact_id: str,
    kind: ArtifactKind,
    uri: str | None = None,
) -> ArtifactRef:
    return ArtifactRef(
        artifact_id=artifact_id,
        kind=kind,
        uri=uri or f"minio://runs/run_001/tasks/task_001/{artifact_id}",
        media_type="application/json",
        sha256="1" * 64,
        size_bytes=128,
        metadata={"storage_key": f"runs/run_001/tasks/task_001/{artifact_id}"},
    )
