import json
import unittest
from datetime import datetime, timezone

from agentic_data_platform.dashboard.projections import RunDashboardProjection
from agentic_data_platform.domain.run_records import (
    ArtifactKind,
    ArtifactRef,
    BenchmarkTaskInstance,
    EvaluatorResult,
    JudgeConfig,
    ModelConfig,
    ModelMode,
    RunnerConfig,
    RunnerKind,
    RunRecord,
    RunStatus,
    SandboxBackend,
)


class RunDashboardProjectionTest(unittest.TestCase):
    def test_projection_exposes_all_mvp_run_statuses(self):
        for status in [
            RunStatus.QUEUED,
            RunStatus.DISPATCHED,
            RunStatus.PROVISIONING,
            RunStatus.RUNNING,
            RunStatus.EVALUATING,
            RunStatus.SUCCEEDED,
            RunStatus.FAILED,
            RunStatus.CANCELED,
        ]:
            with self.subTest(status=status):
                run = _minimal_run(status=status)

                payload = RunDashboardProjection.from_run(run).to_dict()

                self.assertEqual(payload["status"], status.value)
                self.assertEqual(payload["progress"]["status"], status.value)
                self.assertEqual(
                    payload["progress"]["is_terminal"],
                    status in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELED},
                )
                self.assertEqual(payload["project"]["project_id"], "pilot-project")
                self.assertEqual(payload["task"]["instance_id"], "conference-expense-03")
                self.assertIsInstance(json.dumps(payload), str)

    def test_completed_run_projection_shows_score_feedback_metrics_and_artifacts(self):
        run = _minimal_run(status=RunStatus.SUCCEEDED)
        run.artifacts.extend(
            [
                _artifact("trajectory", ArtifactKind.TRAJECTORY, "minio://runs/run_001/trajectory.jsonl"),
                _artifact("workspace", ArtifactKind.WORKSPACE_SNAPSHOT, "file:///srv/private/workspace/snapshot.json"),
                _artifact("eval-report", ArtifactKind.EVALUATOR_REPORT, "file:///srv/private/eval/report.json"),
            ]
        )
        run.evaluator_result = EvaluatorResult(
            evaluator_id="mock-judge-v0",
            status="completed",
            score=0.91,
            metrics={"task_success": True, "trajectory_quality": 0.88},
            verbal_feedback="The generated spreadsheet is correct and includes every receipt.",
            judge=JudgeConfig(
                provider="mock",
                model_name="deterministic-judge",
                rubric_version="latent-skill-v0",
            ),
            artifact_refs=["file:///srv/private/eval/report.json"],
        )

        payload = RunDashboardProjection.from_run(run).to_dict()

        self.assertEqual(payload["evaluator"]["status"], "completed")
        self.assertEqual(payload["evaluator"]["score"], 0.91)
        self.assertTrue(payload["evaluator"]["metrics"]["task_success"])
        self.assertIn("spreadsheet", payload["evaluator"]["verbal_feedback_summary"])
        self.assertEqual(payload["evaluator"]["judge"]["rubric_version"], "latent-skill-v0")
        self.assertEqual(payload["progress"]["artifact_count"], 3)
        self.assertEqual(payload["artifacts"][0]["uri"], "minio://runs/run_001/trajectory.jsonl")
        self.assertNotIn("uri", payload["artifacts"][1])
        self.assertEqual(payload["artifacts"][1]["storage_key"], "runs/run_001/workspace")
        self.assertNotIn("/srv/private", json.dumps(payload))

    def test_projection_exposes_multiple_evaluator_results_with_latest_primary(self):
        run = _minimal_run(status=RunStatus.SUCCEEDED)
        run.evaluator_results = [
            EvaluatorResult(
                evaluator_id="harbor-verifier-v1",
                mode="harbor_verifier",
                status="completed",
                score=0.65,
                metrics={"reward": 0.65},
                verbal_feedback="",
                judge=None,
                artifact_refs=["file:///srv/private/eval/verifier.json"],
                metadata={"verifier_version": "harbor-2026-05-29", "access_token": "raw-secret"},
            ),
            EvaluatorResult(
                evaluator_id="llm-judge-v0",
                mode="llm_judge",
                status="completed",
                score=0.91,
                metrics={"task_success": True},
                verbal_feedback="The generated spreadsheet is correct and includes every receipt.",
                judge=JudgeConfig(
                    provider="openai",
                    model_name="gpt-5",
                    rubric_version="latent-skill-v0",
                    metadata={"judge_prompt_version": "v3"},
                ),
                artifact_refs=["https://storage.example/eval/judge.json?X-Amz-Signature=secret"],
            ),
        ]
        run.evaluator_result = run.evaluator_results[-1]

        payload = RunDashboardProjection.from_run(run).to_dict()
        rendered = json.dumps(payload)

        self.assertEqual(payload["evaluator"]["evaluator_id"], "llm-judge-v0")
        self.assertEqual([result["mode"] for result in payload["evaluator_results"]], ["harbor_verifier", "llm_judge"])
        self.assertEqual(payload["evaluator_results"][0]["metadata"]["verifier_version"], "harbor-2026-05-29")
        self.assertEqual(payload["evaluator_results"][0]["metadata"]["access_token"], "[redacted]")
        self.assertNotIn("judge", payload["evaluator_results"][0])
        self.assertEqual(payload["evaluator_results"][1]["judge"]["metadata"]["judge_prompt_version"], "v3")
        self.assertNotIn("/srv/private", rendered)
        self.assertNotIn("raw-secret", rendered)
        self.assertNotIn("X-Amz-Signature", rendered)

    def test_projection_exposes_safe_model_provider_usage_summary(self):
        run = _minimal_run(status=RunStatus.SUCCEEDED)
        run.evaluator_result = EvaluatorResult(
            evaluator_id="harbor-verifier-v1",
            mode="harbor_verifier",
            status="completed",
            score=1.0,
            metrics={"reward": 1.0},
            verbal_feedback="",
            judge=None,
            artifact_refs=[],
            metadata={
                "provider_usage": {
                    "schema_version": "model-provider-usage-v1",
                    "source": "harbor_atif_final_metrics",
                    "provider": "openai-compatible",
                    "model_name": "deepseek-v4-flash",
                    "input_tokens": 1000,
                    "output_tokens": 250,
                    "total_tokens": 1250,
                    "cost_usd": 0.0125,
                    "api_key": "sk-secret",
                }
            },
        )

        payload = RunDashboardProjection.from_run(run).to_dict()
        rendered = json.dumps(payload)

        self.assertEqual(
            payload["evaluator"]["model_provider_usage"],
            {
                "schema_version": "model-provider-usage-v1",
                "source": "harbor_atif_final_metrics",
                "provider": "openai-compatible",
                "model_name": "deepseek-v4-flash",
                "input_tokens": 1000,
                "output_tokens": 250,
                "total_tokens": 1250,
                "cost_usd": 0.0125,
            },
        )
        self.assertNotIn("sk-secret", rendered)

    def test_failed_run_projection_shows_failure_reason_and_evaluator_failure(self):
        run = _minimal_run(status=RunStatus.FAILED, failure_reason="sandbox command timed out")
        run.evaluator_result = EvaluatorResult(
            evaluator_id="mock-judge-v0",
            status="failed",
            score=None,
            metrics={"task_success": False},
            verbal_feedback="",
            judge=JudgeConfig(
                provider="mock",
                model_name="deterministic-judge",
                rubric_version="latent-skill-v0",
            ),
            artifact_refs=[],
            failure_reason="judge prompt could not be rendered",
        )

        payload = RunDashboardProjection.from_run(run).to_dict()

        self.assertEqual(payload["failure_reason"], "sandbox command timed out")
        self.assertEqual(payload["evaluator"]["status"], "failed")
        self.assertEqual(payload["evaluator"]["failure_reason"], "judge prompt could not be rendered")
        self.assertIsNone(payload["evaluator"].get("score"))

    def test_projection_strips_signed_url_query_parameters_from_artifact_links(self):
        run = _minimal_run(status=RunStatus.SUCCEEDED)
        run.artifacts.append(
            _artifact(
                "eval-report",
                ArtifactKind.EVALUATOR_REPORT,
                "https://storage.example/runs/run_001/report.json?X-Amz-Signature=secret#fragment",
            )
        )
        run.evaluator_result = EvaluatorResult(
            evaluator_id="mock-judge-v0",
            status="completed",
            score=0.88,
            metrics={"task_success": True},
            verbal_feedback="Looks correct.",
            judge=JudgeConfig(
                provider="mock",
                model_name="deterministic-judge",
                rubric_version="latent-skill-v0",
            ),
            artifact_refs=[
                "https://storage.example/runs/run_001/report.json?X-Amz-Signature=secret#fragment"
            ],
        )

        payload = RunDashboardProjection.from_run(run).to_dict()
        rendered = json.dumps(payload)

        self.assertEqual(payload["artifacts"][0]["uri"], "https://storage.example/runs/run_001/report.json")
        self.assertEqual(
            payload["evaluator"]["artifact_refs"][0],
            "https://storage.example/runs/run_001/report.json",
        )
        self.assertNotIn("secret", rendered)
        self.assertNotIn("X-Amz-Signature", rendered)

    def test_projection_hides_unsafe_storage_keys(self):
        run = _minimal_run(status=RunStatus.SUCCEEDED)
        run.artifacts.extend(
            [
                ArtifactRef(
                    artifact_id="absolute-storage-key",
                    kind=ArtifactKind.WORKSPACE_SNAPSHOT,
                    uri="file:///srv/private/workspace/snapshot.json",
                    media_type="application/json",
                    metadata={"storage_key": "/srv/private/workspace/snapshot.json"},
                ),
                ArtifactRef(
                    artifact_id="traversal-storage-key",
                    kind=ArtifactKind.EVALUATOR_REPORT,
                    uri="minio://runs/run_001/report.json",
                    media_type="application/json",
                    metadata={"storage_key": "../private/report.json?token=secret"},
                ),
            ]
        )

        payload = RunDashboardProjection.from_run(run).to_dict()
        rendered = json.dumps(payload)

        self.assertNotIn("storage_key", payload["artifacts"][0])
        self.assertNotIn("storage_key", payload["artifacts"][1])
        self.assertNotIn("/srv/private", rendered)
        self.assertNotIn("secret", rendered)


def _minimal_run(
    *,
    status: RunStatus,
    failure_reason: str | None = None,
) -> RunRecord:
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
        status=status,
        created_at=created,
        updated_at=created,
        failure_reason=failure_reason,
    )


def _artifact(artifact_id: str, kind: ArtifactKind, uri: str) -> ArtifactRef:
    return ArtifactRef(
        artifact_id=artifact_id,
        kind=kind,
        uri=uri,
        media_type="application/json",
        sha256="2" * 64,
        size_bytes=128,
        metadata={"storage_key": f"runs/run_001/{artifact_id}"},
    )
