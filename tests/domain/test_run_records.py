import json
import unittest
from datetime import datetime, timezone

from agentic_data_platform.domain.run_records import (
    ArtifactKind,
    ArtifactRef,
    BenchmarkTaskInstance,
    EvaluatorConfig,
    EvaluatorResult,
    JudgeConfig,
    ModelConfig,
    ModelMode,
    RunnerConfig,
    RunnerKind,
    RunRecord,
    RunStatus,
    SandboxBackend,
    SkillObjectRef,
    SkillRepresentation,
    TerminalTurn,
)


class RunRecordTest(unittest.TestCase):
    def test_terminal_benchmark_run_serializes_required_mvp_metadata(self):
        run = RunRecord.create(
            run_id="run_001",
            project_id="pilot-project",
            owner_team="pilot group",
            task=BenchmarkTaskInstance(
                benchmark_suite="SkillFlow",
                benchmark_version="hf:zhang-ziao/SkillFlow-Task@2026-05-28",
                task_family="receipt-to-spreadsheet",
                instance_id="brazil-conference-01",
                source_uri="https://huggingface.co/datasets/zhang-ziao/SkillFlow-Task",
                input_artifact_refs=["minio://benchmarks/skillflow/brazil/input.tar.zst"],
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
        )

        payload = run.to_dict()

        self.assertEqual(payload["status"], "queued")
        self.assertEqual(payload["task"]["benchmark_suite"], "SkillFlow")
        self.assertEqual(payload["task"]["benchmark_version"], "hf:zhang-ziao/SkillFlow-Task@2026-05-28")
        self.assertEqual(payload["model"]["mode"], "api")
        self.assertEqual(payload["runner"]["sandbox_backend"], "docker_terminal")
        self.assertTrue(payload["runner"]["internet_access"])
        self.assertEqual(payload["task"]["required_artifacts"], ["trajectory", "workspace_snapshot", "evaluator_report"])
        self.assertIsInstance(json.dumps(payload), str)

    def test_run_record_captures_full_terminal_trajectory_and_final_workspace(self):
        run = self._minimal_run()
        run.transition_to(RunStatus.PROVISIONING)
        run.transition_to(RunStatus.RUNNING)

        run.add_turn(
            TerminalTurn(
                turn_index=0,
                command="python parse_receipts.py ./pdfs --out receipts.xlsx",
                cwd="/workspace",
                started_at=datetime(2026, 5, 28, 12, 0, 0, tzinfo=timezone.utc),
                completed_at=datetime(2026, 5, 28, 12, 0, 3, tzinfo=timezone.utc),
                exit_code=0,
                stdout="created receipts.xlsx\n",
                stderr="",
                changed_paths=["receipts.xlsx"],
                model_call_id="call_001",
            )
        )
        run.attach_artifact(
            ArtifactRef(
                artifact_id="artifact_workspace_001",
                kind=ArtifactKind.WORKSPACE_SNAPSHOT,
                uri="minio://runs/run_001/final-workspace.tar.zst",
                media_type="application/zstd",
                sha256="4" * 64,
                size_bytes=4096,
                metadata={"file_count": 12},
            )
        )

        payload = run.to_dict()

        self.assertEqual(payload["trajectory"][0]["command"], "python parse_receipts.py ./pdfs --out receipts.xlsx")
        self.assertEqual(payload["trajectory"][0]["exit_code"], 0)
        self.assertEqual(payload["trajectory"][0]["stdout"], "created receipts.xlsx\n")
        self.assertEqual(payload["trajectory"][0]["changed_paths"], ["receipts.xlsx"])
        self.assertEqual(payload["artifacts"][0]["kind"], "workspace_snapshot")
        self.assertEqual(payload["artifacts"][0]["metadata"]["file_count"], 12)

    def test_evaluator_feedback_is_dashboard_visible_and_status_checked(self):
        run = self._minimal_run()
        run.transition_to(RunStatus.PROVISIONING)
        run.transition_to(RunStatus.RUNNING)
        run.transition_to(RunStatus.EVALUATING)

        run.attach_evaluator_result(
            EvaluatorResult(
                evaluator_id="llm-judge-v0",
                status="completed",
                score=0.82,
                metrics={
                    "task_success": True,
                    "trajectory_quality": 0.78,
                    "skill_quality": 0.71,
                },
                verbal_feedback="The spreadsheet is correct except for one missing receipt date.",
                judge=JudgeConfig(
                    provider="openai",
                    model_name="gpt-5",
                    rubric_version="latent-skill-benchmark-2026-05-28",
                ),
                artifact_refs=["minio://runs/run_001/evaluator-report.json"],
            )
        )
        run.transition_to(RunStatus.SUCCEEDED)

        payload = run.to_dict()

        self.assertEqual(payload["status"], "succeeded")
        self.assertEqual(payload["evaluator_result"]["score"], 0.82)
        self.assertTrue(payload["evaluator_result"]["metrics"]["task_success"])
        self.assertIn("missing receipt date", payload["evaluator_result"]["verbal_feedback"])
        self.assertEqual(payload["evaluator_result"]["judge"]["rubric_version"], "latent-skill-benchmark-2026-05-28")

    def test_multiple_evaluator_results_keep_primary_latest_summary(self):
        run = self._minimal_run()
        run.transition_to(RunStatus.PROVISIONING)
        run.transition_to(RunStatus.RUNNING)
        run.transition_to(RunStatus.EVALUATING)

        run.attach_evaluator_result(
            EvaluatorResult(
                evaluator_id="harbor-verifier-v1",
                mode="harbor_verifier",
                status="completed",
                score=0.7,
                metrics={"reward": 0.7, "verifier_version": "harbor-2026-05-29"},
                verbal_feedback="",
                judge=None,
                artifact_refs=["minio://runs/run_001/evaluation/harbor-verifier/report.json"],
                metadata={"verifier_version": "harbor-2026-05-29"},
            )
        )
        run.attach_evaluator_result(
            EvaluatorResult(
                evaluator_id="llm-judge-v0",
                mode="llm_judge",
                status="completed",
                score=0.82,
                metrics={"task_success": True},
                verbal_feedback="The spreadsheet is correct except for one missing receipt date.",
                judge=JudgeConfig(
                    provider="openai",
                    model_name="gpt-5",
                    rubric_version="latent-skill-benchmark-2026-05-28",
                ),
                artifact_refs=["minio://runs/run_001/evaluation/llm-judge/report.json"],
            )
        )
        run.transition_to(RunStatus.SUCCEEDED)

        payload = run.to_dict()

        self.assertEqual([result["mode"] for result in payload["evaluator_results"]], ["harbor_verifier", "llm_judge"])
        self.assertEqual(payload["evaluator_result"]["evaluator_id"], "llm-judge-v0")
        self.assertEqual(payload["evaluator_results"][0]["metadata"]["verifier_version"], "harbor-2026-05-29")
        self.assertNotIn("judge", payload["evaluator_results"][0])

    def test_model_mode_rejects_local_weights_for_v0(self):
        with self.assertRaisesRegex(ValueError, "API-based model access"):
            ModelConfig(
                provider="local",
                model_name="qwen-local",
                mode="local_weights",
                prompt_template_version="terminal-agent-v0",
            )

    def test_hybrid_evaluator_config_requires_judge(self):
        with self.assertRaisesRegex(ValueError, "hybrid evaluator config requires a judge"):
            EvaluatorConfig(evaluator_id="hybrid-v0", mode="hybrid", judge=None)

    def test_skill_object_schema_allows_opaque_or_continuous_representations(self):
        skill = SkillObjectRef(
            skill_id="skill_001",
            producing_run_id="run_001",
            representation=SkillRepresentation.OPAQUE,
            artifact_refs=["minio://runs/run_001/skills/continuous-skill.bin"],
            benchmark_suite="SkillFlow",
            task_family="receipt-to-spreadsheet",
            metadata={"embedding_dim": 4096},
        )

        payload = skill.to_dict()

        self.assertEqual(payload["representation"], "opaque")
        self.assertEqual(payload["producing_run_id"], "run_001")
        self.assertEqual(payload["artifact_refs"], ["minio://runs/run_001/skills/continuous-skill.bin"])
        self.assertEqual(payload["metadata"]["embedding_dim"], 4096)

    def test_status_transitions_follow_recorded_lifecycle(self):
        run = self._minimal_run()

        with self.assertRaisesRegex(ValueError, "Invalid run status transition"):
            run.transition_to(RunStatus.SUCCEEDED)

        run.transition_to(RunStatus.PROVISIONING)
        run.transition_to(RunStatus.RUNNING)
        run.transition_to(RunStatus.EVALUATING)
        run.transition_to(RunStatus.SUCCEEDED)

        self.assertEqual(run.status, RunStatus.SUCCEEDED)

    def test_scheduler_dispatches_before_worker_provisioning(self):
        run = self._minimal_run()

        run.transition_to(RunStatus.DISPATCHED)
        run.transition_to(RunStatus.PROVISIONING)

        self.assertEqual(run.status, RunStatus.PROVISIONING)

    def test_stale_dispatch_can_return_to_queued_for_scheduler_recovery(self):
        run = self._minimal_run()

        run.transition_to(RunStatus.DISPATCHED)
        run.transition_to(RunStatus.QUEUED)

        self.assertEqual(run.status, RunStatus.QUEUED)

    def _minimal_run(self):
        return RunRecord.create(
            run_id="run_001",
            project_id="pilot-project",
            owner_team="pilot group",
            task=BenchmarkTaskInstance(
                benchmark_suite="SkillLearnBench",
                benchmark_version="git:cxcscmu/SkillLearnBench@abc123",
                task_family="spreadsheet-from-documents",
                instance_id="conference-expense-03",
                source_uri="https://github.com/cxcscmu/SkillLearnBench",
                input_artifact_refs=["minio://benchmarks/skilllearnbench/conference/input.tar.zst"],
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
                entrypoint=["python", "-m", "skilllearnbench.runner"],
                internet_access=True,
                resource_limits={"cpu": 2, "memory_gib": 8, "timeout_seconds": 3600},
            ),
        )
