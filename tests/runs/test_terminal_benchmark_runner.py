import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from agentic_data_platform.artifacts.store import ArtifactPersistence, LocalArtifactStore
from agentic_data_platform.benchmarks.adapters import (
    BenchmarkTaskSpec,
    SkillFlowBenchmarkAdapter,
    SkillLearnBenchBenchmarkAdapter,
)
from agentic_data_platform.benchmarks.fixtures import load_fixture_catalog
from agentic_data_platform.domain.run_records import JudgeConfig, ModelConfig, ModelMode, RunStatus
from agentic_data_platform.evaluation.mock import MockEvaluatorAdapter
from agentic_data_platform.models.providers import ModelCommand, ScriptedModelProvider
from agentic_data_platform.runs.terminal_benchmark import TerminalBenchmarkRunner, TerminalBenchmarkRunRequest
from agentic_data_platform.sandbox.docker_terminal import SandboxCommandResult, WorkspaceFile, WorkspaceSnapshot


class TerminalBenchmarkRunnerTest(unittest.TestCase):
    def test_runner_accepts_fixture_catalog_registration(self):
        catalog = load_fixture_catalog("SkillLearnBench")
        registration = SkillLearnBenchBenchmarkAdapter(
            benchmark_version=catalog.benchmark_version,
            source_uri=catalog.source_uri,
        ).register_task(
            catalog.to_task_spec(
                task_family="financial-analysis",
                instance_id="financial-analysis-1",
            )
        )
        sandbox = FakeSandbox()

        with tempfile.TemporaryDirectory() as temp_dir:
            runner = TerminalBenchmarkRunner(
                artifact_persistence=ArtifactPersistence(LocalArtifactStore(Path(temp_dir))),
                evaluator=MockEvaluatorAdapter(
                    evaluator_id="mock-judge-v0",
                    score=0.91,
                    verbal_feedback="The generated workbook satisfies the financial analysis rubric.",
                ),
            )

            result = runner.run(
                TerminalBenchmarkRunRequest(
                    run_id="run_fixture_001",
                    project_id="pilot-project",
                    owner_team="pilot group",
                    registration=registration,
                    model_provider=ScriptedModelProvider(
                        model=ModelConfig(
                            provider="mock-api",
                            model_name="scripted-terminal-agent",
                            mode=ModelMode.API,
                            prompt_template_version="terminal-agent-v0",
                        ),
                        commands=[ModelCommand(command="python solve.py", cwd="/workspace")],
                    ),
                    judge=JudgeConfig(
                        provider="mock",
                        model_name="deterministic-judge",
                        rubric_version="latent-skill-v0",
                    ),
                    rubric_id="latent-skill-v0",
                    sandbox=sandbox,
                )
            )

        self.assertEqual(result.run.status, RunStatus.SUCCEEDED)
        self.assertEqual(result.run.task.instance_id, "financial-analysis-1")
        self.assertEqual(
            result.run.task.metadata["instruction_ref"],
            "tasks/financial-analysis/financial-analysis-1/instruction.md",
        )
        self.assertEqual(
            result.run.runner.metadata["runner_contract"],
            "skilllearnbench-original-wrapper-v0",
        )

    def test_runner_executes_benchmark_through_sandbox_and_persists_visible_results(self):
        adapter = SkillFlowBenchmarkAdapter(
            benchmark_version="hf:zhang-ziao/SkillFlow-Task@2026-05-28",
            source_uri="https://huggingface.co/datasets/zhang-ziao/SkillFlow-Task",
        )
        registration = adapter.register_task(
            BenchmarkTaskSpec(
                task_family="receipt-to-spreadsheet",
                instance_id="conference-expense-03",
                instruction="Read receipts and create receipts.xlsx.",
                input_artifact_refs=["minio://benchmarks/skillflow/conference/input.tar.zst"],
                runner_image="python:3.12-slim",
                runner_entrypoint=["python", "-m", "skillflow.runner"],
            )
        )
        sandbox = FakeSandbox()

        with tempfile.TemporaryDirectory() as temp_dir:
            runner = TerminalBenchmarkRunner(
                artifact_persistence=ArtifactPersistence(LocalArtifactStore(Path(temp_dir))),
                evaluator=MockEvaluatorAdapter(
                    evaluator_id="mock-judge-v0",
                    score=0.86,
                    verbal_feedback="The spreadsheet contains all requested receipts.",
                ),
            )

            result = runner.run(
                TerminalBenchmarkRunRequest(
                    run_id="run_001",
                    project_id="pilot-project",
                    owner_team="pilot group",
                    registration=registration,
                    model_provider=ScriptedModelProvider(
                        model=ModelConfig(
                            provider="mock-api",
                            model_name="scripted-terminal-agent",
                            mode=ModelMode.API,
                            prompt_template_version="terminal-agent-v0",
                        ),
                        commands=[
                            ModelCommand(command="python solve.py", cwd="/workspace", model_call_id="call_001"),
                            ModelCommand(command="python verify.py", cwd="/workspace", model_call_id="call_002"),
                        ],
                    ),
                    judge=JudgeConfig(
                        provider="mock",
                        model_name="deterministic-judge",
                        rubric_version="latent-skill-v0",
                    ),
                    rubric_id="latent-skill-v0",
                    sandbox=sandbox,
                )
            )

            trajectory_path = Path(temp_dir) / "runs/run_001/tasks/conference-expense-03/trajectory/trajectory.jsonl"
            workspace_path = Path(temp_dir) / "runs/run_001/tasks/conference-expense-03/workspace/snapshot.json"
            evaluator_path = Path(temp_dir) / "runs/run_001/tasks/conference-expense-03/evaluation/mock-judge-v0/report.json"

            self.assertTrue(trajectory_path.exists())
            self.assertTrue(workspace_path.exists())
            self.assertTrue(evaluator_path.exists())

        self.assertEqual(sandbox.commands, ["python solve.py", "python verify.py"])
        self.assertEqual(result.run.status, RunStatus.SUCCEEDED)
        self.assertEqual(len(result.run.trajectory), 2)
        self.assertEqual(result.run.trajectory[0].model_call_id, "call_001")
        self.assertEqual(result.run.trajectory[1].changed_paths, ["receipts.xlsx"])
        self.assertEqual(len(result.run.artifacts), 3)
        self.assertEqual(result.run.evaluator_result.score, 0.86)
        self.assertEqual(result.dashboard["status"], "succeeded")
        self.assertEqual(result.dashboard["progress"]["turn_count"], 2)
        self.assertEqual(result.dashboard["progress"]["artifact_count"], 3)
        self.assertEqual(result.dashboard["evaluator"]["score"], 0.86)
        self.assertIn("spreadsheet", result.dashboard["evaluator"]["verbal_feedback_summary"])

    def test_runner_marks_run_failed_when_evaluator_fails(self):
        adapter = SkillFlowBenchmarkAdapter(
            benchmark_version="hf:zhang-ziao/SkillFlow-Task@2026-05-28",
            source_uri="https://huggingface.co/datasets/zhang-ziao/SkillFlow-Task",
        )
        registration = adapter.register_task(
            BenchmarkTaskSpec(
                task_family="receipt-to-spreadsheet",
                instance_id="conference-expense-03",
                instruction="Read receipts and create receipts.xlsx.",
                input_artifact_refs=["minio://benchmarks/skillflow/conference/input.tar.zst"],
                runner_image="python:3.12-slim",
                runner_entrypoint=["python", "-m", "skillflow.runner"],
            )
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            runner = TerminalBenchmarkRunner(
                artifact_persistence=ArtifactPersistence(LocalArtifactStore(Path(temp_dir))),
                evaluator=MockEvaluatorAdapter(
                    evaluator_id="mock-judge-v0",
                    failure_reason="rubric missing",
                ),
            )

            result = runner.run(
                TerminalBenchmarkRunRequest(
                    run_id="run_001",
                    project_id="pilot-project",
                    owner_team="pilot group",
                    registration=registration,
                    model_provider=ScriptedModelProvider(
                        model=ModelConfig(
                            provider="mock-api",
                            model_name="scripted-terminal-agent",
                            mode=ModelMode.API,
                            prompt_template_version="terminal-agent-v0",
                        ),
                        commands=[ModelCommand(command="python solve.py", cwd="/workspace")],
                    ),
                    judge=JudgeConfig(
                        provider="mock",
                        model_name="deterministic-judge",
                        rubric_version="latent-skill-v0",
                    ),
                    rubric_id="latent-skill-v0",
                    sandbox=FakeSandbox(),
                )
            )

        self.assertEqual(result.run.status, RunStatus.FAILED)
        self.assertEqual(result.run.failure_reason, "rubric missing")
        self.assertEqual(result.dashboard["evaluator"]["failure_reason"], "rubric missing")


class FakeSandbox:
    def __init__(self) -> None:
        self.commands: list[str] = []

    def execute(self, command: str, *, cwd: str | None = None, timeout_seconds: int | None = None) -> SandboxCommandResult:
        self.commands.append(command)
        started = datetime(2026, 5, 28, 12, len(self.commands), 0, tzinfo=timezone.utc)
        return SandboxCommandResult(
            run_id="run_001",
            command=command,
            cwd=cwd or "/workspace",
            started_at=started,
            completed_at=started,
            exit_code=0,
            stdout=f"ran {command}\n",
            stderr="",
            changed_paths=["receipts.xlsx"],
            metadata={"timeout_seconds": timeout_seconds},
        )

    def capture_workspace(self) -> WorkspaceSnapshot:
        return WorkspaceSnapshot(
            run_id="run_001",
            workspace_path="/tmp/fake-workspace",
            captured_at=datetime(2026, 5, 28, 12, 3, 0, tzinfo=timezone.utc),
            files=[WorkspaceFile(path="receipts.xlsx", size_bytes=128, sha256="3" * 64)],
        )
