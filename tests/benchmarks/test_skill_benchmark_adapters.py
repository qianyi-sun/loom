import unittest

from agentic_data_platform.benchmarks.adapters import (
    BenchmarkTaskSpec,
    SkillFlowBenchmarkAdapter,
    SkillLearnBenchBenchmarkAdapter,
)
from agentic_data_platform.domain.run_records import RunnerKind, SandboxBackend


class SkillBenchmarkAdapterTest(unittest.TestCase):
    def test_skillflow_adapter_registers_task_and_original_runner_config(self):
        adapter = SkillFlowBenchmarkAdapter(
            benchmark_version="hf:zhang-ziao/SkillFlow-Task@2026-05-28",
            source_uri="https://huggingface.co/datasets/zhang-ziao/SkillFlow-Task",
        )

        registration = adapter.register_task(
            BenchmarkTaskSpec(
                task_family="receipt-to-spreadsheet",
                instance_id="conference-expense-03",
                instruction="Read receipt PDFs and create receipts.xlsx.",
                input_artifact_refs=["minio://benchmarks/skillflow/conference/input.tar.zst"],
                runner_image="python:3.12-slim",
                runner_entrypoint=["python", "-m", "skillflow.runner"],
                metadata={"expected_output": "receipts.xlsx"},
            )
        )

        self.assertEqual(registration.task.benchmark_suite, "SkillFlow")
        self.assertEqual(registration.task.benchmark_version, "hf:zhang-ziao/SkillFlow-Task@2026-05-28")
        self.assertEqual(registration.task.task_family, "receipt-to-spreadsheet")
        self.assertEqual(registration.task.required_artifacts, ["trajectory", "workspace_snapshot", "evaluator_report"])
        self.assertEqual(registration.task.metadata["instruction"], "Read receipt PDFs and create receipts.xlsx.")
        self.assertEqual(registration.task.metadata["expected_output"], "receipts.xlsx")
        self.assertEqual(registration.runner.kind, RunnerKind.ORIGINAL_BENCHMARK)
        self.assertEqual(registration.runner.sandbox_backend, SandboxBackend.DOCKER_TERMINAL)
        self.assertEqual(registration.runner.entrypoint, ["python", "-m", "skillflow.runner"])
        self.assertTrue(registration.runner.internet_access)
        self.assertEqual(registration.runner.resource_limits["cpu"], 2)
        self.assertEqual(registration.runner.resource_limits["memory_gib"], 8)
        self.assertEqual(registration.runner.resource_limits["timeout_seconds"], 3600)

    def test_skilllearnbench_adapter_registers_task_without_separate_contract(self):
        adapter = SkillLearnBenchBenchmarkAdapter(
            benchmark_version="git:cxcscmu/SkillLearnBench@abc123",
            source_uri="https://github.com/cxcscmu/SkillLearnBench",
        )

        registration = adapter.register_task(
            BenchmarkTaskSpec(
                task_family="file-organization",
                instance_id="instance-05",
                instruction="Organize the files and write a summary.",
                input_artifact_refs=["minio://benchmarks/skilllearnbench/instance-05/input.tar.zst"],
                runner_image="python:3.12-slim",
                runner_entrypoint=["python", "-m", "skilllearnbench.runner"],
            )
        )

        self.assertEqual(registration.task.benchmark_suite, "SkillLearnBench")
        self.assertEqual(registration.task.source_uri, "https://github.com/cxcscmu/SkillLearnBench")
        self.assertEqual(registration.runner.metadata["adapter"], "SkillLearnBench")
        self.assertEqual(registration.runner.metadata["runner_contract"], "original_benchmark_wrapper")

    def test_task_spec_requires_instruction_and_runner_entrypoint(self):
        with self.assertRaisesRegex(ValueError, "instruction"):
            BenchmarkTaskSpec(
                task_family="receipt-to-spreadsheet",
                instance_id="conference-expense-03",
                instruction="",
                input_artifact_refs=["minio://input"],
                runner_image="python:3.12-slim",
                runner_entrypoint=["python", "-m", "skillflow.runner"],
            )

        with self.assertRaisesRegex(ValueError, "runner_entrypoint"):
            BenchmarkTaskSpec(
                task_family="receipt-to-spreadsheet",
                instance_id="conference-expense-03",
                instruction="Read receipts.",
                input_artifact_refs=["minio://input"],
                runner_image="python:3.12-slim",
                runner_entrypoint=[],
            )
