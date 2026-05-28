import unittest

from agentic_data_platform.benchmarks.adapters import (
    SkillFlowBenchmarkAdapter,
    SkillLearnBenchBenchmarkAdapter,
)
from agentic_data_platform.benchmarks.fixtures import load_fixture_catalog, load_fixture_catalogs


class BenchmarkFixtureCatalogTest(unittest.TestCase):
    def test_loads_checked_in_fixture_catalogs_offline(self):
        catalogs = load_fixture_catalogs()

        self.assertEqual({catalog.suite_name for catalog in catalogs}, {"SkillFlow", "SkillLearnBench"})
        for catalog in catalogs:
            self.assertTrue(catalog.source_uri.startswith("https://"))
            self.assertTrue(catalog.source_version)
            self.assertGreaterEqual(len(catalog.task_families), 2)
            self.assertGreaterEqual(len(catalog.task_instances()), 2)

            for instance in catalog.task_instances():
                self.assertEqual(instance.required_artifacts, ["trajectory", "workspace_snapshot", "evaluator_report"])
                self.assertTrue(instance.instruction_ref)
                self.assertTrue(instance.input_files)
                self.assertTrue(instance.input_artifact_refs)
                self.assertIn(instance.task_family, {family.name for family in catalog.task_families})

    def test_fixture_instances_convert_to_adapter_registrations(self):
        skillflow = load_fixture_catalog("SkillFlow")
        skillflow_spec = skillflow.to_task_spec(
            task_family="OCR-Data-Extraction",
            instance_id="task_family_invoice_images",
        )
        skillflow_registration = SkillFlowBenchmarkAdapter(
            benchmark_version=skillflow.benchmark_version,
            source_uri=skillflow.source_uri,
        ).register_task(skillflow_spec)

        self.assertEqual(skillflow_registration.task.benchmark_suite, "SkillFlow")
        self.assertEqual(skillflow_registration.task.task_family, "OCR-Data-Extraction")
        self.assertEqual(skillflow_registration.task.instance_id, "task_family_invoice_images")
        self.assertEqual(
            skillflow_registration.task.metadata["instruction_ref"],
            "test_tasks/OCR-Data-Extraction/task_family_invoice_images/instruction.md",
        )
        self.assertEqual(skillflow_registration.runner.metadata["runner_contract"], "skillflow-original-wrapper-v0")

        skilllearnbench = load_fixture_catalog("SkillLearnBench")
        skilllearnbench_spec = skilllearnbench.to_task_spec(
            task_family="organize-messy-files",
            instance_id="organize-messy-files-1",
        )
        skilllearnbench_registration = SkillLearnBenchBenchmarkAdapter(
            benchmark_version=skilllearnbench.benchmark_version,
            source_uri=skilllearnbench.source_uri,
        ).register_task(skilllearnbench_spec)

        self.assertEqual(skilllearnbench_registration.task.benchmark_suite, "SkillLearnBench")
        self.assertEqual(skilllearnbench_registration.task.task_family, "organize-messy-files")
        self.assertEqual(skilllearnbench_registration.task.instance_id, "organize-messy-files-1")
        self.assertEqual(
            skilllearnbench_registration.runner.metadata["runner_contract"],
            "skilllearnbench-original-wrapper-v0",
        )

    def test_unknown_fixture_lookup_fails_with_actionable_message(self):
        catalog = load_fixture_catalog("SkillFlow")

        with self.assertRaisesRegex(ValueError, "Unknown fixture instance"):
            catalog.to_task_spec(task_family="missing-family", instance_id="missing-instance")
