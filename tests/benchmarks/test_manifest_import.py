import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from agentic_data_platform.benchmarks.manifests import catalog_from_local_tree, catalog_from_path_manifest


class BenchmarkManifestImportTest(unittest.TestCase):
    def test_imports_skillflow_paths_into_catalog_shape(self):
        catalog = catalog_from_path_manifest(
            suite_name="SkillFlow",
            source_uri="https://huggingface.co/datasets/zhang-ziao/SkillFlow-Task",
            source_version="hf-snapshot-001",
            paths=[
                "test_tasks/OCR-Data-Extraction/task_family_invoice_images/instruction.md",
                "test_tasks/OCR-Data-Extraction/task_family_invoice_images/task.toml",
                "test_tasks/OCR-Data-Extraction/task_family_invoice_images/environment/input.pdf",
                "test_tasks/OCR-Data-Extraction/task_family_invoice_images/tests/test_outputs.py",
                "test_tasks/Sales-Pivot-Analysis/product-sales-pivot/instruction.md",
                "test_tasks/Sales-Pivot-Analysis/product-sales-pivot/task.toml",
                "test_tasks/Sales-Pivot-Analysis/product-sales-pivot/environment/input.csv",
                "test_tasks/Sales-Pivot-Analysis/product-sales-pivot/tests/test_outputs.py",
            ],
        )

        self.assertEqual(catalog.suite_name, "SkillFlow")
        self.assertEqual(catalog.benchmark_version, "hf:zhang-ziao/SkillFlow-Task@hf-snapshot-001")
        self.assertEqual({family.name for family in catalog.task_families}, {"OCR-Data-Extraction", "Sales-Pivot-Analysis"})

        instance = catalog.to_task_spec(
            task_family="OCR-Data-Extraction",
            instance_id="task_family_invoice_images",
        )
        self.assertEqual(instance.runner_contract, "skillflow-original-wrapper-v0")
        self.assertEqual(
            instance.metadata["instruction_ref"],
            "test_tasks/OCR-Data-Extraction/task_family_invoice_images/instruction.md",
        )
        self.assertIn(
            "test_tasks/OCR-Data-Extraction/task_family_invoice_images/environment/input.pdf",
            instance.metadata["input_files"],
        )

    def test_imports_skilllearnbench_paths_into_catalog_shape(self):
        catalog = catalog_from_path_manifest(
            suite_name="SkillLearnBench",
            source_uri="https://github.com/cxcscmu/SkillLearnBench",
            source_version="638284f5982f6be085a955435d2ec7a5258f5513",
            paths=[
                "tasks/organize-messy-files/organize-messy-files-1/instruction.md",
                "tasks/organize-messy-files/organize-messy-files-1/task.toml",
                "tasks/organize-messy-files/organize-messy-files-1/environment/input.zip",
                "tasks/organize-messy-files/organize-messy-files-1/tests/test_outputs.py",
                "tasks/financial-analysis/financial-analysis-2/instruction.md",
                "tasks/financial-analysis/financial-analysis-2/task.toml",
                "tasks/financial-analysis/financial-analysis-2/environment/portfolio.xlsx",
                "tasks/financial-analysis/financial-analysis-2/tests/test_outputs.py",
            ],
        )

        self.assertEqual(catalog.suite_name, "SkillLearnBench")
        self.assertEqual(
            catalog.benchmark_version,
            "git:cxcscmu/SkillLearnBench@638284f5982f6be085a955435d2ec7a5258f5513",
        )
        self.assertEqual({family.name for family in catalog.task_families}, {"organize-messy-files", "financial-analysis"})

        instance = catalog.to_task_spec(
            task_family="financial-analysis",
            instance_id="financial-analysis-2",
        )
        self.assertEqual(instance.runner_contract, "skilllearnbench-original-wrapper-v0")
        self.assertEqual(
            instance.metadata["instruction_ref"],
            "tasks/financial-analysis/financial-analysis-2/instruction.md",
        )
        self.assertEqual(
            instance.input_artifact_refs,
            [
                "git://github.com/cxcscmu/SkillLearnBench/tasks/financial-analysis/financial-analysis-2@638284f5982f6be085a955435d2ec7a5258f5513"
            ],
        )

    def test_import_requires_instruction_and_task_config(self):
        with self.assertRaisesRegex(ValueError, "instruction.md"):
            catalog_from_path_manifest(
                suite_name="SkillFlow",
                source_uri="https://huggingface.co/datasets/zhang-ziao/SkillFlow-Task",
                source_version="hf-snapshot-001",
                paths=[
                    "test_tasks/OCR-Data-Extraction/task_family_invoice_images/task.toml",
                ],
            )

        with self.assertRaisesRegex(ValueError, "task.toml"):
            catalog_from_path_manifest(
                suite_name="SkillLearnBench",
                source_uri="https://github.com/cxcscmu/SkillLearnBench",
                source_version="638284f5982f6be085a955435d2ec7a5258f5513",
                paths=[
                    "tasks/financial-analysis/financial-analysis-2/instruction.md",
                ],
            )

    def test_imports_from_local_upstream_tree(self):
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write(root / "tasks/financial-analysis/financial-analysis-1/instruction.md")
            _write(root / "tasks/financial-analysis/financial-analysis-1/task.toml")
            _write(root / "tasks/financial-analysis/financial-analysis-1/environment/input.xlsx")
            _write(root / "tasks/financial-analysis/financial-analysis-1/tests/test_outputs.py")
            _write(root / "tasks/financial-analysis/financial-analysis-1/.DS_Store")

            catalog = catalog_from_local_tree(
                suite_name="SkillLearnBench",
                source_uri="https://github.com/cxcscmu/SkillLearnBench",
                source_version="638284f5982f6be085a955435d2ec7a5258f5513",
                root=root,
            )

        spec = catalog.to_task_spec(
            task_family="financial-analysis",
            instance_id="financial-analysis-1",
        )
        self.assertEqual(spec.metadata["instruction_ref"], "tasks/financial-analysis/financial-analysis-1/instruction.md")
        self.assertNotIn(".DS_Store", "\n".join(spec.metadata["input_files"]))


def _write(path: Path, text: str = "fixture\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
