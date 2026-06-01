import unittest

from agentic_data_platform.harbor.benchmark_provider import HarborBenchmarkProvider
from agentic_data_platform.harbor.task_uploads import HarborTaskArchiveValidationResult


class HarborBenchmarkProviderTest(unittest.TestCase):
    def test_lists_versioned_harbor_dataset_catalogs(self):
        catalog = HarborBenchmarkProvider().list_catalogs()[0]

        self.assertEqual(catalog.suite_name, "HarborTerminalBench")
        self.assertEqual(catalog.benchmark_version, "harbor:terminal-bench/terminal-bench-2")
        self.assertEqual(catalog.source_uri, "harbor://datasets/terminal-bench/terminal-bench-2")
        self.assertEqual(catalog.source_version, "terminal-bench-2")
        self.assertEqual(catalog.source_version_type, "harbor-dataset-ref")
        self.assertEqual(catalog.metadata["provider"], "harbor")
        self.assertEqual(catalog.metadata["source_type"], "harbor_dataset")
        self.assertEqual(catalog.metadata["harbor_dataset_ref"], "terminal-bench/terminal-bench-2")
        self.assertEqual(catalog.metadata["environment_types"], ["docker"])
        self.assertEqual(catalog.metadata["verifier_type"], "harbor_verifier")
        self.assertEqual(catalog.metadata["artifact_conventions"]["raw_jobs"], "jobs/")

        tasks = catalog.task_instances()
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].task_family, "terminal-bench-2")
        self.assertEqual(tasks[0].instance_id, "dataset-ref")
        self.assertEqual(tasks[0].runner_contract, "harbor-local-docker-v0")
        self.assertEqual(tasks[0].metadata["harbor_run"]["dataset_ref"], "terminal-bench/terminal-bench-2")
        self.assertEqual(tasks[0].metadata["harbor_run"]["backend"], "cli")

    def test_builds_catalog_for_uploaded_harbor_task_archive(self):
        validation = HarborTaskArchiveValidationResult(
            filename="task.zip",
            normalized_root="uploaded-task",
            task_name="uploaded-terminal-task",
            files=["instruction.md", "task.toml", "environment/Dockerfile", "tests/test.sh"],
            declared_artifacts=["report.json"],
            environment={"type": "docker"},
            resource_requirements={"timeout_sec": 600},
            validation_errors=[],
            validation_warnings=[],
        )

        catalog = HarborBenchmarkProvider().catalog_for_task_upload(
            project_id="pilot-project",
            upload_id="upload-001",
            storage_key="harbor-task-uploads/pilot-project/upload-001/task.zip",
            archive_sha256="sha256:abc123",
            validation=validation,
        )

        self.assertEqual(catalog.suite_name, "HarborUploadedTask")
        self.assertEqual(catalog.benchmark_version, "upload:upload-001")
        self.assertEqual(catalog.source_uri, "object-store://harbor-task-uploads/pilot-project/upload-001/task.zip")
        self.assertEqual(catalog.source_version, "sha256:abc123")
        self.assertEqual(catalog.source_version_type, "sha256")
        self.assertEqual(catalog.metadata["project_id"], "pilot-project")
        self.assertEqual(catalog.metadata["source_type"], "harbor_task_upload")
        self.assertEqual(catalog.metadata["declared_artifacts"], ["report.json"])

        task = catalog.task_instances()[0]
        self.assertEqual(task.task_family, "uploaded-terminal-task")
        self.assertEqual(task.instance_id, "upload-001")
        self.assertEqual(task.metadata["harbor_run"]["task_archive_storage_key"], "harbor-task-uploads/pilot-project/upload-001/task.zip")
        self.assertEqual(task.metadata["harbor_run"]["backend"], "cli")
        self.assertEqual(task.metadata["environment"], {"type": "docker"})
