import tempfile
import unittest
import zipfile
from io import BytesIO
from pathlib import Path

from agentic_data_platform.harbor.task_uploads import (
    HarborTaskArchiveError,
    materialize_harbor_task_archive,
    validate_harbor_task_archive,
)


class HarborTaskUploadValidationTest(unittest.TestCase):
    def test_validates_harbor_task_zip_and_extracts_metadata(self):
        payload = _harbor_task_zip(
            root="invoice-task",
            task_toml="\n".join(
                [
                    'schema_version = "1.2"',
                    'artifacts = ["/logs/artifacts/receipts.xlsx"]',
                    "",
                    "[task]",
                    'name = "latent/invoice-extraction"',
                    'description = "Read receipts and produce a spreadsheet."',
                    "",
                    "[verifier]",
                    "timeout_sec = 120.0",
                    "",
                    "[agent]",
                    "timeout_sec = 300.0",
                    "",
                    "[environment]",
                    "build_timeout_sec = 180.0",
                    'os = "linux"',
                    "allow_internet = true",
                    "",
                ]
            ),
        )

        result = validate_harbor_task_archive(payload, filename="invoice-task.zip")

        self.assertEqual(result.normalized_root, "invoice-task")
        self.assertEqual(result.task_name, "latent/invoice-extraction")
        self.assertEqual(result.declared_artifacts, ["/logs/artifacts/receipts.xlsx"])
        self.assertEqual(result.environment["allow_internet"], True)
        self.assertEqual(result.resource_requirements["agent_timeout_sec"], 300.0)
        self.assertEqual(result.resource_requirements["verifier_timeout_sec"], 120.0)
        self.assertEqual(result.resource_requirements["environment_build_timeout_sec"], 180.0)
        self.assertIn("tests/test.sh", result.files)
        self.assertEqual(result.validation_errors, [])
        self.assertEqual(result.validation_warnings, [])

    def test_rejects_missing_instruction_with_actionable_error(self):
        payload = _zip_bytes(
            {
                "task.toml": '[task]\nname = "missing/instruction"\n[verifier]\ntimeout_sec = 60\n',
                "environment/Dockerfile": "FROM ubuntu:24.04\n",
                "tests/test.sh": "echo 1 > /logs/verifier/reward.txt\n",
            }
        )

        with self.assertRaisesRegex(HarborTaskArchiveError, "instruction.md"):
            validate_harbor_task_archive(payload, filename="missing-instruction.zip")

    def test_rejects_zip_path_traversal(self):
        payload = _zip_bytes({"task.toml": "[task]\nname = 'unsafe'\n", "../escape.txt": "bad"})

        with self.assertRaisesRegex(HarborTaskArchiveError, "unsafe archive path"):
            validate_harbor_task_archive(payload, filename="unsafe.zip")

    def test_rejects_absolute_zip_paths_before_normalization(self):
        payload = _zip_bytes({"/instruction.md": "unsafe", "task.toml": "[task]\nname = 'unsafe'\n"})

        with self.assertRaisesRegex(HarborTaskArchiveError, "unsafe archive path"):
            validate_harbor_task_archive(payload, filename="absolute.zip")

    def test_materializes_normalized_task_directory(self):
        payload = _harbor_task_zip(root="custom-task")

        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "task"
            result = materialize_harbor_task_archive(payload, filename="custom-task.zip", destination=target)

            self.assertEqual(result.task_name, "latent/custom-task")
            self.assertTrue((target / "instruction.md").is_file())
            self.assertTrue((target / "task.toml").is_file())
            self.assertTrue((target / "environment" / "Dockerfile").is_file())
            self.assertTrue((target / "tests" / "test.sh").is_file())
            self.assertFalse((target / "custom-task").exists())


def _harbor_task_zip(*, root: str = "", task_toml: str | None = None) -> bytes:
    prefix = f"{root.rstrip('/')}/" if root else ""
    return _zip_bytes(
        {
            f"{prefix}instruction.md": "Create `/app/receipts.xlsx` from the provided receipts.\n",
            f"{prefix}task.toml": task_toml
            or "\n".join(
                [
                    'schema_version = "1.2"',
                    'artifacts = ["/logs/artifacts/receipts.xlsx"]',
                    "",
                    "[task]",
                    'name = "latent/custom-task"',
                    "",
                    "[verifier]",
                    "timeout_sec = 120.0",
                    "",
                    "[agent]",
                    "timeout_sec = 300.0",
                    "",
                    "[environment]",
                    'os = "linux"',
                    "allow_internet = true",
                    "",
                ]
            ),
            f"{prefix}environment/Dockerfile": "FROM ubuntu:24.04\nWORKDIR /app\n",
            f"{prefix}tests/test.sh": "mkdir -p /logs/verifier\nprintf '1\\n' > /logs/verifier/reward.txt\n",
        }
    )


def _zip_bytes(files: dict[str, str]) -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path, content in files.items():
            archive.writestr(path, content)
    return buffer.getvalue()
