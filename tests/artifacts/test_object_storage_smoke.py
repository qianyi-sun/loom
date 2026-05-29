import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from agentic_data_platform.artifacts.object_storage_smoke import run_object_storage_smoke
from agentic_data_platform.artifacts.store import LocalArtifactStore


class ObjectStorageSmokeTest(unittest.TestCase):
    def test_smoke_uploads_downloads_and_returns_result_metadata(self):
        with TemporaryDirectory() as temp_dir:
            result = run_object_storage_smoke(
                LocalArtifactStore(Path(temp_dir)),
                key="smoke/test.txt",
                payload=b"object storage smoke\n",
            )

        self.assertEqual(result["key"], "smoke/test.txt")
        self.assertEqual(result["size_bytes"], 21)
        self.assertEqual(result["download_verified"], True)
        self.assertEqual(result["presigned_url_available"], True)
