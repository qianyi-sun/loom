import asyncio
import unittest
from types import SimpleNamespace

from sqlalchemy.pool import StaticPool

from agentic_data_platform.harbor.registry_sync import (
    list_registry_dataset_catalogs,
    sync_harbor_registry_catalogs,
)
from agentic_data_platform.persistence import create_database_engine, session_scope
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.repositories import BenchmarkCatalogRepository


class HarborRegistrySyncTest(unittest.TestCase):
    def test_lists_catalogs_from_registry_dataset_summaries_with_freshness_metadata(self):
        catalogs = asyncio.run(
            list_registry_dataset_catalogs(
                FakeRegistryClient(
                    [
                        _summary("terminal-bench", "2.0", 89, "Terminal benchmark"),
                        _summary("aider-polyglot", "1.0", 225, "Aider benchmark"),
                    ]
                ),
                checked_at="2026-06-01T15:00:00Z",
            )
        )

        self.assertEqual([catalog.suite_name for catalog in catalogs], ["Harbor:terminal-bench", "Harbor:aider-polyglot"])
        terminal_bench = catalogs[0]
        self.assertEqual(terminal_bench.benchmark_version, "harbor:terminal-bench@2.0")
        self.assertEqual(terminal_bench.source_uri, "harbor://datasets/terminal-bench@2.0")
        self.assertEqual(terminal_bench.source_version, "2.0")
        self.assertEqual(terminal_bench.source_version_type, "harbor-registry-version")
        self.assertEqual(terminal_bench.metadata["source_type"], "harbor_registry_dataset")
        self.assertEqual(terminal_bench.metadata["registry_sync"]["status"], "fresh")
        self.assertEqual(terminal_bench.metadata["registry_sync"]["checked_at"], "2026-06-01T15:00:00Z")
        self.assertEqual(terminal_bench.metadata["registry_task_count"], 89)

        task = terminal_bench.task_instances()[0]
        self.assertEqual(task.task_family, "terminal-bench")
        self.assertEqual(task.instance_id, "registry-dataset")
        self.assertEqual(task.metadata["harbor_run"]["dataset_ref"], "terminal-bench@2.0")
        self.assertEqual(task.metadata["harbor_run"]["extra_args"], ["--n-tasks", "1", "--quiet"])

    def test_syncs_registry_catalogs_into_benchmark_repository(self):
        engine = create_database_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        upgrade_database(engine)
        try:
            with session_scope(engine) as session:
                result = asyncio.run(
                    sync_harbor_registry_catalogs(
                        BenchmarkCatalogRepository(session),
                        FakeRegistryClient([_summary("terminal-bench", "2.0", 89, "Terminal benchmark")]),
                        checked_at="2026-06-01T15:00:00Z",
                    )
                )
                catalogs = BenchmarkCatalogRepository(session).list_fixture_catalogs()

            self.assertEqual(result.catalog_count, 1)
            self.assertEqual(result.errors, [])
            self.assertEqual(catalogs[0].suite_name, "Harbor:terminal-bench")
            self.assertEqual(catalogs[0].metadata["registry_sync"]["status"], "fresh")
        finally:
            engine.dispose()

    def test_sync_preserves_valid_catalogs_and_reports_invalid_registry_entries(self):
        engine = create_database_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        upgrade_database(engine)
        try:
            with session_scope(engine) as session:
                result = asyncio.run(
                    sync_harbor_registry_catalogs(
                        BenchmarkCatalogRepository(session),
                        FakeRegistryClient(
                            [
                                _summary("terminal-bench", "2.0", 89, "Terminal benchmark"),
                                _summary("unstable-bench", "latest", 5, "Unpinned benchmark"),
                            ]
                        ),
                        checked_at="2026-06-01T15:00:00Z",
                    )
                )
                catalogs = BenchmarkCatalogRepository(session).list_fixture_catalogs()

            self.assertEqual(result.catalog_count, 1)
            self.assertEqual(catalogs[0].suite_name, "Harbor:terminal-bench")
            self.assertEqual(
                result.errors,
                [
                    {
                        "dataset": "unstable-bench",
                        "message": "Harbor registry dataset unstable-bench has unresolved latest version",
                    }
                ],
            )
        finally:
            engine.dispose()


class FakeRegistryClient:
    def __init__(self, summaries):
        self.summaries = summaries

    async def list_datasets(self):
        return list(self.summaries)


def _summary(name: str, version: str, task_count: int, description: str):
    return SimpleNamespace(name=name, version=version, task_count=task_count, description=description)


if __name__ == "__main__":
    unittest.main()
