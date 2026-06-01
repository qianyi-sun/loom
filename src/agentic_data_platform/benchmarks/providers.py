from __future__ import annotations

from typing import Protocol

from agentic_data_platform.benchmarks.fixtures import BenchmarkFixtureCatalog


class BenchmarkProvider(Protocol):
    def list_catalogs(self) -> list[BenchmarkFixtureCatalog]:
        """Return platform-readable benchmark catalogs without executing runs."""
