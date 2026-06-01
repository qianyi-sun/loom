from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from agentic_data_platform.benchmarks.fixtures import BenchmarkFixtureCatalog
from agentic_data_platform.harbor.benchmark_provider import HarborBenchmarkProvider, HarborDatasetCatalogSpec
from agentic_data_platform.persistence import create_database_engine, session_scope
from agentic_data_platform.persistence.repositories import BenchmarkCatalogRepository
from agentic_data_platform.service.config import load_service_settings


class HarborRegistryClient(Protocol):
    async def list_datasets(self) -> list[Any]:
        ...


@dataclass(frozen=True)
class HarborRegistrySyncResult:
    catalog_count: int
    checked_at: str
    errors: list[dict[str, str]]

    def to_dict(self) -> dict[str, object]:
        return {
            "catalog_count": self.catalog_count,
            "checked_at": self.checked_at,
            "errors": list(self.errors),
        }


async def list_registry_dataset_catalogs(
    registry_client: HarborRegistryClient | None = None,
    *,
    checked_at: str | None = None,
    limit: int | None = None,
    registry_source: str = "harbor_default_registry",
) -> list[BenchmarkFixtureCatalog]:
    """Read Harbor registry datasets into platform benchmark catalog objects."""

    catalogs, _errors = await _registry_dataset_catalogs_with_errors(
        registry_client,
        checked_at=checked_at,
        limit=limit,
        registry_source=registry_source,
    )
    return catalogs


async def sync_harbor_registry_catalogs(
    repository: BenchmarkCatalogRepository,
    registry_client: HarborRegistryClient | None = None,
    *,
    checked_at: str | None = None,
    limit: int | None = None,
    registry_source: str = "harbor_default_registry",
) -> HarborRegistrySyncResult:
    timestamp = checked_at or _now()
    catalogs, errors = await _registry_dataset_catalogs_with_errors(
        registry_client,
        checked_at=timestamp,
        limit=limit,
        registry_source=registry_source,
    )

    for catalog in catalogs:
        repository.upsert_fixture_catalog(catalog)
    return HarborRegistrySyncResult(catalog_count=len(catalogs), checked_at=timestamp, errors=errors)


async def _registry_dataset_catalogs_with_errors(
    registry_client: HarborRegistryClient | None = None,
    *,
    checked_at: str | None = None,
    limit: int | None = None,
    registry_source: str,
) -> tuple[list[BenchmarkFixtureCatalog], list[dict[str, str]]]:
    client = registry_client or _default_registry_client()
    timestamp = checked_at or _now()
    try:
        summaries = await client.list_datasets()
    except Exception as exc:
        return [], [{"message": str(exc)}]

    catalogs: list[BenchmarkFixtureCatalog] = []
    errors: list[dict[str, str]] = []
    for summary in summaries[:limit]:
        try:
            spec = _catalog_spec_from_summary(
                summary,
                checked_at=timestamp,
                registry_source=registry_source,
            )
        except Exception as exc:
            errors.append({"dataset": _summary_name(summary), "message": str(exc)})
            continue
        catalogs.extend(HarborBenchmarkProvider(dataset_specs=[spec]).list_catalogs())
    return catalogs, errors


def _catalog_spec_from_summary(
    summary: Any,
    *,
    checked_at: str,
    registry_source: str,
) -> HarborDatasetCatalogSpec:
    name = _non_empty_attr(summary, "name")
    version = _non_empty_attr(summary, "version")
    if version == "latest":
        raise ValueError(f"Harbor registry dataset {name} has unresolved latest version")
    description = _optional_attr(summary, "description")
    task_count = _optional_int_attr(summary, "task_count")
    return HarborDatasetCatalogSpec(
        suite_name=f"Harbor:{name}",
        dataset_ref=f"{name}@{version}",
        display_family=name,
        source_version=version,
        instance_id="registry-dataset",
        source_type="harbor_registry_dataset",
        source_version_type="harbor-registry-version",
        description=description or "",
        task_count=task_count,
        registry_sync={
            "status": "fresh",
            "checked_at": checked_at,
            "source": registry_source,
        },
    )


def _default_registry_client() -> HarborRegistryClient:
    from harbor.registry.client.factory import RegistryClientFactory

    return RegistryClientFactory.create()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _non_empty_attr(value: Any, name: str) -> str:
    raw = getattr(value, name, None)
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"Harbor registry dataset summary missing {name}")
    return raw.strip()


def _optional_attr(value: Any, name: str) -> str | None:
    raw = getattr(value, name, None)
    return raw.strip() if isinstance(raw, str) and raw.strip() else None


def _optional_int_attr(value: Any, name: str) -> int | None:
    raw = getattr(value, name, None)
    return raw if isinstance(raw, int) else None


def _summary_name(summary: Any) -> str:
    raw = getattr(summary, "name", None)
    return raw.strip() if isinstance(raw, str) and raw.strip() else "<unknown>"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sync Harbor registry datasets into the platform benchmark catalog.")
    parser.add_argument("--database-url", default="", help="Database URL. Defaults to DATABASE_URL from the service environment.")
    parser.add_argument("--limit", type=int, default=None, help="Optional max number of registry datasets to sync.")
    parser.add_argument("--dry-run", action="store_true", help="List catalogs without writing them to the database.")
    args = parser.parse_args(argv)

    async def _run() -> dict[str, object]:
        catalogs, errors = await _registry_dataset_catalogs_with_errors(
            checked_at=_now(),
            limit=args.limit,
            registry_source="harbor_default_registry",
        )
        if args.dry_run:
            return {
                "catalog_count": len(catalogs),
                "errors": errors,
                "catalogs": [
                    {
                        "suite_name": catalog.suite_name,
                        "benchmark_version": catalog.benchmark_version,
                        "source_uri": catalog.source_uri,
                        "source_version": catalog.source_version,
                    }
                    for catalog in catalogs
                ],
            }

        database_url = args.database_url or load_service_settings().database_url
        if not database_url:
            raise ValueError("database URL is required unless --dry-run is set")
        engine = create_database_engine(database_url, pool_pre_ping=True)
        try:
            with session_scope(engine) as session:
                result = await sync_harbor_registry_catalogs(
                    BenchmarkCatalogRepository(session),
                    limit=args.limit,
                )
                return result.to_dict()
        finally:
            engine.dispose()

    print(json.dumps(asyncio.run(_run()), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
