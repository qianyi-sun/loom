"""Resolve the installed capacity Alembic configuration and script tree."""

from __future__ import annotations

from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path


@dataclass(frozen=True)
class CapacityMigrationResources:
    """Filesystem paths Alembic requires from the installed resource package."""

    config: Path
    scripts: Path


def resolve_capacity_migration_resources(
    alembic_ini: Path | None = None,
) -> CapacityMigrationResources:
    """Resolve an explicit test configuration or the installed resource package."""

    if alembic_ini is not None:
        config = alembic_ini.resolve()
    else:
        package_root = files("capacity_migrations")
        config = Path(str(package_root.joinpath("alembic.ini"))).resolve()
    scripts = config.parent
    if not config.is_file() or not scripts.joinpath("versions").is_dir():
        raise RuntimeError("capacity migration resources are missing")
    return CapacityMigrationResources(config=config, scripts=scripts)


__all__ = ["CapacityMigrationResources", "resolve_capacity_migration_resources"]
