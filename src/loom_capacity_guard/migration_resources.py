"""Resolve the installed protected-capacity Alembic script tree."""

from __future__ import annotations

from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path


@dataclass(frozen=True)
class CapacityGuardMigrationResources:
    """Filesystem paths Alembic requires from the installed resource package."""

    config: Path
    scripts: Path


def resolve_capacity_guard_migration_resources(
    alembic_ini: Path | None = None,
) -> CapacityGuardMigrationResources:
    """Resolve an explicit test configuration or the installed resource package."""

    if alembic_ini is not None:
        config = alembic_ini.resolve()
    else:
        package_root = files("capacity_guard_migrations")
        config = Path(str(package_root.joinpath("alembic.ini"))).resolve()
    scripts = config.parent
    if not config.is_file() or not scripts.joinpath("versions").is_dir():
        raise RuntimeError("protected capacity migration resources are missing")
    return CapacityGuardMigrationResources(config=config, scripts=scripts)


__all__ = [
    "CapacityGuardMigrationResources",
    "resolve_capacity_guard_migration_resources",
]
