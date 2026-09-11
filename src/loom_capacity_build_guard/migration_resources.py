"""Locate the independent build-assignment schema in installed runtimes."""

from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path


@dataclass(frozen=True, slots=True)
class BuildGuardMigrationResources:
    config: Path
    scripts: Path


def resolve_build_guard_migration_resources() -> BuildGuardMigrationResources:
    scripts = Path(str(files("capacity_build_guard_migrations"))).resolve()
    config = scripts / "alembic.ini"
    if not config.is_file() or not (scripts / "versions").is_dir():
        raise RuntimeError("build guard migration resources are missing")
    return BuildGuardMigrationResources(config=config, scripts=scripts)
