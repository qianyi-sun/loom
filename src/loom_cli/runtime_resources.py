"""Package-owned runtime resources for non-editable Loom CLI installs."""

from __future__ import annotations

from importlib import resources
from importlib.resources.abc import Traversable

from loom_config.loader import Schema, load_schema


def _resource(*parts: str) -> Traversable:
    resource = resources.files("loom_cli.data")
    for part in parts:
        resource = resource.joinpath(part)
    return resource


def load_bundled_schema() -> Schema:
    """Load the canonical schema copy shipped inside the wheel."""
    with resources.as_file(_resource("loom-schema.toml")) as schema_path:
        return load_schema(schema_path)


def read_bundled_text(*parts: str) -> str:
    """Read one UTF-8 runtime asset from ``loom_cli.data``."""
    return _resource(*parts).read_text(encoding="utf-8")
