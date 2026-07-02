"""Instance field mapping from upstream rows (#242 sub-plan 3)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MappingError(Exception):
    field: str
    path: str
    detail: str

    def __str__(self) -> str:
        return f"mapping failed for {self.field!r} ({self.path!r}): {self.detail}"


def _resolve_path(row: dict[str, Any], path: str) -> Any:
    parts = path.split(".")
    if not parts or parts[0] != "row":
        raise ValueError("mapping path must start with 'row.'")
    current: Any = row
    for part in parts[1:]:
        if not isinstance(current, dict):
            raise KeyError(part)
        if part not in current:
            raise KeyError(part)
        current = current[part]
    return current


def resolve_mapping(row: dict[str, Any], mapping: dict[str, str]) -> dict[str, Any]:
    """Evaluate ``instance_mapping`` dotted paths against one upstream row."""
    instance: dict[str, Any] = {}
    for field, path in mapping.items():
        try:
            instance[field] = _resolve_path(row, path)
        except (KeyError, TypeError, ValueError) as exc:
            raise MappingError(field, path, str(exc)) from exc
    return instance
