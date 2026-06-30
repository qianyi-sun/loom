"""Load ``storage-lifecycle.toml`` into a typed RetentionConfig.

Kept separate from ``storage_retention.py`` so the renderer module
stays pure data — no I/O, no TOML parsing — which keeps it easy to
test and import from tools that build a config in-memory (e.g., a
future ``loom cluster render`` step).
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from loom.storage_retention import RetentionConfig, RetentionRule

_REQUIRED_TOP_LEVEL = {"backend"}
_KNOWN_RULE_FIELDS = {"bucket", "strategy", "days", "hours", "rule_id"}


def load_retention_config(path: Path) -> RetentionConfig:
    """Parse storage-lifecycle.toml.

    Raises ``ValueError`` with operator-actionable messages on:
    - missing/empty file
    - unknown top-level keys
    - unknown per-rule keys
    - any constraint violation surfaced by ``RetentionRule.__post_init__``
      or ``RetentionConfig.__post_init__``.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"storage-lifecycle config not found: {path}",
        )

    raw = tomllib.loads(path.read_text(encoding="utf-8"))

    missing = _REQUIRED_TOP_LEVEL - set(raw.keys())
    if missing:
        raise ValueError(
            f"storage-lifecycle.toml missing required fields: "
            f"{sorted(missing)}",
        )

    backend = str(raw["backend"]).strip()
    rules_raw: list[dict[str, Any]] = list(raw.get("retention", []))
    rules: list[RetentionRule] = []
    for i, entry in enumerate(rules_raw):
        if not isinstance(entry, dict):
            raise ValueError(
                f"[[retention]] entry #{i} must be a TOML table; "
                f"got {type(entry).__name__}",
            )
        unknown = set(entry.keys()) - _KNOWN_RULE_FIELDS
        if unknown:
            raise ValueError(
                f"[[retention]] entry #{i} (bucket={entry.get('bucket')!r}) "
                f"has unknown keys: {sorted(unknown)} "
                f"(known: {sorted(_KNOWN_RULE_FIELDS)})",
            )
        rules.append(RetentionRule(
            bucket=str(entry["bucket"]),
            strategy=entry["strategy"],
            days=entry.get("days"),
            hours=entry.get("hours"),
            rule_id=entry.get("rule_id"),
        ))
    return RetentionConfig(backend=backend, rules=tuple(rules))
