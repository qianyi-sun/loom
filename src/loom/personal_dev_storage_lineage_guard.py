"""Fail-closed preflight for personal-dev StatefulSet storage upgrades."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

import yaml  # type: ignore[import-untyped]

_EXPECTED_STATEFUL_SETS = {"loom-dev-minio", "loom-dev-postgres"}
_MAX_MANIFEST_BYTES = 4 * 1024 * 1024


class StorageLineageGuardError(ValueError):
    """Raised when a reviewed manifest lacks the exact storage contract."""


def _claim_templates(path: Path) -> dict[str, bytes]:
    try:
        with path.open("rb") as manifest:
            payload = manifest.read(_MAX_MANIFEST_BYTES + 1)
        if not 0 < len(payload) <= _MAX_MANIFEST_BYTES:
            raise StorageLineageGuardError("manifest size is invalid")
        documents = yaml.safe_load_all(payload.decode("utf-8"))
        stateful_sets: dict[str, bytes] = {}
        for document in documents:
            if not isinstance(document, dict) or document.get("kind") != "StatefulSet":
                continue
            metadata = document.get("metadata")
            spec = document.get("spec")
            if not isinstance(metadata, dict) or not isinstance(spec, dict):
                raise StorageLineageGuardError("StatefulSet shape is invalid")
            name = metadata.get("name")
            templates = spec.get("volumeClaimTemplates")
            if (
                document.get("apiVersion") != "apps/v1"
                or metadata.get("namespace") != "loom-dev"
                or name not in _EXPECTED_STATEFUL_SETS
                or name in stateful_sets
                or not isinstance(templates, list)
                or len(templates) != 1
                or not isinstance(templates[0], dict)
            ):
                raise StorageLineageGuardError("StatefulSet storage contract is invalid")
            try:
                stateful_sets[name] = json.dumps(
                    templates,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                    allow_nan=False,
                ).encode("ascii")
            except (RecursionError, TypeError, ValueError):
                raise StorageLineageGuardError("StatefulSet claim template is invalid") from None
    except StorageLineageGuardError:
        raise
    except (OSError, UnicodeError, yaml.YAMLError):
        raise StorageLineageGuardError("manifest is invalid") from None
    if set(stateful_sets) != _EXPECTED_STATEFUL_SETS:
        raise StorageLineageGuardError("StatefulSet storage inventory is incomplete")
    return stateful_sets


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate immutable personal-dev StatefulSet claim templates."
    )
    parser.add_argument("--current", type=Path, required=True)
    parser.add_argument("--previous", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        current = _claim_templates(arguments.current)
        previous = _claim_templates(arguments.previous)
        if current != previous:
            raise StorageLineageGuardError(
                "StatefulSet claim templates differ from installed storage lineage"
            )
    except StorageLineageGuardError as exc:
        sys.stderr.write(f"personal-dev storage lineage rejected: {exc}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
