"""Deterministic synthesis for platform-owned fanout manifests."""

from __future__ import annotations

import re
from collections.abc import Mapping
from uuid import UUID, uuid5

from loom.pipeline.spec import ARTIFACT_TYPE_PATTERN, NAME_PATTERN


def synthesize_fanout_manifest(
    items: list[tuple[str, str, str]],
    *,
    namespace: UUID,
    item_binding_name: str,
    artifact_ids_by_output: Mapping[str, UUID] | None = None,
) -> dict[str, object]:
    if not re.fullmatch(NAME_PATTERN, item_binding_name):
        raise ValueError("invalid item binding name")
    ordered = sorted(items, key=lambda item: item[0].encode())
    if len({item[0] for item in ordered}) != len(ordered):
        raise ValueError("fanout shard keys must be unique")
    result: list[dict[str, object]] = []
    for shard_key, output_name, artifact_type in ordered:
        if not re.fullmatch(NAME_PATTERN, output_name) or not re.fullmatch(
            ARTIFACT_TYPE_PATTERN, artifact_type
        ):
            raise ValueError("invalid fanout output identity")
        artifact_id = (
            uuid5(namespace, f"{shard_key}\x00{output_name}\x00{artifact_type}")
            if artifact_ids_by_output is None
            else artifact_ids_by_output.get(output_name)
        )
        if artifact_id is None:
            raise ValueError("fanout output has no preallocated Artifact identity")
        result.append(
            {
                "shard_key": shard_key,
                "artifact_bindings": [
                    {
                        "name": item_binding_name,
                        "artifact_id": str(artifact_id),
                        "artifact_type": artifact_type,
                    }
                ],
                "parameters": {},
            }
        )
    return {"schema_version": "loom.fanout-manifest.v1", "items": result}


__all__ = ["synthesize_fanout_manifest"]
