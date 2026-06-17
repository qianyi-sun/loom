"""Pin Pydantic `model_fields` shape per service to a JSON snapshot.

This locks the pre-#146 settings surface. The migration (codegen
from `config/loom-schema.toml`) must reproduce the same field
names, annotations, defaults, and required-ness byte-for-byte.

To regenerate snapshots after an intentional schema change, run:
    pytest tests/loom_config/test_settings_snapshot.py --snapshot-update
(only on the rare deliberate-field-change PR).
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from loom_control_plane.config import ControlPlaneSettings
from loom_llm_gateway.config import GatewaySettings
from loom_service.config import LoomServiceSettings
from loom_worker.config import WorkerSettings

_SNAPSHOT_DIR = Path(__file__).parent / "snapshots"

_CASES = [
    ("control_plane", ControlPlaneSettings),
    ("llm_gateway", GatewaySettings),
    ("loom_service", LoomServiceSettings),
    ("worker", WorkerSettings),
]


def _dump_fields(cls: type) -> dict[str, Any]:
    """Stable shape: {field_name: {annotation, default, required}}.

    `annotation` is `str(field.annotation)` (pydantic's canonical
    repr stays consistent across runs); `default` is `repr(...)` so
    SecretStr defaults and PydanticUndefined serialize cleanly;
    `required` is whether the field has a default.
    """
    out: dict[str, dict[str, Any]] = {}
    for name, field in cls.model_fields.items():
        out[name] = {
            "annotation": str(field.annotation),
            "default": repr(field.default),
            "required": field.is_required(),
        }
    return out


@pytest.mark.parametrize("service_name,cls", _CASES)
def test_settings_shape_matches_snapshot(service_name: str, cls: type) -> None:
    snapshot_path = _SNAPSHOT_DIR / f"{service_name}.json"
    actual = _dump_fields(cls)
    if os.environ.get("UPDATE_SNAPSHOTS") == "1":
        snapshot_path.write_text(
            json.dumps(actual, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    expected = json.loads(snapshot_path.read_text(encoding="utf-8"))
    assert actual == expected, (
        f"{service_name} Settings shape drifted from snapshot.\n"
        f"If intentional, regenerate with UPDATE_SNAPSHOTS=1 pytest "
        f"{snapshot_path.parent.name}/test_settings_snapshot.py"
    )
