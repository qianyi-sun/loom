#!/usr/bin/env python3
"""Package LiteLLM's offline metadata while building the Harbor image."""

from __future__ import annotations

import importlib.metadata
import json
import re
from pathlib import Path
from urllib.request import urlopen

BACKUP = "litellm/model_prices_and_context_window_backup.json"


def _validate(payload: bytes) -> None:
    data = json.loads(payload)
    if (
        not isinstance(data, dict)
        or not data
        or not all(isinstance(entry, dict) for entry in data.values())
    ):
        raise ValueError("model metadata must be a non-empty object of model entries")
    if not any(
        name not in {"sample_spec", "fallback_generalizations"} and entry.get("litellm_provider")
        for name, entry in data.items()
    ):
        raise ValueError("model metadata contains no provider entries")


def prepare_model_cost_map() -> str:
    # Looking up package metadata must not import LiteLLM and trigger its download.
    distribution = importlib.metadata.distribution("litellm")
    target = Path(str(distribution.locate_file(BACKUP)))
    try:
        _validate(target.read_bytes())
    except (OSError, ValueError):
        pass
    else:
        return f"LiteLLM {distribution.version}: using bundled offline model metadata"

    version = distribution.version
    if not re.fullmatch(r"[0-9]+(?:\.[0-9]+)+(?:[a-zA-Z0-9.-]+)?", version):
        raise RuntimeError(f"LiteLLM {version}: cannot select an exact release metadata snapshot")
    url = (
        f"https://raw.githubusercontent.com/BerriAI/litellm/v{version}/"
        "model_prices_and_context_window.json"
    )
    try:
        with urlopen(url, timeout=30) as response:
            payload = response.read()
        _validate(payload)
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            f"LiteLLM {version}: bundled model metadata is missing or invalid and the "
            f"matching release snapshot is unavailable or invalid ({url}); "
            "rebuild with a package containing valid metadata or restore access to this release. "
            "Runtime network fallback is disabled."
        ) from exc
    target.write_bytes(payload)
    target.chmod(0o644)
    return f"LiteLLM {version}: packaged offline model metadata from {url}"


if __name__ == "__main__":
    print(prepare_model_cost_map())
