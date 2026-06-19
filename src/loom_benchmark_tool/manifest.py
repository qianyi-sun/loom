from __future__ import annotations

import json
import tomllib
from pathlib import Path
from typing import Any

from loom.models.task import TaskConfig

MANIFEST_FILENAME = "manifest.json"


def repo_id_for(org: str, benchmark: str) -> str:
    """`PRHW/loom-benchmark-humaneval`.

    Keep this in one place so publish, register, and the worker derive the same
    dataset repository id from a benchmark id.
    """
    return f"{org}/loom-benchmark-{benchmark}"


def read_manifest_from_hf(
    *,
    hf_org: str,
    benchmark: str,
    hf_token: str | None = None,
    revision: str = "main",
) -> dict[str, Any]:
    """Download + parse manifest.json from a published HF dataset repo."""
    from huggingface_hub import hf_hub_download

    path = hf_hub_download(
        repo_id=repo_id_for(hf_org, benchmark),
        filename=MANIFEST_FILENAME,
        repo_type="dataset",
        revision=revision,
        token=hf_token,
    )
    data: dict[str, Any] = json.loads(Path(path).read_text())
    return data


def load_task_config_from_bundle(bundle_dir: Path) -> dict[str, Any]:
    """Read and validate the task config that makes a bundle runnable."""
    config = tomllib.loads((bundle_dir / "task.toml").read_text())
    TaskConfig.model_validate(config)
    return config
