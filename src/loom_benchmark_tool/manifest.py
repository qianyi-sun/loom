from __future__ import annotations

import json
import tomllib
from pathlib import Path
from typing import Any

from loom.models.task import TaskConfig
from loom.trial.workspace import TB21_AGENT_WORKSPACE_POLICY, WorkspaceStagingPolicy

MANIFEST_FILENAME = "manifest.json"
MANIFEST_SCHEMA_VERSION = 4


def tb21_workspace_policy_isolated(value: object) -> bool:
    """Return whether persisted provenance proves the rev-6 staging gate.

    A parsed-but-different policy is not sufficient for TB2.1 activation: the
    reviewed exclusion set is part of the immutable profile contract.
    """
    if not isinstance(value, dict):
        return False
    try:
        policy = WorkspaceStagingPolicy.from_provenance(value)
    except ValueError:
        return False
    expected = WorkspaceStagingPolicy.from_provenance(TB21_AGENT_WORKSPACE_POLICY)
    return policy == expected


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
