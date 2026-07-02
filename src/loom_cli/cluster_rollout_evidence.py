"""Version-compatible rollout evidence helpers.

These helpers collect operator evidence without relying on brittle shell
templates or exposing secret-bearing Docker/container metadata.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any, Literal

ClusterStatusFormat = Literal["table", "json"]


def _diagnostic(
    code: str,
    message: str,
    **details: str,
) -> dict[str, str]:
    return {"code": code, "message": message, **details}


def normalize_cluster_status_format(
    requested_format: str,
) -> tuple[ClusterStatusFormat, list[dict[str, str]]]:
    value = requested_format.strip().lower()
    if value == "text":
        return "table", [
            _diagnostic(
                "cluster_status_format_alias",
                "`loom cluster status --format text` is a legacy spelling; using `table`.",
                requested_format=requested_format,
                normalized_format="table",
            ),
        ]
    if value == "table":
        return "table", []
    if value == "json":
        return "json", []
    raise ValueError(
        "cluster status format must be one of table, json, or legacy alias text; "
        f"got {requested_format!r}"
    )


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def build_docker_image_evidence(
    inspect_docs: list[dict[str, Any]],
    *,
    expected_repo_tags: list[str],
) -> dict[str, Any]:
    images: list[dict[str, Any]] = []
    observed_tags: set[str] = set()
    diagnostics: list[dict[str, str]] = []

    for index, doc in enumerate(inspect_docs):
        repo_tags = _string_list(doc.get("RepoTags"))
        repo_digests = _string_list(doc.get("RepoDigests"))
        if "RepoTags" not in doc:
            diagnostics.append(
                _diagnostic(
                    "docker_repo_tags_unavailable",
                    "Docker image inspect output did not include RepoTags.",
                    image_index=str(index),
                )
            )
        observed_tags.update(repo_tags)
        images.append(
            {
                "id": str(doc.get("Id") or ""),
                "repo_tags": repo_tags,
                "repo_digests": repo_digests,
            }
        )

    for expected in expected_repo_tags:
        if expected not in observed_tags:
            diagnostics.append(
                _diagnostic(
                    "docker_expected_repo_tag_missing",
                    "Expected Docker repo tag was not present in image inspect RepoTags.",
                    expected_repo_tag=expected,
                )
            )

    return {
        "schema_version": 1,
        "ok": not diagnostics,
        "images": images,
        "expected_repo_tags": expected_repo_tags,
        "diagnostics": diagnostics,
    }


def load_docker_inspect_json(path: Path) -> list[dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        raise ValueError("Docker inspect JSON must be a list of objects")
    return raw


def docker_image_inspect(images: list[str]) -> list[dict[str, Any]]:
    completed = subprocess.run(
        ["docker", "image", "inspect", *images],
        check=False,
        text=True,
        capture_output=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or "docker image inspect failed")
    raw = json.loads(completed.stdout)
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        raise ValueError("docker image inspect returned unexpected JSON")
    return raw


def render_rollout_evidence_json(evidence: dict[str, Any]) -> str:
    return json.dumps(evidence, indent=2, sort_keys=True) + "\n"
