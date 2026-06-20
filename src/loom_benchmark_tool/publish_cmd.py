"""`python -m loom_benchmark_tool publish <benchmark>` — convert + push
the bundles to a HuggingFace dataset repo.

This is the Loom-team-side operation. Run it ONCE per benchmark per
release (or on adapter changes). It:

1. Fetches the upstream source (cached locally).
2. Walks every BenchmarkInstance, converts to a Loom task bundle.
3. Stages the bundles in a single tempdir laid out as
   `{instance_id}/<bundle files…>`.
4. Computes a per-task `sha256` checksum.
5. Writes `manifest.json` at the root.
6. Pushes the whole tree to `{org}/loom-benchmark-{benchmark}` on HF
   in one upload commit.

Per-deploy seeding then calls `register` (separate subcommand) which
reads the manifest off HF, validates each per-task config, and inserts
task rows pointing at
`hf://{org}/loom-benchmark-{benchmark}@<revision>/{instance_id}/`. No
upstream conversion needed per-deploy.

The manifest schema lives at the top of this file so the register
command + worker can read it without reaching back into adapter code.
"""

from __future__ import annotations

import json
import tempfile
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from huggingface_hub import HfApi
from loom_benchmarks.fetch import fetch_upstream
from loom_benchmarks.registry import REGISTRY
from loom_benchmarks.util import sha256_of_dir

from loom.license_policy import tags_with_license_execution_policy
from loom_benchmark_tool.import_cmd import _select_instances, _validate_instance_id
from loom_benchmark_tool.manifest import (
    MANIFEST_FILENAME,
    load_task_config_from_bundle,
    repo_id_for,
)

# Manifest schema version. Bump when the per-task field set changes in
# a way the register/worker code needs to fork on. The shape:
#
# {
#   "schema_version": int,       # 1 = legacy, 2 = series/tags, 3 = task_config
#   "benchmark_id": str,
#   "display_name": str,
#   "series": str | null,        # v2+: grouping label ("aime", …)
#   "license_spdx": str,
#   "license_url": str,
#   "upstream_kind": str,
#   "upstream_locator": str,
#   "upstream_revision": str,
#   "loom_adapter_version": str | null,
#   "published_at": str (ISO-8601 UTC),
#   "splits": list[str],
#   "task_count": int,
#   "tasks": [
#     {
#       "task_id": str,            # the DB-level id (benchmark/instance)
#       "instance_id": str,        # the adapter-level id
#       "hf_path": str,            # path within the HF repo, ends in "/"
#       "checksum": str,           # 64-char sha256 hex over the bundle tree
#       "license_spdx": str,
#       "split": str,
#       "tags": dict[str, str],    # v2+: open-ended task metadata
#       "task_config": dict,       # v3+: validated raw task.toml payload
#     }
#   ]
# }
#
# Legacy v1 manifests have no `series` or `tags`; the register path
# treats those as `series=None` and `tags={}` so the migration is
# rolling. v1/v2 manifests also have no `task_config`; the register path keeps
# them as explicit non-runnable placeholders until republished or backfilled.
MANIFEST_SCHEMA_VERSION = 3


def _safe_dirname(instance_id: str) -> str:
    """Turn `HumanEval/0` into `HumanEval_0` so it's a single HF path
    segment. The reverse mapping isn't needed by the worker — only the
    manifest's `hf_path` field is consulted, and that's verbatim. The
    `task_id` keeps the original slash form for the DB."""
    return instance_id.replace("/", "_")


def _bundle_checksum(bundle_dir: Path) -> str:
    """Stable sha256 over every file in the bundle. Sorted by relative
    path so independent re-builds produce the same digest if the
    bundle contents match byte-for-byte. Same hashing scheme the
    import command writes into `tasks.checksum`."""
    return cast(str, sha256_of_dir(bundle_dir))


def run_publish(
    *,
    benchmark: str,
    hf_org: str,
    hf_token: str,
    cache_dir: Path,
    limit: int | None = None,
    instance_ids: Iterable[str] | None = None,
    private: bool = False,
    refresh: bool = False,
) -> dict[str, Any]:
    """Convert + push. Returns
    `{"published": N, "warnings": M, "repo_id": str, "revision": str}`.

    The HF dataset repo is created (idempotently) under the given org,
    then every converted bundle is uploaded under
    `{instance_id_safe}/` and `manifest.json` is written at the root.
    A single `HfApi.upload_folder` call wraps the whole tree in one
    commit, so partial failures don't leave half-published trees.

    `private=True` makes the dataset private (gated by HF token). The
    default is public — Loom-team-shipped benchmarks are all
    redistributable per their upstream licenses; if you're publishing
    a license-restricted benchmark, pass private=True and document the
    access path separately.
    """
    adapter = REGISTRY[benchmark]
    cache_dir.mkdir(parents=True, exist_ok=True)
    source_dir = fetch_upstream(
        adapter.upstream_source,
        cache_root=cache_dir,
        refresh=refresh,
    )
    selected_instances = _select_instances(
        adapter,
        source_dir=source_dir,
        instance_ids=instance_ids,
        limit=limit,
    )

    api = HfApi(token=hf_token)
    repo_id = repo_id_for(hf_org, benchmark)
    # `exist_ok=True` — first publish creates, subsequent re-publish
    # under the same id just adds a commit.
    api.create_repo(
        repo_id=repo_id,
        repo_type="dataset",
        private=private,
        exist_ok=True,
    )

    stats = {"published": 0, "warnings": 0}
    task_entries: list[dict[str, Any]] = []

    with tempfile.TemporaryDirectory() as staging:
        staging_dir = Path(staging)

        for split, inst in selected_instances:
            _validate_instance_id(inst.instance_id)
            safe_name = _safe_dirname(inst.instance_id)
            bundle_dir = staging_dir / safe_name
            bundle_dir.mkdir(parents=True, exist_ok=True)
            converted = adapter.convert_instance(inst, out_dir=bundle_dir)
            checksum = _bundle_checksum(bundle_dir)
            # Sanity-check the adapter's claimed checksum matches
            # our recomputed one. Misalignment would mean the DB
            # row's checksum (set at register time) won't match what
            # the worker fetches. Fail loudly here rather than at
            # claim time on a remote box.
            if converted.checksum != checksum:
                stats["warnings"] += 1
            task_config = load_task_config_from_bundle(bundle_dir)
            task_entries.append(
                {
                    "task_id": converted.task_id,
                    "instance_id": inst.instance_id,
                    "hf_path": f"{safe_name}/",
                    "checksum": checksum,
                    "license_spdx": converted.license_spdx,
                    "split": split,
                    # PR-1 (series/tags): per-task metadata. Empty for
                    # adapters that haven't been reworked yet - register
                    # treats absent + {} identically.
                    "tags": tags_with_license_execution_policy(
                        inst.tags,
                        getattr(adapter, "license_execution_policy", None),
                    ),
                    "task_config": task_config,
                }
            )
            stats["published"] += 1

        manifest = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "benchmark_id": adapter.name,
            "display_name": adapter.display_name,
            # PR-1: series grouping. Adapters declare `series = "aime"`
            # (or similar) as a class attr; absence → standalone.
            "series": getattr(adapter, "series", None),
            "license_spdx": adapter.license_spdx,
            "license_url": adapter.license_url,
            "upstream_kind": adapter.upstream_source.kind,
            "upstream_locator": adapter.upstream_source.locator,
            "upstream_revision": adapter.upstream_source.revision or "",
            "loom_adapter_version": getattr(adapter, "version", None),
            "published_at": datetime.now(UTC).isoformat(),
            "splits": list(adapter.splits),
            "task_count": len(task_entries),
            "tasks": task_entries,
        }
        (staging_dir / MANIFEST_FILENAME).write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        )

        commit_info = api.upload_folder(
            repo_id=repo_id,
            repo_type="dataset",
            folder_path=str(staging_dir),
            commit_message=(
                f"Publish {adapter.name}: {len(task_entries)} task(s) "
                f"({adapter.upstream_source.locator})"
            ),
        )

    return {
        "published": stats["published"],
        "warnings": stats["warnings"],
        "repo_id": repo_id,
        "revision": getattr(commit_info, "oid", "main"),
    }
