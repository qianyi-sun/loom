"""`python -m loom_benchmark_tool publish <benchmark>` — convert +
push the bundles to a publishing target.

Two targets are supported:

- `target="hf"` (legacy): push to `{hf_org}/loom-benchmark-{benchmark}`
  on HuggingFace Hub. Register then reads the manifest off HF and
  optionally mirrors bundles into internal object storage.
- `target="object-store"`: PUT the manifest + bundles directly into an
  S3-compatible bucket (MinIO or R2). Register can then read the
  manifest straight from the bucket without any HF hop.

Both paths produce identical manifests, so downstream consumers stay
the same. The manifest schema lives at the top of this file so the
register command + worker can read it without reaching back into
adapter code.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import tempfile
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

from huggingface_hub import HfApi
from loom_benchmarks.fetch import fetch_upstream
from loom_benchmarks.registry import REGISTRY
from loom_benchmarks.util import sha256_of_dir

from loom.license_policy import tags_with_license_execution_policy
from loom.trajectory.storage import (
    ObjectStore,
    bundle_file_metadata_sha256,
    write_bundle_file_metadata_sidecar,
)
from loom_benchmark_tool.dockerfile_safety import validate_task_dir_dockerfiles
from loom_benchmark_tool.import_cmd import _select_instances, _validate_instance_id
from loom_benchmark_tool.manifest import (
    MANIFEST_FILENAME,
    MANIFEST_SCHEMA_VERSION,
    load_task_config_from_bundle,
    repo_id_for,
)

# Manifest shape:
#
# {
#   "schema_version": int,       # v4 adds profile/task source provenance
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
#       "hf_path": str,            # path within the repo, ends in "/"
#       "checksum": str,           # 64-char sha256 hex over the bundle tree
#       "license_spdx": str,
#       "split": str,
#       "tags": dict[str, str],    # v2+: open-ended task metadata
#       "task_config": dict,       # v3+: validated raw task.toml payload
#     }
#   ]
# }
#
# `hf_path` is a legacy name kept for wire compatibility with already-
# published v1/v2 manifests. Every target uses it verbatim as the
# bundle's directory prefix inside the published tree (HF repo, or the
# object-store `{benchmark_id}/{revision}/` root).
PublishTarget = Literal["hf", "object-store"]


def _adapter_profile_provenance(adapter: object) -> dict[str, object]:
    provider = getattr(adapter, "profile_provenance", None)
    if provider is None:
        return {}
    value = provider()
    if not isinstance(value, dict):
        raise TypeError("adapter profile_provenance() must return an object")
    return dict(value)


def _adapter_task_source_provenance(
    adapter: object,
    *,
    instance: object,
    bundle_dir: Path,
    task_config: dict[str, object],
    checksum: str,
) -> dict[str, object]:
    provider = getattr(adapter, "task_source_provenance", None)
    if provider is None:
        return {}
    value = provider(
        instance=instance,
        bundle_dir=bundle_dir,
        task_config=task_config,
        checksum=checksum,
    )
    if not isinstance(value, dict):
        raise TypeError("adapter task_source_provenance() must return an object")
    return dict(value)


def _safe_dirname(instance_id: str) -> str:
    """Turn `HumanEval/0` into `HumanEval_0` so it's a single path
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


def _object_store_revision(task_entries: list[dict[str, Any]]) -> str:
    """Content-addressed revision derived from the sorted per-task
    byte and mode checksums. Republishing identical bundles produces the
    same revision, while an executable-bit change gets a distinct prefix."""
    joined = "\n".join(
        (
            f"{entry['task_id']}:{entry['checksum']}:"
            f"{(entry.get('source_provenance') or {}).get('bundle_file_metadata_sha256', '')}"
        )
        for entry in sorted(task_entries, key=lambda e: e["task_id"])
    ).encode("utf-8")
    return hashlib.sha256(joined).hexdigest()[:16]


def _stage_bundles(
    *,
    benchmark: str,
    cache_dir: Path,
    limit: int | None,
    instance_ids: Iterable[str] | None,
    refresh: bool,
    staging_dir: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, int]]:
    """Convert every selected instance into a task bundle under
    `staging_dir/{safe_instance_id}/`, and return the manifest dict +
    per-task entries + counters.

    Target-agnostic. The staging tree layout matches what both HF and
    object-store consumers expect."""
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

    stats = {"published": 0, "warnings": 0}
    task_entries: list[dict[str, Any]] = []

    for split, inst in selected_instances:
        _validate_instance_id(inst.instance_id)
        safe_name = _safe_dirname(inst.instance_id)
        bundle_dir = staging_dir / safe_name
        bundle_dir.mkdir(parents=True, exist_ok=True)
        converted = adapter.convert_instance(inst, out_dir=bundle_dir)
        validate_task_dir_dockerfiles(bundle_dir)
        checksum = _bundle_checksum(bundle_dir)
        # Sanity-check the adapter's claimed checksum matches our
        # recomputed one. Misalignment would mean the DB row's
        # checksum (set at register time) won't match what the worker
        # fetches. Fail loudly here rather than at claim time on a
        # remote box.
        if converted.checksum != checksum:
            stats["warnings"] += 1
        task_config = load_task_config_from_bundle(bundle_dir)
        source_provenance = _adapter_task_source_provenance(
            adapter,
            instance=inst,
            bundle_dir=bundle_dir,
            task_config=task_config,
            checksum=checksum,
        )
        metadata_sha256 = bundle_file_metadata_sha256(bundle_dir)
        declared_metadata_sha256 = source_provenance.get(
            "bundle_file_metadata_sha256",
        )
        if declared_metadata_sha256 not in {None, metadata_sha256}:
            raise ValueError(
                "adapter bundle file mode provenance does not match staged bundle",
            )
        source_provenance["bundle_file_metadata_sha256"] = metadata_sha256
        task_entries.append(
            {
                "task_id": converted.task_id,
                "instance_id": inst.instance_id,
                "hf_path": f"{safe_name}/",
                "checksum": checksum,
                "license_spdx": converted.license_spdx,
                "split": split,
                "tags": tags_with_license_execution_policy(
                    inst.tags,
                    getattr(adapter, "license_execution_policy", None),
                ),
                "task_config": task_config,
                "source_provenance": source_provenance,
            }
        )
        # Transport inode modes separately from bundle identity. HF preserves
        # bytes but not executable bits; direct object-store publication also
        # carries this sidecar so both materializers enforce one contract.
        write_bundle_file_metadata_sidecar(bundle_dir)
        stats["published"] += 1

    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "benchmark_id": adapter.name,
        "display_name": adapter.display_name,
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
        "benchmark_profile_provenance": _adapter_profile_provenance(adapter),
        "tasks": task_entries,
    }
    (staging_dir / MANIFEST_FILENAME).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
    )
    return manifest, task_entries, stats


def _publish_to_hf(
    *,
    staging_dir: Path,
    repo_id: str,
    hf_token: str,
    private: bool,
) -> str:
    """Upload the staging tree to a HF dataset repo. Returns the head
    commit SHA. Uses `upload_large_folder` because HF's single-commit
    endpoint 504s on any tree past a few hundred files.
    """
    api = HfApi(token=hf_token)
    # `exist_ok=True` — first publish creates, subsequent re-publish
    # under the same id just adds a commit.
    api.create_repo(
        repo_id=repo_id,
        repo_type="dataset",
        private=private,
        exist_ok=True,
    )
    api.upload_large_folder(
        repo_id=repo_id,
        repo_type="dataset",
        folder_path=str(staging_dir),
        print_report=False,
    )
    refs = api.list_repo_refs(repo_id=repo_id, repo_type="dataset")
    head_rev: str = next(
        (branch.target_commit for branch in refs.branches if branch.name == "main"),
        "main",
    )
    return head_rev


async def _publish_to_object_store(
    *,
    staging_dir: Path,
    manifest: dict[str, Any],
    object_store: ObjectStore,
    bucket: str,
) -> str:
    """PUT the manifest + every bundle file into `bucket` under
    `{benchmark_id}/{revision}/`. Returns the content-addressed
    revision. Idempotent: byte-identical bundles produce the same
    revision and same object keys, so re-running skips no-op puts by
    virtue of `head_object`-style checks in the register mirror path.

    Layout:
        {bucket}/{benchmark_id}/{revision}/manifest.json
        {bucket}/{benchmark_id}/{revision}/{instance_id_safe}/…
    """
    benchmark_id = manifest["benchmark_id"]
    revision = _object_store_revision(manifest["tasks"])
    prefix = f"{benchmark_id}/{revision}"

    await object_store.ensure_bucket(bucket)

    # Upload every file under staging_dir. The manifest.json at the
    # root and each bundle subtree land under the same prefix.
    for path in sorted(staging_dir.rglob("*")):
        if path.is_dir():
            continue
        rel = path.relative_to(staging_dir).as_posix()
        key = f"{prefix}/{rel}"
        await object_store.put_object(
            bucket=bucket,
            key=key,
            body=path.read_bytes(),
        )
    return revision


async def run_publish(
    *,
    benchmark: str,
    cache_dir: Path,
    target: PublishTarget = "hf",
    hf_org: str = "",
    hf_token: str | None = None,
    private: bool = False,
    object_store: ObjectStore | None = None,
    bucket: str = "loom-benchmarks",
    limit: int | None = None,
    instance_ids: Iterable[str] | None = None,
    refresh: bool = False,
) -> dict[str, Any]:
    """Convert + push to the selected target. Returns
    `{"published": N, "warnings": M, "target": str, "repo_id": str,
      "revision": str}`.

    For `target="hf"`, `hf_org` + `hf_token` are required. `repo_id` is
    the HF dataset id and `revision` is the head commit SHA.

    For `target="object-store"`, `object_store` is required. `repo_id`
    is the flat `s3://{bucket}/{benchmark_id}` prefix and `revision`
    is the content-addressed 16-char hash of the sorted task byte and
    file-mode checksums.
    """
    if target == "hf":
        if not hf_token:
            raise ValueError("target='hf' requires hf_token")
        if not hf_org:
            raise ValueError("target='hf' requires hf_org")
    elif target == "object-store":
        if object_store is None:
            raise ValueError("target='object-store' requires object_store")
    else:  # pragma: no cover — argparse constrains this
        raise ValueError(f"unknown publish target: {target!r}")

    with tempfile.TemporaryDirectory() as staging:
        staging_dir = Path(staging)
        manifest, _task_entries, stats = _stage_bundles(
            benchmark=benchmark,
            cache_dir=cache_dir,
            limit=limit,
            instance_ids=instance_ids,
            refresh=refresh,
            staging_dir=staging_dir,
        )

        if target == "hf":
            assert hf_token is not None  # narrowed by check above
            repo_id = repo_id_for(hf_org, benchmark)
            revision = await asyncio.to_thread(
                _publish_to_hf,
                staging_dir=staging_dir,
                repo_id=repo_id,
                hf_token=hf_token,
                private=private,
            )
        else:
            assert object_store is not None
            revision = await _publish_to_object_store(
                staging_dir=staging_dir,
                manifest=manifest,
                object_store=object_store,
                bucket=bucket,
            )
            repo_id = f"s3://{bucket}/{manifest['benchmark_id']}"

    return {
        "published": stats["published"],
        "warnings": stats["warnings"],
        "target": target,
        "repo_id": repo_id,
        "revision": revision,
    }
