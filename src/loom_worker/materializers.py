"""Materializer protocol + implementations for `bundle["source"]`.

Before this module the worker's `_materialize_task_dir` was a 100-line
if/elif chain on `source.startswith("hf://" | "fixture://" | "s3://")`,
each branch with its own error handling + tempdir-cleanup. Adding a
new scheme meant editing one giant function; testing one branch in
isolation meant mocking the others.

Now: a `Materializer` is anything that can answer `matches(source)`
and `materialize(...)`. The worker iterates the registered list and
dispatches to the first that claims the source. Each impl owns its
own cleanup-on-failure.

To add a scheme (e.g. `gs://` for GCS, `file://` for direct local
mounts), implement the protocol + add an instance to
`build_default_materializers()`. No edits to existing impls.

The runtime contract — `task_dir` is the empty tempdir the dispatcher
prepared, and the impl returns the same path filled with the bundle's
contents. If the impl can't materialize (source not for it),
`matches()` returns False and the dispatcher tries the next. If
`materialize()` raises, the dispatcher cleans the tempdir and lets
the exception propagate.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
from collections.abc import Iterable
from pathlib import Path
from typing import Protocol, runtime_checkable
from uuid import UUID

from loom.trajectory.storage import ObjectStore

logger = logging.getLogger(__name__)


@runtime_checkable
class Materializer(Protocol):
    """Knows how to populate a `task_dir` from a `bundle["source"]`
    string.

    Implementations are stateless wrt. the trial — instance-level
    configuration (cache dirs, object stores, fixtures roots) is
    passed at construction time, not on `materialize`.
    """

    def matches(self, source: str | None) -> bool:
        """True if this materializer claims `source`. Called by the
        dispatcher to pick a handler. None / empty / unknown returns
        False."""
        ...

    async def materialize(
        self, *, source: str, task_dir: Path, trial_id: UUID,
    ) -> Path:
        """Populate `task_dir` (an existing empty dir) with the bundle.
        Return `task_dir` on success; raise on failure."""
        ...


def _parse_hf_source(source: str) -> tuple[str, str, str]:
    """Parse `hf://{org}/{repo}@{rev}/{path}` into
    (repo_id, revision, path). Revision defaults to "main" if absent.
    """
    if not source.startswith("hf://"):
        raise ValueError(f"not an hf:// URL: {source!r}")
    without_scheme = source[len("hf://"):]
    if "/" not in without_scheme:
        raise ValueError(f"hf:// URL missing repo: {source!r}")
    org, rest = without_scheme.split("/", 1)
    if "@" in rest:
        repo, after_at = rest.split("@", 1)
        revision, _, path = after_at.partition("/")
    else:
        repo, _, path = rest.partition("/")
        revision = "main"
    if not org or not repo:
        raise ValueError(f"hf:// URL missing org or repo: {source!r}")
    return f"{org}/{repo}", revision, path


class HFMaterializer:
    """`hf://{org}/{repo}@{rev}/{path}` — snapshot_download the bundle.

    Uses `huggingface_hub.snapshot_download` with `allow_patterns` so
    only the one bundle's files transfer, not the entire repo. When
    `cache_dir` is set, snapshots persist there across worker
    restarts; otherwise HF defaults to `~/.cache/huggingface/`.
    """

    def __init__(self, cache_dir: Path | None = None) -> None:
        self._cache_dir = cache_dir

    def matches(self, source: str | None) -> bool:
        return isinstance(source, str) and source.startswith("hf://")

    async def materialize(
        self, *, source: str, task_dir: Path, trial_id: UUID,
    ) -> Path:
        # Lazy import: huggingface_hub is heavy and only needed when an
        # hf:// source actually shows up. Workers in pure-S3 deployments
        # skip the import entirely.
        from huggingface_hub import snapshot_download

        repo_id, revision, path = _parse_hf_source(source)
        if not path:
            raise ValueError(f"hf:// URL missing bundle path: {source!r}")
        pattern_root = path.rstrip("/")
        snapshot = await asyncio.to_thread(
            snapshot_download,
            repo_id=repo_id,
            revision=revision,
            repo_type="dataset",
            allow_patterns=[f"{pattern_root}/*"],
            cache_dir=str(self._cache_dir) if self._cache_dir else None,
        )
        bundle_dir = Path(snapshot) / pattern_root
        if not bundle_dir.is_dir():
            raise FileNotFoundError(
                f"hf:// bundle dir not found after snapshot: {bundle_dir}",
            )
        # Flatten bundle contents into the root of task_dir (mirrors
        # what the s3:// impl produces).
        for src_path in bundle_dir.iterdir():
            dst = task_dir / src_path.name
            if src_path.is_dir():
                shutil.copytree(src_path, dst, dirs_exist_ok=True)
            else:
                shutil.copy2(src_path, dst)
        logger.info(
            "materialized_task_dir trial=%s hf=%s repo=%s path=%s",
            trial_id, source, repo_id, path,
        )
        return task_dir


class FixtureMaterializer:
    """`fixture://<task_id>` — copy from `<fixtures_root>/<task_id>/`.

    Dev compose mounts `tests/fixtures/tasks/` into the worker so the
    canary hello-world trial runs end-to-end. In production
    `fixtures_root` is None and this materializer no-ops with a
    warning — production uses s3:// for benchmark-imported bundles."""

    def __init__(self, fixtures_root: Path | None = None) -> None:
        self._root = fixtures_root

    def matches(self, source: str | None) -> bool:
        return isinstance(source, str) and source.startswith("fixture://")

    async def materialize(
        self, *, source: str, task_dir: Path, trial_id: UUID,
    ) -> Path:
        task_id = source[len("fixture://"):]
        if self._root is None:
            logger.warning(
                "materialize_task_dir trial=%s fixture:// source %r but "
                "fixtures_root unset; leaving dir empty",
                trial_id, source,
            )
            return task_dir
        src = self._root / task_id
        if not src.is_dir():
            logger.warning(
                "materialize_task_dir trial=%s fixture %r not found at %s; "
                "leaving dir empty",
                trial_id, task_id, src,
            )
            return task_dir
        # dirs_exist_ok because the dispatcher pre-created task_dir.
        shutil.copytree(src, task_dir, dirs_exist_ok=True)
        logger.info(
            "materialized_task_dir trial=%s fixture=%s from=%s",
            trial_id, task_id, src,
        )
        return task_dir


class S3Materializer:
    """`s3://bucket/prefix/` — pull every object via `download_prefix`.

    Benchmark-imported tasks follow this shape. An s3:// URL without a
    key prefix (`s3://bucket/`) is rejected with an empty dir + warning
    — without a prefix, download_prefix would drain the entire bucket
    into one trial's workspace.
    """

    def __init__(self, object_store: ObjectStore) -> None:
        self._object_store = object_store

    def matches(self, source: str | None) -> bool:
        return isinstance(source, str) and source.startswith("s3://")

    async def materialize(
        self, *, source: str, task_dir: Path, trial_id: UUID,
    ) -> Path:
        without_scheme = source[len("s3://"):]
        if "/" not in without_scheme:
            logger.warning(
                "bundle source %s has no key prefix; skipping materialize",
                source,
            )
            return task_dir
        bucket, prefix = without_scheme.split("/", 1)
        if not prefix:
            logger.warning(
                "bundle source %s has empty key prefix; refusing to "
                "drain entire bucket — leaving task_dir empty",
                source,
            )
            return task_dir
        count = await self._object_store.download_prefix(
            bucket=bucket, prefix=prefix, out_dir=task_dir,
        )
        logger.info(
            "materialized_task_dir trial=%s objects=%d source=%s",
            trial_id, count, source,
        )
        return task_dir


def build_default_materializers(
    *,
    object_store: ObjectStore,
    fixtures_root: Path | None = None,
    benchmark_cache: Path | None = None,
) -> tuple[Materializer, ...]:
    """The set the worker registers at startup. Tests can pass a
    different tuple to exercise individual materializers in isolation.
    Order matters: dispatcher picks the first matching impl."""
    return (
        HFMaterializer(cache_dir=benchmark_cache),
        FixtureMaterializer(fixtures_root=fixtures_root),
        S3Materializer(object_store=object_store),
    )


async def dispatch_materialize(
    *,
    source: str | None,
    task_dir: Path,
    trial_id: UUID,
    materializers: Iterable[Materializer],
) -> Path:
    """Iterate `materializers`; the first whose `matches(source)`
    returns True handles the bundle. On any exception the caller-
    prepared `task_dir` is removed before the exception propagates so
    failed claims don't leak `/tmp` inodes.

    Unmatched sources (None, `git+...`, unknown schemes) leave the
    `task_dir` empty + log at INFO. The operator runbook documents
    the volume-mount alternative for those."""
    for m in materializers:
        if m.matches(source):
            try:
                return await m.materialize(
                    source=source,  # type: ignore[arg-type]  # matches() ensures str
                    task_dir=task_dir,
                    trial_id=trial_id,
                )
            except BaseException:
                shutil.rmtree(task_dir, ignore_errors=True)
                raise
    logger.info(
        "materialize_task_dir trial=%s left dir empty (source=%r)",
        trial_id, source,
    )
    return task_dir
