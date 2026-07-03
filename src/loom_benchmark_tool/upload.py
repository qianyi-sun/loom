"""Walk a converted task dir and upload every file to MinIO under
`prefix`. One-shot put_object per file — these bundles are small (≤2
MB for HumanEval) so we don't need multipart."""

from __future__ import annotations

from pathlib import Path

from loom.trajectory.storage import ObjectStore
from loom_benchmark_tool.dockerfile_safety import validate_task_dir_dockerfiles


async def upload_task_dir(
    *, store: ObjectStore, bucket: str, prefix: str, task_dir: Path,
) -> int:
    """Returns the number of objects uploaded.

    Refuses empty `prefix` (would spray the entire task dir under the
    bucket root) and refuses any prefix segment that is `..` or starts
    at an absolute root — mirrors the `ObjectStore.download_prefix`
    contract so the round-trip is symmetric."""
    if not prefix:
        raise ValueError(
            "upload_task_dir requires a non-empty prefix; refusing to "
            "spray task files under bucket root",
        )
    if ".." in Path(prefix).parts or prefix.startswith("/"):
        raise ValueError(
            f"upload_task_dir prefix {prefix!r} contains traversal or "
            f"absolute root; reject",
        )
    if not prefix.endswith("/"):
        prefix = prefix + "/"
    validate_task_dir_dockerfiles(task_dir)
    count = 0
    for path in sorted(task_dir.rglob("*")):
        if path.is_dir():
            continue
        rel = path.relative_to(task_dir).as_posix()
        await store.put_object(
            bucket=bucket, key=prefix + rel, body=path.read_bytes(),
        )
        count += 1
    return count
