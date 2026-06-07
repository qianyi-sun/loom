"""Walk a converted task dir and upload every file to MinIO under
`prefix`. One-shot put_object per file — these bundles are small (≤2
MB for HumanEval) so we don't need multipart."""

from __future__ import annotations

from pathlib import Path

from loom.trajectory.storage import ObjectStore


async def upload_task_dir(
    *, store: ObjectStore, bucket: str, prefix: str, task_dir: Path,
) -> int:
    """Returns the number of objects uploaded."""
    if not prefix.endswith("/"):
        prefix = prefix + "/"
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
