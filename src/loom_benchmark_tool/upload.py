"""Walk a converted task dir and upload every file to MinIO under
`prefix`. One-shot put_object per file — these bundles are small (≤2
MB for HumanEval) so we don't need multipart."""

from __future__ import annotations

from pathlib import Path, PurePosixPath

from loom.task_image_bundle_manifest import (
    TaskImageBundleContentManifestV1,
    capture_task_image_bundle_manifest,
    parse_task_image_bundle_manifest,
    read_verified_task_image_bundle_file,
    task_image_bundle_manifest_key,
)
from loom.trajectory.storage import (
    BUNDLE_FILE_METADATA_NAME,
    ObjectStore,
    upload_bundle_file_metadata,
)
from loom_benchmark_tool.dockerfile_safety import validate_task_dir_dockerfiles


async def upload_task_dir(
    *,
    store: ObjectStore,
    bucket: str,
    prefix: str,
    task_dir: Path,
    content_manifest: TaskImageBundleContentManifestV1 | None = None,
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
            f"upload_task_dir prefix {prefix!r} contains traversal or absolute root; reject",
        )
    if not prefix.endswith("/"):
        prefix = prefix + "/"
    if content_manifest is not None:
        manifest = parse_task_image_bundle_manifest(
            content_manifest.canonical_bytes, expected_sha256=content_manifest.digest,
        )
        manifest_key = task_image_bundle_manifest_key(manifest.digest)
        if (
            prefix != PurePosixPath(prefix).as_posix() + "/" or "\\" in prefix
            or any(ord(char) < 32 or ord(char) == 127 for char in prefix)
            or not prefix.endswith(f"/{manifest.digest}/") or manifest_key.startswith(prefix)
            or any(len((prefix + name).encode("utf-8")) > 1024 for name in (
                *(item.path for item in manifest.files), BUNDLE_FILE_METADATA_NAME,
            ))
        ):
            raise ValueError("content-addressed bundle upload prefix is invalid")
        if capture_task_image_bundle_manifest(task_dir) != manifest:
            raise ValueError("staged bundle no longer matches registered content")
        validate_task_dir_dockerfiles(task_dir)
        for file in manifest.files:
            body = read_verified_task_image_bundle_file(task_dir, file)
            await store.put_object(bucket=bucket, key=prefix + file.path, body=body)
        await store.put_object(
            bucket=bucket, key=prefix + BUNDLE_FILE_METADATA_NAME,
            body=manifest.mode_metadata_bytes,
        )
        # Publish last, outside the data prefix: legacy materializers must not
        # count/hash this new transport artifact as an authored task file.
        await store.put_object(bucket=bucket, key=manifest_key, body=manifest.canonical_bytes)
        return len(manifest.files)
    validate_task_dir_dockerfiles(task_dir)
    count = 0
    for path in sorted(task_dir.rglob("*")):
        if path.is_dir():
            continue
        rel = path.relative_to(task_dir).as_posix()
        if rel == BUNDLE_FILE_METADATA_NAME:
            continue
        await store.put_object(
            bucket=bucket,
            key=prefix + rel,
            body=path.read_bytes(),
        )
        count += 1
    await upload_bundle_file_metadata(
        store=store,
        bucket=bucket,
        prefix=prefix,
        task_dir=task_dir,
    )
    return count
