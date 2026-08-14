"""Exact S3/MinIO object deletion adapter for staging lifecycle GC."""

from __future__ import annotations

import hashlib
from collections import defaultdict, deque
from collections.abc import Callable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, TypeVar
from uuid import UUID

from botocore.exceptions import ClientError

from loom.data_lifecycle_gc import LifecycleGcExecutionError, RegisteredObject

_T = TypeVar("_T")


def _not_found(exc: ClientError) -> bool:
    code = str(exc.response.get("Error", {}).get("Code", ""))
    return code in {"404", "NoSuchKey", "NoSuchVersion", "NotFound"}


class S3ExactObjectDeleter:
    """Verify exact version/bytes before deleting one registered object."""

    def __init__(
        self,
        client: Any,
        *,
        read_chunk_bytes: int = 1024 * 1024,
        workers: int = 32,
    ) -> None:
        if read_chunk_bytes <= 0:
            raise ValueError("read_chunk_bytes must be positive")
        if not 1 <= workers <= 64:
            raise ValueError("GC object workers must be in [1, 64]")
        self._client = client
        self._read_chunk_bytes = read_chunk_bytes
        self._workers = workers

    @staticmethod
    def _params(item: RegisteredObject) -> dict[str, str]:
        params = {"Bucket": item.bucket, "Key": item.object_key}
        if item.version_id is not None:
            params["VersionId"] = item.version_id
        return params

    def _verify_identity(self, item: RegisteredObject) -> None:
        params = self._params(item)
        response = self._client.head_object(**params)
        if int(response.get("ContentLength", -1)) != item.size_bytes:
            raise LifecycleGcExecutionError(
                f"registered object size drifted: {item.bucket}/{item.object_key}"
            )
        observed_version = response.get("VersionId")
        if item.version_id is not None and not (
            observed_version == item.version_id
            or (item.version_id == "null" and observed_version is None)
        ):
            raise LifecycleGcExecutionError(
                f"registered object version drifted: {item.bucket}/{item.object_key}"
            )
        if item.content_sha256 is None:
            return
        body = self._client.get_object(**params)["Body"]
        digest = hashlib.sha256()
        try:
            while chunk := body.read(self._read_chunk_bytes):
                digest.update(chunk)
        finally:
            body.close()
        if digest.hexdigest() != item.content_sha256:
            raise LifecycleGcExecutionError(
                f"registered object digest drifted: {item.bucket}/{item.object_key}"
            )

    def delete_exact(self, item: RegisteredObject) -> None:
        self._verify_identity(item)
        self._client.delete_object(**self._params(item))

    def exact_absent(self, item: RegisteredObject) -> bool:
        try:
            self._client.head_object(**self._params(item))
        except ClientError as exc:
            if _not_found(exc):
                return True
            raise
        return False

    def _bounded_map(
        self,
        items: Sequence[RegisteredObject],
        operation: Callable[[RegisteredObject], _T],
    ) -> dict[UUID, _T]:
        """Run exact object calls concurrently without unbounded future submission."""
        pending: deque[tuple[RegisteredObject, Future[_T]]] = deque()
        iterator = iter(items)
        results: dict[UUID, _T] = {}
        with ThreadPoolExecutor(
            max_workers=self._workers, thread_name_prefix="lifecycle-s3"
        ) as pool:

            def fill() -> None:
                while len(pending) < self._workers * 2:
                    try:
                        item = next(iterator)
                    except StopIteration:
                        return
                    pending.append((item, pool.submit(operation, item)))

            fill()
            while pending:
                item, future = pending.popleft()
                results[item.id] = future.result()
                fill()
        return results

    def delete_exact_many(self, items: Sequence[RegisteredObject]) -> None:
        """Verify exact bytes first, then use explicit S3 batches of at most 1000 keys."""
        if not items:
            return
        self._bounded_map(items, self._verify_identity)
        by_bucket: dict[str, list[RegisteredObject]] = defaultdict(list)
        for item in items:
            by_bucket[item.bucket].append(item)
        for bucket, bucket_items in sorted(by_bucket.items()):
            for offset in range(0, len(bucket_items), 1000):
                batch = bucket_items[offset : offset + 1000]
                objects = [
                    {
                        "Key": item.object_key,
                        **({"VersionId": item.version_id} if item.version_id is not None else {}),
                    }
                    for item in batch
                ]
                response = self._client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": objects, "Quiet": False},
                )
                errors = response.get("Errors", ())
                if errors:
                    raise LifecycleGcExecutionError(
                        f"exact object batch delete failed for {bucket}: {len(errors)} errors"
                    )
                deleted = response.get("Deleted")
                if not isinstance(deleted, list):
                    raise LifecycleGcExecutionError(
                        f"exact object batch delete returned no identity evidence for {bucket}"
                    )
                expected = {(item.object_key, item.version_id or "") for item in batch}
                observed: set[tuple[str, str]] = set()
                for item in deleted:
                    if not isinstance(item, dict):
                        continue
                    key = str(item.get("Key", ""))
                    version = str(item.get("VersionId", ""))
                    if (
                        item.get("VersionId") is None
                        and (key, "null") in expected
                        and (key, "") not in expected
                    ):
                        version = "null"
                    observed.add((key, version))
                if observed != expected:
                    raise LifecycleGcExecutionError(
                        f"exact object batch delete identity drifted for {bucket}"
                    )

    def exact_absent_many(self, items: Sequence[RegisteredObject]) -> dict[UUID, bool]:
        return self._bounded_map(items, self.exact_absent)


__all__ = ["S3ExactObjectDeleter"]
