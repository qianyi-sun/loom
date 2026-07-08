"""State-backend plugins (#672).

The default backend snapshots per-family state as timestamped
``.tar.gz`` archives under
``s3://loom-<env>-artifacts/family-state/<batch_id>/<family_key>/``.
Each ``upload`` writes a fresh snapshot; ``state_uri`` is the exact
object key so we get audit history for free.
"""

from __future__ import annotations

import io
import tarfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from loom.trajectory.storage import ObjectStore


@dataclass
class S3ArtifactsStateBackend:
    store: ObjectStore
    bucket: str
    default_params: dict[str, Any] = field(default_factory=dict)

    def _prefix(self, batch_id: UUID, family_key: str) -> str:
        return f"family-state/{batch_id}/{family_key}/"

    def _uri_for(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    async def initialize(
        self,
        *,
        batch_id: UUID,
        family_key: str,
        params: dict[str, Any],
    ) -> str:
        # Materialise an empty snapshot so the URI is always retrievable.
        prefix = self._prefix(batch_id, family_key)
        empty_key = f"{prefix}state-{_ts()}.tar.gz"
        empty_tar = _tar_bytes_for_empty()
        await self.store.put_object(bucket=self.bucket, key=empty_key, body=empty_tar)
        return self._uri_for(empty_key)

    async def download(
        self,
        state_uri: str,
        dst: Path,
        params: dict[str, Any],
    ) -> None:
        key = state_uri.removeprefix(f"s3://{self.bucket}/")
        data = await self.store.get_object(bucket=self.bucket, key=key)
        dst.mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
            tf.extractall(dst)

    async def upload(
        self,
        state_uri: str,
        src: Path,
        params: dict[str, Any],
    ) -> str:
        prev_key = state_uri.removeprefix(f"s3://{self.bucket}/")
        prefix = prev_key.rsplit("/", 1)[0] + "/"
        new_key = f"{prefix}state-{_ts()}.tar.gz"
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tf:
            for path in sorted(src.rglob("*")):
                if path.is_file():
                    tf.add(path, arcname=path.relative_to(src).as_posix())
        buf.seek(0)
        await self.store.put_object(bucket=self.bucket, key=new_key, body=buf.getvalue())
        return self._uri_for(new_key)


def _ts() -> str:
    return datetime.now(UTC).strftime("%Y%m%dt%H%M%S%fz")


def _tar_bytes_for_empty() -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz"):
        pass
    return buf.getvalue()
