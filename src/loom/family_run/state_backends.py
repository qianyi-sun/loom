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
    # Default None so the entry-point registry can instantiate with
    # zero args (see ``loom.family_run.registry.resolve_plugin``);
    # production callers poke ``store`` + ``bucket`` in post-hoc from
    # the orchestrator's ``OrchestratorContext``. Any call to
    # ``initialize/download/upload`` before they are set raises
    # ``RuntimeError`` — safer than silently defaulting to a wrong
    # bucket.
    store: ObjectStore | None = None
    bucket: str | None = None
    default_params: dict[str, Any] = field(default_factory=dict)

    def _require_store(self) -> ObjectStore:
        if self.store is None:
            raise RuntimeError(
                "S3ArtifactsStateBackend.store not configured; "
                "orchestrator must set it before calling backend methods",
            )
        return self.store

    def _require_bucket(self) -> str:
        if self.bucket is None:
            raise RuntimeError(
                "S3ArtifactsStateBackend.bucket not configured; "
                "orchestrator must set it before calling initialize()",
            )
        return self.bucket

    def _prefix(self, batch_id: UUID, family_key: str) -> str:
        return f"family-state/{batch_id}/{family_key}/"

    def _uri_for(self, key: str) -> str:
        return f"s3://{self._require_bucket()}/{key}"

    @staticmethod
    def _parse_uri(state_uri: str) -> tuple[str, str]:
        """Return ``(bucket, key)`` from an ``s3://`` URI.

        Downloads/uploads MUST respect whichever bucket the incoming
        ``state_uri`` names — the service seeds URIs against the
        environment's artifacts bucket (e.g. ``loom-staging-artifacts``),
        which is not necessarily the constructor default (#727).
        """
        if not state_uri.startswith("s3://"):
            raise ValueError(f"expected s3:// URI, got {state_uri!r}")
        rest = state_uri.removeprefix("s3://")
        bucket, _, key = rest.partition("/")
        if not bucket or not key:
            raise ValueError(f"malformed s3:// URI: {state_uri!r}")
        return bucket, key

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
        bucket = self._require_bucket()
        store = self._require_store()
        await store.put_object(bucket=bucket, key=empty_key, body=empty_tar)
        return self._uri_for(empty_key)

    async def download(
        self,
        state_uri: str,
        dst: Path,
        params: dict[str, Any],
    ) -> None:
        bucket, key = self._parse_uri(state_uri)
        store = self._require_store()
        data = await store.get_object(bucket=bucket, key=key)
        dst.mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
            tf.extractall(dst)

    async def upload(
        self,
        state_uri: str,
        src: Path,
        params: dict[str, Any],
    ) -> str:
        bucket, prev_key = self._parse_uri(state_uri)
        prefix = prev_key.rsplit("/", 1)[0] + "/"
        new_key = f"{prefix}state-{_ts()}.tar.gz"
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tf:
            for path in sorted(src.rglob("*")):
                if path.is_file():
                    tf.add(path, arcname=path.relative_to(src).as_posix())
        buf.seek(0)
        store = self._require_store()
        await store.put_object(bucket=bucket, key=new_key, body=buf.getvalue())
        return f"s3://{bucket}/{new_key}"


def _ts() -> str:
    return datetime.now(UTC).strftime("%Y%m%dt%H%M%S%fz")


def _tar_bytes_for_empty() -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz"):
        pass
    return buf.getvalue()
