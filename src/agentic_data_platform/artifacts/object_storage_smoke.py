from __future__ import annotations

import json
import time
from typing import Any, Protocol
from uuid import uuid4

from agentic_data_platform.artifacts.store import Artifacpilot groupjectStore, build_s3_artifact_store


class ObjectStorageSettings(Protocol):
    object_storage_endpoint: str
    object_storage_bucket: str
    object_storage_access_key: str
    object_storage_secret_key: str
    object_storage_region: str


def run_object_storage_smoke(
    store: Artifacpilot groupjectStore,
    *,
    key: str | None = None,
    payload: bytes = b"agentic data platform object storage smoke\n",
) -> dict[str, Any]:
    smoke_key = key or f"smoke/{uuid4().hex}.txt"
    store.ensure_bucket()
    stored = store.put_bytes(
        smoke_key,
        payload,
        media_type="text/plain",
        metadata={"content_type": "object_storage_smoke"},
    )
    downloaded = store.get_bytes(smoke_key)
    if downloaded != payload:
        raise RuntimeError("object storage smoke download did not match uploaded payload")
    presigned_url = store.presigned_get_url(smoke_key, expires_in_seconds=300)
    return {
        "bucket": stored.metadata.get("storage_bucket"),
        "key": stored.key,
        "uri": stored.uri,
        "size_bytes": stored.size_bytes,
        "sha256": stored.sha256,
        "download_verified": True,
        "presigned_url_available": bool(presigned_url),
    }


def run_configured_object_storage_smoke(
    settings: ObjectStorageSettings | None = None,
    *,
    attempts: int = 30,
    delay_seconds: float = 2.0,
) -> dict[str, Any]:
    if settings is None:
        from agentic_data_platform.service.config import load_service_settings

        service_settings = load_service_settings()
    else:
        service_settings = settings
    store = build_s3_artifact_store(
        endpoint_url=service_settings.object_storage_endpoint,
        bucket=service_settings.object_storage_bucket,
        access_key=service_settings.object_storage_access_key,
        secret_key=service_settings.object_storage_secret_key,
        region=service_settings.object_storage_region,
    )

    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            return run_object_storage_smoke(store, key="smoke/object-storage-smoke.txt")
        except Exception as exc:
            last_error = exc
            time.sleep(delay_seconds)

    raise RuntimeError("object storage smoke check failed") from last_error


def main() -> None:
    print(json.dumps(run_configured_object_storage_smoke(), sort_keys=True))


if __name__ == "__main__":
    main()
