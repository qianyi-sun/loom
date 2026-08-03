"""Distributed-MinIO drive headroom via the admin API.

In distributed / multi-node mode every MinIO replica owns a ReadWriteOnce
Longhorn PVC already attached to its running ``loom-minio-*`` pod, so the
lifecycle maintenance Job cannot co-mount the drives to ``statvfs`` them
(Multi-Attach, #1113).  Instead we ask MinIO itself over the network:
``GET /minio/admin/v3/info`` returns per-drive ``totalspace``/``availspace``
plus ``free_inodes``/``used_inodes`` (total inodes = free + used), which is
exactly the byte + inode headroom :class:`DriveHeadroom` needs.

The admin API is authenticated with the ordinary S3 SigV4 credentials
(``S3SigV4Auth``); no Prometheus JWT or drive mount is involved.
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlsplit

from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from botocore.httpsession import URLLib3Session

from loom.data_lifecycle_capacity import DriveHeadroom

_ADMIN_INFO_PATH = "/minio/admin/v3/info"
_DEFAULT_REGION = "us-east-1"


def parse_admin_info_drives(payload: dict[str, Any]) -> list[DriveHeadroom]:
    """Fold a ``/minio/admin/v3/info`` document into per-drive headroom.

    Only ``state == "ok"`` drives are counted, deduplicated by ``uuid`` so a
    drive reported by multiple peers is not double-counted.  Raises when no
    healthy drive is present so admission fails closed rather than on an empty
    ``min()``.
    """
    info = payload.get("info", payload)
    servers = info.get("servers")
    if not isinstance(servers, list) or not servers:
        raise RuntimeError("minio admin info returned no servers")
    seen: set[str] = set()
    drives: list[DriveHeadroom] = []
    for server in servers:
        for drive in server.get("drives") or []:
            if drive.get("state") != "ok":
                continue
            key = str(drive.get("uuid") or drive.get("endpoint") or "")
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            free_inodes = int(drive["free_inodes"])
            used_inodes = int(drive["used_inodes"])
            drives.append(
                DriveHeadroom(
                    total_bytes=int(drive["totalspace"]),
                    free_bytes=int(drive["availspace"]),
                    total_inodes=free_inodes + used_inodes,
                    free_inodes=free_inodes,
                )
            )
    if not drives:
        raise RuntimeError("minio admin info reported no healthy drives")
    return drives


def probe_minio_admin_drives(
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    region: str = _DEFAULT_REGION,
    http_session: URLLib3Session | None = None,
) -> list[DriveHeadroom]:
    """Query the live MinIO admin API for per-drive byte + inode headroom."""
    parsed = urlsplit(endpoint_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise RuntimeError("minio admin endpoint is invalid")
    request = AWSRequest(
        method="GET", url=f"{parsed.scheme}://{parsed.netloc}{_ADMIN_INFO_PATH}"
    )
    S3SigV4Auth(Credentials(access_key, secret_key), "s3", region).add_auth(request)
    session = http_session or URLLib3Session()
    response = session.send(request.prepare())
    if response.status_code != 200:
        raise RuntimeError(
            f"minio admin info failed: HTTP {response.status_code}"
        )
    return parse_admin_info_drives(json.loads(response.content))


__all__ = ["parse_admin_info_drives", "probe_minio_admin_drives"]
