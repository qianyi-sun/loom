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
from collections.abc import Mapping
from urllib.parse import urlsplit

from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from botocore.httpsession import URLLib3Session

from loom.data_lifecycle_capacity import DriveHeadroom

_ADMIN_INFO_PATH = "/minio/admin/v3/info"
_DEFAULT_REGION = "us-east-1"
_MAX_ADMIN_INFO_BYTES = 1 << 20


def _strict_drive_integer(drive: Mapping[str, object], field: str) -> int:
    value = drive.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError("minio admin drive telemetry is invalid")
    return value


def parse_admin_info_drives(
    payload: object,
    *,
    expected_drive_count: int,
) -> list[DriveHeadroom]:
    """Fold a ``/minio/admin/v3/info`` document into per-drive headroom.

    Only ``state == "ok"`` drives are counted. The exact number of unique
    healthy drives must match the configured topology, so incomplete,
    duplicated, unhealthy, or malformed telemetry fails closed.
    """
    if (
        isinstance(expected_drive_count, bool)
        or not isinstance(expected_drive_count, int)
        or expected_drive_count < 1
    ):
        raise ValueError("minio admin expected drive count is invalid")
    if not isinstance(payload, Mapping):
        raise RuntimeError("minio admin info payload is invalid")
    info = payload.get("info", payload)
    if not isinstance(info, Mapping):
        raise RuntimeError("minio admin info payload is invalid")
    servers = info.get("servers")
    if not isinstance(servers, list):
        raise RuntimeError("minio admin info servers are invalid")
    if not servers:
        raise RuntimeError("minio admin info returned no servers")
    seen_uuids: set[str] = set()
    seen_endpoints: set[str] = set()
    drives: list[DriveHeadroom] = []
    for server in servers:
        if not isinstance(server, Mapping):
            raise RuntimeError("minio admin info server is invalid")
        server_drives = server.get("drives")
        if not isinstance(server_drives, list):
            raise RuntimeError("minio admin info drives are invalid")
        for drive in server_drives:
            if not isinstance(drive, Mapping):
                raise RuntimeError("minio admin info drive is invalid")
            state = drive.get("state")
            if not isinstance(state, str):
                raise RuntimeError("minio admin drive telemetry is invalid")
            if state != "ok":
                continue
            uuid = drive.get("uuid")
            endpoint = drive.get("endpoint")
            if not isinstance(uuid, str) or not uuid:
                raise RuntimeError("minio admin drive identity is invalid")
            if not isinstance(endpoint, str) or not endpoint:
                raise RuntimeError("minio admin drive identity is invalid")
            if uuid in seen_uuids:
                raise RuntimeError("minio admin drive UUID identity is duplicated")
            if endpoint in seen_endpoints:
                raise RuntimeError("minio admin drive endpoint is duplicated")
            seen_uuids.add(uuid)
            seen_endpoints.add(endpoint)
            total_bytes = _strict_drive_integer(drive, "totalspace")
            free_bytes = _strict_drive_integer(drive, "availspace")
            free_inodes = _strict_drive_integer(drive, "free_inodes")
            used_inodes = _strict_drive_integer(drive, "used_inodes")
            drives.append(
                DriveHeadroom(
                    total_bytes=total_bytes,
                    free_bytes=free_bytes,
                    total_inodes=free_inodes + used_inodes,
                    free_inodes=free_inodes,
                )
            )
    if len(drives) != expected_drive_count:
        raise RuntimeError("minio admin healthy drive count drifted")
    return drives


def probe_minio_admin_drives(
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    expected_drive_count: int,
    region: str = _DEFAULT_REGION,
    http_session: URLLib3Session | None = None,
) -> list[DriveHeadroom]:
    """Query the live MinIO admin API for per-drive byte + inode headroom."""
    parsed = urlsplit(endpoint_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise RuntimeError("minio admin endpoint is invalid")
    request = AWSRequest(
        method="GET",
        url=f"{parsed.scheme}://{parsed.netloc}{_ADMIN_INFO_PATH}",
        stream_output=True,
    )
    S3SigV4Auth(Credentials(access_key, secret_key), "s3", region).add_auth(request)
    owned_session = http_session is None
    session = http_session or URLLib3Session()
    try:
        response = session.send(request.prepare())
        raw = response.raw
        try:
            if response.status_code != 200:
                raise RuntimeError(
                    f"minio admin info failed: HTTP {response.status_code}"
                )
            content = raw.read(_MAX_ADMIN_INFO_BYTES + 1)
            if not isinstance(content, bytes) or len(content) > _MAX_ADMIN_INFO_BYTES:
                raise RuntimeError("minio admin info response is invalid")
            return parse_admin_info_drives(
                json.loads(content),
                expected_drive_count=expected_drive_count,
            )
        finally:
            try:
                raw.close()
            finally:
                raw.release_conn()
    finally:
        if owned_session:
            session.close()


__all__ = ["parse_admin_info_drives", "probe_minio_admin_drives"]
