from __future__ import annotations

from typing import Any

import pytest

from loom.data_lifecycle_capacity_minio import (
    parse_admin_info_drives,
    probe_minio_admin_drives,
)


def _drive(uuid: str, *, total: int, avail: int, free_i: int, used_i: int, state: str = "ok"):
    return {
        "uuid": uuid,
        "endpoint": f"http://loom-minio-{uuid}.svc:9000/data",
        "state": state,
        "totalspace": total,
        "usedspace": total - avail,
        "availspace": avail,
        "free_inodes": free_i,
        "used_inodes": used_i,
    }


def _payload(drives: list[dict[str, Any]]) -> dict[str, Any]:
    # Real /minio/admin/v3/info shape: one drive per server, `servers` at top.
    return {"servers": [{"state": "ok", "drives": [d]} for d in drives]}


def test_parse_admin_info_folds_bytes_and_inode_headroom() -> None:
    drives = parse_admin_info_drives(
        _payload(
            [
                _drive("0", total=1000, avail=400, free_i=90, used_i=10),
                _drive("1", total=1000, avail=800, free_i=30, used_i=70),
            ]
        )
    )
    assert len(drives) == 2
    # total_inodes = free + used = 100 on each.
    assert [d.disk_free_percent for d in drives] == [40, 80]
    assert [d.inode_free_percent for d in drives] == [90, 30]


def test_parse_admin_info_accepts_info_wrapped_payload() -> None:
    inner = _payload([_drive("0", total=1000, avail=250, free_i=50, used_i=50)])
    drives = parse_admin_info_drives({"info": inner})
    assert [d.disk_free_percent for d in drives] == [25]


def test_parse_admin_info_skips_unhealthy_and_dedups_uuid() -> None:
    drives = parse_admin_info_drives(
        _payload(
            [
                _drive("0", total=1000, avail=500, free_i=50, used_i=50),
                _drive("0", total=1000, avail=999, free_i=99, used_i=1),  # dup uuid
                _drive("2", total=1000, avail=10, free_i=1, used_i=99, state="offline"),
            ]
        )
    )
    assert len(drives) == 1
    assert drives[0].disk_free_percent == 50


def test_parse_admin_info_fails_closed_without_healthy_drives() -> None:
    with pytest.raises(RuntimeError, match="no healthy drives"):
        parse_admin_info_drives(
            _payload([_drive("0", total=1, avail=1, free_i=1, used_i=1, state="offline")])
        )
    with pytest.raises(RuntimeError, match="no servers"):
        parse_admin_info_drives({"servers": []})


class _Resp:
    def __init__(self, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self.content = content


class _Session:
    def __init__(self, resp: _Resp) -> None:
        self._resp = resp
        self.sent: list[Any] = []

    def send(self, prepared: Any) -> _Resp:
        self.sent.append(prepared)
        return self._resp


def test_probe_signs_admin_info_and_parses(monkeypatch) -> None:
    import json

    body = json.dumps(
        _payload([_drive("0", total=1000, avail=600, free_i=60, used_i=40)])
    ).encode()
    session = _Session(_Resp(200, body))
    drives = probe_minio_admin_drives(
        endpoint_url="http://loom-minio:9000",
        access_key="ak",
        secret_key="sk",
        http_session=session,  # type: ignore[arg-type]
    )
    assert [d.disk_free_percent for d in drives] == [60]
    assert [d.inode_free_percent for d in drives] == [60]
    # The request was SigV4-signed (Authorization header present) and targeted
    # the admin-info path.
    prepared = session.sent[0]
    assert prepared.url.endswith("/minio/admin/v3/info")
    assert "Authorization" in prepared.headers


def test_probe_raises_on_non_200() -> None:
    session = _Session(_Resp(403, b"denied"))
    with pytest.raises(RuntimeError, match="HTTP 403"):
        probe_minio_admin_drives(
            endpoint_url="http://loom-minio:9000",
            access_key="ak",
            secret_key="sk",
            http_session=session,  # type: ignore[arg-type]
        )
