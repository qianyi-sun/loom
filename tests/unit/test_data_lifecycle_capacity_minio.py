from __future__ import annotations

from typing import Any

import pytest

from loom import data_lifecycle_capacity_minio as minio_capacity_module
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
        ),
        expected_drive_count=2,
    )
    assert len(drives) == 2
    # total_inodes = free + used = 100 on each.
    assert [d.disk_free_percent for d in drives] == [40, 80]
    assert [d.inode_free_percent for d in drives] == [90, 30]


def test_parse_admin_info_accepts_info_wrapped_payload() -> None:
    inner = _payload([_drive("0", total=1000, avail=250, free_i=50, used_i=50)])
    drives = parse_admin_info_drives({"info": inner}, expected_drive_count=1)
    assert [d.disk_free_percent for d in drives] == [25]


def test_parse_admin_info_rejects_partial_drive_inventory() -> None:
    with pytest.raises(RuntimeError, match="drive count"):
        parse_admin_info_drives(
            _payload([_drive("0", total=1000, avail=500, free_i=50, used_i=50)]),
            expected_drive_count=2,
        )


def test_parse_admin_info_rejects_unhealthy_drive_inventory() -> None:
    with pytest.raises(RuntimeError, match="drive count"):
        parse_admin_info_drives(
            _payload(
                [
                    _drive("0", total=1000, avail=500, free_i=50, used_i=50),
                    _drive(
                        "1",
                        total=1000,
                        avail=10,
                        free_i=1,
                        used_i=99,
                        state="offline",
                    ),
                ]
            ),
            expected_drive_count=2,
        )


def test_parse_admin_info_rejects_duplicate_drive_identity() -> None:
    with pytest.raises(RuntimeError, match="identity is duplicated"):
        parse_admin_info_drives(
            _payload(
                [
                    _drive("0", total=1000, avail=500, free_i=50, used_i=50),
                    _drive("0", total=1000, avail=999, free_i=99, used_i=1),
                ]
            ),
            expected_drive_count=2,
        )


def test_parse_admin_info_rejects_duplicate_drive_endpoint() -> None:
    first = _drive("0", total=1000, avail=500, free_i=50, used_i=50)
    second = _drive("1", total=1000, avail=999, free_i=99, used_i=1)
    second["endpoint"] = first["endpoint"]
    with pytest.raises(RuntimeError, match="endpoint is duplicated"):
        parse_admin_info_drives(
            _payload([first, second]),
            expected_drive_count=2,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("totalspace", True),
        ("availspace", "500"),
        ("free_inodes", None),
        ("used_inodes", 1.5),
    ],
)
def test_parse_admin_info_rejects_malformed_numeric_types(
    field: str,
    value: object,
) -> None:
    drive = _drive("0", total=1000, avail=500, free_i=50, used_i=50)
    drive[field] = value
    with pytest.raises(RuntimeError, match="telemetry is invalid"):
        parse_admin_info_drives(
            _payload([drive]),
            expected_drive_count=1,
        )


def test_parse_admin_info_rejects_malformed_server_or_drive_shapes() -> None:
    with pytest.raises(RuntimeError, match="servers are invalid"):
        parse_admin_info_drives({"servers": "not-a-list"}, expected_drive_count=1)
    with pytest.raises(RuntimeError, match="server is invalid"):
        parse_admin_info_drives({"servers": ["not-a-map"]}, expected_drive_count=1)
    with pytest.raises(RuntimeError, match="drives are invalid"):
        parse_admin_info_drives(
            {"servers": [{"drives": "not-a-list"}]},
            expected_drive_count=1,
        )
    with pytest.raises(RuntimeError, match="drive is invalid"):
        parse_admin_info_drives(
            {"servers": [{"drives": ["not-a-map"]}]},
            expected_drive_count=1,
        )


def test_parse_admin_info_rejects_empty_or_nonpositive_expected_count() -> None:
    with pytest.raises(ValueError, match="expected drive count"):
        parse_admin_info_drives(
            _payload([_drive("0", total=1000, avail=500, free_i=50, used_i=50)]),
            expected_drive_count=0,
        )


def test_parse_admin_info_fails_closed_without_healthy_drives() -> None:
    with pytest.raises(RuntimeError, match="drive count"):
        parse_admin_info_drives(
            _payload(
                [
                    _drive(
                        "0",
                        total=1,
                        avail=1,
                        free_i=1,
                        used_i=1,
                        state="offline",
                    )
                ]
            ),
            expected_drive_count=1,
        )
    with pytest.raises(RuntimeError, match="no servers"):
        parse_admin_info_drives({"servers": []}, expected_drive_count=1)


def test_parse_admin_info_rejects_missing_drive_identity() -> None:
    drive = _drive("0", total=1000, avail=500, free_i=50, used_i=50)
    drive["uuid"] = ""
    drive["endpoint"] = ""
    with pytest.raises(RuntimeError, match="identity is invalid"):
        parse_admin_info_drives(
            _payload([drive]),
            expected_drive_count=1,
        )


class _Raw:
    def __init__(self, content: bytes) -> None:
        self._content = content
        self.read_amounts: list[int] = []
        self.closed = False
        self.released = False

    def read(self, amount: int) -> bytes:
        self.read_amounts.append(amount)
        return self._content[:amount]

    def close(self) -> None:
        self.closed = True

    def release_conn(self) -> None:
        self.released = True


class _Resp:
    def __init__(self, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self.raw = _Raw(content)

    @property
    def content(self) -> bytes:
        raise AssertionError("streaming probe must not materialize response.content")


class _Session:
    def __init__(self, resp: _Resp) -> None:
        self._resp = resp
        self.sent: list[Any] = []
        self.closed = False

    def send(self, prepared: Any) -> _Resp:
        self.sent.append(prepared)
        return self._resp

    def close(self) -> None:
        self.closed = True


def test_probe_signs_admin_info_and_parses() -> None:
    import json

    body = json.dumps(
        _payload([_drive("0", total=1000, avail=600, free_i=60, used_i=40)])
    ).encode()
    session = _Session(_Resp(200, body))
    drives = probe_minio_admin_drives(
        endpoint_url="http://loom-minio:9000",
        access_key="ak",
        secret_key="sk",
        expected_drive_count=1,
        http_session=session,  # type: ignore[arg-type]
    )
    assert [d.disk_free_percent for d in drives] == [60]
    assert [d.inode_free_percent for d in drives] == [60]
    # The request was SigV4-signed (Authorization header present) and targeted
    # the admin-info path.
    prepared = session.sent[0]
    assert prepared.url.endswith("/minio/admin/v3/info")
    assert prepared.stream_output is True
    assert "Authorization" in prepared.headers
    assert session._resp.raw.read_amounts == [(1 << 20) + 1]
    assert session._resp.raw.closed and session._resp.raw.released
    assert not session.closed


def test_probe_closes_only_an_internally_owned_http_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json

    body = json.dumps(
        _payload([_drive("0", total=1000, avail=600, free_i=60, used_i=40)])
    ).encode()
    session = _Session(_Resp(200, body))
    monkeypatch.setattr(minio_capacity_module, "URLLib3Session", lambda: session)

    probe_minio_admin_drives(
        endpoint_url="http://loom-minio:9000",
        access_key="ak",
        secret_key="sk",
        expected_drive_count=1,
    )

    assert session.closed
    assert session._resp.raw.closed and session._resp.raw.released


def test_probe_rejects_unbounded_admin_info_response() -> None:
    session = _Session(_Resp(200, b" " * ((1 << 20) + 1)))
    with pytest.raises(RuntimeError, match="response is invalid"):
        probe_minio_admin_drives(
            endpoint_url="http://loom-minio:9000",
            access_key="ak",
            secret_key="sk",
            expected_drive_count=1,
            http_session=session,  # type: ignore[arg-type]
        )
    assert not session.closed
    assert session._resp.raw.read_amounts == [(1 << 20) + 1]
    assert session._resp.raw.closed and session._resp.raw.released


def test_probe_raises_on_non_200() -> None:
    session = _Session(_Resp(403, b"denied"))
    with pytest.raises(RuntimeError, match="HTTP 403"):
        probe_minio_admin_drives(
            endpoint_url="http://loom-minio:9000",
            access_key="ak",
            secret_key="sk",
            expected_drive_count=1,
            http_session=session,  # type: ignore[arg-type]
        )
    assert session._resp.raw.read_amounts == []
    assert session._resp.raw.closed and session._resp.raw.released
