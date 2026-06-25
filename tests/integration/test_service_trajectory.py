"""Trajectory paginated read + download route (Plan 18 Task 4).

`traj_setup` lives in `tests/integration/conftest.py` so both
trajectory + ATIF tests share it (and the underlying MinIO container).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
from fastapi import FastAPI
from sqlalchemy import create_engine, insert, select, update
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Task, Trial


async def test_trajectory_paginates(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r1 = await ac.get(
            f"/api/v1/trials/{trial_id}/trajectory?limit=2",
            headers={"Authorization": f"Bearer {raw}"},
        )
        j1 = r1.json()
        assert len(j1["events"]) == 2
        assert j1["events"][0]["kind"] == "trial_start"
        assert j1["next_cursor"] == 2

        r2 = await ac.get(
            f"/api/v1/trials/{trial_id}/trajectory?limit=10&"
            f"cursor={j1['next_cursor']}",
            headers={"Authorization": f"Bearer {raw}"},
        )
        j2 = r2.json()
    assert len(j2["events"]) == 3
    assert j2["next_cursor"] is None


async def test_trajectory_unknown_trial_404(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    app, raw, _team_id, _trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{uuid4()}/trajectory",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


async def test_trajectory_object_missing_returns_empty_page(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
) -> None:
    """A trial row exists but the trajectory object was never written
    (queued/just-claimed, or crashed pre-first-event); we return an
    empty page so the SPA shows "no events yet" rather than a 404."""
    app, raw, team_id, _trial_id = traj_setup
    bare_trial = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        task_row_id = s.execute(
            select(Task.id).limit(1),
        ).scalar_one()
        s.execute(insert(Trial).values(
            id=bare_trial, task_id=task_row_id, team_id=team_id,
            state="queued", config={}, requires_caps={},
            submitted_at=datetime.now(UTC),
        ))
        s.commit()
    sync_engine.dispose()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{bare_trial}/trajectory",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    assert r.json() == {"events": [], "next_cursor": None}


async def test_trajectory_download_proxies_object_through_service(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/trajectory/download",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200
    assert "location" not in r.headers
    assert b'"kind": "trial_start"' in r.content


async def test_artifact_download_proxies_object_through_service(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id, trial_id = traj_setup
    settings = app.state.settings
    artifact_key = f"{team_id}/{trial_id}/main/result.txt"
    existing = {
        bucket["Name"]
        for bucket in app.state.minio_client.list_buckets()["Buckets"]
    }
    if settings.artifacts_bucket not in existing:
        app.state.minio_client.create_bucket(Bucket=settings.artifacts_bucket)
    app.state.minio_client.put_object(
        Bucket=settings.artifacts_bucket,
        Key=artifact_key,
        Body=b"hello artifact",
    )

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            update(Trial)
            .where(Trial.id == trial_id)
            .values(
                trajectory_index={
                    "artifacts": [
                        {
                            "step_name": "main",
                            "bucket": settings.artifacts_bucket,
                            "key": artifact_key,
                            "size": 14,
                        }
                    ],
                },
            ),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/artifacts/download",
            params={"key": artifact_key},
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200
    assert "location" not in r.headers
    assert r.content == b"hello artifact"


# ──────────────────────────────────────────────────────────────────────
# #5 Slice 1: seq-cursor events endpoint
# ──────────────────────────────────────────────────────────────────────


async def test_events_default_returns_all(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    """`after_seq=-1` (the default) returns every event in seq order.
    Mirrors the legacy /trajectory list but keys on the event payload's
    `seq` field instead of a line cursor — forward-compatible naming
    for the upcoming Postgres event table (#5 Phase 2)."""
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/events",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    seqs = [e["seq"] for e in body["events"]]
    assert seqs == [0, 1, 2, 3, 4]
    assert body["next_after_seq"] == 4


async def test_events_after_seq_skips_already_seen(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    """`after_seq=N` returns events with seq strictly greater than N,
    so the client can resume a stream without re-emitting events it
    already processed."""
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/events?after_seq=2",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    body = r.json()
    assert [e["seq"] for e in body["events"]] == [3, 4]
    assert body["next_after_seq"] == 4


async def test_events_after_terminal_seq_returns_empty(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    """`after_seq` larger than any event's seq returns an empty list
    and `next_after_seq=null` — the standard "no new events, poll
    again" response shape."""
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/events?after_seq=999",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    body = r.json()
    assert body == {"events": [], "next_after_seq": None}


async def test_events_limit_respected(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    """`limit=N` returns at most N events; client resumes via the
    returned `next_after_seq`."""
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/events?limit=2",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    body = r.json()
    assert [e["seq"] for e in body["events"]] == [0, 1]
    assert body["next_after_seq"] == 1


async def test_events_unknown_trial_returns_404(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    app, raw, _team_id, _trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{uuid4()}/events",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


# ──────────────────────────────────────────────────────────────────────
# #5 Slice 1: SSE stream
# ──────────────────────────────────────────────────────────────────────


def _parse_sse(body: str) -> list[dict[str, str]]:
    """Tiny SSE parser — splits on blank lines, returns each message
    as a dict of header → value (data, event, id, comment)."""
    messages: list[dict[str, str]] = []
    for block in body.split("\n\n"):
        block = block.strip("\n")
        if not block:
            continue
        msg: dict[str, str] = {}
        for line in block.split("\n"):
            if line.startswith(":"):
                msg.setdefault("comment", line[1:].strip())
                continue
            if ":" not in line:
                continue
            field, _, value = line.partition(":")
            value = value.lstrip(" ")
            if field in msg:
                msg[field] += "\n" + value
            else:
                msg[field] = value
        messages.append(msg)
    return messages


async def test_stream_emits_replay_then_completes_on_terminal(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    """Trial is already in a terminal state (succeeded — seeded by the
    fixture), so the SSE stream emits every event from MinIO and then
    a `complete` event, closing cleanly."""
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        timeout=10.0,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/stream",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200, r.text
    assert r.headers["content-type"].startswith("text/event-stream")
    assert r.headers["cache-control"] == "no-cache"
    assert r.headers["x-accel-buffering"] == "no"

    messages = _parse_sse(r.text)
    # First message is the "stream open" comment.
    assert messages[0].get("comment") == "stream open"
    # Then 5 data messages (one per seeded event), then complete.
    data_messages = [m for m in messages if "data" in m and m.get("event") != "complete"]
    assert len(data_messages) == 5
    parsed = [json.loads(m["data"]) for m in data_messages]
    assert [e["seq"] for e in parsed] == [0, 1, 2, 3, 4]
    # Each data message carries the SSE `id` field for client reconnect.
    assert [m["id"] for m in data_messages] == ["0", "1", "2", "3", "4"]
    # Final message is the `complete` event with last_seq.
    complete = next(m for m in messages if m.get("event") == "complete")
    payload = json.loads(complete["data"])
    assert payload["final_state"] == "succeeded"
    assert payload["last_seq"] == 4


async def test_stream_resumes_from_after_seq(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    """Reconnecting clients pass `after_seq=N`; stream emits only
    events with seq > N (standard SSE Last-Event-ID semantics)."""
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        timeout=10.0,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/stream?after_seq=2",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    messages = _parse_sse(r.text)
    data_messages = [
        m for m in messages
        if "data" in m and m.get("event") != "complete"
    ]
    parsed = [json.loads(m["data"]) for m in data_messages]
    assert [e["seq"] for e in parsed] == [3, 4]


async def test_stream_unknown_trial_returns_404(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    app, raw, _team_id, _trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{uuid4()}/stream",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404
