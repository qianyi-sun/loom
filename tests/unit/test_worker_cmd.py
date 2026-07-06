"""Unit tests for `loom worker` CLI (#317 Phase 3b)."""
from __future__ import annotations

import datetime as _dt
import json
from unittest.mock import MagicMock, patch

import pytest

from loom_cli import worker_cmd
from loom_worker.setup_admission import NodeHealthSnapshot

# ─── Format helpers ────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0, "0s"),
        (59, "59s"),
        (60, "1m"),
        (3599, "59m"),
        (3600, "1.0h"),
        (86_399, "24.0h"),
        (86_400, "1.0d"),
        (864_000, "10.0d"),
    ],
)
def test_human_age(seconds: int, expected: str) -> None:
    assert worker_cmd._human_age(seconds) == expected


@pytest.mark.parametrize(
    ("bytes_", "expected"),
    [
        (0, "0.0 B"),
        (1023, "1023.0 B"),
        (1024, "1.0 KiB"),
        (1024 * 1024, "1.0 MiB"),
        (1024 * 1024 * 1024, "1.0 GiB"),
    ],
)
def test_human_size(bytes_: int, expected: str) -> None:
    assert worker_cmd._human_size(bytes_) == expected


# ─── Docker timestamp parser ───────────────────────────────────────


def test_parse_docker_created_z_suffix() -> None:
    """Docker's standard `...Z` suffix."""
    dt = worker_cmd._parse_docker_created("2026-06-22T12:34:56.789Z")
    assert dt.year == 2026
    assert dt.tzinfo is not None


def test_parse_docker_created_nanosecond_precision() -> None:
    """Docker sometimes emits nanosecond-precision timestamps.
    Python's datetime can't parse beyond microsecond — we truncate."""
    dt = worker_cmd._parse_docker_created(
        "2026-06-22T12:34:56.123456789Z",
    )
    assert dt.year == 2026
    assert dt.microsecond == 123_456


def test_parse_docker_created_with_offset() -> None:
    """Explicit timezone offset preserved through truncation."""
    dt = worker_cmd._parse_docker_created("2026-06-22T12:34:56.123+02:00")
    assert dt.tzinfo is not None


# ─── _gather_cache_images (mocked Docker) ──────────────────────────


def _stub_image(*, created: str, size: int, cache_key: str,
                tag: str) -> MagicMock:
    img = MagicMock()
    img.attrs = {
        "Created": created,
        "Size": size,
        "Config": {"Labels": {"loom.cache-key": cache_key}},
    }
    img.tags = [tag] if tag else []
    return img


def test_gather_cache_images_sorts_newest_first() -> None:
    now = _dt.datetime.now(_dt.UTC)
    old = (now - _dt.timedelta(days=5)).isoformat()
    new = (now - _dt.timedelta(minutes=10)).isoformat()

    client = MagicMock()
    client.images.list.return_value = [
        _stub_image(created=old, size=2_000_000, cache_key="oldkey",
                    tag="loom-trial-cache:oldkey"),
        _stub_image(created=new, size=1_000_000, cache_key="newkey",
                    tag="loom-trial-cache:newkey"),
    ]
    with patch("docker.from_env", return_value=client):
        rows = worker_cmd._gather_cache_images()

    assert [r["cache_key"] for r in rows] == ["newkey", "oldkey"]
    assert rows[0]["size_bytes"] == 1_000_000


def test_gather_cache_images_filters_by_loom_label() -> None:
    client = MagicMock()
    client.images.list.return_value = []
    with patch("docker.from_env", return_value=client):
        worker_cmd._gather_cache_images()
    client.images.list.assert_called_once_with(
        filters={"label": "loom.trial-cache=true"},
    )


def test_gather_cache_images_raises_on_unreachable_daemon() -> None:
    from docker.errors import DockerException
    with patch("docker.from_env",
               side_effect=DockerException("no socket")):
        with pytest.raises(RuntimeError, match="cannot reach Docker"):
            worker_cmd._gather_cache_images()


# ─── setup status (#275) ───────────────────────────────────────────


def _stub_container(
    *,
    name: str,
    status: str,
    labels: dict[str, str],
) -> MagicMock:
    container = MagicMock()
    container.id = "1234567890abcdef"
    container.name = name
    container.status = status
    container.attrs = {"Config": {"Labels": labels}}
    return container


def test_gather_setup_status_reports_health_and_labeled_containers() -> None:
    client = MagicMock()
    client.containers.list.return_value = [
        _stub_container(
            name="loom-sidecar-trial-api",
            status="running",
            labels={
                "loom.setup-container": "true",
                "loom.task-sidecar": "true",
                "loom.trial_id": "trial-1",
                "loom.task_id": "task-1",
                "loom.task_sidecar": "api",
            },
        )
    ]

    with patch("docker.from_env", return_value=client):
        status = worker_cmd._gather_setup_status(
            read_snapshot=lambda: NodeHealthSnapshot(
                io_full_avg10=76.15,
                swap_total_mb=4096,
                swap_free_mb=2048,
                d_state_processes=1,
            ),
        )

    assert status["health"]["ok"] is False
    assert status["health"]["reason"] == "node_io_pressure"
    assert status["containers"] == [
        {
            "id": "1234567890ab",
            "name": "loom-sidecar-trial-api",
            "status": "running",
            "kind": "setup-sidecar",
            "trial_id": "trial-1",
            "task_id": "task-1",
            "detail": "api",
        }
    ]
    assert client.containers.list.call_args_list[0].kwargs == {
        "all": True,
        "filters": {"label": "loom.setup-container=true"},
    }
    assert client.containers.list.call_args_list[1].kwargs == {
        "all": True,
        "filters": {"label": "loom.trial-container=true"},
    }


# ─── dispatch (handler integration) ────────────────────────────────


def test_dispatch_cache_stats_json_empty(
    capsys: pytest.CaptureFixture[str],
) -> None:
    client = MagicMock()
    client.images.list.return_value = []
    with patch("docker.from_env", return_value=client):
        rc = worker_cmd.dispatch(["cache", "stats", "--json"])
    assert rc == 0
    out = capsys.readouterr().out.strip()
    assert json.loads(out) == []


def test_dispatch_cache_stats_table_empty(
    capsys: pytest.CaptureFixture[str],
) -> None:
    client = MagicMock()
    client.images.list.return_value = []
    with patch("docker.from_env", return_value=client):
        rc = worker_cmd.dispatch(["cache", "stats"])
    assert rc == 0
    assert "no trial-cache" in capsys.readouterr().out


def test_dispatch_cache_stats_table_with_rows(
    capsys: pytest.CaptureFixture[str],
) -> None:
    now = _dt.datetime.now(_dt.UTC)
    client = MagicMock()
    client.images.list.return_value = [
        _stub_image(
            created=(now - _dt.timedelta(hours=2)).isoformat(),
            size=5_000_000,
            cache_key="abc123",
            tag="loom-trial-cache:abc123",
        ),
    ]
    with patch("docker.from_env", return_value=client):
        rc = worker_cmd.dispatch(["cache", "stats"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "abc123" in out
    assert "1 cached image(s)" in out
    assert "CACHE_KEY" in out  # header


def test_dispatch_setup_status_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    client = MagicMock()
    client.containers.list.return_value = []
    with patch("docker.from_env", return_value=client):
        with patch(
            "loom_cli.worker_cmd.read_node_health_snapshot",
            return_value=NodeHealthSnapshot(
                io_full_avg10=0.0,
                swap_total_mb=0,
                swap_free_mb=0,
                d_state_processes=0,
            ),
        ):
            rc = worker_cmd.dispatch(["setup", "status", "--json"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["health"]["reason"] == "healthy"
    assert out["containers"] == []


def test_dispatch_unknown_subcommand_errors() -> None:
    """argparse should reject unknown subcommands with non-zero exit."""
    with pytest.raises(SystemExit) as exc:
        worker_cmd.dispatch(["nonexistent"])
    assert exc.value.code != 0
