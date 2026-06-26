"""Unit tests for `parse_backends_from_capabilities` — the pure logic
piece shared by GET /api/v1/backends + POST /api/v1/batches' worker
availability check.

DB-backed `get_active_backends` is exercised by the route integration
tests in tests/integration/test_service_batches_crud.py."""

from __future__ import annotations

import pytest

from loom_service.worker_backends import parse_backends_from_capabilities


def test_empty_input_returns_empty_set() -> None:
    assert parse_backends_from_capabilities([]) == set()


def test_single_worker_single_backend() -> None:
    assert parse_backends_from_capabilities(
        [[{"backend": "docker"}]],
    ) == {"docker"}


def test_single_worker_multiple_backends_unioned() -> None:
    assert parse_backends_from_capabilities(
        [[{"backend": "docker"}, {"backend": "modal"}]],
    ) == {"docker", "modal"}


def test_multiple_workers_union() -> None:
    assert parse_backends_from_capabilities([
        [{"backend": "docker"}],
        [{"backend": "modal"}],
        [{"backend": "docker"}],  # duplicate — set dedups
    ]) == {"docker", "modal"}


def test_missing_backend_key_defaults_to_docker() -> None:
    """Workers registered before Plan 28 PR-3 omitted the backend key
    in their capability dict. They only served docker, so we default
    there — a regression-guard for that backward-compat path."""
    assert parse_backends_from_capabilities([
        [{"agent_runtimes": ["oracle"]}],  # no `backend` key
    ]) == {"docker"}


def test_non_list_row_skipped() -> None:
    """A corrupt JSONB row (e.g. dict instead of list) shouldn't crash
    the route — silently skipped, the other live rows still count."""
    assert parse_backends_from_capabilities([
        {"backend": "wrong-shape"},  # row should be a list
        [{"backend": "docker"}],
    ]) == {"docker"}


def test_non_dict_cap_entry_skipped() -> None:
    assert parse_backends_from_capabilities([
        [{"backend": "docker"}, "bogus-string-entry", 42],
    ]) == {"docker"}


def test_non_string_backend_value_skipped() -> None:
    assert parse_backends_from_capabilities([
        [{"backend": 123}, {"backend": "docker"}],
    ]) == {"docker"}


def test_heartbeat_freshness_window_documented_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression guard for issue #68: the freshness window must be
    a finite small value. It should be stricter than the CP
    crash-detector expiry, so workers leave the catalog before their
    in-flight trials are reclaimed.

    The actual freshness predicate lives in `get_active_backends`
    (DB-touching, covered by integration tests). This unit test pins
    the constant so a future tuning change is intentional, not silent.
    """
    monkeypatch.setenv("LOOM_CP_DB_URL", "postgresql+psycopg://u:p@h/db")
    monkeypatch.setenv("LOOM_CP_MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("LOOM_CP_MINIO_ACCESS_KEY", "ak")
    monkeypatch.setenv("LOOM_CP_MINIO_SECRET_KEY", "sk")
    monkeypatch.setenv("LOOM_CP_LLM_GATEWAY_URL", "http://gateway:9100")

    from loom_control_plane.config import ControlPlaneSettings
    from loom_service.worker_backends import _HEARTBEAT_FRESHNESS_SEC

    cp_settings = ControlPlaneSettings(_env_file=None)
    assert 10 <= _HEARTBEAT_FRESHNESS_SEC <= 60, (
        f"freshness window {_HEARTBEAT_FRESHNESS_SEC}s is outside the "
        "expected 10-60s range"
    )
    assert _HEARTBEAT_FRESHNESS_SEC < cp_settings.worker_heartbeat_expiry_sec, (
        f"freshness window {_HEARTBEAT_FRESHNESS_SEC}s must stay below "
        f"CP reclaim expiry {cp_settings.worker_heartbeat_expiry_sec}s"
    )
