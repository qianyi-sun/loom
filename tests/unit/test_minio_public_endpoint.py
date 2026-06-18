"""Unit tests for MinIO public-endpoint URL helpers.

Covers:
1. Default (no minio_public_endpoint) → URL unchanged.
2. Public endpoint set → host:port rewritten, scheme promoted, path + query
   string preserved.
3. Public endpoint includes port → host AND port both rewritten.

SigV4 presigned URLs returned to users are covered by integration tests that
assert routes use the public presign client instead of this legacy rewrite
helper.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom_service.storage import rewrite_to_public

# ─── helper: build a minimal LoomServiceSettings-like namespace ───────────────

def _settings(minio_public_endpoint: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        minio_public_endpoint=minio_public_endpoint,
        trajectories_bucket="trajectories",
        artifacts_bucket="artifacts",
        signed_url_expiry_sec=3600,
    )


# ─── 1. Default behaviour: no rewrite ─────────────────────────────────────────

def test_rewrite_noop_when_public_endpoint_unset() -> None:
    url = (
        "http://minio:9000/trajectories/team/trial/atif.json"
        "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=abc123"
    )
    result = rewrite_to_public(url, _settings(minio_public_endpoint=None))
    assert result == url


def test_rewrite_noop_when_public_endpoint_empty_string() -> None:
    """Empty string is falsy — treated as «not set»."""
    url = "http://minio:9000/trajectories/k"
    result = rewrite_to_public(url, _settings(minio_public_endpoint=""))
    assert result == url


# ─── 2. Rewrite: host + scheme promoted, path + query preserved ───────────────

def test_rewrite_replaces_host_and_promotes_scheme() -> None:
    qs = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=deadbeef"
    path = "/trajectories/team-id/trial-id/atif.json"
    internal = f"http://minio:9000{path}?{qs}"

    result = rewrite_to_public(
        internal,
        _settings(minio_public_endpoint="https://minio.example.com"),
    )

    assert result.startswith("https://minio.example.com")
    assert path in result
    assert qs in result
    # Internal hostname must NOT appear
    assert "minio:9000" not in result


def test_rewrite_preserves_query_string_verbatim() -> None:
    qs = (
        "X-Amz-Algorithm=AWS4-HMAC-SHA256"
        "&X-Amz-Credential=AKID%2F20240101%2Fus-east-1%2Fs3%2Faws4_request"
        "&X-Amz-Date=20240101T000000Z"
        "&X-Amz-Expires=3600"
        "&X-Amz-SignedHeaders=host"
        "&X-Amz-Signature=abc123def456"
    )
    path = "/artifacts/team/trial/step/out.json"
    internal = f"http://minio:9000{path}?{qs}"

    result = rewrite_to_public(
        internal,
        _settings(minio_public_endpoint="https://minio.example.com"),
    )

    # Every query parameter must survive the rewrite
    for param in qs.split("&"):
        assert param in result, f"Query param lost: {param}"


def test_rewrite_preserves_path() -> None:
    path = "/trajectories/aaaa/bbbb/events.jsonl"
    internal = f"http://minio:9000{path}?X-Amz-Signature=x"

    result = rewrite_to_public(
        internal,
        _settings(minio_public_endpoint="https://minio.example.com"),
    )

    assert path in result


# ─── 3. Public endpoint includes port ────────────────────────────────────────

def test_rewrite_with_explicit_port_in_public_endpoint() -> None:
    internal = (
        "http://minio:9000/traj/t/a/events.jsonl"
        "?X-Amz-Signature=sig"
    )

    result = rewrite_to_public(
        internal,
        _settings(minio_public_endpoint="https://minio.example.com:4430"),
    )

    assert "minio.example.com:4430" in result
    assert "minio:9000" not in result
    assert "X-Amz-Signature=sig" in result


def test_rewrite_http_to_http_preserves_scheme() -> None:
    """If the public endpoint is http (not https), scheme stays http."""
    internal = "http://minio:9000/traj/k?sig=x"
    result = rewrite_to_public(
        internal,
        _settings(minio_public_endpoint="http://public-minio.internal:9001"),
    )
    assert result.startswith("http://public-minio.internal:9001")


# ─── 4. Legacy route fixture retained for compatibility experiments ──────────

def _make_app(minio_public_endpoint: str | None = None) -> FastAPI:
    """Build the loom_service FastAPI app with mocked minio + settings."""
    from loom_service.routes.atif import router as atif_router
    from loom_service.routes.trajectory import router as traj_router
    from loom_service.routes.trials import router as trials_router

    app = FastAPI()
    app.include_router(trials_router, prefix="/api/v1")
    app.include_router(atif_router, prefix="/api/v1")
    app.include_router(traj_router, prefix="/api/v1")

    team_id = uuid4()
    trial_id = uuid4()

    # Fake presigned URL that boto3 would return (internal hostname)
    _fake_presigned = (
        f"http://minio:9000/trajectories/{team_id}/{trial_id}/atif.json"
        "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=fakesig"
    )
    _fake_presigned_traj = (
        f"http://minio:9000/trajectories/{team_id}/{trial_id}/events.jsonl"
        "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=fakesig"
    )

    mock_minio = MagicMock()
    mock_minio.generate_presigned_url.side_effect = lambda op, Params, ExpiresIn: (  # noqa: N803  # boto3 kwarg names
        _fake_presigned_traj
        if "events.jsonl" in Params.get("Key", "")
        else _fake_presigned
    )

    # Fake async session that returns our trial stub
    from loom.db.schema import Trial

    fake_trial = MagicMock(spec=Trial)
    fake_trial.id = trial_id
    fake_trial.team_id = team_id
    fake_trial.task_id = "t"
    fake_trial.state = "succeeded"
    fake_trial.failure_reason = None
    fake_trial.submitted_at = datetime.now(UTC)
    fake_trial.started_at = datetime.now(UTC)
    fake_trial.finished_at = datetime.now(UTC)
    fake_trial.attempt_count = 1
    fake_trial.result = None
    fake_trial.config = {}
    fake_trial.trajectory_index = {}

    # Build a mock async session / session_factory
    mock_scalar = MagicMock()
    mock_scalar.scalar_one_or_none.return_value = fake_trial
    mock_execute = MagicMock()
    mock_execute.scalars.return_value.all.return_value = [fake_trial]
    mock_execute.scalar_one_or_none.return_value = fake_trial

    mock_session = MagicMock()
    mock_session.__aenter__ = MagicMock(return_value=mock_session)
    mock_session.__aexit__ = MagicMock(return_value=False)
    mock_session.execute = MagicMock(return_value=mock_execute)
    # Make execute awaitable

    async def _async_execute(*_a: object, **_kw: object) -> MagicMock:
        return mock_execute

    mock_session.execute = _async_execute

    mock_factory = MagicMock()
    mock_factory.return_value.__aenter__ = MagicMock(return_value=mock_session)
    mock_factory.return_value.__aexit__ = MagicMock(return_value=False)

    # Fake settings
    class _FakeSettings:
        trajectories_bucket = "trajectories"
        artifacts_bucket = "artifacts"
        signed_url_expiry_sec = 3600
        minio_public_endpoint = minio_public_endpoint

    app.state.settings = _FakeSettings()
    app.state.minio_client = mock_minio
    app.state.session_factory = mock_factory

    return app, team_id, trial_id, _fake_presigned, _fake_presigned_traj


def _fake_token(team_id: object) -> str:
    """Produce a raw token that hashes to a DB entry — NOT used here; we
    bypass auth by overriding the dependency."""
    return f"loom_team_{uuid4().hex}"


def _client_no_auth(app: FastAPI) -> TestClient:
    """TestClient with auth dependency overridden to a no-op."""

    # We patch require_scope + require_team_or_admin to pass silently

    return TestClient(app, raise_server_exceptions=True)


# The route tests need DB session + auth wiring which is complex to fake in
# pure unit tests — the integration conftest.py covers that.  Here we test
# the storage helper directly for completeness and rely on integration tests
# for end-to-end route wiring.

def test_rewrite_to_public_type_stability() -> None:
    """rewrite_to_public always returns str."""
    url = "http://minio:9000/b/k?sig=x"
    assert isinstance(rewrite_to_public(url, _settings(None)), str)
    assert isinstance(
        rewrite_to_public(url, _settings("https://public.example.com")), str,
    )
