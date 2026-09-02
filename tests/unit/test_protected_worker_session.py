"""Secure runtime-role configuration for protected worker sessions."""

from __future__ import annotations

import importlib
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from starlette.requests import Request


@pytest.mark.parametrize(
    ("path", "payload"),
    (
        (
            "/trials/claim",
            {
                "worker_id": "00000000-0000-4000-8000-000000000401",
                "caps": [],
            },
        ),
        (
            "/work/claim",
            {
                "schema_version": "loom.work-claim-request.v1",
                "worker_id": "00000000-0000-4000-8000-000000000401",
                "capability_snapshot_digest": "sha256:" + "a" * 64,
                "supported_work_kinds": ["trial", "execution_attempt"],
                "free_slots": 1,
            },
        ),
    ),
)
def test_protected_claim_uses_only_atomic_runtime_transaction(
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    payload: dict[str, object],
) -> None:
    from loom.auth import AuthContext
    from loom_control_plane.routes import workers

    class ClaimStore:
        def __init__(self) -> None:
            self.claim_calls = 0

        @asynccontextmanager
        async def assert_session(self, **_kwargs: object) -> AsyncIterator[object]:
            raise AssertionError("claim route opened a separate session transaction")
            yield object()

        async def claim_assigned_trial(self, **_kwargs: object) -> None:
            self.claim_calls += 1

    @asynccontextmanager
    async def session_factory() -> AsyncIterator[object]:
        yield object()

    async def verify_bearer_token(*_args: object, **_kwargs: object) -> AuthContext:
        return AuthContext(
            token_hash=b"ordinary-bearer-digest",
            type="worker",
            scopes=["worker:claim"],
            team_id=None,
            expires_at=None,
        )

    store = ClaimStore()
    monkeypatch.setattr(workers, "verify_bearer_token", verify_bearer_token)
    app = FastAPI()
    app.state.session_factory = session_factory
    app.state.protected_worker_session_store = store
    app.include_router(workers.router)

    with TestClient(app) as client:
        response = client.post(
            path,
            headers={
                "Authorization": "Bearer ordinary",
                "X-Loom-Executor-Worker-Credential": "protected-credential",
            },
            json=payload,
        )

    assert response.status_code == 204
    assert store.claim_calls == 1


def _module():  # type: ignore[no-untyped-def]
    return importlib.import_module("loom_control_plane.protected_worker_session")


def _write_runtime_url(path: Path) -> Path:
    path.write_text(
        "postgresql+psycopg://guard_runtime:secret@postgres/loom\n",
        encoding="ascii",
    )
    path.chmod(0o600)
    return path


@pytest.mark.parametrize(
    "route_name",
    (
        "publish_live_preview_frame",
        "heartbeat_attempt",
        "get_attempt_control",
        "append_attempt_events",
        "report_attempt_started",
        "report_attempt_failed",
        "report_attempt_cancelled",
        "report_attempt_complete",
        "report_input_materialization_evidence",
        "report_worker_lost_cleanup",
        "read_input_manifest",
        "read_input_file",
        "prepare_checkpoint",
        "commit_checkpoint_session",
        "renew_checkpoint_token",
        "put_checkpoint_part",
        "complete_checkpoint_file",
        "abort_checkpoint_session",
        "prepare_final_output",
        "renew_final_output",
        "put_final_output_part",
        "complete_final_output_file",
        "commit_final_output_session",
        "abort_final_output_session",
    ),
)
def test_configured_runtime_rejects_uncredentialed_execution_attempt_route(
    route_name: str,
) -> None:
    from loom_control_plane.routes.execution_attempts import router

    app = FastAPI()
    app.state.protected_worker_session_store = object()
    app.include_router(router)
    route = next(
        candidate
        for candidate in router.routes
        if isinstance(candidate, APIRoute) and candidate.name == route_name
    )
    path = route.path_format.format(
        attempt_id="00000000-0000-4000-8000-000000000401",
        binding_name="input",
        file_index=0,
        item_key="item",
        part_number=1,
        sequence=0,
        session_id="00000000-0000-4000-8000-000000000402",
    )
    method = next(iter(route.methods - {"HEAD", "OPTIONS"}))

    with TestClient(app) as client:
        response = client.request(method, path)

    assert response.status_code == 401
    assert response.json() == {"detail": "protected worker session rejected"}


@pytest.mark.parametrize(
    ("router_module", "route_name", "method", "path", "body"),
    (
        (
            "loom_control_plane.routes.trials",
            "get_trial",
            "GET",
            "/trials/00000000-0000-4000-8000-000000000401",
            None,
        ),
        (
            "loom_control_plane.routes.trials",
            "get_trial_llm_calls",
            "GET",
            "/trials/00000000-0000-4000-8000-000000000401/llm-calls",
            None,
        ),
        (
            "loom_control_plane.routes.trajectory",
            "get_trajectory_url",
            "GET",
            "/trials/00000000-0000-4000-8000-000000000401/trajectory",
            None,
        ),
        (
            "loom_control_plane.routes.artifacts",
            "mint_artifact_upload_url",
            "POST",
            "/artifacts/upload-url",
            {
                "trial_id": "00000000-0000-4000-8000-000000000401",
                "key": "result.json",
            },
        ),
    ),
)
def test_configured_runtime_rejects_uncredentialed_worker_sensitive_trial_route(
    router_module: str,
    route_name: str,
    method: str,
    path: str,
    body: dict[str, str] | None,
) -> None:
    from loom.auth import AuthContext
    from loom_control_plane.protected_worker_session import protected_worker_principal

    module = importlib.import_module(router_module)
    route = next(
        candidate
        for candidate in module.router.routes
        if isinstance(candidate, APIRoute) and candidate.name == route_name
    )
    assert route.path_format
    app = FastAPI()
    app.state.protected_worker_session_store = object()
    app.dependency_overrides[protected_worker_principal] = lambda: AuthContext(
        token_hash=b"ordinary-bearer-digest",
        type="worker",
        scopes=["worker:index", "worker:report"],
        team_id=None,
        expires_at=None,
    )
    app.include_router(module.router)

    with TestClient(app) as client:
        response = client.request(method, path, json=body)

    assert response.status_code == 401
    assert response.json() == {"detail": "protected worker session rejected"}


@pytest.mark.asyncio
async def test_execution_attempt_worker_auth_uses_protected_credential_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom.auth import AuthContext
    from loom_control_plane.protected_worker_session import ProtectedWorkerSession
    from loom_control_plane.routes import execution_attempts

    ordinary = AuthContext(
        token_hash=b"ordinary-bearer-digest",
        type="worker",
        scopes=["worker:report"],
        team_id=None,
        expires_at=None,
    )
    protected = ProtectedWorkerSession(
        worker_id=UUID("00000000-0000-4000-8000-000000000401"),
        worker_incarnation=UUID("00000000-0000-4000-8000-000000000402"),
        intent_id=UUID("00000000-0000-4000-8000-000000000403"),
        pool_name="oldlab",
        hostname="trt-eai-oldlab-3",
        candidate_sha="a" * 40,
        slurm_job_id="12345",
        credential_sha256="b" * 64,
    )

    @asynccontextmanager
    async def session_factory() -> AsyncIterator[object]:
        yield object()

    async def verify_bearer_token(*_args: object, **_kwargs: object) -> AuthContext:
        return ordinary

    monkeypatch.setattr(execution_attempts, "verify_bearer_token", verify_bearer_token)
    request = Request(
        {
            "type": "http",
            "app": SimpleNamespace(
                state=SimpleNamespace(session_factory=session_factory),
            ),
        }
    )
    request.state.protected_worker_session = protected

    authenticated = await execution_attempts._worker_auth(
        request,
        "Bearer ordinary",
        scope="worker:report",
    )

    assert authenticated.token_hash == bytes.fromhex("b" * 64)
    assert ordinary.token_hash == b"ordinary-bearer-digest"


def test_runtime_database_url_loader_accepts_owner_only_regular_file(
    tmp_path: Path,
) -> None:
    module = _module()
    path = _write_runtime_url(tmp_path / "runtime-database-url")

    loaded = module.load_protected_worker_runtime_db_url(path)

    assert loaded.drivername == "postgresql+psycopg"
    assert loaded.username == "guard_runtime"
    assert loaded.database == "loom"


@pytest.mark.parametrize("unsafe", ("symlink", "permissions", "relative", "oversized"))
def test_runtime_database_url_loader_rejects_unsafe_files(
    tmp_path: Path,
    unsafe: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()
    target = _write_runtime_url(tmp_path / "runtime-database-url")
    path = target
    if unsafe == "symlink":
        path = tmp_path / "runtime-database-url-link"
        path.symlink_to(target)
    elif unsafe == "permissions":
        target.chmod(0o640)
    elif unsafe == "relative":
        monkeypatch.chdir(tmp_path)
        path = Path("runtime-database-url")
    else:
        target.write_bytes(b"x" * (module.MAX_RUNTIME_DATABASE_URL_BYTES + 1))

    with pytest.raises(module.ProtectedWorkerRuntimeConfigurationError):
        module.load_protected_worker_runtime_db_url(path)


def test_runtime_database_url_loader_opens_fifo_nonblocking_and_rejects_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()
    path = tmp_path / "runtime-database-url"
    os.mkfifo(path, mode=0o600)
    real_open = os.open

    def open_nonblocking(candidate: object, flags: int, *args: object, **kwargs: object) -> int:
        if candidate == path.name:
            assert flags & os.O_NONBLOCK
        return real_open(candidate, flags, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(module.os, "open", open_nonblocking)

    with pytest.raises(module.ProtectedWorkerRuntimeConfigurationError):
        module.load_protected_worker_runtime_db_url(path)


@pytest.mark.parametrize(
    "payload",
    (
        b"",
        b"not-a-database-url\n",
        b"postgresql+psycopg://guard:secret@postgres/loom\nextra\n",
        b"postgresql+psycopg://guard:secret@postgres/loom\x00\n",
        b"mysql://guard:secret@database/loom\n",
    ),
)
def test_runtime_database_url_loader_rejects_malformed_content(
    tmp_path: Path,
    payload: bytes,
) -> None:
    module = _module()
    path = tmp_path / "runtime-database-url"
    path.write_bytes(payload)
    path.chmod(0o600)

    with pytest.raises(module.ProtectedWorkerRuntimeConfigurationError):
        module.load_protected_worker_runtime_db_url(path)
