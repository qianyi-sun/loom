from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest
from fastapi import FastAPI, HTTPException
from pydantic import ValidationError
from starlette.requests import Request

from loom.auth import AuthContext
from loom_service.routes.pipeline import (
    PipelineInputImportCompleteV1,
    PipelineInputImportCreateV1,
    create_pipeline_input_import,
    put_pipeline_input_part,
)


class ImportAdapter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def create_import(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("create", kwargs))
        return {"import_id": str(uuid4()), "state": "uploading"}

    async def put_import_part(self, **kwargs: Any) -> dict[str, Any]:
        chunks = [chunk async for chunk in kwargs["body"]]
        body = b"".join(chunks)
        self.calls.append(
            (
                "part",
                {
                    **kwargs,
                    "body": body,
                },
            )
        )
        return {"part_number": kwargs["part_number"], "size_bytes": len(body)}


def _admin(team_id: Any, user_id: Any) -> AuthContext:
    return AuthContext(
        token_hash=b"import-admin",
        type="user",
        scopes=["read:own", "submit"],
        team_id=team_id,
        expires_at=None,
        user_id=user_id,
        role="platform_admin",
    )


def _request(
    adapter: object,
    *,
    method: str = "POST",
    body: bytes = b"",
    headers: dict[str, str] | None = None,
) -> Request:
    app = FastAPI()
    app.state.pipeline_public_adapter = adapter
    sent = False

    async def receive() -> dict[str, Any]:
        nonlocal sent
        if sent:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    encoded_headers = [
        (name.lower().encode("ascii"), value.encode("ascii"))
        for name, value in (headers or {}).items()
    ]
    return Request(
        {
            "type": "http",
            "method": method,
            "path": "/api/v1/pipeline-input-imports",
            "headers": encoded_headers,
            "app": app,
        },
        receive=receive,
    )


async def test_import_create_and_streamed_part_preserve_admin_scope_and_headers() -> None:
    team_id = uuid4()
    user_id = uuid4()
    import_id = uuid4()
    upload_session_id = uuid4()
    payload_bytes = b"one deterministic tar.zst part"
    payload_sha = "sha256:" + "a" * 64
    adapter = ImportAdapter()
    session = SimpleNamespace(marker="import-session")
    sc = cast(Any, (session, _admin(team_id, user_id)))
    create_payload = PipelineInputImportCreateV1.model_construct(
        kind="dataset",
        manifest={"schema_version": "behavior.input-import.v1"},
        recipe="behavior-recovery@1",
    )

    created = await create_pipeline_input_import(
        request=_request(adapter),
        sc=sc,
        payload=create_payload,
        idempotency_key="import-create",
    )
    receipt = await put_pipeline_input_part(
        request=_request(
            adapter,
            method="PUT",
            body=payload_bytes,
            headers={
                "X-Loom-Upload-Session-Id": str(upload_session_id),
                "X-Loom-Upload-Token": "rotated-token",
                "Content-Length": str(len(payload_bytes)),
                "X-Loom-Content-SHA256": payload_sha,
            },
        ),
        sc=sc,
        import_id=import_id,
        part_number=1,
        upload_session_id=upload_session_id,
        upload_token="rotated-token",
        content_length=len(payload_bytes),
        content_sha256=payload_sha,
    )

    assert created["state"] == "uploading"
    assert receipt == {"part_number": 1, "size_bytes": len(payload_bytes)}
    assert adapter.calls[0] == (
        "create",
        {
            "session": session,
            "team_id": team_id,
            "user_id": user_id,
            "payload": create_payload,
            "idempotency_key": "import-create",
        },
    )
    part = adapter.calls[1][1]
    assert part["session"] is session
    assert part["team_id"] == team_id
    assert part["user_id"] == user_id
    assert part["import_id"] == import_id
    assert part["part_number"] == 1
    assert part["body"] == payload_bytes
    assert part["upload_session_id"] == upload_session_id
    assert part["upload_token"] == "rotated-token"
    assert part["content_length"] == len(payload_bytes)
    assert part["content_sha256"] == payload_sha


async def test_import_create_rejects_non_admin_before_adapter_call() -> None:
    team_id = uuid4()
    user_id = uuid4()
    adapter = ImportAdapter()
    member = AuthContext(
        token_hash=b"import-member",
        type="user",
        scopes=["read:own", "submit"],
        team_id=team_id,
        expires_at=None,
        user_id=user_id,
        role="member",
    )

    class Result:
        def scalar_one_or_none(self) -> str:
            return "member"

    class Session:
        async def execute(self, _statement: object) -> Result:
            return Result()

    with pytest.raises(HTTPException) as exc:
        await create_pipeline_input_import(
            request=_request(adapter),
            sc=cast(Any, (Session(), member)),
            payload=PipelineInputImportCreateV1.model_construct(
                kind="dataset",
                manifest={"schema_version": "behavior.input-import.v1"},
                recipe="behavior-recovery@1",
            ),
            idempotency_key="member-import",
        )
    assert exc.value.status_code == 403
    assert cast(dict[str, object], exc.value.detail)["reason_code"] == "team_admin_required"
    assert adapter.calls == []


def test_import_request_models_are_closed_and_scalar_types_are_strict() -> None:
    create = {
        "kind": "dataset",
        "manifest": {"schema_version": "behavior.input-import.v1"},
        "recipe": "behavior-recovery@1",
    }
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        PipelineInputImportCreateV1.model_validate({**create, "object_store_key": "private"})
    with pytest.raises(ValidationError):
        PipelineInputImportCompleteV1.model_validate(
            {
                "upload_session_id": uuid4(),
                "bundle_sha256": "sha256:" + "a" * 64,
                "bundle_size_bytes": True,
                "parts": [],
            }
        )
