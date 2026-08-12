from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest
from fastapi import FastAPI, HTTPException
from starlette.requests import Request

from loom.auth import AuthContext
from loom_service.routes.pipeline import (
    PipelineInputImportCreateV1,
    create_pipeline_input_import,
)


def _request(adapter: object) -> Request:
    app = FastAPI()
    app.state.pipeline_public_adapter = adapter
    return Request({"type": "http", "method": "POST", "path": "/", "headers": [], "app": app})


async def test_member_cannot_reach_admin_import_adapter() -> None:
    class Adapter:
        called = False

        async def create_import(self, **_kwargs: Any) -> None:
            self.called = True

    class Result:
        def scalar_one_or_none(self) -> str:
            return "member"

    class Session:
        async def execute(self, _statement: object) -> Result:
            return Result()

    adapter = Adapter()
    context = AuthContext(
        token_hash=b"member",
        type="user",
        scopes=["read:own", "submit"],
        team_id=uuid4(),
        expires_at=None,
        user_id=uuid4(),
        role="member",
    )
    with pytest.raises(HTTPException) as error:
        await create_pipeline_input_import(
            request=_request(adapter),
            sc=cast(Any, (Session(), context)),
            payload=PipelineInputImportCreateV1.model_construct(
                kind="policy",
                recipe="behavior-recovery@1",
                manifest={"schema_version": "behavior.input-import.v1"},
            ),
            idempotency_key="member-import",
        )
    assert error.value.status_code == 403
    assert not adapter.called


async def test_materialization_requires_authenticated_submit_scope() -> None:
    from loom_service.routes.pipeline import (
        PipelineMaterializeInputsV1,
        materialize_pipeline_inputs,
    )

    context = AuthContext(
        token_hash=b"readonly",
        type="user",
        scopes=["read:own"],
        team_id=uuid4(),
        expires_at=None,
        user_id=uuid4(),
        role="member",
    )
    with pytest.raises(HTTPException) as error:
        await materialize_pipeline_inputs(
            request=_request(SimpleNamespace()),
            sc=cast(Any, (SimpleNamespace(), context)),
            name="behavior-recovery",
            version=1,
            payload=PipelineMaterializeInputsV1(
                task_set_id=uuid4(),
                inputs={name: uuid4() for name in ("dataset", "policy", "mop_bank")},
                parameters={"episodes_per_instance": 1, "seed_base": 0},
            ),
            idempotency_key="readonly-materialize",
        )
    assert error.value.status_code == 403
