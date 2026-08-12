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
    PipelineMaterializeInputsV1,
    materialize_pipeline_inputs,
)


class MaterializeAdapter:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def materialize_inputs(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {
            "materialization_id": str(uuid4()),
            "state": "committed",
            "results": [],
        }


def _request(adapter: object | None) -> Request:
    app = FastAPI()
    if adapter is not None:
        app.state.pipeline_public_adapter = adapter
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/v1/pipeline-recipes/behavior-recovery/1/materialize-inputs",
            "headers": [],
            "app": app,
        }
    )


def _member(team_id: Any, user_id: Any) -> AuthContext:
    return AuthContext(
        token_hash=b"materialize",
        type="user",
        scopes=["read:own", "submit"],
        team_id=team_id,
        expires_at=None,
        user_id=user_id,
        role="member",
    )


async def test_materialize_route_forwards_only_normalized_typed_request() -> None:
    team_id = uuid4()
    user_id = uuid4()
    task_set_id = uuid4()
    input_ids = {name: uuid4() for name in ("dataset", "policy", "mop_bank")}
    payload = PipelineMaterializeInputsV1.model_validate(
        {
            "inputs": input_ids,
            "parameters": {"episodes_per_instance": 1, "seed_base": 0},
            "task_set_id": task_set_id,
        }
    )
    adapter = MaterializeAdapter()
    session = SimpleNamespace(marker="same-session")

    result = await materialize_pipeline_inputs(
        request=_request(adapter),
        sc=cast(Any, (session, _member(team_id, user_id))),
        name="behavior-recovery",
        version=1,
        payload=payload,
        idempotency_key="materialize-exact",
    )

    assert result["state"] == "committed"
    assert len(adapter.calls) == 1
    assert adapter.calls[0] == {
        "session": session,
        "team_id": team_id,
        "user_id": user_id,
        "recipe_name": "behavior-recovery",
        "recipe_version": 1,
        "payload": payload,
        "idempotency_key": "materialize-exact",
    }


async def test_materialize_missing_adapter_fails_closed_before_side_effect() -> None:
    team_id = uuid4()
    user_id = uuid4()
    payload = PipelineMaterializeInputsV1(
        inputs={"dataset": uuid4(), "policy": uuid4(), "mop_bank": uuid4()},
        parameters={"episodes_per_instance": 1, "seed_base": 0},
        task_set_id=uuid4(),
    )

    with pytest.raises(HTTPException) as exc:
        await materialize_pipeline_inputs(
            request=_request(None),
            sc=cast(Any, (SimpleNamespace(), _member(team_id, user_id))),
            name="behavior-recovery",
            version=1,
            payload=payload,
            idempotency_key="materialize-no-adapter",
        )
    assert exc.value.status_code == 503
    assert cast(object, exc.value.detail) == {
        "reason_code": "adapter_unavailable",
        "message": "Pipeline adapter is not configured",
    }


def test_materialize_request_is_closed_and_strict_at_route_boundary() -> None:
    base = {
        "inputs": {"dataset": uuid4(), "policy": uuid4(), "mop_bank": uuid4()},
        "parameters": {"episodes_per_instance": 1, "seed_base": 0},
        "task_set_id": uuid4(),
    }
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        PipelineMaterializeInputsV1.model_validate({**base, "raw_graph": {}})
    with pytest.raises(ValidationError):
        PipelineMaterializeInputsV1.model_validate({**base, "task_set_id": str(uuid4())})


def test_materialize_behavior_defaults_are_expanded_and_invalid_values_reject() -> None:
    base = {
        "inputs": {"dataset": uuid4(), "policy": uuid4(), "mop_bank": uuid4()},
        "task_set_id": uuid4(),
    }
    omitted = PipelineMaterializeInputsV1.model_validate({**base, "parameters": {}})
    explicit = PipelineMaterializeInputsV1.model_validate(
        {
            **base,
            "parameters": {"episodes_per_instance": 1, "seed_base": 0},
        }
    )
    assert omitted == explicit
    assert omitted.parameters == {"episodes_per_instance": 1, "seed_base": 0}

    for parameters in (
        {"episodes_per_instance": 0},
        {"episodes_per_instance": True},
        {"seed_base": -1},
        {"seed_base": 2**32},
        {"extra": 1},
    ):
        with pytest.raises(ValidationError):
            PipelineMaterializeInputsV1.model_validate({**base, "parameters": parameters})
