"""The real worker preparation path must preserve signed image/start bindings."""

import importlib
import json
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from loom_worker import main_loop as ml
from loom_worker.runner_pool import RunnerPool
from loom_worker.vllm_registry import WorkerVLLMRegistry
from tests.unit.test_main_loop_cleanup import _FakeCPClient, _FakeSettings
from tests.unit.test_task_image_publication_signing import NOW
from tests.unit.test_worker_task_image_execution import accepting, evidence


def delivery(kwargs):
    return dict(
        schema="loom.task-image-execution-delivery/v2",
        claim=kwargs["expected_claim"].model_dump(mode="json", exclude_none=True),
        grant_envelope=kwargs["wire"].decode(), frozen_plan=kwargs["plan_wire"].decode(),
        publications=[item.decode() for item in kwargs["publication_wires"]],
        keyset=kwargs["keyset_wire"].decode(),
    )


@pytest.mark.parametrize("case", ["valid", "no_root", "wrong_worker", "wrong_task", "layered", "denied"])
async def test_signed_claim_preparation_and_start_stay_bound(tmp_path, monkeypatch, case):
    task_dir = tmp_path / "source"
    task_dir.mkdir()
    grant, kwargs = evidence(task_dir)
    cp, settings, captured = _FakeCPClient(), _FakeSettings(), {}
    cp.consume_task_image_execution_start = accepting()
    if case == "denied":
        cp.consume_task_image_execution_start = AsyncMock(side_effect=ConnectionError("offline"))
    m = importlib.import_module("loom_worker.task_image_execution")
    assert hasattr(m, "WorkerExecutionTrust"), "main-loop release trust adapter missing"
    trust = m.WorkerExecutionTrust(
        root=kwargs["trust_root"], purpose="production", shadow_campaign_id=None,
    )
    payload = dict(
        trial_id=grant["claim"]["trial_id"], team_id=grant["claim"]["team_id"],
        task_id=json.loads(grant["canonical_task_config"])["task"]["id"],
        attempt_count=grant["claim"]["trial_attempt_count"],
        config={"agent_name": "oracle", "agent_model": None},
        task_image_execution=delivery(kwargs),
    )
    worker_id = UUID(grant["claim"]["worker_id"])
    if case == "wrong_worker":
        worker_id = UUID("99999999-9999-4999-8999-999999999999")
    if case == "wrong_task":
        payload["task_id"] = "different-task"
    expected_image = grant["components"][0]["image"]
    materialize = AsyncMock(return_value=task_dir)
    resolve = AsyncMock(return_value="unsigned:layer" if case == "layered" else expected_image)
    layer = AsyncMock(side_effect=AssertionError("must not derive an unsigned layer"))
    monkeypatch.setattr(ml, "_materialize_task_dir", materialize)
    monkeypatch.setattr(ml, "resolve_task_image", resolve)
    monkeypatch.setattr(ml, "_resolve_layered_trial_image", layer)
    monkeypatch.setattr(ml, "_host_cpu_arch", lambda: "x86_64")
    monkeypatch.setattr(m, "_clock", lambda: NOW)

    class Runner:
        def __init__(self, **values):
            captured.update(values)

        async def run(self):
            assert await captured["start_authorization"]() is True
            captured["ran"] = True

        async def interrupt_attempt(self):
            pass

    runner = Mock(side_effect=Runner)
    monkeypatch.setattr(ml, "LocalTrialRunner", runner)
    pool = RunnerPool(max_concurrent=1)
    await ml._spawn_trial(
        pool=pool, settings=settings, cp_client=cp, gateway_client=None,
        object_store=None, worker_id=worker_id, payload=payload,
        vllm_registry=WorkerVLLMRegistry(enabled=False),
        execution_trust=None if case == "no_root" else trust,
    )
    await pool.wait_all(timeout=2)
    assert cp.bundle_requests == 0
    layer.assert_not_called()
    if case == "valid":
        assert captured["ran"]
        assert captured["start_authorization"] is not None
        assert resolve.call_args.kwargs["registry_image"] == expected_image
        assert resolve.call_args.kwargs["build_if_missing"] is False
        assert captured["sidecar_runtime_factory"]().registry_images == {
            item["component"]: item["image"] for item in grant["components"]
        }
        cp.consume_task_image_execution_start.assert_awaited_once()
        assert not task_dir.exists()
    else:
        assert not captured.get("ran")
        assert cp.patch_calls[-1]["state"] == "failed"
        if case != "denied":
            cp.consume_task_image_execution_start.assert_not_called()
        if case in {"no_root", "wrong_worker", "wrong_task"}:
            materialize.assert_not_called()
            runner.assert_not_called()
        else:
            assert not task_dir.exists()
