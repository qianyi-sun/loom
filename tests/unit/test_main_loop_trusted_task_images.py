"""The real worker preparation path must preserve signed image/start bindings."""

import asyncio
import importlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from loom_worker import main_loop as ml
from loom_worker.runner_pool import RunnerPool
from loom_worker.vllm_registry import WorkerVLLMRegistry
from tests.unit.test_main_loop_cleanup import _FakeCPClient, _FakeSettings
from tests.unit.test_task_image_publication_signing import NOW
from tests.unit.test_worker_claim_loop import _healthy_setup_node
from tests.unit.test_worker_task_image_execution import accepting, evidence


def delivery(kwargs):
    return dict(
        schema="loom.task-image-execution-delivery/v2",
        claim=kwargs["expected_claim"].model_dump(mode="json", exclude_none=True),
        grant_envelope=kwargs["wire"].decode(), frozen_plan=kwargs["plan_wire"].decode(),
        publications=[item.decode() for item in kwargs["publication_wires"]],
        keyset=kwargs["keyset_wire"].decode(),
    )


@pytest.mark.parametrize("case", ["valid", "no_root", "wrong_worker", "wrong_task", "layered", "denied", "cancelled"])
async def test_signed_claim_preparation_and_start_stay_bound(tmp_path, monkeypatch, case):
    task_dir = tmp_path / "source"
    task_dir.mkdir()
    grant, kwargs = evidence(task_dir)
    cp, settings, captured = _FakeCPClient(), _FakeSettings(), {}
    cp.consume_task_image_execution_start = accepting()
    if case == "denied":
        cp.consume_task_image_execution_start = AsyncMock(side_effect=ConnectionError("offline"))
    elif case == "cancelled":
        cp.consume_task_image_execution_start = AsyncMock(side_effect=asyncio.CancelledError)
    m = importlib.import_module("loom_worker.task_image_execution")
    assert hasattr(m, "WorkerExecutionTrust"), "main-loop release trust adapter missing"
    trust = m.WorkerExecutionTrust(
        root=kwargs["trust_root"], purpose="production", shadow_campaign_id=None,
        clock=lambda: NOW,
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
        if case == "cancelled":
            assert cp.patch_calls == []
        else:
            assert cp.patch_calls[-1]["state"] == "failed"
        if case not in {"denied", "cancelled"}:
            cp.consume_task_image_execution_start.assert_not_called()
        if case in {"no_root", "wrong_worker", "wrong_task"}:
            materialize.assert_not_called()
            runner.assert_not_called()
        else:
            assert not task_dir.exists()


async def test_shared_claim_parser_preserves_signed_wire_and_trusted_context(tmp_path, monkeypatch):
    grant, kwargs = evidence(tmp_path)
    raw_delivery = delivery(kwargs)
    payload = dict(
        trial_id=grant["claim"]["trial_id"], team_id=grant["claim"]["team_id"],
        task_id=json.loads(grant["canonical_task_config"])["task"]["id"],
        attempt_count=grant["claim"]["trial_attempt_count"], config={}, requires_caps={},
        provider_connection_id=None, family_key=None, family_state_uri=None,
        family_run_spec=None, state="claimed", task_image_execution=raw_delivery,
    )
    cp = _FakeCPClient()
    cp.claim_work = AsyncMock(side_effect=[dict(
        schema_version="loom.work-claim.v1", work_kind="trial", payload=payload,
    ), None])
    spawn = AsyncMock()
    monkeypatch.setattr(ml, "_spawn_trial", spawn)
    m = importlib.import_module("loom_worker.task_image_execution")
    trust = m.WorkerExecutionTrust(
        root=kwargs["trust_root"], purpose="production", shadow_campaign_id=None,
    )
    settings = _FakeSettings()
    settings.max_concurrent = 1
    assert await ml._claim_available_work(
        pool=RunnerPool(max_concurrent=1), settings=settings, cp_client=cp,
        gateway_client=None, object_store=None, worker_id=UUID(grant["claim"]["worker_id"]),
        capability_snapshot_digest="sha256:" + "1" * 64,
        pipeline_run=AsyncMock(), vllm_registry=WorkerVLLMRegistry(enabled=False),
        read_setup_health=_healthy_setup_node, execution_trust=trust,
    ) == 1
    assert spawn.call_args.kwargs["payload"]["task_image_execution"] == raw_delivery
    assert spawn.call_args.kwargs["execution_trust"] is trust


async def test_release_trust_cannot_enable_legacy_body_capability_claims(tmp_path):
    _, kwargs = evidence(tmp_path)
    m = importlib.import_module("loom_worker.task_image_execution")
    trust = m.WorkerExecutionTrust(
        root=kwargs["trust_root"], purpose="production", shadow_campaign_id=None,
    )
    cp = Mock()
    with pytest.raises(ValueError, match="authenticated shared-queue"):
        await ml._claim_available_work(
            pool=RunnerPool(max_concurrent=1), settings=_FakeSettings(), cp_client=cp,
            gateway_client=None, object_store=None, worker_id=UUID(kwargs["expected_claim"].worker_id),
            capability_snapshot_digest=None, pipeline_run=None,
            vllm_registry=WorkerVLLMRegistry(enabled=False), execution_trust=trust,
        )
    assert cp.mock_calls == []


async def test_release_trust_rejects_http_before_worker_registration(tmp_path, monkeypatch):
    _, kwargs = evidence(tmp_path)
    m = importlib.import_module("loom_worker.task_image_execution")
    trust = m.WorkerExecutionTrust(
        root=kwargs["trust_root"], purpose="production", shadow_campaign_id=None,
    )
    effects = Mock(side_effect=AssertionError("worker assembly must not start"))
    monkeypatch.setattr(ml, "install_signal_handlers", effects)
    with pytest.raises(ValueError, match="HTTPS"):
        await ml.run_worker(
            SimpleNamespace(control_plane_url="http://cp.example"), execution_trust=trust,
        )
    effects.assert_not_called()
