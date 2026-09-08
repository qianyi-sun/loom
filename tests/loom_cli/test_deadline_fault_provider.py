"""The live fixture requires an authenticated receipt join, not request order."""

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr

from loom_cli.deadline_canary_manifest import render_fixture_resources
from loom_cli.deadline_fault_provider import (
    MODEL,
    CanaryBinding,
    FaultLedger,
    ReceiptApproval,
    _serve_bounded,
    create_fault_app,
)


def test_unapproved_request_cannot_select_hold_or_success() -> None:
    binding = CanaryBinding(case="B", team_id=uuid4(), provider_connection_id=uuid4())
    ledger = FaultLedger(binding)
    receipt_id = uuid4()
    ledger.queue(receipt_id)
    assert ledger.snapshot()["requests"][0]["action"] is None
    assert ledger.snapshot()["full_canary_passed"] is False


def test_retry_requires_new_attempt_and_new_grant_after_first_deadline() -> None:
    binding = CanaryBinding(case="B", team_id=uuid4(), provider_connection_id=uuid4())
    ledger = FaultLedger(binding)
    now = datetime.now(UTC)
    trial_id, attempt1, grant1 = uuid4(), uuid4(), uuid4()
    ledger.arm(trial_id=trial_id, step_id="main")
    first = ReceiptApproval(
        receipt_id=uuid4(),
        team_id=binding.team_id,
        trial_id=trial_id,
        provider_connection_id=binding.provider_connection_id,
        step_id="main",
        agent_attempt_id=attempt1,
        step_jwt_id=grant1,
        deadline=now + timedelta(seconds=10),
    )
    ledger.queue(first.receipt_id)
    assert ledger.approve(first, now=now) == "hold"
    second = first.model_copy(update={"receipt_id": uuid4()})
    ledger.queue(second.receipt_id)
    with pytest.raises(ValueError, match="attempt"):
        ledger.approve(second, now=now + timedelta(seconds=1))


def test_cross_trial_receipt_rejected_without_exposing_identity() -> None:
    binding = CanaryBinding(case="A", team_id=uuid4(), provider_connection_id=uuid4())
    ledger = FaultLedger(binding)
    ledger.arm(trial_id=uuid4(), step_id="main")
    approval = ReceiptApproval(
        receipt_id=uuid4(),
        team_id=binding.team_id,
        trial_id=uuid4(),
        provider_connection_id=binding.provider_connection_id,
        step_id="main",
        agent_attempt_id=uuid4(),
        step_jwt_id=uuid4(),
        deadline=datetime.now(UTC) + timedelta(seconds=10),
    )
    ledger.queue(approval.receipt_id)
    with pytest.raises(ValueError, match="binding"):
        ledger.approve(approval)


def test_rendered_fixture_has_no_runtime_credentials_or_restart() -> None:
    binding = CanaryBinding(case="A", team_id=uuid4(), provider_connection_id=uuid4())
    docs = render_fixture_resources(
        run_id=uuid4(),
        binding=binding,
        candidate_sha="a" * 40,
        gateway_image="registry.invalid/loom-llm-gateway@sha256:" + "b" * 64,
    )
    job = docs[1]["spec"]
    pod = job["template"]["spec"]
    assert job["backoffLimit"] == 0 and job["activeDeadlineSeconds"] == 190
    assert pod["automountServiceAccountToken"] is False
    assert pod["restartPolicy"] == "Never"
    assert pod["containers"][0]["securityContext"]["readOnlyRootFilesystem"] is True
    serialized = json.dumps(docs)
    assert "loom-secrets" not in serialized and "DB_URL" not in serialized
    assert "hostPath" not in serialized and "hostNetwork" not in serialized
    assert docs[3]["spec"]["egress"] == []
    with pytest.raises(ValueError, match="immutable"):
        render_fixture_resources(
            run_id=uuid4(),
            binding=binding,
            candidate_sha="a" * 40,
            gateway_image="registry.invalid/loom-llm-gateway:latest",
        )


def test_replay_unbounded_requests_and_expired_grant_are_rejected() -> None:
    binding = CanaryBinding(case="A", team_id=uuid4(), provider_connection_id=uuid4())
    ledger = FaultLedger(binding)
    trial = uuid4()
    ledger.arm(trial_id=trial, step_id="main")
    rid = uuid4()
    ledger.queue(rid)
    with pytest.raises(ValueError):
        ledger.queue(rid)
    for _ in range(3):
        ledger.queue(uuid4())
    with pytest.raises(ValueError):
        ledger.queue(uuid4())
    assert len(ledger.snapshot()["requests"]) == 4
    receipt = ReceiptApproval(
        receipt_id=rid,
        team_id=binding.team_id,
        trial_id=trial,
        provider_connection_id=binding.provider_connection_id,
        step_id="main",
        agent_attempt_id=uuid4(),
        step_jwt_id=uuid4(),
        deadline=datetime.now(UTC),
    )
    with pytest.raises(ValueError, match="deadline"):
        ledger.approve(receipt)


async def test_http_auth_arm_and_cancel_boundaries() -> None:
    binding = CanaryBinding(case="A", team_id=uuid4(), provider_connection_id=uuid4())
    app = create_fault_app(
        binding, provider_key=SecretStr("p" * 40), operator_key=SecretStr("o" * 40)
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://fixture"
    ) as client:
        p = {"Authorization": "Bearer " + "p" * 40}
        o = {"Authorization": "Bearer " + "o" * 40}
        assert (await client.get("/operator/evidence", headers=p)).status_code == 401
        assert (await client.get("/v1/models", headers=o)).status_code == 401
        assert (await client.get("/v1/models", headers=p)).status_code == 200
        assert (
            await client.post(
                "/operator/arm", headers=o, json={"trial_id": str(uuid4()), "step_id": "main"}
            )
        ).status_code == 200
        body = {"model": MODEL, "messages": []}
        assert (await client.post("/v1/chat/completions", headers=p, json=body)).status_code == 409
        pending = asyncio.create_task(
            client.post(
                "/v1/chat/completions", headers={**p, "X-Request-ID": str(uuid4())}, json=body
            )
        )
        await asyncio.sleep(0.03)
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        state = (await client.get("/operator/evidence", headers=o)).json()
        assert state["requests"][0]["outcome"] == "cancelled"
        assert state["requests"][0]["action"] is None
        assert (await client.post("/operator/close", headers=o)).status_code == 200
        assert (await client.get("/healthz")).status_code == 503
        assert (await client.get("/v1/models", headers=p)).status_code == 410
        assert (await client.get("/operator/evidence", headers=o)).status_code == 200


def test_installed_module_cli_starts_without_gateway_credentials(tmp_path: Path) -> None:
    binding = CanaryBinding(case="A", team_id=uuid4(), provider_connection_id=uuid4())
    binding_path = tmp_path / "binding.json"
    binding_path.write_text(binding.model_dump_json())
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    env = {key: value for key, value in os.environ.items() if not key.startswith("LOOM_")}
    env.update(CANARY_TEST_PROVIDER="p" * 40, CANARY_TEST_OPERATOR="o" * 40)
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "loom_cli.deadline_fault_provider",
            "--binding",
            str(binding_path),
            "--provider-key-source",
            "env:CANARY_TEST_PROVIDER",
            "--operator-key-source",
            "env:CANARY_TEST_OPERATOR",
            "--port",
            str(port),
            "--lifetime-seconds",
            "30",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        with httpx.Client(timeout=0.5, trust_env=False) as client:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                try:
                    result = client.get(f"http://127.0.0.1:{port}/healthz")
                    if result.status_code == 200:
                        break
                except httpx.TransportError:
                    pass
                time.sleep(0.03)
            else:
                raise AssertionError("standalone fixture CLI did not become ready")
    finally:
        process.terminate()
        try:
            stdout, stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate(timeout=3)
    # Pinned Uvicorn restores handlers and re-raises SIGTERM after its graceful
    # shutdown. A bounded intentional stop is therefore -SIGTERM, not zero.
    assert process.returncode == -signal.SIGTERM
    assert b"p" * 40 not in stdout + stderr and b"o" * 40 not in stdout + stderr


async def test_server_lifetime_expires_without_external_signal() -> None:
    binding = CanaryBinding(case="A", team_id=uuid4(), provider_connection_id=uuid4())
    app = create_fault_app(
        binding, provider_key=SecretStr("p" * 40), operator_key=SecretStr("o" * 40)
    )
    started = time.monotonic()
    await _serve_bounded(app, host="127.0.0.1", port=0, lifetime=0.1)
    assert time.monotonic() - started < 3
