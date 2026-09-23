"""Real WebSocket/TCP transport and PostgreSQL lease fencing; no external services."""
from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from loom.db.schema import ServiceExecutionTarget
from loom.execution_contract import NetworkAccess
from loom.execution_runtime_contract import TASK_EGRESS_OUTPUT
from loom.models.networking import WebAllowlist, WebDestination
from loom_control_plane.service_execution import enqueue_execution_transition
from loom_control_plane.service_execution_output import VerifiedExecutionPod
from loom_llm_gateway.routes import task_egress
from loom_llm_gateway.task_egress import TaskEgressConfig, TaskEgressRuntime
from tests.integration.test_issue_1748_deadline_canary import _serve
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401 -- disposable fixture ownership
    _requirements,
    _reserve,
    _runtime_contract,
    _seed_ready_trial,
)


async def test_task_tunnel_authenticates_native_identity_and_revokes_active_connection(
    postgres_url: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import loom_llm_gateway.task_egress as transport

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    app = FastAPI()
    app.state.session_factory = sessions
    app.state.task_egress = TaskEgressRuntime(TaskEgressConfig(protected_cidrs=("8.8.8.8/32",)))
    app.include_router(task_egress.router)
    policy = WebAllowlist(destinations=(WebDestination(host="packages.example.org", protocol="https"),))
    now = datetime.now(UTC)
    monkeypatch.setattr(transport, "_FENCE_SECONDS", 0.02)
    upstream_handlers = set()
    async def upstream(reader, writer):
        current = asyncio.current_task()
        upstream_handlers.add(current)
        try:
            assert await reader.read(100) == b"download-request"
            writer.write(b"package-bytes")
            await writer.drain()
            await reader.read()
        finally:
            writer.close()
            await writer.wait_closed()
            upstream_handlers.remove(current)
    server = await asyncio.start_server(upstream, "127.0.0.1", 0)
    address = server.sockets[0].getsockname()
    dials = []
    async def fixture_dial(destination, protected):
        # Only the Internet dial is replaced: real WS, sockets, Pod checks,
        # immutable policy, DB fencing and cleanup remain production code.
        dials.append(destination)
        return await asyncio.open_connection(*address)
    monkeypatch.setattr(task_egress, "connect_destination", fixture_dial)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now,
                requirements=_requirements().model_copy(update={"network_access": NetworkAccess.APPROVED_ALLOWLIST, "task_egress": policy}),
                runtime_contract=_runtime_contract(now=now).model_copy(update={"task_egress": policy, "output_declarations": (TASK_EGRESS_OUTPUT,)}))
            row = await session.get(ServiceExecutionTarget, target.target_id)
            row.spec_json = {**row.spec_json, "cluster_scope_id": "egress-fixture", "pod_identity_audience": "loom-execution"}
            lease.pod_uid, lease.pod_ip, lease.observed_state = "egress-pod", "10.42.0.50", "running"
            await session.commit()
        app.state.execution_pod_reviewer = AsyncMock()
        app.state.execution_pod_reviewer.review.return_value = VerifiedExecutionPod(
            cluster_scope_id="egress-fixture", namespace=target.namespace_name,
            service_account="loom-execution-attempt", audience="loom-execution", pod_uid="egress-pod",
        )
        headers = {"Authorization": "Bearer bound-pod-credential", "X-Loom-Execution-Lease-Id": str(lease.id),
            "X-Loom-Execution-Generation": "1", "X-Loom-Execution-Role": "attempt",
            "X-Loom-Runtime-Contract-SHA256": lease.runtime_contract_sha256,
            "X-Loom-Phase-Deadline": lease.deadline_at.isoformat()}
        async with _serve(app) as root:
            url = root.replace("http:", "ws:") + "/internal/service-execution/task-egress"
            for altered in ({"Authorization": "wrong"}, {"X-Loom-Execution-Role": "verifier"},
                            {"X-Loom-Runtime-Contract-SHA256": "sha256:" + "0" * 64}):
                async with connect(url, additional_headers={**headers, **altered}) as ws:
                    with pytest.raises(ConnectionClosed):
                        await ws.recv()
                assert not dials
            async with connect(url, additional_headers=headers) as ws:
                await ws.send(json.dumps(policy.destinations[0].model_dump()))
                assert json.loads(await ws.recv()) == {"status": "ready"}
                await ws.send(b"download-request")
                assert await ws.recv() == b"package-bytes"
                async with sessions() as session:
                    await enqueue_execution_transition(session, lease_id=lease.id,
                        expected_generation=1, desired_state="cancel", now=datetime.now(UTC))
                    await session.commit()
                with pytest.raises(ConnectionClosed) as exc:
                    await asyncio.wait_for(ws.recv(), timeout=2)
                assert exc.value.rcvd.reason == "task_egress_identity_rejected"
            async with connect(url, additional_headers=headers) as ws:
                with pytest.raises(ConnectionClosed):
                    await ws.recv()
            assert dials == [policy.destinations[0]]
            assert not app.state.task_egress.connections
            assert app.state.execution_pod_reviewer.review.call_args.kwargs["token"] == "bound-pod-credential"
    finally:
        server.close()
        await server.wait_closed()
        if upstream_handlers:
            await asyncio.wait_for(asyncio.gather(*upstream_handlers), timeout=2)
        await engine.dispose()
