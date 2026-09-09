"""Real Gateway HTTP + PostgreSQL fencing behind a NAT/proxy address."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.auth import verify_step_jwt
from loom.db.schema import ServiceExecutionLease, ServiceExecutionTarget
from loom.nebius_kubernetes import NebiusKubernetesConnection
from loom_control_plane.service_execution import enqueue_execution_transition
from loom_control_plane.service_execution_output import (
    ServiceExecutionBrokerError,
    ServiceExecutionPeerV1,
    authorize_service_execution_peer,
)
from loom_llm_gateway.pod_identity import ExecutionPodReviewer
from loom_llm_gateway.routes import service_execution
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401 -- owned disposable state
    _reserve,
    _seed_ready_trial,
)


async def test_gateway_native_pod_identity_keeps_fences_across_public_proxy(
    postgres_url: str,
) -> None:
    now = datetime.now(UTC)
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    signing_key = "disposable-pod-identity-signing-key-" + "x" * 64
    app = FastAPI()
    app.state.session_factory = sessions
    app.state.settings = SimpleNamespace(
        step_jwt_signing_key=SimpleNamespace(get_secret_value=lambda: signing_key)
    )
    app.state.service_execution_output_service = SimpleNamespace(
        prepare=AsyncMock(return_value={"accepted": True})
    )
    app.include_router(service_execution.router)
    review_calls = []
    native_state = {"uid": "pod-west", "authenticated": True, "audience": "loom-execution"}
    reviewer = ExecutionPodReviewer(
        {
            "west": NebiusKubernetesConnection(
                endpoint="https://west.example",
                ca_file=Path("unused"),
                credentials_file=Path("unused"),
            )
        }
    )
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            target_row = await session.get(ServiceExecutionTarget, target.target_id)
            target_row.spec_json = {
                **target_row.spec_json,
                "cluster_scope_id": "west",
                "pod_identity_audience": "loom-execution",
            }
            lease.pod_uid = None
            lease.pod_ip = "10.42.0.12"
            lease.observed_state = "running"
            await session.commit()
            lease_id = lease.id

        revoke_on_review = False

        async def native_api(request: httpx.Request) -> httpx.Response:
            nonlocal revoke_on_review
            if revoke_on_review:
                revoke_on_review = False
                async with sessions() as other_session:
                    await enqueue_execution_transition(
                        other_session,
                        lease_id=lease_id,
                        expected_generation=1,
                        desired_state="cancel",
                        now=now,
                    )
                    await other_session.commit()
            review_calls.append(request)
            assert request.url.host == "west.example"
            body = json.loads(request.content)
            assert body["spec"]["audiences"] == ["loom-execution"]
            return httpx.Response(
                201,
                json={
                    "status": {
                        "authenticated": native_state["authenticated"],
                        "audiences": [native_state["audience"]],
                        "user": {
                            "username": f"system:serviceaccount:{target.namespace_name}:loom-execution-attempt",
                            "extra": {
                                "authentication.kubernetes.io/pod-uid": [native_state["uid"]]
                            },
                        },
                    }
                },
            )

        reviewer._clients["west"] = httpx.AsyncClient(transport=httpx.MockTransport(native_api))
        reviewer._credentials["west"] = SimpleNamespace(
            get_token=AsyncMock(return_value="native-iam"), close=AsyncMock()
        )
        app.state.execution_pod_reviewer = reviewer
        identity = {"lease_id": str(lease_id), "generation": 1, "execution_role": "attempt"}
        output = {
            **identity,
            "schema_version": "loom.service-execution-output-prepare.v1",
            "request_id": str(uuid4()),
            "files": [
                {
                    "relative_path": "result.json",
                    "media_type": "application/json",
                    "size_bytes": 2,
                    "sha256": "sha256:" + "a" * 64,
                }
            ],
        }
        transport = httpx.ASGITransport(
            app=app, client=("192.0.2.10", 12345), raise_app_exceptions=False
        )
        async with httpx.AsyncClient(
            transport=transport, base_url="https://loom.example"
        ) as client:
            spoofed = {"X-Forwarded-For": "10.42.0.12"}
            response = await client.post(
                "/internal/service-execution/token", json=identity, headers=spoofed
            )
            assert response.status_code == 403 and not review_calls
            headers = {**spoofed, "Authorization": "Bearer bound-token-one"}
            response = await client.post(
                "/internal/service-execution/token", json=identity, headers=headers
            )
            assert (
                response.status_code == 503
                and response.json()["detail"] == "workload_identity_not_observed"
            )
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, lease_id)
                current.pod_uid = "pod-west"
                await session.commit()
            response = await client.post(
                "/internal/service-execution/token", json=identity, headers=headers
            )
            assert response.status_code == 200, response.text
            assert (
                verify_step_jwt(
                    response.json()["token"], signing_key=signing_key
                ).service_execution_lease_id
                == lease_id
            )
            assert json.loads(review_calls[-1].content)["spec"]["token"] == "bound-token-one"
            headers["Authorization"] = "Bearer rotated-token-two"
            response = await client.post(
                "/internal/service-execution/outputs/prepare", json=output, headers=headers
            )
            assert response.status_code == 201, response.text
            assert json.loads(review_calls[-1].content)["spec"]["token"] == "rotated-token-two"
            for invalid in ("foreign-pod", "deleted-pod", "wrong-audience"):
                native_state.update(
                    uid="foreign" if invalid == "foreign-pod" else "pod-west",
                    authenticated=invalid != "deleted-pod",
                    audience="kubernetes" if invalid == "wrong-audience" else "loom-execution",
                )
                response = await client.post(
                    "/internal/service-execution/token", json=identity, headers=headers
                )
                assert response.status_code == 403, response.text
            native_state.update(uid="pod-west", authenticated=True, audience="loom-execution")
            # Revoke in a separate committed transaction while TokenReview is in flight.
            revoke_on_review = True
            response = await client.post(
                "/internal/service-execution/token", json=identity, headers=headers
            )
            assert (
                response.status_code == 409
                and response.json()["detail"] == "execution_generation_fenced"
            )
            response = await client.post(
                "/internal/service-execution/outputs/prepare", json=output, headers=headers
            )
            assert response.status_code == 201, response.text
            input_headers = {
                **headers,
                "X-Loom-Execution-Lease-Id": str(lease_id),
                "X-Loom-Execution-Generation": "1",
                "X-Loom-Execution-Role": "attempt",
            }
            response = await client.get(
                "/internal/service-execution/inputs/manifest", headers=input_headers
            )
            assert (
                response.status_code == 409
                and response.json()["detail"] == "execution_generation_fenced"
            )
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, lease_id)
                current.cleanup_requested_at = now - timedelta(seconds=2)
                current.cleanup_deadline_at = now - timedelta(seconds=1)
                await session.commit()
            response = await client.post(
                "/internal/service-execution/outputs/prepare", json=output, headers=headers
            )
            assert (
                response.status_code == 409
                and response.json()["detail"] == "execution_output_window_closed"
            )
        async with sessions() as session:
            # Internal callers cannot silently downgrade a native target to IP auth.
            try:
                await authorize_service_execution_peer(
                    session,
                    identity=ServiceExecutionPeerV1.model_validate(identity),
                    peer_ip="10.42.0.12",
                )
            except ServiceExecutionBrokerError as exc:
                assert exc.reason == "execution_pod_identity_invalid"
            else:
                raise AssertionError("native identity was bypassed")
    finally:
        await reviewer.close()
        await engine.dispose()
