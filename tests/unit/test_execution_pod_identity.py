"""Native TokenReview validates the explicit cluster, audience and bound Pod."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from loom.nebius_kubernetes import NebiusKubernetesConnection
from loom_control_plane.service_execution_output import ServiceExecutionBrokerError
from loom_llm_gateway.pod_identity import ExecutionPodReviewer


def _status() -> dict:
    return {
        "authenticated": True,
        "audiences": ["loom-execution"],
        "user": {
            "username": "system:serviceaccount:execution:attempt",
            "extra": {"authentication.kubernetes.io/pod-uid": ["pod-1"]},
        },
    }


@pytest.mark.parametrize(
    "invalid",
    [
        None,
        "denied",
        "audience",
        "namespace",
        "service-account",
        "unbound",
        "duplicate",
        "malformed",
    ],
)
async def test_native_review_binds_cluster_audience_and_pod(invalid: str | None) -> None:
    connections = {
        scope: NebiusKubernetesConnection(
            endpoint=f"https://{scope}.example",
            ca_file=Path("unused-ca"),
            credentials_file=Path("unused-key"),
        )
        for scope in ("north", "west")
    }
    reviewer = ExecutionPodReviewer(connections)
    observed = []

    def handler(request: httpx.Request) -> httpx.Response:
        observed.append(request)
        assert request.url.host == "west.example"
        assert request.headers["authorization"] == "Bearer native-west"
        assert json.loads(request.content)["spec"] == {
            "token": "bound-west",
            "audiences": ["loom-execution"],
        }
        status = _status()
        if invalid == "denied":
            status["authenticated"] = False
        if invalid == "audience":
            status["audiences"] = ["kubernetes"]
        if invalid == "namespace":
            status["user"]["username"] = "system:serviceaccount:foreign:attempt"
        if invalid == "service-account":
            status["user"]["username"] = "system:serviceaccount:execution:admin"
        if invalid == "unbound":
            status["user"]["extra"] = {}
        if invalid == "duplicate":
            status["user"]["extra"]["authentication.kubernetes.io/pod-uid"] *= 2
        return httpx.Response(201, json={"status": [] if invalid == "malformed" else status})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    reviewer._clients["west"] = client
    reviewer._credentials["west"] = SimpleNamespace(
        get_token=AsyncMock(return_value="native-west"), close=AsyncMock()
    )
    try:
        if invalid:
            with pytest.raises(ServiceExecutionBrokerError, match="execution_pod_identity_invalid"):
                await reviewer.review(
                    cluster_scope_id="west",
                    namespace="execution",
                    service_account="attempt",
                    audience="loom-execution",
                    token="bound-west",
                )
        else:
            result = await reviewer.review(
                cluster_scope_id="west",
                namespace="execution",
                service_account="attempt",
                audience="loom-execution",
                token="bound-west",
            )
            assert result.pod_uid == "pod-1" and result.cluster_scope_id == "west"
        assert len(observed) == 1
        with pytest.raises(ServiceExecutionBrokerError, match="execution_pod_review_unavailable"):
            await reviewer.review(
                cluster_scope_id="unknown",
                namespace="execution",
                service_account="attempt",
                audience="loom-execution",
                token="bound-west",
            )
        assert len(observed) == 1
    finally:
        await reviewer.close()


@pytest.mark.parametrize("code", [301, 401, 403, 429, 500])
async def test_native_review_failure_never_exposes_response_or_credentials(code: int) -> None:
    connection = NebiusKubernetesConnection(
        endpoint="https://west.example", ca_file=Path("unused"), credentials_file=Path("unused")
    )
    reviewer = ExecutionPodReviewer({"west": connection})
    reviewer._clients["west"] = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                code,
                text="provider secret details",
                headers={"Location": "https://foreign.example"},
            ),
        )
    )
    reviewer._credentials["west"] = SimpleNamespace(
        get_token=AsyncMock(return_value="native-secret"), close=AsyncMock()
    )
    try:
        with pytest.raises(ServiceExecutionBrokerError) as caught:
            await reviewer.review(
                cluster_scope_id="west",
                namespace="execution",
                service_account="attempt",
                audience="loom-execution",
                token="pod-secret",
            )
        assert str(caught.value) == "execution_pod_review_unavailable"
    finally:
        await reviewer.close()
