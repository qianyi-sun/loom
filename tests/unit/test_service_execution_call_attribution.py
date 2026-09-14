from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from loom.auth import AuthContext
from loom_llm_gateway.routes._facade_common import build_raw_provider_log


def test_raw_provider_log_uses_authenticated_lease_not_client_metadata() -> None:
    lease_id = uuid4()
    context = AuthContext(
        token_hash=b"", type="step_session", scopes=["llm:call"], team_id=uuid4(),
        expires_at=datetime.now(UTC), trial_id=uuid4(), step_id="agent",
        service_execution_lease_id=lease_id, service_execution_generation=3,
    )
    payload = build_raw_provider_log(
        dialect="openai_facade", provider="openai-compatible",
        provider_connection_id=uuid4(), attempt=1, request_method="POST",
        request_url="https://provider.example/chat/completions",
        request_headers={"Authorization": "Bearer secret-1234"},
        request_body={"service_execution": {"lease_id": "forged", "generation": 99}},
        response_status_code=200, response_headers={},
        response_body={"service_execution": {"lease_id": "also-forged"}},
        api_key="secret-1234", auth_context=context,
    )
    assert payload["service_execution"] == {"lease_id": str(lease_id), "generation": 3}
    assert "secret-1234" not in str(payload)
