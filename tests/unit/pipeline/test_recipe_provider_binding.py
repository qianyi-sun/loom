from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import ValidationError

from loom.pipeline.control_bindings import RecipeProviderBindingApplyV1

D = "sha256:" + "c" * 64


def _payload() -> dict[str, object]:
    return {
        "status": "active",
        "recipe_digest": D,
        "environment": "staging",
        "provider_connection_id": uuid4(),
        "provider": "anthropic",
        "model": "claude-opus-4-7",
        "wire_api": "messages",
        "runner_lock_sha256": D,
        "provider_asset_manifest_sha256": D,
        "provider_asset_locks": [
            {
                "role": "primitive",
                "image_path": "/opt/behavior/provider-assets/behavior_recovery_primitive/client.bin",
                "sha256": D,
            }
        ],
        "mcp_server_locks": [
            {
                "logical_name": "recovery_video",
                "transport": "stdio",
                "interface_version": "1",
                "package_or_image_sha256": D,
                "configuration_sha256": D,
            }
        ],
        "provider_request_limit_per_attempt": 512,
        "provider_cost_limit_microusd_per_attempt": 30_000_000,
        "per_call_timeout_seconds": 600,
        "allowed_team_ids": [],
    }


def test_primitive_binding_is_exact_and_closed() -> None:
    assert RecipeProviderBindingApplyV1.model_validate(_payload()).model == "claude-opus-4-7"
    with pytest.raises(ValidationError):
        RecipeProviderBindingApplyV1.model_validate({**_payload(), "model": "fallback"})
    with pytest.raises(ValidationError, match="Extra inputs"):
        RecipeProviderBindingApplyV1.model_validate({**_payload(), "api_key": "secret"})


def test_primitive_binding_requires_exact_mcp_and_limits() -> None:
    payload = _payload()
    payload["mcp_server_locks"] = []
    with pytest.raises(ValidationError):
        RecipeProviderBindingApplyV1.model_validate(payload)
    with pytest.raises(ValidationError):
        RecipeProviderBindingApplyV1.model_validate(
            {**_payload(), "provider_request_limit_per_attempt": 511}
        )
    with pytest.raises(ValidationError):
        RecipeProviderBindingApplyV1.model_validate(
            {**_payload(), "provider_cost_limit_microusd_per_attempt": 29_999_999}
        )
    with pytest.raises(ValidationError):
        RecipeProviderBindingApplyV1.model_validate(
            {**_payload(), "per_call_timeout_seconds": 599}
        )
