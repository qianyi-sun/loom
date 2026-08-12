from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from loom.pipeline.control_bindings import JudgeExecutionProfileV1
from loom_service.pipeline_control_bindings import _redacted_profile

D = "sha256:" + "d" * 64


def test_public_profile_projection_redacts_connection_and_credentials() -> None:
    now = datetime.now(UTC)
    profile = JudgeExecutionProfileV1.model_validate(
        {
            "schema_version": "loom.judge-execution-profile.v1",
            "profile_id": uuid4(),
            "profile_name": "approved_profile",
            "version": 1,
            "status": "active",
            "recipe_name": "behavior-recovery",
            "recipe_version": 1,
            "recipe_digest": D,
            "node_key": "offline_judge",
            "environment": "staging",
            "agent_name": "codex",
            "agent_version": "0.146.0",
            "agent_adapter": "codex_pipeline_locked_home_v1",
            "agent_adapter_digest": D,
            "provider_connection_id": uuid4(),
            "provider": "openai",
            "model": "gpt-5.6-sol",
            "wire_api": "responses",
            "runner_lock_sha256": D,
            "provider_asset_manifest_sha256": D,
            "provider_asset_locks": [
                {
                    "role": "judge",
                    "image_path": "/opt/behavior/provider-assets/behavior_offline_judge/judge",
                    "sha256": D,
                }
            ],
            "mcp_server_locks": [
                {
                    "logical_name": name,
                    "transport": "stdio",
                    "interface_version": "1",
                    "package_or_image_sha256": D,
                    "configuration_sha256": D,
                }
                for name in ("video", "video_demo")
            ],
            "provider_request_limit_per_attempt": 256,
            "provider_cost_limit_microusd_per_attempt": 30_000_000,
            "per_call_timeout_seconds": 60,
            "allowed_team_ids": [],
            "created_by": uuid4(),
            "created_at": now,
            "updated_by": uuid4(),
            "updated_at": now,
        }
    )
    public = _redacted_profile(profile)
    assert "provider_connection_id" not in public
    assert not {
        "api_key",
        "bearer_token",
        "credentials",
        "encrypted_api_key_ref",
    }.intersection(public)
