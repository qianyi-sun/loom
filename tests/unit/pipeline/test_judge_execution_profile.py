from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from loom.pipeline.control_bindings import (
    JudgeExecutionProfileApplyV1,
    JudgeExecutionProfileV1,
    registered_judge_adapter_digest,
    validate_registered_judge_adapter,
)
from loom.pipeline.keys import canonical_digest

D = "sha256:" + "a" * 64


def _payload(adapter: str = "codex_pipeline_locked_home_v1") -> dict[str, object]:
    codex = adapter == "codex_pipeline_locked_home_v1"
    return {
        "status": "active",
        "recipe_digest": D,
        "environment": "staging",
        "agent_name": "codex" if codex else "synthetic_judge",
        "agent_version": "0.146.0" if codex else "1.0.0",
        "agent_adapter": adapter,
        "agent_adapter_digest": registered_judge_adapter_digest(adapter),
        "provider_connection_id": uuid4(),
        "provider": "openai" if codex else "anthropic",
        "model": "gpt-5.6-sol" if codex else "claude-sonnet-4-6",
        "wire_api": "responses" if codex else "messages",
        "runner_lock_sha256": D,
        "provider_asset_manifest_sha256": D,
        "provider_asset_locks": [
            {
                "role": "judge",
                "image_path": "/opt/behavior/provider-assets/behavior_offline_judge/judge.bin",
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
    }


def test_two_registered_judge_combinations_are_closed_and_digest_bound() -> None:
    for adapter in ("codex_pipeline_locked_home_v1", "synthetic_judge_v1"):
        value = JudgeExecutionProfileApplyV1.model_validate(_payload(adapter))
        validate_registered_judge_adapter(value)
        assert canonical_digest(value).startswith("sha256:")


def test_judge_profile_rejects_raw_overrides_and_adapter_drift() -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        JudgeExecutionProfileApplyV1.model_validate({**_payload(), "install_script": "curl x|sh"})
    value = JudgeExecutionProfileApplyV1.model_validate(
        {**_payload(), "agent_adapter_digest": "sha256:" + "b" * 64}
    )
    with pytest.raises(ValueError, match="adapter digest"):
        validate_registered_judge_adapter(value)


def test_judge_limits_and_asset_paths_fail_closed() -> None:
    with pytest.raises(ValidationError, match="request limit"):
        JudgeExecutionProfileApplyV1.model_validate(
            {**_payload(), "provider_request_limit_per_attempt": 257}
        )
    payload = _payload()
    payload["provider_asset_locks"] = [
        {"role": "judge", "image_path": "/opt/behavior/provider-assets/x/../key", "sha256": D}
    ]
    with pytest.raises(ValidationError):
        JudgeExecutionProfileApplyV1.model_validate(payload)


def test_required_baseline_profile_name_is_accepted() -> None:
    now = datetime.now(UTC)
    profile = JudgeExecutionProfileV1.model_validate(
        {
            **_payload(),
            "schema_version": "loom.judge-execution-profile.v1",
            "profile_id": uuid4(),
            "profile_name": "behavior-judge-codex-gpt-5.6-sol-v1",
            "version": 1,
            "recipe_name": "behavior-recovery",
            "recipe_version": 1,
            "node_key": "offline_judge",
            "created_by": uuid4(),
            "created_at": now,
            "updated_by": uuid4(),
            "updated_at": now,
        }
    )
    assert profile.profile_name == "behavior-judge-codex-gpt-5.6-sol-v1"
